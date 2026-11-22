"""Hybrid reasoning client.

Split of responsibilities (the PayLens governance contract):

* **LLM (optional)**     - planning choices and natural-language narration.
  It may only choose from machine-verifiable options (template names,
  dimension names, booleans) and is told never to invent numbers.
* **SQL + Python**       - every number, rate, p-value and recommendation
  gate. Even narration receives facts as structured inputs and only words
  them; it cannot alter numeric fields.

With ``PAYLENS_LLM_PROVIDER=none`` (the default) everything still runs: a
deterministic keyword planner and a fact-driven template narrator take over,
so the demo needs no API key and produces identical numeric output.
"""
from __future__ import annotations

import json
import re
from typing import Any

import httpx

from paylens.config import SETTINGS


class LLMClient:
    def __init__(self, provider: str | None = None) -> None:
        self.provider = provider or SETTINGS.llm_provider
        self._client: httpx.Client | None = None
        if self.provider == "openai" and SETTINGS.llm_api_key:
            self._client = httpx.Client(
                base_url=SETTINGS.llm_base_url or "https://api.openai.com/v1",
                headers={"Authorization": f"Bearer {SETTINGS.llm_api_key}"},
                timeout=SETTINGS.llm_timeout_s,
            )

    @property
    def enabled(self) -> bool:
        return self.provider == "openai" and self._client is not None

    # --------------------------------------------------------------- planning
    def plan(self, question: str, horizon_end: Any) -> dict:
        """Return an investigation plan. Validated; LLM output is advisory."""
        fallback = deterministic_plan(question, horizon_end)
        if not self.enabled:
            return fallback
        system = (
            "You are the planner of a payment analytics system. Choose an "
            "investigation plan as strict JSON with keys: window_hours (int), "
            "focus_method (one of upi, debit_card, credit_card, netbanking or null), "
            "run_funnel (bool), run_segment (bool), run_anomaly (bool), "
            "run_experiment (bool), rationale (string). Do not invent numbers."
        )
        try:
            resp = self._client.post(
                "/chat/completions",
                json={
                    "model": SETTINGS.llm_model,
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user",
                         "content": f"Investigation request: {question} "
                                    f"Data horizon end: {horizon_end}"},
                    ],
                    "temperature": 0,
                    "response_format": {"type": "json_object"},
                },
            )
            resp.raise_for_status()
            plan = json.loads(resp.json()["choices"][0]["message"]["content"])
            return _validate_plan(plan, fallback)
        except Exception:
            # LLM unavailable or returned garbage: fail safe to the
            # deterministic planner. Investigation must never block on the LLM.
            return fallback

    # ------------------------------------------------------------- narration
    def narrate(self, section: str, facts: dict, fallback: str) -> str:
        """Word a section from structured facts. Numeric fields are pinned."""
        if not self.enabled:
            return fallback
        system = (
            "You write one concise factual paragraph for a payment analytics "
            "report section. Use ONLY the numbers provided in FACTS; never "
            "compute, round or invent additional figures. Keep numbers and "
            "units exactly as given."
        )
        try:
            resp = self._client.post(
                "/chat/completions",
                json={
                    "model": SETTINGS.llm_model,
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user",
                         "content": f"SECTION: {section}\nFACTS: {json.dumps(facts, default=str)}"},
                    ],
                    "temperature": 0,
                },
            )
            resp.raise_for_status()
            text = resp.json()["choices"][0]["message"]["content"].strip()
            guarded = _strip_unsupported_numbers(text, facts)
            return guarded if guarded else fallback
        except Exception:
            return fallback


# --------------------------------------------------------------------------
# deterministic fallbacks (provider = none, or LLM failure)
# --------------------------------------------------------------------------
METHOD_KEYWORDS = {
    "upi": "upi",
    "debit": "debit_card",
    "credit card": "credit_card",
    "netbanking": "netbanking",
}


def deterministic_plan(question: str, horizon_end: Any) -> dict:
    q = question.lower()
    focus = None
    for kw, method in METHOD_KEYWORDS.items():
        if re.search(rf"\b{kw}\b", q):
            focus = method
            break
    window_hours = 24
    m = re.search(r"last\s+(\d+)\s*hours?", q)
    if m:
        window_hours = min(int(m.group(1)), 72)
    wants_experiment = bool(re.search(r"experiment|a/b|rollout|checkout", q))
    return {
        "window_hours": window_hours,
        "focus_method": focus,
        "run_funnel": True,
        "run_segment": True,
        "run_anomaly": True,
        "run_experiment": True,
        "wants_experiment_explicit": wants_experiment,
        "rationale": (
            f"Deterministic plan: {window_hours}h window"
            + (f", focus method '{focus}'" if focus else ", all methods")
            + "; funnel first, then segments, anomaly and experiment checks."
        ),
        "planner": "deterministic",
    }


def _validate_plan(plan: dict, fallback: dict) -> dict:
    """Coerce an LLM plan onto the allowed option set; drop invalid fields."""
    out = dict(fallback)
    out["planner"] = "llm"
    try:
        wh = int(plan.get("window_hours", fallback["window_hours"]))
        if 1 <= wh <= 72:
            out["window_hours"] = wh
    except (TypeError, ValueError):
        pass
    fm = plan.get("focus_method")
    if fm in {"upi", "debit_card", "credit_card", "netbanking", None}:
        out["focus_method"] = fm
    for key in ("run_funnel", "run_segment", "run_anomaly", "run_experiment"):
        if isinstance(plan.get(key), bool):
            out[key] = plan[key]
    rat = plan.get("rationale")
    if isinstance(rat, str) and 0 < len(rat) < 500:
        out["rationale"] = rat
    return out


_NUM_TOKEN = re.compile(r"-?\d[\d,]*\.?\d*\s*(?:%|pp|ms|x|bn|k)?", re.IGNORECASE)


def _strip_unsupported_numbers(text: str, facts: dict) -> str:
    """Guardrail: narration may only contain numbers that appear in facts.

    If any un-pinned number is found, fall back to the deterministic
    narration entirely (conservative, keeps identifiers intact).
    """
    allowed: set[float] = set()

    def collect(node) -> None:
        if isinstance(node, dict):
            for v in node.values():
                collect(v)
        elif isinstance(node, (list, tuple)):
            for v in node:
                collect(v)
        elif isinstance(node, bool):
            return
        elif isinstance(node, (int, float)):
            allowed.add(float(node))
            allowed.add(round(float(node), 2))
            allowed.add(round(float(node), 4))
            allowed.add(float(f"{node:.2f}"))
            allowed.add(float(f"{node:.1f}"))
            allowed.add(float(f"{node:g}"))

    collect(facts)
    for token in _NUM_TOKEN.findall(text):
        raw = token.strip()
        for suffix in ("%", "pp", "ms", "bn", "k", "x"):
            if raw.lower().endswith(suffix):
                raw = raw[: -len(suffix)]
        raw = raw.replace(",", "")
        if not raw:
            continue
        try:
            val = float(raw)
        except ValueError:
            continue
        if not any(abs(val - a) <= max(1e-9, abs(a) * 1e-6) for a in allowed):
            return fallback_text(text)
    return text


def fallback_text(text: str) -> str:
    # marker consumed by narrate(): signals "LLM wording rejected"
    return ""
