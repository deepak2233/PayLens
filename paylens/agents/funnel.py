"""Funnel Agent - WHERE payments drop.

Reads attempt-level funnel counts for the current window and the preceding
baseline window, computes per-stage conversion deltas and decides whether
the overall drop is material enough to justify deeper investigation.
All numbers come from one governed template; the LLM never touches them.
"""
from __future__ import annotations

from paylens.agents.state import InvestigationState
from paylens.tools.governed_sql import GovernedSqlTool

MATERIAL_DROP_PP = 2.0  # absolute percentage points


def _pct(x: float | None) -> float | None:
    return None if x is None else round(x * 100.0, 2)


def run_funnel(state: InvestigationState, sql: GovernedSqlTool) -> dict:
    params = {
        "window_start": state["window_start"],
        "window_end": state["window_end"],
        "method": state["plan"].get("focus_method"),
    }
    cur = sql.run("funnel_counts", params)["rows"][0]
    base_params = {
        "window_start": state["baseline_start"],
        "window_end": state["window_start"],
        "method": params["method"],
    }
    base = sql.run("funnel_counts", base_params)["rows"][0]

    stages = []
    for stage, cur_col, base_col in [
        ("started", "started", "started"),
        ("method_selected", "method_selected", "method_selected"),
        ("initiated", "initiated", "initiated"),
        ("succeeded", "succeeded", "succeeded"),
    ]:
        cur_conv = (cur[cur_col] / cur["started"]) if cur["started"] else None
        base_conv = (base[base_col] / base["started"]) if base["started"] else None
        delta = None if (cur_conv is None or base_conv is None) else (cur_conv - base_conv) * 100
        stages.append({
            "stage": stage,
            "current": cur[cur_col],
            "baseline": base[base_col],
            "conversion_current": _pct(cur_conv),
            "conversion_baseline": _pct(base_conv),
            "delta_pp": round(delta, 2) if delta is not None else None,
        })

    cur_rate = cur["succeeded"] / cur["settled"] if cur["settled"] else None
    base_rate = base["succeeded"] / base["settled"] if base["settled"] else None
    delta_pp = (cur_rate - base_rate) * 100 if (cur_rate is not None and base_rate is not None) else None

    biggest = None
    stage_deltas = [s for s in stages[1:] if s["delta_pp"] is not None]
    if stage_deltas:
        biggest = min(stage_deltas, key=lambda s: s["delta_pp"])

    return {
        "agent": "funnel",
        "window": {"start": state["window_start"].isoformat(), "end": state["window_end"].isoformat()},
        "baseline": {"start": state["baseline_start"].isoformat(), "end": state["window_start"].isoformat()},
        "stages": stages,
        "overall": {
            "success_rate_current": _pct(cur_rate),
            "success_rate_baseline": _pct(base_rate),
            "delta_pp": round(delta_pp, 2) if delta_pp is not None else None,
            "settled_current": cur["settled"],
            "settled_baseline": base["settled"],
            "pending_current": cur["started"] - cur["settled"],
        },
        "material_drop": bool(delta_pp is not None and delta_pp <= -MATERIAL_DROP_PP),
        "biggest_stage_drop": biggest,
        "metric_version": state.get("metric_version", 2),
        "provenance": {
            "templates": ["funnel_counts"],
            "attribution": "attempts attributed by started_at; settled attempts only",
        },
    }
