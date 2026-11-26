"""Experiment Agent - DID the change help, and can we trust the readout?

Checks, in order:
  1. Assignment integrity (SRM)        - broken randomization invalidates all.
  2. Sample size vs minimum detectable effect.
  3. Treatment effect                  - two-proportion z-test on success.
  4. Guardrails                        - latency, duplicate charges, refunds,
                                          gateway timeouts.
This legacy attempt-grain readout is exploratory. Use the registered
user-grain readout for rollout review.
"""
from __future__ import annotations

from paylens.agents.state import InvestigationState
from paylens.stats import ab_test as stats
from paylens.tools.governed_sql import GovernedSqlTool

EXPECTED_RATIO = {"A": 0.5, "B": 0.5}
MIN_P_VALUE = 0.05
GUARDRAIL_TOLERANCE = 0.10  # treatment may degrade a guardrail by <=10% rel.


def run_experiment(state: InvestigationState, sql: GovernedSqlTool) -> dict:
    # resolve experiment: explicit in plan, else the most recent assignment
    experiment_id = state["plan"].get("experiment_id")
    if not experiment_id:
        rows = sql.run("list_experiments", {})["rows"]
        if not rows:
            return {"agent": "experiment", "skipped": "no experiment data available"}
        experiment_id = rows[0]["experiment_id"]

    # The experiment cohort is defined by the assignment join on
    # experiment_id, not by a calendar window - time bounds stay empty
    # unless the caller pins the experiment explicitly.
    window = {"window_start": None, "window_end": None}
    assigns = sql.run("experiment_assignments", {"experiment_id": experiment_id})["rows"]
    counts = {r["variant"]: r["assigned"] for r in assigns}
    srm = stats.srm_check(counts, EXPECTED_RATIO)

    outcomes = sql.run("experiment_outcomes", {
        "experiment_id": experiment_id, **window})["rows"]
    by_variant = {r["variant"]: r for r in outcomes}
    guard = sql.run("experiment_guardrails", {
        "experiment_id": experiment_id, **window})["rows"]
    guard_by_variant = {r["variant"]: r for r in guard}
    refunds = sql.run("experiment_refunds", {
        "experiment_id": experiment_id, **window})["rows"]
    refunds_by_variant = {r["variant"]: r["refunds"] for r in refunds}
    lat_rows = sql.run("experiment_latency", {
        "experiment_id": experiment_id, **window})["rows"]
    lat_by_variant: dict[str, list[int]] = {}
    for r in lat_rows:
        lat_by_variant.setdefault(r["variant"], []).append(r["latency_ms"])

    if "A" not in by_variant or "B" not in by_variant:
        return {"agent": "experiment", "experiment_id": experiment_id,
                "skipped": "missing arm data", "srm": srm}

    a, b = by_variant["A"], by_variant["B"]
    if (a["attempts"] or 0) <= 0 or (b["attempts"] or 0) <= 0:
        # an assignment with zero attempts cannot enter the z-test (it would
        # divide by zero); report honestly instead of crashing the agent.
        return {"agent": "experiment", "experiment_id": experiment_id,
                "skipped": "arm with zero attempts; readout not possible",
                "srm": srm}
    ztest = stats.two_proportion_ztest(a["succeeded"], a["attempts"],
                                       b["succeeded"], b["attempts"])
    mde = stats.min_detectable_effect(min(a["attempts"], b["attempts"]),
                                      baseline=ztest["p_a"])

    def _rate(v):  # refund rate per successful payment
        succ = by_variant[v]["succeeded"] or 0
        return (refunds_by_variant.get(v, 0) / succ) if succ else None

    guardrails = []
    for variant in ("A", "B"):
        g = guard_by_variant.get(variant, {})
        lat = lat_by_variant.get(variant, [])
        lat_p50 = stats.percentile(lat, 0.50)
        lat_p95 = stats.percentile(lat, 0.95)
        guardrails.append({
            "variant": variant,
            "latency_p50_ms": lat_p50,
            "latency_p95_ms": lat_p95,
            "timeout_rate": round(g.get("timeout_rate") or 0.0, 4),
            "dup_charge_rate": round(g.get("dup_charge_rate") or 0.0, 4),
            "refund_rate": round(_rate(variant), 4) if _rate(variant) is not None else None,
        })
    ga = {g["variant"]: g for g in guardrails}["A"]
    gb = {g["variant"]: g for g in guardrails}["B"]

    guardrail_checks = [
        stats.guardrail_ok("latency_p95_ms", ga["latency_p95_ms"] or 0.0,
                           gb["latency_p95_ms"] or 0.0,
                           GUARDRAIL_TOLERANCE, "ms", higher_is_bad=True,
                           absolute_floor=100.0),
        stats.guardrail_ok("timeout_rate", ga["timeout_rate"], gb["timeout_rate"],
                           GUARDRAIL_TOLERANCE, "ratio", higher_is_bad=True,
                           absolute_floor=0.005),
        stats.guardrail_ok("dup_charge_rate", ga["dup_charge_rate"], gb["dup_charge_rate"],
                           GUARDRAIL_TOLERANCE, "ratio", higher_is_bad=True,
                           absolute_floor=0.005),
        stats.guardrail_ok("refund_rate", ga["refund_rate"] or 0, gb["refund_rate"] or 0,
                           GUARDRAIL_TOLERANCE, "ratio", higher_is_bad=True,
                           absolute_floor=0.005),
    ]

    decision, reasons = _decide(srm, ztest, mde, guardrail_checks, a["attempts"], b["attempts"])

    return {
        "agent": "experiment",
        "experiment_id": experiment_id,
        "assignments": counts,
        "srm": {k: (round(v, 4) if isinstance(v, float) else v) for k, v in srm.items()
                if k not in ("counts", "expected_ratio")},
        "control": {
            "variant": "A", "attempts": a["attempts"], "succeeded": a["succeeded"],
            "success_rate": round(ztest["p_a"] * 100, 2),
        },
        "treatment": {
            "variant": "B", "attempts": b["attempts"], "succeeded": b["succeeded"],
            "success_rate": round(ztest["p_b"] * 100, 2),
        },
        "effect": {
            "delta_pp": round(ztest["delta"] * 100, 2),
            "relative_uplift_pct": round((ztest["p_b"] / ztest["p_a"] - 1) * 100, 2)
                                   if ztest["p_a"] else None,
            "z": round(ztest["z"], 3),
            "p_value": f"{ztest['p_value']:.3g}",
            "significant": bool(ztest["p_value"] < MIN_P_VALUE),
            "mde_pp": round(mde * 100, 2),
        },
        "guardrails": guardrails,
        "guardrail_checks": [
            {"name": g.name, "control": g.control, "treatment": g.treatment,
             "threshold": g.threshold, "unit": g.unit, "ok": g.ok}
            for g in guardrail_checks
        ],
        "decision": decision,
        "decision_reasons": reasons,
        "provenance": {
            "templates": ["experiment_assignments", "experiment_outcomes",
                          "experiment_guardrails", "experiment_refunds",
                          "experiment_latency"],
            "statistics": "two-proportion z-test, Wilson CI, chi-square SRM",
        },
    }


def _decide(srm: dict, ztest: dict, mde: float, guardrails, n_a: int, n_b: int) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if not srm.get("ok"):
        return "invalid: randomization integrity check failed (SRM)", \
               ["sample-ratio mismatch detected; do not trust this readout"]
    if ztest["p_value"] >= MIN_P_VALUE:
        return "inconclusive", ["difference is not statistically significant at alpha=0.05"]
    if min(n_a, n_b) < 1000:
        return "inconclusive", ["sample size below the pre-registered minimum"]
    if ztest["delta"] < 0:
        return "hold: significant degradation", \
               ["treatment is significantly worse than control; do not ship"]
    if ztest["delta"] < mde:
        reasons.append("effect is significant but below the pre-registered minimum "
                       "detectable effect; business value not established")
    failed = [g.name for g in guardrails if not g.ok]
    if failed:
        return "hold: guardrail breach", [f"guardrail degraded: {', '.join(failed)}"] + reasons
    reasons.append("Legacy settled-attempt comparison needs a registered user-level readout")
    return "exploratory: registered user protocol required", reasons
