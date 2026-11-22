"""Segment Agent - WHO is affected.

Runs only after the Funnel Agent found a material drop. Compares settled
success rates across approved dimensions and method x gateway slices,
ranks segments by "success deficit" (rate loss weighted by volume) and
returns the segments that concentrate the drop.
"""
from __future__ import annotations

from paylens.agents.state import InvestigationState
from paylens.tools.governed_sql import GovernedSqlTool

DIMENSIONS = ["method", "gateway", "bank", "app_version", "geo", "customer_tier"]
MIN_VOLUME = 200          # a segment needs volume before we blame it
MIN_DELTA_PP = 3.0        # and a clear adverse delta


def _rate(succ: int | None, settled: int | None) -> float | None:
    if not settled:
        return None
    return round(succ / settled * 100.0, 2)


def run_segment(state: InvestigationState, sql: GovernedSqlTool) -> dict:
    common = {
        "window_start": state["window_start"],
        "window_end": state["window_end"],
        "baseline_start": state["baseline_start"],
        "method": state["plan"].get("focus_method"),
    }

    dimension_rankings: dict[str, list[dict]] = {}
    for dim in DIMENSIONS:
        rows = sql.run("success_rate_by_dimension", common, dimension=dim)["rows"]
        ranked = []
        for r in rows:
            if not r["cur_settled"] and not r["base_settled"]:
                continue
            cur = _rate(r["cur_succeeded"], r["cur_settled"])
            base = _rate(r["base_succeeded"], r["base_settled"])
            delta = (cur - base) if (cur is not None and base is not None) else None
            deficit = ((base - cur) / 100.0 * r["cur_settled"]) if delta is not None else 0.0
            ranked.append({
                "value": str(r["dim_value"]),
                "success_rate_current": cur,
                "success_rate_baseline": base,
                "delta_pp": round(delta, 2) if delta is not None else None,
                "attempts_current": r["cur_settled"],
                "success_deficit": round(deficit, 1),
            })
        ranked.sort(key=lambda d: d["success_deficit"], reverse=True)
        dimension_rankings[dim] = ranked

    # method x gateway slices (single governed query per window)
    slice_rows_cur = sql.run("segment_profile", {
        "window_start": state["window_start"], "window_end": state["window_end"],
        "method": common["method"], "gateway": None,
    })["rows"]
    slice_rows_base = sql.run("segment_profile", {
        "window_start": state["baseline_start"], "window_end": state["window_start"],
        "method": common["method"], "gateway": None,
    })["rows"]
    base_by_key = {(r["method"], r["gateway"]): r for r in slice_rows_base}

    combos = []
    for r in slice_rows_cur:
        b = base_by_key.get((r["method"], r["gateway"]))
        cur = _rate(r["succeeded"], r["settled"])
        base = _rate(b["succeeded"], b["settled"]) if b else None
        delta = (cur - base) if (cur is not None and base is not None) else None
        deficit = ((base - cur) / 100.0 * r["settled"]) if delta is not None else 0.0
        combos.append({
            "method": r["method"],
            "gateway": r["gateway"],
            "success_rate_current": cur,
            "success_rate_baseline": base,
            "delta_pp": round(delta, 2) if delta is not None else None,
            "attempts_current": r["attempts"],
            "settled_current": r["settled"],
            "pending_current": r["pending"],
            "success_deficit": round(deficit, 1),
        })
    combos.sort(key=lambda c: c["success_deficit"], reverse=True)

    affected = [c for c in combos
                if c["delta_pp"] is not None and c["delta_pp"] <= -MIN_DELTA_PP
                and c["settled_current"] >= MIN_VOLUME]

    return {
        "agent": "segment",
        "dimension_rankings": dimension_rankings,
        "method_gateway_slices": combos,
        "affected_segments": affected,
        "thresholds": {"min_volume": MIN_VOLUME, "min_delta_pp": MIN_DELTA_PP},
        "provenance": {
            "templates": ["success_rate_by_dimension", "segment_profile"],
            "dimensions": DIMENSIONS,
        },
    }
