"""Anomaly Agent - WHAT changed.

For the worst affected segment this agent builds a success-rate time series
at the configured grain (hour for the intraday demo store, day for
multi-month real sources), detects a change point by comparing each bucket
against its matched baseline bucket (hour-of-day / weekday pooled,
volume-aware z-score), and correlates the change point with infrastructure
deployment records.

The output is explicitly framed as *timing evidence*: correlation with a
deployment is a lead for engineers, never proof of causality.
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta

from paylens.agents.state import InvestigationState
from paylens.tools.governed_sql import GovernedSqlTool

Z_THRESHOLD = 2.0
MIN_DROP_PP = 5.0
MIN_SUSTAINED_HOURS = 2
# A deployment counts as a correlate if it landed up to 1h BEFORE the
# change-point bucket (the bucket may already contain degraded traffic from
# the first minutes of the hour) or within 6h after it. Day grain widens the
# windows to one and three days respectively.
CORRELATION_WINDOW = timedelta(hours=6)
CORRELATION_LEAD_TOLERANCE = timedelta(hours=1)
DAY_CORRELATION_WINDOW = timedelta(days=3)
DAY_CORRELATION_LEAD_TOLERANCE = timedelta(days=1)


def _z_rate(rate: float, volume: int, base_rate: float, base_volume: int) -> float:
    """Two-proportion pooled-variance z-score for one bucket vs its baseline."""
    p = (rate * volume + base_rate * base_volume) / (volume + base_volume)
    se = math.sqrt(max(p * (1 - p) * (1 / volume + 1 / base_volume), 1e-12))
    return (rate - base_rate) / se


def _hour_series(state, sql, method, gateway) -> tuple[list, dict | None]:
    rows = sql.run("hourly_success_rate", {
        "window_start": state["baseline_start"],
        "window_end": state["window_end"],
        "method": method,
        "gateway": gateway,
    })["rows"]
    hourly = {r["hour_bucket"]: (r["success_rate"], r["settled"])
              for r in rows if r["hour_bucket"]}

    # hour-of-day baseline from the baseline day(s)
    base_day_start = state["baseline_start"]
    cur_day_start = state["window_start"]
    baseline_by_hod: dict[int, tuple[float, int]] = {}
    for h in range(24):
        ts = base_day_start + timedelta(hours=h)
        key = ts.strftime("%Y-%m-%dT%H")
        if key in hourly:
            baseline_by_hod[h] = hourly[key]

    change_point = None
    series = []
    run_len = 0
    for h in range(24):
        ts = cur_day_start + timedelta(hours=h)
        key = ts.strftime("%Y-%m-%dT%H")
        if key not in hourly:
            run_len = 0
            continue
        rate, volume = hourly[key]
        base_rate, base_volume = baseline_by_hod.get(h, (None, 0))
        drop_pp = None
        z = None
        if base_rate is not None and base_volume > 0 and volume > 0:
            drop_pp = (rate - base_rate) * 100
            z = _z_rate(rate, volume, base_rate, base_volume)
        severe = (z is not None and z < -Z_THRESHOLD
                  and drop_pp is not None and drop_pp <= -MIN_DROP_PP)
        run_len = run_len + 1 if severe else 0
        series.append({
            "hour": ts.strftime("%H:00"),
            "date": ts.date().isoformat(),
            "success_rate": round(rate * 100, 2) if rate is not None else None,
            "baseline_same_hour": round(base_rate * 100, 2) if base_rate is not None else None,
            "delta_pp": round(drop_pp, 2) if drop_pp is not None else None,
            "z": round(z, 2) if z is not None else None,
            "attempts": volume,
        })
        if severe and run_len >= MIN_SUSTAINED_HOURS and change_point is None:
            change_point = series[-min(run_len, 2)]  # first hour of the sustained run
    if change_point is not None:
        change_point = dict(change_point)
        change_point["label"] = f"{change_point['date']} {change_point['hour']}"
    return series, change_point


def _day_series(state, sql, method, gateway) -> tuple[list, dict | None]:
    """Day-grain change-point detection for multi-month real sources.

    Each current-window day is compared against the pooled settled rate of
    the same *weekday* across the baseline window (weekly seasonality is the
    dominant pattern at day grain). Censoring-safe: days without settled
    attempts are absent from the governed query and never treated as zeros.
    """
    rows = sql.run("daily_success_rate", {
        "window_start": state["baseline_start"],
        "window_end": state["window_end"],
        "method": method,
        "gateway": gateway,
    })["rows"]
    daily = {r["hour_bucket"]: (r["success_rate"], r["settled"])
             for r in rows if r["hour_bucket"]}

    # weekday-pooled baseline over the baseline window
    base_by_wd: dict[int, tuple[float, float]] = {}  # weekday -> (succ, settled)
    ts = state["baseline_start"]
    while ts < state["window_start"]:
        r = daily.get(ts.date().isoformat())
        if r and r[1] > 0:
            succ, settled = base_by_wd.get(ts.weekday(), (0.0, 0))
            base_by_wd[ts.weekday()] = (succ + r[0] * r[1], settled + r[1])
        ts += timedelta(days=1)

    change_point = None
    series = []
    run_len = 0
    ts = state["window_start"]
    while ts < state["window_end"]:
        cur_ts = ts
        key = cur_ts.date().isoformat()
        ts += timedelta(days=1)
        if key not in daily:
            run_len = 0
            continue
        rate, volume = daily[key]
        base_rate = base_vol = None
        wd = cur_ts.weekday()
        if wd in base_by_wd and base_by_wd[wd][1] > 0:
            succ, base_vol = base_by_wd[wd]
            base_rate = succ / base_vol
        drop_pp = None
        z = None
        if base_rate is not None and base_vol > 0 and volume > 0:
            drop_pp = (rate - base_rate) * 100
            z = _z_rate(rate, volume, base_rate, base_vol)
        severe = (z is not None and z < -Z_THRESHOLD
                  and drop_pp is not None and drop_pp <= -MIN_DROP_PP)
        run_len = run_len + 1 if severe else 0
        series.append({
            "hour": "daily",
            "date": key,
            "success_rate": round(rate * 100, 2),
            "baseline_same_hour": round(base_rate * 100, 2) if base_rate is not None else None,
            "delta_pp": round(drop_pp, 2) if drop_pp is not None else None,
            "z": round(z, 2) if z is not None else None,
            "attempts": volume,
        })
        if severe and run_len >= MIN_SUSTAINED_HOURS and change_point is None:
            change_point = dict(series[-min(run_len, 2)])
            change_point["label"] = key
    return series, change_point


def run_anomaly(state: InvestigationState, sql: GovernedSqlTool) -> dict:
    segments = state.get("segments", {}).get("affected_segments") or []
    if not segments:
        return {"agent": "anomaly", "skipped": "no affected segment to investigate"}

    grain = state.get("config", {}).get("grain", "hour") if state.get("config") else "hour"
    target = segments[0]
    method, gateway = target["method"], target["gateway"]
    if grain == "day":
        series, change_point = _day_series(state, sql, method, gateway)
        lead_tol, corr_win = DAY_CORRELATION_LEAD_TOLERANCE, DAY_CORRELATION_WINDOW
    else:
        series, change_point = _hour_series(state, sql, method, gateway)
        lead_tol, corr_win = CORRELATION_LEAD_TOLERANCE, CORRELATION_WINDOW

    deployments = sql.run("deployments_in_window", {
        "baseline_start": state["baseline_start"],
        "window_end": state["window_end"],
    })["rows"]

    correlated = None
    if change_point:
        cp_ts = datetime.fromisoformat(f"{change_point['date']}T{change_point['hour'][:5]}"
                                       if change_point["hour"] != "daily"
                                       else f"{change_point['date']}T00:00")
        candidates = []
        for dep in deployments:
            dep_ts = dep["deployed_at"]
            if isinstance(dep_ts, str):
                dep_ts = datetime.fromisoformat(dep_ts)
            delta = cp_ts - dep_ts
            same_scope = (not dep.get("gateway")
                          or dep["gateway"] == gateway
                          or dep["service"].startswith("payment"))
            if (-lead_tol) <= delta <= corr_win and same_scope:
                lead = int(delta.total_seconds() // 60)
                candidates.append({
                    "change_id": dep["change_id"],
                    "service": dep["service"],
                    "gateway": dep["gateway"],
                    "deployed_at": dep_ts.isoformat(),
                    "note": dep["note"],
                    "lead_time_minutes": lead,
                    "relation": ("within the change-point bucket"
                                 if delta <= lead_tol else "preceded the change point")
                                 if delta <= timedelta(0) else "followed the change point",
                })
        if candidates:
            correlated = min(candidates, key=lambda c: abs(c["lead_time_minutes"]))

    # Pre/post context: mean settled success rate on either side of the change
    # point (baseline-window buckets count toward the "before" side).
    mean_before = mean_after = None
    if change_point:
        cp_index = next((i for i, s in enumerate(series)
                         if s["date"] == change_point["date"]
                         and s["hour"] == change_point["hour"]), None)
        if cp_index is not None:
            pre = [s["success_rate"] for s in series[:cp_index] if s["success_rate"] is not None]
            post = [s["success_rate"] for s in series[cp_index:] if s["success_rate"] is not None]
            mean_before = round(sum(pre) / len(pre), 2) if pre else None
            mean_after = round(sum(post) / len(post), 2) if post else None
    return {
        "agent": "anomaly",
        "grain": grain,
        "segment": {"method": method, "gateway": gateway},
        "hourly_series": series,
        "change_point": change_point,
        "rate_context": {"mean_before_change_point": mean_before,
                         "mean_after_change_point": mean_after},
        "correlated_deployment": correlated,
        "interpretation": (
            "Timing evidence only: the metric change point follows the listed "
            "deployment; causality must be confirmed by engineers in gateway "
            "logs before any rerouting or rollback decision."
        ),
        "provenance": {
            "templates": ["daily_success_rate" if grain == "day" else "hourly_success_rate",
                          "deployments_in_window"],
            "method": (f"{grain}-over-{grain} volume-aware z-score vs matched baseline "
                       f"({'weekday-pooled' if grain == 'day' else 'hour-of-day'}), "
                       f"sustained for >=2 {grain}s"),
        },
    }
