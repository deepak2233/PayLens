"""Explain aggregate conversion changes over fully observed start cohorts."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from itertools import groupby

from paylens.tools.governed_sql import GovernedSqlError, GovernedSqlTool

METRIC = {
    "id": "payment_success_within_sla",
    "version": 1,
    "numerator": "Attempts whose first terminal event was a success within the selected SLA",
    "denominator": "Explicitly started attempts in equal-duration cohorts with the full SLA elapsed",
}
_FIELDS = ("started", "selected", "initiated", "success_sla", "fail_sla",
           "later", "success_observed", "settled_observed", "timeout_sla")


def utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)


def query(sql: GovernedSqlTool, template: str, params: dict) -> list[dict]:
    result = sql.run(template, params)
    if result["meta"]["truncated"]:
        raise GovernedSqlError("Scope is too large to compute complete evidence")
    return result["rows"]


def _rates(counter: dict) -> dict:
    started = counter["started"]
    settled = counter["settled_observed"]
    return {
        **counter,
        "unresolved_at_cutoff": started - settled,
        "no_terminal_within_sla": started - counter["success_sla"] - counter["fail_sla"],
        "success_within_sla_pct": 100 * counter["success_sla"] / started if started else None,
        "settled_only_success_pct": 100 * counter["success_observed"] / settled if settled else None,
    }


def _attribution(groups: dict, baseline: dict, current: dict) -> tuple[dict | None, list[dict]]:
    if not baseline["started"] or not current["started"]:
        return None, []

    totals = {"within_pp": 0.0, "mix_pp": 0.0, "unmatched_pp": 0.0}
    segments = []
    for (method, gateway), periods in groups.items():
        before, after = periods["baseline"], periods["current"]
        if not before["started"] and not after["started"]:
            continue
        w_a, w_b = before["started"] / baseline["started"], after["started"] / current["started"]
        r_a, r_b = before["success_within_sla_pct"], after["success_within_sla_pct"]
        if r_a is None or r_b is None:
            within, mix, unmatched = 0.0, 0.0, w_b * (r_b or 0) - w_a * (r_a or 0)
        else:
            within, mix, unmatched = (w_a + w_b) * (r_b - r_a) / 2, (r_b + r_a) * (w_b - w_a) / 2, 0.0
        components = {"within_pp": within, "mix_pp": mix, "unmatched_pp": unmatched}
        for name, amount in components.items():
            totals[name] += amount
        segments.append({
            "method": method, "gateway": gateway, "baseline": before, "current": after,
            "baseline_share_pct": 100 * w_a, "current_share_pct": 100 * w_b,
            "impact_pp": sum(components.values()), **components,
        })
    segments.sort(key=lambda item: (item["impact_pp"], item["method"], item["gateway"]))
    return totals, segments


def build_incident(sql: GovernedSqlTool, *, observed_through: datetime, window_hours: int,
                   outcome_minutes: int, method: str | None) -> dict:
    observed_through = utc(observed_through)
    mature_end = observed_through - timedelta(minutes=outcome_minutes)
    current_start = mature_end - timedelta(hours=window_hours)
    baseline_start = current_start - timedelta(hours=window_hours)
    rows = query(sql, "cohort_sla_v1", {
        "baseline_start": baseline_start, "current_start": current_start, "mature_end": mature_end,
        "observed_through": observed_through, "outcome_seconds": 60 * outcome_minutes, "method": method,
    })
    cohorts = ("baseline", "current", "recent")
    periods = {p: dict.fromkeys(_FIELDS, 0) for p in cohorts}
    hourly = defaultdict(lambda: dict.fromkeys(_FIELDS, 0))
    groups = defaultdict(lambda: {p: dict.fromkeys(_FIELDS, 0) for p in ("baseline", "current")})
    for row in rows:
        period = row["period"]
        hour = hourly[period, row["bucket"]]
        segment = groups[row["method"], row["gateway"]].get(period)
        for field in _FIELDS:
            count = row[field]
            periods[period][field] += count
            hour[field] += count
            if segment is not None:
                segment[field] += count
    baseline, current = (_rates(periods[p]) for p in ("baseline", "current"))
    segments = {key: {p: _rates(v) for p, v in values.items()} for key, values in groups.items()}
    attribution, ranked = _attribution(segments, baseline, current)
    delta = (current["success_within_sla_pct"] - baseline["success_within_sla_pct"]
             if attribution is not None else None)
    top = ranked[0] if ranked and ranked[0]["impact_pp"] < 0 else None
    # Descriptive screening on matching relative hour buckets. A small or
    # incomplete bucket cannot trigger an incident marker.
    signals = []
    for (period, bucket), counts in sorted(hourly.items()):
        if period != "current":
            continue
        earlier = (datetime.fromisoformat(bucket) - timedelta(hours=window_hours)).strftime("%Y-%m-%dT%H")
        before = hourly.get(("baseline", earlier))
        gap = (_rates(counts)["success_within_sla_pct"] - _rates(before)["success_within_sla_pct"]
               if before and min(counts["started"], before["started"]) >= 20 else None)
        signals.append((bucket, gap is not None and gap <= -5))
    onset = None
    for flagged, run in groupby(signals, key=lambda point: point[1]):
        points = list(run)
        if flagged and len(points) >= 2:
            for (before, _), (after, _) in zip(points, points[1:]):
                if datetime.fromisoformat(after) - datetime.fromisoformat(before) == timedelta(hours=1):
                    onset = before
                    break
        if onset:
            break

    changes = query(sql, "deployments_in_window", {
        "baseline_start": current_start, "window_end": mature_end,
    })
    related = []
    if onset:
        first = utc(datetime.fromisoformat(onset))
        for change in changes:
            stamp = change["deployed_at"]
            stamp = utc(datetime.fromisoformat(stamp) if isinstance(stamp, str) else stamp)
            if abs(first - stamp) <= timedelta(hours=2) and (not top or change["gateway"] == top["gateway"]):
                related.append({**change, "deployed_at": stamp.isoformat()})
    missing_start = query(sql, "cohort_start_quality_v1", {
        "baseline_start": baseline_start, "observed_through": observed_through, "method": method,
    })[0]["missing_start"]

    return {
        "metric": METRIC, "outcome_minutes": outcome_minutes, "method": method, "delta_pp": delta,
        "windows": {"baseline_start": baseline_start, "current_start": current_start,
                    "mature_end": mature_end, "observed_through": observed_through},
        "baseline": baseline, "current": current, "recent_attempts": periods["recent"]["started"],
        "excluded_without_start": missing_start, "attribution": attribution, "segments": ranked[:30],
        "segment_count": len(ranked),
        "hourly": [{"cohort": p, "hour": bucket, **_rates(v)}
                   for (p, bucket), v in sorted(hourly.items()) if p != "recent"],
        "incident": {"onset_hour_utc": onset, "nearby_changes": related, "largest_loss": top},
    }
