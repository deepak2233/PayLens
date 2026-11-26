"""Statistical engine for A/B experiments.

Pure Python (no network, no LLM): two-proportion z-test, Wilson confidence
intervals, sample-ratio-mismatch (SRM) check, percentile latency. These are
the ONLY functions allowed to produce experiment p-values in PayLens.
"""
from __future__ import annotations

import math
from dataclasses import dataclass


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def two_proportion_ztest(success_a: int, n_a: int, success_b: int, n_b: int) -> dict:
    """H0: p_a == p_b. Returns z, two-sided p-value and the absolute delta."""
    if n_a <= 0 or n_b <= 0:
        raise ValueError("both arms need positive sample sizes")
    p_a = success_a / n_a
    p_b = success_b / n_b
    p_pool = (success_a + success_b) / (n_a + n_b)
    se = math.sqrt(p_pool * (1 - p_pool) * (1 / n_a + 1 / n_b))
    if se == 0:
        return {"z": 0.0, "p_value": 1.0, "delta": p_b - p_a, "p_a": p_a, "p_b": p_b}
    z = (p_b - p_a) / se
    # clamp at double-precision resolution: |z| beyond ~8.3 underflows to 0
    p_value = max(2.0 * (1.0 - _norm_cdf(abs(z))), 1e-16)
    return {"z": z, "p_value": p_value, "delta": p_b - p_a, "p_a": p_a, "p_b": p_b}


def wilson_interval(successes: int, n: int, alpha: float = 0.05) -> tuple[float, float]:
    """Wilson score interval - safe for the small-p rates seen in guardrails.

    ``n == 0`` means no information: the interval spans the full [0, 1]
    range rather than claiming certainty about a rate that was never measured.
    """
    if n == 0:
        return (0.0, 1.0)
    z = 1.959963984540054 if alpha == 0.05 else _z_from_alpha(alpha)
    p = successes / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return (max(0.0, centre - half), min(1.0, centre + half))


def _z_from_alpha(alpha: float) -> float:
    """Two-sided z quantile: CDF(1 - alpha/2)."""
    return _z_at_cdf(1 - alpha / 2)


def _z_at_cdf(p: float) -> float:
    # inverse of the standard normal CDF via bisection (sufficient precision)
    lo, hi = -10.0, 10.0
    for _ in range(200):
        mid = (lo + hi) / 2
        if _norm_cdf(mid) < p:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def srm_check(counts: dict[str, int], expected_ratio: dict[str, float]) -> dict:
    """Chi-square goodness-of-fit against the planned allocation (df = k-1).

    A significant mismatch means randomization integrity is broken and the
    experiment readout must not be trusted.

    Edge cases made explicit:
    * arms observed in the data but absent from the plan (rogue arms) fail
      the check outright - they are never silently ignored;
    * planned arms with zero observations contribute to chi-square
      (observed = 0), so an entirely missing arm is caught;
    * un-normalized expected ratios are normalized defensively; a plan with
      no positive ratios is invalid;
    * empty counts are "no assignments", not a pass.
    """
    planned = {arm: r for arm, r in expected_ratio.items() if r > 0}
    if not planned:
        return {"ok": False, "reason": "no planned allocation",
                "chi2": None, "p_value": None}
    total = sum(counts.values())
    if total == 0:
        return {"ok": False, "reason": "no assignments",
                "chi2": None, "p_value": None}
    ratio_sum = sum(planned.values())

    rogue = sorted(a for a in counts if a not in planned and counts[a] > 0)
    chi2 = 0.0
    arms = sorted(set(planned) | set(counts))
    for arm in arms:
        observed = counts.get(arm, 0)
        expected = total * planned.get(arm, 0.0) / ratio_sum
        if expected == 0:
            continue  # rogue arm: flagged via `rogue`, never a 0-division
        chi2 += (observed - expected) ** 2 / expected
    df = max(1, len(planned) - 1)
    p_value = _chi2_sf(chi2, df)
    out: dict = {
        "ok": p_value >= 0.001 and not rogue,  # industry-standard SRM threshold
        "chi2": chi2,
        "p_value": p_value,
        "degrees_of_freedom": df,
        "counts": counts,
        "expected_ratio": expected_ratio,
    }
    if rogue:
        out["reason"] = f"arms observed but not planned: {rogue}"
    return out


def _chi2_sf(x: float, df: int) -> float:
    """Survival function of the chi-square distribution.

    df == 1 uses the exact normal relation; df > 1 uses the
    Wilson-Hilferty transform (accurate to ~1e-3 for the tail region the
    SRM threshold cares about, without a scipy dependency).
    """
    if df == 1:
        if x <= 0:
            return 1.0
        return 2.0 * (1.0 - _norm_cdf(math.sqrt(x)))
    if x <= 0:
        return 1.0
    z = ((x / df) ** (1.0 / 3.0) - (1.0 - 2.0 / (9.0 * df))) / math.sqrt(2.0 / (9.0 * df))
    return 1.0 - _norm_cdf(z)


def percentile(values: list[int | float], q: float) -> float | None:
    """Linear-interpolated percentile; ``None`` when there are no samples.

    Returning 0.0 for an empty list would fabricate a "0 ms" latency in
    reports - honest absence is reported as ``None`` instead.
    """
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * q
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(s[int(k)])
    return s[f] + (s[c] - s[f]) * (k - f)


def min_detectable_effect(n_per_arm: int, baseline: float, power: float = 0.8, alpha: float = 0.05) -> float:
    """Approximate MDE (absolute proportion points) for a two-proportion test."""
    if n_per_arm <= 0:
        raise ValueError("n_per_arm must be positive")
    # the variance term degenerates at baseline 0 or 1 - clamp so the
    # approximation stays finite for all-dead or all-alive baselines
    base = min(max(baseline, 1e-6), 1 - 1e-6)
    z_alpha = _z_at_cdf(1 - alpha / 2)
    z_beta = _z_at_cdf(power)
    return (z_alpha + z_beta) * math.sqrt(2 * base * (1 - base) / n_per_arm)


@dataclass
class Guardrail:
    name: str
    control: float
    treatment: float
    threshold: float
    unit: str
    ok: bool


def guardrail_ok(name: str, control: float, treatment: float,
                 threshold: float, unit: str, higher_is_bad: bool,
                 absolute_floor: float | None = None) -> Guardrail:
    """A guardrail breaches only if the treatment degrades it beyond BOTH the
    relative threshold and an absolute floor - this avoids flagging noise on
    rare-event rates (e.g. 1 vs 2 duplicate charges in ten thousand)."""
    if higher_is_bad:
        degraded = treatment > control * (1 + threshold)
    else:
        degraded = treatment < control * (1 - threshold)
    if degraded and absolute_floor is not None:
        degraded = abs(treatment - control) > absolute_floor
    ok = not degraded
    return Guardrail(name=name, control=control, treatment=treatment,
                     threshold=threshold, unit=unit, ok=ok)


def difference_interval(success_a: int, n_a: int, success_b: int, n_b: int,
                        alpha: float = 0.05) -> tuple[float, float]:
    """Newcombe hybrid-score confidence interval for independent B minus A risks."""
    p_a, p_b = success_a / n_a, success_b / n_b
    a_low, a_high = wilson_interval(success_a, n_a, alpha)
    b_low, b_high = wilson_interval(success_b, n_b, alpha)
    delta = p_b - p_a
    return (delta - math.hypot(p_b - b_low, a_high - p_a),
            delta + math.hypot(b_high - p_b, p_a - a_low))
