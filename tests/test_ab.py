"""Statistical engine: known-answer tests against scipy where available."""
from __future__ import annotations

import math

from paylens.stats import ab_test as st


def test_two_proportion_ztest_known_values():
    # 40,000/50,000 vs 41,000/50,000 - the reference readout
    res = st.two_proportion_ztest(40_000, 50_000, 41_000, 50_000)
    assert math.isclose(res["p_a"], 0.80)
    assert math.isclose(res["p_b"], 0.82)
    assert math.isclose(res["delta"], 0.02)
    # z ~ 8.06 for this classic setup
    assert abs(res["z"]) > 7.5
    try:
        from scipy.stats import norm

        p_scipy = 2 * (1 - norm.cdf(abs(res["z"])))
        assert abs(res["p_value"] - p_scipy) < 1e-9
    except ImportError:
        assert res["p_value"] < 1e-14


def test_ztest_equal_proportions_gives_p_one():
    res = st.two_proportion_ztest(4_000, 50_000, 4_000, 50_000)
    assert math.isclose(res["p_value"], 1.0)
    assert math.isclose(res["z"], 0.0)


def test_wilson_interval_contains_estimate_and_shrinks():
    lo, hi = st.wilson_interval(40_000, 50_000)
    assert lo < 0.80 < hi
    assert hi - lo < 0.02
    lo_small, hi_small = st.wilson_interval(0, 5)
    assert lo_small == 0.0 and hi_small < 0.5  # rare events stay conservative


def test_srm_detects_ratio_mismatch():
    ok = st.srm_check({"A": 5_000, "B": 5_000}, {"A": 0.5, "B": 0.5})
    assert ok["ok"] is True
    bad = st.srm_check({"A": 5_500, "B": 4_500}, {"A": 0.5, "B": 0.5})
    assert bad["ok"] is False and bad["p_value"] < 0.001


def test_percentile_interpolates():
    vals = [10, 20, 30, 40]
    assert st.percentile(vals, 0.5) == 25.0
    assert st.percentile(vals, 0.95) == 38.5
    assert st.percentile([], 0.5) is None  # honest absence, never a fabricated 0 ms


def test_mde_matches_reference_scale():
    # n=50k/arm, baseline 80% -> MDE ~ 0.71pp with the standard 80%-power formula
    mde = st.min_detectable_effect(50_000, 0.80)
    assert 0.0065 < mde < 0.008


def test_guardrail_needs_relative_and_absolute_degradation():
    # 1 vs 2 duplicate charges in 10k: relative breach but absolute noise
    g = st.guardrail_ok("dup", 0.0, 0.0002, 0.10, "ratio",
                        higher_is_bad=True, absolute_floor=0.005)
    assert g.ok is True
    # a real breach: +2pp absolute on a rate
    g2 = st.guardrail_ok("timeout", 0.02, 0.05, 0.10, "ratio",
                         higher_is_bad=True, absolute_floor=0.005)
    assert g2.ok is False
    # lower-is-bad direction (latency)
    g3 = st.guardrail_ok("p95", 1500.0, 1800.0, 0.10, "ms",
                         higher_is_bad=True, absolute_floor=100.0)
    assert g3.ok is False


def test_srm_supports_multi_arm_allocations():
    # three arms, balanced -> pass
    ok = st.srm_check({"A": 10_000, "B": 10_000, "C": 10_000},
                      {"A": 1 / 3, "B": 1 / 3, "C": 1 / 3})
    assert ok["ok"] is True and ok["degrees_of_freedom"] == 2
    # three arms, badly skewed -> fail with df=2 chi-square (Wilson-Hilferty path)
    bad = st.srm_check({"A": 15_000, "B": 10_000, "C": 5_000},
                       {"A": 1 / 3, "B": 1 / 3, "C": 1 / 3})
    assert bad["ok"] is False and bad["p_value"] < 0.001
