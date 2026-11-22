"""Upworthy adapter: real randomized A/B tests -> governed readouts.

The archive stores *aggregate* arm units: impressions and clicks per package
variant. Readouts therefore run the same statistics as the in-store
Experiment Agent (SRM gate -> two-proportion z-test -> MDE -> decision) on
the real aggregate counts. No user-level rows are synthesized - the store
never fabricates microdata that a source did not publish.

Expected allocation: Upworthy randomized equally among the variants of a
test, so SRM uses a uniform expected ratio over the test's k arms (the
general-df chi-square path). Multi-arm tests are read out as one comparison
per non-control arm against the control arm with Bonferroni-corrected
alpha; significance requires the corrected p to clear 0.05.
"""
from __future__ import annotations

import csv
from collections import defaultdict

from paylens.realdata.contracts import UpworthyRow
from paylens.stats import ab_test as stats

MIN_P_VALUE = 0.05  # mirrors paylens.agents.experiment
MIN_ARM_UNITS = 1000  # mirrors the pre-registered minimum scale
SOURCE_LABEL = "Upworthy Research Archive (CC BY 4.0)"


def _validate_rows(path) -> list[UpworthyRow]:
    rows: list[UpworthyRow] = []
    with path.open(newline="", encoding="utf-8") as f:
        for raw in csv.DictReader(f):
            raw = {k: v for k, v in raw.items() if k not in ("", None)}
            try:
                rows.append(UpworthyRow(
                    package_index=len(rows),
                    clickability_test_id=raw["clickability_test_id"],
                    headline=raw.get("headline") or "",
                    impressions=int(raw["impressions"]),
                    clicks=int(raw["clicks"] or 0),
                ))
            except Exception:
                continue  # counted in the loader's validation report, not here
    return rows


def _readout(test_id: str, arms: list[UpworthyRow]) -> dict:
    """One test readout on real aggregate counts.

    Control = first arm; each remaining arm is compared against the control
    with a Bonferroni-corrected alpha (k-1 comparisons). The reported
    treatment is the comparison with the smallest corrected p.
    """
    control, treatments = arms[0], arms[1:]
    k = len(arms)
    names = [f"pkg{a.package_index}" for a in arms]
    srm = stats.srm_check({n: a.impressions for n, a in zip(names, arms)},
                          {n: 1.0 / k for n in names})

    best = None
    for t in treatments:
        z = stats.two_proportion_ztest(control.clicks, control.impressions,
                                       t.clicks, t.impressions)
        corrected_p = min(1.0, z["p_value"] * len(treatments))
        if best is None or corrected_p < best["corrected_p"]:
            best = {"t": t, "z": z, "corrected_p": corrected_p}
    t, ztest, corrected_p = best["t"], best["z"], best["corrected_p"]

    mde = stats.min_detectable_effect(min(control.impressions, t.impressions),
                                      baseline=ztest["p_a"])

    if not srm.get("ok"):
        decision = "invalid: randomization integrity check failed (SRM)"
        reasons = ["sample-ratio mismatch against the planned equal split"]
    elif corrected_p >= MIN_P_VALUE:
        decision, reasons = "inconclusive", ["not significant at Bonferroni-corrected alpha=0.05"]
    elif min(control.impressions, t.impressions) < MIN_ARM_UNITS:
        decision, reasons = "inconclusive", ["arm size below the minimum scale"]
    elif ztest["delta"] < 0:
        decision = "hold: treatment significantly worse than control"
        reasons = ["real negative effect; do not ship this variant"]
    elif ztest["delta"] < mde:
        decision = "ship candidate: recommend staged rollout with human approval"
        reasons = ["significant, but below the pre-registered minimum detectable effect"]
    else:
        decision = "ship candidate: recommend staged rollout with human approval"
        reasons = ["assignment integrity, significance and scale passed"]

    def arm(v: UpworthyRow) -> dict:
        return {
            "variant": f"pkg{v.package_index}",
            "units": v.impressions,
            "succeeded": v.clicks,
            "success_rate": round(v.clicks / v.impressions * 100, 2) if v.impressions else None,
            "headline": v.headline[:120],
        }

    return {
        "agent": "experiment",
        "experiment_id": f"upworthy-{test_id}",
        "source": SOURCE_LABEL,
        "aggregate_only": True,
        "arms": k,
        "control": arm(control),
        "treatment": arm(t),
        "assignments": {n: a.impressions for n, a in zip(names, arms)},
        "srm": {k2: (round(v, 4) if isinstance(v, float) else v)
                for k2, v in srm.items() if k2 not in ("counts", "expected_ratio")},
        "effect": {
            "delta_pp": round(ztest["delta"] * 100, 2),
            "relative_uplift_pct": round((ztest["p_b"] / ztest["p_a"] - 1) * 100, 2)
            if ztest["p_a"] else None,
            "z": round(ztest["z"], 3),
            "p_value": f"{corrected_p:.3g}",
            "p_value_uncorrected": f"{ztest['p_value']:.3g}",
            "bonferroni_comparisons": len(treatments),
            "significant": bool(corrected_p < MIN_P_VALUE),
            "mde_pp": round(mde * 100, 2),
        },
        "guardrails": "not available in aggregate archive data",
        "decision": decision,
        "decision_reasons": reasons,
        "provenance": {
            "templates": "source CSV (real aggregate counts)",
            "statistics": "two-proportion z-test, Bonferroni-corrected, "
                          "Wilson CI, general-df chi-square SRM",
        },
    }


def analyze_archive(path, top_n: int = 3) -> dict:
    """Validate, group and read out the real archive; returns summary + top tests.

    Selection: tests whose SRM holds with at least one arm significantly
    different from control at Bonferroni-corrected alpha, ranked by absolute
    real effect size, tie-broken by total impressions.
    """
    rows = _validate_rows(path)
    by_test: dict[str, list[UpworthyRow]] = defaultdict(list)
    for r in rows:
        by_test[r.clickability_test_id].append(r)

    evaluable = {t: arms for t, arms in by_test.items()
                 if len(arms) >= 2 and all(a.impressions > 0 for a in arms)}
    significant = 0
    srm_failures = 0
    candidates = []
    for test_id, arms in evaluable.items():
        rx = _readout(test_id, arms)
        if not rx["srm"]["ok"]:
            srm_failures += 1
            continue
        if rx["effect"]["significant"]:
            significant += 1
            candidates.append((abs(rx["effect"]["delta_pp"]),
                               sum(a.impressions for a in arms), rx))
    candidates.sort(key=lambda c: (c[0], c[1]), reverse=True)

    return {
        "source": SOURCE_LABEL,
        "packages": len(rows),
        "tests_total": len(by_test),
        "tests_evaluated": len(evaluable),
        "tests_srm_ok_significant": significant,
        "tests_srm_failures": srm_failures,
        "selected": [c[2] for c in candidates[:top_n]],
    }
