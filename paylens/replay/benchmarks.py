"""Readouts from compact, attributed public data snapshots."""
from __future__ import annotations

import json
from functools import cache
from pathlib import Path

from paylens.stats.ab_test import srm_check, two_proportion_ztest

SNAPSHOT = Path(__file__).parent / "data" / "benchmarks.json"


def archive_readout(arms: list[list[int]]) -> dict:
    expected = {str(i): 1.0 / len(arms) for i in range(len(arms))}
    actual = {str(i): arm[0] for i, arm in enumerate(arms)}
    srm = srm_check(actual, expected)
    n, successes = arms[0]
    comparisons = [two_proportion_ztest(successes, n, y, m) for m, y in arms[1:]]
    result = min(comparisons, key=lambda value: value["p_value"])
    return {"delta_pp": 100 * result["delta"],
            "adjusted_p": min(1.0, result["p_value"] * len(comparisons)),
            "srm_ok": srm["ok"]}


@cache
def public_benchmarks():
    data = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    for sample in data["upworthy"]["samples"]:
        sample.update(archive_readout(sample["arms"]))
    return data
