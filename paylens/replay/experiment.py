"""Registered fixed-horizon experiment readouts at the randomized-user grain."""
from __future__ import annotations

from datetime import datetime, timedelta

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from paylens.replay.incident import query, utc
from paylens.stats.ab_test import difference_interval, srm_check, two_proportion_ztest
from paylens.tools.governed_sql import GovernedSqlTool


class ExperimentProtocol(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    experiment_id: str = Field(min_length=1, max_length=64, pattern=r"^[a-zA-Z0-9_-]+$")
    enrollment_start: AwareDatetime
    enrollment_end: AwareDatetime
    outcome_hours: int = Field(ge=1, le=168)
    treatment_share: float = Field(ge=0.05, le=0.95)
    min_users_per_arm: int = Field(ge=100, le=100_000_000)
    min_effect_pp: float = Field(gt=0, le=50)
    timeout_margin_pp: float = Field(gt=0, le=20)
    duplicate_charge_margin_pp: float = Field(gt=0, le=10)
    refund_margin_pp: float = Field(gt=0, le=20)

    @model_validator(mode="after")
    def valid_enrollment(self):
        if not timedelta(0) < self.enrollment_end - self.enrollment_start <= timedelta(days=90):
            raise ValueError("Enrollment must span more than zero and at most 90 days")
        return self


ARM_FIELDS = ("assigned", "mature", "attempted", "terminal", "converted", "timeout", "duplicate_charge", "refund")
ALPHA = 0.05


def build_experiment(sql: GovernedSqlTool, protocol: ExperimentProtocol, *, registered_at: datetime,
                     observed_through: datetime, sources_complete: bool) -> dict:
    observed_through = utc(observed_through)
    final_after = utc(protocol.enrollment_end) + timedelta(hours=protocol.outcome_hours)
    rows = query(sql, "experiment_itt_v1", {
        "experiment_id": protocol.experiment_id, "enrollment_start": protocol.enrollment_start,
        "enrollment_end": protocol.enrollment_end, "observed_through": observed_through,
        "outcome_seconds": 3600 * protocol.outcome_hours,
    })
    conflicts = query(sql, "assignment_conflicts_v1", {"experiment_id": protocol.experiment_id})[0]["conflicts"]
    arms = {r["variant"]: {field: r[field] for field in ARM_FIELDS} for r in rows}
    for variant in ("A", "B"):
        arms.setdefault(variant, dict.fromkeys(ARM_FIELDS, 0))

    counts = {label: arm["assigned"] for label, arm in arms.items()}
    expected = {"A": 1 - protocol.treatment_share, "B": protocol.treatment_share}
    srm = srm_check(counts, expected)
    both_observed = min(counts["A"], counts["B"]) > 0
    srm_evaluable = min(expected.values()) * sum(counts.values()) >= 5
    gates = [
        {"name": "Preregistered protocol", "state": "pass" if utc(registered_at) <= protocol.enrollment_start else "fail"},
        {"name": "Outcome window closed", "state": "pass" if observed_through >= final_after else "pending"},
        {"name": "Source completeness attested", "state": "pass" if sources_complete else "pending"},
        {"name": "Stable user assignments", "state": "fail" if conflicts else "pass"},
        {"name": "Sample-ratio mismatch", "state": ("fail" if srm.get("reason", "").startswith("arms observed")
                                                    or (srm_evaluable and not srm["ok"]) else
                                                    "pass" if srm_evaluable and both_observed else "pending")},
        {"name": "Minimum assigned users per arm",
         "state": "pass" if min(counts["A"], counts["B"]) >= protocol.min_users_per_arm else "pending"},
    ]
    effect = None
    guardrails = []
    if any(gate["state"] == "fail" for gate in gates):
        verdict = "invalid"
    elif any(gate["state"] == "pending" for gate in gates):
        verdict = "inconclusive"
    else:
        a, b = arms["A"], arms["B"]
        stats = two_proportion_ztest(a["converted"], a["mature"], b["converted"], b["mature"])
        low, high = difference_interval(a["converted"], a["mature"], b["converted"], b["mature"])
        effect = {
            "delta_pp": 100 * stats["delta"], "ci95_pp": [100 * low, 100 * high],
            "p_value": stats["p_value"], "minimum_effect_pp": protocol.min_effect_pp,
            "control_pct": 100 * stats["p_a"], "treatment_pct": 100 * stats["p_b"],
        }
        gates.append({"name": "Effect clears minimum meaningful lift",
                      "state": "pass" if 100 * low > protocol.min_effect_pp else "fail" if high < 0 else "pending"})
        margins = {
            "timeout": protocol.timeout_margin_pp,
            "duplicate_charge": protocol.duplicate_charge_margin_pp,
            "refund": protocol.refund_margin_pp,
        }
        for name, limit in margins.items():
            low_g, high_g = difference_interval(a[name], a["mature"], b[name], b["mature"],
                                                alpha=ALPHA / len(margins))
            state = "pass" if 100 * high_g <= limit else "fail" if 100 * low_g > limit else "pending"
            guardrails.append({
                "name": name, "state": state, "margin_pp": limit,
                "ci_pp": [100 * low_g, 100 * high_g],
                "control_events": a[name], "treatment_events": b[name],
            })
        states = {gate["state"] for gate in gates + guardrails}
        verdict = "hold" if "fail" in states else "inconclusive" if "pending" in states else "review_candidate"

    return {
        "metric": {"id": "user_conversion_within_assignment_horizon", "version": 1,
                   "denominator": "All eligible assigned users after their outcome window closes"},
        "protocol": protocol.model_dump(mode="json"), "registered_at": utc(registered_at),
        "observed_through": observed_through, "final_after": final_after, "arms": arms,
        "gates": gates, "assignment_conflicts": conflicts, "srm": srm,
        "effect": effect, "guardrails": guardrails, "verdict": verdict,
        "method": "One fixed horizon; Newcombe risk-difference intervals; Bonferroni across three guardrails.",
    }
