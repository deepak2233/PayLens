"""Agent + supervisor behaviour on the seeded store."""
from __future__ import annotations

import pytest

from paylens.agents.llm import LLMClient, deterministic_plan
from paylens.agents.supervisor import build_supervisor
from paylens.tools.governed_sql import GovernedSqlError, GovernedSqlTool

QUESTION = ("Investigate the UPI success-rate drop over the last 24 hours. "
            "Compare with yesterday and identify affected segments.")


@pytest.fixture(scope="module")
def investigation(store):
    supervisor = build_supervisor(LLMClient("none"))
    return supervisor.run(QUESTION)


def test_governed_sql_rejects_unknown_template_and_bad_params(session):
    sql = GovernedSqlTool(session, run_id="test-gov", agent="pytest")
    with pytest.raises(GovernedSqlError):
        sql.run("not_a_template", {})
    with pytest.raises(GovernedSqlError):
        sql.run("funnel_counts", {"window_start": "yesterday",
                                  "window_end": None, "method": None})
    with pytest.raises(GovernedSqlError):
        sql.run("success_rate_by_dimension",
                {"window_start": "2026-11-20T00:00:00", "window_end": "2026-11-20T01:00:00",
                 "baseline_start": "2026-11-19T00:00:00", "method": None},
                dimension="credit_card_number")  # not in the approved list


def test_deterministic_plan_parses_method_and_window():
    plan = deterministic_plan("Investigate the UPI success-rate drop over the last 24 hours", None)
    assert plan["focus_method"] == "upi"
    assert plan["window_hours"] == 24
    assert plan["run_funnel"] and plan["run_experiment"]
    plan2 = deterministic_plan("check debit cards over last 48 hours", None)
    assert plan2["focus_method"] == "debit_card"
    assert plan2["window_hours"] == 48


def test_finding_reports_drop_and_concentration(investigation):
    report = investigation["report"]
    assert "decreased" in report["finding"].lower()
    assert "gateway-b" in report["finding"]
    funnel = report["funnel"]
    assert funnel["material_drop"] is True
    assert funnel["overall"]["delta_pp"] <= -5


def test_segment_agent_ranks_gateway_b_first(investigation):
    report = investigation["report"]
    affected = report["affected_segments"]
    assert affected, "expected at least one affected segment"
    top = affected[0]
    assert top["method"] == "upi"
    assert top["gateway"] == "gateway-b"
    assert top["delta_pp"] <= -10
    rankings = report["segment_rankings"]
    assert rankings["gateway"][0]["value"] == "gateway-b"


def test_anomaly_detects_incident_hour_and_deployment(investigation):
    report = investigation["report"]
    anomaly = report["anomaly"]
    cp = anomaly.get("change_point")
    assert cp is not None, "change point must be detected"
    assert cp["hour"] in {"10:00", "11:00"}  # incident starts 10:15
    dep = anomaly.get("correlated_deployment")
    assert dep is not None
    assert dep["change_id"] == "CHG-8841"
    assert anomaly["interpretation"].startswith("Timing evidence")


def test_experiment_readout_is_significant_and_exploratory(investigation):
    report = investigation["report"]
    exp = report["experiment"]
    assert exp["srm"]["ok"] is True
    assert exp["effect"]["significant"] is True
    assert exp["decision"].startswith("exploratory")
    assert exp["control"]["success_rate"] < exp["treatment"]["success_rate"]
    for g in exp["guardrail_checks"]:
        assert g["ok"], f"guardrail {g['name']} unexpectedly breached"


def test_report_has_provenance_and_actions(investigation):
    report = investigation["report"]
    prov = report["provenance"]
    assert prov["metric_version"] == 2
    assert len(prov["governed_queries"]) >= 10
    templates = {q["template"] for q in prov["governed_queries"]}
    assert "funnel_counts" in templates
    assert "experiment_outcomes" in templates
    assert report["recommended_actions"]
    assert any("CHG-8841" in a for a in report["recommended_actions"])
    assert report["title"] == "Payment Performance Investigation"


def test_agent_errors_channel_stays_clean(investigation):
    # the seeded happy path should produce no agent errors
    assert investigation["report"]["provenance"]["agent_errors"] == []
