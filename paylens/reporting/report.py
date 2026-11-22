"""Investigation report assembly.

Combines agent outputs into the structured report the bank receives:
finding, evidence with numbers, affected segments, change-point analysis,
experiment readout, recommended actions, and full provenance (which governed
queries, which metric version, which data window) so every claim can be
re-executed and verified.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import select

from paylens.agents.llm import LLMClient
from paylens.agents.state import InvestigationState
from paylens.storage.models import QueryAuditLog


def _fmt_pp(x: float | None) -> str:
    return "n/a" if x is None else f"{x:+.2f} pp"


def build_report(state: InvestigationState, llm: LLMClient, session, sql) -> dict:
    funnel = state.get("funnel")
    segments = state.get("segments")
    anomaly = state.get("anomaly")
    experiment = state.get("experiment")
    errors = state.get("errors", [])

    overall = (funnel or {}).get("overall", {})
    affected = (segments or {}).get("affected_segments") or []
    # only volume-gated segments may be named as "affected" in the report
    worst = affected[0] if affected else {}
    change_point = (anomaly or {}).get("change_point") or {}
    correlated = (anomaly or {}).get("correlated_deployment") or {}
    exp_decision = (experiment or {}).get("decision")
    exp_effect = (experiment or {}).get("effect", {})
    real_experiments = state.get("real_experiments") or []

    # ------------------------------------------------------------------ facts
    facts: dict[str, Any] = {
        "success_rate_current": overall.get("success_rate_current"),
        "success_rate_baseline": overall.get("success_rate_baseline"),
        "delta_pp": overall.get("delta_pp"),
        "worst_segment_delta_pp": worst.get("delta_pp"),
        "worst_segment_method": worst.get("method"),
        "worst_segment_gateway": worst.get("gateway"),
        "change_point_hour": change_point.get("hour"),
        "experiment_decision": exp_decision,
        "experiment_delta_pp": exp_effect.get("delta_pp"),
    }

    finding = _finding_sentence(funnel, worst, correlated, llm, facts)

    evidence: list[str] = []
    if funnel:
        evidence.append(
            f"Overall success rate moved from {overall.get('success_rate_baseline')}% "
            f"(baseline) to {overall.get('success_rate_current')}% in the current window "
            f"({_fmt_pp(overall.get('delta_pp'))}), computed on "
            f"{overall.get('settled_current'):,} settled attempts "
            f"({overall.get('pending_current'):,} still pending)."
        )
    if funnel and funnel.get("biggest_stage_drop"):
        b = funnel["biggest_stage_drop"]
        evidence.append(
            f"Largest funnel-stage conversion change: '{b['stage']}' "
            f"{b['conversion_baseline']}% -> {b['conversion_current']}% "
            f"({_fmt_pp(b['delta_pp'])})."
        )
    if affected:
        seg_lines = []
        for seg in affected[:3]:
            seg_lines.append(
                f"{seg['method']} via {seg['gateway']}: {seg['success_rate_baseline']}% -> "
                f"{seg['success_rate_current']}% ({_fmt_pp(seg['delta_pp'])}, "
                f"{seg['settled_current']:,} attempts)"
            )
        evidence.append("Decline concentrated in: " + "; ".join(seg_lines) + ".")
    if anomaly and anomaly.get("change_point"):
        cp = anomaly["change_point"]
        bucket_label = cp.get("label") or f"{cp['date']} {cp['hour']}"
        line = (
            f"Change point detected at {bucket_label} for "
            f"{anomaly['segment']['method']} via {anomaly['segment']['gateway']}: "
            f"{cp['baseline_same_hour']}% -> {cp['success_rate']}% "
            f"(z = {cp['z']}, {anomaly.get('grain', 'hour')} grain)."
        )
        if correlated:
            relation = correlated.get("relation", "preceded the change point")
            line += (
                f" Deployment {correlated['change_id']} on {correlated['service']} "
                f"({correlated['gateway'] or 'shared service'}) {relation} "
                f"(deployed {correlated['deployed_at']}) - timing evidence, "
                "not proven causation."
            )
        evidence.append(line)
    if experiment and not experiment.get("skipped"):
        c, t = experiment["control"], experiment["treatment"]
        evidence.append(
            f"A/B readout ({experiment['experiment_id']}): {c['variant']} "
            f"{c['success_rate']}% vs {t['variant']} {t['success_rate']}% "
            f"({_fmt_pp(exp_effect.get('delta_pp'))}, p = {exp_effect.get('p_value')}, "
            f"SRM p = {experiment['srm'].get('p_value')}). Decision: {exp_decision}."
        )
    if real_experiments:
        src = real_experiments[0].get("source", "external archive")
        lines = []
        for rx in real_experiments[:3]:
            eff = rx["effect"]
            lines.append(
                f"{rx['experiment_id']}: control {rx['control']['success_rate']}% vs "
                f"treatment {rx['treatment']['success_rate']}% "
                f"({_fmt_pp(eff.get('delta_pp'))}, p = {eff.get('p_value')}, "
                f"SRM p = {rx['srm'].get('p_value')}) -> {rx['decision']}"
            )
        evidence.append(
            f"Real aggregate A/B readouts ({src}, arm units are the source's "
            f"own impressions): " + " | ".join(lines) + "."
        )

    actions = _recommended_actions(anomaly, experiment, funnel)

    audit_rows = session.execute(
        select(QueryAuditLog).where(QueryAuditLog.run_id == state["run_id"])
    ).scalars().all()
    provenance = {
        "run_id": state["run_id"],
        "metric_version": state.get("metric_version", 2),
        "grain": (state.get("config") or {}).get("grain", "hour"),
        "data_source": (state.get("config") or {}).get("data_source"),
        "window": {
            "start": state["window_start"].isoformat(),
            "end": state["window_end"].isoformat(),
        },
        "baseline": {
            "start": state["baseline_start"].isoformat(),
            "end": state["window_start"].isoformat(),
        },
        "governed_queries": [
            {"agent": q.agent, "template": q.template, "rows": q.row_count,
             "duration_ms": q.duration_ms, "sql": q.sql_text}
            for q in audit_rows
        ],
        "agent_errors": errors,
        "planner": (state.get("plan") or {}).get("planner", "deterministic"),
        "plan_rationale": (state.get("plan") or {}).get("rationale"),
    }

    return {
        "title": "Payment Performance Investigation",
        "finding": finding,
        "evidence": evidence,
        "affected_segments": affected,
        "funnel": funnel,
        "segment_rankings": (segments or {}).get("dimension_rankings"),
        "anomaly": anomaly,
        "experiment": experiment,
        "recommended_actions": actions,
        "provenance": provenance,
        "real_experiments": real_experiments or None,
    }


def _finding_sentence(funnel, worst, correlated, llm: LLMClient, facts: dict) -> str:
    fallback = "No material change detected in the requested window."
    if funnel and funnel.get("material_drop"):
        ov = funnel["overall"]
        if worst.get("method"):
            # concentration is only ever claimed from the volume-gated
            # affected list - never from the raw slice ranking
            seg_txt = (
                f" The decline is concentrated in {worst['method']} transactions "
                f"routed through {worst['gateway']} ({_fmt_pp(worst.get('delta_pp'))} for that slice)."
            )
        else:
            seg_txt = (
                " The drop is not concentrated in any segment above the "
                "configured volume and delta thresholds; treat it as "
                "platform-wide pending verification."
            )
        dep_txt = ""
        if correlated:
            dep_txt = (
                f" A deployment on {correlated['service']} ({correlated['change_id']}) "
                f"fell {correlated.get('relation', 'before the change point')}."
            )
        fallback = (
            f"Overall payment success rate decreased from {ov.get('success_rate_baseline')}% "
            f"to {ov.get('success_rate_current')}% in the current window."
            + seg_txt + dep_txt
            + " Review required before any mitigation; the timing evidence does not by itself prove causation."
        )
    narrated = llm.narrate("finding", facts, fallback)
    return narrated or fallback


def _recommended_actions(anomaly, experiment, funnel) -> list[str]:
    actions: list[str] = []
    correlated = (anomaly or {}).get("correlated_deployment")
    if correlated:
        actions.append(
            f"Verify {correlated['change_id']} on {correlated['service']}: inspect "
            f"{correlated['gateway'] or 'service'} timeout logs around the change point "
            "and the deployment diff before and after."
        )
        actions.append(
            "Assess whether traffic can be safely rerouted away from the degraded "
            "gateway, then monitor success rate and latency for at least one full "
            "traffic cycle after mitigation."
        )
    elif anomaly and anomaly.get("change_point"):
        actions.append(
            "Pull gateway and application logs around the detected change point "
            "and validate capacity/error budgets for the affected slice."
        )
    if experiment and not experiment.get("skipped"):
        decision = experiment.get("decision", "")
        if decision.startswith("ship"):
            actions.append(
                "Proceed to a staged rollout of the treatment variant with human "
                "approval; keep guardrail monitoring active during ramp-up."
            )
        elif decision.startswith("hold"):
            actions.append(
                "Keep the current experience; address the breached guardrails and "
                "re-run the experiment before any rollout."
            )
        elif decision.startswith("inconclusive"):
            actions.append(
                "Extend the experiment to reach the pre-registered sample size, or "
                "re-evaluate the metric definition; do not ship on this readout."
            )
        elif decision.startswith("invalid"):
            actions.append(
                "Fix the assignment mechanism and re-run with verified randomization; "
                "the current readout is unusable."
            )
    if funnel and funnel.get("overall", {}).get("pending_current"):
        actions.append(
            f"Re-run this investigation after the {funnel['overall']['pending_current']:,} "
            "pending attempts settle; late settlement can shift the exact rates."
        )
    if not actions:
        actions.append("No action required: metrics are within normal ranges for the window.")
    return actions
