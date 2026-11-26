"""LangGraph supervisor - the stateful investigation workflow.

Graph topology:

    START -> plan -> funnel -[material drop?]-> segment -[affected?]-> anomaly
                        \\                            \\
                         \\--> experiment? --> report --> END
                          \\--> report

* conditional edges encode the dependency rules (the Segment Agent only
  investigates after the Funnel Agent found a material drop; the Experiment
  Agent returns "inconclusive" instead of ever recommending without
  sufficient evidence);
* every node runs against a fresh governed-SQL session and has a retry
  budget (2) with errors accumulated in shared state;
* a MemorySaver checkpointer stores each run's state so an investigation
  can be inspected/resumed by ``run_id`` (swap for PostgresSaver in prod).

The LLM plans and words; SQL and Python decide every number.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select

from paylens.agents.anomaly import run_anomaly
from paylens.agents.experiment import run_experiment
from paylens.agents.funnel import run_funnel
from paylens.agents.segment import run_segment
from paylens.agents.state import InvestigationState
from paylens.agents.llm import LLMClient
from paylens.reporting.report import build_report
from paylens.storage.db import session_scope
from paylens.storage.models import Investigation, PaymentAttempt
from paylens.tools.governed_sql import GovernedSqlTool

MAX_RETRIES = 2


def data_horizon(session) -> datetime:
    """Latest event time present in the store (ceil to the next hour)."""
    latest = session.execute(select(func.max(PaymentAttempt.last_event_at))).scalar()
    if latest is None:
        latest = datetime.now(timezone.utc)
    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=timezone.utc)
    return (latest + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)


class PayLensSupervisor:
    def __init__(self, llm: LLMClient | None = None, *, sessions=session_scope) -> None:
        from langgraph.checkpoint.memory import MemorySaver
        from langgraph.graph import END, START, StateGraph

        self.llm = llm or LLMClient()
        self.sessions = sessions
        builder = StateGraph(InvestigationState)
        builder.add_node("plan", self._plan_node)
        builder.add_node("funnel", self._agent_node("funnel", run_funnel))
        builder.add_node("segment", self._agent_node("segment", run_segment))
        builder.add_node("anomaly", self._agent_node("anomaly", run_anomaly))
        builder.add_node("experiment", self._agent_node("experiment", run_experiment))
        builder.add_node("report", self._report_node)

        builder.add_edge(START, "plan")
        builder.add_conditional_edges("plan", self._route_plan,
                                      {"funnel": "funnel", "report": "report"})
        builder.add_conditional_edges("funnel", self._route_after_funnel,
                                      {"funnel": "funnel", "segment": "segment", "report": "report"})
        builder.add_conditional_edges("segment", self._route_after_segment,
                                      {"segment": "segment", "anomaly": "anomaly",
                                       "experiment": "experiment", "report": "report"})
        builder.add_conditional_edges("anomaly", self._route_after_anomaly,
                                      {"anomaly": "anomaly", "experiment": "experiment",
                                       "report": "report"})
        builder.add_edge("experiment", "report")
        builder.add_edge("report", END)
        self.graph = builder.compile(checkpointer=MemorySaver())

    # ------------------------------------------------------------------ api
    def run(self, question: str, metric_version: int = 2,
            config: dict | None = None) -> dict:
        """Run one investigation. ``config`` (optional) pins the window and
        grain for external data sources instead of trusting the LLM plan:
        {window_hours, window_end, grain, focus_method, run_experiment,
        data_source}. Synthetic-path callers omit it."""
        run_id = str(uuid.uuid4())
        with self.sessions() as session:
            session.add(Investigation(run_id=run_id, question=question,
                                      metric_version=metric_version, status="running"))
            session.commit()
        state: InvestigationState = {
            "run_id": run_id,
            "question": question,
            "metric_version": metric_version,
            "config": dict(config) if config else {},
            "retries": {},
            "errors": [],
        }
        final = self.graph.invoke(state, config={"configurable": {"thread_id": run_id}})
        with self.sessions() as session:
            inv = session.get(Investigation, run_id)
            inv.report = final.get("report", {})
            inv.status = "completed"
            inv.finished_at = datetime.now(timezone.utc)
            if final.get("window_start"):
                inv.window_start = final["window_start"]
                inv.window_end = final["window_end"]
            session.commit()
        return {"run_id": run_id, "report": final.get("report", {})}

    # ----------------------------------------------------------------- nodes
    def _plan_node(self, state: InvestigationState) -> dict:
        config = state.get("config") or {}
        if config.get("window_hours"):
            # config-driven plan: external data owns the window, the grain
            # and the experiment routing (LLM still words the report).
            window_end = config.get("window_end")
            if window_end is None:
                with self.sessions() as session:
                    window_end = data_horizon(session)
            window_start = window_end - timedelta(hours=config["window_hours"])
            baseline_start = window_start - timedelta(hours=config["window_hours"])
            plan = {
                "window_hours": config["window_hours"],
                "run_funnel": True,
                "run_experiment": bool(config.get("run_experiment", False)),
                "focus_method": config.get("focus_method"),
                "planner": "config-driven",
                "rationale": (f"window pinned by data source config "
                              f"({config.get('data_source', 'external')})"),
            }
            return {
                "plan": plan,
                "window_start": window_start,
                "window_end": window_end,
                "baseline_start": baseline_start,
                "grain": config.get("grain", "hour"),
                "real_experiments": config.get("real_experiments", []),
            }
        with self.sessions() as session:
            horizon = data_horizon(session)
        plan = self.llm.plan(state["question"], horizon.isoformat())
        window_end = horizon
        window_start = window_end - timedelta(hours=plan["window_hours"])
        baseline_start = window_start - timedelta(hours=plan["window_hours"])
        return {
            "plan": plan,
            "window_start": window_start,
            "window_end": window_end,
            "baseline_start": baseline_start,
            "grain": "hour",
        }

    # LangGraph node name -> state channel (they differ for 'segment')
    STATE_KEY = {"funnel": "funnel", "segment": "segments",
                 "anomaly": "anomaly", "experiment": "experiment"}

    def _agent_node(self, name: str, fn):
        state_key = self.STATE_KEY[name]

        def node(state: InvestigationState) -> dict:
            try:
                with self.sessions() as session:
                    sql = GovernedSqlTool(session, state["run_id"], agent=name)
                    return {state_key: fn(state, sql)}
            except Exception as exc:  # retry with the same inputs
                retries = dict(state.get("retries", {}))
                retries[name] = retries.get(name, 0) + 1
                return {
                    "retries": retries,
                    "errors": [{"agent": name, "error": f"{type(exc).__name__}: {exc}"}],
                }
        return node

    def _report_node(self, state: InvestigationState) -> dict:
        with self.sessions() as session:
            sql = GovernedSqlTool(session, state["run_id"], agent="report")
            report = build_report(state, self.llm, session, sql)
        return {"report": report}

    # ---------------------------------------------------------------- routing
    def _route_plan(self, state: InvestigationState) -> str:
        return "funnel" if state.get("plan", {}).get("run_funnel", True) else "report"

    def _route_after_funnel(self, state: InvestigationState) -> str:
        if not state.get("funnel"):
            retries = state.get("retries", {}).get("funnel", 0)
            return "funnel" if retries <= MAX_RETRIES else "report"
        return "segment" if state["funnel"].get("material_drop") else "report"

    def _route_after_segment(self, state: InvestigationState) -> str:
        if not state.get("segments"):
            retries = state.get("retries", {}).get("segment", 0)
            if retries <= MAX_RETRIES:
                return "segment"
            return "experiment" if self._experiment_planned(state) else "report"
        if state["segments"].get("affected_segments"):
            return "anomaly"
        return "experiment" if self._experiment_planned(state) else "report"

    def _route_after_anomaly(self, state: InvestigationState) -> str:
        if not state.get("anomaly"):
            retries = state.get("retries", {}).get("anomaly", 0)
            if retries <= MAX_RETRIES:
                return "anomaly"
            return "experiment" if self._experiment_planned(state) else "report"
        return "experiment" if self._experiment_planned(state) else "report"

    def _experiment_planned(self, state: InvestigationState) -> bool:
        return bool(state.get("plan", {}).get("run_experiment", False))


def build_supervisor(llm: LLMClient | None = None) -> PayLensSupervisor:
    return PayLensSupervisor(llm)
