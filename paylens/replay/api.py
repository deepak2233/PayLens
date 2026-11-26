"""Public replay and authenticated live analysis share the same calculation code."""
from __future__ import annotations

import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import func, select

from paylens.api.access import require_access, require_live
from paylens.replay.demo import ANCHOR, EXPERIMENT_ID, SCENARIOS, STREAMS, demo_engine
from paylens.replay.experiment import ExperimentProtocol, build_experiment
from paylens.replay.incident import build_incident, utc
from paylens.storage.db import analytics_scope, session_scope
from paylens.storage.models import DataWatermark, ExperimentProtocolRecord, Investigation, PaymentEventRecord
from paylens.tools.governed_sql import GovernedSqlError, GovernedSqlTool, write_evidence

router = APIRouter(prefix="/api/v2")
live = APIRouter(prefix="/api/v2/live", dependencies=[Depends(require_access), Depends(require_live)])
Scenario = Literal["incident", "mix", "delay"]


class ReplayScope(BaseModel):
    window_hours: int = Field(default=24, ge=6, le=48)
    outcome_minutes: int = Field(default=30, ge=1, le=1440)
    method: str | None = Field(default=None, min_length=1, max_length=32)


def _read_horizon(session, streams):
    marks = {row.stream: utc(row.complete_through) for row in session.scalars(
        select(DataWatermark).where(DataWatermark.stream.in_(streams), DataWatermark.valid)
    )}
    if len(marks) == len(streams):
        return min(marks.values()), True
    now = datetime.now(timezone.utc)
    latest = session.scalar(select(func.max(PaymentEventRecord.ts)).where(PaymentEventRecord.ts <= now))
    if latest is None:
        raise HTTPException(409, "The live store has no observed payment events")
    return utc(latest), False


def _response(run_id: str, source: dict, report: dict, sql: GovernedSqlTool, started: float):
    return {
        "run_id": run_id, "source": source, "report": report,
        "evidence": sql.evidence, "duration_ms": round((time.perf_counter() - started) * 1000, 1),
    }


def _incident(scope: ReplayScope, scenario: str | None, back_minutes: int = 0) -> dict:
    started = time.perf_counter()
    run_id = str(uuid.uuid4())
    with analytics_scope(demo_engine(scenario) if scenario else None) as session:
        if scenario:
            observed, complete = ANCHOR - timedelta(minutes=back_minutes), True
        else:
            observed, complete = _read_horizon(session, ("payments",))
        sql = GovernedSqlTool(session, run_id, "incident_replay", record_audit=False)
        report = build_incident(sql, observed_through=observed, **scope.model_dump())
    source = {"id": scenario or "live", "type": "synthetic" if scenario else "private_stream",
              "title": SCENARIOS[scenario] if scenario else "Live payment events",
              "coverage": "fixture" if scenario else "attested" if complete else "latest_event_only"}
    return _response(run_id, source, report, sql, started)


def _experiment(experiment_id: str, public: bool, early: bool = False) -> dict:
    run_id = str(uuid.uuid4())
    started = time.perf_counter()
    with analytics_scope(demo_engine("experiment") if public else None) as session:
        row = session.get(ExperimentProtocolRecord, experiment_id)
        if row is None:
            raise HTTPException(404, "No registered protocol for this experiment")
        spec = ExperimentProtocol.model_validate(row.policy)
        if public:
            observed, complete = (utc(spec.enrollment_end) if early else ANCHOR), True
        else:
            observed, complete = _read_horizon(session, STREAMS)
        sql = GovernedSqlTool(session, run_id, "experiment_readout", record_audit=False)
        report = build_experiment(sql, spec, registered_at=row.registered_at,
                                  observed_through=observed, sources_complete=complete)
    source = {"id": experiment_id, "type": "synthetic" if public else "private_stream",
              "title": "Checkout assignment trial" if public else experiment_id,
              "coverage": "fixture" if public else "attested" if complete else "latest_event_only"}
    return _response(run_id, source, report, sql, started)


def _store(payload: dict, agent: str) -> None:
    from fastapi.encoders import jsonable_encoder

    stamp = datetime.now(timezone.utc)
    with session_scope() as session:
        session.add(Investigation(
            run_id=payload["run_id"], question=agent, status="completed", metric_version=1,
            report=jsonable_encoder(payload), finished_at=stamp,
        ))
        write_evidence(session, payload["run_id"], agent, payload["evidence"])


@router.get("/health")
def public_health():
    return {"status": "ok", "service": "paylens", "api_version": 2}


@router.get("/replay")
def replay(scenario: Scenario = "incident", window_hours: int = Query(24, ge=6, le=48),
           outcome_minutes: int = Query(30, ge=1, le=1440),
           method: Literal["upi", "card"] | None = None,
           back_minutes: int = Query(0, ge=0, le=720)):
    if back_minutes not in (0, 180, 360, 720):
        raise HTTPException(422, "Choose a supported replay clock")
    try:
        return _incident(ReplayScope(window_hours=window_hours, outcome_minutes=outcome_minutes,
                                     method=method), scenario, back_minutes)
    except GovernedSqlError as exc:
        raise HTTPException(422, str(exc)) from exc


@router.get("/experiments/checkout")
def checkout(stage: Literal["final", "early"] = "final"):
    return _experiment(EXPERIMENT_ID, public=True, early=stage == "early")


@router.get("/benchmarks")
def benchmarks():
    from paylens.replay.benchmarks import public_benchmarks

    return public_benchmarks()


@live.post("/replay")
def live_replay(scope: ReplayScope):
    try:
        result = _incident(scope, None)
    except GovernedSqlError as exc:
        raise HTTPException(422, str(exc)) from exc
    _store(result, "incident_replay")
    return result


@live.post("/experiments/{experiment_id}")
def live_experiment(experiment_id: str):
    try:
        result = _experiment(experiment_id, public=False)
    except GovernedSqlError as exc:
        raise HTTPException(422, str(exc)) from exc
    _store(result, "experiment_readout")
    return result


@live.get("/reports/{run_id}")
def live_report(run_id: uuid.UUID):
    with session_scope() as session:
        row = session.get(Investigation, str(run_id))
        if not row or row.question not in {"incident_replay", "experiment_readout"}:
            raise HTTPException(404, "Report not found")
        return row.report
