"""FastAPI application - the analytics + dashboard surface.

Demo layer runs entirely on SQLite; the same app runs unchanged against
PostgreSQL in the Docker layer (only PAYLENS_DATABASE_URL differs).

Endpoints:
  GET  /                              -> minimal responsive dashboard
  GET  /api/health                    -> liveness + data horizon
  POST /api/demo/seed                 -> generate + ingest the demo dataset
  POST /api/investigations            -> run the agent investigation
  POST /api/uploads                   -> bring-your-own-data: CSV upload -> same governed analysis
  GET  /api/investigations            -> list investigations
  GET  /api/investigations/{run_id}   -> full report incl. provenance
  GET  /api/metrics/success-rate      -> versioned metric, optional grouping
  GET  /api/funnel                    -> funnel counts for a window
  GET  /api/experiments/{id}/readout  -> statistical readout (no agent run)
  GET  /api/audit/queries             -> governed-query audit log
"""
from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.exc import DBAPIError
from pydantic import BaseModel, Field
from sqlalchemy import func, select

from paylens import __version__
from paylens.agents.state import InvestigationState
from paylens.api.access import HOSTED, require_access
from paylens.agents.supervisor import data_horizon
from paylens.config import REPO_ROOT, SETTINGS
from paylens.metrics.definitions import SUCCESS_RATE_V2
from paylens.pipeline import run_investigation, seed_database
from paylens.storage.db import create_all, session_scope
from paylens.storage.models import (
    DeploymentRecordDB,
    Investigation,
    PaymentAttempt,
    QueryAuditLog,
)
from paylens.tools.governed_sql import GovernedSqlTool

_DASHBOARD = Path(__file__).parent / "dashboard.html"
_PUBLIC = REPO_ROOT / "public"


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    if not HOSTED and SETTINGS.is_sqlite:
        create_all()
    yield


app = FastAPI(
    title="PayLens API",
    version=__version__,
    description="Payments analytics & experimentation agents",
    lifespan=lifespan,
)


private = APIRouter(dependencies=[Depends(require_access)])


class InvestigateRequest(BaseModel):
    question: str = Field(
        default="Investigate the UPI success-rate drop over the last 24 hours. "
                "Compare with yesterday and identify affected segments.",
        min_length=5, max_length=500,
    )
    metric_version: int = Field(default=SUCCESS_RATE_V2, ge=2, le=2)


@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse(_PUBLIC / "index.html")


app.mount("/assets", StaticFiles(directory=_PUBLIC / "assets"), name="assets")


@private.get("/legacy", response_class=HTMLResponse)
def legacy_dashboard():
    return HTMLResponse(_DASHBOARD.read_text(encoding="utf-8"))


@app.exception_handler(DBAPIError)
async def database_unavailable(_request, _exc):
    return JSONResponse({"detail": "Analytics store unavailable or query exceeded its deadline"}, status_code=503)


@app.middleware("http")
async def response_headers(request, call_next):
    response = await call_next(request)
    if request.url.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


@app.get("/api/health")
def health() -> dict:
    if HOSTED or SETTINGS.is_postgres or SETTINGS.api_token:
        return {"status": "ok", "service": "paylens"}
    with session_scope() as session:
        horizon = data_horizon(session)
        attempts = session.execute(select(func.count()).select_from(PaymentAttempt)).scalar()
    return {
        "status": "ok",
        "event_bus": SETTINGS.event_bus,
        "llm_provider": SETTINGS.llm_provider,
        "attempts_stored": attempts,
        "data_horizon": horizon.isoformat(),
    }


@private.post("/api/demo/seed")
def demo_seed(
    attempts_per_day: int = Query(default=20_000, ge=500, le=200_000),
    experiment_users_per_arm: int = Query(default=10_000, ge=500, le=100_000),
) -> dict:
    if HOSTED or SETTINGS.is_postgres:
        raise HTTPException(403, "Seed is available only in the local SQLite demo")
    stats = seed_database(
        attempts_per_day=attempts_per_day,
        experiment_users_per_arm=experiment_users_per_arm,
    )
    return JSONResponse(stats)


@private.post("/api/investigations")
def create_investigation(req: InvestigateRequest) -> dict:
    result = run_investigation(req.question, metric_version=req.metric_version)
    return JSONResponse(result)


@private.post("/api/uploads")
def upload_csv(file: UploadFile) -> dict:
    """Bring-your-own-data: analyze an uploaded payments CSV export.

    One row per payment attempt (schema: see the web console's Analyze Data
    tab). The file streams straight through the canonical event pipeline -
    governed ingest, governed SQL, audited agents - so the report is the
    same artifact the demo and real-source paths produce. Store semantics
    match the demo: reproducible, never additive (each upload replaces the
    dataset). Hard size cap: 200 MB."""
    from paylens.realdata.csv_upload import CsvUploadError, run_csv_investigation

    max_bytes = 8 * 1024 * 1024
    if file.size is not None and file.size > max_bytes:
        raise HTTPException(413, "CSV exceeds the 8 MiB upload limit")
    total = 0

    def _stream():
        nonlocal total
        for raw in file.file:
            total += len(raw)
            if total > max_bytes:
                raise HTTPException(status_code=413,
                                    detail="CSV exceeds the 8 MiB upload limit")
            yield raw.decode("utf-8-sig", errors="replace")

    try:
        result = run_csv_investigation(_stream())
    except CsvUploadError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return JSONResponse(result)


@private.get("/api/investigations")
def list_investigations(limit: int = Query(default=20, ge=1, le=100)) -> list[dict]:
    with session_scope() as session:
        rows = session.execute(
            select(Investigation).order_by(Investigation.created_at.desc()).limit(limit)
        ).scalars().all()
        return [
            {"run_id": r.run_id, "question": r.question, "status": r.status,
             "created_at": r.created_at.isoformat(),
             "finding": (r.report or {}).get("finding")}
            for r in rows
        ]


@private.get("/api/investigations/{run_id}")
def get_investigation(run_id: str) -> dict:
    with session_scope() as session:
        inv = session.get(Investigation, run_id)
        if inv is None:
            raise HTTPException(status_code=404, detail="investigation not found")
        return {
            "run_id": inv.run_id, "question": inv.question, "status": inv.status,
            "created_at": inv.created_at.isoformat(),
            "finished_at": inv.finished_at.isoformat() if inv.finished_at else None,
            "metric_version": inv.metric_version,
            "report": inv.report,
        }


@private.get("/api/metrics/success-rate")
def success_rate(
    window_hours: int = Query(default=24, ge=1, le=72),
    metric_version: int = Query(default=SUCCESS_RATE_V2, ge=2, le=2),
    group_by: Literal["method", "gateway", "geo", "customer_tier", "app_version"] | None = None,
) -> dict:
    with session_scope() as session:
        end = data_horizon(session)
        start = end - timedelta(hours=window_hours)
        sql = GovernedSqlTool(session, run_id=str(uuid.uuid4()), agent="api")
        if group_by:
            res = sql.run("success_rate_by_dimension",
                          {"window_start": start, "window_end": end,
                           "baseline_start": start - timedelta(hours=window_hours),
                           "method": None},
                          dimension=group_by)
            rows = []
            for r in res["rows"]:
                cur = r["cur_succeeded"] / r["cur_settled"] if r["cur_settled"] else None
                base = r["base_succeeded"] / r["base_settled"] if r["base_settled"] else None
                rows.append({
                    group_by: r["dim_value"],
                    "success_rate_current": round(cur * 100, 2) if cur is not None else None,
                    "success_rate_baseline": round(base * 100, 2) if base is not None else None,
                    "attempts_current": r["cur_settled"],
                })
            return {"metric": "payment_success_rate", "version": metric_version,
                    "window_hours": window_hours, "group_by": group_by, "rows": rows}
        res = sql.run("funnel_counts", {"window_start": start, "window_end": end, "method": None})
        r = res["rows"][0]
        rate = r["succeeded"] / r["settled"] if r["settled"] else None
        return {
            "metric": "payment_success_rate", "version": metric_version,
            "window_hours": window_hours,
            "success_rate": round(rate * 100, 2) if rate is not None else None,
            "settled": r["settled"], "succeeded": r["succeeded"],
            "pending": r["started"] - r["settled"],
        }


@private.get("/api/funnel")
def funnel(window_hours: int = Query(default=24, ge=1, le=72)) -> dict:
    with session_scope() as session:
        end = data_horizon(session)
        start = end - timedelta(hours=window_hours)
        sql = GovernedSqlTool(session, run_id=str(uuid.uuid4()), agent="api")
        r = sql.run("funnel_counts",
                    {"window_start": start, "window_end": end, "method": None})["rows"][0]
        return {"window_hours": window_hours,
                "stages": {
                    "payment_started": r["started"],
                    "method_selected": r["method_selected"],
                    "payment_initiated": r["initiated"],
                    "settled": r["settled"],
                    "succeeded": r["succeeded"],
                    "failed": r["failed"],
                }}


@private.get("/api/experiments/{experiment_id}/readout")
def experiment_readout(experiment_id: str) -> dict:
    from paylens.agents.experiment import run_experiment

    with session_scope() as session:
        horizon = data_horizon(session)
        sql = GovernedSqlTool(session, run_id=str(uuid.uuid4()), agent="api")
        state: InvestigationState = {
            "run_id": str(uuid.uuid4()),
            "plan": {"experiment_id": experiment_id},
            "baseline_start": horizon - timedelta(days=7),
            "window_end": horizon,
        }
        return JSONResponse(run_experiment(state, sql))


@private.get("/api/audit/queries")
def audit_queries(limit: int = Query(default=50, ge=1, le=500)) -> list[dict]:
    with session_scope() as session:
        rows = session.execute(
            select(QueryAuditLog).order_by(QueryAuditLog.executed_at.desc()).limit(limit)
        ).scalars().all()
        return [
            {"run_id": q.run_id, "agent": q.agent, "template": q.template,
             "row_count": q.row_count, "duration_ms": q.duration_ms,
             "executed_at": q.executed_at.isoformat(), "sql": q.sql_text}
            for q in rows
        ]


@private.get("/api/deployments")
def deployments(limit: int = Query(default=20, ge=1, le=100)) -> list[dict]:
    with session_scope() as session:
        rows = session.execute(
            select(DeploymentRecordDB)
            .order_by(DeploymentRecordDB.deployed_at.desc()).limit(limit)
        ).scalars().all()
        return [
            {"change_id": d.change_id, "service": d.service, "gateway": d.gateway,
             "deployed_at": d.deployed_at.isoformat(), "note": d.note}
            for d in rows
        ]


from paylens.replay.api import live, router  # noqa: E402

app.include_router(router)
app.include_router(live)
app.include_router(private)
