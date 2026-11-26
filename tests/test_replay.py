"""Regressions for delayed outcomes, traffic mix and user-level effects."""
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from paylens.api.main import app
from paylens.replay.demo import ANCHOR, _payment
from paylens.replay.incident import build_incident
from paylens.storage.db import analytics_scope, make_engine
from paylens.storage.ingest import IngestConsumer
from paylens.storage.models import Base, DataWatermark
from paylens.tools.governed_sql import GovernedSqlTool


def test_scenarios_distinguish_delay_mix_and_performance():
    with TestClient(app) as client:
        data = {}
        for name in ("incident", "mix", "delay"):
            response = client.get(f"/api/v2/replay?scenario={name}")
            assert response.status_code == 200
            body = response.json()
            report = body["report"]
            assert sum(report["attribution"].values()) == pytest.approx(report["delta_pp"])
            assert report["current"]["started"] == report["baseline"]["started"] == 1440
            assert report["recent_attempts"] == 30
            assert body["evidence"][0]["template"] == "cohort_sla_v1"
            data[name] = report
        assert data["incident"]["attribution"]["within_pp"] < -4
        assert data["incident"]["incident"]["nearby_changes"][0]["change_id"] == "CHG-8841"
        assert data["mix"]["attribution"]["mix_pp"] < -4
        assert abs(data["mix"]["attribution"]["within_pp"]) < 4
        assert data["delay"]["current"]["later"] > 100
        assert data["delay"]["current"]["settled_only_success_pct"] > data["delay"]["current"]["success_within_sla_pct"]
        long_sla = client.get("/api/v2/replay?scenario=delay&outcome_minutes=180").json()["report"]
        assert long_sla["current"]["success_within_sla_pct"] > data["delay"]["current"]["success_within_sla_pct"]


def test_fixed_horizon_keeps_assigned_users_and_blocks_early_readout():
    with TestClient(app) as client:
        result = client.get("/api/v2/experiments/checkout").json()["report"]
        assert result["verdict"] == "review_candidate"
        for arm in result["arms"].values():
            assert arm["mature"] == arm["assigned"] == 1600
            assert arm["mature"] > arm["attempted"] >= arm["converted"]
        assert result["effect"]["control_pct"] == pytest.approx(100 * result["arms"]["A"]["converted"] / 1600)
        assert result["effect"]["ci95_pp"][0] > result["protocol"]["min_effect_pp"]
        assert all(g["state"] == "pass" for g in result["guardrails"])
        early = client.get("/api/v2/experiments/checkout?stage=early").json()["report"]
        assert early["verdict"] == "inconclusive"
        assert early["effect"] is None
        assert any(g["name"] == "Outcome window closed" and g["state"] == "pending" for g in early["gates"])


def test_cutoff_is_exact_and_late_unseen_events_invalidate_watermark(tmp_path):
    db = make_engine(f"sqlite:///{tmp_path / 'cutoff.db'}")
    Base.metadata.create_all(db)
    cutoff = datetime(2026, 10, 2, 0, 0, tzinfo=timezone.utc)
    start = cutoff - timedelta(hours=1) + timedelta(microseconds=999999)
    on_time = _payment("on-time", start, "A", "upi", "success", 60)
    late = _payment("late", start, "A", "upi", "success", 60)
    late[-1]["ts"] += timedelta(microseconds=1)
    future = _payment("future", start, "A", "upi", "success", 7200)
    with Session(db) as s, s.begin():
        worker = IngestConsumer(None, s)
        worker.process_records([on_time[-1], *late, *future], [])
        worker.process_records(on_time[:-1], [])
        s.add(DataWatermark(stream="payments", complete_through=cutoff))
    with analytics_scope(db) as session:
        sql = GovernedSqlTool(session, "replay", record_audit=False)
        r = build_incident(sql, observed_through=cutoff, window_hours=24,
                           outcome_minutes=1, method=None)["current"]
        assert (r["started"], r["success_sla"], r["later"], r["unresolved_at_cutoff"]) == (3, 1, 1, 1)
    arrival = _payment("new", start, "A", "upi", "success", 7)
    with Session(db) as s, s.begin():
        IngestConsumer(None, s).process_records(arrival, [])
        assert not s.get(DataWatermark, "payments").valid
    db.dispose()


def test_public_benchmarks_use_declared_grains_and_private_routes_require_auth(monkeypatch):
    from paylens.api import access

    monkeypatch.setattr(access, "HOSTED", True)
    with TestClient(app) as client:
        data = client.get("/api/v2/benchmarks").json()
        assert data["shoppers"]["sessions"] == 12_330
        assert sum(row["purchases"] for row in data["shoppers"]["groups"]) == data["shoppers"]["purchases"]
        assert data["upworthy"]["excluded_risk"] > 0
        assert all(sample["arms"] and "adjusted_p" in sample for sample in data["upworthy"]["samples"])
        assert data["pix"]["months"][0]["floor_pct"] == 99.9
        assert client.post("/api/v2/live/replay", json={}).status_code == 503
        assert client.get("/api/audit/queries").status_code == 503


def test_watermark_and_conflict_gate_cannot_be_overridden(tmp_path):
    from paylens.replay.demo import protocol
    from paylens.replay.experiment import build_experiment

    db = make_engine(f"sqlite:///{tmp_path / 'gates.db'}")
    Base.metadata.create_all(db)
    spec = protocol()
    at = spec.enrollment_start + timedelta(minutes=10)
    exposure = {"experiment_id": spec.experiment_id, "assigned_at": at, "ts": at,
                "pseudo_customer_id": "one", "event_id": "assign-A", "variant": "A"}
    with Session(db) as s, s.begin():
        s.add(DataWatermark(stream="assignments", complete_through=ANCHOR))
    with Session(db) as s, s.begin():
        worker = IngestConsumer(None, s)
        worker.process_records([], [exposure, {**exposure, "event_id": "assign-B", "variant": "B"}])
        assert not s.get(DataWatermark, "assignments").valid
    with analytics_scope(db) as s:
        sql = GovernedSqlTool(s, "test", record_audit=False)
        r = build_experiment(sql, spec, registered_at=at, observed_through=ANCHOR, sources_complete=False)
        assert r["verdict"] == "invalid"
        assert r["assignment_conflicts"] == 1
        assert r["effect"] is None
    db.dispose()
