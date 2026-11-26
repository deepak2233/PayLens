"""Integration check against disposable Kafka and PostgreSQL services."""
from __future__ import annotations

import json
import subprocess
import sys
import time
import uuid
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient
from kafka import KafkaProducer
from sqlalchemy import func, select, text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from paylens.api.main import app  # noqa: E402
from paylens.config import SETTINGS  # noqa: E402
from paylens.events.bus import KafkaBus  # noqa: E402
from paylens.replay.demo import _payment  # noqa: E402
from paylens.replay.experiment import ExperimentProtocol  # noqa: E402
from paylens.replay.ops import attest, migrate  # noqa: E402
from paylens.storage.db import analytics_scope, session_scope  # noqa: E402
from paylens.storage.models import (  # noqa: E402
    ExperimentAssignment, ExperimentProtocolRecord, PaymentAttempt, PaymentEventRecord, QueryAuditLog, RejectedMessage,
)


def main():
    if not SETTINGS.is_postgres or not SETTINGS.analytics_database_url or not SETTINGS.api_token:
        raise SystemExit("Configure disposable PostgreSQL credentials and an API token for the smoke test")
    migrate()
    marker = "smoke-" + uuid.uuid4().hex[:10]
    now = datetime.now(timezone.utc).replace(microsecond=0)
    start = now - timedelta(hours=2)
    current = _payment(marker + "-cur", start, "Gateway B", "upi", "timeout", 30)
    baseline = _payment(marker + "-base", start - timedelta(hours=24), "Gateway B", "upi", "success", 30)

    with analytics_scope() as session:
        assert session.scalar(text("SHOW transaction_read_only")) == "on"
    spec = ExperimentProtocol(
        experiment_id=marker, enrollment_start=now - timedelta(days=7),
        enrollment_end=now - timedelta(days=6), outcome_hours=24, treatment_share=0.5,
        min_users_per_arm=100, min_effect_pp=1, timeout_margin_pp=8,
        duplicate_charge_margin_pp=8, refund_margin_pp=8,
    )
    trial_events, assignments = [], []
    for variant, successes in (("A", 40), ("B", 80)):
        for i in range(100):
            unit = f"{marker}-{variant}-{i}"
            assigned = spec.enrollment_start + timedelta(minutes=i)
            assignments.append({"event_id": unit, "ts": assigned.isoformat(), "assigned_at": assigned.isoformat(),
                                "experiment_id": marker, "variant": variant, "pseudo_customer_id": unit})
            if i < successes:
                trial_events.extend(_payment(unit, assigned + timedelta(seconds=10),
                                             "Gateway B", "upi", "success", 10, unit))
    with session_scope() as session:
        session.add(ExperimentProtocolRecord(experiment_id=marker, policy=spec.model_dump(mode="json"),
                                             registered_at=spec.enrollment_start - timedelta(days=1)))
    worker = subprocess.Popen([sys.executable, "-m", "paylens.streaming.consumer"])
    try:
        bus = KafkaBus()
        # Same-partition records deliberately replay a terminal before its start.
        bus.publish_many(SETTINGS.topic_payment_events, [
            {**event, "ts": event["ts"].isoformat()} for event in
            [current[-1], *current[:-1], *baseline, current[0], baseline[-1]]
        ])
        bus.publish_many(SETTINGS.topic_payment_events, [
            {**event, "ts": event["ts"].isoformat()} for event in trial_events
        ])
        bus.publish_many(SETTINGS.topic_experiments, assignments)
        bus.producer.close()
        with closing(KafkaProducer(bootstrap_servers=SETTINGS.kafka_bootstrap, **SETTINGS.kafka_security)) as producer:
            producer.send(SETTINGS.topic_payment_events, b"{malformed").get(timeout=20)

        deadline = time.monotonic() + 35
        while time.monotonic() < deadline:
            if worker.poll() is not None:
                raise RuntimeError("Consumer stopped before committing its batch")
            with session_scope() as session:
                attempts = session.scalars(select(PaymentAttempt).where(
                    PaymentAttempt.attempt_id.in_([marker + "-base", marker + "-cur"]))).all()
                rejected = session.scalar(select(func.count()).select_from(RejectedMessage)
                                          .where(RejectedMessage.topic == SETTINGS.topic_payment_events))
                events = session.scalar(select(func.count()).select_from(PaymentEventRecord)
                                        .where(PaymentEventRecord.attempt_id.in_([marker + "-base", marker + "-cur"])))
                assigned_count = session.scalar(select(func.count()).select_from(ExperimentAssignment)
                                                .where(ExperimentAssignment.experiment_id == marker))
                trials_count = session.scalar(select(func.count()).select_from(PaymentEventRecord)
                                              .where(PaymentEventRecord.attempt_id.like(f"{marker}-_-%")))
            if len(attempts) == 2 and rejected and events == 8 and assigned_count == 200 and trials_count == 480:
                break
            time.sleep(0.3)
        else:
            raise AssertionError("Kafka data did not reach PostgreSQL with expected counts")
        assert {"succeeded", "failed"} == {a.status for a in attempts}
        assert next(a for a in attempts if a.status == "failed").started_at == start
        for stream in ("payments", "assignments", "guardrails"):
            attest(stream, now)
        headers = {"Authorization": f"Bearer {SETTINGS.api_token}"}
        with TestClient(app) as client:
            assert client.post("/api/v2/live/replay", json={}).status_code == 401
            response = client.post("/api/v2/live/replay", json={}, headers=headers)
            assert response.status_code == 200, response.text
            run = response.json()
            assert run["source"]["coverage"] == "attested"
            assert run["report"]["delta_pp"] == -100.0
            retrieved = client.get("/api/v2/live/reports/" + run["run_id"], headers=headers)
            assert retrieved.json()["run_id"] == run["run_id"]
            experiment = client.post("/api/v2/live/experiments/" + marker, headers=headers)
            assert experiment.status_code == 200, experiment.text
            trial = experiment.json()["report"]
            assert trial["verdict"] == "review_candidate", trial
            assert trial["arms"]["A"]["assigned"] == 100 and trial["arms"]["A"]["converted"] == 40
            assert trial["arms"]["B"]["assigned"] == 100 and trial["arms"]["B"]["converted"] == 80
        with session_scope() as session:
            count = session.scalar(select(func.count()).select_from(QueryAuditLog)
                                   .where(QueryAuditLog.run_id == run["run_id"]))
        assert count == 3
        print(json.dumps({"status": "passed", "attempts": 2, "unique_events": events, "quarantined": rejected,
                          "audited_queries": count, "authenticated_report_delta_pp": run["report"]["delta_pp"],
                          "trial_assigned": assigned_count, "trial_verdict": trial["verdict"]}))
    finally:
        worker.terminate()
        try:
            worker.wait(timeout=10)
        except subprocess.TimeoutExpired:
            worker.kill()
            worker.wait()


if __name__ == "__main__":
    main()
