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
from paylens.replay.ops import attest, migrate  # noqa: E402
from paylens.storage.db import analytics_scope, session_scope  # noqa: E402
from paylens.storage.models import PaymentAttempt, PaymentEventRecord, QueryAuditLog, RejectedMessage  # noqa: E402


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
    worker = subprocess.Popen([sys.executable, "-m", "paylens.streaming.consumer"])
    try:
        bus = KafkaBus()
        # Same-partition records deliberately replay a terminal before its start.
        bus.publish_many(SETTINGS.topic_payment_events, [
            {**event, "ts": event["ts"].isoformat()} for event in
            [current[-1], *current[:-1], *baseline, current[0], baseline[-1]]
        ])
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
            if len(attempts) == 2 and rejected and events == 8:
                break
            time.sleep(0.3)
        else:
            raise AssertionError("Kafka data did not reach PostgreSQL with expected counts")
        assert {"succeeded", "failed"} == {a.status for a in attempts}
        assert next(a for a in attempts if a.status == "failed").started_at == start
        attest("payments", now)
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
        with session_scope() as session:
            count = session.scalar(select(func.count()).select_from(QueryAuditLog)
                                   .where(QueryAuditLog.run_id == run["run_id"]))
        assert count == 3
        print(json.dumps({"status": "passed", "attempts": 2, "unique_events": events, "quarantined": rejected,
                          "audited_queries": count, "authenticated_report_delta_pp": run["report"]["delta_pp"]}))
    finally:
        worker.terminate()
        try:
            worker.wait(timeout=10)
        except subprocess.TimeoutExpired:
            worker.kill()
            worker.wait()


if __name__ == "__main__":
    main()
