"""At-least-once Kafka ingestion. PostgreSQL commits before Kafka offsets."""
from __future__ import annotations

import hashlib
import logging
import signal
from contextlib import closing

from pydantic import ValidationError
from sqlalchemy.orm import Session

from paylens.config import SETTINGS
from paylens.events.schemas import ExperimentExposure, PaymentEvent
from paylens.storage.db import session_scope
from paylens.storage.ingest import IngestConsumer, invalidate_watermarks, upsert
from paylens.storage.models import RejectedMessage

log = logging.getLogger("paylens.consumer")


def ingest_batch(session: Session, records: list) -> dict:
    payments, exposures, rejected = [], [], []
    for record in records:
        is_payment = record.topic == SETTINGS.topic_payment_events
        model = PaymentEvent if is_payment else ExperimentExposure
        try:
            item = model.model_validate_json(record.value or b"")
            required = {"event_id", "ts"} if is_payment else {"event_id", "ts", "assigned_at"}
            if not required.issubset(item.model_fields_set):
                raise ValueError("missing stable event ID or timestamp")
        except (ValidationError, ValueError):
            rejected.append({
                "topic": record.topic, "partition": record.partition, "offset": record.offset,
                "payload_sha256": hashlib.sha256(record.value or b"").hexdigest(),
                "reason": "invalid_event_contract",
            })
            continue
        (payments if is_payment else exposures).append(item.model_dump())
    stats = IngestConsumer(None, session).process_records(payments, exposures)
    if rejected:
        session.execute(upsert(session, RejectedMessage).on_conflict_do_nothing(
            index_elements=["topic", "partition", "offset"]), rejected)
        invalidate_watermarks(session, ("payments", "assignments", "guardrails"))
    return {**stats, "rejected": len(rejected)}


def main() -> None:
    from kafka import KafkaConsumer
    from kafka.structs import OffsetAndMetadata

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    running = True

    def stop(_signum, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    with closing(KafkaConsumer(
        SETTINGS.topic_payment_events,
        SETTINGS.topic_experiments,
        bootstrap_servers=SETTINGS.kafka_bootstrap,
        group_id=SETTINGS.consumer_group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_records=2_000,
        **SETTINGS.kafka_security,
    )) as consumer:
        log.info("Kafka ingest ready group=%s", SETTINGS.consumer_group)
        while running:
            batches = consumer.poll(timeout_ms=1_000)
            if not batches:
                continue
            records = [record for partition in batches.values() for record in partition]
            with session_scope() as session:
                stats = ingest_batch(session, records)
            consumer.commit({partition: OffsetAndMetadata(batch[-1].offset + 1, "")
                             for partition, batch in batches.items() if batch})
            log.info("batch committed %s", stats)


if __name__ == "__main__":
    main()
