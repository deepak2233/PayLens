"""Kafka ingest worker for the Docker layer.

Runs the same IngestConsumer pipeline used by the demo, but pulls from real
Kafka topics with a consumer group. Duplicate handling, out-of-order state
derivation and delayed-outcome semantics are identical to the demo.

Usage (inside the consumer container):
    python -m paylens.streaming.consumer
"""
from __future__ import annotations

import json
import logging
import signal
import time

from paylens.config import SETTINGS
from paylens.storage.db import create_all, session_scope
from paylens.storage.ingest import IngestConsumer

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("paylens.consumer")

_running = True


def _stop(signum, _frame) -> None:
    global _running
    _running = False


def main() -> None:
    from kafka import KafkaConsumer  # requires the prod requirements

    create_all()
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    consumer = KafkaConsumer(
        SETTINGS.topic_payment_events,
        SETTINGS.topic_exposures,
        bootstrap_servers=SETTINGS.kafka_bootstrap,
        group_id=SETTINGS.consumer_group,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # commit only after a successful DB write
        max_poll_records=5_000,
    )
    log.info("consumer up: group=%s bootstrap=%s",
             SETTINGS.consumer_group, SETTINGS.kafka_bootstrap)

    buffer: list[dict] = []
    last_commit = time.time()
    while _running:
        records = consumer.poll(timeout_ms=1_000)
        for _tp, batch in records.items():
            for rec in batch:
                buffer.append(rec.value)
        due = len(buffer) >= 5_000 or (buffer and time.time() - last_commit >= 5)
        if not due:
            continue
        # route records by schema: exposures carry variant+experiment_id,
        # payment events carry attempt_id
        exposures = [r for r in buffer if "experiment_id" in r and "variant" in r]
        payments = [r for r in buffer if "attempt_id" in r]
        unknown = len(buffer) - len(exposures) - len(payments)
        if unknown:
            log.warning("discarded %s unrecognised records", unknown)
        with session_scope() as session:
            stats = IngestConsumer(bus=None, session=session).process_records(
                payments, exposures)
        log.info("ingested payments=%s exposures=%s duplicates=%s invalid=%s",
                 stats.get("events_ingested", 0), stats.get("assignments", 0),
                 stats.get("duplicates", 0), stats.get("invalid", 0))
        buffer.clear()
        consumer.commit()
        last_commit = time.time()
    consumer.close()
    log.info("consumer stopped")


if __name__ == "__main__":
    main()
