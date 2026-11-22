"""Produce the synthetic dataset to real Kafka (Docker layer).

    python scripts/produce_to_kafka.py [--attempts-per-day 50000]

Reads PAYLENS_KAFKA_BOOTSTRAP (default localhost:9092) and publishes:
  - payment lifecycle events  -> payment-events
  - experiment exposures      -> experiment-exposures
Deployments are written directly to PostgreSQL by seed_deployments().
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from paylens.config import SETTINGS  # noqa: E402
from paylens.events.bus import KafkaBus, TOPIC_EXPOSURES, TOPIC_PAYMENTS  # noqa: E402
from paylens.events.generator import PaymentEventGenerator  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempts-per-day", type=int, default=50_000)
    parser.add_argument("--experiment-users-per-arm", type=int, default=50_000)
    args = parser.parse_args()

    gen = PaymentEventGenerator(
        attempts_per_day=args.attempts_per_day,
        experiment_users_per_arm=args.experiment_users_per_arm,
        seed=SETTINGS.seed,
    )
    data = gen.generate()

    bus = KafkaBus()
    n1 = bus.publish_many(TOPIC_PAYMENTS, data.payment_events)
    n2 = bus.publish_many(TOPIC_EXPOSURES, data.exposures)
    print(f"published {n1} payment events and {n2} exposures to Kafka")

    from paylens.storage.db import session_scope
    from paylens.storage.ingest import ingest_deployments

    with session_scope() as session:
        ingest_deployments(session, data.deployments)
    print(f"wrote {len(data.deployments)} deployment records to the database")


if __name__ == "__main__":
    main()
