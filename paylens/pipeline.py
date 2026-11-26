"""End-to-end demo pipeline: generate -> publish -> ingest -> investigate.

One entry point used by the CLI, the FastAPI demo endpoints and the tests,
so every layer exercises exactly the same code path.
"""
from __future__ import annotations

from datetime import datetime

from paylens.config import SETTINGS
from paylens.events.bus import InMemoryBus, TOPIC_EXPOSURES, TOPIC_PAYMENTS
from paylens.events.generator import PaymentEventGenerator
from paylens.metrics.definitions import seed_metric_definitions
from paylens.storage.db import create_all, session_scope
from paylens.storage.ingest import IngestConsumer, ingest_deployments


def seed_database(delete_first: bool = True, anchor: datetime | None = None,
                  attempts_per_day: int | None = None,
                  experiment_users_per_arm: int | None = None) -> dict:
    """Create schema, generate the synthetic dataset, publish it on the bus
    and run the ingest consumer. Returns ingest statistics.

    ``anchor`` pins the dataset to a fixed investigation moment (the end of
    the incident day) so demo runs are reproducible; when omitted it falls
    back to PAYLENS_ANCHOR_TS, else wall-clock now."""
    if anchor is None and SETTINGS.anchor_iso:
        anchor = datetime.fromisoformat(SETTINGS.anchor_iso)
    if delete_first and not SETTINGS.is_sqlite:
        raise ValueError("Demo reset requires an isolated SQLite database")
    if delete_first:
        from paylens.storage.db import engine
        from paylens.storage.models import Base

        Base.metadata.drop_all(engine)  # demo runs are reproducible, never additive
    create_all()
    with session_scope() as session:
        seed_metric_definitions(session)

    generator = PaymentEventGenerator(
        anchor=anchor,
        attempts_per_day=attempts_per_day or 20_000,
        experiment_users_per_arm=experiment_users_per_arm or 10_000,
        seed=SETTINGS.seed,
    )
    data = generator.generate()

    bus = InMemoryBus()
    bus.publish_many(TOPIC_PAYMENTS, data.payment_events)
    bus.publish_many(TOPIC_EXPOSURES, data.exposures)

    stats: dict = {"deployments": 0, "events": len(data.payment_events)}
    with session_scope() as session:
        stats["deployments"] = ingest_deployments(session, data.deployments)

    # ingest in batches - mirrors how the Kafka consumer would process partitions
    totals = {
        "events_received": 0, "events_ingested": 0, "duplicates": 0,
        "invalid": 0, "attempts_upserted": 0, "assignments": 0,
        "assignments_backfilled": 0,
    }
    while bus.lag(TOPIC_PAYMENTS) > 0 or bus.lag(TOPIC_EXPOSURES) > 0:
        with session_scope() as session:
            consumer = IngestConsumer(bus, session)
            run = consumer.run_once(batch_size=25_000)
        for k in totals:
            if k in run:
                totals[k] += run[k]
        if run.get("events_received", 0) == 0 and bus.lag(TOPIC_PAYMENTS) == 0 and bus.lag(TOPIC_EXPOSURES) == 0:
            break
    stats.update(totals)
    return stats


def horizon() -> datetime:
    from paylens.agents.supervisor import data_horizon
    from paylens.storage.db import session_scope as _scope

    with _scope() as session:
        return data_horizon(session)


def run_investigation(question: str, metric_version: int = 2,
                      config: dict | None = None) -> dict:
    from paylens.agents.supervisor import build_supervisor

    supervisor = build_supervisor()
    return supervisor.run(question, metric_version=metric_version, config=config)


if __name__ == "__main__":  # pragma: no cover
    import json
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "seed":
        print(json.dumps(seed_database(), indent=2, default=str))
    elif len(sys.argv) > 1 and sys.argv[1] == "investigate":
        result = run_investigation(
            "Investigate the UPI success-rate drop over the last 24 hours. "
            "Compare with yesterday and identify affected segments."
        )
        print(json.dumps(result["report"], indent=2, default=str))
    else:
        print("usage: python -m paylens.pipeline [seed|investigate]")
