"""Operator commands for schema migrations, protocol registration and checkpoints."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import inspect, text

from paylens.metrics.definitions import seed_metric_definitions
from paylens.replay.demo import STREAMS
from paylens.replay.experiment import ExperimentProtocol
from paylens.replay.incident import utc
from paylens.storage.db import engine, session_scope
from paylens.storage.ingest import lock_ingest, upsert
from paylens.storage.models import Base, DataWatermark, ExperimentProtocolRecord


def migrate(bind=engine):
    with bind.begin() as connection:
        if bind.dialect.name == "postgresql":
            connection.execute(text("SELECT pg_advisory_xact_lock(736418219)"))
        Base.metadata.create_all(connection)
        if bind.dialect.name == "postgresql":
            target_types = (
                ("payment_attempts", "method", 32), ("payment_attempts", "gateway", 64),
                ("payment_attempts", "variant", 32), ("experiment_assignments", "variant", 32),
            )
            schema = inspect(connection)
            for table, column, length in target_types:
                current = next(c["type"] for c in schema.get_columns(table) if c["name"] == column)
                if getattr(current, "length", None) != length:
                    connection.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} TYPE varchar({length})"))
        for table in Base.metadata.tables.values():
            for index in table.indexes:
                index.create(connection, checkfirst=True)


def register(spec: ExperimentProtocol) -> None:
    with session_scope() as session:
        lock_ingest(session)
        previous = session.get(ExperimentProtocolRecord, spec.experiment_id)
        if previous:
            if previous.policy != spec.model_dump(mode="json"):
                raise ValueError("Protocol already registered with different parameters")
            return
        now = datetime.now(timezone.utc)
        if now > spec.enrollment_start:
            raise ValueError("Register the experiment before enrollment starts")
        session.add(ExperimentProtocolRecord(experiment_id=spec.experiment_id, registered_at=now,
                                              policy=spec.model_dump(mode="json")))


def attest(stream: str, through: datetime) -> None:
    if through.tzinfo is None or through > datetime.now(timezone.utc):
        raise ValueError("Checkpoint must be a timezone-aware timestamp at or before the present")
    with session_scope() as session:
        lock_ingest(session)
        mark = session.get(DataWatermark, stream)
        if mark and mark.valid and through < utc(mark.complete_through):
            raise ValueError("A valid checkpoint cannot be moved backward")
        stmt = upsert(session, DataWatermark).values(stream=stream, complete_through=through, valid=True)
        session.execute(stmt.on_conflict_do_update(index_elements=["stream"],
                                                  set_={"complete_through": through, "valid": True}))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("migrate")
    protocol_parser = actions.add_parser("register")
    protocol_parser.add_argument("file", type=Path)
    checkpoint = actions.add_parser("attest")
    checkpoint.add_argument("stream", choices=STREAMS)
    checkpoint.add_argument("through", type=datetime.fromisoformat)
    args = parser.parse_args()
    try:
        if args.action == "migrate":
            migrate()
            with session_scope() as session:
                seed_metric_definitions(session)
        elif args.action == "register":
            register(ExperimentProtocol.model_validate_json(args.file.read_bytes()))
        else:
            attest(args.stream, args.through)
    except ValueError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
