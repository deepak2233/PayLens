"""Ingest pipeline: validate -> de-duplicate -> derive attempt state -> store.

Correctness rules implemented here (each covered by a unit test):

* **Idempotency / dedup** - ``event_id`` is the global idempotency key.
  Re-delivered events (at-least-once delivery from Kafka) are counted and
  discarded; they never inflate metrics.
* **Out-of-order safety** - attempt state is derived from a monotone stage
  rank. A late ``gateway_response`` can fill a missing funnel timestamp but
  can never regress a terminal outcome.
* **Delayed outcomes** - an attempt without a terminal event stays
  ``settled = False``; success-rate metrics exclude unsettled attempts and
  report the pending share separately instead of guessing.
* **First-write-wins assignment** - an experiment assignment is immutable;
  late attempts are back-filled with their variant via the assignment join.
"""
from __future__ import annotations

import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Iterable

from pydantic import ValidationError
from sqlalchemy import func, select, text, tuple_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from paylens.events.bus import TOPIC_EXPOSURES, TOPIC_PAYMENTS, InMemoryBus
from paylens.events.schemas import (
    EVENT_TO_STAGE,
    STAGE_RANK,
    EventType,
    ExperimentExposure,
    PaymentEvent,
)
from paylens.storage.models import (
    AssignmentConflict,
    DataWatermark,
    DeploymentRecordDB,
    ExperimentAssignment,
    PaymentAttempt,
    PaymentEventRecord,
)

_STATUS_LABEL = {
    "started": "started",
    "method_selected": "method_selected",
    "initiated": "initiated",
    "gateway_response": "awaiting_gateway",
    "terminal": "terminal",
}


def _naive_utc(dt: datetime) -> datetime:
    """The store keeps naive UTC datetimes (portable across SQLite/Postgres)."""
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def upsert(session: Session, table):
    insert = pg_insert if session.get_bind().dialect.name == "postgresql" else sqlite_insert
    return insert(table)


def lock_ingest(session: Session) -> None:
    if session.get_bind().dialect.name == "postgresql":
        # Keep the event ledger and attempt projection atomic across workers.
        session.execute(text("SELECT pg_advisory_xact_lock(736418219)"))


def invalidate_watermarks(session: Session, streams: tuple[str, ...], earliest: datetime | None = None) -> None:
    stmt = update(DataWatermark).where(DataWatermark.stream.in_(streams), DataWatermark.valid)
    if earliest is not None:
        stmt = stmt.where(DataWatermark.complete_through >= _naive_utc(earliest))
    session.execute(stmt.values(valid=False))


def _chunked(items: list, size: int = 500) -> Iterable[list]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


class IngestConsumer:
    """Transport-agnostic ingest worker (memory bus in demo, Kafka in prod)."""

    def __init__(self, bus: InMemoryBus | None, session: Session) -> None:
        self.bus = bus
        self.session = session
        self.dead_letter: list[dict] = []

    # ------------------------------------------------------------------ api
    def process_records(self, payments: list[dict], exposures: list[dict]) -> dict:
        """Ingest pre-fetched raw records (Kafka path); same pipeline as the bus path."""
        lock_ingest(self.session)
        stats: dict = {"assignments": 0}
        exposure_customers: list[str] = []
        if exposures:
            stats["assignments"], exposure_customers = self._ingest_exposures(exposures)
        if payments:
            stats.update(self._ingest_payment_events(payments))
        # Back-fill variants only for customers seen in THIS batch's exposures:
        # bounded work, independent of the assignment table's total size.
        stats["assignments_backfilled"] = self._backfill_variants(exposure_customers)
        return stats

    def run_once(self, batch_size: int = 25_000) -> dict:
        stats = {
            "events_received": 0,
            "events_ingested": 0,
            "duplicates": 0,
            "invalid": 0,
            "attempts_upserted": 0,
            "assignments": 0,
            "assignments_backfilled": 0,
        }
        t0 = time.monotonic()

        # 1. Experiment assignments first so attempts can join their variant.
        raw_exposures = self.bus.drain(TOPIC_EXPOSURES, batch_size)

        # 2. Payment events.
        raw_events = self.bus.drain(TOPIC_PAYMENTS, batch_size)
        stats.update(self.process_records(raw_events, raw_exposures))

        stats["duration_ms"] = int((time.monotonic() - t0) * 1000)
        self.session.commit()
        return stats

    # ------------------------------------------------------- exposures
    def _ingest_exposures(self, raw_exposures: list[dict]) -> tuple[int, list[str]]:
        """Upsert assignments (first write wins). Returns (count, customers)."""
        events = []
        for raw in raw_exposures:
            try:
                events.append(ExperimentExposure.model_validate(raw))
            except ValidationError:
                self.dead_letter.append(raw)
        if not events:
            return 0, []
        keys = sorted({(ev.experiment_id, ev.pseudo_customer_id) for ev in events})
        prior = {}
        for chunk in _chunked(keys, 200):
            rows = self.session.execute(
                select(ExperimentAssignment.experiment_id, ExperimentAssignment.pseudo_customer_id,
                       ExperimentAssignment.variant)
                .where(tuple_(ExperimentAssignment.experiment_id,
                              ExperimentAssignment.pseudo_customer_id).in_(chunk))
            )
            prior.update({(exp, user): variant for exp, user, variant in rows})
        new = []
        conflicts = []
        for exp in events:
            key = (exp.experiment_id, exp.pseudo_customer_id)
            if key not in prior:
                prior[key] = exp.variant
                new.append({
                    "experiment_id": exp.experiment_id,
                    "variant": exp.variant,
                    "pseudo_customer_id": exp.pseudo_customer_id,
                    "assigned_at": _naive_utc(exp.assigned_at),
                })
            if prior[key] != exp.variant:
                conflicts.append({"event_id": exp.event_id, "experiment_id": exp.experiment_id})
        for chunk in _chunked(new, 200):
            self.session.execute(upsert(self.session, ExperimentAssignment).values(chunk)
                                 .on_conflict_do_nothing(index_elements=["experiment_id", "pseudo_customer_id"]))
        for chunk in _chunked(conflicts, 200):
            self.session.execute(upsert(self.session, AssignmentConflict).values(chunk)
                                 .on_conflict_do_nothing(index_elements=["event_id"]))
        if new:
            invalidate_watermarks(self.session, ("assignments",), min(row["assigned_at"] for row in new))
        return len(new), [user for _, user in keys]

    def _backfill_variants(self, customers: list[str]) -> int:
        """Tag attempts from customers whose exposure was ingested in this batch.

        Scoped to the batch's customers on purpose: the cost is proportional
        to what arrived now, never to the total size of the assignment table.
        """
        if not customers:
            return 0
        from sqlalchemy import update

        pairs = self._variant_lookup(customers)
        if not pairs:
            return 0
        updated = 0
        for customer, (experiment_id, variant) in pairs.items():
            res = self.session.execute(
                update(PaymentAttempt)
                .where(PaymentAttempt.pseudo_customer_id == customer,
                       PaymentAttempt.experiment_id.is_(None))
                .values(experiment_id=experiment_id, variant=variant)
            )
            updated += res.rowcount or 0
        return updated

    # --------------------------------------------------- payment events
    def _ingest_payment_events(self, raw_events: list[dict]) -> dict:
        stats = {"events_received": len(raw_events), "events_ingested": 0,
                 "duplicates": 0, "invalid": 0, "attempts_upserted": 0}
        valid: list[PaymentEvent] = []
        seen_ids: set[str] = set()
        for raw in raw_events:
            try:
                ev = PaymentEvent.model_validate(raw)
            except ValidationError:
                stats["invalid"] += 1
                self.dead_letter.append(raw)
                continue
            if ev.event_id in seen_ids:
                stats["duplicates"] += 1  # duplicate within the same batch
                continue
            seen_ids.add(ev.event_id)
            valid.append(ev)

        if not valid:
            return stats

        # Drop events already stored (Kafka at-least-once re-delivery).
        existing_ids: set[str] = set()
        for chunk in _chunked([e.event_id for e in valid]):
            rows = self.session.execute(
                select(PaymentEventRecord.event_id).where(PaymentEventRecord.event_id.in_(chunk))
            ).scalars().all()
            existing_ids.update(rows)
        fresh = [e for e in valid if e.event_id not in existing_ids]
        stats["duplicates"] += len(valid) - len(fresh)
        if fresh:
            invalidate_watermarks(self.session, ("payments", "guardrails"), min(_naive_utc(e.ts) for e in fresh))

        # Deployments may ride along in the payload stream (producer emits
        # them as sentinel records) - none in this schema; kept for clarity.

        by_attempt: dict[str, list[PaymentEvent]] = defaultdict(list)
        for ev in fresh:
            by_attempt[ev.attempt_id].append(ev)

        attempt_ids = list(by_attempt.keys())
        stored = self._load_attempts(attempt_ids)
        # One batched lookup for the whole batch (never one query per attempt).
        customer_ids = [r.pseudo_customer_id for r in fresh]
        variant_map = self._variant_lookup(customer_ids)
        rows = []
        for attempt_id, events in by_attempt.items():
            rows.append(self._derive_attempt(attempt_id, events,
                                             stored.get(attempt_id), variant_map))
        self._upsert_attempts(rows)
        self._insert_events(fresh)
        stats["events_ingested"] = len(fresh)
        stats["attempts_upserted"] = len(rows)
        return stats

    def _load_attempts(self, attempt_ids: list[str]) -> dict[str, dict]:
        out: dict[str, dict] = {}
        for chunk in _chunked(attempt_ids):
            starts = dict(self.session.execute(
                select(PaymentEventRecord.attempt_id, func.min(PaymentEventRecord.ts))
                .where(PaymentEventRecord.attempt_id.in_(chunk),
                       PaymentEventRecord.event_type == EventType.PAYMENT_STARTED.value)
                .group_by(PaymentEventRecord.attempt_id)
            ).all())
            rows = self.session.execute(
                select(PaymentAttempt).where(PaymentAttempt.attempt_id.in_(chunk))
            ).scalars().all()
            for r in rows:
                out[r.attempt_id] = {
                    "attempt_id": r.attempt_id,
                    "txn_id": r.txn_id,
                    "pseudo_customer_id": r.pseudo_customer_id,
                    "method": r.method,
                    "gateway": r.gateway,
                    "bank": r.bank,
                    "amount_paise": r.amount_paise,
                    "currency": r.currency,
                    "app_version": r.app_version,
                    "geo": r.geo,
                    "customer_tier": r.customer_tier,
                    "experiment_id": r.experiment_id,
                    "variant": r.variant,
                    "started_at": _naive_utc(starts[r.attempt_id]) if r.attempt_id in starts else None,
                    "_fallback_start": _naive_utc(r.started_at),
                    "method_selected_at": _naive_utc(r.method_selected_at) if r.method_selected_at else None,
                    "initiated_at": _naive_utc(r.initiated_at) if r.initiated_at else None,
                    "gateway_responded_at": _naive_utc(r.gateway_responded_at) if r.gateway_responded_at else None,
                    "terminal_at": _naive_utc(r.terminal_at) if r.terminal_at else None,
                    "status": r.status,
                    "status_rank": r.status_rank,
                    "error_code": r.error_code,
                    "latency_ms": r.latency_ms,
                    "settled": r.settled,
                    "is_duplicate_charge": r.is_duplicate_charge,
                    "event_count": r.event_count,
                    "last_event_at": _naive_utc(r.last_event_at) if r.last_event_at else None,
                }
        return out

    def _variant_lookup(self, customer_ids: list[str]) -> dict[str, tuple[str, str]]:
        """Deterministic assignment lookup.

        A customer assigned to multiple experiments yields the assignment
        with the lowest experiment_id (then earliest assigned_at) - the
        result never depends on row order returned by the database.
        """
        out = {}
        for chunk in _chunked(list(set(customer_ids))):
            rows = self.session.execute(
                select(ExperimentAssignment.experiment_id, ExperimentAssignment.variant,
                       ExperimentAssignment.pseudo_customer_id)
                .where(ExperimentAssignment.pseudo_customer_id.in_(chunk))
                .order_by(ExperimentAssignment.pseudo_customer_id,
                          ExperimentAssignment.experiment_id,
                          ExperimentAssignment.assigned_at)
            ).all()
            for experiment_id, variant, customer in rows:
                if customer not in out:  # first row in the deterministic order wins
                    out[customer] = (experiment_id, variant)
        return out

    def _derive_attempt(self, attempt_id: str, events: list[PaymentEvent],
                        stored: dict | None, variant_map: dict[str, tuple[str, str]]) -> dict:
        events = sorted(events, key=lambda e: (_naive_utc(e.ts),
                                               STAGE_RANK[EVENT_TO_STAGE[e.event_type]]))
        first = events[0]
        row = dict(stored) if stored else {
            "attempt_id": attempt_id,
            "txn_id": first.txn_id,
            "pseudo_customer_id": first.pseudo_customer_id,
            "method": first.method,
            "gateway": first.gateway,
            "bank": first.bank,
            "amount_paise": first.amount_paise,
            "currency": first.currency,
            "app_version": first.app_version,
            "geo": first.geo,
            "customer_tier": first.customer_tier,
            "experiment_id": None,
            "variant": None,
            "started_at": None,
            "method_selected_at": None,
            "initiated_at": None,
            "gateway_responded_at": None,
            "terminal_at": None,
            "status": "started",
            "status_rank": 0,
            "error_code": None,
            "latency_ms": None,
            "settled": False,
            "is_duplicate_charge": False,
            "event_count": 0,
            "last_event_at": None,
        }
        fallback = row.pop("_fallback_start", _naive_utc(first.ts))
        if row["experiment_id"] is None:
            lookup = variant_map.get(row["pseudo_customer_id"])
            if lookup is not None:
                row["experiment_id"], row["variant"] = lookup

        for ev in events:
            stage = EVENT_TO_STAGE[ev.event_type]
            ts = _naive_utc(ev.ts)
            if stage == "post_terminal":
                # refunds and other post-terminal records: kept on the event
                # log, but they must never alter funnel state or status.
                row["last_event_at"] = ts if row["last_event_at"] is None else max(row["last_event_at"], ts)
                continue
            rank = STAGE_RANK[stage]
            col = {
                "started": "started_at",
                "method_selected": "method_selected_at",
                "initiated": "initiated_at",
                "gateway_response": "gateway_responded_at",
            }.get(stage)
            if col:
                # earliest timestamp wins (out-of-order safe)
                if row[col] is None or ts < row[col]:
                    row[col] = ts
            if stage == "terminal":
                failed = ev.event_type == EventType.PAYMENT_FAILED
                terminal = row["terminal_at"]
                if terminal is None or ts < terminal or (ts == terminal and failed and row["status"] != "failed"):
                    row["status"] = "failed" if failed else "succeeded"
                    row["status_rank"] = 4
                    row["terminal_at"] = ts
                    row["settled"] = True
                    if ev.error_code:
                        row["error_code"] = ev.error_code
            if stage == "gateway_response":
                if ev.error_code and not row["error_code"]:
                    row["error_code"] = ev.error_code
                if ev.latency_ms is not None and row["latency_ms"] is None:
                    row["latency_ms"] = ev.latency_ms
            if ev.is_duplicate_charge:
                row["is_duplicate_charge"] = True
            row["status_rank"] = max(row["status_rank"], rank)
            if row["status_rank"] < 4:
                row["status"] = _STATUS_LABEL[max(
                    (s for s, r in STAGE_RANK.items() if r == row["status_rank"]),
                    default="started")]
            row["last_event_at"] = ts if row["last_event_at"] is None else max(row["last_event_at"], ts)

        row["event_count"] = (row["event_count"] or 0) + len(events)
        if row["started_at"] is None:
            row["started_at"] = min(fallback, _naive_utc(first.ts))
        return row

    def _upsert_attempts(self, rows: list[dict]) -> None:
        if not rows:
            return
        cols = [c for c in rows[0].keys()]
        # SQLite limits bound parameters per statement (~999); keep chunks small
        chunk_size = max(1, 900 // len(cols))
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            stmt = upsert(self.session, PaymentAttempt).values(chunk)
            set_expr = {c: stmt.excluded[c] for c in cols if c != "attempt_id"}
            stmt = stmt.on_conflict_do_update(index_elements=["attempt_id"], set_=set_expr)
            self.session.execute(stmt)

    def _insert_events(self, events: list[PaymentEvent]) -> None:
        rows = [
            {
                "event_id": e.event_id,
                "attempt_id": e.attempt_id,
                "event_type": e.event_type.value,
                "ts": _naive_utc(e.ts),
                "status": e.status,
                "error_code": e.error_code,
                "latency_ms": e.latency_ms,
                "is_duplicate_charge": e.is_duplicate_charge,
                "payload": json.loads(e.model_dump_json()),
            }
            for e in events
        ]
        cols = len(rows[0])
        for chunk in _chunked(rows, size=max(1, 900 // cols)):
            stmt = upsert(self.session, PaymentEventRecord).values(chunk)
            stmt = stmt.on_conflict_do_nothing(index_elements=["event_id"])
            self.session.execute(stmt)


def ingest_deployments(session: Session, deployments: list[dict]) -> int:
    for dep in deployments:
        row = dict(dep)
        if isinstance(row.get("deployed_at"), str):
            row["deployed_at"] = datetime.fromisoformat(row["deployed_at"])
        row["deployed_at"] = _naive_utc(row["deployed_at"])
        session.execute(upsert(session, DeploymentRecordDB).values(**row)
                        .on_conflict_do_nothing(index_elements=["change_id"]))
    session.commit()
    return len(deployments)
