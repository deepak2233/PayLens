"""ORM models - the analytical store.

Key design decisions (mirrored in sql/init.sql for PostgreSQL deployments):

1. ``payment_attempts`` vs ``payment_events``: one attempt row per payment
   attempt, many events per attempt. Five events for one attempt must never
   be counted as five payments - success-rate metrics always read the
   attempts table.
2. ``event_id`` has a UNIQUE constraint: the ingest layer de-duplicates on
   insert (idempotent consumer semantics).
3. Attempt status is derived with a monotone stage rank so out-of-order and
   late events can never regress a terminal outcome.
4. ``metric_definitions`` stores versioned metric logic so any report can
   reproduce exactly how a number was computed.
5. ``query_audit_log`` records every governed SQL execution for auditability.
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class PaymentAttempt(Base):
    __tablename__ = "payment_attempts"

    attempt_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    txn_id: Mapped[str] = mapped_column(String(64), index=True)
    pseudo_customer_id: Mapped[str] = mapped_column(String(64), index=True)
    method: Mapped[str] = mapped_column(String(20), index=True)
    gateway: Mapped[str] = mapped_column(String(30), index=True)
    bank: Mapped[str] = mapped_column(String(30), default="")
    amount_paise: Mapped[int]
    currency: Mapped[str] = mapped_column(String(8), default="INR")
    app_version: Mapped[str] = mapped_column(String(20), default="")
    geo: Mapped[str] = mapped_column(String(12), default="")
    customer_tier: Mapped[str] = mapped_column(String(12), default="")
    experiment_id: Mapped[str | None] = mapped_column(String(64), index=True, nullable=True)
    variant: Mapped[str | None] = mapped_column(String(2), nullable=True)

    # funnel state
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    method_selected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    initiated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    gateway_responded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    terminal_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    status: Mapped[str] = mapped_column(String(20), default="started", index=True)
    status_rank: Mapped[int] = mapped_column(Integer, default=0)
    error_code: Mapped[str | None] = mapped_column(String(40), nullable=True)
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    settled: Mapped[bool] = mapped_column(Boolean, default=False)
    is_duplicate_charge: Mapped[bool] = mapped_column(Boolean, default=False)
    event_count: Mapped[int] = mapped_column(Integer, default=0)
    last_event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class PaymentEventRecord(Base):
    __tablename__ = "payment_events"

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    attempt_id: Mapped[str] = mapped_column(String(64), index=True)
    event_type: Mapped[str] = mapped_column(String(30), index=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    status: Mapped[str | None] = mapped_column(String(20), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(40), nullable=True)
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_duplicate_charge: Mapped[bool] = mapped_column(Boolean, default=False)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)


class ExperimentAssignment(Base):
    __tablename__ = "experiment_assignments"
    __table_args__ = (UniqueConstraint("experiment_id", "pseudo_customer_id", name="uq_assignment"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    experiment_id: Mapped[str] = mapped_column(String(64), index=True)
    variant: Mapped[str] = mapped_column(String(2))
    pseudo_customer_id: Mapped[str] = mapped_column(String(64), index=True)
    assigned_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class MetricDefinition(Base):
    __tablename__ = "metric_definitions"
    __table_args__ = (UniqueConstraint("metric_name", "version", name="uq_metric_version"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    metric_name: Mapped[str] = mapped_column(String(60), index=True)
    version: Mapped[int]
    description: Mapped[str] = mapped_column(Text)
    formula_sql: Mapped[str] = mapped_column(Text)
    owner: Mapped[str] = mapped_column(String(60), default="payments-analytics")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class DeploymentRecordDB(Base):
    __tablename__ = "deployments"

    change_id: Mapped[str] = mapped_column(String(40), primary_key=True)
    service: Mapped[str] = mapped_column(String(60))
    gateway: Mapped[str] = mapped_column(String(30), default="")
    deployed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    note: Mapped[str] = mapped_column(Text, default="")


class QueryAuditLog(Base):
    __tablename__ = "query_audit_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64), index=True)
    agent: Mapped[str] = mapped_column(String(40))
    template: Mapped[str] = mapped_column(String(80))
    params_json: Mapped[dict] = mapped_column(JSON, default=dict)
    row_count: Mapped[int]
    duration_ms: Mapped[int]
    sql_text: Mapped[str] = mapped_column(Text)
    executed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Investigation(Base):
    __tablename__ = "investigations"

    run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    question: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(20), default="running", index=True)
    window_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    window_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    metric_version: Mapped[int] = mapped_column(Integer, default=2)
    report: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


Index("ix_attempts_started_method_gateway", PaymentAttempt.started_at, PaymentAttempt.method, PaymentAttempt.gateway)
Index("ix_attempts_last_event", PaymentAttempt.last_event_at)  # serves data_horizon()
Index("ix_events_attempt_ts", PaymentEventRecord.attempt_id, PaymentEventRecord.ts)
