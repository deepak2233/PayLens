"""Versioned metric definitions.

Every number an agent reports must trace back to a definition in this
registry (and to the ``metric_definitions`` table seeded from it). Changing
business logic means shipping a NEW version - old reports stay reproducible
because they pin the version they used.
"""
from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from paylens.storage.models import MetricDefinition


@dataclass(frozen=True)
class MetricSpec:
    name: str
    version: int
    description: str
    formula_sql: str


METRICS: list[MetricSpec] = [
    MetricSpec(
        name="payment_success_rate",
        version=1,
        description=(
            "succeeded / (succeeded + failed) over attempts that reached the "
            "gateway (payment_initiated seen), regardless of settlement delay."
        ),
        formula_sql=(
            "SELECT SUM(CASE WHEN status='succeeded' THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*),0) "
            "FROM payment_attempts WHERE status_rank = 4 AND initiated_at IS NOT NULL "
            "AND started_at >= :window_start AND started_at < :window_end"
        ),
    ),
    MetricSpec(
        name="payment_success_rate",
        version=2,
        description=(
            "succeeded / all settled (terminal-reached) attempts attributed by "
            "started_at within the window; unsettled attempts are excluded and "
            "reported as pending_share. Current business definition."
        ),
        formula_sql=(
            "SELECT SUM(CASE WHEN status='succeeded' THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*),0) "
            "FROM payment_attempts WHERE settled = TRUE "
            "AND started_at >= :window_start AND started_at < :window_end"
        ),
    ),
    MetricSpec(
        name="pending_share",
        version=2,
        description="Share of attempts in the window that have not reached a terminal outcome yet.",
        formula_sql=(
            "SELECT SUM(CASE WHEN settled = FALSE THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*),0) "
            "FROM payment_attempts WHERE started_at >= :window_start AND started_at < :window_end"
        ),
    ),
    MetricSpec(
        name="gateway_timeout_rate",
        version=1,
        description=(
            "Attempts whose final failure code is GATEWAY_TIMEOUT / all attempts "
            "that reached the gateway."
        ),
        formula_sql=(
            "SELECT SUM(CASE WHEN error_code='GATEWAY_TIMEOUT' THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*),0) "
            "FROM payment_attempts WHERE initiated_at IS NOT NULL "
            "AND started_at >= :window_start AND started_at < :window_end"
        ),
    ),
]


def seed_metric_definitions(session: Session) -> None:
    from paylens.storage.ingest import _dialect_insert

    rows = [
        {"metric_name": m.name, "version": m.version,
         "description": m.description, "formula_sql": m.formula_sql}
        for m in METRICS
    ]
    stmt = _dialect_insert(MetricDefinition.__table__).values(rows)
    if session.bind.dialect.name in ("sqlite", "postgresql"):
        stmt = stmt.on_conflict_do_nothing(index_elements=["metric_name", "version"])
    session.execute(stmt)
    session.commit()


SUCCESS_RATE_V2 = 2  # version pinned by investigations by default
