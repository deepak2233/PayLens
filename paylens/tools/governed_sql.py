"""Governed SQL execution - the only database gateway agents may use.

Guarantees:
  * **No free-form SQL.** The caller supplies a template NAME plus params;
    the SQL text lives in ``paylens/storage/sql_templates.py`` and is never
    assembled from user or LLM input. The single dynamic value ({DIMENSION})
    must be a member of ``VALID_DIMENSIONS`` (identifier whitelist).
  * **Bound parameters** - values travel via SQLAlchemy ``text()`` binding,
    never string interpolation.
  * **Read-only** - only SELECT templates exist; executions run in a read
    transaction and results are capped at ``max_rows``.
  * **Auditable** - every call writes ``query_audit_log`` (agent, template,
    params, row count, duration, rendered SQL) so any reported number can be
    re-executed and verified later.
"""
from __future__ import annotations

import time
from hashlib import sha256
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from paylens.config import SETTINGS
from paylens.replay.templates import REPLAY_TEMPLATES
from paylens.storage.sql_templates import (
    TEMPLATES,
    VALID_DIMENSIONS,
    GovernedSqlError,
)

__all__ = ["GovernedSqlTool", "GovernedSqlError", "render_sql"]

_DIALECT_HOUR_BUCKET = {
    "sqlite": "strftime('%Y-%m-%dT%H', started_at)",
    "postgresql": "to_char(started_at, 'YYYY-MM-DD\"T\"HH24')",
}

_DIALECT_DAY_BUCKET = {
    "sqlite": "strftime('%Y-%m-%d', started_at)",
    "postgresql": "to_char(started_at, 'YYYY-MM-DD')",
}
_DEADLINE = {
    "sqlite": "datetime(substr({column}, 1, 19), '+' || :outcome_seconds || ' seconds') || '.'"
              " || substr(substr({column} || '.000000', 21) || '000000', 1, 6)",
    "postgresql": "({column} + (:outcome_seconds * interval '1 second'))",
}
_APPROVED = TEMPLATES | REPLAY_TEMPLATES


def _naive_utc(dt: datetime) -> datetime:
    """The store keeps naive UTC datetimes; bind params must match."""
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def render_sql(template_sql: str, dialect: str, dimension: str | None = None) -> str:
    sql = template_sql
    if "{HOUR_BUCKET}" in sql:
        sql = sql.replace("{HOUR_BUCKET}", _DIALECT_HOUR_BUCKET[dialect])
    if "{DAY_BUCKET}" in sql:
        sql = sql.replace("{DAY_BUCKET}", _DIALECT_DAY_BUCKET[dialect])
    if "{ATTEMPT_DEADLINE}" in sql:
        sql = sql.replace("{ATTEMPT_DEADLINE}", _DEADLINE[dialect].format(column="a.started_at"))
    if "{ASSIGNMENT_DEADLINE}" in sql:
        sql = sql.replace("{ASSIGNMENT_DEADLINE}", _DEADLINE[dialect].format(column="assigned_at"))
    if "{DIMENSION}" in sql:
        if dimension is None or dimension not in VALID_DIMENSIONS:
            raise GovernedSqlError(
                f"dimension '{dimension}' is not in the approved list {sorted(VALID_DIMENSIONS)}"
            )
        sql = sql.replace("{DIMENSION}", dimension)
    return sql


class GovernedSqlTool:
    def __init__(self, session: Session, run_id: str, agent: str = "system",
                 *, record_audit: bool = True) -> None:
        self.session = session
        self.run_id = run_id
        self.agent = agent
        self.record_audit = record_audit
        self.evidence: list[dict] = []

    def list_templates(self) -> list[dict]:
        return [
            {"name": t.name, "description": t.description, "params": sorted(t.params)}
            for t in _APPROVED.values()
        ]

    def run(self, template_name: str, params: dict[str, Any],
            dimension: str | None = None) -> dict:
        """Execute one governed template. Returns {'columns', 'rows', 'meta'}."""
        if template_name not in _APPROVED:
            raise GovernedSqlError(f"unknown template '{template_name}'")
        template = _APPROVED[template_name]
        clean_params = {
            k: _naive_utc(v) if isinstance(v, datetime) else v
            for k, v in template.validate(params).items()
        }
        dialect = self.session.get_bind().dialect.name
        sql = render_sql(template.sql, dialect, dimension).strip()
        bindings = {
            k: v.isoformat(sep=" ", timespec="microseconds") if dialect == "sqlite" and isinstance(v, datetime)
            else v for k, v in clean_params.items()
        }

        t0 = time.monotonic()
        if dialect == "postgresql":
            self.session.execute(text("SELECT set_config('statement_timeout', :timeout, true)"),
                                 {"timeout": f"{SETTINGS.sql_timeout_s}s"})
        stmt = text(sql).bindparams(**bindings)
        result = self.session.execute(stmt)
        keys = list(result.keys())
        rows = result.fetchmany(SETTINGS.sql_max_rows + 1)
        truncated = len(rows) > SETTINGS.sql_max_rows
        rows = rows[: SETTINGS.sql_max_rows]
        duration_ms = int((time.monotonic() - t0) * 1000)

        params_json = {k: v.isoformat() if isinstance(v, datetime) else v for k, v in clean_params.items()}
        entry = {"template": template_name, "params": params_json, "rows": len(rows),
                 "duration_ms": duration_ms, "sha256": sha256(sql.encode()).hexdigest(), "sql": sql}
        self.evidence.append(entry)
        if self.record_audit:
            write_evidence(self.session, self.run_id, self.agent, [entry])

        as_dicts = [dict(zip(keys, r)) for r in rows]
        return {
            "template": template_name,
            "params": params_json,
            "columns": keys,
            "rows": as_dicts,
            "meta": {"row_count": len(rows), "truncated": truncated,
                     "duration_ms": duration_ms},
        }


def write_evidence(session: Session, run_id: str, agent: str, entries: list[dict]) -> None:
    from paylens.storage.models import QueryAuditLog

    session.add_all(QueryAuditLog(run_id=run_id, agent=agent, template=entry["template"],
                                params_json=entry["params"], row_count=entry["rows"],
                                duration_ms=entry["duration_ms"], sql_text=entry["sql"])
                    for entry in entries)
