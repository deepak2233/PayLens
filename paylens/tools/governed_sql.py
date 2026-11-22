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
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from paylens.config import SETTINGS
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
    if "{DIMENSION}" in sql:
        if dimension is None or dimension not in VALID_DIMENSIONS:
            raise GovernedSqlError(
                f"dimension '{dimension}' is not in the approved list {sorted(VALID_DIMENSIONS)}"
            )
        sql = sql.replace("{DIMENSION}", dimension)
    return sql


class GovernedSqlTool:
    def __init__(self, session: Session, run_id: str, agent: str = "system") -> None:
        self.session = session
        self.run_id = run_id
        self.agent = agent

    def list_templates(self) -> list[dict]:
        return [
            {"name": t.name, "description": t.description, "params": sorted(t.params)}
            for t in TEMPLATES.values()
        ]

    def run(self, template_name: str, params: dict[str, Any],
            dimension: str | None = None) -> dict:
        """Execute one governed template. Returns {'columns', 'rows', 'meta'}."""
        if template_name not in TEMPLATES:
            raise GovernedSqlError(f"unknown template '{template_name}'")
        template = TEMPLATES[template_name]
        clean_params = {
            k: _naive_utc(v) if isinstance(v, datetime) else v
            for k, v in template.validate(params).items()
        }
        sql = render_sql(template.sql, self.session.bind.dialect.name, dimension)

        t0 = time.monotonic()
        stmt = text(sql).bindparams(**clean_params)
        result = self.session.execute(stmt)
        keys = list(result.keys())
        rows = result.fetchmany(SETTINGS.sql_max_rows + 1)
        truncated = len(rows) > SETTINGS.sql_max_rows
        rows = rows[: SETTINGS.sql_max_rows]
        duration_ms = int((time.monotonic() - t0) * 1000)

        # audit trail (even for failures of downstream logic - the query ran)
        from paylens.storage.models import QueryAuditLog

        self.session.add(QueryAuditLog(
            run_id=self.run_id,
            agent=self.agent,
            template=template_name,
            params_json={k: (v.isoformat() if hasattr(v, "isoformat") else v)
                         for k, v in clean_params.items()},
            row_count=len(rows),
            duration_ms=duration_ms,
            sql_text=sql,
        ))
        self.session.commit()

        as_dicts = [dict(zip(keys, r)) for r in rows]
        return {
            "template": template_name,
            "params": {k: (v.isoformat() if hasattr(v, "isoformat") else v)
                       for k, v in clean_params.items()},
            "columns": keys,
            "rows": as_dicts,
            "meta": {"row_count": len(rows), "truncated": truncated,
                     "duration_ms": duration_ms},
        }
