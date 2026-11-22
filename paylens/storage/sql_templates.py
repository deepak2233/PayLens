"""Governed SQL templates.

The ONLY queries agents may run. Each template is engineer-authored SQL with
bound parameters; agents (and the LLM) can choose a template and fill in an
allow-listed parameter value, but can never compose or modify SQL text.

Every parameter has a validation predicate; every execution is recorded in
``query_audit_log`` by the executor in ``paylens/tools/governed_sql.py``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

# ---------------------------------------------------------------------------
# parameter validators
#
# Governance model: SQL text is never composed from inputs; dimension
# *columns* come only from VALID_DIMENSIONS (identifier whitelist); all
# *values* travel as bound parameters. Value vocabularies are store-specific
# (the synthetic demo uses the sets below; real sources carry their own), so
# value validators enforce boundable strings, not fixed vocabularies.
# ---------------------------------------------------------------------------
VALID_DIMENSIONS = {
    "method", "gateway", "bank", "app_version", "geo", "customer_tier",
    "error_code", "variant",
}
VALID_METHODS = {"upi", "debit_card", "credit_card", "netbanking", None}  # demo vocabulary
VALID_VARIANTS = {"A", "B", None}  # demo vocabulary


def _opt_text(max_len: int = 64) -> Callable:
    def check(v) -> bool:
        if v is None:
            return True
        if not isinstance(v, str) or not (1 <= len(v) <= max_len):
            return False
        return all(ch.isprintable() for ch in v)
    return check


_OPT_TEXT = _opt_text()


class GovernedSqlError(ValueError):
    """Any governance violation: unknown template, invalid params, or a
    dimension outside the approved list. Subclasses ValueError so callers
    can catch either."""
    pass


def _ts(v) -> bool:
    from datetime import datetime

    return isinstance(v, datetime)


def _opt_ts(v) -> bool:
    from datetime import datetime

    return v is None or isinstance(v, datetime)


def _opt_str(choices: set) -> Callable:
    def check(v) -> bool:
        return v is None or (isinstance(v, str) and v in choices)
    return check


@dataclass(frozen=True)
class Template:
    name: str
    description: str
    sql: str  # named-bound parameters (:name); SQLAlchemy text() semantics
    params: dict[str, Callable] = field(default_factory=dict)

    def validate(self, given: dict) -> dict:
        unknown = set(given) - set(self.params)
        if unknown:
            raise GovernedSqlError(f"template '{self.name}' got unknown params {unknown}")
        missing = [k for k in self.params if k not in given]
        if missing:
            raise GovernedSqlError(f"template '{self.name}' missing params {missing}")
        out = {}
        for key, check in self.params.items():
            val = given[key]
            if not check(val):
                raise GovernedSqlError(f"invalid value for param '{key}': {val!r}")
            out[key] = val
        return out


TEMPLATES: dict[str, Template] = {
    # ------------------------------------------------------------- funnel
    "funnel_counts": Template(
        name="funnel_counts",
        description="Funnel stage counts and success counts for a window (attempts, not events).",
        sql="""
        SELECT
            COUNT(*)                                                              AS started,
            SUM(CASE WHEN method_selected_at IS NOT NULL THEN 1 ELSE 0 END)        AS method_selected,
            SUM(CASE WHEN initiated_at      IS NOT NULL THEN 1 ELSE 0 END)        AS initiated,
            SUM(CASE WHEN settled           THEN 1 ELSE 0 END)                    AS settled,
            SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END)                  AS succeeded,
            SUM(CASE WHEN settled AND status <> 'succeeded' THEN 1 ELSE 0 END)     AS failed
        FROM payment_attempts
        WHERE started_at >= :window_start AND started_at < :window_end
              AND (:method IS NULL OR method = :method)
        """,
        params={"window_start": _ts, "window_end": _ts, "method": _OPT_TEXT},
    ),
    "success_rate_by_dimension": Template(
        name="success_rate_by_dimension",
        description=(
            "Success rate and attempt volume grouped by one approved dimension "
            "for two windows (current vs baseline), for drop attribution. "
            "{DIMENSION} is filled only from VALID_DIMENSIONS by the executor."
        ),
        sql="""
        SELECT {DIMENSION}                                                               AS dim_value,
               SUM(CASE WHEN in_current AND settled AND status='succeeded' THEN 1 ELSE 0 END) AS cur_succeeded,
               SUM(CASE WHEN in_current AND settled THEN 1 ELSE 0 END)                        AS cur_settled,
               SUM(CASE WHEN in_baseline AND settled AND status='succeeded' THEN 1 ELSE 0 END) AS base_succeeded,
               SUM(CASE WHEN in_baseline AND settled THEN 1 ELSE 0 END)                       AS base_settled
        FROM (
            SELECT {DIMENSION},
                   started_at >= :window_start AND started_at < :window_end          AS in_current,
                   started_at >= :baseline_start AND started_at < :window_start      AS in_baseline,
                   settled, status
            FROM payment_attempts
            WHERE started_at >= :baseline_start AND started_at < :window_end
                  AND (:method IS NULL OR method = :method)
        ) t
        WHERE in_current OR in_baseline
        GROUP BY {DIMENSION}
        """,
        params={
            "window_start": _ts, "window_end": _ts, "baseline_start": _ts,
            "method": _OPT_TEXT,
        },
        # NOTE: {dimension} is formatted in Python from VALID_DIMENSIONS only.
    ),
    # ------------------------------------------------------------ anomaly
    "hourly_success_rate": Template(
        name="hourly_success_rate",
        description="Hourly success rate for a method/gateway slice across the analysis horizon. "
                    "{HOUR_BUCKET} is rendered per SQL dialect by the executor.",
        sql="""
        SELECT
            CAST({HOUR_BUCKET} AS TEXT)                                          AS hour_bucket,
            SUM(CASE WHEN status='succeeded' THEN 1 ELSE 0 END)                  AS succeeded,
            COUNT(*)                                                             AS settled,
            SUM(CASE WHEN status='succeeded' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*),0) AS success_rate
        FROM payment_attempts
        WHERE settled
          AND started_at >= :window_start AND started_at < :window_end
          AND method = :method
          AND (:gateway IS NULL OR gateway = :gateway)
        GROUP BY hour_bucket
        ORDER BY hour_bucket
        """,
        params={"window_start": _ts, "window_end": _ts,
                "method": _OPT_TEXT,
                "gateway": _OPT_TEXT},
    ),
    "daily_success_rate": Template(
        name="daily_success_rate",
        description="Daily success rate for a method/second-dimension slice across the "
                    "analysis horizon (grain for multi-month real sources). "
                    "{DAY_BUCKET} is rendered per SQL dialect by the executor.",
        sql="""
        SELECT
            CAST({DAY_BUCKET} AS TEXT)                                           AS hour_bucket,
            SUM(CASE WHEN status='succeeded' THEN 1 ELSE 0 END)                  AS succeeded,
            COUNT(*)                                                             AS settled,
            SUM(CASE WHEN status='succeeded' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*),0) AS success_rate
        FROM payment_attempts
        WHERE settled
          AND started_at >= :window_start AND started_at < :window_end
          AND method = :method
          AND (:gateway IS NULL OR gateway = :gateway)
        GROUP BY hour_bucket
        ORDER BY hour_bucket
        """,
        params={"window_start": _ts, "window_end": _ts,
                "method": _OPT_TEXT,
                "gateway": _OPT_TEXT},
    ),
    "error_code_breakdown": Template(
        name="error_code_breakdown",
        description="Failure error-code mix for a slice, current vs baseline window.",
        sql="""
        SELECT
            error_code,
            SUM(CASE WHEN started_at >= :window_start AND started_at < :window_end THEN 1 ELSE 0 END)  AS cur_count,
            SUM(CASE WHEN started_at >= :baseline_start AND started_at < :window_start THEN 1 ELSE 0 END) AS base_count
        FROM payment_attempts
        WHERE settled AND status = 'failed'
          AND started_at >= :baseline_start AND started_at < :window_end
          AND (:method IS NULL OR method = :method)
          AND (:gateway IS NULL OR gateway = :gateway)
        GROUP BY error_code
        ORDER BY cur_count DESC
        """,
        params={"window_start": _ts, "window_end": _ts, "baseline_start": _ts,
                "method": _OPT_TEXT,
                "gateway": _OPT_TEXT},
    ),
    "deployments_in_window": Template(
        name="deployments_in_window",
        description="Infrastructure change records inside the baseline-to-current horizon.",
        sql="""
        SELECT change_id, service, gateway, deployed_at, note
        FROM deployments
        WHERE deployed_at >= :baseline_start AND deployed_at < :window_end
        ORDER BY deployed_at
        """,
        params={"baseline_start": _ts, "window_end": _ts},
    ),
    "segment_profile": Template(
        name="segment_profile",
        description="Volume and success rate of one exact segment slice (for report evidence).",
        sql="""
        SELECT method, gateway,
               SUM(CASE WHEN settled AND status='succeeded' THEN 1 ELSE 0 END) AS succeeded,
               SUM(CASE WHEN settled THEN 1 ELSE 0 END)                        AS settled,
               SUM(CASE WHEN settled THEN 0 ELSE 1 END)                        AS pending,
               COUNT(*)                                                        AS attempts
        FROM payment_attempts
        WHERE started_at >= :window_start AND started_at < :window_end
              AND (:method IS NULL OR method = :method)
              AND (:gateway IS NULL OR gateway = :gateway)
        GROUP BY method, gateway
        ORDER BY attempts DESC
        """,
        params={"window_start": _ts, "window_end": _ts,
                "method": _OPT_TEXT,
                "gateway": _OPT_TEXT},
    ),
    # --------------------------------------------------------- experiment
    "experiment_assignments": Template(
        name="experiment_assignments",
        description="Assignment counts per variant for SRM (sample-ratio mismatch) checks.",
        sql="""
        SELECT variant, COUNT(*) AS assigned
        FROM experiment_assignments
        WHERE experiment_id = :experiment_id
        GROUP BY variant
        """,
        params={"experiment_id": lambda v: isinstance(v, str) and len(v) <= 64},
    ),
    "experiment_outcomes": Template(
        name="experiment_outcomes",
        description="Per-variant success outcome over the experiment's measured attempts.",
        sql="""
        SELECT a.variant,
               COUNT(*)                                                       AS attempts,
               SUM(CASE WHEN a.status='succeeded' THEN 1 ELSE 0 END)           AS succeeded,
               SUM(CASE WHEN a.status='succeeded' THEN 1 ELSE 0 END) * 1.0
                   / NULLIF(COUNT(*), 0)                                       AS success_rate
        FROM payment_attempts a
        WHERE a.experiment_id = :experiment_id
              AND a.settled
              AND (:window_start IS NULL OR a.started_at >= :window_start)
              AND (:window_end IS NULL OR a.started_at < :window_end)
        GROUP BY a.variant
        """,
        params={"experiment_id": lambda v: isinstance(v, str) and len(v) <= 64,
                "window_start": _opt_ts, "window_end": _opt_ts},
    ),
    "experiment_guardrails": Template(
        name="experiment_guardrails",
        description="Guardrail metrics per variant: latency quantiles, timeout rate, duplicate-charge rate.",
        sql="""
        SELECT
            a.variant,
            COUNT(*)                                                        AS attempts,
            SUM(CASE WHEN a.error_code='GATEWAY_TIMEOUT' THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*),0) AS timeout_rate,
            SUM(CASE WHEN a.is_duplicate_charge THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*),0)          AS dup_charge_rate
        FROM payment_attempts a
        WHERE a.experiment_id = :experiment_id
              AND a.settled
              AND (:window_start IS NULL OR a.started_at >= :window_start)
              AND (:window_end IS NULL OR a.started_at < :window_end)
        GROUP BY a.variant
        """,
        params={"experiment_id": lambda v: isinstance(v, str) and len(v) <= 64,
                "window_start": _opt_ts, "window_end": _opt_ts},
    ),
    "experiment_latency": Template(
        name="experiment_latency",
        description="Gateway latency samples per variant (percentiles computed in Python).",
        sql="""
        SELECT a.variant, a.latency_ms
        FROM payment_attempts a
        WHERE a.experiment_id = :experiment_id
              AND a.latency_ms IS NOT NULL
              AND (:window_start IS NULL OR a.started_at >= :window_start)
              AND (:window_end IS NULL OR a.started_at < :window_end)
        """,
        params={"experiment_id": lambda v: isinstance(v, str) and len(v) <= 64,
                "window_start": _opt_ts, "window_end": _opt_ts},
    ),
    "list_experiments": Template(
        name="list_experiments",
        description="Experiment ids present in the assignment table (most recent last).",
        sql="""
        SELECT DISTINCT experiment_id
        FROM experiment_assignments
        ORDER BY experiment_id
        LIMIT 5
        """,
        params={},
    ),
    "experiment_refunds": Template(
        name="experiment_refunds",
        description="Refund events per variant joined through the measured attempts.",
        sql="""
        SELECT a.variant, COUNT(e.event_id) AS refunds
        FROM payment_events e
        JOIN payment_attempts a ON a.attempt_id = e.attempt_id
        WHERE e.event_type = 'refund_issued'
              AND a.experiment_id = :experiment_id
              AND (:window_start IS NULL OR a.started_at >= :window_start)
              AND (:window_end IS NULL OR a.started_at < :window_end)
        GROUP BY a.variant
        """,
        params={"experiment_id": lambda v: isinstance(v, str) and len(v) <= 64,
                "window_start": _opt_ts, "window_end": _opt_ts},
    ),
    # ------------------------------------------------------------ health
    "pending_share": Template(
        name="pending_share",
        description="Unsettled-attempt share per window (delayed-outcome monitoring).",
        sql="""
        SELECT SUM(CASE WHEN settled THEN 0 ELSE 1 END) * 1.0 / NULLIF(COUNT(*),0) AS pending_share,
               COUNT(*)                                                             AS attempts
        FROM payment_attempts
        WHERE started_at >= :window_start AND started_at < :window_end
        """,
        params={"window_start": _ts, "window_end": _ts},
    ),
}
