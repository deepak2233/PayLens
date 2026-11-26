"""Fixed SQL for payment cohorts, experiment outcomes and evidence."""
from paylens.storage.sql_templates import Template, _opt_text, _ts


def _duration(value):
    return type(value) is int and 60 <= value <= 31 * 86400


_WINDOWS = {
    "baseline_start": _ts, "current_start": _ts, "mature_end": _ts, "observed_through": _ts,
    "outcome_seconds": _duration, "method": _opt_text(32),
}
_EXPERIMENT = {"experiment_id": lambda value: isinstance(value, str) and 0 < len(value) <= 64}

REPLAY_TEMPLATES = {
    "cohort_sla_v1": Template(
        "cohort_sla_v1", "First terminal outcome by the same SLA for two full start cohorts.",
        """
        WITH cohort AS (
            SELECT a.*,
                   CASE WHEN started_at < :current_start THEN 'baseline'
                        WHEN started_at < :mature_end THEN 'current'
                        ELSE 'recent' END AS period,
                   {ATTEMPT_DEADLINE} AS deadline
            FROM payment_attempts a
            WHERE started_at >= :baseline_start AND started_at < :observed_through
              AND (:method IS NULL OR method = :method)
              AND EXISTS (
                  SELECT 1 FROM payment_events s
                  WHERE s.attempt_id = a.attempt_id AND s.event_type = 'payment_started'
                    AND s.ts = a.started_at
              )
        ),
        terminal AS (
            SELECT c.attempt_id, e.ts, e.event_type, e.error_code,
                   ROW_NUMBER() OVER (PARTITION BY c.attempt_id ORDER BY e.ts, e.event_type, e.event_id) AS rn
            FROM cohort c
            JOIN payment_events e ON e.attempt_id = c.attempt_id
            WHERE e.event_type IN ('payment_succeeded', 'payment_failed')
              AND e.ts >= c.started_at AND e.ts <= :observed_through
        )
        SELECT period, {HOUR_BUCKET} AS bucket, method, gateway, COUNT(*) AS started,
               SUM(CASE WHEN method_selected_at BETWEEN started_at AND deadline THEN 1 ELSE 0 END) AS selected,
               SUM(CASE WHEN initiated_at BETWEEN started_at AND deadline THEN 1 ELSE 0 END) AS initiated,
               SUM(CASE WHEN t.ts <= deadline AND t.event_type = 'payment_succeeded' THEN 1 ELSE 0 END) AS success_sla,
               SUM(CASE WHEN t.ts <= deadline AND t.event_type = 'payment_failed' THEN 1 ELSE 0 END) AS fail_sla,
               SUM(CASE WHEN t.ts > deadline THEN 1 ELSE 0 END) AS later,
               SUM(CASE WHEN t.event_type = 'payment_succeeded' THEN 1 ELSE 0 END) AS success_observed,
               COUNT(t.attempt_id) AS settled_observed,
               SUM(CASE WHEN t.ts <= deadline AND t.error_code = 'GATEWAY_TIMEOUT'
                        THEN 1 ELSE 0 END) AS timeout_sla
        FROM cohort c
        LEFT JOIN terminal t ON c.attempt_id = t.attempt_id AND t.rn = 1
        GROUP BY period, {HOUR_BUCKET}, method, gateway
        ORDER BY bucket, method, gateway, period
        """, _WINDOWS,
    ),
    "cohort_start_quality_v1": Template(
        "cohort_start_quality_v1", "Projections without a matching explicit start in the selected range.",
        """
        SELECT COUNT(*) AS missing_start
        FROM payment_attempts a
        WHERE a.started_at >= :baseline_start AND a.started_at < :observed_through
          AND (:method IS NULL OR a.method = :method)
          AND NOT EXISTS (
              SELECT 1 FROM payment_events s
              WHERE s.attempt_id = a.attempt_id AND s.event_type = 'payment_started'
                AND s.ts = a.started_at
          )
        """, {k: _WINDOWS[k] for k in ("baseline_start", "observed_through", "method")},
    ),
    "experiment_itt_v1": Template(
        "experiment_itt_v1",
        "One row per assigned arm, including mature assigned users with zero observed starts.",
        """
        WITH assigned AS (
            SELECT variant, pseudo_customer_id, assigned_at, {ASSIGNMENT_DEADLINE} AS deadline
            FROM experiment_assignments
            WHERE experiment_id = :experiment_id AND assigned_at >= :enrollment_start
              AND assigned_at < :enrollment_end AND assigned_at <= :observed_through
        ),
        user_signals AS (
            SELECT u.variant, u.pseudo_customer_id,
                   CASE WHEN u.deadline <= :observed_through THEN 1 ELSE 0 END AS mature,
                   MAX(CASE WHEN a.attempt_id IS NOT NULL THEN 1 ELSE 0 END) AS attempted,
                   MAX(CASE WHEN e.event_type IN ('payment_succeeded', 'payment_failed')
                            THEN 1 ELSE 0 END) AS terminal,
                   MAX(CASE WHEN e.event_type = 'payment_succeeded' AND e.event_id = (
                        SELECT ft.event_id FROM payment_events ft
                        WHERE ft.attempt_id = a.attempt_id
                          AND ft.event_type IN ('payment_failed', 'payment_succeeded')
                          AND ft.ts BETWEEN a.started_at AND u.deadline
                        ORDER BY ft.ts, ft.event_type, ft.event_id LIMIT 1
                   ) THEN 1 ELSE 0 END) AS converted,
                   MAX(CASE WHEN e.error_code = 'GATEWAY_TIMEOUT' THEN 1 ELSE 0 END) AS timeout_seen,
                   MAX(CASE WHEN e.is_duplicate_charge THEN 1 ELSE 0 END) AS duplicate_seen,
                   MAX(CASE WHEN e.event_type = 'refund_issued' THEN 1 ELSE 0 END) AS refund_seen
            FROM assigned u
            LEFT JOIN payment_attempts a ON a.pseudo_customer_id = u.pseudo_customer_id
              AND u.deadline <= :observed_through
              AND a.started_at BETWEEN u.assigned_at AND u.deadline
              AND EXISTS (
                  SELECT 1 FROM payment_events s
                  WHERE s.attempt_id = a.attempt_id AND s.event_type = 'payment_started'
                    AND s.ts = a.started_at
              )
            LEFT JOIN payment_events e ON e.attempt_id = a.attempt_id
              AND e.ts BETWEEN a.started_at AND u.deadline
            GROUP BY u.variant, u.pseudo_customer_id, u.deadline
        )
        SELECT variant, COUNT(*) AS assigned, SUM(mature) AS mature,
               SUM(attempted) AS attempted, SUM(terminal) AS terminal, SUM(converted) AS converted,
               SUM(timeout_seen) AS timeout, SUM(duplicate_seen) AS duplicate_charge,
               SUM(refund_seen) AS refund
        FROM user_signals GROUP BY variant ORDER BY variant
        """, {**_EXPERIMENT, "enrollment_start": _ts, "enrollment_end": _ts,
              "observed_through": _ts, "outcome_seconds": _duration},
    ),
    "assignment_conflicts_v1": Template(
        "assignment_conflicts_v1", "Conflicting assignments received for the configured experiment.",
        "SELECT COUNT(*) AS conflicts FROM assignment_conflicts WHERE experiment_id = :experiment_id",
        _EXPERIMENT,
    ),
}
