"""Metric + generator calibration checks on the seeded store."""
from __future__ import annotations

from datetime import datetime, timedelta

from paylens.storage.sql_templates import TEMPLATES
from paylens.tools.governed_sql import GovernedSqlTool

WINDOW_END = datetime(2026, 11, 20, 0, 0)  # end of incident day (anchor-derived)


def _sql(session):
    return GovernedSqlTool(session, run_id="test-metrics", agent="pytest")


def test_day_over_day_drop_exists(session):
    sql = _sql(session)
    cur = sql.run("funnel_counts", {"window_start": WINDOW_END - timedelta(hours=24),
                                   "window_end": WINDOW_END, "method": None})["rows"][0]
    base = sql.run("funnel_counts", {"window_start": WINDOW_END - timedelta(hours=48),
                                     "window_end": WINDOW_END - timedelta(hours=24),
                                     "method": None})["rows"][0]
    cur_rate = cur["succeeded"] / cur["settled"]
    base_rate = base["succeeded"] / base["settled"]
    assert base_rate >= 0.85, "baseline day should be healthy"
    assert base_rate - cur_rate >= 0.05, "incident day must show a >= 5pp drop"
    assert base_rate - cur_rate <= 0.20


def test_funnel_stages_are_monotonic(session):
    sql = _sql(session)
    r = sql.run("funnel_counts", {"window_start": WINDOW_END - timedelta(hours=24),
                                  "window_end": WINDOW_END, "method": None})["rows"][0]
    assert r["started"] >= r["method_selected"] >= r["initiated"] >= r["settled"]
    assert r["settled"] == r["succeeded"] + r["failed"]


def test_pending_share_is_bounded(session):
    sql = _sql(session)
    r = sql.run("pending_share", {"window_start": WINDOW_END - timedelta(hours=24),
                                  "window_end": WINDOW_END})["rows"][0]
    assert 0.0 <= r["pending_share"] <= 0.15


def test_metric_definitions_are_versioned(session):
    from paylens.storage.models import MetricDefinition

    rows = session.query(MetricDefinition).all()
    names = {(r.metric_name, r.version) for r in rows}
    assert ("payment_success_rate", 1) in names
    assert ("payment_success_rate", 2) in names


def test_every_template_is_valid_sql(session):
    """Smoke-render each template with type-correct parameters."""
    sql = _sql(session)
    candidates = {
        "window_start": WINDOW_END - timedelta(hours=2),
        "window_end": WINDOW_END,
        "baseline_start": WINDOW_END - timedelta(hours=4),
        "method": None,
        "gateway": None,
        "experiment_id": "exp-2026-11-checkout-upi-v2",
    }
    ran = 0
    for name, t in TEMPLATES.items():
        params = {k: candidates[k] for k in t.params}
        kwargs = {"dimension": "method"} if "{DIMENSION}" in t.sql else {}
        sql.run(name, params, **kwargs)
        ran += 1
    assert ran == len(TEMPLATES)


def test_audit_log_records_every_query(session):
    from paylens.storage.models import QueryAuditLog

    n = session.query(QueryAuditLog).filter_by(run_id="test-metrics").count()
    assert n >= 5
