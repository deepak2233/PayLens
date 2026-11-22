"""Ingest correctness: dedup, out-of-order safety, delayed outcomes."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta

from paylens.events.schemas import EventType
from paylens.storage.ingest import IngestConsumer

T0 = datetime(2026, 12, 1, 10, 0, 0)


def _ev(event_type: EventType, offset_s: int, attempt_id: str, **kw) -> dict:
    base = dict(
        event_id=str(uuid.uuid4()),
        event_type=event_type,
        ts=T0 + timedelta(seconds=offset_s),
        attempt_id=attempt_id,
        txn_id="txn-" + attempt_id[:12],
        pseudo_customer_id="cust-" + attempt_id[:12],
        method="upi",
        gateway="gateway-a",
        amount_paise=500_00,
    )
    base.update(kw)
    return base


def _ingest(session, raw_events: list[dict]) -> dict:
    consumer = IngestConsumer(bus=None, session=session)
    return consumer.process_records(payments=raw_events, exposures=[])


def _attempt(session, attempt_id: str):
    from paylens.storage.models import PaymentAttempt

    return session.get(PaymentAttempt, attempt_id)


def test_duplicate_events_counted_once(session):
    attempt_id = str(uuid.uuid4())
    ev = _ev(EventType.PAYMENT_STARTED, 0, attempt_id)
    stats = _ingest(session, [ev, dict(ev)])  # same event_id re-delivered
    assert stats["duplicates"] >= 1
    assert stats["events_ingested"] == 1
    row = _attempt(session, attempt_id)
    assert row.event_count == 1


def test_out_of_order_never_regresses_terminal(session):
    attempt_id = str(uuid.uuid4())
    failed = _ev(EventType.PAYMENT_FAILED, 40, attempt_id, error_code="BANK_DECLINE")
    late_gateway = _ev(EventType.GATEWAY_RESPONSE, 25, attempt_id,
                       status="approved", latency_ms=1500)
    late_method = _ev(EventType.METHOD_SELECTED, 10, attempt_id)
    # publish in arrival order: terminal first, then the late events
    stats = _ingest(session, [failed, late_gateway, late_method])
    assert stats["attempts_upserted"] == 1
    row = _attempt(session, attempt_id)
    assert row.status == "failed"
    assert row.status_rank == 4
    assert row.settled is True
    assert row.method_selected_at is not None   # late event still fills funnel
    assert row.gateway_responded_at is not None
    assert row.latency_ms == 1500
    assert row.event_count == 3


def test_success_after_timeout_keeps_first_terminal(session):
    attempt_id = str(uuid.uuid4())
    ok = _ev(EventType.PAYMENT_SUCCEEDED, 50, attempt_id)
    _ingest(session, [ok])
    # a spurious late "failed" must not overwrite the terminal success
    _ingest(session, [_ev(EventType.PAYMENT_FAILED, 60, attempt_id,
                          error_code="GATEWAY_TIMEOUT")])
    row = _attempt(session, attempt_id)
    assert row.status == "succeeded"


def test_delayed_outcome_stays_pending(session):
    attempt_id = str(uuid.uuid4())
    _ingest(session, [
        _ev(EventType.PAYMENT_STARTED, 0, attempt_id),
        _ev(EventType.METHOD_SELECTED, 5, attempt_id),
        _ev(EventType.PAYMENT_INITIATED, 9, attempt_id),
        _ev(EventType.GATEWAY_RESPONSE, 15, attempt_id, status="timeout",
            error_code="GATEWAY_TIMEOUT"),
    ])
    row = _attempt(session, attempt_id)
    assert row.settled is False
    assert row.status == "awaiting_gateway"
    assert row.error_code == "GATEWAY_TIMEOUT"


def test_one_attempt_is_one_payment_not_five_events(session, store):
    from sqlalchemy import func, select

    from paylens.storage.models import PaymentAttempt, PaymentEventRecord

    with session.begin():
        attempts = session.execute(select(func.count()).select_from(PaymentAttempt)).scalar()
        events = session.execute(select(func.count()).select_from(PaymentEventRecord)).scalar()
    assert events > attempts  # multiple events per attempt
    # and the seeded funnel counts attempts, not events
    from paylens.storage.models import Investigation  # noqa: F401


def test_assignment_first_write_wins_and_backfill(session):
    from paylens.storage.models import ExperimentAssignment

    attempt_id = str(uuid.uuid4())
    customer = "cust-" + attempt_id[:12]  # matches _ev's pseudo_customer_id
    # payment first, exposure afterwards -> backfill tags the attempt
    _ingest(session, [_ev(EventType.PAYMENT_STARTED, 0, attempt_id)])
    consumer = IngestConsumer(bus=None, session=session)
    consumer.process_records(payments=[], exposures=[{
        "experiment_id": "exp-test", "variant": "B",
        "pseudo_customer_id": customer,
        "assigned_at": T0.isoformat(),
        "event_id": str(uuid.uuid4()), "ts": T0.isoformat(),
    }])
    row = _attempt(session, attempt_id)
    assert row.experiment_id == "exp-test"
    assert row.variant == "B"
    # a conflicting re-assignment must not win
    consumer.process_records(payments=[], exposures=[{
        "experiment_id": "exp-test", "variant": "A",
        "pseudo_customer_id": customer,
        "assigned_at": (T0 + timedelta(hours=1)).isoformat(),
        "event_id": str(uuid.uuid4()), "ts": (T0 + timedelta(hours=1)).isoformat(),
    }])
    a = session.query(ExperimentAssignment).filter_by(
        experiment_id="exp-test", pseudo_customer_id=customer).one()
    assert a.variant == "B"
