"""Regression tests for edge/corner cases found in the edge-case audit.

Each test pins a specific boundary: rogue/missing SRM arms, degenerate
baselines, empty samples, negative latency, multi-experiment customers and
experiments whose arms never produced a settled attempt.
"""
from __future__ import annotations

import pytest

from paylens.stats import ab_test as st


# ------------------------------------------------------------------ SRM edges


def test_srm_rogue_arm_fails_the_check():
    ok = st.srm_check({"A": 5_000, "B": 5_000}, {"A": 0.5, "B": 0.5})
    assert ok["ok"] is True
    rogue = st.srm_check({"A": 5_000, "B": 5_000, "C": 300}, {"A": 0.5, "B": 0.5})
    assert rogue["ok"] is False
    assert "C" in rogue["reason"]


def test_srm_missing_planned_arm_fails():
    # planned B never appeared in the data: observed 0 vs expected 5k
    out = st.srm_check({"A": 10_000}, {"A": 0.5, "B": 0.5})
    assert out["ok"] is False and out["p_value"] < 0.001


def test_srm_unnormalized_ratios_are_normalized():
    a = st.srm_check({"A": 5_000, "B": 5_000}, {"A": 0.5, "B": 0.5})
    b = st.srm_check({"A": 5_000, "B": 5_000}, {"A": 5.0, "B": 5.0})
    assert a["ok"] is True and b["ok"] is True
    assert abs(a["chi2"] - b["chi2"]) < 1e-9


def test_srm_degenerate_inputs():
    assert st.srm_check({}, {"A": 0.5, "B": 0.5})["ok"] is False
    assert st.srm_check({"A": 10}, {})["ok"] is False          # no planned allocation
    assert st.srm_check({"A": 0, "B": 0}, {"A": 0.5, "B": 0.5})["ok"] is False


# --------------------------------------------------------------- stats edges


def test_percentile_empty_is_none():
    assert st.percentile([], 0.5) is None
    assert st.percentile([], 0.95) is None


def test_mde_stays_finite_on_degenerate_baselines():
    for baseline in (0.0, 1.0):
        mde = st.min_detectable_effect(50_000, baseline)
        assert 0 < mde < 1
    with pytest.raises(ValueError):
        st.min_detectable_effect(0, 0.2)


def test_wilson_no_data_claims_no_certainty():
    assert st.wilson_interval(0, 0) == (0.0, 1.0)


def test_ztest_rejects_nonpositive_samples():
    with pytest.raises(ValueError):
        st.two_proportion_ztest(10, 0, 5, 100)


# ------------------------------------------------------- event contract edge


def test_negative_latency_rejected_by_contract():
    from paylens.events.schemas import PaymentEvent

    base = {"event_type": "gateway_response", "attempt_id": "a1", "txn_id": "t1",
            "pseudo_customer_id": "c1", "method": "upi", "gateway": "g",
            "amount_paise": 100}
    with pytest.raises(Exception):
        PaymentEvent.model_validate({**base, "latency_ms": -5})
    ev = PaymentEvent.model_validate({**base, "latency_ms": 0})  # zero is valid
    assert ev.latency_ms == 0


# ------------------------------------------------------------- ingest edges


def test_ingest_dead_letters_invalid_latency_keeps_valid(session, store):
    from paylens.events.bus import InMemoryBus, TOPIC_PAYMENTS
    from paylens.storage.ingest import IngestConsumer

    bus = InMemoryBus()
    good = {"event_id": "evt-good-1", "event_type": "payment_started",
            "attempt_id": "edge-a1", "txn_id": "t", "pseudo_customer_id": "edge-c1",
            "method": "upi", "gateway": "gateway-a", "amount_paise": 500}
    bad = dict(good, event_id="evt-bad-1", event_type="gateway_response",
               attempt_id="edge-a2", latency_ms=-42)
    bus.publish_many(TOPIC_PAYMENTS, [good, bad])

    consumer = IngestConsumer(bus, session)
    stats = consumer.run_once(batch_size=100)
    assert stats["invalid"] == 1
    assert len(consumer.dead_letter) == 1
    assert stats["events_ingested"] == 1


def test_variant_lookup_deterministic_for_multi_experiment_customer(session, store):
    from datetime import datetime, timezone

    from paylens.events.bus import InMemoryBus, TOPIC_PAYMENTS
    from paylens.storage.ingest import IngestConsumer
    from paylens.storage.models import ExperimentAssignment, PaymentAttempt

    session.add(ExperimentAssignment(
        experiment_id="exp-zz", variant="B", pseudo_customer_id="edge-multi",
        assigned_at=datetime(2026, 1, 1, tzinfo=timezone.utc)))
    session.add(ExperimentAssignment(
        experiment_id="exp-aa", variant="A", pseudo_customer_id="edge-multi",
        assigned_at=datetime(2026, 1, 2, tzinfo=timezone.utc)))
    session.commit()

    bus = InMemoryBus()
    bus.publish_many(TOPIC_PAYMENTS, [{
        "event_id": "evt-multi-1", "event_type": "payment_started",
        "attempt_id": "edge-a9", "txn_id": "t9", "pseudo_customer_id": "edge-multi",
        "method": "upi", "gateway": "gateway-a", "amount_paise": 700}])
    consumer = IngestConsumer(bus, session)
    consumer.run_once(batch_size=100)

    row = session.get(PaymentAttempt, "edge-a9")
    assert row.experiment_id == "exp-aa" and row.variant == "A"


def test_experiment_with_zero_settled_attempts_skips_gracefully(session, store):
    from datetime import datetime, timezone

    from paylens.agents.experiment import run_experiment
    from paylens.storage.models import ExperimentAssignment
    from paylens.tools.governed_sql import GovernedSqlTool

    session.add(ExperimentAssignment(
        experiment_id="E-EDGE-ZERO", variant="A",
        pseudo_customer_id="nobody-a",
        assigned_at=datetime(2026, 1, 3, tzinfo=timezone.utc)))
    session.add(ExperimentAssignment(
        experiment_id="E-EDGE-ZERO", variant="B",
        pseudo_customer_id="nobody-b",
        assigned_at=datetime(2026, 1, 3, tzinfo=timezone.utc)))
    session.commit()

    state = {"run_id": "edge", "plan": {"experiment_id": "E-EDGE-ZERO"},
             "baseline_start": None, "window_end": None}
    result = run_experiment(state, GovernedSqlTool(session, "edge", agent="test"))
    # no settled attempts for either arm -> graceful skip, never a crash
    assert result["skipped"] in ("missing arm data",
                                 "arm with zero attempts; readout not possible")
    assert result["srm"]["counts"] == {"A": 1, "B": 1}
