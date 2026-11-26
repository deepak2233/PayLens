"""Real-data layer tests: mapping, contracts, verification gate, readouts.

These run against an isolated temporary SQLite engine (monkeypatched) so
they never disturb the shared synthetic demo store.
"""
from __future__ import annotations

import pytest

from paylens.realdata.contracts import OlistOrderRow, UpworthyRow
from paylens.realdata.olist import BRT, STATE_TO_REGION, _brt_to_utc
from paylens.realdata.sources import SourceVerificationError, verify_source

# --------------------------------------------------------------------- fixtures


@pytest.fixture()
def olist_raw(tmp_path):
    """A tiny but real-shaped Olist raw directory (bronze layer)."""
    orders = "\n".join([
        "order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,"
        "order_delivered_carrier_date,order_delivered_customer_date,order_estimated_delivery_date",
        # delivered with approval: purchase 2018-08-20 09:00 BRT -> 12:00 UTC
        "o1,c1,delivered,2018-08-20 09:00:00,2018-08-20 09:30:00,2018-08-22 10:00:00,2018-08-25 14:00:00,2018-08-30 00:00:00",
        # canceled without approval: terminal pinned to purchase time
        "o2,c2,canceled,2018-08-21 08:00:00,,,,2018-08-25 00:00:00",
        # shipped at snapshot: never settles (pending)
        "o3,c3,shipped,2018-08-22 11:00:00,2018-08-22 11:20:00,2018-08-23 09:00:00,,2018-08-28 00:00:00",
        # unavailable: real failure code
        "o4,c1,unavailable,2018-08-23 12:00:00,2018-08-23 12:05:00,,,2018-08-30 00:00:00",
    ]) + "\n"
    payments = "\n".join([
        "order_id,payment_sequential,payment_type,payment_installments,payment_value",
        "o1,1,credit_card,3,100.00",
        "o1,2,voucher,1,18.50",     # split payment: attempt amount = sum
        "o2,1,boleto,1,42.00",
        "o3,1,debit_card,1,75.00",
        "o4,1,credit_card,1,10.00",
    ]) + "\n"
    customers = "\n".join([
        "customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state",
        "c1,u1,01001,sao paulo,SP",
        "c2,u2,60010,fortaleza,CE",
        "c3,u3,30100,belo horizonte,MG",
    ]) + "\n"
    (tmp_path / "olist_orders_dataset.csv").write_text(orders, encoding="utf-8")
    (tmp_path / "olist_order_payments_dataset.csv").write_text(payments, encoding="utf-8")
    (tmp_path / "olist_customers_dataset.csv").write_text(customers, encoding="utf-8")
    return tmp_path


@pytest.fixture()
def isolated_engine(tmp_path, monkeypatch):
    """Bind every engine reference to a throwaway SQLite file."""
    from contextlib import contextmanager

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    url = f"sqlite:///{tmp_path / 'realdata_test.db'}"
    engine = create_engine(url, connect_args={"check_same_thread": False})
    factory = sessionmaker(bind=engine, expire_on_commit=False)

    @contextmanager
    def scope():
        s = factory()
        try:
            yield s
            s.commit()
        finally:
            s.close()

    for target in ("paylens.storage.db.engine",
                   "paylens.realdata.loader.engine"):
        monkeypatch.setattr(target, engine)
    monkeypatch.setattr("paylens.realdata.loader.session_scope", scope)
    # bypass the checksum gate for fixture data (gate itself is tested below)
    monkeypatch.setattr("paylens.realdata.loader.verify_source",
                        lambda raw_dir, sid: {"source": sid, "files": []})
    return engine


# ------------------------------------------------------------------- olist mapping


def test_brt_to_utc_and_regions():
    import datetime as dt

    brt_ts = dt.datetime(2018, 8, 20, 9, 0)
    utc = _brt_to_utc(brt_ts)
    assert utc == dt.datetime(2018, 8, 20, 12, 0)
    assert BRT == dt.timedelta(hours=-3)
    assert STATE_TO_REGION["SP"] == "SE"
    assert STATE_TO_REGION["CE"] == "NE"


def test_olist_contract_accepts_and_rejects():
    ok = OlistOrderRow.model_validate({
        "order_id": "o1", "customer_id": "c1", "order_status": "delivered",
        "order_purchase_timestamp": "2018-08-20 09:00:00",
        "order_approved_at": None,
    })
    assert ok.order_approved_at is None
    with pytest.raises(Exception):
        OlistOrderRow.model_validate({
            "order_id": "o1", "customer_id": "c1", "order_status": "teleported",
            "order_purchase_timestamp": "2018-08-20 09:00:00",
        })


def test_load_olist_mapping_and_store(isolated_engine, olist_raw):
    from sqlalchemy import select

    from paylens.realdata.loader import load_olist
    from paylens.storage.models import PaymentAttempt, PaymentEventRecord

    prov = load_olist(olist_raw)
    assert prov["stats"]["orders"] == 4
    assert prov["stats"]["invalid"] == 0
    assert all(v["rejected"] == 0 for v in prov["validation"])

    with isolated_engine.connect() as conn:
        rows = {r[0]: r for r in conn.execute(
            select(PaymentAttempt.attempt_id, PaymentAttempt.status,
                   PaymentAttempt.settled, PaymentAttempt.amount_paise,
                   PaymentAttempt.method, PaymentAttempt.gateway,
                   PaymentAttempt.customer_tier)).fetchall()}

        o1 = rows["o1"]
        assert o1[1] == "succeeded" and o1[2] is True          # delivered -> settled success
        assert o1[3] == round((100.00 + 18.50) * 100)          # split payment summed to minor units
        assert o1[4] == "credit_card" and o1[5] == "SP"        # primary type, second dim = state
        assert rows["o2"][1] == "failed" and rows["o2"][2] is True
        assert rows["o3"][1] != "succeeded" and rows["o3"][2] is False   # shipped -> pending
        assert rows["o4"][1] == "failed"                        # unavailable -> failure
        assert rows["o1"][6] == "SE"                            # real Brazilian region tier

        events = conn.execute(
            select(PaymentEventRecord.event_type)
            .where(PaymentEventRecord.attempt_id == "o1")
            .order_by(PaymentEventRecord.ts)
        ).scalars().all()
        assert events == ["payment_started", "method_selected",
                          "payment_initiated", "gateway_response", "payment_succeeded"]
        # canceled order: no approval stage, terminal pinned to last observed time
        ev2 = conn.execute(
            select(PaymentEventRecord.event_type)
            .where(PaymentEventRecord.attempt_id == "o2")
        ).scalars().all()
        assert "payment_initiated" not in ev2 and "gateway_response" not in ev2
        assert ev2[-1] == "payment_failed"


def test_load_olist_is_idempotent(isolated_engine, olist_raw):
    from sqlalchemy import func, select

    from paylens.realdata.loader import load_olist
    from paylens.storage.models import PaymentAttempt, PaymentEventRecord

    first = load_olist(olist_raw)
    second = load_olist(olist_raw)  # fresh store each run, identical outcome
    assert first["stats"]["orders"] == second["stats"]["orders"] == 4
    with isolated_engine.connect() as conn:
        n_attempts = conn.execute(select(func.count())
                                  .select_from(PaymentAttempt)).scalar()
        n_events = conn.execute(select(func.count())
                                .select_from(PaymentEventRecord)).scalar()
    assert n_attempts == 4
    assert n_events == first["stats"]["events_ingested"]


# ---------------------------------------------------------------- verification gate


def test_verify_source_rejects_checksum_mismatch(tmp_path):
    (tmp_path / "olist_orders_dataset.csv").write_text("corrupted\n", encoding="utf-8")
    with pytest.raises(SourceVerificationError):
        verify_source(tmp_path, "olist")


def test_verify_source_rejects_missing_file(tmp_path):
    with pytest.raises(SourceVerificationError):
        verify_source(tmp_path, "olist")


# ---------------------------------------------------------------- upworthy readouts


def _arm(idx, impressions, clicks, headline="h"):
    return UpworthyRow(package_index=idx, clickability_test_id="t", headline=headline,
                       impressions=impressions, clicks=clicks)


def test_upworthy_multi_arm_bonferroni():
    from paylens.realdata.upworthy import _readout

    # 3 arms, balanced SRM; treatment clearly beats control
    arms = [_arm(0, 100_000, 2_000), _arm(1, 100_000, 3_500), _arm(2, 100_000, 2_100)]
    rx = _readout("test-multi", arms)
    assert rx["arms"] == 3
    assert rx["srm"]["ok"] is True
    assert rx["effect"]["bonferroni_comparisons"] == 2
    assert rx["effect"]["delta_pp"] > 0
    # corrected p = raw p * 2 (still tiny here)
    raw = float(rx["effect"]["p_value_uncorrected"])
    assert float(rx["effect"]["p_value"]) == pytest.approx(min(1.0, raw * 2), rel=0.05)
    assert rx["decision"].startswith("exploratory")


def test_upworthy_negative_effect_holds():
    from paylens.realdata.upworthy import _readout

    arms = [_arm(0, 50_000, 5_000), _arm(1, 50_000, 3_000)]
    rx = _readout("test-neg", arms)
    assert rx["effect"]["delta_pp"] < 0 and rx["effect"]["significant"] is True
    assert rx["decision"].startswith("hold")


def test_upworthy_srm_failure_invalidates():
    from paylens.realdata.upworthy import _readout

    arms = [_arm(0, 100_000, 2_000), _arm(1, 10_000, 300)]  # 10:1 imbalance
    rx = _readout("test-srm", arms)
    assert rx["srm"]["ok"] is False
    assert rx["decision"].startswith("invalid")


def test_upworthy_insignificant_inconclusive():
    from paylens.realdata.upworthy import _readout

    arms = [_arm(0, 2_000, 100), _arm(1, 2_000, 104)]
    rx = _readout("test-insig", arms)
    assert rx["effect"]["significant"] is False
    assert rx["decision"] == "inconclusive"
