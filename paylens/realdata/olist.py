"""Olist adapter: real marketplace orders -> PayLens canonical event stream.

Mapping contract (documented, deterministic, lossless where it matters):

* one **attempt** = one Olist order; ``amount`` = sum of the order's
  payment rows in minor currency units (centavos), ``currency = BRL``;
* ``method``    = the order's primary payment_type (lowest payment_sequential);
* ``gateway``   = the customer's Brazilian state. Olist exposes no payment
  gateway, so the store's second segment dimension is populated with the
  closest available routing region; reports using real data present this
  column as "region", not "gateway";
* ``geo``       = customer_state; ``customer_tier`` = Brazilian macro-region;
* canonical funnel stages (UTC, source timestamps are BRT = UTC-3):
    started / method_selected  @ order_purchase_timestamp
    initiated / gateway_response @ order_approved_at
      latency_ms = approval lag in ms (None when approval precedes purchase -
      a real data quirk in Olist);
    terminal:
      delivered        -> PAYMENT_SUCCEEDED @ order_delivered_customer_date
      canceled/unavailable -> PAYMENT_FAILED @ last observed event time
        (Olist publishes no cancel timestamp; pinned to last known event -
        documented limitation, never inferred as a payment-time failure);
    everything else (created/approved/invoiced/processing/shipped) emits NO
    terminal event -> real pending share at the data horizon;
* ``event_id`` is a SHA-1 of (attempt, event_type, ts) so re-running the
  adapter yields identical, dedup-safe events;
* delivered-carrier milestones are folded out: the stage model is
  payments-shaped and carrier handoff has no canonical stage.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta

from paylens.events.schemas import EventType
from paylens.realdata.contracts import (
    OlistCustomerRow,
    OlistOrderRow,
    OlistPaymentRow,
    ValidationReport,
    parse_file,
)

BRT = timedelta(hours=-3)  # source local time offset (see module caveat)

BRAZILIAN_REGIONS = {
    "N": {"AC", "AP", "AM", "PA", "RO", "RR", "TO"},
    "NE": {"AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"},
    "CO": {"DF", "GO", "MS", "MT"},
    "SE": {"ES", "MG", "RJ", "SP"},
    "S": {"PR", "RS", "SC"},
}
STATE_TO_REGION = {s: r for r, states in BRAZILIAN_REGIONS.items() for s in states}


def _brt_to_utc(dt: datetime | None) -> datetime | None:
    return None if dt is None else (dt - BRT).replace(tzinfo=None)


def _event_id(attempt_id: str, event_type: EventType, ts: datetime) -> str:
    basis = f"{attempt_id}:{event_type.value}:{ts.isoformat()}"
    return hashlib.sha1(basis.encode()).hexdigest()[:32]


def load_dimensions(raw_dir) -> tuple[ValidationReport, ValidationReport, dict, dict]:
    """Silver pass over payments and customers (small dimension tables)."""
    pay_report = ValidationReport("olist_order_payments_dataset.csv")
    by_order: dict[str, list[OlistPaymentRow]] = {}
    for row in parse_file(raw_dir / "olist_order_payments_dataset.csv",
                          OlistPaymentRow, pay_report):
        by_order.setdefault(row.order_id, []).append(row)

    cust_report = ValidationReport("olist_customers_dataset.csv")
    state_by_customer: dict[str, str] = {}
    for row in parse_file(raw_dir / "olist_customers_dataset.csv",
                          OlistCustomerRow, cust_report):
        state_by_customer[row.customer_id] = row.customer_state
    return pay_report, cust_report, by_order, state_by_customer


def iter_order_events(raw_dir, limit_orders: int | None = None):
    """Bronze -> silver: yields (attempt_meta, canonical_event_dicts) per order."""
    pay_report, cust_report, by_order, state_by_customer = load_dimensions(raw_dir)

    order_report = ValidationReport("olist_orders_dataset.csv")
    emitted = 0
    for row in parse_file(raw_dir / "olist_orders_dataset.csv", OlistOrderRow,
                          order_report,
                          datetime_fields=("order_purchase_timestamp",
                                           "order_approved_at",
                                           "order_delivered_carrier_date",
                                           "order_delivered_customer_date",
                                           "order_estimated_delivery_date")):
        if limit_orders is not None and emitted >= limit_orders:
            break
        emitted += 1

        payments = sorted(by_order.get(row.order_id, []),
                          key=lambda p: p.payment_sequential)
        primary = payments[0] if payments else None
        amount_minor = round(sum(p.payment_value for p in payments) * 100)
        state = state_by_customer.get(row.customer_id, "XX")

        purchase = _brt_to_utc(row.order_purchase_timestamp)
        approved = _brt_to_utc(row.order_approved_at)
        delivered = _brt_to_utc(row.order_delivered_customer_date)

        attempt_id = row.order_id
        customer_id = f"olist-{row.customer_id}"
        region = STATE_TO_REGION.get(state, "unknown")

        def ev(t: EventType, ts: datetime | None, **extra) -> dict | None:
            if ts is None:
                return None
            base = {
                "event_id": _event_id(attempt_id, t, ts),
                "event_type": t.value,
                "ts": ts,
                "attempt_id": attempt_id,
                "txn_id": f"{attempt_id}-p{primary.payment_sequential}"
                          if primary else attempt_id,
                "pseudo_customer_id": customer_id,
                "method": primary.payment_type if primary else "not_defined",
                "gateway": state,          # second segment dimension (see docstring)
                "bank": "",
                "amount_paise": max(1, amount_minor),
                "currency": "BRL",
                "app_version": "olist-marketplace",
                "geo": state,
                "customer_tier": region,
                "is_duplicate_charge": False,
            }
            base.update(extra)
            return base

        events = []
        started = ev(EventType.PAYMENT_STARTED, purchase)
        if started:
            events.append(started)
            method_ev = ev(EventType.METHOD_SELECTED, purchase)
            if method_ev:
                events.append(method_ev)
        if approved is not None:
            latency_ms = None
            if purchase is not None and approved >= purchase:
                latency_ms = int((approved - purchase).total_seconds() * 1000)
            init = ev(EventType.PAYMENT_INITIATED, approved)
            resp = ev(EventType.GATEWAY_RESPONSE, approved,
                      status="approved", latency_ms=latency_ms)
            if init:
                events.append(init)
            if resp:
                events.append(resp)

        # terminal outcome (see docstring: censoring-safe pending semantics)
        if row.order_status == "delivered":
            terminal_ts = delivered or approved or purchase
            term = ev(EventType.PAYMENT_SUCCEEDED, terminal_ts)
            if term:
                events.append(term)
        elif row.order_status in ("canceled", "unavailable"):
            last_known = max(x for x in (purchase, approved, delivered) if x)
            term = ev(EventType.PAYMENT_FAILED, last_known,
                      error_code=f"order_{row.order_status}")
            if term:
                events.append(term)

        attempt_meta = {
            "attempt_id": attempt_id,
            "customer_id": customer_id,
            "method": primary.payment_type if primary else "not_defined",
            "gateway": state,
            "region": region,
            "status": row.order_status,
            "amount_minor": amount_minor,
        }
        yield attempt_meta, events

    yield {"_reports": [order_report.summary(), pay_report.summary(),
                        cust_report.summary()]}, None
