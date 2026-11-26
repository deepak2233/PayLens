"""Event schema definitions.

Every event published to Kafka (or the in-memory demo bus) is validated
against these Pydantic models before it touches the database. Invalid events
are dead-lettered, never silently coerced.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class EventType(str, Enum):
    PAYMENT_STARTED = "payment_started"
    METHOD_SELECTED = "method_selected"
    PAYMENT_INITIATED = "payment_initiated"
    GATEWAY_RESPONSE = "gateway_response"
    PAYMENT_SUCCEEDED = "payment_succeeded"
    PAYMENT_FAILED = "payment_failed"
    REFUND_ISSUED = "refund_issued"
    EXPERIMENT_EXPOSURE = "experiment_exposure"


class PaymentEvent(BaseModel):
    """One lifecycle event of one payment attempt.

    Categorical values (method, gateway, geo) are validated as bounded
    strings, not fixed Literals: the demo store uses the synthetic
    vocabulary (upi/debit_card/credit_card/netbanking) while real-source
    adapters (paylens.realdata) carry their own vocabularies (e.g. Olist
    payment types). Structure is strict; vocabulary is producer-owned and
    every value is bound as a SQL parameter downstream.
    """

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()), min_length=1, max_length=64)
    event_type: EventType
    ts: datetime = Field(default_factory=_utcnow)
    attempt_id: str = Field(min_length=1, max_length=64)
    txn_id: str = Field(min_length=1, max_length=64)
    pseudo_customer_id: str = Field(min_length=1, max_length=64)
    method: str = Field(min_length=1, max_length=32)  # demo: upi/debit_card/...
    gateway: str = Field(min_length=1, max_length=64)  # e.g. "gateway-a"
    bank: str = Field("", max_length=30)
    amount_paise: int = Field(gt=0, le=9_223_372_036_854_775_807)
    currency: str = Field("INR", max_length=8)
    app_version: str = Field("3.2.0", max_length=20)
    geo: str = Field("IN-MH", max_length=12)
    customer_tier: str = Field("retail", max_length=12)
    status: Optional[str] = Field(None, max_length=20)
    error_code: Optional[str] = Field(None, max_length=40)
    # negative latencies are clock-skew artifacts, never real signals: the
    # contract rejects them so they are dead-lettered, not stored
    latency_ms: Optional[int] = Field(default=None, ge=0, le=2_147_483_647)
    is_duplicate_charge: bool = False

    model_config = {"frozen": True}


class ExperimentExposure(BaseModel):
    """A user assignment to an A/B experiment arm.

    ``variant`` is a bounded string: the demo uses A/B, real sources may
    use 2..k named arms.
    """

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()), min_length=1, max_length=64)
    ts: datetime = Field(default_factory=_utcnow)
    experiment_id: str = Field(min_length=1, max_length=64)
    variant: str = Field(min_length=1, max_length=32)
    pseudo_customer_id: str = Field(min_length=1, max_length=64)
    assigned_at: datetime = Field(default_factory=_utcnow)

    model_config = {"frozen": True}


class DeploymentRecord(BaseModel):
    """Infrastructure change record used by the Anomaly Agent as a correlate."""

    change_id: str
    service: str
    gateway: str = ""
    deployed_at: datetime
    note: str = ""

    model_config = {"frozen": True}


# Stage ordering used for out-of-order-safe status derivation.
STAGE_RANK = {
    "started": 0,
    "method_selected": 1,
    "initiated": 2,
    "gateway_response": 3,
    "terminal": 4,
    "post_terminal": 4,  # refunds etc.: recorded, but never changes funnel state
}

EVENT_TO_STAGE = {
    EventType.PAYMENT_STARTED: "started",
    EventType.METHOD_SELECTED: "method_selected",
    EventType.PAYMENT_INITIATED: "initiated",
    EventType.GATEWAY_RESPONSE: "gateway_response",
    EventType.PAYMENT_SUCCEEDED: "terminal",
    EventType.PAYMENT_FAILED: "terminal",
    EventType.REFUND_ISSUED: "post_terminal",
    EventType.EXPERIMENT_EXPOSURE: "post_terminal",
}


def validate_event(raw: dict) -> PaymentEvent:
    """Validate a raw dict (e.g. a Kafka message value) into a PaymentEvent."""
    return PaymentEvent.model_validate(raw)
