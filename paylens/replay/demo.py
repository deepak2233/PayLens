"""Finite, reproducible public scenarios passed through canonical event ingest."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import cache
from pathlib import Path
from random import Random
from tempfile import TemporaryDirectory
from threading import Lock

from sqlalchemy.orm import Session

from paylens.replay.experiment import ExperimentProtocol
from paylens.storage.db import make_engine
from paylens.storage.ingest import IngestConsumer
from paylens.storage.models import Base, DataWatermark, DeploymentRecordDB, ExperimentProtocolRecord

ANCHOR = datetime(2026, 11, 20, tzinfo=timezone.utc)
EXPERIMENT_ID = "checkout-v2"
SCENARIOS = {
    "incident": "Gateway regression",
    "mix": "Traffic-mix shift",
    "delay": "Delayed confirmations",
}
STREAMS = ("payments", "assignments", "guardrails")
_DIRECTORY = TemporaryDirectory(prefix="paylens-replay-")
_LOCKS = {name: Lock() for name in (*SCENARIOS, "experiment")}


def protocol() -> ExperimentProtocol:
    return ExperimentProtocol(
        experiment_id=EXPERIMENT_ID,
        enrollment_start=ANCHOR - timedelta(days=8), enrollment_end=ANCHOR - timedelta(days=5),
        outcome_hours=24, treatment_share=0.5, min_users_per_arm=750, min_effect_pp=0.5,
        timeout_margin_pp=1.5, duplicate_charge_margin_pp=0.5, refund_margin_pp=0.6,
    )


def _payment(key: str, at: datetime, gateway: str, method: str, state: str,
             terminal_delay: int, customer: str | None = None) -> list[dict]:
    base = {
        "attempt_id": key, "txn_id": key, "pseudo_customer_id": customer or key, "method": method,
        "gateway": gateway, "amount_paise": 25_000, "currency": "INR",
    }
    steps = [("payment_started", 0), ("method_selected", 1), ("payment_initiated", 2)]
    if state != "pending":
        steps.append(("payment_succeeded" if state == "success" else "payment_failed", terminal_delay))
    return [
        {**base, "event_id": f"{key}-{idx}", "event_type": stage, "ts": at + timedelta(seconds=sec),
         "error_code": "GATEWAY_TIMEOUT" if stage == "payment_failed" and state == "timeout" else None}
        for idx, (stage, sec) in enumerate(steps)
    ]


def incident_events(name: str) -> tuple[list[dict], list[dict]]:
    rng = Random(904)
    events = []
    changes = []
    start = ANCHOR - timedelta(days=5)
    change_at = ANCHOR - timedelta(hours=14)
    if name in {"incident", "delay"}:
        changes.append({"change_id": "CHG-8841" if name == "incident" else "CHG-9072",
                        "service": "gateway adapter" if name == "incident" else "confirmation worker",
                        "gateway": "Gateway B", "deployed_at": change_at,
                        "note": "Synthetic deployment marker"})
    for hour in range(120):
        for unit in range(60):
            idx = hour * 60 + unit
            at = start + timedelta(hours=hour, seconds=60 * unit + 5)
            card = rng.random() < 0.20
            route = rng.random()
            draw = rng.random()
            slow = rng.random()
            gateway_b_share = (0.71 if at >= ANCHOR - timedelta(hours=24) else 0.22) if name == "mix" else 0.57
            gateway = "Gateway C" if card else "Gateway B" if route < gateway_b_share else "Gateway A"
            method = "card" if card else "upi"
            base_rate = 0.76 if name == "mix" and gateway == "Gateway B" else 0.935
            degradation = gateway == "Gateway B" and at >= change_at
            success_rate = base_rate - (0.31 if name == "incident" and degradation else 0)
            state = "success" if draw < success_rate else "timeout" if gateway == "Gateway B" else "fail"
            delay = 7200 if name == "delay" and degradation and slow < 0.55 else 30
            events.extend(_payment(f"{name}-{idx}", at, gateway, method, state, delay))
    # Exercise duplicates and event-time ordering without expanding the cohort.
    events.extend(events[7:63:5])
    rng.shuffle(events)
    return events, changes


def experiment_events(protocol_: ExperimentProtocol) -> tuple[list[dict], list[dict]]:
    rng = Random(428)
    events, exposures = [], []
    for variant in ("A", "B"):
        for i in range(1_600):
            customer = f"exp-{variant}-{i}"
            at = protocol_.enrollment_start + timedelta(seconds=i * 125)
            exposures.append({"event_id": f"assign-{customer}", "ts": at, "assigned_at": at,
                              "experiment_id": protocol_.experiment_id, "variant": variant,
                              "pseudo_customer_id": customer})
            outcome = rng.random()
            if outcome >= 0.92:
                continue  # assigned with zero observed attempts
            state = "success" if outcome < (0.69 if variant == "A" else 0.75) else "fail"
            events.extend(_payment(customer, at + timedelta(seconds=30), "Gateway A", "upi", state, 30, customer))
    return events, exposures


@cache
def _build(name: str):
    engine = make_engine(f"sqlite:///{Path(_DIRECTORY.name) / (name + '.db')}")
    Base.metadata.create_all(engine)
    with Session(engine) as session, session.begin():
        if name == "experiment":
            spec = protocol()
            events, exposures = experiment_events(spec)
            IngestConsumer(None, session).process_records(events, exposures)
            session.add(ExperimentProtocolRecord(experiment_id=EXPERIMENT_ID,
                                                 policy=spec.model_dump(mode="json"),
                                                 registered_at=spec.enrollment_start - timedelta(days=1)))
        else:
            events, changes = incident_events(name)
            IngestConsumer(None, session).process_records(events, [])
            session.add_all(DeploymentRecordDB(**change) for change in changes)
        session.add_all(DataWatermark(stream=stream, complete_through=ANCHOR) for stream in STREAMS)
    return engine


def demo_engine(name: str):
    with _LOCKS[name]:
        return _build(name)
