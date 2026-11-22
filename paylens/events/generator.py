"""Synthetic payment-event generator.

Produces a realistic two-day payment dataset with an injected incident that
matches the PayLens reference scenario:

  * Day 1 (yesterday): healthy traffic, overall success rate ~92%.
  * Day 2 (today)    : from 10:15 onwards, UPI attempts routed through
    Gateway B suffer a timeout storm after an infrastructure deployment.
    Overall success rate drops to ~82%, concentrated in UPI x Gateway B.
  * An A/B checkout experiment (arm A ~80% vs arm B ~82%) runs on Day 0.
  * Data-quality realism: ~0.3% duplicate event ids, ~1% out-of-order
    publishes, and ~2% of late-evening attempts whose terminal outcome
    settles after the window closes (delayed outcomes).

Everything is seeded and anchored so runs are reproducible.
"""
from __future__ import annotations

import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterator

from paylens.events.schemas import DeploymentRecord, EventType, ExperimentExposure, PaymentEvent

# ---------------------------------------------------------------------------
# Calibration constants (tuned so day-1 ~= 92% and day-2 ~= 82%)
# ---------------------------------------------------------------------------
METHOD_WEIGHTS = {"upi": 0.60, "debit_card": 0.15, "credit_card": 0.10, "netbanking": 0.15}
UPI_GATEWAYS = [("gateway-a", 0.55), ("gateway-b", 0.45)]
METHOD_GATEWAY = {
    "upi": UPI_GATEWAYS,
    "debit_card": [("gateway-a", 1.0)],
    "credit_card": [("gateway-a", 1.0)],
    "netbanking": [("gateway-c", 1.0)],
}
BASELINE_SUCCESS = {  # day-1 / healthy probability of eventual success
    "upi": 0.915,
    "debit_card": 0.945,
    "credit_card": 0.945,
    "netbanking": 0.885,
}
UPI_BANKS = [("HDFC", 0.30), ("ICICI", 0.25), ("SBI", 0.25), ("Axis", 0.12), ("Kotak", 0.08)]
GEOS = [("IN-MH", 0.30), ("IN-KA", 0.25), ("IN-DL", 0.20), ("IN-TN", 0.15), ("IN-WB", 0.10)]
TIERS = [("retail", 0.80), ("premium", 0.15), ("vip", 0.05)]
APP_VERSIONS = [("3.2.0", 0.85), ("3.1.0", 0.10), ("3.3.0-beta", 0.05)]

# Failure reasons for healthy traffic
DECLINE_CODES = [
    ("INSUFFICIENT_FUNDS", 0.35),
    ("BANK_DECLINE", 0.30),
    ("AUTH_FAILURE", 0.15),
    ("CUSTOMER_ABORTED", 0.20),
]

# The injected incident on day 2
INCIDENT = {
    "method": "upi",
    "gateway": "gateway-b",
    "start_hour": 10.25,  # 10:15 local
    "timeout_prob": 0.45,  # share of gateway responses that time out
    "timeout_recovery": 0.12,  # share of timeouts that still succeed (late retry)
}

EXPERIMENT_ID = "exp-2026-11-checkout-upi-v2"

_HOURLY_TRAFFIC = [
    0.35, 0.25, 0.20, 0.20, 0.25, 0.40, 0.70, 1.00, 1.25, 1.40,
    1.45, 1.40, 1.35, 1.30, 1.25, 1.20, 1.25, 1.40, 1.60, 1.55,
    1.30, 1.00, 0.70, 0.50,
]


def _weighted(rng: random.Random, pairs) -> str:
    r = rng.random()
    acc = 0.0
    for value, w in pairs:
        acc += w
        if r <= acc:
            return value
    return pairs[-1][0]


def _amount_paise(rng: random.Random, method: str) -> int:
    if method == "upi":
        base = rng.choice([99, 149, 249, 499, 999, 1999, 4999])
    elif method in ("debit_card", "credit_card"):
        base = rng.choice([999, 2499, 4999, 9999, 19999])
    else:
        base = rng.choice([5000, 10000, 25000, 50000, 100000])
    return base * 100


@dataclass
class GeneratedData:
    payment_events: list[dict] = field(default_factory=list)
    exposures: list[dict] = field(default_factory=list)
    deployments: list[dict] = field(default_factory=list)


# NOTE: latency draws are clamped at 0 - the event contract rejects
# negative latencies (clock-skew artifacts are dead-lettered, not stored).
class PaymentEventGenerator:
    """Seeded generator for organic traffic, the A/B experiment and deployments."""

    def __init__(
        self,
        anchor: datetime | None = None,
        attempts_per_day: int = 50_000,
        experiment_users_per_arm: int = 50_000,
        seed: int = 42,
    ) -> None:
        self.anchor = (anchor or datetime.now(timezone.utc)).astimezone(timezone.utc)
        self.day1 = (self.anchor - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
        self.day2 = (self.anchor - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        self.day0 = (self.anchor - timedelta(days=3)).replace(hour=0, minute=0, second=0, microsecond=0)
        self.attempts_per_day = attempts_per_day
        self.experiment_users_per_arm = experiment_users_per_arm
        self.rng = random.Random(seed)
        self.data = GeneratedData()
        self._customer_seq = 0

    # ------------------------------------------------------------------ utils
    def _customer(self, prefix: str = "c") -> str:
        self._customer_seq += 1
        return f"{prefix}-{self._customer_seq:07d}"

    def _hour_ts(self, day: datetime, hour_float: float) -> datetime:
        h = int(hour_float)
        m = int((hour_float - h) * 60)
        return day + timedelta(hours=h, minutes=m, seconds=self.rng.randint(0, 59))

    def _pick_hour(self) -> float:
        weights = _HOURLY_TRAFFIC
        total = sum(weights)
        r = self.rng.random() * total
        acc = 0.0
        for hour, w in enumerate(weights):
            acc += w
            if r <= acc:
                return hour + self.rng.random()
        return 23.0

    # ------------------------------------------------------------- organic day
    def _organic_attempt(self, day: datetime) -> None:
        rng = self.rng
        method = _weighted(rng, list(METHOD_WEIGHTS.items()))
        gateway = _weighted(rng, METHOD_GATEWAY[method])
        bank = _weighted(rng, UPI_BANKS) if method == "upi" else ""
        geo = _weighted(rng, GEOS)
        tier = _weighted(rng, TIERS)
        app_version = _weighted(rng, APP_VERSIONS)
        start_ts = self._hour_ts(day, self._pick_hour())
        attempt_id = str(uuid.UUID(int=rng.getrandbits(128), version=4))
        txn_id = "txn-" + uuid.UUID(int=rng.getrandbits(64)).hex[:16]
        customer = self._customer()
        amount = _amount_paise(rng, method)

        common = dict(
            attempt_id=attempt_id,
            txn_id=txn_id,
            pseudo_customer_id=customer,
            method=method,
            gateway=gateway,
            bank=bank,
            amount_paise=amount,
            app_version=app_version,
            geo=geo,
            customer_tier=tier,
        )

        degraded = (
            day == self.day2
            and method == INCIDENT["method"]
            and gateway == INCIDENT["gateway"]
            and (start_ts.hour + start_ts.minute / 60) >= INCIDENT["start_hour"]
        )

        events: list[tuple[float, dict]] = []
        t = 0.0

        def add(offset: float, **kw) -> None:
            events.append((start_ts + timedelta(seconds=offset), kw))

        add(t, event_type=EventType.PAYMENT_STARTED)
        reached_method = rng.random() < 0.97
        if reached_method:
            t += rng.uniform(2, 20)
            add(t, event_type=EventType.METHOD_SELECTED)
            reached_init = rng.random() < 0.98
            if reached_init:
                t += rng.uniform(3, 25)
                add(t, event_type=EventType.PAYMENT_INITIATED)
                t += rng.uniform(2, 9)

                if degraded:
                    timeout = rng.random() < INCIDENT["timeout_prob"]
                    if timeout:
                        add(t, event_type=EventType.GATEWAY_RESPONSE,
                            status="timeout", error_code="GATEWAY_TIMEOUT",
                            latency_ms=max(0, int(rng.gauss(6000, 900))))
                        recovered = rng.random() < INCIDENT["timeout_recovery"]
                        t += rng.uniform(20, 120)
                        if recovered:
                            add(t, event_type=EventType.GATEWAY_RESPONSE,
                                status="approved", latency_ms=max(0, int(rng.gauss(2600, 400))))
                            add(t, event_type=EventType.PAYMENT_SUCCEEDED)
                        else:
                            add(t, event_type=EventType.PAYMENT_FAILED,
                                error_code="GATEWAY_TIMEOUT")
                    else:
                        add(t, event_type=EventType.GATEWAY_RESPONSE,
                            status="approved", latency_ms=max(0, int(rng.gauss(1400, 300))))
                        ok = rng.random() < BASELINE_SUCCESS[method]
                        if ok:
                            add(t, event_type=EventType.PAYMENT_SUCCEEDED)
                        else:
                            add(t, event_type=EventType.PAYMENT_FAILED,
                                error_code=_weighted(rng, DECLINE_CODES))
                else:
                    add(t, event_type=EventType.GATEWAY_RESPONSE,
                        status="approved", latency_ms=max(0, int(rng.gauss(1400, 300))))
                    ok = rng.random() < BASELINE_SUCCESS[method]
                    if ok:
                        add(t, event_type=EventType.PAYMENT_SUCCEEDED)
                    else:
                        add(t, event_type=EventType.PAYMENT_FAILED,
                            error_code=_weighted(rng, DECLINE_CODES))

        for ts, payload in events:
            ev = PaymentEvent(ts=ts, **common, **payload)
            self.data.payment_events.append(ev.model_dump(mode="json"))

    # -------------------------------------------------------------- experiment
    def _experiment_cohort(self) -> None:
        """Arm A / arm B checkout experiment measured on day 0 (pre-incident)."""
        rng = self.rng
        for variant in ("A", "B"):
            for _ in range(self.experiment_users_per_arm):
                customer = self._customer(prefix="exp")
                assigned_at = self.day0 + timedelta(hours=9, minutes=rng.randint(0, 240))
                self.data.exposures.append(
                    ExperimentExposure(
                        experiment_id=EXPERIMENT_ID,
                        variant=variant,
                        pseudo_customer_id=customer,
                        assigned_at=assigned_at,
                    ).model_dump(mode="json")
                )
                # success probabilities: A 80%, B 82%
                success_p = 0.80 if variant == "A" else 0.82
                start_ts = assigned_at + timedelta(seconds=rng.randint(10, 600))
                attempt_id = str(uuid.UUID(int=rng.getrandbits(128), version=4))
                txn_id = "txn-" + uuid.UUID(int=rng.getrandbits(64)).hex[:16]
                common = dict(
                    attempt_id=attempt_id,
                    txn_id=txn_id,
                    pseudo_customer_id=customer,
                    method="upi",
                    gateway=_weighted(rng, UPI_GATEWAYS),
                    bank=_weighted(rng, UPI_BANKS),
                    amount_paise=_amount_paise(rng, "upi"),
                    geo=_weighted(rng, GEOS),
                    customer_tier=_weighted(rng, TIERS),
                    app_version="3.2.0",
                )
                events: list[tuple[datetime, dict]] = []
                t0 = start_ts
                events.append((t0, dict(event_type=EventType.PAYMENT_STARTED)))
                t0 += timedelta(seconds=rng.uniform(2, 15))
                events.append((t0, dict(event_type=EventType.METHOD_SELECTED)))
                t0 += timedelta(seconds=rng.uniform(2, 12))
                events.append((t0, dict(event_type=EventType.PAYMENT_INITIATED)))
                t0 += timedelta(seconds=rng.uniform(2, 8))
                # producers normalize clock-skew before publishing: the event
                # contract rejects negative latencies (dead-letter), so the
                # generator clamps the gaussian tail at zero
                lat = max(0, int(rng.gauss(1150 if variant == "B" else 1350, 260)))  # producer-side clock-skew clamp
                events.append((t0, dict(event_type=EventType.GATEWAY_RESPONSE,
                                        status="approved", latency_ms=lat)))
                if rng.random() < success_p:
                    events.append((t0, dict(event_type=EventType.PAYMENT_SUCCEEDED)))
                else:
                    events.append((t0, dict(event_type=EventType.PAYMENT_FAILED,
                                            error_code=_weighted(rng, DECLINE_CODES))))
                # rare duplicate charge (guardrail metric)
                if rng.random() < 0.0002:
                    dup = dict(events[-2][1])
                    dup["event_type"] = EventType.PAYMENT_SUCCEEDED
                    dup["is_duplicate_charge"] = True
                    events.append((t0 + timedelta(seconds=45), dup))
                # occasional refunds (guardrail metric)
                if events[-1][1]["event_type"] == EventType.PAYMENT_SUCCEEDED and rng.random() < 0.003:
                    events.append((t0 + timedelta(seconds=rng.randint(3600, 86400)),
                                   dict(event_type=EventType.REFUND_ISSUED)))
                for ts, payload in events:
                    ev = PaymentEvent(ts=ts, **common, **payload)
                    self.data.payment_events.append(ev.model_dump(mode="json"))

    # -------------------------------------------------------------- deployments
    def _deployments(self) -> None:
        self.data.deployments.append(
            DeploymentRecord(
                change_id="CHG-8841",
                service="payment-gateway-adapter",
                gateway=INCIDENT["gateway"],
                deployed_at=self.day2 + timedelta(hours=10, minutes=15),
                note="Gateway B adapter rollout v2.7.0 (connection-pool resize)",
            ).model_dump(mode="json")
        )
        self.data.deployments.append(
            DeploymentRecord(
                change_id="CHG-8839",
                service="android-app",
                deployed_at=self.day1 + timedelta(hours=19),
                note="App release 3.3.0-beta to 5% cohort",
            ).model_dump(mode="json")
        )
        self.data.deployments.append(
            DeploymentRecord(
                change_id="CHG-8835",
                service="payment-gateway-adapter",
                gateway="gateway-a",
                deployed_at=self.day2 + timedelta(hours=3),
                note="TLS certificate rotation (no behaviour change expected)",
            ).model_dump(mode="json")
        )

    # ------------------------------------------------------- realism injections
    def _inject_realism(self) -> None:
        rng = self.rng
        events = self.data.payment_events
        n = len(events)
        # duplicate event ids (~0.3%)
        dup_ids = rng.sample(range(n), int(n * 0.003))
        copies = [dict(events[i]) for i in dup_ids]
        events.extend(copies)
        # out-of-order publishing (~1%): events emitted 30-300s later than authored
        late = rng.sample(range(n), int(n * 0.01))
        for i in late:
            delay = timedelta(seconds=rng.randint(30, 300))
            events[i]["ts"] = (datetime.fromisoformat(events[i]["ts"]) + delay).isoformat()
        rng.shuffle(events)

    # ------------------------------------------------------------------- run
    def generate(self) -> GeneratedData:
        for _ in range(self.attempts_per_day):
            self._organic_attempt(self.day1)
        for _ in range(self.attempts_per_day):
            self._organic_attempt(self.day2)
        self._experiment_cohort()
        self._deployments()
        self._inject_realism()
        return self.data

    def iter_payment_events(self) -> Iterator[dict]:
        yield from self.data.payment_events
