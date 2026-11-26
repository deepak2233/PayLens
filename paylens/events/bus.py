"""Event transport abstraction.

The demo layer runs on an :class:`InMemoryBus` (a buffered, partitioned
queue emulator). The production layer swaps in :class:`KafkaBus`, which
talks to a real Apache Kafka cluster. Both implement the same tiny
interface so the ingest consumer is transport-agnostic.
"""
from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import batched
from typing import Callable, Iterable

from paylens.config import SETTINGS

TOPIC_PAYMENTS = "payment-events"
TOPIC_EXPOSURES = "experiment-exposures"

MessageHandler = Callable[[str, dict], None]


@dataclass
class InMemoryBus:
    """Minimal in-memory stand-in for Kafka used by the demo layer and tests.

    Messages are buffered per topic; consumers drain them with explicit
    offset tracking, mirroring Kafka's consumer-group semantics closely
    enough for the demo to be a faithful behavioural model.
    """

    _buffers: dict[str, list[dict]] = field(default_factory=lambda: defaultdict(list))
    _offsets: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def publish(self, topic: str, message: dict) -> None:
        self._buffers[topic].append(message)

    def publish_many(self, topic: str, messages: Iterable[dict]) -> int:
        n = 0
        for m in messages:
            self.publish(topic, m)
            n += 1
        return n

    def drain(self, topic: str, limit: int = 100_000) -> list[dict]:
        """Read unseen messages from a topic, advancing the consumer offset."""
        buf = self._buffers[topic]
        off = self._offsets[topic]
        batch = buf[off : off + limit]
        self._offsets[topic] = off + len(batch)
        return batch

    def lag(self, topic: str) -> int:
        return len(self._buffers[topic]) - self._offsets[topic]


class KafkaBus:
    """Production transport (requires ``kafka-python``, only used in Docker layer)."""

    def __init__(self, bootstrap: str | None = None) -> None:
        from kafka import KafkaProducer  # imported lazily: demo layer has no kafka dep

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap or SETTINGS.kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=5,
            linger_ms=20,
            **SETTINGS.kafka_security,
        )

    def publish(self, topic: str, message: dict) -> None:
        self._send(topic, message).get(timeout=60)

    def _send(self, topic: str, message: dict):
        key = message.get("attempt_id") or message["pseudo_customer_id"]
        return self.producer.send(topic, message, key=key.encode("utf-8"))

    def publish_many(self, topic: str, messages: Iterable[dict]) -> int:
        n = 0
        for batch in batched(messages, 500):
            futures = [self._send(topic, message) for message in batch]
            for future in futures:
                future.get(timeout=60)
            n += len(batch)
        return n


def make_bus() -> InMemoryBus | KafkaBus:
    """Factory honouring PAYLENS_EVENT_BUS (memory | kafka)."""
    if SETTINGS.event_bus == "kafka":
        return KafkaBus()
    return InMemoryBus()
