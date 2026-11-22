"""Silver-layer data contracts for real sources.

Raw source rows are validated against these Pydantic models BEFORE any
mapping happens. Rows that fail are dead-lettered (counted, sampled into
the run provenance) and never reach the store - the same fail-closed gate
the Kafka path applies to streaming events.
"""
from __future__ import annotations

import csv
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import ClassVar

from pydantic import BaseModel, ConfigDict, field_validator


class RawRow(BaseModel):
    """Base for raw source rows: forbid surprises, tolerate blank strings."""
    model_config = ConfigDict(str_strip_whitespace=True)

    @field_validator("*", mode="before")
    @classmethod
    def _blank_to_none(cls, v):
        if isinstance(v, str) and v.strip() == "":
            return None
        return v


class OlistOrderRow(RawRow):
    order_id: str
    customer_id: str
    order_status: str
    order_purchase_timestamp: datetime
    order_approved_at: datetime | None = None
    order_delivered_carrier_date: datetime | None = None
    order_delivered_customer_date: datetime | None = None
    order_estimated_delivery_date: datetime | None = None

    KNOWN_STATUSES: ClassVar[set[str]] = {"created", "approved", "invoiced",
                                          "processing", "shipped", "delivered",
                                          "canceled", "unavailable"}

    @field_validator("order_status")
    @classmethod
    def _known_status(cls, v: str) -> str:
        if v not in cls.KNOWN_STATUSES:
            raise ValueError(f"unknown order_status: {v!r}")
        return v


class OlistPaymentRow(RawRow):
    order_id: str
    payment_sequential: int
    payment_type: str
    payment_installments: int
    payment_value: float

    KNOWN_TYPES: ClassVar[set[str]] = {"credit_card", "boleto", "voucher",
                                       "debit_card", "not_defined"}

    @field_validator("payment_type")
    @classmethod
    def _known_type(cls, v: str) -> str:
        if v not in cls.KNOWN_TYPES:
            raise ValueError(f"unknown payment_type: {v!r}")
        return v

    @field_validator("payment_value")
    @classmethod
    def _non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError("negative payment_value")
        return v


class OlistCustomerRow(RawRow):
    customer_id: str
    customer_unique_id: str
    customer_zip_code_prefix: str
    customer_city: str
    customer_state: str

    @field_validator("customer_state")
    @classmethod
    def _br_state(cls, v: str) -> str:
        if len(v) != 2 or not v.isalpha() or not v.isupper():
            raise ValueError(f"malformed customer_state: {v!r}")
        return v


class UpworthyRow(RawRow):
    package_index: int
    clickability_test_id: str
    headline: str
    impressions: int
    clicks: int

    @field_validator("clicks")
    @classmethod
    def _clicks_within(cls, v: int, info) -> int:
        impressions = info.data.get("impressions")
        if impressions is not None and v > impressions:
            raise ValueError("clicks exceed impressions")
        return v


class ValidationReport:
    """Counts and dead-letter samples for one parsed file."""

    def __init__(self, name: str, dead_letter_sample: int = 5) -> None:
        self.name = name
        self.rows_read = 0
        self.accepted = 0
        self.reject_reasons: Counter[str] = Counter()
        self.dead_letter: list[dict] = []
        self._sample_size = dead_letter_sample

    def accept(self, row: dict) -> None:
        self.rows_read += 1
        self.accepted += 1
        row["_file"] = self.name
        row["_row"] = self.rows_read

    def reject(self, row: dict, reason: str) -> None:
        self.rows_read += 1
        self.reject_reasons[reason.split(":")[0].strip()] += 1
        if len(self.dead_letter) < self._sample_size:
            self.dead_letter.append({"row_number": self.rows_read,
                                     "reason": reason, "raw": row})

    def summary(self) -> dict:
        return {
            "file": self.name,
            "rows_read": self.rows_read,
            "accepted": self.accepted,
            "rejected": self.rows_read - self.accepted,
            "reject_reasons": dict(self.reject_reasons),
            "dead_letter_sample": self.dead_letter,
        }


def _parse_datetime(value):
    if value is None:
        return None
    return datetime.fromisoformat(str(value))


def parse_file(path: Path, model: type[RawRow], report: ValidationReport,
               datetime_fields: tuple[str, ...] = ()):
    """Stream one raw CSV through its contract; yields validated model rows."""
    with path.open(newline="", encoding="utf-8") as f:
        for raw in csv.DictReader(f):
            try:
                for field_name in datetime_fields:
                    if raw.get(field_name):
                        raw[field_name] = _parse_datetime(raw[field_name])
                for int_field in ("payment_sequential", "payment_installments",
                                  "impressions", "clicks", "payment_value"):
                    if raw.get(int_field):
                        raw[int_field] = float(raw[int_field]) \
                            if int_field == "payment_value" else int(raw[int_field])
                row = model.model_validate(raw)
            except Exception as exc:
                report.reject(raw, f"{type(exc).__name__}: {exc}")
                continue
            report.accept(raw)
            yield row
