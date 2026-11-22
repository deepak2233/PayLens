"""Bring-your-own-data: CSV uploads flow through the SAME event pipeline.

One row per payment attempt is expanded into the canonical PaymentEvent
stream (started -> method_selected -> initiated -> gateway_response ->
terminal), so the governed ingest consumer - not a special case - derives
attempt state, funnel stages and A/B assignment. Everything downstream
(metrics, agents, governed SQL, audit log) is identical to the demo and
real-source paths.

Window semantics match the governed pipeline: the last 24 hours are the
"current" window, the previous 24 hours the baseline, so a file must span
at least ~46 hours (two full days).
"""
from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path

from paylens.events.schemas import EventType, ExperimentExposure, PaymentEvent

REQUIRED_COLUMNS = ("attempt_id", "started_at")
STAGE_SOURCE_COLUMNS = ("stage_reached", "status")

#: console-facing stage -> how the canonical event stream is expanded
_STAGE_EXPANSION = {
    "started": ("started",),
    "method_selected": ("started", "method_selected"),
    "initiated": ("started", "method_selected", "initiated"),
    "gateway_response": ("started", "method_selected", "initiated", "gateway_response"),
    "succeeded": ("started", "method_selected", "initiated", "gateway_response", "terminal"),
    "failed": ("started", "method_selected", "initiated", "gateway_response", "terminal"),
    "pending": ("started", "method_selected", "initiated", "gateway_response"),
}

_DIMENSION_COLUMNS = ("method", "gateway", "bank", "app_version", "geo", "customer_tier")
_MIN_SPAN_HOURS = 46.0
_MIN_ROWS = 50
_KNOWN_COLUMNS = frozenset(
    REQUIRED_COLUMNS + STAGE_SOURCE_COLUMNS + _DIMENSION_COLUMNS
    + ("experiment_id", "variant", "amount", "currency", "customer_id")
)


class CsvUploadError(ValueError):
    """Raised with an operator-friendly message when a upload is not analysable."""


def read_attempts_csv(source) -> tuple[list[dict], dict]:
    """Read a CSV file/buffer into row dicts.

    ``source`` is a path or a text-iterable (the API endpoint streams the
    uploaded file straight through, no temp copy). Returns (rows, stats).
    """
    stats = {"bytes": 0, "rows": 0, "skipped_header": 0}
    if isinstance(source, (str, Path)):
        with open(source, newline="", encoding="utf-8-sig", errors="replace") as fh:
            return _parse(fh, stats)
    return _parse(source, stats)


def _parse(fh, stats: dict) -> tuple[list[dict], dict]:
    reader = csv.DictReader(fh)
    if not reader.fieldnames:
        raise CsvUploadError("The file is empty.")
    header = [h.strip().lower() for h in reader.fieldnames]
    missing = [c for c in REQUIRED_COLUMNS if c not in header]
    if not any(c in header for c in STAGE_SOURCE_COLUMNS) or missing:
        raise CsvUploadError(
            "Missing required columns "
            f"{missing or []}{'' if any(c in header for c in STAGE_SOURCE_COLUMNS) else ' + stage_reached (or status)'}. "
            f"Columns found: {', '.join(header) or 'none'}"
        )
    reader.fieldnames = header
    rows: list[dict] = []
    for row in reader:
        stats["bytes"] += sum(len(str(v)) for v in row.values() if v) + len(row)
        rows.append({k: (v or "").strip() for k, v in row.items() if k})
    stats["rows"] = len(rows)
    return rows, stats


def _parse_ts(value: str) -> datetime:
    value = value.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    ts = datetime.fromisoformat(value)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def rows_to_events(rows: list[dict]) -> tuple[list[PaymentEvent], list[ExperimentExposure], dict]:
    """Expand one-row-per-attempt CSV rows into canonical event streams.

    Returns (payment_events, exposures, stats). Raises CsvUploadError with an
    operator-friendly message when the data cannot be analysed.
    """
    events: list[PaymentEvent] = []
    exposures: list[ExperimentExposure] = []
    parsed = 0
    for n, row in enumerate(rows):
        raw_stage = (row.get("stage_reached") or row.get("status") or "").lower()
        if raw_stage not in _STAGE_EXPANSION:
            continue
        try:
            ts = _parse_ts(row["started_at"])
        except (KeyError, ValueError):
            continue
        parsed += 1
        attempt_id = row["attempt_id"] or f"csv-{n}"
        attempt_id = attempt_id[:64]
        txn_id = attempt_id
        customer = (row.get("customer_id") or attempt_id)[:64]
        amount = row.get("amount") or ""
        try:
            amount_paise = max(1, round(float(amount) * 100)) if amount else 10_000
        except ValueError:
            amount_paise = 10_000
        dims = {
            "method": (row.get("method") or "unknown")[:32],
            "gateway": (row.get("gateway") or "unknown")[:64],
            "bank": (row.get("bank") or "")[:30],
            "app_version": (row.get("app_version") or "3.2.0")[:20],
            "geo": (row.get("geo") or "IN-MH")[:12],
            "customer_tier": (row.get("customer_tier") or "retail")[:12],
        }
        currency = (row.get("currency") or "INR")[:8]

        for step, stage in enumerate(_STAGE_EXPANSION[raw_stage]):
            ets = ts + timedelta(seconds=step)
            if stage == "started":
                events.append(PaymentEvent(
                    event_type=EventType.PAYMENT_STARTED, ts=ets, attempt_id=attempt_id,
                    txn_id=txn_id, pseudo_customer_id=customer,
                    amount_paise=amount_paise, currency=currency, **dims))
            elif stage == "method_selected":
                events.append(PaymentEvent(
                    event_type=EventType.METHOD_SELECTED, ts=ets, attempt_id=attempt_id,
                    txn_id=txn_id, pseudo_customer_id=customer,
                    amount_paise=amount_paise, currency=currency, **dims))
            elif stage == "initiated":
                events.append(PaymentEvent(
                    event_type=EventType.PAYMENT_INITIATED, ts=ets, attempt_id=attempt_id,
                    txn_id=txn_id, pseudo_customer_id=customer,
                    amount_paise=amount_paise, currency=currency, **dims))
            elif stage == "gateway_response":
                events.append(PaymentEvent(
                    event_type=EventType.GATEWAY_RESPONSE, ts=ets, attempt_id=attempt_id,
                    txn_id=txn_id, pseudo_customer_id=customer, status=raw_stage,
                    amount_paise=amount_paise, currency=currency, **dims))
            else:  # terminal
                etype = EventType.PAYMENT_SUCCEEDED if raw_stage == "succeeded" else EventType.PAYMENT_FAILED
                events.append(PaymentEvent(
                    event_type=etype, ts=ets, attempt_id=attempt_id,
                    txn_id=txn_id, pseudo_customer_id=customer,
                    amount_paise=amount_paise, currency=currency, **dims))

        if row.get("experiment_id") and row.get("variant"):
            exposures.append(ExperimentExposure(
                ts=ts, experiment_id=row["experiment_id"][:64],
                variant=row["variant"][:32], pseudo_customer_id=customer,
                assigned_at=ts))

    if parsed < _MIN_ROWS:
        raise CsvUploadError(
            f"Only {parsed} of {len(rows)} rows parsed into valid attempts "
            f"(need at least {_MIN_ROWS}). Check the template: attempt_id, "
            "started_at (ISO), stage_reached in started/method_selected/"
            "initiated/succeeded/failed/pending.")
    tss = [e.ts for e in events if e.event_type == EventType.PAYMENT_STARTED]
    span_h = (max(tss) - min(tss)).total_seconds() / 3600
    if span_h < _MIN_SPAN_HOURS:
        raise CsvUploadError(
            f"Data spans only {span_h:.1f} h. The analysis compares the last "
            "24 h against the previous 24 h - the file must span at least "
            "~46 hours (two full days).")
    return events, exposures, {"attempts": parsed, "events": len(events),
                               "exposures": len(exposures), "span_hours": round(span_h, 1)}


def run_csv_investigation(source, question: str | None = None) -> dict:
    """Full governed analysis of an uploaded CSV, same pipeline as the demo.

    Replaces the store contents (demo semantics: reproducible, never
    additive), ingests the canonical event stream and runs the agent
    investigation with governed SQL + full audit trail.
    """
    rows, _ = read_attempts_csv(source)
    events, exposures, stats = rows_to_events(rows)

    from paylens.metrics.definitions import seed_metric_definitions
    from paylens.storage.db import create_all, engine, session_scope
    from paylens.storage.ingest import IngestConsumer
    from paylens.storage.models import Base

    Base.metadata.drop_all(engine)  # upload runs are reproducible, never additive
    create_all()
    with session_scope() as session:
        seed_metric_definitions(session)

    ingest_stats = {"events_received": len(events), "attempts_upserted": 0}
    with session_scope() as session:
        raw_events = [e.model_dump() for e in events]
        raw_exposures = [x.model_dump() for x in exposures]
        consumer = IngestConsumer(None, session)
        out = consumer.process_records(raw_events, raw_exposures)
        ingest_stats["attempts_upserted"] = out.get("attempts_upserted", 0)
        ingest_stats["invalid"] = out.get("invalid", 0)
        ingest_stats["duplicates"] = out.get("duplicates", 0)

    from paylens.pipeline import run_investigation

    q = question or ("Investigate the payment success-rate change over the "
                     "last 24 hours compared with the previous 24 hours and "
                     "identify affected segments.")
    result = run_investigation(q)
    result["upload_stats"] = {**stats, **ingest_stats}
    return result
