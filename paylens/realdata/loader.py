"""Real-data loader: the medallion flow into PayLens.

bronze  raw source CSVs on disk (checksum-verified, never modified)
silver  contract-validated rows -> canonical PayLens payment events
gold    the existing attempt/event store, filled through the *same*
        IngestConsumer the Kafka path uses (dedup, idempotency, stage
        derivation) - the real-data path and the streaming path converge
        on one correctness spine, then one investigation produces the
        report with full provenance (license, checksums, mapping notes).
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from paylens.metrics.definitions import seed_metric_definitions
from paylens.realdata import olist as olist_mod
from paylens.realdata import upworthy as upworthy_mod
from paylens.realdata.sources import SourceVerificationError, verify_source
from paylens.storage.db import create_all, engine, session_scope
from paylens.storage.ingest import IngestConsumer
from paylens.storage.models import Base

EVENTS_PER_BATCH = 20_000


def _fresh_store() -> None:
    """Real-data runs are reproducible: drop and recreate the gold store."""
    Base.metadata.drop_all(engine)
    create_all()
    with session_scope() as session:
        seed_metric_definitions(session)


def load_olist(raw_dir: Path, limit_orders: int | None = None) -> dict:
    """Verify -> validate -> map -> ingest the real Olist orders.

    Returns a provenance record: verification, validation reports (with
    dead-letter samples), mapping notes and ingest statistics.
    """
    verification = verify_source(raw_dir, "olist")
    _fresh_store()

    totals = {
        "events_received": 0, "events_ingested": 0, "duplicates": 0,
        "invalid": 0, "attempts_upserted": 0, "assignments": 0,
        "assignments_backfilled": 0,
    }
    orders = 0
    reports: list[dict] = []
    batch: list[dict] = []
    t0 = time.monotonic()

    def flush() -> None:
        nonlocal batch
        if not batch:
            return
        with session_scope() as session:
            consumer = IngestConsumer(None, session)
            run = consumer.process_records(payments=batch, exposures=[])
            for k in totals:
                if k in run:
                    totals[k] += run[k]
        batch = []

    for meta, events in olist_mod.iter_order_events(raw_dir, limit_orders):
        if events is None:  # sentinel carries the validation reports
            reports = meta["_reports"]
            continue
        orders += 1
        batch.extend(events)
        if len(batch) >= EVENTS_PER_BATCH:
            flush()
    flush()

    return {
        "source": olist_mod.__doc__.splitlines()[0].strip(" ."),
        "verification": verification,
        "validation": reports,
        "mapping": {
            "attempt": "olist order_id",
            "amount": "sum(order payments) in centavos, currency BRL",
            "method": "primary payment_type (lowest payment_sequential)",
            "second_dimension": "customer_state (presented as region)",
            "terminal": "delivered=succeeded; canceled/unavailable=failed at "
                        "last observed event; other statuses pending (censoring-safe)",
            "timezone": "source BRT (UTC-3) -> store UTC",
        },
        "stats": {"orders": orders, **totals,
                  "duration_ms": int((time.monotonic() - t0) * 1000)},
    }


def real_investigation(raw_dir: Path, window_days: int = 60,
                       limit_orders: int | None = None,
                       anchor: str | None = None,
                       focus_method: str | None = None,
                       include_upworthy: bool = True,
                       upworthy_top: int = 3) -> dict:
    """End-to-end real-data flow: load real orders, investigate, read out
    real A/B tests. Everything the report claims carries its provenance."""
    from datetime import datetime

    from paylens.pipeline import run_investigation

    if not raw_dir.exists():
        raise SourceVerificationError(f"raw data dir not found: {raw_dir}")

    real_experiments: list[dict] = []
    upworthy_summary: dict | None = None
    upworthy_file = raw_dir / "upworthy-archive-exploratory-packages-03.12.2020.csv"
    if include_upworthy:
        verify_source(raw_dir, "upworthy")
        upworthy_summary = upworthy_mod.analyze_archive(upworthy_file, top_n=upworthy_top)
        real_experiments = upworthy_summary["selected"]

    olist_provenance = load_olist(raw_dir, limit_orders=limit_orders)

    config = {
        "window_hours": window_days * 24,
        "grain": "day",
        "data_source": "olist (real orders) + upworthy (real A/B archive)",
        "focus_method": focus_method,
        "run_experiment": False,  # real experiments come from the archive readouts
        "real_experiments": real_experiments,
    }
    if anchor:
        config["window_end"] = datetime.fromisoformat(anchor).replace(tzinfo=None)

    question = ("Investigate the order fulfillment success rate over the last "
                f"{window_days} days of the Olist marketplace data. Compare "
                "against the preceding equally long baseline period, identify "
                "affected payment-method and region segments, and read out the "
                "real randomized experiments from the Upworthy archive.")
    result = run_investigation(question, config=config)
    result["report"]["real_experiments"] = real_experiments
    result["report"]["data_provenance"] = {
        "olist": olist_provenance,
        "upworthy": {
            "summary": {k: v for k, v in (upworthy_summary or {}).items()
                        if k != "selected"},
        },
    }
    result["report"]["provenance"]["real_experiments_count"] = len(real_experiments)
    return result


def dump_report(result: dict, out_path: Path) -> None:
    out_path.write_text(json.dumps(result["report"], indent=2, default=str))


if __name__ == "__main__":  # pragma: no cover
    import sys

    raw = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("paylens_data/raw")
    res = real_investigation(raw)
    print(json.dumps(res["report"], indent=2, default=str)[:4000])
