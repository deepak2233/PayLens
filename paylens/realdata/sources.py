"""Real open-source data sources for PayLens.

Each source is registered with its provenance: origin, license, download
URLs and expected SHA-256 checksums. The loader refuses to ingest any file
whose checksum or row count does not match - a corrupted or silently
substituted download can never enter the store.
"""
from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SourceFile:
    name: str
    url: str
    sha256: str
    rows: int  # data rows, header excluded


@dataclass(frozen=True)
class RealSource:
    id: str
    title: str
    origin: str
    license: str
    files: tuple[SourceFile, ...]
    notes: str = ""


OLIST = RealSource(
    id="olist",
    title="Olist Brazilian E-commerce Public Dataset (real marketplace data)",
    origin="Kaggle: olistbr/brazilian-ecommerce; 99,441 real orders placed "
           "2016-09 to 2018-09 on the Olist marketplace (verified mirror: "
           "HuggingFace datasets bulutttt/olist-raw-data)",
    license="CC BY-NC-SA 4.0 (attribution + non-commercial + share-alike)",
    files=(
        SourceFile(
            name="olist_orders_dataset.csv",
            url="https://huggingface.co/datasets/bulutttt/olist-raw-data/"
                "resolve/main/olist_orders_dataset.csv",
            sha256="8df58ef3d2d7e9944010f7beecd9b75367f5588ec6e3c91cec19ae3345ef9ecf",
            rows=99441,
        ),
        SourceFile(
            name="olist_order_payments_dataset.csv",
            url="https://huggingface.co/datasets/bulutttt/olist-raw-data/"
                "resolve/main/olist_order_payments_dataset.csv",
            sha256="4f713964f2815dbbaa40b9488268c55aac3627bfce5aa96cf58d1f3616de3cc0",
            rows=103886,
        ),
        SourceFile(
            name="olist_customers_dataset.csv",
            url="https://huggingface.co/datasets/bulutttt/olist-raw-data/"
                "resolve/main/olist_customers_dataset.csv",
            sha256="983a422239e1712ded753b3bf9ecf47dc73f144d306029dcfa99e70a226883d2",
            rows=99441,
        ),
    ),
    notes="Timestamps are local Brazilian time (BRT, UTC-3); DST 2016-2018 "
          "adds a known +/-1h caveat on a small share of days. No cancel "
          "timestamps exist: terminal failures are pinned to the last "
          "observed event time (documented mapping limitation).",
)

UPWORTHY = RealSource(
    id="upworthy",
    title="Upworthy Research Archive, exploratory set (real randomized A/B tests)",
    origin="Matias & Munger, Scientific Data 8:195 (2021), doi:10.1038/s41597-"
           "021-00934-7; 22,666 packages across 4,873 real click-through "
           "experiments; hosted on OSF osf.io/jd64p (file 3vqmp)",
    license="CC BY 4.0",
    files=(
        SourceFile(
            name="upworthy-archive-exploratory-packages-03.12.2020.csv",
            url="https://osf.io/download/3vqmp/",
            sha256="8368313b060f4015a0c6fb34e6d788163cee29554144aba9390b14922eb9d8ce",
            rows=22666,
        ),
    ),
    notes="Aggregate arm units (impressions/clicks per package), not user "
          "rows - readouts are computed on real aggregate counts; no "
          "microdata is synthesized.",
)

SOURCES: dict[str, RealSource] = {s.id: s for s in (OLIST, UPWORTHY)}


class SourceVerificationError(RuntimeError):
    """Checksum/row-count mismatch or missing file - refuse to ingest."""


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def verify_source(raw_dir: Path, source_id: str) -> dict:
    """Verify every file of a source against the registered checksum and
    row count. Raises SourceVerificationError on any mismatch (fail-closed)."""
    source = SOURCES[source_id]
    report = {"source": source.id, "title": source.title,
              "license": source.license, "files": []}
    for spec in source.files:
        path = raw_dir / spec.name
        if not path.exists():
            raise SourceVerificationError(
                f"{source.id}: missing file {spec.name}; download from {spec.url}")
        digest = _sha256(path)
        if digest != spec.sha256:
            raise SourceVerificationError(
                f"{source.id}: sha256 mismatch for {spec.name}: "
                f"expected {spec.sha256}, got {digest}")
        # count CSV *records*, not physical lines (quoted fields may span lines)
        with path.open(newline="", encoding="utf-8") as f:
            n = sum(1 for _ in csv.reader(f)) - 1
        if n != spec.rows:
            raise SourceVerificationError(
                f"{source.id}: row count mismatch for {spec.name}: "
                f"expected {spec.rows}, got {n}")
        report["files"].append({"file": spec.name, "sha256": digest, "rows": n})
    return report


def provenance_block(source_id: str, raw_dir: Path) -> dict:
    """Full provenance record for a run report (origin, license, checksums)."""
    source = SOURCES[source_id]
    return {
        "id": source.id,
        "title": source.title,
        "origin": source.origin,
        "license": source.license,
        "notes": source.notes,
        "files": [
            {"file": f.name, "url": f.url, "sha256": f.sha256, "rows": f.rows}
            for f in source.files
        ],
        "verified": json.dumps({f.name: True for f in source.files}),
        "_raw_dir": str(raw_dir),
    }


def iter_csv(path: Path, encoding: str = "utf-8"):
    """Stream a CSV as dicts (bronze layer reads nothing into memory wholesale)."""
    with path.open(newline="", encoding=encoding) as f:
        yield from csv.DictReader(f)
