"""Build reduced public benchmarks from verified source data (no subjects or headlines)."""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import sys
import zipfile
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from paylens.realdata.upworthy import at_randomization_risk  # noqa: E402
from paylens.replay.benchmarks import SNAPSHOT, archive_readout  # noqa: E402

SHOPPERS_ZIP_SHA = "2972e6184d3ad7beaaa831d9fc2b059dc3ee29df69d1ec593c466a5cd8485d14"
UPWORTHY_SHA = "8368313b060f4015a0c6fb34e6d788163cee29554144aba9390b14922eb9d8ce"


def verified(path: Path, digest: str | None = None):
    payload = path.read_bytes()
    actual = hashlib.sha256(payload).hexdigest()
    if digest and digest != actual:
        raise ValueError(f"Unexpected source checksum for {path.name}: {actual}")
    return payload, actual


def shoppers(path: Path) -> dict:
    payload, digest = verified(path, SHOPPERS_ZIP_SHA)
    sessions, converted = Counter(), Counter()
    with zipfile.ZipFile(io.BytesIO(payload)) as z:
        for row in csv.DictReader(io.StringIO(z.read("online_shoppers_intention.csv").decode("utf-8"))):
            segment = row["VisitorType"]
            if row["Revenue"] not in ("TRUE", "FALSE"):
                raise ValueError("Unexpected revenue label")
            sessions[segment] += 1
            converted[segment] += int(row["Revenue"] == "TRUE")
    total = sum(sessions.values())
    if total != 12_330:
        raise ValueError("Unexpected shopper cohort size")
    return {
        "url": "https://archive.ics.uci.edu/dataset/468/online+shoppers+purchasing+intention+dataset",
        "data_url": "https://archive.ics.uci.edu/static/public/468/online+shoppers+purchasing+intention+dataset.zip",
        "license": "CC BY 4.0", "sha256": digest,
        "sessions": total, "purchases": sum(converted.values()),
        "conversion_pct": 100 * sum(converted.values()) / total,
        "groups": [{"group": segment, "sessions": sessions[segment], "purchases": converted[segment],
                    "conversion_pct": 100 * converted[segment] / sessions[segment]}
                   for segment in sorted(sessions, key=lambda segment: -sessions[segment])],
    }


def upworthy(path: Path) -> dict:
    payload, digest = verified(path, UPWORTHY_SHA)
    arms = defaultdict(list)
    risky = set()
    for row in csv.DictReader(io.StringIO(payload.decode("utf-8"))):
        test_id = row["clickability_test_id"]
        if not test_id:
            raise ValueError("Archive test ID missing")
        if at_randomization_risk(row):
            risky.add(test_id)
        n, y = int(row["impressions"]), int(row["clicks"] or 0)
        if not 0 <= y <= n:
            raise ValueError("Inconsistent archive count")
        arms[test_id].append([n, y])
    eligible = {test_id: arm for test_id, arm in arms.items()
                if test_id not in risky and len(arm) > 1 and all(n > 0 for n, _ in arm)}
    material = sorted(eligible, key=lambda test_id: hashlib.sha256(test_id.encode()).hexdigest())
    checks = [archive_readout(arm) for arm in eligible.values()]
    return {
        "url": "https://upworthy.natematias.com/about-the-archive.html",
        "data_url": "https://osf.io/download/3vqmp/",
        "correction_url": "https://doi.org/10.1038/s41597-024-03600-w",
        "license": "CC BY 4.0", "sha256": digest, "tests_total": len(arms),
        "excluded_risk": len(risky), "evaluated_tests": len(eligible),
        "srm_flagged": sum(not item["srm_ok"] for item in checks),
        "sample_rule": "First 12 eligible test IDs sorted by SHA-256, independent of outcomes.",
        "samples": [{"id": test_id, "arms": eligible[test_id]} for test_id in material[:12]],
    }


def pix(raw_dir: Path, as_of: date) -> dict:
    base = "https://olinda.bcb.gov.br/olinda/servico/SPI/versao/v1/odata/"
    data = {
        "url": "https://dadosabertos.bcb.gov.br/dataset/estatisticas-do-spi-sistema-de-pagamentos-instantaneos",
        "license": "ODbL 1.0", "months": [], "outages": [], "files": [],
    }
    paths = [{"file": "spi-availability.json", "endpoint": "PixDisponibilidadeSPI"},
             {"file": "spi-outages.json", "endpoint": "PixInterrupcaoSPIEventos?%24top=300"}]
    for source in paths:
        path = raw_dir / source["file"]
        if not path.exists():
            continue
        payload, digest = verified(path)
        data["files"].append({"url": base + source["endpoint"], "sha256": digest})
        for row in json.loads(payload)["value"]:
            if "DataBase" in row:
                month = date.fromisoformat(row["DataBase"])
                if month <= as_of:
                    index, floor = float(row["Indice"]), float(row["MinimoNormativo"])
                    if not 0 <= index <= 100 or not 0 < floor <= 100:
                        raise ValueError("Unexpected availability percentage")
                    data["months"].append({"month": month.isoformat()[:7], "index_pct": index, "floor_pct": floor})
            else:
                start = datetime.strptime(row["DataHoraInicioInt"], "%d/%m/%Y %H:%M:%S")
                end = datetime.strptime(row["DataHoraTerminoInt"], "%d/%m/%Y %H:%M:%S")
                if end.date() <= as_of and start <= end:
                    data["outages"].append({"start": start.isoformat(), "end": end.isoformat(),
                                            "seconds": int((end - start).total_seconds())})
    data["months"] = sorted(data["months"], key=lambda r: r["month"], reverse=True)[:18]
    data["outages"] = sorted(data["outages"], key=lambda r: r["start"], reverse=True)[:12]
    if not data["months"] and not data["outages"]:
        raise ValueError("Download at least one of the SPI source files")
    return data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw_dir", type=Path)
    parser.add_argument("--as-of", type=date.fromisoformat, required=True)
    parser.add_argument("--out", type=Path, default=SNAPSHOT)
    args = parser.parse_args()
    data = {
        "as_of": args.as_of.isoformat(),
        "shoppers": shoppers(args.raw_dir / "shoppers.zip"),
        "upworthy": upworthy(args.raw_dir / "upworthy.csv"),
        "pix": pix(args.raw_dir, args.as_of),
    }
    args.out.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
