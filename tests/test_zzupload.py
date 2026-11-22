"""Bring-your-own-data: CSV uploads flow through the SAME governed pipeline.

Runs last (zz prefix) on purpose: an upload run replaces the store contents
(demo semantics: reproducible, never additive), like the self-contained
real-data suite.
"""
from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone

import pytest

from paylens.realdata.csv_upload import (
    CsvUploadError,
    read_attempts_csv,
    rows_to_events,
    run_csv_investigation,
)

START = datetime(2026, 6, 1, 8, 0, tzinfo=timezone.utc)


def _write_csv(path, hours: int = 48, per_hour: int = 120,
               degraded_from: int = 26) -> str:
    """Deterministic two-day export: healthy day 1, gateway-b breaks mid day 2."""
    rows, n = [], 0
    for h in range(hours):
        ts = (START + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M:%SZ")
        for _ in range(per_hour):
            n += 1
            gw = "gateway-b" if (n // 7) % 2 == 0 else "gateway-a"
            p_ok = 0.55 if (gw == "gateway-b" and h >= degraded_from) else 0.92
            r = (n * 7919) % 100
            if r < p_ok * 100:
                stage = "succeeded"
            elif r < 97:
                stage = "failed"
            else:
                stage = "pending"
            exp = "EXP-2026-06" if n % 4 == 0 else ""
            var = ("A" if n % 2 == 0 else "B") if exp else ""
            rows.append({
                "attempt_id": f"A-{n:07d}", "started_at": ts, "stage_reached": stage,
                "method": ("upi" if n % 3 else "card"), "gateway": gw,
                "bank": ("hdfc" if n % 2 else "icici"), "app_version": "3.4.1",
                "geo": ("in" if n % 5 else "us"), "customer_tier": "tier1",
                "experiment_id": exp, "variant": var, "amount": "250.50",
            })
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    return path


def _tiny_rows(hours: float, per_hour: int = 60):
    out, n = [], 0
    for h in range(int(hours) + 1):
        ts = (START + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M:%SZ")
        for _ in range(per_hour):
            n += 1
            out.append({"attempt_id": f"T-{n}", "started_at": ts,
                        "stage_reached": "succeeded", "method": "upi",
                        "gateway": "gateway-a"})
    return out


def test_rows_to_events_expansion(tmp_path):
    path = _write_csv(tmp_path / "one.csv", hours=48, per_hour=4)
    rows, stats = read_attempts_csv(path)
    assert stats["rows"] == 48 * 4
    events, exposures, info = rows_to_events(rows)
    # succeeded/failed rows expand to 5 events, pending to 4, abandoned to 1..3
    assert info["attempts"] == len(rows)
    assert info["events"] == len(events) > info["attempts"]
    terminals = [e for e in events if e.event_type.value.startswith("payment_succe") or
                 e.event_type.value.startswith("payment_fai")]
    assert terminals, "terminal events must exist for settled rows"
    assert exposures, "experiment rows must produce exposure events"
    assert all(x.variant in ("A", "B") for x in exposures)


def test_end_to_end_same_pipeline(tmp_path):
    path = _write_csv(tmp_path / "upload.csv")
    result = run_csv_investigation(path)
    assert result["run_id"]
    report = result["report"]
    assert report["finding"], "the agents must produce a finding"
    assert result["upload_stats"]["attempts"] == 48 * 120
    # governed SQL ran and was audited - identical to the demo path
    audit = report["provenance"]["governed_queries"]
    assert len(audit) >= 10
    # the injected gateway-b incident must be the top deficit segment
    gw = report["segment_rankings"]["gateway"]
    assert gw[0]["value"] == "gateway-b"
    assert gw[0]["delta_pp"] < -10


def test_span_guard(tmp_path):
    path = _write_csv(tmp_path / "short.csv", hours=24, per_hour=120)
    with pytest.raises(CsvUploadError, match="two full days"):
        run_csv_investigation(path)


def test_missing_column(tmp_path):
    path = tmp_path / "broken.csv"
    path.write_text("attempt_id,foo\nA-1,2026-06-01T08:00:00Z\n")
    with pytest.raises(CsvUploadError, match="Missing required columns"):
        read_attempts_csv(path)


def test_too_few_valid_rows(tmp_path):
    rows = _tiny_rows(hours=48, per_hour=2)  # 98 rows... need < 50 valid
    with pytest.raises(CsvUploadError, match="at least 50"):
        rows_to_events(rows[:40])


def test_api_upload_endpoint(tmp_path):
    from fastapi.testclient import TestClient

    from paylens.api.main import app

    path = _write_csv(tmp_path / "api.csv")
    with TestClient(app) as client:
        with open(path, "rb") as fh:
            resp = client.post("/api/uploads",
                               files={"file": ("api.csv", fh.read(), "text/csv")})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["run_id"]
        assert body["report"]["finding"]
        health = client.get("/api/health")
        assert health.status_code == 200
