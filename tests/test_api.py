"""FastAPI surface tests."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from paylens.api.main import app


@pytest.fixture(scope="module")
def client(store):
    return TestClient(app)


def test_health(client):
    r = client.get("/api/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["attempts_stored"] > 0


def test_dashboard_serves_html(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "PayLens" in r.text
    assert 'name="viewport"' in r.text  # responsive requirement


def test_success_rate_metric(client):
    r = client.get("/api/metrics/success-rate?window_hours=24")
    assert r.status_code == 200
    body = r.json()
    assert body["metric"] == "payment_success_rate"
    assert body["version"] == 2
    assert 0 <= body["success_rate"] <= 100
    assert body["pending"] >= 0


def test_success_rate_grouped_by_method(client):
    r = client.get("/api/metrics/success-rate?window_hours=24&group_by=method")
    assert r.status_code == 200
    rows = r.json()["rows"]
    assert {row["method"] for row in rows} >= {"upi"}


def test_funnel_endpoint(client):
    r = client.get("/api/funnel?window_hours=24")
    stages = r.json()["stages"]
    assert stages["payment_started"] >= stages["method_selected"] >= stages["payment_initiated"]


def test_investigation_flow(client):
    r = client.post("/api/investigations",
                    json={"question": "Investigate the UPI success-rate drop "
                                      "over the last 24 hours. Compare with yesterday."})
    assert r.status_code == 200
    run_id = r.json()["run_id"]
    report = r.json()["report"]
    assert report["finding"]

    single = client.get(f"/api/investigations/{run_id}")
    assert single.status_code == 200
    assert single.json()["status"] == "completed"

    listing = client.get("/api/investigations")
    assert any(item["run_id"] == run_id for item in listing.json())


def test_experiment_readout_endpoint(client):
    r = client.get("/api/experiments/exp-2026-11-checkout-upi-v2/readout")
    assert r.status_code == 200
    body = r.json()
    assert body["srm"]["ok"] is True
    assert body["effect"]["significant"] is True


def test_audit_endpoint(client):
    r = client.get("/api/audit/queries?limit=10")
    assert r.status_code == 200
    rows = r.json()
    assert rows and {"agent", "template", "sql"} <= set(rows[0])
