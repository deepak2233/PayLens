"""Pytest fixtures: a fresh, seeded SQLite store shared by the test session.

The PAYLENS_DATABASE_URL env var is set BEFORE any paylens import so the
module-level engine binds to the test database.
"""
from __future__ import annotations

import os
import pathlib
import sys
import tempfile
from datetime import datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

_DB = os.path.join(tempfile.gettempdir(), "paylens_pytest.db")
os.environ["PAYLENS_DATABASE_URL"] = f"sqlite:///{_DB}"
os.environ.setdefault("PAYLENS_ANCHOR_TS", "2026-11-20T23:59:00+00:00")

import pytest  # noqa: E402

ANCHOR = datetime(2026, 11, 20, 23, 59, tzinfo=timezone.utc)


@pytest.fixture(scope="session")
def store():
    """Seeded store: 1,200 organic attempts/day (enough volume for segment
    thresholds) + 20,000 users per experiment arm (decisive A/B readout:
    the true 2pp effect is ~5 sigma at this scale, robust to any seed)."""
    from paylens.pipeline import seed_database
    from paylens.storage.db import create_all, engine
    from paylens.storage.models import Base

    Base.metadata.drop_all(engine)
    create_all()
    stats = seed_database(
        anchor=ANCHOR,
        attempts_per_day=1_200,
        experiment_users_per_arm=20_000,
    )
    return stats


@pytest.fixture()
def session():
    from paylens.storage.db import session_scope

    with session_scope() as s:
        yield s
