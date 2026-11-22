"""SQLAlchemy engine and session management.

One engine serves both the demo layer (SQLite) and the production layer
(PostgreSQL) - only ``PAYLENS_DATABASE_URL`` changes.
"""
from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import create_engine, event as sa_event
from sqlalchemy.orm import Session, sessionmaker

from paylens.config import SETTINGS

_connect_args = {"check_same_thread": False} if SETTINGS.is_sqlite else {}

engine = create_engine(
    SETTINGS.database_url,
    connect_args=_connect_args,
    pool_pre_ping=True,
    future=True,
)

if SETTINGS.is_sqlite:
    # WAL + busy timeout make concurrent API reads safe during ingest.
    @sa_event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_conn, _record):  # pragma: no cover
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA busy_timeout=5000")
        cur.close()

SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)


@contextmanager
def session_scope() -> Session:
    """Transactional scope; rolls back on error, always closes."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def create_all() -> None:
    from paylens.storage.models import Base

    Base.metadata.create_all(engine)
