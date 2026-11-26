"""SQLAlchemy engine and session management.

One engine serves both the demo layer (SQLite) and the production layer
(PostgreSQL) - only ``PAYLENS_DATABASE_URL`` changes.
"""
from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import create_engine, event as sa_event, text
from sqlalchemy.orm import Session, sessionmaker

from paylens.config import SETTINGS

def make_engine(url: str, **options):
    is_sqlite = url.startswith("sqlite")
    if is_sqlite:
        options.setdefault("connect_args", {"check_same_thread": False})
    elif url.startswith("postgresql"):
        options.setdefault("connect_args", {"options": "-c timezone=UTC", "connect_timeout": 5})
        options.setdefault("pool_size", 3)
        options.setdefault("max_overflow", 2)
    result = create_engine(url, pool_pre_ping=True, **options)
    if is_sqlite:
        @sa_event.listens_for(result, "connect")
        def _set_sqlite_pragma(dbapi_conn, _record):
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA busy_timeout=5000")
            cur.close()
    return result


engine = make_engine(SETTINGS.database_url)
analytics_engine = (make_engine(SETTINGS.analytics_database_url)
                    if SETTINGS.analytics_database_url else engine)

SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)


@contextmanager
def session_scope(factory=None) -> Session:
    """Transactional scope; rolls back on error, always closes."""
    session = (factory or SessionLocal)()
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


@contextmanager
def analytics_scope(bind=None):
    """Pin all evidence queries in a report to one database snapshot."""
    with Session(bind or analytics_engine) as session, session.begin():
        if session.get_bind().dialect.name == "postgresql":
            session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            session.execute(text("SELECT set_config('statement_timeout', :timeout, true)"),
                            {"timeout": f"{SETTINGS.sql_timeout_s}s"})
        else:
            session.connection().exec_driver_sql("BEGIN")
        yield session
