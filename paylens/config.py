"""PayLens configuration.

All settings are environment-driven so the same code base runs:
  - demo layer   : in-memory event bus + SQLite (zero infrastructure)
  - prod layer   : Apache Kafka + PostgreSQL via docker-compose

LLM providers are optional: with no provider configured the agents fall back
to a deterministic rule-based planner/narrator. Numbers are ALWAYS computed by
SQL/Python regardless of the provider (hybrid reasoning policy).
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


@dataclass(frozen=True)
class Settings:
    # --- storage -----------------------------------------------------------
    database_url: str = field(
        default_factory=lambda: _env("PAYLENS_DATABASE_URL", "sqlite:///./paylens_demo.db")
    )

    # --- event transport ---------------------------------------------------
    # "memory" for the demo layer, "kafka" for the docker-compose layer.
    event_bus: str = field(default_factory=lambda: _env("PAYLENS_EVENT_BUS", "memory"))
    kafka_bootstrap: str = field(
        default_factory=lambda: _env("PAYLENS_KAFKA_BOOTSTRAP", "localhost:9092")
    )
    topic_payment_events: str = field(default_factory=lambda: _env("PAYLENS_TOPIC_PAYMENTS", "payment-events"))
    topic_experiments: str = field(default_factory=lambda: _env("PAYLENS_TOPIC_EXPOSURES", "experiment-exposures"))
    consumer_group: str = field(default_factory=lambda: _env("PAYLENS_CONSUMER_GROUP", "paylens-ingest"))

    # --- agents / LLM ------------------------------------------------------
    # "none"  : deterministic planner + templated narration (no network calls)
    # "openai": any OpenAI-compatible endpoint (set OPENAI_API_KEY / OPENAI_BASE_URL / PAYLENS_LLM_MODEL)
    llm_provider: str = field(default_factory=lambda: _env("PAYLENS_LLM_PROVIDER", "none"))
    llm_model: str = field(default_factory=lambda: _env("PAYLENS_LLM_MODEL", "gpt-4o-mini"))
    llm_api_key: str = field(default_factory=lambda: _env("OPENAI_API_KEY", ""))
    llm_base_url: str = field(default_factory=lambda: _env("OPENAI_BASE_URL", ""))
    llm_timeout_s: float = field(default_factory=lambda: float(_env("PAYLENS_LLM_TIMEOUT", "20")))

    # --- governed SQL limits ----------------------------------------------
    sql_max_rows: int = field(default_factory=lambda: int(_env("PAYLENS_SQL_MAX_ROWS", "20000")))
    sql_timeout_s: int = field(default_factory=lambda: int(_env("PAYLENS_SQL_TIMEOUT", "30")))

    # --- demo generator ----------------------------------------------------
    anchor_iso: str = field(default_factory=lambda: _env("PAYLENS_ANCHOR_TS", ""))
    seed: int = field(default_factory=lambda: int(_env("PAYLENS_SEED", "42")))

    @property
    def is_sqlite(self) -> bool:
        return self.database_url.startswith("sqlite")

    @property
    def is_postgres(self) -> bool:
        return self.database_url.startswith("postgresql")


SETTINGS = Settings()

REPO_ROOT = Path(__file__).resolve().parent.parent
