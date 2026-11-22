-- PayLens PostgreSQL bootstrap (docker-compose init script).
-- Mirrors the SQLAlchemy models in paylens/storage/models.py.
-- The analytics role is READ-ONLY: agents connect through it; only the
-- ingest worker holds write privileges on the event tables.

CREATE TABLE IF NOT EXISTS payment_attempts (
    attempt_id           VARCHAR(64) PRIMARY KEY,
    txn_id               VARCHAR(64) NOT NULL,
    pseudo_customer_id   VARCHAR(64) NOT NULL,
    method               VARCHAR(20) NOT NULL,
    gateway              VARCHAR(30) NOT NULL,
    bank                 VARCHAR(30) DEFAULT '',
    amount_paise         BIGINT NOT NULL,
    currency             VARCHAR(8) DEFAULT 'INR',
    app_version          VARCHAR(20) DEFAULT '',
    geo                  VARCHAR(12) DEFAULT '',
    customer_tier        VARCHAR(12) DEFAULT '',
    experiment_id        VARCHAR(64),
    variant              VARCHAR(2),
    started_at           TIMESTAMPTZ NOT NULL,
    method_selected_at   TIMESTAMPTZ,
    initiated_at         TIMESTAMPTZ,
    gateway_responded_at TIMESTAMPTZ,
    terminal_at          TIMESTAMPTZ,
    status               VARCHAR(20) DEFAULT 'started',
    status_rank          INTEGER DEFAULT 0,
    error_code           VARCHAR(40),
    latency_ms           INTEGER,
    settled              BOOLEAN DEFAULT FALSE,
    is_duplicate_charge  BOOLEAN DEFAULT FALSE,
    event_count          INTEGER DEFAULT 0,
    last_event_at        TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_attempts_started ON payment_attempts (started_at);
CREATE INDEX IF NOT EXISTS ix_attempts_method_gateway ON payment_attempts (method, gateway);
CREATE INDEX IF NOT EXISTS ix_attempts_experiment ON payment_attempts (experiment_id);
CREATE INDEX IF NOT EXISTS ix_attempts_last_event ON payment_attempts (last_event_at);

CREATE TABLE IF NOT EXISTS payment_events (
    event_id            VARCHAR(64) PRIMARY KEY,   -- global dedup key
    attempt_id          VARCHAR(64) NOT NULL,
    event_type          VARCHAR(30) NOT NULL,
    ts                  TIMESTAMPTZ NOT NULL,
    status              VARCHAR(20),
    error_code          VARCHAR(40),
    latency_ms          INTEGER,
    is_duplicate_charge BOOLEAN DEFAULT FALSE,
    payload             JSONB DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS ix_events_attempt ON payment_events (attempt_id, ts);
CREATE INDEX IF NOT EXISTS ix_events_type ON payment_events (event_type);

CREATE TABLE IF NOT EXISTS experiment_assignments (
    id                 SERIAL PRIMARY KEY,
    experiment_id      VARCHAR(64) NOT NULL,
    variant            VARCHAR(2) NOT NULL,
    pseudo_customer_id VARCHAR(64) NOT NULL,
    assigned_at        TIMESTAMPTZ NOT NULL,
    CONSTRAINT uq_assignment UNIQUE (experiment_id, pseudo_customer_id)
);

CREATE TABLE IF NOT EXISTS metric_definitions (
    id           SERIAL PRIMARY KEY,
    metric_name  VARCHAR(60) NOT NULL,
    version      INTEGER NOT NULL,
    description  TEXT NOT NULL,
    formula_sql  TEXT NOT NULL,
    owner        VARCHAR(60) DEFAULT 'payments-analytics',
    created_at   TIMESTAMPTZ DEFAULT now(),
    CONSTRAINT uq_metric_version UNIQUE (metric_name, version)
);

CREATE TABLE IF NOT EXISTS deployments (
    change_id   VARCHAR(40) PRIMARY KEY,
    service     VARCHAR(60) NOT NULL,
    gateway     VARCHAR(30) DEFAULT '',
    deployed_at TIMESTAMPTZ NOT NULL,
    note        TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS query_audit_log (
    id           SERIAL PRIMARY KEY,
    run_id       VARCHAR(64) NOT NULL,
    agent        VARCHAR(40) NOT NULL,
    template     VARCHAR(80) NOT NULL,
    params_json  JSONB DEFAULT '{}',
    row_count    INTEGER NOT NULL,
    duration_ms  INTEGER NOT NULL,
    sql_text     TEXT NOT NULL,
    executed_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_audit_run ON query_audit_log (run_id);

CREATE TABLE IF NOT EXISTS investigations (
    run_id         VARCHAR(64) PRIMARY KEY,
    question       TEXT NOT NULL,
    status         VARCHAR(20) DEFAULT 'running',
    window_start   TIMESTAMPTZ,
    window_end     TIMESTAMPTZ,
    metric_version INTEGER DEFAULT 2,
    report         JSONB DEFAULT '{}',
    created_at     TIMESTAMPTZ DEFAULT now(),
    finished_at    TIMESTAMPTZ
);

-- Read-only analytics role used by the agent connection pool.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'paylens_analytics') THEN
        CREATE ROLE paylens_analytics LOGIN PASSWORD 'paylens_readonly_password';
    END IF;
END $$;
GRANT USAGE ON SCHEMA public TO paylens_analytics;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO paylens_analytics;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO paylens_analytics;
