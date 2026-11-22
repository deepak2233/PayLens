# Engineering review — PayLens v1.0

A second-pass adversarial review of the codebase against a top-tier
engineering bar (correctness under scale, operability, hygiene), performed
after the initial build. Findings are ordered by severity; every "fixed"
item is covered by the test suite, and the reference demo run produces
byte-identical findings after the fixes.

## Findings and actions

| ID | Severity | Area | Finding | Action |
|----|----------|------|---------|--------|
| R-01 | High | Ingest performance | `_derive_attempt` ran one experiment-assignment query **per attempt** (a SELECT on `experiment_assignments` for every organic attempt; ~248k queries per full run). Correct but quadratic-feeling at production rates. | One batched `variant_lookup` per ingest batch, passed into attempt derivation. Fixed. |
| R-02 | High | Ingest performance | `_backfill_variants` scanned the **entire assignment table** and issued one UPDATE per assignment on every batch, even when nothing needed back-filling (20k statements per batch in the full demo). | Back-fill is now scoped to customers whose exposures arrived in the current batch; cost is proportional to the batch, never to the table. Fixed. |
| R-03 | Medium | Store | `data_horizon()` calls `MAX(last_event_at)` on every investigation and health check with **no index** on `last_event_at` — a full scan per call at scale. | Added `ix_attempts_last_event` (SQLAlchemy model + `sql/init.sql`). Fixed. |
| R-04 | Medium | Statistics | `srm_check` computed the chi-square p-value with the **df = 1** formula for any arm count — wrong for multi-arm experiments. | Exact normal relation for df = 1, Wilson–Hilferty transform for df > 1; `degrees_of_freedom` returned; multi-arm unit test added (2-arm results unchanged). Fixed. |
| R-05 | Medium | Reproducibility | `seed_database(delete_first=True)` ignored the flag — running `cli.py demo` twice stacked two datasets on top of each other. | Flag honored (`drop_all` before `create_all`); demo runs are now idempotent. Fixed. |
| R-06 | Low | API | `@app.on_event("startup")` — deprecated FastAPI pattern. | Replaced with a `lifespan` context manager. Fixed. |
| R-07 | Low | Agents | Dead variable in the Anomaly Agent (`before` computed, never used). | Replaced with a `rate_context` block (mean settled rate before/after the change point) — data the report consumer actually uses. Fixed. |
| R-08 | Low | Hygiene | 8 unused imports (pyflakes F401); dead JS in the dashboard. | Removed; `ruff check` (F, E9, W6) is now clean and enforced in CI. Fixed. |
| R-09 | Low | Hygiene | No lint/test/CI configuration, no `.gitignore`, hardcoded dev password in compose, no package version. | `pyproject.toml` (ruff + pytest config), `.gitignore`, GitHub Actions CI (lint + demo-layer tests + Docker image builds), `${PAYLENS_PG_PASSWORD:-...}` interpolation in compose, `paylens.__version__` surfaced in the API metadata. Fixed. |

## Accepted limitations (deliberate, documented)

- **SQLite demo store.** The demo layer prioritizes a one-command run over throughput; the production path is the same code against PostgreSQL via docker-compose. Chunked upserts already respect SQLite's per-statement parameter limits.
- **Synchronous investigation endpoint.** `POST /api/investigations` runs the graph inline (a reference run takes seconds on the demo store and stays within one minute on SQLite). Production would hand it to a job queue and poll by `run_id`; the report JSON already supports that pattern.
- **Kafka consumer is not covered by unit tests.** It reuses the tested `IngestConsumer` pipeline and differs only in transport plumbing; CI builds its Docker image but a service-container test job is deferred until CI hardware runs the stack.
- **Static dashboard.** The review page reads the API and renders; the 48h-baseline derivation in its JS is a demo heuristic, not a metric-definition surface.
- **No mypy gate yet.** Type hints cover the public surface; strict whole-repo typing is a follow-up, tracked here rather than pretended.
- **JWT/authN on the API.** Out of scope for a design artifact; the review table in the design doc (§11) treats it as the deployment's responsibility.

## Verification after the fixes

- `ruff check paylens tests cli.py` — clean.
- `pytest` — 36 passed (35 prior + new multi-arm SRM test).
- Full demo run before/after the refactor — identical findings (92.63% → 81.24%, UPI × gateway-b −24.47 pp, change point 10:00, CHG-8841 correlate, SRM p = 1.0).
