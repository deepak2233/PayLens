# PayLens: Payments Analytics & Experimentation Agents

PayLens is an AI-powered payment analytics system for banks, fintechs and
payment platforms. It answers the questions payment teams actually ask:
**why** did success rate drop, **who** is affected, **what** changed, and
**did** the fix actually work - with every number traceable to a governed,
audited SQL query.

> PayLens does **not** process or authorize payments. It analyzes payment
> data after events are generated. The bundled dataset is synthetic.

## Architecture in one paragraph

Payment events flow through **Kafka** into **PostgreSQL**, where a consumer
maintains validated, de-duplicated attempt state and consistent **versioned
metric definitions**. Four specialist agents - **Funnel**, **Segment**,
**Anomaly** and **Experiment** - are orchestrated by a **LangGraph**
supervisor that plans the investigation, branches on intermediate findings
and checkpoints state. Agents never compose SQL: they pick from governed,
read-only templates whose every execution is written to an audit log.
**The LLM plans and words; SQL and Python compute every number** (hybrid
reasoning - the system runs fully without any LLM API key).

## Two ways to run it

### 1. Demo layer - one command, zero infrastructure

```bash
pip install -r requirements.txt
python cli.py demo            # seed SQLite + run the reference investigation
python cli.py api             # dashboard on http://localhost:8000
```

The demo uses an in-memory event bus that implements the same
producer/consumer contract as Kafka, and SQLite via SQLAlchemy.

### 2. Production layer - Kafka + PostgreSQL via Docker

```bash
pip install -r requirements-prod.txt
docker compose up --build
docker compose run producer                     # push synthetic data through Kafka
docker compose run --rm api python cli.py investigate "Investigate the UPI success-rate drop over the last 24 hours. Compare with yesterday and identify affected segments."
open http://localhost:8000/
```

`docker-compose.yml` starts Kafka (KRaft), PostgreSQL (schema in
`sql/init.sql`), the ingest consumer (`paylens/streaming/consumer.py`) and
the FastAPI service. `PAYLENS_DATABASE_URL` is the only switch between
layers.

## The reference scenario (bundled synthetic data)

### 3. Interactive web console (static, Vercel-ready)

`web/index.html` is a self-contained console (no build step, no external
dependencies) covering every aspect of the system with the repository's
reference run embedded: overview KPIs, funnel, segment rankings, anomaly
chart with the change point + deployment marker, A/B readout, the governed
SQL audit log, the real-data (Olist/Upworthy) results and the architecture.

```bash
# serve locally
python -m http.server 8080 --directory web     # http://localhost:8080

# or deploy to Vercel (zero config - see vercel.json)
vercel        # from the repo root
```

It stays fully functional as a static site; to browse fresh data, run the
API locally (`uvicorn paylens.api.main:app --port 8000`) and connect from
the **Connect / Deploy** tab.

## The reference scenario (bundled synthetic data)

Two days of traffic (~92% success on day 1). At 10:15 on day 2, an
infrastructure deployment (`CHG-8841`) triggers a timeout storm for UPI
traffic routed via Gateway B; overall success lands near 82%. A 50/50
checkout A/B experiment runs on day 0 (A ~80% vs B ~82%). The generator
also injects ~0.3% duplicate event ids, ~1% out-of-order publishes and
delayed terminal outcomes - so the ingest pipeline's correctness rules are
exercised, not assumed.

A full-scale reference run (`cli.py demo`) reports:

| Finding | Value |
|---|---|
| Overall success rate | 91.24% -> 79.85% (-11.39 pp) |
| Worst slice | UPI via gateway-b: 91.0% -> 65.68% (-25.32 pp) |
| Change point | 10:00 bucket on the incident day (z = -7.25) |
| Deployment correlate | CHG-8841, within the change-point hour |
| A/B readout | A 80.15% vs B 81.91% (+1.76 pp, p = 0.0015) -> ship candidate |

## What the agents guarantee

| Agent | Question | Gate |
|---|---|---|
| Funnel | Where do payments drop? | Only segments get investigated if the overall drop is material (>= 2 pp) |
| Segment | Who is affected? | Ranked by volume-weighted success deficit; min volume 200, min delta 3 pp |
| Anomaly | What changed? | Hour-over-hour volume-aware z-score, sustained >= 2h; deployments are *timing evidence*, never causation |
| Experiment | Did the change work? | SRM check -> significance -> MDE -> guardrails; anything short returns *inconclusive*, never a rollout claim |

## Governance model

* Agents choose **template names + validated parameters**; SQL text is
  engineer-owned (`paylens/storage/sql_templates.py`).
* Every execution lands in `query_audit_log` (agent, template, params,
  rows, duration, full SQL) - reports link findings to queries.
* Metrics are versioned (`metric_definitions`); reports pin the version so
  old numbers stay reproducible.
* Attempt state derives from a monotone stage rank: duplicates never count
  twice, out-of-order events never regress a terminal outcome, and
  unsettled attempts are reported as `pending`, never guessed.

## Layout

```
paylens/
  events/       schemas, synthetic generator, in-memory + Kafka bus
  storage/      SQLAlchemy models, ingest pipeline, governed SQL templates
  metrics/      versioned metric definitions
  tools/        governed SQL executor + audit log
  agents/       funnel, segment, anomaly, experiment, LangGraph supervisor
  stats/        two-proportion z-test, Wilson CI, SRM, MDE, percentiles
  reporting/    report assembly (finding, evidence, actions, provenance)
  api/          FastAPI app + minimal responsive dashboard
  streaming/    Kafka ingest worker (production layer)
scripts/        produce_to_kafka.py
sql/            PostgreSQL bootstrap + read-only analytics role
tests/          unit tests (dedup, out-of-order, metrics, stats, agents, API)
cli.py          one-command demo
```

## Real open-source data (no synthetic required)

The pipeline runs on **real, checksum-verified open-source data**:

| Source | Content | License |
|---|---|---|
| [Olist Brazilian E-commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (verified HF mirror) | 99,441 real orders / 103,886 payments, 2016-2018 | CC BY-NC-SA 4.0 |
| [Upworthy Research Archive](https://osf.io/jd64p/) (Sci Data 2021) | 4,873 real randomized A/B tests, 22,666 packages, 81M impressions | CC BY 4.0 |

```bash
# place the CSVs in a directory (see paylens/realdata/sources.py for URLs + SHA-256)
python cli.py real-data --raw-dir ../paylens_data/raw --window-days 60
```

This verifies checksums, validates contracts (bronze -> silver), maps to the
canonical event schema, ingests through the **same** IngestConsumer as the
Kafka path (gold), runs the agent graph at day grain, and reads out all
real experiments with Bonferroni-corrected multi-arm statistics. The report
carries full provenance (licenses, checksums, mapping notes, dead-letter
samples). Reference run: 495,156 events ingested in ~112 s, zero rejects.

## Quality tooling

```bash
ruff check paylens tests cli.py   # pyflakes-level correctness (CI-enforced)
pytest -q                         # 36 unit tests
docker compose up --build         # production layer builds (CI builds images too)
```

CI (`.github/workflows/ci.yml`) runs lint, the demo-layer test suite and
builds both Docker images on every push. A structured engineering review of
the codebase - findings, fixes and accepted limitations - lives in
[docs/REVIEW.md](docs/REVIEW.md).

## Tests

Coverage highlights: duplicate-event idempotency, out-of-order status
derivation, delayed outcomes, first-write-wins assignments, metric
versioning, two-proportion z-test against known values, SRM detection,
change-point detection on the synthetic incident, and the full LangGraph
investigation on seeded data.
