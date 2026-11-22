# PayLens as a service — packaging guide

PayLens was built so the same analysis can be sold, not just demoed: every
number is produced by versioned, parameterized, row-capped SQL executed by
audited agents, and every decision stops short of autonomous mitigation.
That governance layer is precisely what makes it safe to point at data you
do not own — the core requirement for selling analytics as a service.

This document is a packaging proposal, not a claim of existing deployments.
Pricing figures are a suggested starting structure, to be adapted to your
market.

## The three tiers

| | Self-serve (browser) | Hosted API (teams) | Private deployment (enterprise) |
|---|---|---|---|
| **How it runs** | Customer uploads a CSV in the console's *Analyze Data* tab; parsing and analysis run entirely client-side | `POST /api/uploads` against a PayLens instance you host; files stream through the governed pipeline | PayLens runs inside the customer's VPC (Docker/Kafka/Postgres), pointed at their live payments database |
| **Volume** | ~100 MB / 1 M rows per file | 200 MB per request (streaming) | unbounded (bounded by their warehouse) |
| **Data leaves customer?** | Never — nothing is uploaded anywhere | Only to your tenant | Never — you never touch the data |
| **Audit trail** | Steps list (client-side replica) | Full governed-query audit log per run | Full audit log, retained in their DB |
| **Delivery** | Static site (Vercel) — already deployed | One container + API keys | docker-compose / Helm + support contract |
| **Suggested price** | Free (open source, lead-gen) | per-run or monthly (e.g. ₹2,000/run, ₹25,000/month for 100 runs — adjust to market) | annual license + implementation fee (custom quote) |

## Why the governance layer is the sales story

* **No free-form SQL** — agents run versioned templates reviewed like code.
  A customer's DBA can read every query the product will ever run before
  signing.
* **Bound parameters + row caps** — bounded results, explicit truncation
  flags; no runaway queries on production payment tables.
* **Complete audit log** — agent, template, row count, duration for every
  call. This is the artifact procurement teams ask for.
* **Pseudonymisation at the boundary** — PayLens only ever sees
  `pseudo_customer_id`; no PII enters the pipeline, which simplifies the
  data-processing agreement dramatically.
* **Human-in-the-loop** — the product recommends, people decide. Selling
  "autonomous payments remediation" would be a harder, riskier promise.

## Cross-service attach (the consulting angle)

The upload paths are designed to be resold by integrators and analytics
consultancies:

1. **Diagnostic-as-a-service** — a fixed-scope engagement: customer exports
   two days of attempts, you deliver the PayLens report (funnel, segments,
   anomaly timing, A/B readout) with a review call. The console's upload
   tab is the productized version of that first meeting.
2. **Retainer monitoring** — nightly `cli.py analyze` / scheduled
   `/api/uploads` runs against the customer's export or replica DB; the
   governed audit log doubles as the delivery artifact.
3. **Experiment readout service** — teams shipping checkout changes get
   SRM-checked, guardrailed readouts without hiring a data scientist; the
   z-test/SRM engine and its tests are the proof it is done right.

## What to say honestly (and what not to)

* The hosted tier is currently **single-tenant per deployment** — one
  store per instance, each upload replaces the dataset (reproducible
  semantics). A multi-tenant control plane with per-tenant stores and API
  keys is the main engineering investment between here and SaaS.
* The client-side engine is a **replica** of the governed semantics for
  zero-friction trials; only the backend path produces the full audited
  trail (governed SQL templates, row caps, query log). Say which one a
  customer is getting.
* The LLM layer is **advisory and optional** — planning and narration with
  a deterministic fallback. Do not sell "AI magic numbers"; sell "the
  numbers are reproducible and every query is auditable".
* No numbers in reports are invented: findings trace to governed queries,
  and the real-data suite (Olist, Upworthy) proves the pipeline behaves on
  messy external data — including producing an honest null when there is
  no incident.

## Suggested go-to-market sequence

1. Ship the static console (done — Vercel-ready, zero config) as the
   public demo with the embedded reference run.
2. Open-source repo (done) as the trust anchor: tests, governance model
   and audit log are inspectable.
3. Self-serve upload tab (done) as the free hook — "bring your own CSV".
4. Hosted API tier behind a paywall once API keys + multi-tenant stores
   land (engineering roadmap above).
5. Private deployments via the existing docker-compose + Kafka/Postgres
   layer for regulated customers (banks, PSPs).
