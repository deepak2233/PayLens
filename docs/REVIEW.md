# PayLens engineering review

Baseline audited at commit [a0d197e](https://github.com/deepak2233/PayLens/tree/a0d197ea9a4dff3327609ab66223630666a3eb5c). The useful baseline components are canonical event ingest, parameterized SQL templates, a LangGraph investigation, and real-data adapters. These findings compare repository behavior. A model ranking would require the same prompts, tools and scoring.

| Area | Baseline behavior | Updated behavior |
|---|---|---|
| Delivery | Vercel copied one HTML file, and the deployed health endpoint returned 404. A second browser implementation approximated Python behavior for CSV uploads. | The console calls deployed FastAPI endpoints for calculations and shows query evidence. Server-side CSV uploads have isolated stores. |
| Delayed outcomes | The baseline rate excludes all unresolved attempts. It can move when the observation window gets younger or a provider slows down. | The replay compares two complete start cohorts at the same SLA, with pending and late counts exposed. |
| Incident attribution | Volume-weighted segment ranking found the worst slice but did not reconcile routing mix with changes within routes. | Exact midpoint decomposition reconciles within-route, mix and unmatched-route effects to the total delta. |
| Experimental unit | The experiment agent computed its primary metric on settled attempts. Multiple attempts can overweight one user, and assigned users with no observed start disappeared. | The registered readout uses assignment as the denominator, one result per user and one fixed look. Unknown coverage keeps the readout inconclusive. |
| Guardrails | Missing latency and refund observations could enter guardrail tests as zero. Positive Upworthy examples could be labelled rollout candidates. | The experiment requires a reconciled telemetry checkpoint and uncertainty bounds on tolerable increases in timeouts, duplicate charges and refunds. Public aggregate headline readouts stay exploratory. |
| Source meaning | Olist order delivery and cancellation were encoded as familiar payment outcomes. Upworthy did not apply the documented problematic date-range filter. | The public app identifies the grain and outcome of each source. The archive builder and the legacy adapter both exclude documented randomization-risk tests. |
| Operations | The Kafka deserializer could stop on a malformed value. The worker logged questionable data with no durable quarantine. Legacy CSV analysis dropped its global tables. | The worker quarantines record coordinates and a checksum, commits Kafka offsets after the database transaction, and handles replay. CSV processing uses an isolated database. |

## Research decisions

- **FLINT** (October 2026 preprint) reported finance text-to-SQL errors from ambiguous identifiers and used approved query templates and domain-specific lookup. PayLens uses this principle with typed scopes, versioned SQL and bound parameters. Its evaluation measures the calculations and their source lineage. [Sehanobish et al.](https://arxiv.org/abs/2610.03524)
- **CUPED reliability** (June 2026 preprint) studies when pre-experiment adjustments preserve valid treatment estimates. PayLens uses the unadjusted assignment-based estimate because the deployed data does not supply trusted pre-treatment covariates. [Ensuring Trustworthy Online A/B Testing](https://arxiv.org/abs/2606.18750)
- **Sample-ratio mismatch** is a diagnostic for assignment or measurement problems. PayLens checks the preregistered expected split, rejects rogue arms and records conflicting assignments. SRM alone cannot establish valid randomization. [Microsoft’s SRM taxonomy](https://www.microsoft.com/en-us/research/publication/diagnosing-sample-ratio-mismatch-in-online-controlled-experiments-a-taxonomy-and-rules-of-thumb-for-practitioners/)
- **Anytime-valid A/B testing** provides an alternative to fixed-horizon inference when teams repeatedly inspect live results. PayLens withholds the effect before its fixed readout date. Applying sequential inference to this user-level conversion metric would require an estimator and stopping policy validated for that grain. [Lindon et al., AISTATS 2025](https://proceedings.mlr.press/v258/lindon25a.html)

## Known boundaries

Replay reconstructs event-time state from the currently available ledger. A completeness checkpoint represents an operator’s upstream reconciliation. Reproducing exactly what was visible on a prior day would additionally require persistent arrival timestamps and snapshot retention. The hourly onset is a descriptive screening rule and deployment timing is an association. The gateway event schema and guardrail instrumentation need reconciliation with the gateway and settlement ledgers before payment policy changes.

The event fixtures and broker smoke test establish correctness on controlled workloads. Throughput, recovery objectives and live-payment accuracy require measurements on the intended deployment. The single write lock favors correctness at current scale. Partitioned projections and a load test should precede a high-volume rollout.
