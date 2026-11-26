# PayLens service boundaries

| Component | Responsibility |
|---|---|
| Payment application | Produce pseudonymous payment lifecycle events and stable IDs. Publish complete assignment data independently of checkout outcomes. |
| Kafka worker | Write the event ledger and attempt projection, quarantine malformed records and commit Kafka offsets after PostgreSQL succeeds. |
| PostgreSQL | Keep event IDs unique, store experiment protocols and coverage checkpoints, and persist live reports and their query audits. |
| FastAPI | Execute bounded, read-only evidence queries and persist live report evidence through a separate write transaction. |
| Vercel console | Serve the public replay and open-data benchmarks. Access a private feed only when a token and the two database connections are configured. |

The Compose stack is for local validation. A live deployment requires a managed database, a Kafka service and a continuously running worker. Restrict database access to the relevant event, coverage or audit tables for each service account. Give analysts access through the application’s configured network and identity boundary. Rotate the deployment’s access token outside application code.

The archived one-file browser console in `web/` and the legacy LangGraph routes are available for reference. The live V2 endpoints and current console share the same calculation path for repeatable incident and experiment readouts.
