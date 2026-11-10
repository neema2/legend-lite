# Remote files and their credentials today (homework for B3, 2026-11-10)

Read-only research (an agent of the orchestrator session; nothing run, no secret read or printed).

## DataCube in the browser
- The person types S3 keys into the picker (`datacube/src/ui/source-picker.ts:617-636`); they live in the tab's memory only
  (`app/sources.ts:891`); a saved cube carrying a key is refused (`cube-document.ts:587`).
- The keys travel as a host operation `{op:'secret', name, scope, credentials}` (`remote.ts:103-118`) to the in-tab planner,
  and `DuckDbHostOperations.java:165-181` WRITES THE VALUES INTO THE SQL TEXT:
  `CREATE OR REPLACE SECRET "…" (TYPE S3, KEY_ID '…', SECRET '…', SCOPE '…')`. **The planned SQL holds the secret** — against
  the rule that a plan never holds one. Error messages are redacted by value (`remote.ts:129-145`).
## Python
- The page POSTs `{url, name, s3?}` with the keys in the body (`datacube/src/services/python-host.ts:597`); Python runs the
  same `CREATE SECRET` literals on its DuckDB (`legend_lite/engine.py:701-729`).
## The core product
- Reads no remote file: `SourceAdmission.admit` refuses every scheme but `file:` (`exec/SourceAdmission.java:47-50`);
  `Sessions.openingFor` refuses a credentialed connection (`exec/Sessions.java:505-509`).
- The plan already carries vault references by NAME (`executionplan/AuthenticationSpec.java`, `planner/ConnectionPlans.java:71-79`);
  nothing in exec resolves them yet.
## Native remote reads in the repo
DuckDB only (httpfs, Parquet, CSV, Iceberg; WASM has no Delta, the wheel no Azure). Every other dialect refuses
(`SqlDialect.java:226`). Snowflake stages, BigQuery external tables, ClickHouse `s3()`, Trino, Databricks: no code.

## Shape for B3 (a sketch, for the design review)
A source names its credential (`CredentialRef`: a name, a kind, a scope), never a value; exec holds a secret store
(configuration, like `SourceAdmission`) from name to values, refuses an unknown name by name, opens a session-scoped
secret through the connector, drops it at close; a guard: no field of the plan may hold a credential value.
**Open:** whether DuckDB's `CREATE SECRET` takes bound parameters. If not, the secret must be opened by the connector (the
seam's "opens" act) or through DuckDB's `credential_chain` provider, since exec may not write values into statement text.
