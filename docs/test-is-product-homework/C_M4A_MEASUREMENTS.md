# C4a. M4a's measurements: who reads the system metamodel store, where, and how many databases it takes (2026-11-01)

M4 (`docs/TEST_IS_THE_PRODUCT_PLAN_2026_10_31.md` §6 B13) puts the graph's system database on one code path. Before
building it, M4a measured the code of 90ea4e5c (the M3 checkpoint) with a probe never committed as code
(`probes/m4a-hooks.diff.txt`, its runner `probes/m4a-run.sh.txt`). The hooks:
- `PlanMaker.metamodel`, the product's read of the store: the dialect, whether it runs in process, whether it has
  temporary tables, how many tables, and the test.
- `SystemDatabase.routeFor`, the test road's route.
- `SystemDatabase.seed`, the test road's open: the engine, the step count and a hash of the steps, so the hash is
  the database's content.

The runs: every core test package, both corpora (both passes) and the five channel B suites, all green.

## 1. The product's reads (`PlanMaker.metamodel`)

| where | plans that read the store | executed |
|---|---|---|
| core's test packages | 11, all from ONE test (`PlanMakerTest.plan`), one per dialect: DuckDB, H2 and SQLite in process; Postgres, MariaDB, MySQL, ClickHouse, Snowflake, Databricks and BigQuery with temporary tables; Trino without | none |
| the corpora, channel B | 0 (their bodies run on the test road) | -- |

No core test executes a product query over the system store. On a server engine today, the product creates the
store's tables as temporary tables on the USER's session (`Metamodel.session`), which B13 retires.

## 2. The test road's routes (`SystemDatabase`)

| where | routes | databases opened (one per graph per engine) | distinct contents (step hashes) | steps per database |
|---|---|---|---|---|
| core's test packages | 77 (integration 76, exec 1) | 75, all DuckDB | **6** (29, 28, 11, 5, 1, 1 opens each) | 114-117 |
| each corpus pass (DuckDB, H2; database and host) | 239, 239, 239, 230 | 1 each | 1 each | 120-121 |
| channel B | 2 (grammar) | 1 | 1 | 114 |

**Reading.**
- **Keeping the system database by its content** (B13 item 2, `PlanSessions`' process-long store, which never
  evicts): core's 75 opens become 6 databases, and the corpus's one stays one. No graph count makes the store grow
  past the distinct metamodels a process plans. The memory risk raised before M4a does not arise.
- **Every route's database is the whole store** (114-121 steps: the session's statements and every table's DDL and
  rows), so a target naming the whole store is what the test road already builds. The product's plans name only the
  tables they read; under B13 item 1 they name the whole store, which makes each graph's plans share one database.
- **A database with no temporary table** (B13 item 4): one product plan in core (Trino, in `PlanMakerTest`), no
  executed one. The test road's Trino lane is the lane sessions' to measure. The rows stay inline there: today's
  form, kept as the named exception.

## 3. The open question (posted to the user before M4b)

On a server engine, B13 item 2 runs the product's store reads on the host's system connection, and refuses by name
when none is configured. Today the product answers them on the user's own session, with temporary tables. A product
query over the store on Postgres, MariaDB, MySQL, ClickHouse, Snowflake, Databricks or BigQuery with no system
connection configured would turn from an answer into a refusal. Core executes none, so nothing in core's tests moves;
a deployment that issues one would. Posted case by case, as D30 was.
