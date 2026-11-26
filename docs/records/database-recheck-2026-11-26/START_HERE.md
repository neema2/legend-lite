# Lean SQL on every database: START HERE (the follow-up to Stage B)

*Written 2026-11-26 by the edition program's orchestrator, for a session that starts with none of this context.
Status: **researched, nothing built.** It starts right after Stage B lands.*

## 1. Why this exists, in the user's words

- With the fold rule's ruling (2026-11-26, about 18:10 UTC): "relook at any database where the sql blows up - make
  sure we are running latest versions and relook at sql/database docs don't trust our docs one what is and is not
  supported".
- On two earlier claims of ours that were wrong: "H2 has array type I thought?" and "H2 has array type, and bigquery
  has array and struct - array on bigquery has to be single type".
- On when (about 19:15 UTC): "I don't think we should distract ourselves right now on the database list lean sql but
  we should write down all the research and all the context and any artifacts so we can do it next as a follow up
  right after B is done".

The orchestrator agreed with deferring it: the largest fixes change how a list is written, which Stage B's step 6
reworks in its later slices (6.2, 6.4), so doing both at once would do some of it twice.

## 2. What is in this folder

| File | What it is |
|---|---|
| `README.md` | the one ranked list across the eleven databases: sizes today, the one cause, fourteen wins with estimates, our claims that were false, versions, defects found |
| `clickhouse-trino-postgres.md` | the full report: ClickHouse 26.3, Trino 483 and Postgres 16.15 were run; every claim cited or probed |
| `sqlite-mysql-mariadb.md` | the full report: SQLite probed on its driver jar; MySQL and MariaDB from the vendors' documentation; SQLite's one date-time text form; MariaDB's list order |
| `h2-bigquery-snowflake-databricks.md` | the full report: H2 probed on the shipped jar; the three services from the vendors' documentation |
| `probes/` | the probe programs and statements, as `.txt` (the SQL each agent ran on H2, SQLite, Postgres, Trino; the small Java programs that re-plan the measured queries; the scripts that rank the families) |

**Not kept, because they are regenerated in a minute:** the dumps of every planned statement (about 7 MB a group).
`probes/*/planner-dump/Dump.java.txt` is the program: it plans each line of the size baseline's query set with the
product's own planning entry (`Compiler.query(...).executionPlan(...)`, the statements read by
`com.legend.leansql.LeanSqlBaseline.statements`) for a list of database types, and writes database, query number,
length, expression and SQL as tab-separated lines. It runs with plain `java` on the classpath of the built
`//core:lean_sql_baseline_test` (its runfiles' jars), no Bazel run needed once that target is built. **Its totals
must equal `core/src/test/resources/lean-sql/baseline.tsv`'s for the set `channel_a`** (all three passes did:
DuckDB 529,909; ClickHouse 1,774,195; MySQL 2,106,971 and so on); if they do not, the classpath is stale.

## 3. The finding in one paragraph

For the same 2,300 queries DuckDB writes 530 thousand characters and MySQL 2.1 million. Most of the difference has
one cause: **a list, or a computed value, is written more than once per operation.** A null guard writes it twice
(`CASE WHEN xs IS NULL THEN NULL ELSE f(xs) END`); turning a list into rows writes it as the row source and again in
every column that reads an element. Operations nest, so a chain of N list steps writes the first list 2^N times. The
fix has one shape on every database: bind the value once (a one-row derived table, a lateral where the database has
one, the row source's own value column) and read the name. The second cause is particular to ClickHouse: 303
characters of settings on every statement, on the false belief that a setting cannot ride the connection.

## 4. The work, in the order suggested

Each row is a slice of its own. The estimate is from the planned text, not from a change.

| # | Slice | Where in the code (from the reports) | Estimated saving |
|---|---|---|---|
| D1 | **Defects first.** (a) MariaDB returns a `UNION ALL` of literal rows in no promised order: 37 statements, the likely cause of its lane's run-to-run list-order failures (to reproduce on the lane first). (b) ClickHouse parses a text to a Float with its fast parser (`parseFloat` answers a last digit off, the smallest subnormal as zero): `precise_float_parsing = 1`. (c) ClickHouse's setting `query_plan_use_new_logical_join_step = 0` is obsolete from 26.5: before any upgrade, find another guard for the raises it protects. (d) BigQuery: a mixed list's sort orders by a STRUCT, which its documentation says cannot be ordered (to check on the account). | `MariaDb.java` (the literal rows; `SideOrdinal`'s javadoc already records the reordering); `ClickHouse.java` (`SETTINGS`); `BigQuery.java`, `TaggedLists.sort` | none: correctness |
| D2 | **Bind once, MySQL and MariaDB.** `MariaDb.unlessNull` and the list-to-rows form. MySQL may correlate a derived table (`correlatesDerived` is already true and unused by the guard); MariaDB needs a derived table where the list reads no row, else a one-row `JSON_TABLE` | `MariaDb.java`, `MySql.java` | 500 and 350 thousand, plus 320 and 230 for the rows form (overlapping) |
| D3 | **ClickHouse's settings on the connection.** The plan's target declares them, exec passes them as connection properties (`clickhouse_setting_*`, which the test harness already uses) or as an HTTP session's; a session lite did not open reads them back from `system.settings`. The correlated-subquery setting stays on the statement where it is 1. This touches the plan and exec wall (AGENTS.md G4, the charter's A3): design it first, and bring it to the user | `ClickHouse.java` (`SETTINGS`, `query`), `exec/ClickHouseConnector`, the plan's target | 684 thousand (38.5% of ClickHouse's SQL) |
| D4 | **The same bind-once shape** on Postgres (the guard dropped over an array constructor, computed lists bound once), Snowflake (`markerPositions`, `explodedColumn`), BigQuery (`positions`, `unlessNull`), SQLite (`json_each.value`) | each dialect | 56+, 100, 115, 75 thousand |
| D5 | **A literal typed once** per column or array on ClickHouse (`toInt64(n)` inside a typed `values()`) and Trino (`BIGINT 'n'`) | `ClickHouse.java`, `Trino.java` | 90 to 110 thousand each |
| D6 | **Smaller ones:** MySQL's `LATERAL` wrapper around a `JSON_TABLE` argument (110 thousand) and its default `SEPARATOR ','` (20); Trino's own `date_diff` and `date_trunc` (55); ClickHouse's `joinStrings` with the list written once; Snowflake's list statistics (30); H2's literal list read at a literal position (38); identifiers quoted only where they need it (30 to 50 on each of six databases) | each dialect | as given |
| D7 | **H2's list operations.** Most of what `H2.listCall` refuses can be written over `SYSTEM_RANGE` and the array read by index (probed: filter, map, distinct, sort, reverse, slice, position, zip, the aggregates; not fold, not an exact product; `CARDINALITY(NULL)` must be `COALESCE`d). This is the bulk of H2's 135 PCT gaps (`docs/records/h2-pct-full-2026-11-26.md`) and is coverage, not size | `H2.java`, `docs/H2_BACKEND.md` | coverage |

## 5. What Stage B already did, so it is not done twice

- A list of mixed types **written in the query** needs no list in the SQL: membership is `value IN (the elements of
  the value's type)` and two written lists compare position by position (step 6.1; `TaggedLists.contains`,
  `sameWrittenLists`). The reports measured the tree BEFORE this, so their H2 and BigQuery numbers for a written
  mixed list's `contains` are already gone.
- ClickHouse writes a Float literal as the number, not its text parsed (the next batch).
- MongoDB's `acos` and `asin` raise lite's error (the next batch).
- **SQLite's one date-time text form** (`'YYYY-MM-DD HH:MM:SS.fffffffff'`, nine digits always; the source-by-source
  table is in `sqlite-mysql-mariadb.md`, section (a)) is Stage B's, in step 6, because the product's `==` must
  compare a date-time by its instant on SQLite before the asserts become the product's `==` (slice 6.5). If Stage B
  landed without it, it is this program's first slice.
- Step 6.4 reworks how lists are compared on every database. Read what it landed before D2 and D4.

## 6. The rules a slice is held to

1. **The tests' answers judge it** (core, PCT on its database's lane, the corpus), and **the product's SQL size may
   only go down** (`core/src/test/resources/lean-sql/baseline.tsv`, regenerated by `bazel run
   //core:update_lean_sql_baseline`; a row that goes up needs the user's word). SQL text staying the same is not a goal.
2. **Every "this database can or cannot" is read in the vendor's documentation or probed on the database**, in the
   session that relies on it, and the dialect's own notes are corrected in the same slice (the claims table in
   `README.md` lists the ones already found false).
3. **Nothing outside a dialect names a database** (the user, 2026-11-26: "Why is anywhere allowed to name database
   anywhere except dialect?"). A fact about a database is a method of its dialect.
4. **The plan is SQL and data; exec sends the plan's statements** (AGENTS.md, G4 and invariant 3). D3 is the one
   slice that touches this: the settings are the target's declaration, never text exec adds.
5. **One database's lane per slice** before it lands (AGENTS.md, "The fast setup": the lane sessions and how to start
   one). MariaDB's lane is noisy until D1(a) lands.
6. A moved test answer or a new difference from legend-engine is brought to the user with its register row.

## 7. Questions to put to the user at the start

1. **Upgrades.** DuckDB 1.5.6 against 2.0.1; Postgres 16.15 against 19.0; ClickHouse 26.3 LTS against 26.8 LTS;
   MariaDB 11.4 against 11.8 LTS; MySQL 8.4 against 9.7 LTS. Nothing in the reports says an upgrade alone shortens
   much (Postgres 18's `array_sort` and `array_reverse` are the exception); ClickHouse's needs D1(c) first. Upgrade
   as part of this, or separately?
2. **H2's list operations (D7):** in this program, or the product track's own task?
3. **D3's design**, before it is built.
