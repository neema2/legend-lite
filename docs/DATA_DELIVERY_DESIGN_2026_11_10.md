# Data delivery, and the rest of the program (2026-11-10)

> **Status: APPROVED by the user, 2026-11-10 ("Do d0 inventory now in parallel while we are doing A").** It was written in the orchestrator session after the checkpoint audits of
> `bec78dfe..5ed43045` (tenets and correctness), the deep review of the branch, and the user's rulings of 2026-11-10:
> - "model data is just another sql statement in the plan";
> - "File is just a pointer like a table";
> - option 1 for databases that cannot read a file;
> - B1 yes;
> - the Java test fixtures bounded;
> - M17 "not implemented yet";
> - the test-data generator is a product feature and stays.
>
> **Updated 2026-11-10 (afternoon)** with the user's rulings on D (errors and print forms, §6.1) and the homework results
> (§7). §6.2 lists the recommendations still waiting for the user's yes; none of them blocks A, B or C.
>
> It replaces the delivery part of `docs/DATA_IN_THE_PLAN_2026_11_09.md` (§2.3-2.5, §4 L4-L6) where they differ. That
> document's other rulings (§8) stand.

## 0. Why

The data-in-the-plan slices each moved one old path onto "a load", built for the one door and the one database they
touched, and tested on the happy path. That had two consequences:
- **Defects.** The checkpoint audits found crashes on unwired doors, a poisoned session, silent truncation, partial
  loads and a DuckDB transaction dying after one error.
- **Machinery.** Bound values, staging tables, per-type casts and a Java CSV splitter, which this design removes.

The slices that were verified stay:
- L2/L3, data as loads;
- the type hole's deletion;
- L4, the catalog on the in-process engines;
- L5, the fixture deletion.

## 1. The model: three ideas, each with one job

1. **Model data is SQL in the plan.** Data known at planning is rendered by the dialect as statements: DDL plus
   chunked multi-row `INSERT … VALUES` with the dialect's own typed literals, the same literal writer every query uses.
   It covers:
   - a connection's test data (its CSV text, read at planning into typed rows);
   - a project's `###Data`;
   - the catalog's rows;
   - a `from()` seed;
   - a `data:` payload.

   The statements always live in a **content blob** the load names by its digest: one form, whatever the size. The plan
   writer puts the blob inside the plan's JSON when the bundle is small and beside it when large, a detail of the writer
   and nothing else. Exec sends the blob's statements unchanged, in one transaction.
   - **The referee** checks a test on legend-engine's own H2, a different database. The planner renders the same rows a
     second time in H2's dialect and carries that copy as the edge's data in the test plan, as it already does for the
     referee's DDL. The referee runs its own copy.
2. **A file is a pointer, like a table.**
   - A file (CSV, JSON, Parquet) is a source the database reads, with its path bound.
   - A model's `file:` source is read by the query directly.
   - A file loaded into a table (`loadCsvToDbTable`) is `INSERT INTO t SELECT … FROM <that source>`.
   - Any file, as upstream. A host may configure an opt-in directory fence, off by default.
   - A remote file that needs a credential names the credential; the plan never holds the secret.
3. **A database that cannot read files refuses a file by name** (option 1):
   - **DuckDB** reads them (`read_csv`, `read_json`, `read_parquet`; in the browser too).
   - **H2** reads CSV in process with `CSVREAD`, its SQL checks refusing malformed records. Its three undetectable
     cases are register rows: a record short by one cell, a record with one extra empty cell, an unclosed quote in the
     last cell.
   - **Every other database** refuses a file load by name.
   - A native loader (Postgres `COPY … FROM STDIN`, MariaDB `LOAD DATA LOCAL INFILE`, ClickHouse `FORMAT CSV`, a cloud
     stage) is added one database at a time, only when someone needs it. Each is "the database reads it", so the model
     does not change.

**No Java parses, binds or casts data.** Exec sends statements and binds a file's path.

## 2. The plan's types

- **`LoadStep.Statement(sql)`**: schema, or SQL a model's author wrote (`testDataSetupSqls`, a seed form,
  `executeInDb`).
- **`LoadStep.Statements(ref)`**: the statements in a content blob, by digest. This is all model data.
- **`LoadStep.File(ref, reads)`**: a file by reference, read by the database through `reads`, its statements with the
  file's path bound.
- **`Binding.Source(location)`**: a `file:` source a query reads, bound at run.

Deleted from the plan:
- `LoadStep.Rows`;
- the `Streamed` delivery and its `Staged` form;
- the bound and chunked cell delivery on the review branch.

## 3. Failure and lifetime rules

1. **A load happens whole or not at all.** It runs in one transaction: its own, or the caller's. A refusal leaves none
   of its rows. **Except where the database takes no transaction** (ClickHouse's driver refuses autocommit off, measured
   2026-11-07 on the timeseries line): there a load's steps run unbracketed, each its own, and its no-leftovers rule is
   rule 2's drop first and drop at release (`DatabaseConnector.takesTransactions`, the orchestrator's check of
   2026-11-10). Upstream runs a connection's setup statement by statement there too, so no register row.
2. **No leftovers.** Session tables are named from the load's identity, qualified to the session's temporary schema,
   dropped before they are made and in `finally`. After a failed load the session takes the next load cleanly.
3. **DuckDB aborts its whole transaction on an error.** The catalog's per-run turn is ended and reopened after a failed
   statement, with the run's registrations re-established.
4. **A shared kept session** (the server's project database, a lane's) serves one request at a time, under an explicit
   lock. **No kept database is made per payload.** A `data:` payload's table lives and dies with the request's session.
   **The browser's uploaded tables** live until the person resets them: a load whose lifetime is the host session.
5. **Every refusal names the data** (the file, the payload or the table) with the database's own words. None is an
   internal exception.

## 4. One road

- **`data:` sources are rewritten wherever any statement is planned:**
  - read, write, program and staged lets;
  - test bodies;
  - the plan that `generatePlan` and `arrowPlan` render. `TypedQuery.plan` renders from the execution plan.
- `resolver/JsonSourceFrame`'s literal path is deleted.
- **A `file:` location is bound on every send path:** `send`, `valuesOf`, `expand`, `bindOnce`, `ProgramRunner`,
  `TestRunner`.
- A door that cannot carry a source refuses it by name before render.

## 5. Proof

- **A test matrix, written first and red before any code:**
  - **doors:** the product read, a product write, plan generation, a test body, a service suite, the server's project
    road;
  - **data kinds:** test data CSV, test data SQL, `###Data` small and large, the catalog, a `from()` seed, a CSV file, a
    `data:` source, a `file:` source;
  - **databases:** DuckDB, H2 and SQLite, plus Postgres run as an unprivileged user;
  - **failure cases:** a bad value, a malformed record, a failure then a clean second load, two concurrent requests, a
    missing or changed file, and a plan read back from JSON and run on a fresh runner.

  A cell that cannot apply is a refusal by name, and that is its test.
- **A type round trip:** every Pure type through a load on each matrix database, and on the referee's H2.
- **Guards:**
  - ArchUnit: only the load planner builds loads and their data steps;
  - no exec class parses data;
  - the census, kept honest (`JsonSourceFrame`'s symbols join M10's row);
  - the Java test fixtures' JDBC seeding unreachable from any product or test-door path.
- **Timing:** the corpus and stress suites, against their last times.

### A1 as built (2026-11-10)

- `core/src/test/java/com/legend/integration/DataDeliveryMatrixTest.java` (in `core_tests_integration`): the six doors
  by the nine data kinds on DuckDB, H2 and SQLite (162 cells); the failure cases (a bad value, a malformed record, a
  missing and a changed file, a failure then a clean load, a bad payload then a clean one, 80 concurrent requests on the
  server's kept database); the type round trip (String, Integer, BIGINT, Float, Decimal, Boolean, StrictDate, DateTime
  through test data and a project's Data, and the referee's H2 copy).
- `DataDeliveryMatrixPostgresTest` (`//core:data_delivery_matrix_postgres_test`, run unprivileged): the same on the
  embedded Postgres, each cell its own workspace database. The server's project road is not run there: the harness's
  declared test connection authenticates as the operating system's user, which the embedded server has no role for.
- A cell expects its rows, or a refusal by name (an internal exception is a crash; a file's refusal must name the
  file). The cells that fail are pinned in each class's `RED`, grouped by the step that clears them; a cell that turns
  green fails the test until it is taken off, and the lists only shrink. One race (two concurrent requests dropping
  one payload's table, the correctness audit's S8) is reported, not compared, until A4.
- Measured at A1: 41 red in process (the from() seed on every door, the write door on declared test data and the
  catalog, `data:` and `file:` on every door but the read, SQLite's boolean, the referee's copy, refusals that name no
  table or payload, SQLite's file load) and 11 on Postgres.

### A2 as built (2026-11-10)

- **The plan:** `LoadStep.Statements(ref)`, a CONTENT blob of the `statements` format (a JSON array of SQL strings,
  `DataRef.Statements.encode`/`decode`, the format's one writer and reader) named by its SHA-256 in the load's bundle.
  `LoadStep.Rows` and the `rows` format are gone. Plan format 38. The plan writer (`PlanJson.writeBundle`) keeps the
  bundle inside the plan's JSON up to 16 KB per plan and moves the largest blobs beside it past that; a blob's digest is
  checked where a plan is read, not on every load's construction.
- **The planner:** `setup.RowLoad` carries each column's declared type; its cells are the dialect's typed literals
  (an integer, a float, a decimal, a boolean, a date, a date-time, a time, JSON, a string), at most 1,000 rows per
  insert. A cell its column's type cannot take is refused at planning, naming the table, the column and the value; a
  number in an integer column that is not an integer is its digits, which the database assigns, as upstream's raw token
  is. `setup.LoadSteps` is the steps with the bundle they read, the one maker of a blob. Every producer takes it: a
  connection's test data and a from()'s seeds (`CsvSeed.planned`), a project's Data (`PlanMaker.project`), the catalog's
  rows and a model's registration (`Ddl.metamodelSteps`, `SystemCatalog`), a `data:` payload (`DataSources`). A server's
  temporary system tables (the form B4 deletes) take the typed literals too, in place of their casts.
- **The referee's copy:** `CsvSeed.planned(steps, dialect, referee)` writes every statement a second time in the
  referee's dialect (the platform's DDL and the rows' inserts; an author's SQL as written), the test plan's edge data
  for a seed (`TestKinds.seed`). B2b replays it.
- **Exec:** `PlanRunner.seed` sends a blob's statements as written, under the load's mark. Deleted: `BulkLoad`,
  `BulkLoads`, DuckDB's `DuckDbAppenderLoad` (the target `:duckdb_load` is `:duckdb_cells`, its cell types alone),
  `Statements.load`, `Sessions.loadsRows`, the census's bulk-load counter, `SqlDialect.loadsRowsInBulk`,
  `planner.ContentRows` and its staging table, `RowLoad.staged`, `//core:duckdb_load_test`.
- **The matrix:** the referee's copy is replayed on its own H2 and read back (green); a bad value is refused naming its
  table on every database (green); SQLite's boolean round-trips. SQLite's date-time still reads back at its text's
  written precision where every other database reads nine subsecond digits: the decoder's rule
  (`JdbcDecode.temporal`), raised as a question, its two cells pinned.
- **Ruled by the user, 2026-11-10 ("Yes to both"):**
  1. A date, date-time or time cell outside its ISO shape is written as text the DATABASE casts (as upstream: quoted,
     the database parses it; Java parses nothing); a database that rejects it fails under the load's name (rule 5).
     A BINARY, VARBINARY, OTHER, DISTINCT, ARRAY or OBJECT column's data stays refused as not implemented yet, by name
     (§9).
  2. The decoder reads a store's declared TIMESTAMP column on SQLite as on every other database (`JdbcDecode.temporal`:
     the database's timestamp, not the text's written precision), so the type round trip's two SQLite cells are green.

### A3 as built (2026-11-10)

- **One form:** `SqlDialect.CsvLoad(statements)`, the database's own reader, each `?` the runner's verified copy's path;
  `LoadStep.Delivery.Reads` alone (plan format 39). Deleted: `Delivery.Streamed`, `CsvLoad.Streamed`, `rowsInsert`
  (Postgres's casts included), `Statements.streams` (JDBC and MongoDB), `DataLoads.cells` and the planner's split of a
  file's cells (census M8's list).
- **DuckDB:** the strict `read_csv`, now with `max_line_size` at the load's size limit (the default 2 MB refused a
  3 MB record). The one try at refusing a record wide by one empty cell in DuckDB's SQL works only where the file holds
  no quoted line break (`null_padding` needs the serial reader, which drops a record whose quote never closes, in
  silence; the parallel reader refuses a quoted line break under `null_padding`): S184's case stays pending for the user.
- **H2:** `SET @legend_csv = ?` then `CSVREAD` with one column more than the table, a cell there refused in the database.
  `CSVREAD` reads an empty cell as NULL as it reads a missing one, so a short record of any width (not only by one, as
  §1 said), a record wide by one empty cell and an unclosed quote load (S186, with the empty line and the quote after a
  space). The pre-L2 check that refused a NULL second-to-last cell would have refused a valid record whose cell there
  is empty; it is not restored.
- **Every other database** refuses a file's load by name at planning, the file named (`EffectPlanner`).
- **The splitter:** `base.CsvCells` keeps one caller, the referee's recorded copy of a file's rows, split lazily when an
  edge asks (census M15, until B2b replays the referee's own copy).
- **The referee's seed copy** (A2's fix, found here): one text per load step, a blob's inserts one script, so the
  edge's ledger, which records a step's text by its place, stays aligned for a table of more than 1,000 rows.

### A4 as built (2026-11-10)

- **Rule 1, whole or not at all:** `PlanRunner.seed` runs a load's steps in one transaction (`Statements.inTransaction`:
  its own, or the caller's joined -- a program's, a catalog turn's). A blob whose second insert fails leaves none of the
  first's rows on DuckDB, H2 and SQLite (`PlanRunnerTest`). H2 commits on DDL, so a load's created table can outlive a
  failed insert; its rows do not.
- **Rule 2, no leftovers:** a `data:` payload's session table is dropped qualified to the session's temporary schema
  (`SqlDialect.temporarySchema`, DuckDB's `temp`: an unqualified drop drops a user's table of the same name when the
  session has none, measured), before it is made and again at release: a target's new `AT_RELEASE` load, run by the
  acquiring source when the request lets its session go, whatever the outcome (`PlanSessions.Source.release`, the
  product door's `finally`). A database the platform keeps made with a target's tables (a lane's shared one) never runs it.
- **Rule 3, DuckDB's aborted turn:** a failed statement inside a run's turn on the catalog rolls the turn back at once
  where the database aborts its transaction (each connector answers `abortsTransactionOnError`: DuckDB, Postgres), the
  session's memo cleared so the run's registration establishes again at its next step (`CatalogTurnFailureTest`); H2,
  which rolls back the failed statement alone, keeps the turn's work.
- **Rule 4, one request at a time:** a view of a kept store's session (the server's project database, a lane's shared
  one) takes the session's lock when made and lets it go when closed (`Sessions.locked`). The matrix's concurrency case
  is compared strictly now: 0 of 80 requests wrong (the race set is empty).
- **Rule 5, refusals name the data:** a load's statements step carries `of`, the data it loads as a refusal names it
  ("the data of the table PEOPLE", "the data: source's payload (2 element(s), its session table ...)"), prefixed to the
  database's own words (plan format 40). The matrix's bad payload is green.
- S173's text updated: the database's reader reads each cell as text and the INSERT casts it.

### A5 as built (2026-11-10)

- **`file:` on every send path:** a `file:` source is admitted by default, any file (the user's ruling, 2026-11-10; the
  fence is opt-in: `SourceAdmission.of(dirs)`, `none()`), and bound ONCE at the run's entry
  (`PlanRunner.admitSources`, called by `PlanRunner.streamRun` and `ProgramRunner.programRun`): every statement of the
  plan that reads a source finds its path, whichever path sends it (the answer, a write, a check, an effect, a keyed
  body's copies, a program's steps).
- **`data:` on every planning path:** `planner.DataSources.Tables` is the one maker of a payload's session table, and
  every statement a program plans reads through it (`ProgramState.read`: a program's statements and the test door's,
  its staged lets' rows, a store write's rows and conditions). A product query's tables are its target's acquisition
  load and its release (A4); a program's are its own load (`AT_STEP`), placed before the first statement that reads the
  payload, and dropped after the program's answer, each drop qualified to the session's temporary schema. A program's
  statements run in one transaction, so a failed program's tables go with its writes where the database's DDL is
  transactional (DuckDB, Postgres, SQLite; H2 and SQLite refuse a `data:` source by name). The product's sequence
  runner takes a program's own load (`PlanRunner.sequence`); a seed or a target's load among its steps is refused.
- **The text plan (generatePlan, arrowPlan):** `TypedQuery.plan` is one statement's text, which carries neither a load
  nor a bound path: a `data:` or `file:` source is refused by name before render
  (`PlanMaker.refuseLiveSourcesInText`), and the execution plan carries both. **Not yet as §4 says:** the text is not
  rendered from the execution plan. Doing so changes the text of every generatePlan and arrowPlan answer (28 test
  classes read it), and no host runs a plan's loads until B3; B3 renders it (its row, §6; the orchestrator's acceptance
  of the sequencing, 2026-11-10).
- **The write door's declared setup:** a write on the caller's session, its declared setup established
  (`ProductRun.Establish.DECLARED_SETUP`), is established as a read is: the program's target's acquisition loads run on
  the session first and its release when the request lets it go (until A5 the caller's session refused them).
- **The catalog in a program:** a product program reading the system metamodel store alone runs on the process's
  catalog, as such a query does (`SystemCatalog.forQuery`; the program re-planned there once its first plan shows it
  reads the store alone, `ProgramState.readsSystem`). One reading the store beside another source, or writing a store,
  is refused by name; on a server database too, until B4.
- **A query given no runtime** runs on the one runtime its `from()`s name (`PlanMaker.runtimeOf`), as lite's server
  chose it (`PureV1Api.runtimeOf`) and as legend-engine runs it: an `ExecuteInput` naming no runtime builds none
  (`legend-engine-core-query-pure-http-api/.../query/pure/api/Execute.java` lines 273-278, `HelperRuntimeBuilder
  .buildPureRuntime` returning null for none, lines 277-279 of `legend-engine-language-pure-compiler/.../toPureGraph/
  HelperRuntimeBuilder.java`), so the plan is the lambda's own and its `from()` places it. None at all stays
  `Compiler.NO_RUNTIME`. **Lite's limit, not upstream's:** a query whose `from()`s name several runtimes, given none, is
  refused by name (upstream plans each `from()` on its own runtime; lite's plan runs on one session). So a `from()`'s
  runtime's test data is the plan's own: its target's setup on the product's doors, a seed on the test door.
- **A database that takes no transaction** (ClickHouse): A4's bracket around a load, and a program's, is skipped there
  (`DatabaseConnector.takesTransactions`, every connector answering; §3 rule 1's exception). Measured on the lane round.
- **The matrix:** in process 162 of 162 green (34 red before A5); on Postgres the three catalog cells B4 owns.
- **The JSON source frame's literal rows are NOT deleted in A5** (measured 2026-11-10): a runtime value's
  `JsonModelConnection` payload for a class no mapping binds still reaches the statement as literal rows
  (`resolver.JsonSourceFrame`). Moving it onto the `data:` road (the frame as the payload's `sourceUrl` relation, its
  row ordinal a column of the payload's table) was built and measured, and reverted: it costs 20 DuckDB and 10 H2
  corpus tests (the XStore and result-sourcing graph fetches). The test road's judges render their statements by roads
  of their own that take no `data:` rewrite (`JudgePlans`, the verdict batch), the referee replays on H2, and H2 holds
  no `data:` table (its dialect has no JSON source column). It moves with B2a and B2b, when the test road's statements
  and the referee's replay take the plan's loads, with H2's `data:` table measured first. Census M10 is REOPENED,
  PENDING B2a (it was counted deleted at L6 with the frame's rows standing; L6's deletions stay guarded).
- **A database that takes no transaction** (ClickHouse): A4's bracket around a load, and a program's, is skipped there
  (`DatabaseConnector.takesTransactions`, every connector answering; §3 rule 1's exception). Measured on the lane round.
- **The matrix:** in process 162 of 162 green (34 red before A5); on Postgres the three catalog cells B4 owns.
- **The JSON source frame** (a runtime value's `JsonModelConnection` for a class no mapping binds) is the payload's
  `sourceUrl` relation of its JSON text and each element's place (`TypedSourceUrl.ORDINAL`, the frame's row identity);
  `DataSources` makes its session table with the ordinal where the relation reads it. Its literal rows
  (`JsonSourceFrame.objectTexts` and the frame's VALUES) are deleted, census M10's list. The one splitter
  (`DataSources.elements`) takes the frame's forms for every `data:` source, as upstream's JSON reader reads a
  `JsonModelConnection`'s data: an array's elements, one object, or a stream of objects; an empty payload is no row
  (until A5 a query's `sourceUrl` took an array alone, and the frame an array of objects alone).

### A6 as built (2026-11-10)

- **H2's time zone** (the correctness audit's S3, a regression from a58c212b: H2 has no `timezone` function, so every
  DateTime parameter on a zoned connection failed at run). `SqlFn.TIMEZONE` keeps DuckDB's two meanings: an instant to
  the zone's wall time, and a plain value read as a wall time in the zone (its instant). One rule tells them apart in
  every dialect that spells both (`AnsiSqlRenderer.zonedInstant`: an operand typed as a zoned timestamp, or a
  `timezone()` of a plain one). H2 spells the first `CAST(FORMATDATETIME(x AT TIME ZONE zone, ...) AS TIMESTAMP(9))`
  (FORMATDATETIME prints a zoned value in its own offset; a CAST of one goes through the session's zone) and the second
  `CAST(CONCAT(CAST(x AS VARCHAR), ' ', zone) AS TIMESTAMP(9) WITH TIME ZONE)`. Measured on 2.5.252 under a non-UTC
  session against DuckDB's `timezone()`: 16 cases equal, DST changes and nine fraction digits included
  (`ZonedDateTimeParameterTest`: a parameter and its literal, in a computed column, answer alike on DuckDB and H2).
- **The other dialects' arms, checked the same way:** DuckDB and Postgres have the function; BigQuery and Snowflake
  already told the two meanings apart; MariaDB and SQLite refuse a zone that is not UTC; MongoDB tells them apart.
  Trino, ClickHouse and Databricks read every operand as a UTC instant, which is right for the first meaning and wrong
  the other way for a plain value read in a named zone (`instantIn`'s inner call: the wall time plus the zone's offset
  where the instant is the wall time minus it). Trino now spells it `CAST(at_timezone(with_timezone(x, zone), 'UTC')
  AS TIMESTAMP(6))` (measured on the embedded Trino 483), ClickHouse `toTimeZone(toDateTime64(toString(x), 6, zone),
  'UTC')` (measured with clickhouse local 25.8) and Databricks `to_utc_timestamp(x, zone)` beside its
  `from_utc_timestamp` (measured on the Databricks lane, runtime 4.2.0, the session in UTC: the orchestrator's relay of
  2026-11-10 18:19 UTC); all three equal DuckDB on the 16 cases. H2's zoned type is named `TIMESTAMP(9) WITH TIME ZONE`
  (its `TIMESTAMPTZ` was no H2 type).
- **At a DST change** `instantIn`'s rule is java.time's (`ZonedDateTime.ofLocal`, stated in its declaration): a
  repeated wall time names its EARLIER offset's instant, a missing one is shifted forward by the gap. Measured: Trino and
  Databricks follow it whole. H2 and ClickHouse read a missing wall time in its later offset (02:30 on 2024-03-10 in New
  York is 06:30 UTC there, 07:30 by the rule). DuckDB reads a repeated one in its later offset (01:30 on 2024-11-03 is
  06:30 UTC there, 05:30 by the rule), and no call or two of DuckDB's chooses the earlier. These are known
  cross-database differences, with no register row: `instantIn` is lite's own function (the Cypher island's, whose
  lanes are DuckDB and Postgres), with no upstream counterpart.
- **A Decimal's or a Number's text** (the tenets audit's M-6): `SqlFn.DECIMAL_TO_TEXT`, a semantic node the Lowerer marks
  by the operand's Pure type, as it marks a Boolean's (`Scalars.pureToString`, `Fold.cellText`). Every dialect writes the
  VARCHAR cast it wrote until then; H2 writes `TO_CHAR` with its leading zero restored for every such operand. The
  dialect's shape walk (`H2.decfloatShaped`) is deleted: it missed an `if`, a `max`, a let and a computed column, where
  `100` printed `1E+2` (`DecimalTextTest`, five shapes and five values, on DuckDB and H2).
- **BigQuery's Date parameter** is upstream's own `PARSE_DATE('%Y-%m-%d', ?)` (`bigQueryExtension.pure` lines 45-47, read
  from the pinned sources; unverified on BigQuery). S182 is withdrawn.

## 6. The whole plan, in order

| # | Step | Ends with |
|---|---|---|
| **A** | **Data delivery, rebuilt** | |
| A1 | The matrix and the type round trip, red | the to-do list |
| A2 | Model data as statements in a blob, the referee's H2 copy beside it; `Rows`, the streamed and staged deliveries, the appender for model data and the review branch's bound delivery deleted | matrix green for model data |
| A3 | Files as table sources: DuckDB's readers, H2's `CSVREAD` with its checks, the rest refused by name; the Java splitter deleted | matrix green for files |
| A4 | The failure and lifetime rules (§3) | the failure cells green |
| A5 | One road (§4) | every door's cells green |
| A6 | The audit's two parameter defects: H2's time zone spelled with `AT TIME ZONE`; H2's decimal printing typed by the Lowerer | their tests |
| ✔ | **Checkpoint:** tenets audit, then correctness, every finding fixed; one lane round on all eleven databases; the timing check | |
| **B** | **The rest of P1: the census to zero** | |
| B1 | The seed forms (`setUpDataSQLs`): the author's statements beside `executeInDb`, not converted (the user: yes) | census M7 |
| B2a | The leftover test-data text on the test road (`SeedTexts`, the planner's own seed path) onto the load planner; and every statement the test road's judges render (`JudgePlans`, the verdict batch) takes the `data:` rewrite, so the JSON source frame's literal rows go (H2's `data:` table measured first; A5 measured the move: 20 DuckDB and 10 H2 corpus tests depend on it) | census M3, M10 |
| B2b | The referee replays its own H2 copy of the plan's loads (from A2) | census M15, the referee |
| B2c | The Java test fixtures' ~770 JDBC statements (103 files) bounded by their guard, not converted (the user: yes) | census M15, the fixtures |
| B2d | **The test-data generator, a product feature:** given a query and a database, it extracts the real rows the query needs, down its join tree, as test data. It becomes a planner function that plans every step upfront and returns one plan; exec runs it; a writer gives the CSV. **Design APPROVED by the user, 2026-11-10** (`docs/data-delivery-homework/TEST_DATA_GENERATOR_DESIGN_2026_11_10.md`): ONE read-only query per table whose `WITH` clauses carry the chain (the starting rows `ORDER BY` key `LIMIT 20`, one level per navigation step, no recursion), all in one read transaction, NO temporary tables; the default starting rows (no row identifiers given) ARE built, by upstream's rule read from the pinned sources; upstream's 46 SQL-text comparisons become a register row (its data checks all run); the TDG appeal and `TestDataGeneratorRun` deleted. S143's skip is lifted when it passes | census M13 |
| B2e | Data parsed but used by no feature yet (ExternalFormat data, ModelStore data, ServiceStore stubs, mapping-test data): recorded as NOT IMPLEMENTED YET, with the rule that when built its data takes the model-data path; listed as feature gaps | census M17 |
| B3 | The browser, Python and DataCube hosts run the plan's loads: model data is statements they send; a dropped file is a pointer DuckDB reads (the CSV/Parquet/JSON upload unchanged); a remote file names its credential; a local path a browser cannot read is refused by name. The browser's regression on `data:` sources is closed. **The text plan renders from the execution plan** (§4; A5 refuses a `data:` or `file:` source there by name until then, the orchestrator's acceptance of the sequencing, 2026-11-10): generatePlan's and arrowPlan's text is the execution plan's, its loads and bound paths carried, the 28 test classes' texts regenerated and EACH change reviewed, never accepted in bulk. **Found by the homework (§7):** today DataCube and the Python app write S3 keys into the SQL text of a DuckDB `CREATE SECRET`, so the planned SQL holds a secret; B3 ends that: the plan names the credential, exec's secret store holds the value, the connector opens a session-scoped secret and drops it at close, and a guard keeps every credential value out of the plan (whether DuckDB takes the key as a bound parameter decides the connector's mechanism; measured first) | census M14 |
| B4 | **Every catalog statement runs on the process's in-process catalog, whatever database holds the user's data** (the user, 2026-11-10: "B4 sounds good for now"). The measurement found no planned statement, among about 30,000 across core, PCT and both corpora, that reads the catalog's tables and a user's tables together: the planner refuses that shape by name. Mixing exists only across separate statements on separate sessions. So no catalog lives on a server: the server forms (per-query temp tables on the user's session, form 2; inline VALUES, form 3) are deleted, and a statement mixing the two stays refused by name. `Class.all()` is the platform plus every loaded project, as ruled 2026-11-08 | census M5, M6 |
| ✔ | **Checkpoint:** audits and fixes; the census reads zero pending | |
| **C** | **P4's last two** | |
| C1 | The per-query comment planned, exec's trace kept apart from the statement (D3) | |
| C2 | MariaDB/MySQL's raised-error read-back declared by the plan's target (D4) | exec edits no SQL |
| ✔ | **Checkpoint:** audits and fixes | |
| **D** | **P2, the edge** | |
| D0 | **Find every piece of compatibility hidden in the product** (the user, 2026-11-10). This covers scalar functions' guards that embed Pure text, SQL that spells a number, date or value the way Pure would print it, error messages formatted for upstream, `EngineStyle` reached from product code, and any other place the product stringifies for compatibility. One inventory lists each site: what it does, the product's TYPED behaviour instead (a guard that raises a typed error, or returns null or NaN, as the function's product semantics say), and what the edge does to stay compatible. Where a product outcome changes (an error where there was text, a null where there was a value), the change goes to the user in one sitting before it lands | the inventory, its rulings |
| D0+ | **D0 is done** (`docs/COMPAT_IN_PRODUCT_INVENTORY_2026_11_10.md` on `claude/p2-compat-inventory`, e7809be7: nine kinds of site with the print forms (§13) and the safe variants (§14), sixteen questions). The print forms were then measured on eleven databases (`docs/data-delivery-homework/PRINT_FORM_PROBE_2026_11_10.md`) | the inventory; the measurement |
| D1 | The compatibility code moves into one edge package: engine-text printers, appeals, referee wiring, `RawSqlBoundary`, and every site D0 found. The product's errors and print forms become §6.1's rules. No product statement carries Pure-formatted text; Pure's and the engine's exact texts are spelled only by the edge | |
| D2 | The rule that the product never depends on the edge, plus a guard that no product SQL or MIR carries a compatibility spelling (Pure-formatted message text, Pure number or date printing) | |
| ✔ | **Checkpoint:** audits and fixes | |
| **E** | **P3, lean product** (every site ruled, `docs/LEAN_PRODUCT_P3_2026_11_10.md` §12) | |
| E1 | REAL (b'), BigQuery's decimal scale in SQL, zoned raw columns refused, BC dates by the database's text, the kinds-from-text and positional arms deleted, the pivot envelope typed by the plan at the edge | |
| E2 | The writer guard extended to `exec` | |
| ✔ | **Checkpoint:** audits and fixes | |
| **F** | **The end** | |
| F1 | One full run: every database, the local gate, both corpora, both stress suites, H2's PCT, unprivileged Postgres; every break fixed | |
| F2 | One PR for `claude/total-wall` against `main`; the user merges | |

### 6.1 The product's errors and print forms (RULED by the user, 2026-11-10)

- **Errors: strict and typed.** A bad input (a zero divisor, the log of a non-positive number, an unparsable text)
  raises a typed error. **A Float divided by zero raises too**, as legend-pure does for any zero divisor (lite's Float
  lane returns Infinity today; a register row): "Float divide zero error is fine - if that ends up causing problem then
  maybe safe divide needed".
- **A product error carries its kind and where it happened in the query**, a fixed text known at planning, not the
  offending value: building the value's text in SQL is what made Snowflake's compile 2.2-2.6x slower. Pure's full
  sentence with the value is spelled at the edge, for PCT and `pure/v1` ("Error whatever is cheap and useful").
- **Safe versions are NOT built now** ("let's not even build them now can push until we need it somewhere"): they are on
  the later list (§9), with their shape kept: a safe version returns empty instead of raising, typed `[0..1]`. The
  timeseries line relies on no bad-input behaviour (it guards its own divides) and would take `safeDivide` first.
- **A value is typed unless the query asks for a string.** A string means one simple text per type, the same on every
  database, produced by the database in one format call ("pick something very very simple and easy for product to do on
  all databases nothing complex"; "Why can't we have a standard string format across databases"):

| type | the product's text | notes |
|---|---|---|
| Integer | plain digits | |
| Decimal | plain digits at its declared scale (`1.50`) | the type for exact amounts (money, Bitcoin): "use decimal where need" |
| Float | **6 decimals, trailing zeros and a trailing `.` trimmed** (`1.5`, `0.3`, `1234567.89`) | the same text on every database measured below about 4.5 billion; below 0.0000005 prints `0`; register rows: exact halfway values round differently by database, `-0` kept or not, binary noise above 4.5 billion on DuckDB, ClickHouse and Snowflake ("let's go back to 6 ... and use decimal where need") |
| Boolean | `true` / `false` | a `CASE` where the database prints `1` |
| StrictDate | `2024-01-02` | |
| DateTime | **ISO 8601 in UTC, six fraction digits and `Z`** (`2024-01-02T10:30:00.123456Z`) | one call on ten of eleven databases; SQLite needs a small expression (its times are text, its format prints milliseconds). Nanoseconds (nine digits) only on the databases that store them (DuckDB's nanosecond type, H2, ClickHouse, Trino, Snowflake), refused by name elsewhere ("Timestamp yes") |
| String, Enum | as is; the value's name | |

  Pure's own exact texts (its Float and date forms, `+0000`, list and instance forms, the TDS text) and the engine's are
  compatibility: the edge spells them.
- **D0's questions (RULED by the user, 2026-11-10, afternoon: "I like all the D0 recommendations except why can't we do
  toString on lists and arrays? Can we just like jsonify them" ... "Great let's do it"):**
  - Q3, Q4: settled by the error ruling above (a product error is its kind and where; the product's own wording per kind
    on every database; no value text).
  - Q5: the engine-only SQL spellings (the flag-gated scan order, the engine lexicon's surplus, upper-casing) move to an
    edge decorator.
  - Q6: compile-time messages in the product's own wording, keyed by kind; legend-engine's words only at `pure/v1`.
  - Q8: pivot column names bare in lite's own CSV and JSON writers; the engine's `__|__` quoting at the edge.
  - **Q12 (changed from the recommendation):** `toString` of a LIST of plain values prints JSON array text, each element
    by the product's print rules above (numbers and booleans bare, strings and dates quoted by the database's JSON
    string quoting), joined by `makeString`'s aggregate: `[1, 2.5, true]`, `["a", "b"]`, `[]`, the same on every
    database (the escaping of unusual characters checked at the lane round). A MAP likewise, as a JSON object
    (`{"k": 1}`). Objects (class instances) and relations stay refused by name, the message pointing to graph fetch and
    serialization. Pure's own `[a, b]` text is the edge's.
  - Q13: `format`'s `%s`, `%d`, `%f`, `%t{}` in the product; `%r` and Pure's sign-after-padding `%0<w>d` at the edge.
  - Q14: `makeString` SKIPS an empty element (Pure's lists hold no nulls); legend-engine's `TDSNull` at the edge.
  - Q15: Pure's exact texts at the edge, as the edge's own SQL render target.

### 6.2 Recommendations waiting for the user's yes

None: D is fully ruled (§6.1, 2026-11-10).

## 7. Homework before its step (read or measured, never guessed)

| Item | For | How | State |
|---|---|---|---|
| Does any statement combine the catalog with user tables? What is undesigned about the server catalog? | B4 | the worker's throwaway probe over core, PCT and the corpora | DONE: none combine; the planner refuses the shape; B4 decided (§6) |
| Every piece of compatibility hidden in the product (D0) | D0 | a read-only research session, in parallel with A | DONE: `docs/COMPAT_IN_PRODUCT_INVENTORY_2026_11_10.md` (branch `claude/p2-compat-inventory`) |
| Which print forms every database makes in one call | D1 | the five lane sessions, read-only, on eleven databases (MongoDB not measured) | DONE: `docs/data-delivery-homework/PRINT_FORM_PROBE_2026_11_10.md`; ruled §6.1 |
| The test-data generator: can every step be planned upfront? (self-references, depth, an empty level) | B2d | read `testdatagen/`, the PCT TDG tests and upstream's TDG as spec input | DONE: one fixed sequence (`docs/data-delivery-homework/TEST_DATA_GENERATOR_HOMEWORK_2026_11_10.md`) |
| Remote files' credentials | B3 | read `datacube/src/remote.ts` and the `Secret` host operation | DONE: keys are written into SQL text today (`docs/data-delivery-homework/REMOTE_FILE_CREDENTIALS_HOMEWORK_2026_11_10.md`); whether DuckDB's `CREATE SECRET` takes bound parameters is measured in B3 |
| MongoDB's print forms | D1 | the lane round | at the A checkpoint |
| The DuckDB wide-by-one-empty CSV record: can DuckDB's own SQL refuse it? | A3 | one try; else a register row for the user | in A3 |
| Every piece of compatibility hidden in the product (scalar guards with Pure text, Pure-style spellings in SQL, upstream-formatted messages) | D0 | read lowering (`Scalars` and the other lowerings), every dialect, the writers and the error paths; grep for Pure-format text in product SQL and messages | before D starts |

## 8. How we stay on track

- **The loop:** the in-process databases (DuckDB, H2, SQLite) on every slice, and Postgres where a slice touches it.
  Every database runs at each checkpoint's lane round and at the end.
- **The scoreboard on every push:**
  - census rows pending;
  - matrix cells red;
  - SQL edits exec still makes at run;
  - the timing check.

  A push that moves none of them, and fixes no crash or wrong answer, is out of scope.
- **Triage:**
  - crashes, wrong answers and wall breaks are fixed;
  - a fidelity edge case outside the wall is refused by name with a register row;
  - a ruled design is never reopened without the user.
- **Effort:** the worker runs at the effort the user set.

## 9. Later, outside this program (recorded so they are not lost)

- Time zones outside an `execute(...)` frame (S183, a known gap; the user: "later").
- A nanosecond Pure type (the user: "later").
- Safe versions of the raising functions (`safeDivide` first, then `safeLog`, the safe parse functions), when something
  needs one (the user, 2026-11-10): typed `[0..1]`, empty in and empty out, in a lite namespace (not
  `meta::pure::functions`) unless Pure's sources already name one.
- P5, judge parity on H2.
- P6, no fallbacks and honest appeals.
- The corpus and stress burn-down.
- Native file loaders per database (§1.3), when someone needs one.
- The features M17 names (ExternalFormat, ModelStore and ServiceStore data, mapping test suites).
- Model data for BINARY, VARBINARY, OTHER, DISTINCT, ARRAY and OBJECT columns (the user's ruling, 2026-11-10: refused by
  name as not implemented yet until then; `RowLoad`).
- One statement that reads both the catalog and a user's tables, a possible product feature (the user, 2026-11-10:
  "later we might want a product feature across both"). Until then it is refused by name.
- PR #81, the timeseries line, in its own session.
