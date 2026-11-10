# Data delivery, and the rest of the program (2026-11-10)

> **Status: for the user's approval.** It was written in the orchestrator session after the checkpoint audits of
> `bec78dfe..5ed43045` (tenets and correctness), the deep review of the branch, and the user's rulings of 2026-11-10:
> - "model data is just another sql statement in the plan";
> - "File is just a pointer like a table";
> - option 1 for databases that cannot read a file.
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
- L4, the catalog;
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
2. **A file is a pointer, like a table.**
   - A file (CSV, JSON, Parquet) is a source the database reads, with its path bound.
   - A model's `file:` source is read by the query directly.
   - A file loaded into a table (`loadCsvToDbTable`) is `INSERT INTO t SELECT … FROM <that source>`.
   - Any file, as upstream. A host may configure an opt-in directory fence, off by default.
3. **A database that cannot read files refuses a file by name** (option 1):
   - **DuckDB** reads them (`read_csv`, `read_json`, `read_parquet`).
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
   of its rows.
2. **No leftovers.** Session tables are named from the load's identity, qualified to the session's temporary schema,
   dropped before they are made and in `finally`. After a failed load the session takes the next load cleanly.
3. **DuckDB aborts its whole transaction on an error.** The catalog's per-run turn is ended and reopened after a failed
   statement, with the run's registrations re-established.
4. **A shared kept session** (the server's project database, a lane's) serves one request at a time, under an explicit
   lock. **No kept database is made per payload.** A `data:` payload's table lives and dies with the request's session.
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
- **A type round trip:** every Pure type through a load on each matrix database.
- **Guards:**
  - ArchUnit: only the load planner builds loads and their data steps;
  - the census, kept honest (`JsonSourceFrame`'s symbols join M10's row);
  - no exec class parses data.
- **Timing:** the corpus and stress suites, against their last times.

## 6. The whole plan, in order

| # | Step | Ends with |
|---|---|---|
| **A** | **Data delivery, rebuilt** | |
| A1 | The matrix and the type round trip, red | the to-do list |
| A2 | Model data as statements in a blob; `Rows`, the streamed and staged deliveries, the appender for model data and the review branch's bound delivery deleted | matrix green for model data |
| A3 | Files as table sources: DuckDB's readers, H2's `CSVREAD` with its checks, the rest refused by name; the Java splitter deleted (kept only where the referee's mirror needs it, at the edge, and named in the census) | matrix green for files |
| A4 | The failure and lifetime rules (§3) | the failure cells green |
| A5 | One road (§4) | every door's cells green |
| A6 | The audit's two parameter defects: H2's time zone spelled with `AT TIME ZONE`; H2's decimal printing typed by the Lowerer | their tests |
| ✔ | **Checkpoint:** tenets audit, then correctness, every finding fixed; one lane round on all eleven databases; the timing check | |
| **B** | **The rest of P1: the census to zero** | |
| B1 | The seed forms (`setUpDataSQLs`) are SQL their author wrote: recorded as the author's statements beside `executeInDb`, not converted | census M7 |
| B2 | Harnesses and the referee seed through the plan's loads; the test-data generator's rows as statements; data parsed and never loaded deleted | census M3, M13, M15, M17 |
| B3 | The TypeScript, Python and DataCube runners run loads: they send statements; a file a browser cannot read is refused by name | census M14 |
| B4 | The server catalog (census M5/M6's last forms): its design written and reviewed first, then built | census M5, M6 |
| ✔ | **Checkpoint:** audits and fixes; the census reads zero pending | |
| **C** | **P4's last two** | |
| C1 | The per-query comment planned, exec's trace kept apart from the statement (D3) | |
| C2 | MariaDB/MySQL's raised-error read-back declared by the plan's target (D4) | exec edits no SQL |
| ✔ | **Checkpoint:** audits and fixes | |
| **D** | **P2, the edge** | |
| D1 | The compatibility code moves into one edge package (engine-text printers, appeals, referee wiring, `RawSqlBoundary`) | |
| D2 | The rule that the product never depends on the edge | |
| ✔ | **Checkpoint:** audits and fixes | |
| **E** | **P3, lean product** (every site ruled, `docs/LEAN_PRODUCT_P3_2026_11_10.md` §12) | |
| E1 | REAL (b'), BigQuery's decimal scale in SQL, zoned raw columns refused, BC dates by the database's text, the kinds-from-text and positional arms deleted, the pivot envelope typed by the plan at the edge | |
| E2 | The writer guard extended to `exec` | |
| ✔ | **Checkpoint:** audits and fixes | |
| **F** | **The end** | |
| F1 | One full run: every database, the local gate, both corpora, both stress suites, H2's PCT, unprivileged Postgres; every break fixed | |
| F2 | One PR for `claude/total-wall` against `main`; the user merges | |

## 7. How we stay on track

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
