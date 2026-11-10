# Data delivery, and the rest of the program (2026-11-10)

> **Status: for the user's go.** It was written in the orchestrator session after the checkpoint audits of
> `bec78dfe..5ed43045` (tenets and correctness), the deep review of the branch, and the user's rulings of 2026-11-10:
> - "model data is just another sql statement in the plan";
> - "File is just a pointer like a table";
> - option 1 for databases that cannot read a file;
> - B1 yes;
> - the Java test fixtures bounded;
> - M17 "not implemented yet";
> - the test-data generator is a product feature and stays.
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
   of its rows.
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
| B2a | The leftover test-data text on the test road (`SeedTexts`, the planner's own seed path) onto the load planner | census M3 |
| B2b | The referee replays its own H2 copy of the plan's loads (from A2) | census M15, the referee |
| B2c | The Java test fixtures' ~770 JDBC statements (103 files) bounded by their guard, not converted (the user: yes) | census M15, the fixtures |
| B2d | **The test-data generator, a product feature:** given a query and a database, it extracts the real rows the query needs, down its join tree, as test data. It becomes a planner function that plans every step upfront (temporary tables joined in the database, recursive SQL for a self-reference) and returns one plan; exec runs it; a writer gives the CSV. Its design is written and reviewed first (§7). S143's skip is lifted when it passes | census M13 |
| B2e | Data parsed but used by no feature yet (ExternalFormat data, ModelStore data, ServiceStore stubs, mapping-test data): recorded as NOT IMPLEMENTED YET, with the rule that when built its data takes the model-data path; listed as feature gaps | census M17 |
| B3 | The browser, Python and DataCube hosts run the plan's loads: model data is statements they send; a dropped file is a pointer DuckDB reads (the CSV/Parquet/JSON upload unchanged); a remote file names its credential; a local path a browser cannot read is refused by name. The browser's regression on `data:` sources is closed | census M14 |
| B4 | The server catalog: first the measurement (does any statement combine the catalog's tables with a user's?), then a design written and reviewed, then built | census M5, M6 |
| ✔ | **Checkpoint:** audits and fixes; the census reads zero pending | |
| **C** | **P4's last two** | |
| C1 | The per-query comment planned, exec's trace kept apart from the statement (D3) | |
| C2 | MariaDB/MySQL's raised-error read-back declared by the plan's target (D4) | exec edits no SQL |
| ✔ | **Checkpoint:** audits and fixes | |
| **D** | **P2, the edge** | |
| D0 | **Find every piece of compatibility hidden in the product** (the user, 2026-11-10). This covers scalar functions' guards that embed Pure text, SQL that spells a number, date or value the way Pure would print it, error messages formatted for upstream, `EngineStyle` reached from product code, and any other place the product stringifies for compatibility. One inventory lists each site: what it does, the product's TYPED behaviour instead (a guard that raises a typed error, or returns null or NaN, as the function's product semantics say), and what the edge does to stay compatible. Where a product outcome changes (an error where there was text, a null where there was a value), the change goes to the user in one sitting before it lands | the inventory, its rulings |
| D1 | The compatibility code moves into one edge package: engine-text printers, appeals, referee wiring, `RawSqlBoundary`, and every site D0 found. The product's guards become typed. No product statement carries Pure-formatted text | |
| D2 | The rule that the product never depends on the edge, plus a guard that no product SQL or MIR carries a compatibility spelling (Pure-formatted message text, Pure number or date printing) | |
| ✔ | **Checkpoint:** audits and fixes | |
| **E** | **P3, lean product** (every site ruled, `docs/LEAN_PRODUCT_P3_2026_11_10.md` §12) | |
| E1 | REAL (b'), BigQuery's decimal scale in SQL, zoned raw columns refused, BC dates by the database's text, the kinds-from-text and positional arms deleted, the pivot envelope typed by the plan at the edge | |
| E2 | The writer guard extended to `exec` | |
| ✔ | **Checkpoint:** audits and fixes | |
| **F** | **The end** | |
| F1 | One full run: every database, the local gate, both corpora, both stress suites, H2's PCT, unprivileged Postgres; every break fixed | |
| F2 | One PR for `claude/total-wall` against `main`; the user merges | |

## 7. Homework before its step (read or measured, never guessed)

| Item | For | How | State |
|---|---|---|---|
| Does any statement combine the catalog with user tables? What is undesigned about the server catalog? | B4 | the worker's throwaway probe over core, PCT and the corpora | running |
| The test-data generator: can every step be planned upfront? (self-references, depth, an empty level) | B2d | read `testdatagen/`, the PCT TDG tests and upstream's TDG as spec input | to do, in the orchestrator session |
| Remote files' credentials | B3 | read `datacube/src/remote.ts` and the `Secret` host operation | to do |
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
- P5, judge parity on H2.
- P6, no fallbacks and honest appeals.
- The corpus and stress burn-down.
- Native file loaders per database (§1.3), when someone needs one.
- The features M17 names (ExternalFormat, ModelStore and ServiceStore data, mapping test suites).
- PR #81, the timeseries line, in its own session.
