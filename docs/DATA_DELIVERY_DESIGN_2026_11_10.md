# Data delivery: one way, built properly (2026-11-10)

> **Status: DRAFT for the user's review.** Written in the orchestrator session after the checkpoint audits of
> `bec78dfe..5ed43045` (tenets and correctness, 2026-11-10). It replaces the data-delivery part of
> `docs/DATA_IN_THE_PLAN_2026_11_09.md` §2.3-2.5 and §4 L4-L6 where they differ. That document's rulings (§8) stand.

## 0. Why a redo, not patches

The data-in-the-plan slices moved each old path onto "a load", one at a time. Each slice was built for the one door
and the one database it moved, and tested only on its happy path. The checkpoint audits then found basic defects:
- crashes on doors the slice never wired (`generatePlan`, `arrowPlan`, writes, test bodies over a `data:` source);
- a failed load that poisons a kept session until restart;
- Postgres truncating text in silence;
- partial CSV loads;
- H2 time-zone parameters failing at run;
- a DuckDB catalog transaction that dies after one error;
- a race on the server's kept project database.

None of these is a design question. They are gaps of coverage and of failure handling. The verified slices stay:
- data as loads (L2, L3);
- the type hole's deletion (P4's core);
- the catalog (L4);
- the fixture deletion (L5).

**This is not a rewrite.** The consolidation on `claude/total-wall-delivery-wip` already has the right shape (§2). What
was missing is what this document adds:
- a test matrix across every door, data kind and database, written first (§9);
- failure and lifetime rules every load obeys (§5-§6);
- a guard that data never appears in SQL text (§8).

The audits' fixes are then made test-first against that matrix, so they are the design's work, not patches on top.

## 1. The model, in the user's words

**Every byte of data reaches a database one way: through a load in the plan.** A load's data is named by reference:
- **Small data written in the model** (test data, a project's `###Data`, a `data:` payload, the catalog's rows): its
  bytes ride **inline in the plan**.
- **Everything else is a pointer.**
  - Big model data: its bytes beside the plan, in the plan's bundle.
  - A test resource file.
  - A model's `file:` source, read where it lies, any file, as upstream.

**The database receives values bound, never pasted into SQL text**, or it reads the file itself with its own reader.

## 2. The plan's types (from the consolidation on `claude/total-wall-delivery-wip`)

- **`LoadStep.Statement(sql)`** holds only **schema** (create or drop a table) or **SQL the model's author wrote**
  (`testDataSetupSqls`, a seed form, `executeInDb`). It never holds data the platform writes.
- **`LoadStep.File(ref, delivery, sizeLimit)`** is the one data step. `ref` is a `DataRef(namespace, name, digest,
  size, format)`.
  - Namespace:
    - `CONTENT`: the model's own rows, as JSON text in the load's bundle;
    - `RESOURCE`: a test resource;
    - later `URL`.
  - Format: `ROWS` (the model's rows, JSON) or `CSV` (a file).
- **`Delivery`** takes one of two forms:
  - **`Reads(statements)`**: the database reads the bytes itself, through a copy exec writes and binds by path (DuckDB's
    `read_csv`).
  - **`Bound(insert, columns, chunk, chunkRows, staged?)`**: exec binds each cell as text into the dialect's typed
    insert, in chunks. `staged` is the dialect's bulk path (DuckDB's appender into a staging table, then one copy).
  - The consolidation calls `Bound` `Streamed`. Rename it: the cells are bound whether they came from the bundle or
    from a CSV file.
- **`Load.content`, the bundle:** digest → text, checked against its digest where a plan is read. In the plan's JSON it
  is written inline while the plan's bundle is at most 16 KB (largest texts moved out first), and beside the plan as
  one blob per digest above that.
- **A model's `file:` JSON source is not a load.** It is a pointer the query reads, as a table is read:
  `Binding.Source(location)`, bound at run. Any path, per the user's ruling. `SourceAdmission` is an opt-in fence a
  host may configure, off by default.

## 3. The one seam on the planning side

- **One delivery choice, in one place.** `RowLoad` already funnels the model's rows: test data CSV, `###Data`, the
  catalog, the `from()` seeds and `data:` elements. `EffectPlanner` handles a `loadCsvToDbTable` file. The dialect's
  delivery (`Reads` or `Bound`, with or without the staged bulk path) and its failure hygiene (§5) are chosen in one
  routine that both use.
  - Any builder that makes a data step outside that routine is moved onto it.
  - Builders that already go through it are left alone: no blanket rewiring.
  - The ArchUnit rule (§8) pins who may construct a data step.
- **`data:` sources are rewritten at one place that every statement passes:** read, write, program and staged lets,
  test-body statements, and the plan a `generatePlan`/`arrowPlan` caller renders. `TypedQuery.plan` renders from the
  execution plan, never from the lowered query that still holds the source.
  - The literal path in `resolver/JsonSourceFrame.sourceUrlFrame` is deleted. It is the audit's M-1, the census
    row's hidden residue.
  - A statement that cannot carry a source refuses by name before render, never with an internal error.

## 4. The one path on the run side

- **A `file:` source's location is plan data, bound on every send path** (`send`, `valuesOf`, `expand`, `bindOnce`,
  `ProgramRunner`, `TestRunner`). It is taken straight from its `Binding.Source`, with no new run input to thread: any
  file loads, as the user ruled.
  - The optional fence (`SourceAdmission`) is consulted only where a host configured one. Off by default.
  - The `stores` for a `RESOURCE` file are already the load runner's input.
- **`PlanRunner.seed` runs each step:**
  - `Statement`: sent.
  - `File`: bytes resolved (the bundle for `CONTENT`, `stores` otherwise) and verified, then delivered:
    - `Reads`: the database reads the bytes;
    - `Bound`: chunked, through the dialect's insert, or staged through the bulk loader.

  There is one delivery routine for every data kind.

## 5. Failure semantics: a load happens whole or not at all

1. **Atomic.** A load runs inside a transaction: its own when the session owns none, the caller's otherwise.
   - A bad cell, a bad record or a database refusal leaves no row of that load.
   - Upstream converts typed cells in Java before its batch, so a bad cell loads nothing there too. Its rare
     mid-batch partial commit is noted in S173's text.
2. **No leftovers.**
   - Staging and session tables are named from the load's identity and qualified to the session's temporary schema
     (DuckDB `temp.main.`), so a drop never reaches a user's table.
   - They are dropped before they are made, and again in `finally`.
   - After any failed load, the session takes the next load cleanly. Each data kind has a test that proves it.
3. **DuckDB aborts the whole transaction on an error.** On a turned DuckDB session (the catalog's per-run
   transaction), the runner ends the turn after a failed statement and opens a new one, re-establishing the run's
   registrations before the next catalog statement. An `assertError` body followed by a metamodel read is the test.
4. **Refusals name the data.** The file, the payload or the table, and the database's or reader's own words. Never an
   `IllegalStateException`, a `NotImplementedException` or a NullPointerException from inside exec.

## 6. Lifetime and concurrency

- A `PER_ACQUISITION` table lives for the acquisition: dropped when the session is released, whatever the source.
- **A kept, shared session** (the server's project database, a lane's database) serves one request at a time: a
  per-session lock is held from the request's loads to its last row. This makes explicit what the single-threaded
  HTTP dispatcher does today.
- **A kept database is never made per payload.** It is keyed by connection, project and `ONCE` loads, never by a
  query's own loads.
  - A `data:` payload's table lives on the request's session and is dropped with it, so nothing accumulates and no
    eviction policy is needed (the user, on the catalog: "we have already lost if we are designing cache eviction
    strategies").
  - One kept database per distinct payload, which `openShared` does today, is the defect to remove.

## 7. Per database

| Database | CSV file | Model rows (bundle) | `data:` JSON elements |
|---|---|---|---|
| DuckDB | `Reads`: `read_csv(?, strict, …, max_line_size from the size)`. S184's ruled rows; the wide-by-one-empty record refused in SQL if one try finds a way, else its row | `Bound` staged (appender) | `Bound` into a JSON column |
| H2, SQLite, Postgres | `Bound`: cells by upstream's rule (`CsvCells`), in one transaction | `Bound` | `Bound` (H2 `? FORMAT JSON`) |
| MariaDB/MySQL, Trino, BigQuery, Snowflake, Databricks, ClickHouse | `Bound`, each dialect's typed insert | `Bound` | refused by name (as today) |
| MongoDB | refused by name | its own loader, or refused by name | refused by name |

- **Casts.** Each dialect's insert casts a bound text cell to the column's **unsized** type (Postgres
  `CAST(? AS VARCHAR)`, never `VARCHAR(200)`, which truncates in silence) and lets the assignment enforce length and
  precision.
- **Unverified dialects.** Spellings that cannot be verified in process are pinned by spelling tests, marked
  "unverified until the end round" in their commit, and verified on their lanes in the end round.

## 8. The guards

1. **ArchUnit:** only `LoadPlanner` constructs `Load` and `LoadStep.File`. `LoadStep.Statement` is built only by the
   schema and author-SQL paths. This replaces today's seven-maker allowlist.
2. **The sentinel test:** this is the guard that would have caught the drift. Every fixture of the matrix (§9) holds a
   unique marker string in its data. For every plan the matrix makes, the marker appears in no SQL text (statements,
   effects, a load's `Statement`), only in the bundle or a resource. It is red today, at 4 known places (the literal
   inserts and `JsonSourceFrame`), and green when done.
3. **Every door:** a test per door (§9) that a plan with each data kind plans, serializes (JSON round trip, the bundle
   included), runs on a fresh runner, and answers.
4. **The census stays honest:** `JsonSourceFrame`'s symbols join M10's row, and a `.java`-only scan is not used for
   BUILD-file symbols.

## 9. The test matrix (written before the code)

- **Doors (6):**
  - the product read (`Execution.run`);
  - a product write program;
  - plan generation (`generatePlan`, `arrowPlan`);
  - a test body (`TestExecution`);
  - a service-suite test;
  - the server's project road (`runOnProjectDatabases`).
- **Data kinds (8):**
  - connection test data, CSV;
  - connection test data, author SQL;
  - `###Data` below 16 KB and above it;
  - the catalog (platform plus registration);
  - a `from()` seed;
  - a `loadCsvToDbTable` file;
  - a `data:` source below and above 16 KB;
  - a `file:` source.
- **Databases (4):** DuckDB, H2 and SQLite in process, plus Postgres as an unprivileged user.
- **Failure cases (each data kind):**
  - a bad cell;
  - a malformed record or document;
  - a failure mid-load, then a second load on the same session succeeding;
  - two concurrent requests on the server's kept database;
  - a missing or changed file, refused by name;
  - a plan read back from JSON and run on a fresh runner.

Not every cell of the matrix applies (a door that cannot carry a kind refuses it by name, and that refusal is the
cell's test). The matrix lives in one test class per door, parameterized by kind and database.

## 10. What is kept, replaced, frozen

- **Kept as is:**
  - L2, L3 (loads for test data and `###Data`);
  - P4's type-hole deletion;
  - L4a, L4b and the L4 product steps (the catalog);
  - L5's fixture deletion;
  - L6a's pointer idea (`Binding.Source`);
  - L6c's bundle idea;
  - the census, the docs and the GATES records.
- **Rebuilt by this design:**
  - the delivery code of L6b (`DataSources`) and L6c (the bundle's threshold and serialization);
  - the CSV streamer's delivery (3322efce, kept per option (a), now atomic and chunked);
  - the consolidation on `claude/total-wall-delivery-wip` (its types are the basis; its delivery code is rebuilt to
    §4-§6 with the matrix);
  - the admission threading (L6a), now default-off.

  These land as new commits on `claude/total-wall`, with no history rewritten. The review branch is not merged as is.
- **Frozen (no new work):**
  - run-time parameter fidelity: keep what is there, and fix only the audit's two defects (H2's missing `TIMEZONE`,
    spelled with `AT TIME ZONE`; H2's decimal printing typed by the Lowerer instead of guessed in the dialect);
  - CSV edge cases beyond option (a) and the one DuckDB SQL try.

## 11. Build order (DuckDB/H2/SQLite in-process loop; Postgres unprivileged where it is touched)

1. **The matrix and the sentinel test, first and red.** The matrix's failing cells are the to-do list.
2. **`file:` locations bound on every send path,** from the plan; the fence default off.
3. **The one delivery routine:**
   - the `Bound` delivery with the bundle, from the consolidation;
   - unsized casts;
   - any stray builder moved onto it.
4. **Failure semantics and lifetime:**
   - atomic loads;
   - staging hygiene;
   - the DuckDB turn restart;
   - the session lock;
   - no kept database per payload.
5. **The doors seam:** `data:` rewritten for every statement; `TypedQuery.plan` from the execution plan;
   `JsonSourceFrame`'s literal path deleted.
6. **CSV under option (a):** `Bound` atomic and chunked, `max_line_size`, the one DuckDB SQL try.
7. **The two parameter defects.**

Then **the delivery checkpoint**, in three parts:
- **One lane round on every database.** This is an exception to "databases only at the end", because bound inserts
  replace literal INSERTs for every database's test data: the riskiest cross-database change of the program.
- **A timing check:** `corpus_duckdb`, `corpus_h2` and both stress suites against their last times, so the new path does
  not slow every test in silence.
- **The checkpoint audit:** tenets, then correctness. Each asks first whether the matrix and the sentinel are green and
  the census is honest. Every finding is fixed before the next phase.

## 12. The scoreboard (on every push)

- census rows still PENDING;
- sentinel violations;
- matrix cells red;
- SQL edits exec still makes at run (D3's comment stamp, D4's MariaDB read-back).

A push that moves none of them, and fixes no crash or wrong answer, is out of scope.
