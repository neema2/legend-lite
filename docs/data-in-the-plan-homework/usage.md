# Data in the plan: how people will use the platform (homework, 2026-11-09)

Homework for the design session on ONE mechanism for data that must be in a database before a query or test runs
(model `###Data`, a connection's test data, CSV files, generated data, JSON model data, URLs to Parquet/Iceberg/Arrow).
Read-only: nothing in the repository was changed.

**How to read the marks.**
- **[doc]**: what a repository document or the code says, cited `path:line` (paths relative to the repository root).
- **[inf]**: my inference from those facts. Not ruled, not measured.
- **[measure]**: a number nobody has taken yet that the design should take before it rules.

---

## 0. The answer on one screen

1. **Lite already moves data into a database in about fourteen ways, in four different shapes** (section 1). The same
   concept, a project's `###Data`, has two shapes today:
   - on the server it is inline in every product plan's target (`ExecutionPlan.Project.made`);
   - in the tab and in Python it is a separate question to the planner (`projectData`), outside any plan.

   Two of the fourteen put a path on the planning machine into the plan's SQL: D3's `loadCsvToDbTable`, and the
   `file:` arm of `sourceUrl`. One puts the data itself into the SQL text: JSON model data inlined as a string literal.
2. **The workflows split along five axes** (section 2):
   - size: bytes to terabytes;
   - churn: every keystroke, or never (a released version);
   - who pays: the modeller after an edit, the first query of a version, or once per CI process;
   - whether the plan travels: within a process, tab worker to page, machine to machine, or into a cache;
   - trust: the browser sandbox, Python with this machine's files, a multi-tenant server.
3. **One mechanism is possible only as one vocabulary with several kinds of reference, not one representation**
   (section 3, R1):
   - inline rows (small);
   - content-addressed data (a key the runner resolves);
   - a location the database reads itself (a URL and a format);
   - a relation the host already holds (a frame, an uploaded file, a warehouse table: named, never carried).

   Every workflow below needs at least one of the four, and no single kind serves them all.
4. **The load must be separate from the query** (R5). It is a load plan per database identity, derived once, never
   embedded in each query plan. The query plan names the identities it needs and how often each is established.
   - The system catalog design already rules exactly this for the metamodel (`docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md:165-173`).
   - Hole 7 v1 did the opposite for `###Data`: the data is inline in every query plan.
5. **The hardest conflicts** (section 4):
   - self-contained plans (portability, G11) against small plans (G3; one plan per query and per test);
   - load at model load against load at first use (hole 7's open question);
   - upstream's establish-on-every-acquisition against the measured cost of reseeding (58 minutes against 25.5 seconds);
   - one identity scheme for content against user-owned databases and live frames;
   - the security rule R4 (no local files on Python) against `file:` and local CSV paths in tests.

---

## 1. What exists today: every way data reaches a database

| # | Data | Form and where it lives | How it reaches the database | In the plan? | Cadence |
|---|---|---|---|---|---|
| 1 | A model's `###Data` (relational CSV) | `.pure` text in the project; a `PRelationalCsvData` | **Server:** `Database.Declared.project = Project(key, made, refused)`, where `made` is the CREATE and INSERT statements as text (`ProjectData.fill` → `CsvSeed.sqls`). The runner makes the database once per identity and keeps it with no eviction. **Tab and Python:** a separate planner question, `projectData`, answers open, use, fill and close statements; the host ATTACHes `__project_<hash16>`. **Test road:** `project` is null, so the model's Data is not filled. | Server: **inline, in every product plan**. Tab and Python: no. | once, when the database is made |
| 2 | A connection's declared test data (`testDataSetupSqls`, `testDataSetupCSV`) | inside a connection definition in the model | `Target.setup`, written at plan time (`CsvSeed.declaredSteps`). The server runs it on **every acquisition**, as upstream establishes a connection. In-memory databases are shared by the target's content (decision A). | inline | every query (server); once per opened database (shared targets) |
| 3 | A body's inline seeds (`from()`'s `testDataSetupCsv`, a named runtime's) | in the query or runtime | `ExecutionPlan.Setup(key, steps, target)`, skipped on a session that already holds `key` | inline | once per session per key |
| 4 | A service test suite's data (`data: [connections: [id: Reference #{stress::TestData}#]]`) | a `###Data` element referenced by the suite | `ServiceTestRunner` builds an overlay LocalH2-shaped connection whose `testDataSetupCsv` is the suite's CSV. `SHARED` keeps one session per distinct provisioning. | inline, as #2 | once per provisioning per process (`SHARED`), or per test |
| 5 | `loadCsvToDbTable(path, table, conn)` | a CSV file, its path resolved at planning by `options.resources()` | `SqlDml.LoadCsv(..., file)` renders `INSERT ... SELECT * FROM read_csv('<local path>')` | **the planning machine's path** (hole D3) | per body |
| 6 | JSON model data for M2M (`JsonModelConnection url: 'data:application/json,[...]'`) | a data URL in the runtime | DuckDB inlines the payload as a SQL string literal, `unnest(CAST('<payload>' AS JSON[]))`. A `file:` URL reads a local path. | **the data inline in SQL text**, or a local path | every query |
| 7 | The system metamodel's rows | derived from the compiled model | `Setup(SYSTEM_STORE)`: every table's DDL and every row. The platform's share is 327K characters (2,582 rows); the stress model's is 18M characters (117K rows). The agreed catalog design moves this to a registration plan per model, never embedded in a query plan. | inline today | once per session per graph |
| 8 | Literals in the query (PCT, `#TDS`, instance literals) | the query text | `SqlSource.Values` or a `Constant` node | inline (it is the query) | every query |
| 9 | DataCube files (upload, examples) | the user's file | `HostOperation.MakeRelation(..., Source.File(at, CSV\|CSV_TEXT\|PARQUET\|JSON))`, into the host's own DuckDB | no: the plan names the table | at upload |
| 10 | Remote files (Parquet, CSV, Iceberg over http or s3) | object storage | `HostOperation` `Source.Remote(url, format)` makes a view; `Secret` holds one key set per mount, scoped to its path | no: the plan names the view | on every read (the view) |
| 11 | Python frames | pandas, polars or Arrow in process memory | registered as a DuckDB table or view, re-read on every query | no | every query |
| 12 | Warehouse tables | the warehouse | Live: the plan runs there, as the person. Snap: rows copied into the host's DuckDB. | no (Snap is "data in" whose source is another plan's answer) | Snap: once |
| 13 | Generated data | made by the product | Create a Class writes `<pkg>::data::<Table>Sample` (100 rows) as `###Data`. `testdatagen` extracts CSV from a live database. DataCube's 200,000 sample trades are made in the tab as Arrow. The markets sample is 1.87 MB of CSV in one Data element. | becomes #1 or #9 | as #1 or #9 |
| 14 | Parameter values | the request | services bind them as `let`s and compile per call. D1 rules run-time parameters for "plan once, run many". | inline, as constants or bound slots | per call |

**The citations for the table [doc].**

- **#1:**
  - `core/src/main/java/com/legend/executionplan/ExecutionPlan.java:764` (`Project`);
  - `core/src/main/java/com/legend/planner/PlanMaker.java:870-886`: `made` is `SetupStep.Statement` text only, never the
    bulk loader's `Rows`;
  - `core/src/main/java/com/legend/exec/PlanSessions.java:223-262` (`PROJECTS` HandleStore, `projectDatabase`);
  - `core/src/main/java/com/legend/cache/HandleStore.java:22-25` ("No eviction ... Entries live for the process");
  - `wasm/src/main/java/planner/Boundary.java:454`;
  - `engine-client/src/project-data.ts:46-54`;
  - `core/src/main/java/com/legend/setup/ProjectData.java:16-33,39`;
  - `docs/PLAN_AS_DATA_2026_11_07.md:1005-1007` (the test road never fills it).
- **#2:**
  - `core/src/main/java/com/legend/setup/CsvSeed.java:290`;
  - `core/src/main/java/com/legend/exec/ProductRun.java:26-29,61-66`;
  - `docs/PLAN_AS_DATA_2026_11_07.md:1010-1015` (the two cadences);
  - `docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md:270-286` (decision A).
- **#3:** `ExecutionPlan.java:266-275`.
- **#4:** `core/src/main/java/com/legend/test/ServiceTestRunner.java:36-50,112-115`.
- **#5:**
  - `core/src/main/java/com/legend/testplan/EffectPlanner.java:152-175`;
  - `core/src/main/java/com/legend/sql/SqlDml.java:41-46`;
  - `core/src/main/java/com/legend/sql/dialect/AnsiSqlRenderer.java:1887-1899`;
  - `core/src/main/java/com/legend/testplan/TestPlanner.java:141`;
  - `docs/PLAN_AS_DATA_2026_11_07.md:848`.
- **#6:**
  - `core/src/main/java/com/legend/sql/dialect/DuckDb.java:841-870`;
  - `core/src/main/java/com/legend/resolver/JsonSourceFrame.java:27-36`.
- **#7:** `docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md:29-35,44-63,165-173`.
- **#9 and #10:**
  - `core/src/main/java/com/legend/sql/dialect/HostOperation.java:47-70,118`;
  - `docs/ONE_PLATFORM_PROGRAM_2026_10_12.md:876-897`;
  - `docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md:558-559`.
- **#11:** `python/README.md:37-46`.
- **#12:** `docs/ONE_PLATFORM_PROGRAM_2026_10_12.md:584-592`.
- **#13:**
  - `docs/DATACUBE_MODEL_DESIGN_2026_10_10.md:499-502`;
  - `core/src/main/java/com/legend/testdatagen/TestDataGenerator.java` (class doc);
  - `docs/ONE_PLATFORM_PROGRAM_2026_10_12.md:938-942`;
  - `docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md:964-970`.
- **#14:** `docs/GOALS_OF_RECORD_2026_11_07.md:277-305`.

**Two portability facts found while reading [doc, from code at HEAD; not run].**
- The tab's TypeScript plan runner refuses any target that has setup steps:
  `engine-client/src/plan-runner.ts:115`: "a plan that sets its database up first is not run here".
- It also reads only plan version 26 (`plan-runner.ts:52,92-93`), while core writes version 30
  (`core/src/main/java/com/legend/executionplan/PlanJson.java:84`).
- [inf] Today's "data in the plan" shapes (#1-#4) are not runnable by the second runner G11 names, the browser host.
  The version gap suggests nothing currently checks that the two runners stay in step.

**How a database is identified today [doc].**
- Shared targets: the hash of the target's whole JSON (`PlanSessions.java:154`).
- A project's database: the hash of the target's database part, `made` statements included (`PlanSessions.java:245`).
- `Project.key`: a hash of the model's Databases and Data rows (`PlanMaker.java:874`).
- `Setup.key`: the planner's spelling of the seeds' sources (`ExecutionPlan.java:266-275`).
- `Database.System.graphKey`: a hash of the seed steps (`docs/PLAN_AS_DATA_2026_11_07.md:157-162`).
- Upstream keys a local H2 by "a checksum of the setup statements"
  (`docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md:273`).
- **[inf]** So every query on the server serializes and hashes all of the project's `made` statements to find its
  database. Hole 7 v1 left measuring the plan's size with Data inline undone
  (`docs/PLAN_AS_DATA_2026_11_07.md:1075-1078`). **[measure]**

---

## 2. Personas and workflows

For each workflow:
- **Where:** where the data lives.
- **Size:** how big it is.
- **Churn:** how often it changes.
- **Pays:** who should pay the cost of loading it, and when.
- **Travels:** whether the plan must cross a process or machine boundary.
- **Trust:** security and privacy.
- **Today:** what lite ships today.

### W1. A modeller iterating in Studio in the browser (level 0), small `###Data`

- **Where [doc]:** `###Data` elements in the project's files, kept by the in-page SDLC in IndexedDB. Rows go into the
  tab's DuckDB in a database of the workspace's own. Studio "blows away and recreates" that database whenever what
  fills it changes (`docs/ONE_PLATFORM_PROGRAM_2026_10_12.md:838-842`). Studio's Data panel can fill a table from the
  person's own .csv or .parquet, kept over the test data until Reset (`docs/STUDIO_FULL_PLAN_2026_10_04.md:99-103`).
  The runtime's connection is redirected to the tab's DuckDB "a session override, never written into the model"
  (`docs/STUDIO_FULL_PLAN_2026_10_04.md:159-161`).
- **Size:** KB. The demo's `trading-data.pure` is 2.7 KB (`query/demo/models/trading-data.pure`).
- **Churn:** very high: every edit to a row or a table.
- **Pays [inf]:** the modeller, at the next run after an edit, lazily. Loading at model load would reload on every
  keystroke-driven recompile.
- **Travels:** no: the tab's worker plans and the tab runs. The plan crosses the worker boundary as a string [inf].
- **Trust:** the browser sandbox. The person's own files mix with the model's rows.
- **Implies [inf]:**
  - building is cheap and dropping is cheap;
  - an identity that changes with every edit means many short-lived databases, so the owner drops the old one;
  - the person's uploaded file over a declared table is a fifth input beside the model's Data. It is not part of the
    model's content key, but it is part of what the database holds.

### W2. A modeller with a large model and large `###Data`, running queries and tests

- **Where [doc]:**
  - the markets sample is 1.87 MB of CSV in one Data element, over a 2.27 MB trading model;
  - "the model held and its Data filled in 10.4 s", and three compiles of about 4 MB before a page draws
    (`docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md:947-949,964-970`);
  - the stress corpus's `stress::TestData` is 575,531 bytes, 5,339 rows over 455 tables
    (`core/src/test/resources/stress/93-testdata.pure:3-4`).
- **Churn:** occasional.
- **Pays [inf]:** once per (model content, host), at first use, never per query. The stress corpus shows every service
  test reading the same `TestData`.
- **Travels:** tab worker to page, or to a server.
- **Implies [inf]:**
  - inline in every plan is too heavy: 1.87 MB in every query plan, hashed per query;
  - the bulk loader matters: one giant INSERT "cost DuckDB most of a 61 s first query"
    (`docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md:267`);
  - `Project.made` uses plain INSERT statements (`PlanMaker.java:878-882`).

### W3. A Query user opening a published project version by name

- **Where [doc]:**
  - a project version's `###Data` (the demo's rows became its Data element:
    `docs/ONE_PLATFORM_PROGRAM_2026_10_12.md:898-902`);
  - in production, a real database through the runtime's connection.
  - The database is named by content so that "a SNAPSHOT republished with new rows under the same coordinates is never
    answered with the old" (`docs/ONE_PLATFORM_PROGRAM_2026_10_12.md:759-765`).
- **Size:** demo KB. A real database is not data in the plan.
- **Churn:** none for a release. A SNAPSHOT can change.
- **Pays:** the first query of that version on that host. On a server, amortized over every user [inf].
- **Travels:**
  - tab: no;
  - server: planned and run in one process;
  - legacy mode: the UI against legend-engine or lite over the same API
    (`docs/ONE_PLATFORM_PROGRAM_2026_10_12.md:59-63`).
- **Trust [doc]:**
  - the plan holds credentials as references only (`docs/PLAN_AS_DATA_2026_11_07.md:149-151`);
  - "Always the user's own identity ... No service accounts" (`docs/SERVER_PROGRAM_2026_09_26.md:18-21`).

### W4. A DataCube analyst with a local file (CSV, Parquet or JSON), in the tab or on Python's page

- **Where [doc]:**
  - every way data comes in goes into the host's database (`docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md:188-192`);
  - a page's file goes to Python a part at a time, any size (`docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md:879-880`);
  - the browser is bounded by the tab (`docs/STUDIO_FULL_PLAN_2026_10_04.md:347-348`).
- **Size:** MB to GB.
- **Churn:** per upload.
- **Pays:** at upload ("data in"), never per query.
- **Travels:** no. The plan names the table only.
- **Trust [doc]:**
  - "A saved cube never stores data. A file is named by identity (name, format, size, SHA-256, columns)". On reopening,
    the user is asked for the file (`docs/DATACUBE_SAVE_SHARE_2026_09_28.md:148-152`).
  - The handle "stays in this browser, never in a document or a share".
- **Implies [inf]:**
  - a reference to data the host holds, never content;
  - SHA-256 identity plus asking the user for the file is the established product precedent for a content-addressed
    reference that the RUNNING side resolves.

### W5. A DataCube analyst on a large remote Parquet or Iceberg table

- **Where [doc]:** object storage, read through `Source.Remote(url, PARQUET|CSV|ICEBERG)` as a view, with S3 keys per
  mount (`HostOperation.java:57,70,118`).
- **Size:** GB to TB.
- **Churn:** Iceberg snapshots change. DuckDB "reads the metadata's current snapshot on every scan", so pinning closes
  nothing (`docs/ONE_PLATFORM_PROGRAM_2026_10_12.md:1201-1203`).
- **Pays:** the database, on every read. Nothing is copied unless the person Snaps.
- **Travels:** a URL could. Its keys must not.
- **Trust [doc]:**
  - remote-file credentials are never read from the URL (`docs/DATACUBE_PRODUCTION_AUDIT_2026_09_26.md:82-84`);
  - R4: an Iceberg table's data files must be remote on Python (`docs/ONE_PLATFORM_PROGRAM_2026_10_12.md:64-71`);
  - PARK-29: the check holds when it runs, not after (`docs/PARKED_WORK_LEDGER.md:723-760`).
- **Implies [inf]:**
  - a location reference (URL and format) that the database reads itself;
  - never inline, never content-addressed;
  - the plan cannot promise determinism for it (G10).

### W6. A warehouse user (Live and Snap)

- **Where [doc]:**
  - the warehouse's own catalogs;
  - "external data (Parquet / Iceberg on object storage) behind views, so users are granted views and never paths"
    (`docs/SERVER_PROGRAM_2026_09_26.md:87-90`);
  - Live runs the plan there as the signed-in person. Snap copies the result into the host's DuckDB, under a row
    limit (`docs/ONE_PLATFORM_PROGRAM_2026_10_12.md:584-592,667-674`).
- **Size:** unbounded.
- **Churn:** live data.
- **Pays:** the warehouse (Live); the host, once (Snap).
- **Travels:** the plan goes to the warehouse's SQL API. The token stays with Python or the tab.
- **Implies [inf]:**
  - Live is not data in the plan;
  - Snap is "data in" whose content is another plan's answer, a source the mechanism may name ("rows of plan P on
    target T") so Snap and a test's fixture share one form.

### W7. A Python notebook user with dataframes

- **Where [doc]:**
  - frames registered into the process's DuckDB;
  - "Every query reads the frame as it is then, including changes made in place; if its columns changed, its model is
    written again" (`python/README.md:42-46`);
  - a host plans in process (`docs/ONE_PLATFORM_PROGRAM_2026_10_12.md:44-48`).
  - A project's queries on Python run on a project database in Python's DuckDB (C5 step 5,
    `docs/ONE_PLATFORM_PROGRAM_2026_10_12.md:804-805,843-845`).
- **Size:** up to RAM.
- **Churn:** every cell, at any time.
- **Pays:** every query (an Arrow conversion, "a few milliseconds per million rows").
- **Travels:** no.
- **Trust [doc]:**
  - the page sends Pure, never SQL;
  - "only SQL that Python's configured planner wrote is ever run on Python's DuckDB"
    (`docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md:199-200`);
  - a query's own `sourceUrl('file:...')` is accepted: "its your datacube started from your python that could just
    read that file anyway" (`:795-804`).
- **Implies [inf]:**
  - a frame is a relation the host holds, with no content identity: it changes in place;
  - the mechanism must let a plan name it without pretending it is content-addressed, so it is not cached as if
    immutable.

### W8. A server serving many users (pure/v1, services, Studio, Query and DataCube against lite as a drop-in legend-engine)

- **Where [doc]:**
  - every project's Data is held in an in-memory database per (connection, project), kept for the life of the process
    ("The server keeps every database for its life ... an edited row adds one",
    `docs/ONE_PLATFORM_PROGRAM_2026_10_12.md:851-852`);
  - a connection's declared test data runs on every acquisition (`ProductRun.java:26-29`);
  - the server holds no compiled models: "a request has no holder" (`docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md:721-724`).
  - pure/v1 compiles and plans per request, binding parameters as `let`s (`docs/GOALS_OF_RECORD_2026_11_07.md:277-280`).
- **Size:** the sum of every project's Data, unbounded over time [inf].
- **Churn:** each edited Data row makes a new database (no eviction).
- **Pays:** the first query of each version. Under hole 7 v1, every query also carries and hashes the Data [inf].
- **Travels:** the plan stays in process today.
  - The catalog design's product half registers projects at load and replaces them on reload
    (`docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md:177-185`).
  - Hole 7's open decision asks whether a project's Data loads at model load or at first query
    (`docs/PLAN_AS_DATA_2026_11_07.md:955-956`).
- **Trust [doc]:**
  - identity pass-through;
  - a catalog filtered by what the caller may see (`docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md:184-185`).
- **Implies [inf]:**
  - a plan made for user A must not carry anything that is A's only: credentials, or data fetched with A's rights;
  - a plan reused for user B must re-resolve under B's identity;
  - concurrent first uses of one identity must make it once (HandleStore's atomic open does this today).

### W9. A CI job running thousands of service tests (the stress corpus)

- **Where [doc]:**
  - one 575 KB `###Data` referenced by most suites (`core/src/test/resources/stress/92-services.pure:71-83`);
  - a 10.8 MB generated services file (`core/src/test/resources/stress/94-fanout-services.pure`);
  - "keyed by runtime, the stress lane held 46 seeded databases, 26 of them the same stress::TestData, ~340 MB each"
    (`ServiceTestRunner.java:114`);
  - the lane at about 11 GB (`docs/GOALS_OF_RECORD_2026_11_07.md:269`);
  - "Per-test seeding is NOT the answer: it took the lane from minutes to ~50" (`:208-209`);
  - "a fresh database per test took 58 min" against 25.5 s shared (`docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md:282`);
  - registration "about 350 ms × 2,000 stress services" if paid per test
    (`docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md:143`).
- **Size:** KB of text becomes hundreds of MB of database.
- **Churn:** none during a run.
- **Pays:** once per distinct content per process. Never per test, never per plan.
- **Travels:** guard 3 runs the decoded plan on the DuckDB lane. Guard D, the planner-free runner binary, is the goal
  (`docs/PLAN_AS_DATA_2026_11_07.md:289-297`). Sharded CI would ship plans [inf].
- **Trust:** none.
- **Implies [inf]:**
  - identity must be by content, so 26 suites share one database;
  - the data must not be embedded per test plan: about 2,000 plans × 575 KB is about 1.1 GB of plan JSON
    [inf, measure];
  - a body that writes gets its own database (decision A rule 2), or a rolled-back transaction (the catalog design).

### W10. The compatibility corpus and PCT (upstream's own tests as specification)

- **Where [doc]:**
  - setup functions that run SQL text through `executeInDb`;
  - `loadCsvToDbTable` over CSV files in legend-engine's checkout, its path resolved at planning;
  - `testDataSetupCsv`;
  - LocalH2 test data.
  - The checkout is TEST INPUT, never a runtime component (AGENTS.md, the reference-checkout tenet).
- **Size:** small, but thousands of tests.
- **Travels:** the decoded plan (guard 3) and the planner-free binary (guard D). A local path breaks both off-machine
  (`docs/PLAN_AS_DATA_2026_11_07.md:848,952-953`).
- **Trust:** the edge may use raw JDBC and Java. It may NOT put edge-only data into a product plan
  (`docs/GOALS_OF_RECORD_2026_11_07.md:165-168`).
- **Implies [inf]:**
  - the CSV file's content, or its content key, travels, never the path;
  - the referee's ledger texts stay the edge's extension (2.7).

### W11. The browser: the wasm planner and DuckDB-wasm

- **Where [doc]:** the tab's DuckDB. A project's database is ATTACHed in memory: "0.4 ms on DuckDB-WASM"
  (`docs/ONE_PLATFORM_PROGRAM_2026_10_12.md:815-817`). A 4 MB model is held in the module
  (`wasm/src/main/java/planner/Boundary.java:446-453`).
- **Travels:** worker to page, as strings.
- **Runner:** a second runner, in TypeScript (`engine-client/src/plan-runner.ts`), refuses setup and reads version 26
  (section 1).
- **Implies [inf]:**
  - the mechanism must be cheap for a TypeScript runner to implement: data only, a small closed set of step kinds;
  - large data should not be copied through `postMessage` per plan;
  - it should reuse the tab's `ProjectDatabases` lifecycle (`engine-client/src/project-data.ts:46-54`).

### W12. A plan built on one machine and run on another (G11)

- **Where [doc]:**
  - the goal: "A plan can be serialized, read back, and run by any runner, including the WebAssembly planner's host"
    (`docs/GOALS_OF_RECORD_2026_11_07.md:142-144`);
  - guard D runs plans the planning pass wrote to files (`docs/PLAN_AS_DATA_2026_11_07.md:296-297`);
  - not settled: a remote planner writing SQL for the tab's DuckDB
    (`docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md:915-917`).
- **Implies [inf]:**
  - every reference must be resolvable on the receiving side: inline data, a content key with a named place to fetch
    it, or a location the receiver may read;
  - no reference to the planning machine's filesystem;
  - DataCube's "bundle" (page plus project files, `docs/DATACUBE_MODEL_DESIGN_2026_10_10.md:236-244`) and its "one
    file, data included" Parquet (`docs/DATACUBE_SAVE_SHARE_2026_09_28.md:226-230`) are the precedents for a portable
    package: plan, load plans and blobs travel together.

### W13. Plan once, run many (cached and stored plans)

- **Where [doc]:**
  - the move from compile-per-call to plan-once is a later decision, "to be decided when measured"
    (`docs/GOALS_OF_RECORD_2026_11_07.md:304-305`);
  - run-time parameters are typed by their declared Pure type (D1, `:281-301`);
  - DataCube's plan caches were found unbounded (`docs/DATACUBE_AUDIT_PASS2_FINDINGS_2026_09_26.md:2765`).
- **Implies [inf]:**
  - a cached plan outlives the database it last ran on (dropped by a Studio reset, or a process restart);
  - its loads must be idempotent by key, so the plan re-establishes, or fails by name (G7), on a fresh runner;
  - a SNAPSHOT republished gets a new key, so an old cached plan never reads new data under the old identity.

### W14. Sharing and publishing

- **Where [doc]:**
  - a link carries the page, not the data. A share file is Parquet with the page in its metadata ("the whole source
    table the cube reads") (`docs/DATACUBE_SAVE_SHARE_2026_09_28.md:220-243`).
  - Publishing requires a released model, "its data where the published place can read it (no file held only in a tab:
    materialize it into the warehouse)" (`docs/DATACUBE_MODEL_DESIGN_2026_10_10.md:250-256`).
- **Implies [inf]:**
  - data and plan are separable;
  - the recipient resolves data by identity or location;
  - "materialize" is a load whose destination is a shared store. The mechanism's content keys should be what a
    publish step uploads under.

### W15. Reading the catalog and the metamodel (`Class.all()`, a browsable catalog)

- **Where [doc]:**
  - the platform's rows once per process per engine;
  - a project's rows registered at load (product) or inside a rolled-back transaction (tests);
  - "never embedded in a query plan" (`docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md:150-206`).
- **Implies:** the same mechanism, with a third cadence: per transaction, rolled back.

### W16. Generating test data

- **Where [doc]:**
  - Create a Class writes 100 sample rows as `###Data` (`docs/DATACUBE_MODEL_DESIGN_2026_10_10.md:499-502`);
  - `testdatagen` extracts the minimal supporting rows from a live database as CSV;
  - "Generate Sample Data…" in Studio (`docs/STUDIO_FULL_PLAN_2026_10_04.md:155-156`).
  - The test-data generator's interleaved plan-and-run moved to the tests
    (`docs/PLAN_AS_DATA_2026_11_07.md:1205-1206`).
- **Implies [inf]:**
  - generated data becomes model content, `###Data`, so the mechanism is the same as W1, W2 and W9 afterwards;
  - generation itself is a plan whose output is rows, as with Snap (W6).

---

## 3. What these workflows require of one "data in the plan" mechanism

Each requirement names the workflows that drive it.

- **R1. One vocabulary with four kinds of reference.** (all)
  - (a) **Inline rows**, typed by the declared store: test fixtures, PCT, a one-request model, small Data.
  - (b) **Content-addressed data:**
    - a key: a hash of the declared table shapes and the rows; with its size and format;
    - resolved at run from a store the RUNNER is configured with: the in-process memo, a bundle file, a server's store;
    - never from the model, never from the planning machine's path.
  - (c) **A location:** a URL and a format (CSV, Parquet, JSON, Iceberg), optionally pinned, read by the database itself.
  - (d) **A relation the host already holds:** a frame, an uploaded file's table, a warehouse table, a user-named
    database. Named, nothing carried.
  - [doc basis] Lite already has the parts:
    - `HostOperation.Source` is `File | Remote | Relation | Query` (`HostOperation.java:47-66`);
    - `SetupStep.Rows` carries inline rows for the bulk loader (`ExecutionPlan.java:820-865`);
    - `Project.key`, `Setup.key` and `graphKey` are identities.
  - [inf] The design unifies these; it need not invent them.
- **R2. What must never be in a plan.**
  - Secrets: references only (`docs/PLAN_AS_DATA_2026_11_07.md:149-151`).
  - A path on the planning machine (D3; `sourceUrl('file:')`).
  - The compiled model, or a Java object (G4).
  - Edge-only data in a product plan (E3).
  - [inf] Data inline above a small limit: plans are logged (`-Dlegend.diagnostics=sql-sizes` prints statements,
    `docs/CORE_SQL_SIZE_2026_11_04.md:9-11`), round-tripped by tests, cached and shipped. Inline data is data in
    logs, caches and diffs. The DataCube audit already found SQL logging leaking S3 keys
    (`docs/DATACUBE_PRODUCTION_AUDIT_2026_09_26.md:290`).
  - (W3, W8, W10, W12)
- **R3. Identity is "what the database holds".**
  - It is the declared shapes and the rows, so two models whose stores differ never share (D100,
    `docs/PLAN_AS_DATA_2026_11_07.md:1016-1018`).
  - It is computed once at planning (Knowledge, `docs/TENETS.md`) and CARRIED, never recomputed from the contents at
    run.
  - [inf] Today's per-query hashing of the whole target conflicts with this.
  - (W2, W8, W9, W13)
- **R4. The cadence is explicit data.** Three kinds exist and are ruled or designed:
  - once when made: a project's Data, C5;
  - every acquisition: a connection's declared test data, upstream's establish;
  - per transaction, rolled back: test registration, a one-request model.
  - Sources: `docs/PLAN_AS_DATA_2026_11_07.md:1010-1015`; `docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md:201-206`.
  - (W1, W3, W8, W9, W15)
- **R5. The load is separate from the query.**
  - A LOAD PLAN, which is data, per identity, derived once per model.
  - The query plan names the identities it needs and their cadence.
  - A portable package (W12) carries the query plan, its load plans and their blobs together.
  - Already ruled for the metamodel (`docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md:165-173`; inline rejected at `:141`:
    "17.65M characters per query").
  - (W2, W8, W9, W11, W12, W15)
- **R6. Idempotent, atomic and concurrent.**
  - Two first uses of one identity make it once (`HandleStore.java:13-21`).
  - A failed load leaves nothing behind (`PlanSessions.closedOnFailure`; `project-data.ts` drops a half-made database).
  - A load can be re-run safely, which is what makes caching plans and dropping databases safe.
  - (W8, W13, W1)
- **R7. Typed by the declared store, and loaded in bulk.**
  - The DDL comes from the declared types, never a reader's guess (`CsvSeed.java:29-36`).
  - Cells travel as text and the database casts them (F7.2).
  - DuckDB loads through its bulk loader.
  - One owner of the choice (`CsvSeed.planned`).
  - (W2, W9)
- **R8. Size limits, measured.**
  - The numbers to place a limit against:
    - product SQL: median 1.9K characters, p99 9K (`docs/CORE_SQL_SIZE_2026_11_04.md:42`);
    - the demo's Data: 2.7 KB;
    - the stress `TestData`: 575 KB;
    - markets: 1.87 MB;
    - the metamodel: 327K to 18M characters.
  - [inf] Inline only below a limit near the size of a query's own SQL; above it, by content key.
  - (all)
- **R9. A lifecycle, not an eviction policy.**
  - The user ruled "no delete strategy" for project databases (`docs/ONE_PLATFORM_PROGRAM_2026_10_12.md:759-765`).
  - The user rejected cache eviction for the catalog: "we have already lost if we are designing cache eviction
    strategies" (`docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md:139`).
  - The server still needs load, reload and unload (catalog §4.4). Studio drops on every edit.
  - [inf] The mechanism must make drop-and-remake always correct ("dropping early is never wrong",
    `ProjectData.java:17-20`).
  - (W1, W8)
- **R10. The host decides which sources it admits, not the plan.**
  - Python refuses local paths and non-remote Iceberg files (R4, `docs/ONE_PLATFORM_PROGRAM_2026_10_12.md:64-71`).
  - The tab reads no machine files.
  - A server may refuse `file:` entirely.
  - A plan naming a source the runner does not admit is refused by name (G7).
  - (W5, W7, W8, W11)
- **R11. Determinism is visible (G10).**
  - Inline and content-addressed loads are deterministic.
  - A URL load is not, unless it is pinned, and Iceberg cannot be pinned today (PARK-29).
  - [inf] The plan should state which kind each load is, and lite's own tests should use content.
  - (W5, W9, W10)
- **R12. Portable across every runner.**
  - Java's exec, the TypeScript plan runner, Python, and the planner-free binary.
  - A JSON round trip (G11), a version, and a small closed set of kinds.
  - (W11, W12)
- **R13. Tests isolated without a second road.**
  - Sharing for read-only bodies (`SHARED`).
  - A database of their own for bodies that write (decision A rule 2).
  - A rolled-back transaction for registration (catalog §4.5).
  - (W9, W10, W15)
- **R14. Upstream fidelity where it is observable.**
  - A connection's test data is established on every acquisition.
  - LocalH2 is keyed by a checksum of its setup statements.
  - A service test runtime carries the suite's data on its connection.
  - Upstream's plan carries "its whole connection" (`docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md:35-37,273`).
  - Departures are register rows.
  - (W3, W9, W10)
- **R15. The product and the test road are one road (G1).**
  - Today the test road never fills a model's `###Data` (`project` is null), while the server does.
  - The tab and Python use a third shape (`projectData`).
  - [inf] One mechanism means those three collapse to one.
  - (W1, W3, W9)
- **R16. Measurable (G3, G9).**
  - Plan size with data, load time per identity, and memory per identity, each a baseline that may only shrink.
  - (all)

---

## 4. Conflicts the design must resolve

- **C1. Self-contained against small.**
  - G11 and guard D want a plan that runs "from nothing" (`docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md:222-226`).
  - G3 and W2/W8/W9 want every query plan small: no 1.87 MB per query, no 575 KB × 2,000.
  - Hole 7 v1 chose self-contained; the catalog design chose separate.
  - [inf] Resolution space:
    - (i) a query plan names identities, and a portable package (plan plus load plans plus blobs) is the self-contained
      unit;
    - (ii) inline below a size limit, by key above it.
- **C2. Load at model load against first use.**
  - Hole 7's open decision (`docs/PLAN_AS_DATA_2026_11_07.md:955-956`).
  - The catalog design registers products at load: "a browsable global catalog must list projects nobody has queried
    yet" (`docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md:177-179`).
  - TENETS: loading is Work, so lazy (`docs/TENETS.md`).
  - W1 recompiles on every edit, so eager loading reloads on every keystroke.
  - W8 amortizes over users.
  - [inf] The two can differ by kind: catalog rows are Knowledge-sized and needed to browse; `###Data` is Work.
- **C3. Upstream's establish-on-every-acquisition against the measured cost.**
  - Fidelity says a connection's test data is re-established on every acquisition (`ProductRun.java:26-29`).
  - The stress corpus measured 58 minutes per-fresh against 25.5 seconds shared.
  - Decision A shares by content and throws away a database a run changed.
  - [inf] The cadence must be explicit (R4) so fidelity and speed are each stated, not mixed.
- **C4. One identity scheme against databases that have no content identity.**
  - Content keys suit `###Data`, test data and the metamodel.
  - A URL database, a user-named database, a Python frame and a warehouse table have no content key: they change
    under the plan.
  - Hole 7 v1 already exempts them (`docs/PLAN_AS_DATA_2026_11_07.md:1055-1057`).
  - [inf] Kind (d) of R1 must exist, and must never be cached as if immutable.
- **C5. Two shapes today for a project's Data.**
  - Server: inside the plan's target.
  - Tab and Python: a separate `projectData` question and the host's own ATTACH lifecycle.
  - [inf] Unifying moves either the tab toward plans with setup (its runner refuses those today), or the server toward
    a separate load plan (the catalog design's shape).
- **C6. Files: portability against convenience against security.**
  - D3 puts the planning machine's path in a test plan.
  - Python accepts `sourceUrl('file:...')` because the person owns the machine
    (`docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md:800-804`).
  - R4 forbids local files reached through Iceberg metadata.
  - A saved DataCube page identifies a file by SHA-256 and asks the user for it.
  - [inf] File content becomes a content-addressed blob (portable, deterministic). A path stays only as a host-local
    convenience the plan cannot carry across machines.
- **C7. Remote URLs: determinism and security against scale.** Iceberg cannot be pinned, and its manifests can name
  local paths (PARK-29). A URL kind must carry the host's admission rule (R10) and an honest "not deterministic" mark.
- **C8. Credentials and identity against plan reuse.**
  - Plans hold references (2.6).
  - The server runs as the user, with no service accounts.
  - A cached or shared plan must not embed data fetched under one user's rights, such as a Snap's rows, when another
    user runs it.
  - [inf] Content-addressed data loaded from an entitled source carries the entitlement of its source; that is an open
    question.
- **C9. Isolation against memory.**
  - The stress lane holds about 11 GB of shared seeded databases.
  - Per-test seeding takes about 50 minutes.
  - Transactions are cheap on DuckDB through the TEMP staging table (350 ms for 114K rows), but H2 commits on DDL
    (`docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md:83-108`).
  - [inf] Sharing by content plus a lifecycle (release when the last test runtime over an identity finishes) is the
    open middle.
- **C10. Edge texts against product data.**
  - The referee's ledger records seeds' texts as written.
  - Under E3 those texts are the edge's extension, never the product's plan.
  - [inf] The load plan's data form (rows) must not be bent to keep the referee's text form. The edge keeps its own
    text beside it, as 2.7 does today.
- **C11. Studio's person-uploaded rows against content identity.**
  - A table filled from the person's file sits over the model's test data until Reset.
  - That database holds content that is not the model's.
  - [inf] Its identity must include the person's files, or that database is a kind-(d) "host-held" one.
- **C12. Privacy of data in plans against debuggability.**
  - Plans are printed (`generatePlan`, Query's SQL button), logged, diffed and round-tripped.
  - Inline data puts possibly personal rows into those surfaces; content keys keep them out.
  - [inf] This favours keys above a very small limit, even for test data copied from production (W16's
    `testdatagen`).

---

## 5. Questions for the user (to ask in the design session)

1. Is one load plan per database identity, separate from query plans, the shape (R5)? The catalog design already
   rules it for the metamodel. Does hole 7 v1's inline `Project.made` move to it?
2. A project's `###Data`: loaded at model load, or at first use (C2)? Is the answer the same for the catalog rows?
3. Should a portable package (plan plus load plans plus blobs) be the unit G11's guard D runs, rather than a plan that
   inlines every row (C1)?
4. Is there an inline size limit, and at what size? Or always by key?
5. D3: does `loadCsvToDbTable`'s file become a content-addressed blob, read at planning, the runner given its bytes?
   And `JsonModelConnection`'s `data:` URL (inline SQL literal today) and `file:` URL: the same?
6. Should the browser's runner implement loads, or does the tab keep `projectData` as the host's own lifecycle (C5)?
7. Do URL sources (Parquet, Iceberg) belong to this mechanism at all, or stay host operations that the plan only names
   (kind d)?

## 6. Measurements to take before ruling [measure]

- The product plan's size with Data inline:
  - the demo project (`query/demo/verify.mjs`, owed by hole 7 v1, `docs/PLAN_AS_DATA_2026_11_07.md:1076`);
  - the markets project (1.87 MB);
  - a stress service test plan.
- The per-query cost of `PlanSessions.projectDatabase` hashing the target's JSON with `made` in it.
- The stress lane's memory by identity (about 340 MB per shared database), and how many distinct identities it holds.
- The first-query time for `made` as INSERT text against `SetupStep.Rows` through the bulk loader, on DuckDB
  (`PlanMaker.java:878-882` uses INSERT text).
- Whether the TypeScript plan runner's version gap (26 against 30) is covered by any test.

## 7. Sources read

**Documents.**
- `docs/GOALS_OF_RECORD_2026_11_07.md`; `docs/PLAN_AS_DATA_2026_11_07.md` (§0, 2.6-2.10, §3, §11 holes 7 and D3, hole 7
  v1, hole 2, slice 5).
- `docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md`.
- `docs/ONE_PLATFORM_PROGRAM_2026_10_12.md` (§1, B, C1-C5, D5, E, G, §7).
- `docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md` (§1-§4, §15).
- `docs/DATACUBE_SAVE_SHARE_2026_09_28.md`; `docs/DATACUBE_MODEL_DESIGN_2026_10_10.md` (§6-§9, §15.2);
  `docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md` (§11).
- `docs/SERVER_PROGRAM_2026_09_26.md`; `docs/WAREHOUSE_W0_HOMEWORK_2026_09_26.md`;
  `docs/WAREHOUSE_FFM_HOMEWORK_2026_09_26.md`.
- `docs/PROTOCOL_PROGRAM_2026_10_05.md` (§1-§2); `docs/STUDIO_FULL_PLAN_2026_10_04.md` (A2, risks);
  `docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md` (§2-§4, §9).
- `docs/CORE_SQL_SIZE_2026_11_04.md`; `docs/TENETS.md`; `docs/DEFERRED_TEST_EXECUTION.md`.
- `docs/PARKED_WORK_LEDGER.md` (PARK-29, PARK-36); `docs/DATACUBE_PRODUCTION_AUDIT_2026_09_26.md`.
- `wasm/README.md`, `native/README.md`, `python/README.md`.

**Code.**
- `executionplan/ExecutionPlan.java`, `PlanJson.java`, `ConnectionSpecification.java`, `AuthenticationSpec.java`.
- `exec/PlanSessions.java`, `ProductRun.java`.
- `planner/PlanMaker.java`.
- `setup/CsvSeed.java`, `ProjectData.java`.
- `testplan/EffectPlanner.java`, `TestPlanner.java`.
- `sql/SqlDml.java`, `SqlSource.java`, `dialect/AnsiSqlRenderer.java`, `DuckDb.java`, `HostOperation.java`.
- `resolver/JsonSourceFrame.java`.
- `server/ConnectionResolver.java`, `PureV1Api.java`, `LegendHttpServer.java`.
- `test/ServiceTestRunner.java`, `PureTestRunner.java`.
- `cache/HandleStore.java`, `ContentStore.java`.
- `ide/package-info.java` (the IDE layer is dormant: no data workflow there yet).
- `wasm/src/main/java/planner/Boundary.java`.
- `engine-client/src/project-data.ts`, `plan-runner.ts`.
- `core/src/test/resources/stress/9*.pure`, `query/demo/models/`.
