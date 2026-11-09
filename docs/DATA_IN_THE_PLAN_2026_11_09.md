# Data in the plan: one load vocabulary, every other mechanism deleted (2026-11-09)

**Status: the user's go of 2026-11-09** ("Lets do the real plan ... I want the right general solution"; "Write into
plan - including provably deleting all other 17 mechanisms please that's the real guarantee that we fixed
everything"). Its open decisions are in §8; nothing in §1-§7 waits on them. It closes D3 of
`docs/PLAN_AS_DATA_2026_11_07.md` §11 and replaces hole 7 v1's inline project fill.

**Progress:** L1 built 2026-11-09 (§4's L1 row, "as built"): D3 is a `File` load by reference, the Windows long path
gone with it; the deletion census (§6, P4) is on with M8's row deleted.

**Evidence:** `docs/data-in-the-plan-homework/` -- `inventory.md` (the 17 mechanisms lite has today, with file:line
and measured sizes), `upstream.md` (what legend-pure and legend-engine do, read from the pinned sources), `usage.md`
(sixteen workflows of the platform's users and what each needs of data in a plan).

**The rule this program enforces:** data enters a database through a LOAD, and only through a load. A load is plan
data: a reference to the data (never a machine's path, and never the rows of anything but small data) and how it is
delivered to the target's database. Exec resolves the reference, verifies its digest and delivers it. Every other way
data reaches a database today is deleted, and guards prove there is no other way left (§6).

---

## 1. Why: what is wrong today

Measured in `inventory.md`:

- **17 mechanisms, 15 shapes, 5 identity schemes, 7 cadences** for one concept: rows that must be in a database before a
  statement reads them.
- **Machine paths in plans:** `loadCsvToDbTable` (D3) and `sourceUrl('file:...')` put the planning machine's path into
  statement text. On the Windows CI runner the D3 path is 308 characters, past Windows' 260, and DuckDB cannot open it
  (PR #92's one red job, 2026-11-09).
- **A resolver function at planning:** `PlanningOptions.resources` is code the planner calls (D3).
- **The same data carried up to three times** in one test plan (the connection's raw text, the setup steps, the session
  target), and the whole project fill inside every server product plan.
- **Data outside every plan:** the browser tab, Python and DataCube load project data, files and URLs with statements
  the platform writes outside any plan; test harnesses seed by JDBC.

## 2. The vocabulary

### 2.1 A reference to data

```
DataRef(namespace, name, digest, size, format)
```

- **namespace:** where the name is meaningful to any runner: `resource` (a repository's test resources), `content`
  (a content store keyed by digest), `bundle` (bytes shipped with the plan), later `url` (§2.4).
- **name:** a name inside the namespace. Never absolute, never a drive letter, never `..`: the record refuses them.
- **digest:** SHA-256 of the bytes. The bytes the plan was planned against, so the same plan loads the same data
  (determinism, G10).
- **size** and **format** (`CSV`, `JSON`, later `PARQUET`).

### 2.2 A load

```
Load(identity, cadence, target, steps)

identity  SHA-256 of a canonical encoding of the load's steps: the declared shapes and every digest in it
          (one scheme for every load, carried in the plan, never recomputed at run)
cadence   BEFORE_STEP      the seeds a program's next statement reads: skipped where the session holds the identity
                           and has run no write since (the seed step's memo), else run and the identity recorded (L1)
          AT_STEP          a program's own load at its place (loadCsvToDbTable): run every time it is reached, a
                           write as an effect is (L1)
          PER_ACQUISITION  established on every session acquisition (a target's setup: L2)
          ONCE             made once per identity and kept (a project's Data: L3)
          PER_TRANSACTION  inside a transaction that is rolled back (the catalog in tests: L4)
target    the database it loads into (the plan's Target)
steps     LoadStep...
```

### 2.3 The steps

```
LoadStep
  Statement(sql)              DDL, or an author's setup statement, written at planning for the target's database
  Rows(table, columns, cells, digest)
                              small data inline: text cells the database casts, through its bulk loader where it has
                              one (today's SetupStep.Rows, with its digest)
  File(ref, table, columns, delivery)
                              data by reference, delivered as the plan says (2.5)
  View(url, format, pin, sql) later (§2.4): the database reads a location itself
  Held(name)                  later (§2.4): a relation the host already holds, named, never copied
```

Small data and large data are one reference: `Rows` is how a small reference travels inline. The threshold is measured
(§8).

### 2.4 Kinds that come later, designed now

- **View (a location):** a URL and a format the database reads itself (Parquet, CSV, Iceberg). Not deterministic unless
  pinned; marked so in the plan; admitted or refused by the runner's policy, never by the plan.
- **Held (a host's relation):** a dataframe, an uploaded file's table, a warehouse table: named, never copied, never
  cached as if it were content.

### 2.5 Delivery: what is the plan's, how is the database's

The plan says WHAT (the reference, the table, its columns) the same way everywhere. HOW the data reaches the database
is chosen at planning for the target's database, as every SQL spelling is (invariant 3):

| Delivery | Databases | What exec does |
|---|---|---|
| `DatabaseReads(sql)` | DuckDB (measured 2026-11-09: `read_csv(?)` takes the path as a bound parameter) | stages the verified bytes in a file of its own and binds that file's path to the statement's one parameter |
| `Streamed(staging)` | H2 (measured 2026-11-09: `CSVREAD(?)` refuses a bound parameter), every server database (they cannot read the runner's files) | splits the verified text into cells and streams them through the database's bulk path (the staging statements written at planning: today's `SetupStep.Rows` staging, Postgres `COPY`) |
| `Uploaded(stage)` | later: warehouses with an upload stage | uploads the verified bytes, then runs the stage's load |

No delivery edits SQL at run: a path is a bound parameter, cells are bound or streamed. Splitting text into cells is
carriage; the database casts every value.

### 2.6 Where loads live in a plan

- **A program's own loads are nodes at their place** (`ExecutionPlan.Load`, `BEFORE_STEP` and `AT_STEP`: L1; until
  then the `Setup` node, the seeds alone).
- **One load section per plan** for the loads a target needs (`PER_ACQUISITION`, `ONCE`), keyed by identity: the
  product's `ExecutionPlan` and the test's `TestPlan` alike. A target names the identities it needs. A load is written
  once in a plan however many statements need it (L2, L3).
- **A portable plan is a package:** the plan, its loads and the bytes of its `bundle` references. A plan alone stays
  small (G3); the package runs from nothing (G11).
- **Plan JSON:** a new version of `PlanJson` and `TestPlanJson`; every runner reads it (§6, P7).

### 2.7 What exec does

1. **Resolve** a reference through the stores the run is configured with. The stores are data, a sealed set:
   `Directory(namespace, root)`, `ContentStore(...)`, `Bundle(bytes by digest)`. They reach the run as its other inputs
   do (the run takes data and sealed sets alone: `ArchitectureTest.aRunTakesDataSealedSourcesAndNamedReceiversAlone`).
2. **Admit or refuse** by the runner's policy: which namespaces, which URL schemes and hosts. A plan cannot widen it.
3. **Verify** the digest of the bytes it read. A mismatch is refused by name.
4. **Deliver** exactly the verified bytes, as the plan says (2.5). The staged copy is the runner's own file (short path,
   cached by digest), so what the database reads is what was verified. The Windows long path disappears with it.

### 2.8 What planning does

- Reads a referenced file once, from a declared resource root (data, not a function), to compute its digest and size:
  knowledge, eager. Data that enters the platform elsewhere (an upload) gets its digest where it enters.
- Chooses the delivery for the target's database and writes its statements.
- Never puts a path in the plan, and never puts the rows of large data in it.

## 3. The 17 mechanisms: each one's fate

Four of the 17 are not loads at all: they are the query language and the user's own program. They stay, and a guard
(§6, P9) proves none of them can carry a load. The other 13 move onto loads and their code is deleted.

| # | Today (inventory §1) | Fate | What is deleted |
|---|---|---|---|
| M1 | `###Data` into the server's project database: `Project.made` inside every product plan's target | a `Load(ONCE)` in the plan's load section, named by identity | `ExecutionPlan.Project.made`/`refused` (the refusal moves to planning the load), `PlanSessions.made`, `ProjectData.fill`'s statement text path |
| M2 | a connection's declared test data on the product path: `Target.setup` | a `Load(PER_ACQUISITION)` | `ExecutionPlan.Target.setup`, `SetupStep` (replaced by `LoadStep`), `PlanSessions.setUp`/`establish`, `refuseSetup` |
| M3 | declared, `from()` and runtime test data on the test road: the `Setup` node, `SeedTexts`, `TestPlan.session.setup`, the raw text in every target | the same loads as M2 (one road, G1) | `ExecutionPlan.Setup` (the node), `Seeds.planned`'s seed path, `SeedTexts`, the raw test data in a plan's connection description |
| M4 | service-suite data: the harness rewrites the runtime into a synthetic `LocalH2` connection | loads planned from the suite's data element | the connection overlay in `ServiceTestRunner`, `RuntimeKey`'s seed part |
| M5 | the system metamodel store on the test road: `SystemRoutes.seeds`, `Setup(SYSTEM_STORE)` | the catalog's registration load (`docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md` §4.3), `PER_TRANSACTION` in tests | `SystemRoutes.seeds`, `Setup.SYSTEM_STORE`, `SystemDatabase` (as the catalog design rules) |
| M6 | the system metamodel store on the product road: three forms by engine | the catalog's loads, `ONCE` per process and per project | the `Target.setup` form, the `Target.session` TEMP-table form, the inline `VALUES` form |
| M7 (seed forms) | `setUpDataSQLs` and the other seed forms, planned through `CsvSeed` | loads | the seed-form arm of `Seeds`/`TestKinds` |
| M8 | `loadCsvToDbTable`: the planning machine's path in an effect's text (D3) | a `File` load step at its program position (`AT_STEP`) | `SqlDml.LoadCsv`, the dialects' `csvSource`, `TestKinds.resourceFile`, `PlanningOptions.resources` (the function) |
| M9 | package setups and on-demand fixtures provided by the harness between a body's plan and its run | loads the plan names; exec establishes them | `PlannedAppeals.stores` as a harness question, `fixturesByStore`, `TestExecution.runFixture`'s seeding role |
| M10 | JSON model data: `data:` inlined as a SQL literal, `file:` as a path in SQL | `data:` a `Rows`/`File` reference; `file:` refused in a plan unless it names an admitted namespace; `http:` a `View` | the JSON-literal inlining, `read_json_objects('<path>')` |
| M13 (output) | the test-data generator's rows enter the next plan as seed text | its output saved as a load | the generator's seed-text hand-off |
| M14 | the browser tab, Python and DataCube load data outside every plan (`projectData`, `testDataSql`, `HostOperation` File/Remote) | the host runners execute loads; a file is a `File` reference, a remote table a `View`, a frame `Held` | `projectData`, `testDataSql`, the `HostOperation` load arms, `engine-client`'s project-data lifecycle |
| M15 | test harnesses seed by JDBC; the referee's mirror replays seed texts | harnesses seed through loads run by the runner; the referee replays the plan's loads | `Seed.sql`, `PostgresSeed`, the harnesses' seeding statements, `SeedTexts` |
| M17 | data parsed and never loaded (ExternalFormat, ModelStore, stubs, mapping-test data) | loaded through the vocabulary where it is relational or JSON; otherwise refused by name where a test or query needs it, never ignored | the silent drop |
| M7 (author's statements) | `executeInDb('INSERT ...')`: SQL the user wrote | **stays**: it is the user's program (world map: programs are input). It is not a platform load | nothing |
| M11 | literal relations and instances in a query, and product writes of them | **stays**: the query's own constants | nothing |
| M12 | model-derived constants in statements (enum value tables, stamps) | **stays**: compile-time knowledge | nothing |
| M16 | parameters, keyed keys, counts, host rows | **stays**: run inputs and values between steps, not loads | nothing |

## 4. The order of work

Each slice lands with every lane green, its deletions done, its guard hard, and its measurements recorded. A slice
that cannot delete what it replaces is not done.

| Slice | What | Deletes | Guard turned on |
|---|---|---|---|
| **L0** | this spec; the homework kept | -- | -- |
| **L1** | `DataRef`, `Load`, `LoadStep` (`Statement`, `Rows`, `File`), the deliveries `DatabaseReads` and `Streamed`; exec's resolver, digest check and staging; stores as run data; plan JSON versions; **M8 (D3) onto a `File` step**. Fixes PR #92's Windows job as a byproduct | M8's list | P5 (no machine path), P6 (digests verified), P4 started (the census, M8's row) |
| **L2** | M2, M3, M4 and M7's seed forms onto loads: one road for test data | their lists | P1 (one record family), P2 (one constructor) |
| **L3** | M1 onto `Load(ONCE)` in the load section | M1's list | P4 census rows M1-M4, M7, M8 |
| **L4** | M5, M6: the catalog design, on loads | their lists | census rows M5, M6 |
| **L5** | M9: fixtures as loads the plan names | M9's list | census row M9 |
| **L6** | M10: JSON model data as references and views | M10's list | census row M10 |
| **L7** | M14: the TypeScript and Python runners execute loads; the cross-runner version guard | M14's list | P7, census row M14 |
| **L8** | M15, M17, M13's output: harnesses and the referee through loads; the never-loaded refused or loaded | their lists | P8, census rows M13, M15, M17; the census complete |

### L1 as built (2026-11-09)

- **Records** (`executionplan`): `DataRef` (refuses an absolute name, a drive letter, a backslash, an empty, `.` or `..`
  segment); `ExecutionPlan.Load(identity, cadence, steps, target)` (the `Setup` node renamed, `BEFORE_STEP` its old
  rule, `AT_STEP` new); `LoadStep` (`SetupStep` renamed) gains `File(ref, delivery)`; `Delivery.DatabaseReads(sql)`,
  `Delivery.Streamed(insert, columns)`. `DataStores` (a directory per namespace) is the runner's and the planner's
  configuration, never in a plan. Plan JSON v31, test plan JSON v11; the TypeScript runner reads v31 (it refuses a
  load node and any target setup, as it refused the setup node).
- **Planning:** `PlanningOptions.resources` (a function) is gone: planning reads the file once through the stores for
  its digest and size; the dialect chooses the delivery (`SqlDialect.csvLoad`: DuckDB reads, H2 streams, every other
  refuses by name). `SqlDml.LoadCsv` holds no file. A file of no row plans no load (it loads what it was planned against:
  nothing). A header whose width is not the table's is refused at planning.
- **Run:** `PlanRunner.seed` runs a `File` step: `DataLoads.verified` (size and SHA-256 against the plan), then
  `DataLoads.stage` (the runner's own temporary copy, deleted after) bound to DuckDB's one `?`, or `DataLoads.cells`
  streamed to H2's insert in one batch. The stores reach the run through the test door (`ExecuteOptions.stores`).
- **The CSV rule** (`base.CsvCells`) is H2's `CSVREAD`'s, measured equal on quoted, padded, short, empty and CRLF input
  (`CsvLoadRenderTest.theStreamedCellsAreTheCellsCsvreadReads`), so the H2 lane loads the rows it loaded before.
- **Interim, named:** the compatibility referee's copy of a loaded file's rows rides the edge's ledger texts at the
  load's step (one insert, the rows split at planning), as every seed's referee copy does today (M15); L8 replaces it
  with the referee replaying the plan's loads.
- **Proof:** `DataLoadsTest` (both deliveries end to end on every in-process database that spells one, through a store
  at a long path; the plan JSON holds no path and reads back; a digest that differs and a run with no store refused by
  name; path-shaped names refused); `PlanTypesAreDataTest` already refuses a `Path` in any plan record;
  `DataMechanismCensusTest` (the 17 rows, M8 deleted, its symbols at zero in every source tree); the corpus's
  `testLoadCsv` passes on the DuckDB and H2 lanes.

## 5. Upstream

lite's choices here are its own design. Where one differs observably from legend-pure or legend-engine (for example a
project's `###Data` made once and kept, which upstream does not do for a product query), it is a row of
`docs/SEMANTICS_REGISTER.md` the user rules, posted before it lands (the fidelity tenet's ledger, not its argument).

## 6. The proof: how we know every other mechanism is gone

The guarantee is mechanical. Each is a test that fails if a second way back opens.

- **P1 One record family.** The plan records (`executionplan`, `testexecutionplan`) hold data for a database only in
  `Load`/`LoadStep`: no other record has a row-shaped field (`List<List<String>>`, bytes) but `Constant` (an answer the
  program wrote, not a load). `PlanTypesAreDataTest` extended.
- **P2 One constructor.** Only the load planner constructs `Load` and `LoadStep` (ArchUnit).
- **P3 One deliverer.** Only exec's loader sends a load's statements, streams its rows or binds a staged file: the JDBC
  census pins the LOADS act and a load's sends to it (`JdbcSurfaceCensusTest`).
- **P4 The deletion census.** A source census lists, per mechanism M1-M17, its deleted symbols (§3's last column),
  each reading zero in every source tree, and for the four that stay, the guard that bounds them. A row per mechanism,
  all 17: the census fails if a deleted symbol returns or a mechanism has no row.
- **P5 No machine path.** `DataRef` refuses absolute names, drive letters and `..`; no plan record has a
  `java.nio.file.Path`; the plan JSON round-trip on every lane refuses a path-shaped string in a load.
- **P6 Digests verified.** Exec refuses bytes whose digest is not the plan's; a test proves it.
- **P7 Every runner reads the same plan.** The TypeScript runner's plan version equals `PlanJson.VERSION` (a test reads
  both); a runner executes each load kind or refuses it by name.
- **P8 Test code seeds through loads.** A source guard over the test trees: no seeding statement sent by a harness
  outside the runner, pinned at today's count and ratcheted to zero by L8.
- **P9 The four that stay carry no load.** An effect's statements are the author's text, folded (no platform-made rows);
  a statement's literal rows come only from the query's own literals (`SqlSource.Values` from lowering); constants
  only from the compiled model; parameters only from the caller. Each by a guard on its one producer.

## 7. What this does not change

- The catalog design (`docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md`) stands; L4 builds it on this vocabulary.
- The separation guards of slice 5 stand; the stores are one more sealed run input.
- Product writes (`write`, `merge`, decision A) stay statements of the query.

## 8. Decisions (ruled by the user 2026-11-09 where marked; the rest each with a recommendation)

1. **Where data lives in a plan (ruled 2026-11-09).** A file is always a reference, at any size: it has a store, so
   nothing is gained by copying it into the plan. Data written in the model text (`testDataSetupCsv`, `###Data`) is
   inline below a threshold, with its digest, and above it goes to the content store and the plan carries a reference.
   The threshold is measured first (the demo, markets at 1.87 MB, a stress body); about 16 KB, near a query's own SQL
   at p99, is the starting proposal.
2. **When a project's `###Data` loads (ruled 2026-11-09).** At first use, `ONCE` per identity, and kept: a register
   row (`docs/SEMANTICS_REGISTER.md` S145). The catalog's rows load with the model, as its design rules.
3. **(Folded into 2.)**
4. **The runners' default admission policy.** Recommended: `resource`, `content` and `bundle` everywhere; `url` only
   where the host enables it, never a local `file:`.
5. **Staging cache.** Recommended: by digest, per process; dropping it is always correct. (L1 stages per load and
   deletes the copy after it; no cache.)
6. **A file's format is declared, one meaning on every database (ruled 2026-11-09).**
   - **The meaning of a file's cells is the platform's, never a database's.** The format is data on the reference
     (today `DataRef.format`, a typed CSV-format record from L2). The dialect spells it in its database's reader
     (`read_csv`'s options, a `COPY`'s) or, where the reader cannot express it, chooses the streamed delivery. The
     delivery is the dialect's (2.5); the cells are not. Letting each database read a file its own way would load one
     file as different rows on different lanes (measured below), the one dialect decision that changed an answer.
   - **The default is DuckDB's rule (the user's pick), which is upstream's** (measured 2026-11-09, read from the pinned
     sources): legend-pure's `loadCsvToDbTable` reads the file in Java (`CsvReader.readCsv`: Apache Commons CSV 1.5,
     `CSVFormat.EXCEL`), drops the header, reads an empty cell as NULL (`LoadToDbTableHelper.collectIterable`:
     `StringIterate.isEmpty`) and binds the rest. On `  Joe  ,"",` both give `[  Joe  ]`, NULL, NULL; a doubled quote,
     CRLF and a space after a closing quote read alike. **So the default needs no register row, and compatibility mode
     needs no format of its own.** (The design said until now that legend-engine reads the file with H2's `CSVREAD`; the
     sources say otherwise: `CSVREAD` is H2's, used by legend-engine for other loads.)
   - **H2's `CSVREAD` rule is nobody's.** It trims a padded cell and reads `""` as an empty string. The H2 lane loaded
     files by it before L1 (`CSVREAD` in the statement) and since L1 (`base.CsvCells`, its rule measured equal): a
     divergence on `main`, register row S146 (pending), removed by L2, whose streamed rule is the default.
   - **A malformed row is an error, never repaired** (invariant 4). Measured: a short row -- DuckDB refuses it, upstream
     binds too few values; a long row -- upstream fails on the extra cell, **DuckDB as lite configures it returns no
     row and no error** (it must be configured to refuse: L2); an empty line -- DuckDB skips it, upstream reads a record
     of one empty cell. L2 measures what each of upstream's does at the insert and writes the rule to refuse each.
   - **User options: a small closed set, added when a real file needs one** (null text, quoted empty, trim, delimiter,
     quote and escape, header; UTF-8 only to start), each a typed value with the default's value as its default. Never
     an option that is a fallback (skip bad rows, ignore errors, pad a short row), never a database's raw option passed
     through, never a per-database option. Declared on the data (the file, or a named format element as Snowflake's
     FILE FORMAT), never in the call: upstream's `loadCsvToDbTable(path, table, conn)` keeps its signature and the
     default. First expected need: the DataCube and Python file uploads (L7).
   - **Model-written test data** (`testDataSetupCsv`) is another format, with its own upstream rule (cells stripped,
     `---null---` NULL), kept as it is.
   - **A cell's value:** upstream converts an `Integer` column's cell in Java (`Long.valueOf`) and refuses a bad one
     with its own message; lite binds the text and the database casts. The outcome is the same refusal, the message
     differs: posted to the user with L2, a register row if ruled.

## 9. Measurements to take (L1 and L3)

- A product plan's JSON size with a project's data: the demo, markets, a stress service body; a stress body's test plan.
- First-load time: `Streamed` against `DatabaseReads` on DuckDB for the stress data.
- The stress lane's memory by load identity.

## 10. Brief for the next session: L2 and L3

Start from `main` (PR #92 merged; the follow-up branch's database-judge message fix). This session's disk allowance is
spent: run in a new session (`docs/CLOUD_SESSIONS.md`).

- **L2, the CSV format first:** `DataRef.format` becomes a typed CSV-format record with the default (decision 6);
  `base.CsvCells` takes the default's rule (no trimming, an empty or `""` cell NULL, a malformed row refused by name),
  its equivalence test measured against DuckDB's reader and Commons CSV 1.5 `EXCEL` (not `CSVREAD`); DuckDB's
  `read_csv` configured so a long or short row is refused (measure `strict_mode`, `null_padding`, `ignore_errors`);
  S146 closed. No corpus test should move (its one file is clean); run both corpus lanes and the H2 PCT lane.
- **L2, test data onto loads (M2, M3, M4, M7's seed forms):** `Target.setup` and the seed `Load` become loads with
  cadences `PER_ACQUISITION` and `BEFORE_STEP`; model-written data inline below the threshold with its digest; the
  census rows M2, M3, M4, M7-seed deleted (their symbols in `DataMechanismCensusTest`), the deleted count moved with a
  dated line.
- **L3, a project's `###Data`:** a `Load(ONCE)` named by identity, made at first use and kept (S145); the inline
  `Project.made` deleted; census row M1.
- **Each slice:** every lane green (local; CI on every OS; the warm lane sessions for the lanes CI does not run, as
  round 25 did), its deletions done, its guard hard, its record in `docs/GATES.md`.
