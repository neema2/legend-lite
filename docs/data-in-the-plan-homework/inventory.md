# Data into a database, a plan, or a run: the inventory (2026-11-09)

Read-only homework for the "data in the plan" design session. Branch `claude/corpus-walk-deletion` at `4fd3b31a`.
Facts only, with file:line evidence; no design proposed. Paths are relative to `/home/user/legend-lite` unless
absolute. The probes that produced the measurements are in `probes/` beside this file (`data_elements.py`,
`relational_csv.py`; their outputs, `engine_data.json` and `csv_legend_engine_src.txt`, were not kept). No Java and no bazel was
run, so every size of a *plan* below is derived from the code and the measured inputs. It is labelled "est." and
was not measured.

---

## 0. Where data can sit in today's plan records

Before the mechanisms, here are the fields of the two plan types that carry data that must reach a database. Most
mechanisms below write one of these.

| Record / field | What it carries | Evidence |
|---|---|---|
| `ExecutionPlan.Target.setup : List<SetupStep>` | steps that "ESTABLISH it once, when its database is opened": a connection's declared test data; on an in-process engine, the system metamodel tables of a product query | `ExecutionPlan.java:735-749`; `PlanMaker.java:840-850` |
| `ExecutionPlan.Target.session : List<String>` | statements every connection runs first: dialect settings; on a server engine, the metamodel's `CREATE TEMPORARY TABLE` + rows | `ExecutionPlan.java:743`; `PlanMaker.java:845-849`, `:963-967` |
| `ExecutionPlan.Database.Declared.connection : Connection` | the plan's copy of the model's connection. A `LocalH2` spec keeps `testDataSetupCsv` / `testDataSetupSqls` **verbatim**, so the raw CSV text is inside the connection description | `ConnectionPlans.java:42-43`; `ConnectionWire.java:90-92` |
| `Database.Declared/Platform.project : Project(key, made, refused)` | the project database's identity, its fill (`made`: DDL + one multi-row INSERT per table, as `SetupStep.Statement`s), and a refusal string | `ExecutionPlan.java:764-775`, `:784-808`; `PlanMaker.java:870-886` |
| `Database.System(type, graphKey)` | which system metamodel store, by a content hash | `ExecutionPlan.java:811-816`; `SystemRoutes.java:98-137` |
| `ExecutionPlan.Setup(key, steps, target)` (a node) | a program's seeds, skipped where the session holds `key` clean; the system store's seed (key `SYSTEM_STORE`). Test door only (refused on the query door, `PlanRunner.java:416-417`) | `ExecutionPlan.java:275-293` |
| `SetupStep.Statement(sql)` / `SetupStep.Rows(stagingTable, createStaging, copy, dropStaging, rows)` | a statement, or text cells for the engine's bulk loader | `ExecutionPlan.java:820-864` |
| `ExecutionPlan.Effect(sql, script)` | `executeInDb`, seed forms, DDL, `loadCsvToDbTable` (with a local path) | `ExecutionPlan.java:233-264` |
| `Sql.statement` text | inline literal data: `VALUES` relations, JSON `data:` payloads, enum value tables, a CSV's local path | §2 M10-M12 |
| `ExecutionPlan.Constant.cells`, `TestPlan.Side.Constant.cells` | values the program wrote, as text | `ExecutionPlan.java:172-189`; `TestPlan.java:398` |
| `TestPlan.session : Target` | the body's runtime target, planned by `PlanMaker.runtimeTarget`: the product target, project fill and declared setup included | `TestPlan.java:37-39`; `TestDoor.java:155`; `PlanMaker.java:401-404` |
| `TestPlan.extensionsByStep` (`edge.SeedTexts`, `edge.EffectTexts`) | the referee ledger's second copy of every seed and effect text (compatibility rules only) | `TestDoor.java:143-147`; `edge/SeedTexts.java:14`, `edge/EffectTexts.java:16` |
| `ExecutionPlan.parameters` | the caller's values, bound at run | `ExecutionPlan.java:124-131` |

**JSON shape.** `PlanJson` writes a full `target` (database with its connection and project, session and setup) into
**every** `Sql` (`PlanJson.java:319`) and every `Setup` node (`PlanJson.java:246`). The format is v30 (`:84`). The
in-memory plan shares one `Target` object, but its JSON repeats the whole target once per statement.

---

## 1. Mechanisms

Each entry gives (1) source of bytes, (2) when and who loads it, and its lifetime, (3) its plan representation,
(4) product or test, (5) identity and caching, (6) sizes, (7) known problems.

### M1. `###Data` relational CSV into the server's project database (hole 7 v1)

1. **Source:** model text. Relational `###Data` elements (`Protocol.PRelationalCsvData`: schema, table, CSV text).
   `ProjectData.rowsKey/fill/refusal` (`setup/ProjectData.java:72-132`) read only the relational kind. ExternalFormat,
   ModelStore and reference bodies are skipped (`:122-125`).
2. **When:**
   - The planner derives the fill once per model and dialect: `PlanMaker.project`, memoized in `ctx.derived(Projects)`
     (`PlanMaker.java:870-892`). `ProjectData.fill` emits DDL plus `CsvSeed.sqls` per table (`ProjectData.java:115-131`).
   - Exec runs `made` **once, when it makes the database**: `PlanSessions.projectDatabase` → `made(...)`
     (`PlanSessions.java:242-289`), run as `PlanRunner.setUp` under the SEED mark.
   - It is made only for `Sessions.Opening.Held`, which is DuckDB `InMemory` (`DuckDbConnector.java:37-38`).
   - It is **not** run for:
     - H2 `LocalH2`/`InMemory`, which open `Named` as `"c_"+key` (`H2Connector.java:94-95`; `PlanSessions.java:259-262`);
     - URL and file databases;
     - connectors with their own sessions;
     - the platform database;
     - write plans, which open through `shared()`/`openOwn` and run `target.setup` only (`ProductRun.java:53-55`;
       `PlanSessions.java:194-211`).
   - Lifetime: the process. `HandleStore PROJECTS` never evicts (`PlanSessions.java:223`; `HandleStore.java:26-28`).
3. **Plan:** `Database.Declared(connection, Project(key, made, refused))`. `made` is `List<SetupStep.Statement>`,
   rendered text with every row as an INSERT literal, not `SetupStep.Rows`. JSON: `"project":{"key","made","refused"}`
   (`PlanJson.java:398`, `:680-701`).
4. **Path:** product (`ProductRun.onProjectDatabases`, `ProductRun.java:44-48`; `ConnectionResolver.RUNNER`). The test
   door's statement targets carry `project = null` (`PlanMaker.java:324-329`). But `TestPlan.session` carries the
   product target with the project (see M3 problems).
5. **Identity:**
   - `Project.key` is SHA-256 of `ProjectData.contentKey`: every `DatabaseDefinition.toString()` plus `rowsKey` (the raw
     CSV texts) (`ProjectData.java:60-65`; `PlanMaker.java:874`).
   - The runner's key is `Hash(PlanJson.writeTarget(identity))`, where `identity` is the database incl. the connection and
     the whole `made` list. It is re-serialized and hashed on **every** open (`PlanSessions.java:243-245`).
   - Hosts name the database `"__project_" + 16 hex` of the same content key (`ProjectData.java:38`, `:51`).
6. **Sizes:**
   - Demo project (`query/demo/models/trading-data.pure`, PARK-35): 2,361 B element, 6 tables, 38 rows, 1,688 B of CSV,
     est. 2.8 KB of INSERT text.
   - `studio/demo/projects/party/PartyData.pure`: 274 B.
   - The stress model's fill is refused (7. below), so its `made` is empty.
   - The plan size with Data inline was never measured. PLAN_AS_DATA §11 hole 7 v1 lists it under "Still to do",
     `docs/PLAN_AS_DATA_2026_11_07.md:1075-1077`.
7. **Problems:**
   - **(a) Carried but unused.** Every declared, non-refused product plan carries the full fill. Exec uses it only on a
     held DuckDB in-memory database (2.).
   - **(b) Repeated in JSON**, once per `Sql` (a `Keyed` plan has two; a write program one per step).
   - **(c) The identity key re-hashes the fill** on every query's open.
   - **(d) The refusal is plan data:**
     - `Project.refused` is a string the runner raises only where it makes a held database (`PlanSessions.java:274-279`).
     - It was moved there from planning in slice 5 because refusing at planning failed "the stress lanes' 5,795
       bodies" (`PlanMaker.java:875-878`).
     - On H2, URL and the other non-held paths the refusal is never raised.
   - **(e) No reload or unload:** an edited row makes a new key and a new database, and the old one lives for the process.
   - **(f) The cadence decision (fill at model load vs first query) is open with the user** (PLAN_AS_DATA "Holes 7 and D3",
     `:929-958`).

### M2. A connection's declared test data on the product path (`testDataSetupCsv`, `testDataSetupSqls`)

1. **Source:** model text: `LocalH2 { testDataSetupCsv; testDataSetupSqls }` in a Connection element.
2. **When:**
   - The planner renders it into `Target.setup` per plan: `CsvSeed.declaredSteps(runtime)` → `CsvSeed.planned`
     (`PlanMaker.java:840-850`; `CsvSeed.java:290-318`, `:79-101`). Not memoized: `target()` recomputes it.
   - Exec runs it **on every query** under `Establish.DECLARED_SETUP`: `PlanSessions.setUp(session, target)` runs it on
     the first open of each source, and the door makes a new source per query (`ProductRun.java:62-66`;
     `PlanSessions.java:114-151`). This is "as legend-engine establishes a connection on every acquisition".
   - On `PlanSessions.shared()` (writes, PCT in-process) it runs **once per shared session**, keyed by the whole target
     (`PlanSessions.java:153-192`).
3. **Plan:**
   - the rendered steps: `Target.setup`, as `SetupStep.Statement` (DDL, SQL texts translated from H2's spelling,
     `CsvSeed.Step.Sql.runOn`) or `SetupStep.Rows` where the dialect bulk-loads (`SqlDialect.loadsRowsInBulk`);
   - **and** the raw CSV and SQL texts inside `Database.Declared.connection.specification` (`ConnectionPlans.java:42-43`).
4. **Path:** product. A runtime other than the plan's own that names test data is refused on the query door
   (`QueryDoor.java:43-45`, `:94-101`).
5. **Identity:**
   - `openShared`: `Hash(writeTarget(target))`, including the setup rows (`PlanSessions.java:154`).
   - Per-query runs have no key.
   - A user-named or URL database with setup is refused by `refuseSetup` (`PlanSessions.java:213-218`).
6. **Sizes:** upstream `core_relational` declares `testDataSetupCsv` 125 times in 49 files and `testDataSetupSqls` 132 times
   in 49 files (probe counts).
7. **Problems:** two copies in the plan (the raw text in the connection, the rendered steps in setup), and the steps
   re-run on every product query, including on a project database that M1 already filled.

### M3. Declared and inline test data on the TEST road (`Seeds` → `Setup` node)

1. **Source:** model text. Three sources:
   - a `from()`'s inline `sqlSetups`/`csvSetups` (instance runtimes);
   - a runtime the `from()` names;
   - the program's runtime.

   Gathered by `Seeds.planned` (`testplan/Seeds.java:87-108`).
2. **When:**
   - Planned per body. `TestKinds.seed` places one `Setup` before the first statement naming the sources
     (`TestKinds.java:367-389`). The memo `generated` lives on `TestKinds`, which is created **per body**
     (`TestDoor.java:117`), so the CSV is re-parsed for every test body.
   - Exec, `PlanRunner.seed`, skips the step where `session.state().established(key)` (`PlanRunner.java:591-622`).
   - `SessionState` forgets every key on a write or a rollback. `establish` keeps only one key at a time
     (`SessionState.java:45-56`).
   - Lifetime: the harness's session (per package, per test, or SHARED per provisioning; see M4 and M9).
3. **Plan:**
   - `ExecutionPlan.Setup(Seeds.key(steps), steps, program.target())`;
   - the referee's copy in `extensionsByStep` → `SeedTexts` (`TestDoor.java:147`);
   - `TestPlan.session` = `PlanMaker.runtimeTarget`, which again holds the runtime's declared setup **and** the project
     fill (`TestDoor.java:155`; `PlanMaker.java:401-404`);
   - every statement's target holds the raw test data in its connection spec.
4. **Path:** test.
5. **Identity:**
   - `Seeds.key` hashes the executed steps' content: length-prefixed, SHA-256, `"seeds:"+hex` (`Seeds.java:226-251`).
   - Session reuse is the harness's (M4, M9).
6. **Sizes:** see M4 (the stress lane is the largest user).
7. **Problems:**
   - `TestPlan.session`'s target data is ignored at run. The harnesses open through `SessionSource.given(conn)`, which
     discards the target (`exec/SessionSource.java:20-22`; `TestRunner.java:42`).
   - The same rows appear up to three times in one test plan:
     - the raw CSV in the connection spec of every `Sql` target;
     - the `Setup` step's rows;
     - `session.setup`'s rows.
   - Every body's plan is written to JSON at least once (`TestPlanJson.requireRoundTrip`, `TestPlanner.java:155`;
     `TestPlanJson.java:54-72`).

### M4. Service test suites' data (embedded, `Reference` to a `###Data` element) → a synthetic connection

1. **Source:**
   - model text: `PServiceTestSuite.testData`, either connection data or `serviceTestData` resolvers;
   - `PDataReference` chased to a `###Data` element (`ServiceTestRunner.java:307-381`).
   - Only `PRelationalCsvData` is used. Any other kind is `Skip` ("embedded data kind ...", `:370-372`).
2. **When:**
   - The harness and product test runner builds a **test runtime overlay** per distinct provisioning. It joins the
     tables' CSV and wraps it as `ConnectionSpecification.carryingTestData(csv)`, which is a `LocalH2(null, csv, null)`,
     with `TestAuth` (`ServiceTestRunner.java:386-437`; `model/ConnectionSpecification.java:49-51`).
   - From there it is M3. Sessions are `FRESH_PER_TEST` (the H2 referee lane) or `SHARED`, one per `List<ProvisionKey>`
     (the DuckDB lane) (`:241`, `:439-446`; `StressSuites.java:63-77`).
3. **Plan:** as M3. The CSV is copied into the connection (`ConnectionPlans.of`), so it rides every target.
4. **Path:** both. The product runner (`com.legend.test`) is the user's service tests, and the stress lane is a caller.
5. **Identity:**
   - Java record equality: `ProvisionKey(store, List<CsvTableKey(schema, table, values)>)` and `RuntimeKey`
     (`ServiceTestRunner.java:128-141`).
   - Then `Seeds.key` on the session.
6. **Sizes:**
   - `stress::TestData` (`core/src/test/resources/stress/93-testdata.pure`) is a 575,115 B element.
   - By the probe's parse: 399 table entries, 5,031 data rows, 55,004 cells, 498,595 B of CSV. The generator's header
     says "5339 rows across 455 tables".
   - It renders to est. 0.71 MB of INSERT text, or est. 0.63 MB as `Rows` JSON cells.
   - It is referenced 5,939 times across about 6,058 `testSuites` blocks in the stress corpus. Other stress Data:
     `combo::ComboData` 11.5 KB (31 refs) and `checks::CheckData` 2.8 KB (41 refs).
7. **Problems (structure, not measured):**
   - Each stress body's test plan holds the ~0.5 MB CSV in every `Sql.target.connection`, in the `Setup` node's target
     and in `session`. It also holds two `Rows` copies (the `Setup` step and `session.setup`).
   - `requireRoundTrip` writes, reads and compares that JSON for about 6,000 bodies per lane run. That is est. ≥ 2.8 MB
     of JSON per body, not measured.
   - The CSV is re-parsed by `CsvSeed` twice per body (once for the `Setup` node, once for `runtimeTarget`).

### M5. The system metamodel store on the test road (`SystemRoutes.seeds` → `Setup(SYSTEM_STORE)` on `Database.System`)

1. **Source:** generated by the platform from the compiled model: `MetamodelSeeds.rows(table, ctx)`
   (`planner/MetamodelSeeds.java:37`). It covers every system table, the platform's rows and the model's.
2. **When:**
   - The planner derives the rows once per graph per engine (`SeedMemo`, `SystemRoutes.java:82-95`, `:146-176`) and
     places one `Setup` step ahead of the first statement that reads the store (`PlanMaker.java:340-376`).
   - Exec runs it once per system session: `SessionState` memo on the constant key `SYSTEM_STORE`
     (`PlanRunner.java:591-622`). The session comes from `SystemDatabase.Leases.sessionFor(System)`. There is one
     current session per engine; a different `graphKey` replaces it, and the old one closes when its last lease ends
     (`exec/SystemDatabase.java:58`, `:99-130`).
   - Since each core test compiles a new model, that is a new `graphKey`, a new session and the whole seed again.
3. **Plan:**
   - On an in-process engine: `Ddl.metamodelSteps` (DDL `Statement`s, then `Rows` for the bulk loader or one INSERT)
     (`Ddl.java:436-452`).
   - On a server engine: `Statement`s of `CREATE TEMPORARY TABLE` plus rows (`Ddl.metamodelTemporary`, `:467-480`).
4. **Path:** test (bodies that read `Class.all()` etc.).
5. **Identity:** `graphKey` is a hand-rolled 128-bit FNV/mix hash over each table's definition and rows, not
   `cache.Hash`, because TeaVM has no `java.security` (`SystemRoutes.java:107-137`). The `Setup` key is the constant
   string.
6. **Sizes** (`docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md` §1.1; `docs/CORE_SQL_SIZE_2026_11_04.md` §2):
   - a one-class model: 34 tables, 2,582 rows, 327,148 chars, about 2,580 of those rows and 327K of those chars the
     platform's own;
   - the stress model: 117,113 rows, 17,980,176 chars;
   - core's tests: this seed is 57.0M of 67.0M characters sent (85%), 25,663 statements, the largest 177,369 chars;
   - corpus: one test reads it (`testRelationalDatabaseMapper`).
7. **Problems:**
   - The seed is re-sent per graph (the 85%).
   - The agreed catalog design (platform rows once, the model's rows registered in a rolled-back transaction) is not
     implemented. Hole 2 v1 only re-keyed the holder (PLAN_AS_DATA `:1119-1128`).

### M6. The system metamodel store on the PRODUCT road (`PlanMaker.metamodel`): three forms by engine

1. **Source:** the same `MetamodelSeeds.rows`, only the tables the query references (`collectSystemTables`).
2. **When:** planned per product query (`PlanMaker.java:939-974`), not memoized. At run:
   - **In-process** (DuckDB, H2, SQLite): `Ddl.metamodelSteps` go into `Target.setup` (`:961-962`). Exec runs them per
     query under `DECLARED_SETUP` (M2's cadence): the referenced tables are dropped, re-created and refilled on every
     query.
   - **Server with temporary tables:** `Ddl.metamodelTemporary` goes into `Target.session` (`:965-967`), run once per
     connection per statement list (`SessionState.needsSessionStatements`). The query reads the tables through renames.
   - **No temporary tables** (Trino, MongoDB): the rows go **inline** in the query as a `SqlSource.Values` of typed
     literals (`:963-964`, `:981-1021`).
3. **Plan:** `Target.setup`, `Target.session`, or the statement text, depending on the dialect.
4. **Path:** product (and PCT channel A, which runs product plans).
5. **Identity:** none of its own (whatever the target's key is).
6. **Sizes:** proportional to the referenced tables; a one-class model's whole store is 327K chars.
7. **Problems:** one concept in three plan shapes in the product alone, plus M5's shape on the test road.
   SYSTEM_CATALOG_DESIGN §4.4 (the product catalog) is not built.

### M7. Effects in test bodies: `executeInDb`, seed forms, DDL

1. **Source:** model or test text.
   - `executeInDb(sql, conn)`: the string is folded at planning from literals and lets. A computed string is refused,
     "a text hole" (`TestKinds.java:449-456`).
   - `setUpDataSQLs` and `setUpDataSQLsV2` forms mapped over one effect: CSV folded, then `CsvSeed.sqls`
     (`Seeds.java:118-148`).
   - DDL text collections (`Seeds.ddlTextSource`/`ddlTexts`, `:158-192`).
   - `dropAndCreateTableInDb`, `dropAndCreateSchemaInDb`, `createTempTable`, `dropTempTable` (`EffectPlanner.java:90-150`).
2. **When:** the planner writes them per body as an `Effect` step's script (`TestKinds.java:430-466`), and exec runs
   them in order on the body's session. Lifetime: the session. A write invalidates the seed memos (`markWriting`).
3. **Plan:** `ExecutionPlan.Effect(sql, Script(statements, bracketed, abort))`, plus `EffectTexts` (the referee's
   spelling) in `extensionsByStep`.
4. **Path:** test only. `QueryDoor` refuses `EFFECT` on the product door (`QueryDoor.java:42`).
5. **Identity:** none. Effects are not memoized and always run.
6. **Sizes:** upstream `core_relational` has 2,247 `executeInDb(` calls in 83 files, 1,513 of them literal
   `executeInDb('insert into ...` in 65 files.
   - Corpus shared fixture: `relationalSetUp.pure` is 77,670 B, with 211 `executeInDb(` and 143 inserts.
   - Other counts: `dropAndCreateTableInDb` 151/39 files; `setUpDataSQLs` 21/7; `setUpDataSQLsV2` 3/3;
     `createTempTable` 5/3; `dropAndCreateSchemaInDb` 12/6 (probe counts).
7. **Problems:** the data is SQL text (INSERTs). The same table load has different shapes depending on whether a test
   author wrote INSERTs, a CSV block or a seed form.

### M8. `loadCsvToDbTable(path, table, conn)`: hole D3

1. **Source:** a **local file**. The Pure path (e.g. `/core_relational/.../employees.csv`) is resolved at planning by
   the caller's `ExecuteOptions.resources : Function<String, Path>` (`ExecuteOptions.java:16-17`, `:25`). The corpus
   maps it into the upstream checkout (`MinimalCorpus.java:608-610`). `TestKinds.resourceFile` returns
   `file.toAbsolutePath()` and refuses a missing file (`TestKinds.java:265-276`, `:457`).
2. **When:** the planner renders the statement per body (`EffectPlanner.loadCsv`, `:152-176`). The **database reads the
   file at run** (`SqlDml.LoadCsv`):
   - DuckDB: `INSERT INTO t (cols) SELECT * FROM read_csv('<abs path>', header=true, ..., all_varchar=true)`
     (`DuckDb.java:801-805`; `AnsiSqlRenderer.java:1887-1888`);
   - H2: `CSVREAD('<abs path>', NULL, 'charset=UTF-8')` (`H2.java:171-177`);
   - every other dialect refuses (`AnsiSqlRenderer.java:1896-1899`; `MongoDb.java:2659`).
3. **Plan:** an `Effect` statement whose text holds the absolute local path. The referee's H2 spelling of the same
   path is held again in `EffectTexts`.
4. **Path:** test only (the effect is refused on the product door; `Seeds.oneEffectMapper` excludes it, `Seeds.java:204`).
5. **Identity:** none.
6. **Sizes:**
   - Upstream has 5 calls in 2 files and one test (`testLoadCsv.pure`). Its one file, `employees.csv`, is **50 bytes**.
   - Upstream holds 1,319 `.csv` files (935 KB in all; the largest is the DataCube walkthrough's `sport-data.csv` at
     499 KB, which lite does not read). legend-pure holds none.
   - The repository has no CSV or Parquet fixture; its three `.csv` files are under `docs/`.
7. **Problems:**
   - The plan carries a machine-local path. That is D3, open and slated for slice 4
     (`PLAN_AS_DATA_2026_11_07.md:848`, `:952-953`, `:1077`; GATES: "D3 ... remain").
   - The resolver is a run option that planning reads. ExecuteOptions calls it a "test-input resource resolver".

### M9. Package setups and on-demand fixtures (`PureTestRunner`)

1. **Source:** model text: `<<test.BeforePackage>>` functions and a caller's shared fixture units (the corpus's
   `relationalSetUp.pure`). They are programs of effects (M7), seed forms (M7) and CSV loads (M8).
2. **When:**
   - The harness and product test runner plans and runs each setup as its **own test plan** on the package's session.
     It runs once per session; an "inert" setup never runs (`PureTestRunner.java:36-45`, `:426-428`, `:431-450`).
   - Fixtures for appeals: after the body is planned and before it runs, the runner provides the setup that seeds each
     store `PlannedAppeals.stores(plan)` names. Each is planned with `TestExecution.planProgram` and run with
     `runFixture`, possibly in a `FixtureAside` (`:281-310`, `:365-420`, `:543-547`).
   - A body with inline CSV (`ProgramFacts.seedsInlineCsv`) gets a private session (`:495-497`).
3. **Plan:**
   - The setups' own `TestPlan`s.
   - The body's plan names stores, not fixtures (`PlannedAppeals.stores`).
   - Which fixture seeds which store is the runner's index (`fixturesByStore`, from `ProgramFacts.seedsStores`,
     `:238-256`). That index is outside every plan.
4. **Path:** both. The product test runner is a product surface; the corpus is a caller.
5. **Identity:**
   - The session is per package.
   - `ran` is a set of setup FQNs per session.
   - A clash is decided by the declared footprint (`mayProvide`, `:343-363`).
6. **Sizes:** upstream `core_relational` has 112 `BeforePackage` in 107 files.
7. **Problems:** the data's identity is "the setup function that seeds store X", known only to the harness. The plan
   says which store; the caller decides which program fills it.

### M10. JSON model connections: `sourceUrl` (`data:`, `file:`, `http:`)

1. **Source:**
   - model text: a runtime's `JsonModelConnection { class; url }` (`model/JsonModelConnection.java`);
   - the user-callable `meta::legend::lite::sourceUrl('...')` (`builtin/Pure.java:577-583`).

   `ModelBuilder` cross-bakes the URL into `ClassMapping.Relational.sourceUrl` (`ModelBuilder.java:257-264`;
   `model/ClassMapping.java:176-183`). `MappingNormalizer` emits `sourceUrl(url)` (`:1528-1543`), which becomes
   `TypedSourceUrl` (`compiler/spec/SourceUrlChecker.java`) and then `SqlSource.SourceUrl(url, alias, outputs)`
   (`Lowerer.java:977-978`; `sql/SqlSource.java:77`).
2. **When:** the dialect renders it into the statement (DuckDB only, `DuckDb.java:847-866`). The database reads it per
   statement execution, with no lifetime beyond the statement.
   - `data:application/json,[...]` → `SELECT unnest(CAST('<the JSON>' AS JSON[])) AS data`: **the payload is inline in
     the SQL text**.
   - `file:` → `read_json_objects('<local path>')`: **a local path in the plan**, the same shape as D3. It is not listed
     as a hole.
   - `http:` → `IllegalStateException("unsupported sourceUrl scheme")`. Every non-DuckDB dialect throws
     `DialectCapability` (`AnsiSqlRenderer.java:313-316`).
3. **Plan:** `Sql.statement` text, and the MIR tree (`Sql.tree`).
4. **Path:** both (M2M on the platform engine, `Database.Platform`, S27).
5. **Identity:** none. The data is part of the statement.
6. **Sizes:** core tests have 39 `sourceUrl` or `data:application/json` mentions and 10 files with `JsonModelConnection`.
   `core/src/test/resources/test-data/` holds three JSON or text files of about 400 B each (`JsonM2MChainIntegrationTest`).
7. **Problems:**
   - a third shape for "rows the query reads" (inline literal) and a second local-path carrier;
   - DuckDB-only;
   - an `http:` URL is refused, not read by the database.

### M11. Literal relations and instances in queries; product writes of literal rows

1. **Source:** query or test text: TDS literals (`TypedTds`), instance collections as relations
   (`TypedCollectionRelation`), `^New` instances. Lowered to `SqlSource.Values` (9 construction sites in `lowering/`).
2. **When:** inline in the statement, per execution. A product write program (`write`, `merge` into
   `#>{db.t}#`, WRITES_DESIGN §8) lands literal rows in a store table as an `Update` step on the plan's own session
   (decision A).
3. **Plan:** `Sql.statement` (+ `tree`). `ExecutionPlan.Update` for writes.
4. **Path:** both. **PCT's only data shape:** PCT expressions are storeless (`PctExecuteNative.java` header;
   `StorelessRuntime`).
5. **Identity:** none.
6. **Sizes:** proportional to the literal. PCT's Snowflake channel B sends 2,749 statements and 4.37M chars, the
   largest 55K (CORE_SQL_SIZE §2).
7. **Problems:** none recorded. This is the "the database does the work" shape for small data.

### M12. Model-derived and generated constants inside statements

- **Enum value tables:** each mapped enum's decode becomes `(VALUES ('A','ACTIVE'), ...) AS _enum(code,name)` in the
  statement (`sql/EnumValueTables.java`; applied in `PlanMaker.java:127`). Product and test. Planning-time model data
  inline.
- **TDG census:** `TypedCsvCensus` is folded at orchestration into instance literals ("replaces the node with instance
  literals", `testdatagen/TestDataGenerationNatives.java` header). Generated model-space data inline.
- **`Constant` nodes and constant verdict sides:** text cells the program wrote (`ExecutionPlan.java:172-189`).
- **Execution-trace stamps:** a UUID per execution, written as a comment and bound as frame-stamp parameters
  (`ExecutionPlan.traceComment`, `:136-138`; `PlanMaker.withStamps`). Generated at run.

### M13. The test-data generator (TDG)

1. **Source:** rows already in a database. The generator walks the query's relation tree and copies the reached rows
   into session TEMP tables (`CREATE TEMP TABLE ... AS SELECT`, child fetches joined in the database), then reads them
   back as a CSV text (`testdatagen/TestDataGenerator.java:1-35`).
2. **When:** run in tests only (`core/src/test/java/com/legend/testdatagen/TestDataGeneratorRun.java`). It plans and runs
   interleaved, so slice 5 moved its run out of the product (`PLAN_AS_DATA :1182-1186`). The product keeps its plan text
   (`TestDataGenerationNatives.planText`), reached from the planner (`testplan/BodyDriver.java:82`).
3. **Plan:** each fetch is a plan statement run by `PlanRunner.effect/describe/rows`, planned one at a time over the
   previous fetch's rows. No single plan holds the run.
4. **Path:** test (the generator is "not offered", S143).
5. **Identity:** none. The temps are per session.
6. **Sizes:** n/a.
7. **Problems:** run-time rows feed planning, so the generator is neither half of the product. Its output CSV is meant
   to return through `setUpDataSQLs` (`assertTestData`), which is M7.

### M14. Host-side loading in the browser tab, Python and DataCube: outside every plan

- **A model's Data in the tab:**
  - `Boundary.testDataSql(model, database, tablesJson)` returns per-table `CsvSeed.sqls` for DuckDB
    (`wasm/src/main/java/planner/Boundary.java:241-286`).
  - `Boundary.projectData(model, databaseType)` returns `ProjectData.of`: `open`/`use`/`fill`/`close` statement lists
    (`:454-456`).
  - The tab runs them itself (`engine-client/src/model-data.ts:93-145`; `project-data.ts:1-30`). The same `ProjectData`
    record as M1, but host-run.
- **Files a person drops** (CSV, Parquet, JSON) are registered in DuckDB-WASM (`registerFileBuffer`) and loaded with
  `HostOperation.MakeRelation(TABLE, from File(at, format))` or `Insert(... from File)`
  (`datacube/src/upload.ts:254-322`; `engine-client/src/tab-data.ts:62-183`; `model-data.ts:93-136`).
- **Remote files** (Parquet, CSV, Iceberg over S3 or HTTP) are mounted as a VIEW under the model's table name:
  `MakeRelation(VIEW, from Remote(url, format))` plus a `Secret` (`datacube/src/remote.ts:1-20`, `:79-80`;
  `sql/dialect/DuckDbHostOperations.java:142-156`). **This, not `SqlSource.SourceUrl`, is where Arrow, Parquet and
  Iceberg URLs are read today.** The database reads them itself, and the planner is untouched.
- **Python's host** runs the same `HostOperations` statements on its DuckDB (`python/legend_lite/_host.py`;
  `_planner.py:32-33` exposes `projectData` and `hostOperations`).
- **Path:** product, but no plan. The host spells nothing; the platform writes the statements
  (`HostOperation`/`HostOperations`, data-only operation records, `sql/dialect/HostOperation.java`).
- **Identity:** the project database is named `__project_<16 hex>` of the content key; files by name, and they stay
  "until reset" (`tab-data.ts:1-4`).

### M15. Direct seeding by test harnesses: outside every plan

- **Core tests:** 103 files run `executeUpdate(...)` or `.execute("CREATE|INSERT ...")` on JDBC, with 777 literal
  CREATE/INSERT/DROP executes.
- **The server tests' `Seed.sql`** runs raw SQL on `ServerSessions.session(ctx, runtime)` inside a `RunScope`
  (`core/src/test/java/com/legend/server/Seed.java`).
- **`PostgresSeed`/`SqlParity`/`IqParity`** fill the parity lanes' tables.
- **The referee's mirror** (`spec/src/test/java/com/legend/harness/ReplayReferee.java:74`, `:186-235`) replays the
  session's recorded non-query statements (the seed ledger, `MinimalCorpus.java:541-543`, `:657-665`) on the engine's
  own H2 with JDBC. This is the edge's own after-run input.
- **The corpus's workspaces** (ATTACH/USE/DETACH, `testbackend/DuckDbWorkspaces`) and `FixtureAside`.

### M16. Data entering a run that is not a database load (for completeness)

- **The caller's parameters:** `ExecutionPlan.Parameter`, validated and bound by `exec/PlanParameters`.
- **Run-time rows feeding the next statement:**
  - a keyed result's keys fill a later statement's regions (`Binding.Key`, `ExecutionPlan.java:669`);
  - a write's count is bound as `#count` (`:212`).
- **Host rows posted back** to the server for the Arrow or JSON answer (`PureV1Api.arrowAnswer(plan, rows)`,
  `:510-550`; `Boundary.executeJsonAnswer/executeArrowAnswer`, `:92-110`).

### M17. Parsed but never loaded anywhere

- ExternalFormat `###Data` bodies: ignored by `ProjectData` (`:122-125`) and skipped by `ServiceTestRunner`.
  ExternalFormat appears only as an expected JSON assertion value (`:474-485`).
- ModelStore data (`PModelStoreData`, `PTestPayload.ModelStoreData`) and ServiceStore stub data
  (`ServiceStubDataParser`) have no consumer outside `parser/` and `protocol/` (grep).
- Mapping tests' test data (`MappingTestReader`/`Composer`) are round-tripped only. The mapping test-suite runner was
  deleted (`docs/DEFERRED_TEST_EXECUTION.md`).

---

## 2. Table of all mechanisms

| # | Mechanism | Bytes from | Loaded by / when | Lifetime | In the plan as | Path | Identity / cache |
|---|---|---|---|---|---|---|---|
| M1 | `###Data` relational → project database | model text | planner derives (memo per model+dialect); exec runs `made` when it makes a held DuckDB in-memory database | process (`HandleStore`, no eviction) | `Database.*.project = Project(key, made[Statement], refused)` | product | SHA-256 of `contentKey`; runner key = SHA-256 of target JSON incl. `made`, recomputed per open |
| M2 | connection `testDataSetupCsv/Sqls` (product) | model text | planner per plan; exec per query (`DECLARED_SETUP`) or once per shared session | per query, or per session | `Target.setup` + raw text in `connection.spec` | product | none per query; `openShared`: hash of target JSON |
| M3 | declared, `from()` and runtime test data (test road) | model or test text | planner per body (re-parsed); exec skips if session holds key | harness session, until a write or rollback | `Setup(key, steps, target)` node + `SeedTexts` + `TestPlan.session.setup` + raw text in every target | test | `Seeds.key` (SHA-256 of content); `SessionState` one key at a time |
| M4 | service-suite data / `DataElementReference` | model text (`###Data`) | harness builds overlay `LocalH2(csv)` → M3 | per test, or SHARED per provisioning | as M3 | both | `ProvisionKey`/`RuntimeKey` record equality, then `Seeds.key` |
| M5 | system metamodel store (test road) | generated from compiled model + platform | planner memo per graph+engine; exec once per system session | one session per engine, replaced per `graphKey` | `Setup(SYSTEM_STORE, …)` on `Database.System(type, graphKey)` | test | FNV/mix `graphKey`; constant `Setup` key |
| M6 | system metamodel store (product) | generated | planner per query; exec per query, per connection, or inline | per query, or connection | `Target.setup` (in-process) / `Target.session` (server TEMP) / inline `VALUES` (Trino, Mongo) | product | none |
| M7 | effects: `executeInDb`, seed forms, DDL | test or model text (folded literals) | planner per body; exec in order | session | `Effect(sql, Script)` + `EffectTexts` | test | none |
| M8 | `loadCsvToDbTable` | local file via resolver | planner resolves the path; the database reads the file at run | session | `Effect` text containing an absolute path (+ ledger copy) | test | none |
| M9 | `BeforePackage` setups, appeal fixtures | model text (programs of M7 and M8) | harness plans and runs separate plans per package session | package session | separate `TestPlan`s; body plan names stores only | both | harness's `fixturesByStore`, `ran` set |
| M10 | JSON model connection / `sourceUrl` | model text (`data:`) or local file (`file:`) | the database reads it per statement | statement | statement text (`unnest(CAST('<json>'…))` / `read_json_objects('<path>')`) | both | none |
| M11 | TDS / instance literals; literal writes | query text | inline per statement; `Update` for writes | statement / the written table | `SqlSource.Values` in text; `Update` | both | none |
| M12 | enum value tables, TDG census, constants, stamps | model / generated | planner inline; stamps at run | statement | statement text, `Constant.cells`, slots | both | none |
| M13 | test-data generator | rows in a database | interleaved plan, run, plan (tests) | session temps | per-fetch plans, no whole plan | test | none |
| M14 | tab, Python, DataCube: project data, files, remote URLs | model text, person's file, remote URL | host runs platform-written statements | host session or "until reset" | **no plan** | product (hosts) | `__project_<hash>`; file names |
| M15 | harness JDBC seeding, referee mirror | test code, seed ledger | harness, directly | test, package or run | **no plan** | test | none |
| M16 | parameters, keyed keys, `#count`, host rows | caller, database | run | run | `parameters`, `Binding.Key`, `COUNT` | both | n/a |
| M17 | ExternalFormat, ModelStore, stub, mapping-test data | model text | never loaded | — | — | — | — |

---

## 3. Measured numbers (the probes in `probes/` beside this file)

**`###Data` elements in `.pure` files** (`data_elements.py`, brace-balanced parse of `###Data` sections):

| Root | Files with `###Data` | Elements | Total bytes | Largest | Kinds |
|---|---|---|---|---|---|
| `core/src/test/resources/stress` | 18 | 22 | 600,219 | 575,115 (`stress::TestData`) | Relational 19, ModelStore 1, other 2 |
| `core/src/test` (all) | 23 | 24 | 600,566 | 575,115 | Relational 21, … |
| `scripts/` (parser fixtures, corpus repros) | 84 | 80 | 14,189 | 1,180 | Relational 70, ServiceStore 4, ExternalFormat 3 |
| `parser-equivalence/` | 22 | 10 | 3,759 | 1,180 | ServiceStore 4, ExternalFormat 3 |
| `query/` (demo) | 2 | 1 | 2,361 | 2,361 | Relational |
| `studio/` (demo) | 1 | 1 | 274 | 274 | Relational |
| `spec/`, `pct/`, `projects/`, `fixtures/` | 0 | 0 | — | — | (`projects/` mentions `###Data` only in MANIFESTs: "no `###Data` element") |
| upstream legend-engine (`.pure`) | 92 | 76 | 26,199 | 998 | Relational 37, "Relation" 28, ExternalFormat 7, ServiceStore 1 |
| upstream legend-engine (Java test strings) | 34 files | 318 occurrences | — | — | — |
| upstream legend-pure | 0 | 0 | — | — | — |

The size distribution of the stress elements: 15 under 1 KB, 5 of 1-10 KB, 1 of 10-100 KB (11.5 KB), and 1 of
100 KB-1 MB (575 KB). Every upstream element is under 1 KB. In core's Java tests, `###Data` occurs 32 times in 18
files (e.g. `server/ProjectRowsTest`, `HostOperationsTest`).

**Relational fills** (`relational_csv.py`):

| Element | Tables | Rows | Cells | CSV bytes | INSERT text (est.) | `Rows` JSON (est.) |
|---|---|---|---|---|---|---|
| `stress::TestData` | 399 | 5,031 | 55,004 | 498,595 | 707,534 | 632,385 |
| `combo::ComboData` | 5 | 26 | 1,155 | 10,930 | 14,945 | 12,658 |
| `demo::trading::TradingData` | 6 | 38 | 221 | 1,688 | 2,785 | 2,191 |
| `demo::party::PartyData` | 1 | 5 | 15 | 137 | 241 | 180 |

`stress::TestData` is referenced 5,939 times; the stress corpus has about 6,058 `testSuites` blocks.

**CSV files:**
- upstream legend-engine: 1,319 files, 934,779 B; the largest is `sport-data.csv` at 499,260 B (not read by lite);
- upstream legend-pure: 0;
- this repository: none outside `docs/`;
- the one CSV the corpus loads: `employees.csv`, 50 B.

**Upstream `core_relational` seeding primitives:**

| Primitive | Count | Files |
|---|---|---|
| `executeInDb(` | 2,247 | 83 |
| …of which literal `insert into` | 1,513 | 65 |
| `dropAndCreateTableInDb(` | 151 | 39 |
| `testDataSetupSqls` | 132 | 49 |
| `testDataSetupCsv` | 125 | 49 |
| `BeforePackage` | 112 | 107 |
| `setUpDataSQLs(` | 21 | 7 |
| `dropAndCreateSchemaInDb(` | 12 | 6 |
| `loadCsvToDbTable(` | 5 | 2 |
| `createTempTable(` | 5 | 3 |
| `setUpDataSQLsV2(` | 3 | 3 |

`relationalSetUp.pure` is 77,670 B.

**System metamodel** (from the two docs):
- per one-class model: 2,582 rows and 327K chars;
- the stress model: 117,113 rows and 18.0M chars;
- core's SQL: 85% (57.0M of 67.0M chars) is this seed;
- one corpus test reads it.

**Not measured, listed as still to do:** a product plan's size with the Data fill inline (PLAN_AS_DATA `:1075-1077`).
The size of a test plan's JSON was not measured either.

---

## 4. Known problems, as facts

1. **A local path in the plan (D3, open):**
   - `loadCsvToDbTable` (M8) puts it in the `Effect` text and the ledger copy;
   - `sourceUrl('file:...')` (M10) does the same in a query statement, and is not on the holes list.
2. **Every product plan carries the whole Data fill** (`Project.made`) for a declared database (M1), whether or not exec
   will use it:
   - it is used only on a held DuckDB in-memory database;
   - not on H2's named in-memory database, a URL, a connector's own sessions, the platform database or a write plan.
   - It is repeated in JSON once per `Sql` (`PlanJson.java:319`).
   - It is re-serialized and hashed on every open (`PlanSessions.java:245`).
3. **The refusal is plan data:** `Project.refused` is raised only where exec makes a held database (`PlanSessions.java:274-279`).
4. **Test plans carry the product's target, never read.**
   - `TestPlan.session = PlanMaker.runtimeTarget(...)` holds the project and the declared setup.
   - The harnesses open through `SessionSource.given`, which ignores the target.
5. **Declared test data has up to three forms in one plan:**
   - raw text in `connection.specification`;
   - rendered steps in `Target.setup`, or in the `Setup` node on the test road;
   - the `session` target's setup.

   With the stress lane's 0.5 MB CSV, each body plan's JSON is est. several MB. Every body plan is JSON-round-tripped
   (`TestPlanner.java:155`).
6. **Re-derivation per body:** the seed CSV is parsed in each body's `TestKinds` (`TestDoor.java:117`) and again for
   `runtimeTarget` (not memoized). Only `Project` and the system store are memoized per model.
7. **Different cadences for the same kind of data:**

   | Cadence | Mechanism |
   |---|---|
   | once per process | project database |
   | per query | declared setup, product metamodel tables |
   | per connection | session statements |
   | per session by key, until a write | `Setup` |
   | per package | `BeforePackage` |
   | per graph per engine | system store |
   | per statement | inline |

8. **Five identity schemes:**

   | Scheme | Used for |
   |---|---|
   | SHA-256 of `contentKey` | `Project.key` |
   | SHA-256 of the target's JSON | `openShared`, `projectDatabase` |
   | SHA-256 of length-prefixed step content | `Seeds.key` |
   | a hand-rolled FNV/mix 128-bit hash | `graphKey` |
   | Java record equality | `ProvisionKey`, `RuntimeKey` |

   On top of these: constant keys (`SYSTEM_STORE`) and generated names (`plan_<16hex>_<n>`, `c_<16hex>`,
   `__project_<16hex>`).
9. **No lifecycle:** `HandleStore` never evicts (PROJECTS, SHARED, NAMED). An edited row makes a new database, and the
   old one stays.
10. **The product's metamodel has three shapes** by engine (M6), and the test road has a fourth (M5). The catalog design
    is agreed but not built.
11. **Hosts bypass the plan:** the tab, Python and DataCube load Data, files and remote URLs (M14) with
    platform-written statements outside any plan. Arrow, Parquet and Iceberg URLs are read there, not through
    `SqlSource.SourceUrl`.
12. **Non-relational data has no loading path** (M17).

---

## 5. The distinct shapes the same concept takes today

"Rows that must be in a database before a query reads them" appears as:

1. **`Project.made`**: a list of rendered DDL plus multi-row INSERT text, once per database, part of the database's
   identity (M1).
2. **`Target.setup`** of `SetupStep.Statement | SetupStep.Rows`: per acquisition or per shared session (M2, M6
   in-process).
3. **`Target.session`** statement strings: per connection (M6 server TEMP tables).
4. **The `Setup(key, steps, target)` node**: per session by content key, until a write (M3, M4, M5).
5. **`SetupStep.Rows`**: text cells for an engine bulk loader, staged then copied. This shape is shared by 2 and 4.
6. **`Effect`/`Script` text**: author-written INSERTs, seed forms, DDL (M7, M9).
7. **A local file path inside statement text**: `read_csv`/`CSVREAD` (M8), `read_json_objects` (M10 `file:`).
8. **Inline literals in the query**: `SqlSource.Values` (M11, M6 on Trino/Mongo, enum tables M12), a `data:` JSON
   string literal (M10), constant cells (M12).
9. **Raw test-data text inside the connection description** (`LocalH2.testDataSetupCsv/Sqls`) (M2, M4).
10. **A content key naming a database the run finds**: `Database.System.graphKey` (M5), `Project.key` (M1).
11. **A named fixture program the caller runs between plan and run** (M9): the plan names a store, the harness picks
    the program.
12. **Host-run statements outside any plan**: `ProjectData.of` open/use/fill/close, `testDataSql`, `HostOperation`
    File/Remote (M14).
13. **Harness JDBC outside any plan**: test seeding, the referee mirror's ledger replay (M15).
14. **A remote URL the database reads itself**: `HostOperation.Source.Remote` (Parquet, CSV, Iceberg), host-only today
    (M14).
15. **Rows produced by a run and fed to the next planning step** (M13) or the next statement (M16 keyed keys).

Shapes 1-5 are the same `SetupStep` vocabulary with three different cadences and three different identity rules.
6, 7 and 8 are statement text. 9 duplicates 2 and 4. 11 to 13 are outside the plan.
