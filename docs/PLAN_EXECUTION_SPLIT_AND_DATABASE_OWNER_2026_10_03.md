# Plan/execution split, and one owner for every database decision (2026-10-03)

Status: **homework done (A1–A3); this line now does B1 and C3 only** (user, 2026-10-03). Everything in the
rebuild's areas is handed to `EXECUTION_PLAN_2026_09_26.md` with its measurements (findings added under W2.1, W4.4a,
W6.2) and announced in `docs/IN_FLIGHT.md` ("A fourth line"):

| step | owner | why |
|---|---|---|
| B1 guards see exactly the product | **this line** | test code and the guardrail target only |
| C3 database registry (one owner) | **this line** | dialects, exec, server; small, announced edits to `Compiler`/`StatementExecutor` |
| C1 / B3 effect analysis and its catch | the rebuild (W4.4a finding; D10) | `compiler/spec`, under the D24 cleanup; D10 already rules demand-driven user paths + compile-all diagnostics |
| B2 missing platform functions | the rebuild (W2.1 / W4.4a; D9) | part of the 191 missing overloads; never hand-ported one by one |
| C4 the reader's `"H2"` defaults | the rebuild (W2.1 finding) | `ContextReading`, `compiler/spec/typed` |
| C2 plan/execution split | the rebuild (W6.2 finding) | moves the root package; only with that session's agreement | Nothing in Phases B–C is built. Phase A (homework) is in progress; its findings are
written into §3 as they land. Context: commit `2defa10e7` ("The dialect is decided once") made the EXECUTION dialect
come from the runtime's declared connection type. The audit after it (in conversation, 2026-10-03) found the rest of
this document: other places still decide by database, `java.sql` is held out of the planner only by lazy class
loading, and an effect analysis swallows compile errors.

This is the first carve-out of the execution plan's **W6.2 "The runner"** (`EXECUTION_PLAN_2026_09_26.md` §W6) and
the split `ArchitectureTest` F1.3b calls "the orchestration/exec-seam split (backlogged)".

---

## 1. Goals

1. **One owner for every database decision.** Given a database, what we do (its dialect, legend-engine text renderer,
   product name, in-memory instance, how a session opens, its protocol facts) is decided in ONE place, keyed by the
   DECLARED `DatabaseType`. Nothing else names a database, a JDBC product string, a JDBC URL or a renderer. This is
   upstream's own shape (`dbExtension.pure`: `createDbConfig(connection.type)` → `loadDbExtension()`).
2. **`java.sql` cannot reach the planner.** The plan surface (what the browser's WASM planner and `generatePlan` use)
   lives in a library that does not depend on `exec`; the BUILD enforces it, not lazy class loading.
3. **Everything we load compiles.** No analysis catches a compile error and assumes an answer.
4. **No fallbacks, no defaults, no hooks** (AGENTS.md invariant 4; the user's rules: no package-private hooks across
   libraries, no test-only seams).

## 2. Decisions (with who decided and when)

| id | decision | by | status |
|---|---|---|---|
| D1 | The dialect is the runtime's DECLARED connection type; the session is only CHECKED against it | user, 2026-10-02 | built (`2defa10e7`) |
| D2 | A storeless query executes on a declared runtime built the way legend-engine's PCT adapter builds one (`StorelessRuntime`) | user, 2026-10-02 | built (`2defa10e7`) |
| D3 | A runtime whose data lives in NO database (file/inline `JsonModelConnection`, instances) runs on the platform's own in-process DuckDB — legend-lite's counterpart of upstream's in-memory M2M (SEMANTICS_REGISTER S27). A runtime that declares a database runs there, its file data inline in that database's SQL | user, 2026-10-02/03 | **rule recorded; implementation choice (a) pending the user's confirmation** — (a) the platform opens its own DuckDB for such queries (recommended); (b) as built: DuckDB dialect on the caller's connection, which must be DuckDB |
| D4 | No `exec_values` grab-bag: the plan side uses nothing from `exec` once the effect analysis moves (§3.4) | user, 2026-10-03 | agreed |
| D5 | The statement-effect analysis (`containsEffect`, `containsTdgGenerator`, maybe `callsVerdict`) is a COMPILER analysis, not `Compiler` API and not executor code | user, 2026-10-03 | agreed |
| D6 | Where upstream's Pure COMPILER (sqlQueryToString, pureToSqlQuery, typeInference, toPostgresModel, executionPlan …) is reached from a program, the ENTRY is ours: a native/`Subsumed` implemented by our Java — never upstream's Pure internals typed or run. An override is either the real Java equivalent or a NAMED wall; never a no-op | user, 2026-10-03 | agreed in principle; per-case after A2 |
| D7 | The blind spots are fixed BEFORE the restructuring | user, 2026-10-03 | agreed |
| D8 | The mixed case (a runtime binding a database AND file data) runs in the declared database with the file data inline; a dialect that cannot spell inline JSON refuses by name. Proven on DuckDB only | measured, 2026-10-03 | built behaviour; other dialects untested |

## 3. What has been measured (the facts this plan stands on)

### 3.1 Every place that decides by database (sweep of `core/src/main`, 2026-10-03)

| # | where | decides | keyed by | defect |
|---|---|---|---|---|
| 1 | `Compiler.dialectFor` / `executesOn` | execution dialect; the D3 DuckDB rule | declared type | — (one owner for execution, since `2defa10e7`) |
| 2 | `StatementExecutor` `toSQLString` (~:471–478) | legend-engine text renderer H2/DB2/Composite | type-NAME string | `default ->` arm; `dbBound == null ? "H2"` |
| 3 | `StatementExecutor.planDialect` (~:1178) | engine plan-text renderer | string | `null` → H2; `default ->` H2 (silent fallback) |
| 4 | `StatementExecutor` ~:760 (`dbType`), ~:1082/1094 (`planConnOf`); `ContextReading` ~:803, ~:845 | the connection's type when unread | — | **defaults to `"H2"`** (see §3.2) |
| 5 | `plan/InProtocol` ~:71, ~:132 | IN-list temp-table prefix/threshold | `"DB2".equals(dbType)` | string decision outside an owner (threshold 50 for `TestDatabaseConnection` is a connection-CLASS fact, not a database fact) |
| 6 | `SqlTextVerdicts` ~:155 | is there an oracle database (H2) | `"H2".equals(dbType)` | string decision |
| 7 | `exec/SystemDatabase.open` ~:111 | in-memory engine for the metamodel tables | **JDBC product name** | product sniffing; `default ->`; why metamodel queries fail on Postgres (no in-memory Postgres) |
| 8 | `server/ConnectionResolver` ~:177 | open a session for a spec (JDBC URLs) | declared type + spec | **first binding's first connection** (`bindings.values().iterator().next()`, `connRefs.get(0)`): first-match-wins; a JSON-only runtime throws "Connection not found" (so D3 queries cannot run through the server today) |
| 9 | `test/StorelessRuntime` | a test connection's declaration | declared type | **untrue specs**: Postgres declares `127.0.0.1:5432`, the embedded Postgres is on a random port |
| 10 | `StatementExecutor.H2_DDL`, `ENGINE_TEXT`, `PlanAllocations` | fixed H2/engine-text renderers (ledger, plans) | hardwired | renderer construction outside an owner |
| — | `AnsiSqlRenderer` constructor `jdbcProduct` (added in `2defa10e7`) | session check | dialect | `EngineStyleH2` was given `"H2"` only because the constructor demands one: meaningless for a text target. Moves to the registry (§4 C3) |

Not decisions (out of scope): the warehouse's attachable-databases table (a separate program), DataCube's generated
catalog rules (data), the `ProtocolReader`/`PureComposer` `switch (type)` (protocol node kinds, not databases).

### 3.2 The `"H2"` defaults are covering a reader gap (full-suite probe, 2026-10-03)

- Upstream: `DatabaseConnection.type : DatabaseType[1]` — REQUIRED, no default
  (`legend-pure .../platform_store_relational/relationalRuntime.pure:26`). `TestDatabaseConnection` extends it with
  no default. Our prelude matches (`prelude.pure:3750`), and `NewChecker.requireAllRequired` rejects a missing `[1]`.
- All 8 default/fallback branches were instrumented and the whole suite run: **4 never fire** (`ContextReading`'s typed
  and raw readers, both `planDialect` fallbacks); **4 fire in exactly 21 legend-engine corpus tests**, both corpus
  lanes, nowhere else (`runs/dialect-homework/h2probe-tests.txt`).
- Every one of those tests DECLARES its database; our static reader fails to read it. Example:
  `executionPlanTest.pure` `testExecutionPLanGenerationForFrom` runs
  `->from(mapping, ^Runtime(connectionStores = meta::pure::mapping::modelToModel::test::shared::getConnection()))`,
  and `getConnection()` (`core_relational/relational/tests/shared.pure:23`) builds
  `TestDatabaseConnection(type = DatabaseType.H2)`; the engine's golden prints `TestDatabaseConnection(type = "H2")`.
  Our `ExecutionContext` reader does not follow `^Runtime(connectionStores = helper())`, gets nothing, and the default
  happens to be right.
- The 21: `planConn-no-context` (11 tests), `plan-no-dbtype` (10), `planConn-no-instance` (1), `toSQLString-unbound`
  (1: `sqlFunction::testAdjustDateTranslationInMappingAndQuery`). Full list: `runs/dialect-homework/h2probe-tests.txt`.

### 3.3 `java.sql` across core (`jdeps -verbose:class` on each library jar, 2026-10-03)

- **Zero `java.sql` use** in every plan-side library: spi, cache, values, error, lexer, sql, protocol, model, parser,
  builtin, platform, compiler_element_type, sql_dialect, compiler, normalizer, lineage, validation, lowering, plan,
  resolver, probe, ide.
- Users: `exec` (15 of its 33 top-level classes), `testdatagen` (2), `server_lib` (3), `test` (`PureTestRunner`,
  `ServiceTestRunner`, `TestObserver`), and the root package `driver` (`Compiler`, `StatementExecutor`, `BodyCompiler`,
  `DatabaseJudge`, `SqlTextVerdicts`).
- `exec` without `java.sql` (18): AssertListener, CanonRider, CanonicalDivergence, CanonicalForm, Census, Column, Ddl,
  EffectSink, Equality, ExecutionResult, ExecutionTrace, H2Settings, InstanceIds, PureAsserts, Row, RowLoad,
  StatementOrigin, TdsCompare.
- The browser planner (`//wasm:boundary`) depends on all of `//core`. It plans without `java.sql` only because those
  paths are never reached; `PlannerRunsOnJavaBaseTest` (a JVM with `--limit-modules java.base`) is the only check. One
  `catch (SQLException)` in `Compiler` once broke it (`exec/JdbcMetadata`'s class comment).

### 3.4 The root package (`//core:driver`, 23 classes; class-level graph, 2026-10-03)

- Depend on `exec` directly (13): AssertErrorNative, AssertVerdicts, BodyCompiler, Compiler, CsvLoad, DatabaseJudge,
  HostJudge, LiteralFold, PlanEnvelope, SeedSqlForms, SqlTextVerdicts, StatementExecutor, VerdictArm. Transitively (1):
  PlanAllocations. Clean (9): AggAwareActivities, ConnectionLets, CrossStoreGuard, ExecuteOptions, KindClass,
  MetamodelSeeds, OpSeeds, ProgramFacts, SqlTextInputs.
- None of the 9 clean classes reaches an execution-side class (no cycle). `LiteralFold`, `PlanEnvelope`, `VerdictArm`
  are used only by execution-side classes (`StatementExecutor`, the judges).
- **`Compiler`**: ~20 plan methods (parseModel, compileModel ×2, parseSources ×3, buildModel, buildModule, compile,
  plan ×2, planStreaming, resultType ×2, target, compileQuery ×2, resolveQuery, hasStatementEffects, programFacts,
  lowerResolved ×2, compileAllBodies, executesOn, dialectFor) and **11 execution entry points** (execute ×4,
  executeResolved ×4, executeWire ×2, executeStreaming).
- The execution entry points use from `Compiler`: the private record `Lowered`, private `lowerQuery`/`lowerParsed`,
  private `wireSchema` (used by nothing else), package `dialectOf(…, Connection)`, public `compileModel`/`resolveQuery`.
- The ONE plan→execution crossing: `programFacts` / `hasStatementEffects` call `StatementExecutor.containsEffect`.
  `containsTdgGenerator` (package-private in `Compiler`) is used by `programFacts` and by `BodyCompiler`.
- Callers that move: 127 direct `Compiler.execute*` calls (`runs/dialect-homework/calls.json`), `QueryService`'s
  internals, `PureV1Api.execute`, `PureTestRunner`, `ServiceTestRunner`, PCT, Channel B. `QueryService`'s own API (505
  call sites) is unchanged.
- Tests using `Compiler` non-public members: `PostgresDialectTest` (`dialectOf`). `NO_RUNTIME` (public) ×2.

### 3.5 The build graph and the guards that move with it

- Depend on `//core:driver`: `//core:core`, `//core:server_lib`, `//core:test`, `//tools/deps:layer_driver`.
- Depend on `//core:core`: core_tests_lib, duckdb_load, server, datacube's three `*_facts_main`, pe_tests_lib,
  pct_tests_lib, spec:generators, spec_tests_lib, tools/deps:core_closure, tools/engine-runner:runner, wasm:boundary.
- **`tools/deps/core-layers.txt` + `CoreLayeringTest`**: every library's direct deps, compared EXACTLY (a new or
  removed edge fails); a new library adds its line in the same push, naming the plan item.
- Guards naming the moving files (each read and updated in the push that moves them, with a dated note):
  `ArchitectureTest` (invariant 4c `phasesNeverDependOnTheDriverLayer` names `Compiler`/`StatementExecutor`; F1.3b
  `rootJavaSqlSurfaceIsPinned`), `JavaEvalLedgerTest` (exact size pin `StatementExecutor.java` 2392; `EVICT_NAMES`; the
  orchestration file list incl. BodyCompiler, Compiler, PlanAllocations, PlanEnvelope, SqlTextVerdicts,
  StatementExecutor), `JdbcSurfaceCensusTest` (Compiler.java, StatementExecutor.java), `ErrorShapeGuardrailTest`
  (Compiler 2, StatementExecutor 3), `DialectBoundaryTest` (Compiler 1), `PlannerRunsOnJavaBaseTest`; also named:
  SqlTextRatchetTest, PipelineStageFailureTest, RawSqlLedgerTest, ParserBoundaryArchTest, NoEagerUserClassLoadsTest
  (each checked at execution for whether it pins).

### 3.6 The effect scan's swallowed compile errors (full-suite probe, 2026-10-03)

`StatementExecutor.containsEffect` catches `TypeInferenceException` when a callee does not type-check and scores it
non-effectful. Its comment: an over-approximating reachability scan walks dead `match` arms whose library closures do
not type-check (toPostgresModel's `SemiStructuredObjectNavigation`); the SQL channel walls a live arm loudly. Measured:
**23 distinct callees**; corpus lanes 248 each (legend-engine library: `toPostgresModel::convertSemiStructuredArrayFlatten`
176, `sqlQueryToString::extractSemiStructuredBracketPathAccess` 176, `sqlQueryToString::useDbNativeImplicitNullOrdering`
40, `tds::toRelation::test` 16, eight `pureToSqlQuery::*`, `runtime::extractDBs`, `typeInference::getDynaFunctionTypeInferenceMap`,
`sqlQueryToString::{processCommonTableExpressions, convertStringToSQLString, h2…getDynaFunctionToSqlForH2,
default…getDynaFunctionToSqlDefault}`, `graphFetch::domain::findGetAllInQualifiedProperties`); core tests 6
(`test::getPeople`, `test::f`, `test::badReturn`). Summary: `runs/dialect-homework/effectprobe-summary.txt`.

**Verdict (user, 2026-10-03): not acceptable as is.** Everything we load must compile (it is a different thing from
being able to run); "it never runs" is asserted, not measured; and upstream's compiler internals should never be typed
at all (D6). A2 classifies every case.

### 3.7 Upstream, for reference

- Dialect per database: `dbExtension.pure:261-290` (`createDbConfig` → `loadDbExtension`).
- PCT adapter: `pct_relational.pure:97-135` (reprocess → `MyDatabase` + `ConnectionStore(connection = $dbc)` → normal
  execution plan).
- M2M in memory: `core/pure/mapping/modelToModel.pure` (`modelToModel::inMemory`), `executionPlan-execution-store-inMemory`.
- Cross-store (JSON + relational) is a real upstream case: `testCrossStoreGraphFetch.pure`
  (`XStore::inMemoryAndRelational`), `xStorePropertyAccessServices.pure`; M2M pushed into Snowflake: `snowflakeM2MUdf`.

## 4. The steps

Every step: homework first; a plan stated before code; the FULL chain (`bazel test //... --cache_test_results=no`)
before the push; corpus (both judges) and PCT (DuckDB, H2, Postgres) and Channel B numbers MEASURED, not assumed;
CI checked after (`gh run list --branch main`). Every pin or ceiling that moves gets a dated note naming this plan.
Rule 15 (`EXECUTION_PLAN` §0b): what a new structure replaces is DELETED in the push that switches. Temporary probes
are inserted and run as ONE chained command (`… && bazel test …`) so a failed insertion cannot start a meaningless
run, and are removed before any commit.

### Phase A — homework (no product change)

- **A1. Clean tree.** DONE 2026-10-03 (a scratch `TmpBlindSpotProbeTest.java` from an interrupted command removed).
- **A2. Classify the effect scan's callees — DONE 2026-10-03.** Probes (chained, removed after): the effect scan
  printed callee, full message, the whole call chain, the scanning caller and the test; the inliner (the EXECUTION
  path) printed every compile failure it hit; `PureTestRunner` marked each test. Full uncached suite. Data:
  `runs/dialect-homework/a2/` (`effect.json`, per-lane logs, `upstream-locations.txt`).

  **The headline facts**
  - 23 callees: 17 upstream library functions (corpus lanes, 248 hits each) and 6 core test helpers.
  - **"It never runs" is FALSE for 6 of the 17**: the inliner (execution) also fails compiling
    `useDbNativeImplicitNullOrdering` (5 tests), `findFunctionSequenceMultiplicity`, `mergeOldAliasToNewAlias`,
    `toRelation::test` (2 tests), `convertSemiStructuredArrayFlatten` (2 tests), `reprocessAliases`.
  - **40 corpus tests** reach a swallowed compile error: **19 are on the corpus fail roster** (expected failures), **21
    PASS** — 20 of them `toPostgresModel::tests::*` — and those 21 rely on the catch: removing it without fixing the
    causes fails them.
  - Every library callee exists in the pinned upstream sources (legend-pure compiles it); locations below.
  - Both scanners hit it: `Compiler.programFacts` (plan side) and `BodyCompiler.execute` (execution side).
  - The existing mechanisms for D6: natives / `NativeFn.JavaRoutine` (our Java implements it), `Subsumed`
    (`builtin/Subsumed.java`: an engine program in a subsystem we replace wholesale; body never spliced, value never
    consumed), `WalledBodies` (`platform/WalledBodies.java`: an engine body refused, with a reason, e.g. "the engine's
    SQL printer — the platform's compiler is the implementation"; throws `WalledBodyException`, NOT a
    `TypeInferenceException`, so the catch never hid a wall).

  **The 17 upstream library callees**

  | callee (meta::…) | upstream (core_relational/… unless noted) | why our typer rejects it | reached from | runs? |
  |---|---|---|---|---|
  | relational::functions::sqlQueryToString::extractSemiStructuredBracketPathAccess | sqlQueryToString/dbExtension.pure:980 | unknown function `isDigit` | toPostgresModel::tests → convertExtractFromSemiStructured (22 tests) | scan only |
  | relational::functions::toPostgresModel::convertSemiStructuredArrayFlatten | sqlDialectTranslation/toPostgresModel.pure:1027 | expected `QuerySpecification`, got `Union` | toPostgresModel::tests::assertConversion → convertElement (22 tests) | **yes** (testConvertJoinTreeNode, testConvertSelectSQLQuery) |
  | relational::functions::sqlQueryToString::useDbNativeImplicitNullOrdering | sqlQueryToString/dbExtension.pure:300 | unknown function `featureFlag::contextHasFlag` | sqlQueryToString::tests (5) | **yes** (all 5) |
  | pure::tds::toRelation::test | core/pure/tds/relation/testTdsToRelation.pure:428 | unknown function `toRelation::transform` | toRelation tests (2) | **yes** (testJoinFunc, testJoinUsing) |
  | relational::functions::pureToSqlQuery::findAliasMappingBySchemaName | pureToSQLQuery/pureToSQLQuery.pure:9548 | unknown function `relation` | applyMilestoningFilters → reprocessJoin; reAliasMergedJoinOperations | scan only |
  | relational::functions::pureToSqlQuery::reprocessAliases | pureToSQLQuery.pure:9575 | expected `TableAlias`, got `V` (generic) | applyMilestoningFilters; reAliasMergedJoinOperations | **yes** (milestoning SemiStructured test) |
  | relational::runtime::extractDBs | helperFunctions/helperFunctions.pure:63 | unknown function `getMappingsFromRuntime` | (entry) | scan only |
  | relational::functions::pureToSqlQuery::orderImmediateChildNodeByJoinAliasDependencies | pureToSQLQuery.pure:396 | unknown function `containsAny` | toSQLQuery | scan only |
  | relational::functions::pureToSqlQuery::defaultState | pureToSQLQuery.pure:338 | unknown function `featureFlag::contextHasFlag` | toSQLQuery | scan only |
  | relational::functions::pureToSqlQuery::findFunctionSequenceMultiplicity | pureToSQLQuery.pure:4462 | unknown function `byPassRouterInfo` | (entry) | **yes** (testFindFunctionSequenceMultiplicity) |
  | relational::functions::pureToSqlQuery::mergeOldAliasToNewAlias | pureToSQLQuery.pure:8987 | cannot access `name` on `V` (generic) | (entry) | **yes** (testMergeOldAliasToNewAlias) |
  | relational::functions::typeInference::getDynaFunctionTypeInferenceMap | relationalExtension.pure:189 | `Varchar.size` [1] given a value of another multiplicity | (entry) | scan only |
  | relational::functions::sqlQueryToString::convertStringToSQLString | sqlQueryToString/dbExtension.pure:921 | no overload of `string::replace` with 2 arguments | createDbExtensionForH2 → getDefaultLiteralProcessors | scan only |
  | relational::functions::sqlQueryToString::default::getDynaFunctionToSqlDefault | sqlQueryToString/extensionDefaults.pure:183 | `ToSql.transform` multiplicity `[*]` incompatible | createDbExtensionForH2 | scan only |
  | relational::functions::sqlQueryToString::h2::v2_1_214::getDynaFunctionToSqlForH2 | sqlQueryToString/dbSpecific/h2/h2Extension2_1_214.pure:194 | `ToSql.transform` multiplicity `[*]` incompatible | createDbExtensionForH2 | scan only |
  | relational::functions::sqlQueryToString::processCommonTableExpressions | sqlQueryToString/dbExtension.pure:737 | unknown function `orElse` | createDbExtensionForH2 → processWindowColumn → processOperation | scan only |
  | pure::graphFetch::domain::findGetAllInQualifiedProperties | core/pure/graphFetch/domain/domainManagement.pure:210 | unknown function `router::routing::isGetAllFunction` | extractDomainTypeClassFromFunction | scan only |

  **The 6 core test helpers** (`test::f`, `test::badReturn`, `test::getPeople`, `m::f`, `m::g`, `m::relay`; and
  `test::wrongReturn` on the execution path only): each is deliberately ill-typed ("declares return type Person but
  body returns Firm", multiplicity mismatches, "expected Integer, got Number"). Scanned only by `BodyCompiler`; the
  execution path then fails on them, which is what their tests check.

  **Buckets (the causes, not the callees)**
  - **(b1) a platform function we do not have** (`isDigit`, `containsAny`, `orElse`, `string::replace` with 2
    arguments): ordinary Pure platform functions; port them (natives) — they are needed by user code too.
  - **(b2) a typer gap on valid upstream Pure**: generics bound to `V` not substituted (`reprocessAliases`,
    `mergeOldAliasToNewAlias`), subtype acceptance (`Union` where `QuerySpecification` is expected), multiplicity
    checks (`Varchar.size`, `ToSql.transform [*]`). Fix the typer (AGENTS.md invariant 1).
  - **(b3) an upstream function absent from the loaded graph** (`toRelation::transform`, `getMappingsFromRuntime`,
    `featureFlag::contextHasFlag`, `relation`): find why the corpus graph lacks it (not loaded, or platform-owned
    with no declaration) — measured per name in B2.
  - **(a) upstream COMPILER machinery reached as an entry** (D6): the router (`byPassRouterInfo`,
    `isGetAllFunction`), the SQL printer and its extension builders (`createDbExtensionForH2`, `processOperation`,
    `processCommonTableExpressions`, `getDynaFunctionToSql*`), pureToSqlQuery's alias machinery, toPostgresModel. Most
    of these tests (`toPostgresModel::tests`, `sqlQueryToString::tests`, `pureToSqlQuery` tests) are **upstream's unit
    tests of its own compiler internals** — machinery legend-lite replaces.
  - **(c) deliberately ill-typed test helpers**: the test expects the compile error (confirm per test in B2).
  - **(d) DECISION FOR THE USER**: should the corpus run upstream's unit tests of compiler machinery we replace
    (`toPostgresModel::tests`, `sqlQueryToString::tests`, `tests::functions::pureToSqlQuery`, the H2 extension
    builders)? Options: (i) make them compile and run (b1–b3 fixes, possibly large); (ii) WALL their entries
    (`WalledBodies`, ENGINE_MACHINERY, with a reason) so they fail loudly and are rostered as out of scope; (iii)
    `Subsume` where a test only needs the value to be typed. Today 21 of them PASS only because the catch hides the
    failure; whichever is chosen, that hiding ends.
- **A3. The F1.3b blind spot — DONE 2026-10-03.** Measured by asking ArchUnit's own import (`CORE_PROD_CLASSES`)
  directly (temporary probe, deleted in the command that ran it; data: `runs/dialect-homework/blindspot.log`,
  `imported.txt`, `jar-classes.txt`). The hypothesis (the import filter drops product classes) was WRONG. Findings:
  1. **One product class is invisible to every `ArchitectureTest` rule**: `com.legend.exec.DuckDbAppenderLoad` (DuckDB's
     bulk loader, target `//core:duckdb_load`), because `//core:guardrails` loads `:core_tests_lib` → `:core`, which
     does not carry `:duckdb_load`. It is exactly what F1.11 (driver-native APIs funnelled) exists to check.
  2. **Four non-product classes are imported as product**: `com.legend.testing.{EmbeddedPostgres, Repo, Upstream}` and
     `com.legend.tools.junit.JUnitMain` (test support on the classpath under `com.legend`). Harmless today; the
     "product" set is not exactly the product.
  3. **F1.3b measures `java.sql` API USE, not connection HANDLING.** ArchUnit sees `BodyCompiler`, `DatabaseJudge`,
     `SqlTextVerdicts` in its import but records 0 `java.sql` dependencies for them (vs 13 for `Compiler`, 8 for
     `StatementExecutor`): they pass `Connection` values along (`env.withConnection(side.connection())`) without
     calling JDBC — ArchUnit records that as a dependency on `ExecEnv`; `jdeps` (constant-pool descriptors) records
     `java.sql.Connection`. The rule does what it says; it is the wrong measure for "never handles a connection",
     which is the property the split needs → enforced by the BUILD boundary (C2) plus a `jdeps` check of
     `:planner`'s closure, not by ArchUnit.
  Fixes go to B1.
- **A4. This document**, updated with A2 and A3.

### Phase B — fix the blind spots

- **B1. The guards see exactly the product.**
  - `//core:guardrails` (and any target running `ArchitectureTest`) loads `:duckdb_load`, so
    `DuckDbAppenderLoad` is checked; F1.11's driver-native funnel re-measured with it (its true baseline, dated).
  - `CORE_PROD_CLASSES` excludes test support (`com.legend.testing..`, `com.legend.tools..`) by name, so the set is
    exactly the product; the import is asserted to equal the product jars' class list (a test, so it cannot drift).
  - F1.3b's text says what it measures (JDBC API use); "who handles a connection" is C2's build boundary + `jdeps`.
- **B2. The A2 fixes**: bucket (a) entries become ours (native/`Subsumed`, our Java); bucket (b) typer gaps fixed in
  the typer (AGENTS.md invariant 1); bucket (c) tests expect the compile error. Size known only after A2.
- **B3. Delete the catch** in `containsEffect`: a callee that does not compile is a compile error, surfaced.

### Phase C — the restructuring

- **C1. `compiler/spec/StatementEffects`**: `containsEffect` (from `StatementExecutor`), `containsTdgGenerator` (from
  `Compiler`), and `callsVerdict` if its users agree (check first). `Compiler.programFacts`/`hasStatementEffects`,
  `BodyCompiler` and `StatementExecutor` call it. Ledger/size pins on `StatementExecutor.java` and `Compiler.java`
  re-pinned (shrink). No other behaviour change.
- **C2. The plan/execution split.**
  - New library `//core:planner` = root package's plan surface: `Compiler` (plan methods only) + the 9 clean classes.
    Its deps: plan-side libraries only — **no `:exec`, no `:testdatagen`** (the build enforces §1.2).
  - `Compiler` exposes ONE public lowering API for execution (the lowered MIR plan, its typed root, its context) in
    place of the private `Lowered`/`lowerQuery`/`lowerParsed` — a contract, not a hook. `containsTdgGenerator` is not
    on it (C1).
  - New front door `com.legend.Execution` in the execution library (today's `//core:driver`): the 11 execution entry
    points, `wireSchema`, and the session part of `dialectOf` (product check, version refinement, one
    `sessionSetup()` loop). With it: StatementExecutor, BodyCompiler, the judges (AssertVerdicts, AssertErrorNative,
    DatabaseJudge, HostJudge, SqlTextVerdicts), LiteralFold, PlanEnvelope, VerdictArm, CsvLoad, SeedSqlForms,
    PlanAllocations. Deps: `:planner`, `:exec`, the rest.
  - `//wasm:boundary` (and any plan-only consumer) depends on `:planner`, not `//core`.
  - 127 `Compiler.execute*` callers → `Execution.execute*` (compiler-checked); `QueryService`, `PureV1Api`, the
    runners, PCT, Channel B switch internally.
  - Same push: `core-layers.txt` (a `planner` line; `driver`'s line), `ArchitectureTest` (invariant 4c names the
    execution classes; a rule that `planner`'s packages do not depend on `java.sql`/`javax.sql`), every guard of §3.5,
    AGENTS.md's entry-point table (§"Entry points") and pipeline text.
  - Gate: `PlannerRunsOnJavaBaseTest`; `jdeps` on `:planner`'s closure shows no `java.sql`; the full chain.
- **C3. The database registry — one owner.** `Databases.of(DatabaseType)` on the PLANNER side, every type listed, no
  `default` arm; each entry declares its capabilities as DATA and plan-side objects (no `java.sql` type):
  execution dialect; JDBC product name; legend-engine text renderer (H2, DB2, Composite); IN-list temp-table facts;
  replay-oracle availability; in-memory-instance JDBC URL; session JDBC URL from a spec; storeless declaration.
  `Databases.named(String)` for consumers holding a Pure enum name (the Pure enum has `SparkSQL`, `DebugPrint`, which
  the Java enum lacks: refused by name). `Databases.PLATFORM` (DuckDB) for D3. The execution side does the JDBC work
  from that data. Replaces §3.1 rows 1–10: `Compiler.dialectFor`, both `StatementExecutor` renderer switches and the
  `planDialect` fallback, `InProtocol`'s DB2 strings, `SqlTextVerdicts`'s H2 string, `SystemDatabase`'s product switch,
  `ConnectionResolver`'s per-type switch (and its first-binding pick: the database comes from `executesOn`),
  `StorelessRuntime`'s switch (declaring the REAL session: the embedded Postgres's actual port), `AnsiSqlRenderer`'s
  `jdbcProduct` constructor argument, `H2_DDL`/`ENGINE_TEXT`/`PlanAllocations` constructions. D3 per the user's
  choice; with (a), a server-path test of a JSON-only runtime (fails today: "Connection not found").
  `DialectBoundaryTest` extended to ZERO outside the registry and `sql/dialect`: database-type literals, renderer
  construction, product strings, `jdbc:` URLs, `case "H2"`-style string decisions.
- **C4. The reader fix.** The static `ExecutionContext` reader follows `->from(m, ^Runtime(connectionStores =
  helper()))`, `toSQLString`'s runtime forms and helper bodies; the four `"H2"` defaults (§3.2) are DELETED; a context
  that truly cannot be read is refused by name. Gate: the 21 corpus tests pass reading their real declarations.
- **C5. Cleanups riding along** (in whichever step touches the file):
  - one exception kind for configuration errors (no runtime, undefined connection, no connection, mixed databases,
    session mismatch — today split between `MappingResolutionException` and `NotImplementedException`);
  - the same wall from `plan` and `execute` for a query with no runtime (today `plan` lowers first and hits the
    resolver's wall; `execute` hits `NO_RUNTIME` first);
  - the test rewrite of `2defa10e7`: imports placed in a separate, unsorted group in 34 files; 35 added lines over 130
    characters;
  - stale docs: `DialectBoundaryTest`'s javadoc ("maps … the JDBC product"), AGENTS.md's `SqlDialect` description;
  - `PctExecuteNative`: its call was reflowed to stay under the ledger's 106-line pin — replace with an honest form
    (justify a pin move, or not add the line).

## 5. Other open work (recorded so it is not lost; NOT in this plan)

- Regex flags as plan data (a typed regex node; dialects spell flags) — includes a live bug: `regexpIndexOf` with a
  flag builds `(?i)(?s)^…`, and `Postgres.pattern()` strips only the first group, so ARE rejects it.
- Structs on Postgres arrive as jsonb `PGobject`: decode design "option A" (Postgres delivers struct roots as JSON
  text; the Executor gains one struct-from-JSON-text arm). A parked experiment: `runs/scratch/executor-json-decode.patch`.
- PCT on Postgres: 112 expected failures (46 refused constructs, 37 collections over jsonb, 19 different errors, 4
  Float rule, 2 identity rows shared with DuckDB, 4 Relation incl. 3 jsonb key order).
- Corpus tests on Postgres (P6): needs homework (the corpus seeds raw H2 SQL).
- (Corrected 2026-10-03: `//datacube:app` on Windows LANDED on main with PR #14, `23b441852`.)

## 6. Process lessons from this session (apply throughout)

- Measure, don't sample: full-suite probes over the uncached chain; static call-site parsing over grep reading.
- A probe insertion and its run are one chained command; probes never reach a commit.
- Read comments when judging code (a "silent default" had a written rationale — still wrong, for a different reason).
- When a guard claims a property, verify the guard sees what it claims (F1.3b).
