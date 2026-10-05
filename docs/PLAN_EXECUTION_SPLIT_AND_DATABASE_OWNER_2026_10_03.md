# Plan/execution split, and one owner for every database decision (2026-10-03)

Status: **homework done (A1–A3); this line now does B1 and C3 only** (user, 2026-10-03). Everything in the
rebuild's areas is handed to `EXECUTION_PLAN_2026_09_26.md` with its measurements (findings added under W2.1, W4.4a,
W6.2) and announced in `docs/IN_FLIGHT.md` ("A fourth line"):

| step | owner | why |
|---|---|---|
| B1 guards see exactly the product | **this line** | test code and the guardrail target only |
| C3 one owner per concern (two owners, like upstream) | **this line** | dialects, exec, server; small, announced edits to `Compiler`/`StatementExecutor` |
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

- **B1. The guards see exactly the product — DONE 2026-10-03.** `ArchitectureTest.theImportIsEveryProductLibrary` (one class per product library; proven by a negative run without `:duckdb_load`: it fails naming it); `DuckDbAppenderLoad` now judged by every rule and satisfies them (F1.11 included, no baseline moved); test support out of `CORE_PROD_CLASSES`; F1.3b documents what it measures.
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
  - **The planner's API, compile once (user, 2026-10-04).** `Compiler`'s ~20 plan-side statics are three stages in
    variants — load the model (`parseModel`, `parseSources` ×3, `buildModel`, `buildModule`, `compileModel` ×2,
    `compileAllBodies`), type a query (`compileQuery` ×2, `resultType` ×2, `target`, `resolveQuery`), plan it
    (`plan` ×2, `planStreaming`, `compile` = `plan(..).sql()`, `lowerResolved` ×2) — and nearly each takes the model
    as TEXT and recompiles it (`compileModel(model)` inside `plan`, `resultType`, `target`, `compileQuery`). The
    `planner` library carries instead ONE compiled-model object: `Planner.compile(sources, options)` (strict or
    tolerant is an option) → `CompiledModel.query(text | spec)` → `TypedQuery` (its type, its `->from` runtime as
    properties) → `.plan()` → `QueryPlan` (SQL, result shape, and the connection target C3b decides). The model is
    compiled once and reused, as upstream compiles `PureModel` once and generates the plan from it. The ~127
    `Compiler.*` callers move to it (compiler-checked); no string-recompiling wrapper survives (rule 15).
  - **Order (user, 2026-10-04): C1 and C2 are THIS line's**, after C3 (C3b, C3c, the guard). The rebuild's W6.2
    keeps only the runner rewrite (`StatementExecutor` stops re-running G–I; staged plan, W6.1/W6.3); B2/B3 (the
    effect scan's swallowed errors) and C4 (the reader) stay the rebuild's. C1 only MOVES the effect scan, catch
    included, so it does not wait for B3.
- **C3. One owner per concern, keyed by the declared `DatabaseType` — the two-owner design (PROPOSED 2026-10-03, after
  the complete sweep below).** Upstream keeps two per-database registries — SQL generation (`dbExtension.pure`,
  `loadDbExtension`) and connection management (Java `DatabaseManager`s) — and so do we:
  1. **The plan-side owner** (java.base only, no JDBC concepts): execution dialect (`dialectFor`, delegated);
     legend-engine text renderer (H2, DB2, Composite); IN-list temp-table facts (DB2); replay-oracle availability (H2).
     Consumers: `Compiler.dialectFor`, `StatementExecutor` (`ENGINE_TEXT`, `H2_DDL`, the `toSQLString` switch :471–478,
     `planDialect` :1178–1189 and its `null`/`default` fallbacks, :1270, :1433), `PlanAllocations` :231, `plan/InProtocol`
     :71/:132, `SqlTextVerdicts` :155, `//wasm` (via `dialectFor`), `tools/census/RenderCensus` (debug tool, by name).
     `named(String)` turns a Pure enum name into a `DatabaseType` once (refusing `SparkSQL`, `DebugPrint`).
  2. **The execution-side owner** (in `exec`): the JDBC product name (the session check — moves OFF the dialect: the
     `jdbcProduct` constructor argument added in `2defa10e7` leaves `AnsiSqlRenderer`, `EngineStyleH2`'s meaningless
     `"H2"` with it); a session's JDBC URL from a connection spec; a private in-memory instance (DuckDB, H2, SQLite;
     Postgres refused by name); `PLATFORM` = DuckDB (D3). Consumers: `Compiler.dialectOf` (product check, then
     `forServer`/`sessionSetup` stay dialect capabilities), `exec/SystemDatabase` (keyed by the declared type, not the
     session's product: its one caller, `StatementExecutor` :2142, has `ctx` and `runtimeFqn`, so `executesOn` gives it),
     `server/ConnectionResolver` (its per-type URL switch; and the database comes from `executesOn`, ending the
     first-binding pick and the loose simple-name runtime lookup), `test/StorelessRuntime` (declares the REAL session).
  3. **The dialect, for what is the dialect's**: `exec/CsvSeed`'s seed-identifier quoting (today the union of DuckDB's and
     H2's reserved words — Postgres is not in it) uses the session dialect's own identifier spelling; the DuckDB
     driver's `JsonNode` decode (`Executor` :638, a driver class name) moves to `DuckDb.normalize` beside H2's `byte[]`
     JSON decode — IF the M2M/JSON paths allow it (checked when editing).
  - **D3, decided (user 2026-10-03): the platform's own DuckDB.** A runtime with no database runs on a private DuckDB the
    execution-side owner opens (as `SystemDatabase` does); a server-path test of a JSON-only runtime (fails today:
    "Connection not found").
  - **The server path compiles once**: `QueryService` compiles the model and hands the SAME context to
    `ConnectionResolver` (which needs `executesOn`, i.e. a compiled model) and to execution; today `compileModel` caches
    nothing, so resolving from a second compile would double the work.
  - **The guard**: `DialectBoundaryTest` pins ZERO outside the two owners and `sql/dialect`: database-type literals,
    renderer construction, type-name/product strings, `jdbc:` URLs, `ConnectionSpecification`-kind decisions about a
    database.
  - **The complete sweep this rests on (2026-10-03, `runs/db-sweep/`)**: every main tree (core, wasm, warehouse,
    testing, tools, datacube/tools) for 12 shapes — type literals, `switch` over a type, type-name strings, dialect
    `instanceof`/constructors/class names, `ConnectionSpecification`/`AuthenticationSpec` kinds, `jdbc:` URLs, driver
    class names, `H2Settings`/`Lexicon`/`TypeNames`/`Spellings` outside the dialects, and dialect capability calls.
    Out of scope by reading: the connection grammar (`ConnectionSectionGrammar`: the parser validates type names),
    `Lexer`'s `H2` keyword, `PureAsserts` (Pure TYPE names), protocol `switch (type)` (node kinds), the warehouse's own
    `TypeNames` and catalog constants (a separate program), `EmbeddedPostgres` and `datacube/tools/catalogfacts`
    (test/tool support that opens what it means to), `CsvSeed`'s `LocalH2` test-data check (a property of that spec
    kind, upstream's too), and dialect capability calls (`sessionSetup`, `needsStaticPivot`, `rawH2IsNative`,
    `forServer`: consumers asking the dialect — the right shape).
- **C3 audit (2026-10-03, before code) — what the proposal above got wrong or left open, and the fix:**
  1. **Two engine-text mappings disagree on purpose.** `toSQLString` maps `Composite` → `EngineStyleComposite` ("the
     engine-DEFAULT spellings"); plan text maps `Composite` → `EngineStyleDB2` ("the plan goldens pin Composite to the
     DB2-family spelling"). One "engine-text renderer" per database would be wrong. **Fix:** the plan-side owner has
     TWO capabilities, `toSqlStringRenderer` and `planTextRenderer`, each refusing what it does not cover by name — and
     `planDialect`'s `default ->` H2 (any other type silently printed as H2) becomes a refusal.
  2. **The `null` → H2 defaults are C4's, not C3's.** `planDialect(null)` and `dbBound == null ? "H2"` stand in for the
     reader gap (§3.2, handed to the rebuild with 21 corpus tests depending on it). C3 cannot delete them without C4.
     **Fix:** C3 leaves those null branches exactly where they are, at the call sites, marked "C4 (handed off)", and
     the owner only ever maps a KNOWN type — no default arm, no null inside the owner.
  3. **`executesOn` needs no compiled model.** It reads runtime bindings and connection declarations, which a PARSED
     model already has. Compiling in `ConnectionResolver` would double the server's compile work; restructuring
     `QueryService` to compile once is out of scope. **Fix:** `executesOn` takes a narrow input (find a runtime, find a
     connection, is-a-model-connection) — ONE implementation, adapted from a `ModelContext` (the compiler) and from a
     `ParsedModel` (the server's resolver).
  4. **D3 belongs to whoever chooses the session.** For the server path that is `ConnectionResolver`: a runtime with no
     database resolves to a private platform DuckDB (from the execution-side owner). A direct API caller still hands
     its own connection, checked against `executesOn` as today; swapping a caller's connection behind its back would
     surprise. **Fix:** D3 implemented in the resolver; the session check stays for direct callers.
  5. **The session product check moves OFF the dialect.** `jdbcProduct` lives on the execution-side owner; the
     `AnsiSqlRenderer` constructor argument and `EngineStyleH2`'s meaningless `"H2"` are deleted (they were added in
     `2defa10e7`).
  6. **`StorelessRuntime` must declare the real session.** The embedded Postgres listens on a random port; PCT knows it
     (`EmbeddedPostgres.shared().port()`). **Fix:** the execution-side owner renders a connection declaration from a
     session's coordinates; `StorelessRuntime.with(model, type)` keeps its shape for in-memory DuckDB/H2 and gains the
     coordinates for a server session (Postgres).
  7. **`CsvSeed`'s identifier quoting** — every call site is inside `CsvSeed`, which already holds the session dialect
     when it seeds. **Fix:** quote by the dialect's own identifier rule; the DuckDB+H2 union goes.
  8. **The DuckDB `JsonNode` decode** — `decodeAny`'s callers have the dialect in scope, and `unwrap` normalizes every
     leaf through `dialect.normalize` first. **Fix:** `DuckDb.normalize` turns its driver's JSON node into text under
     a JSON label (the class named there, in the dialect that owns its driver's quirks), and `decodeAny` loses the
     driver class name. Kept only if the JSON lanes (corpus, PCT Variant) stay green; else recorded and left.
  9. **The guard needs an explicit allowlist** of what is NOT a database decision (grammar, lexer, `PureAsserts`,
     protocol, warehouse, test/tool support), each with its reason — a pattern census alone flags them.
  **Order:** C3a the plan-side owner (+ its consumers, guard step 1); C3b the execution-side owner (product check,
  `SystemDatabase`, `ConnectionResolver` + D3 + `executesOn`'s narrow input, `StorelessRuntime`); C3c the dialect's own
  (`CsvSeed` quoting, `DuckDb.normalize`); then the guard at zero. Each a push on the full chain.
- **C3a — DONE 2026-10-03** (full chain `runs/c3a-full.log`). `//core:database` (`com.legend.database.Databases`; deps
  base, error, model, sql_dialect — no JDBC) owns: `dialect(type)` (was `Compiler.dialectFor`, DELETED, its wasm,
  `datacube/tools/catalogfacts` and test callers moved), `named(String)`, `inListTempTables(type)`, and two platform
  facts: `PLATFORM` (D3, used by `executesOn`) and `REPLAY_ORACLE`. legend-engine's golden TEXT for a type has its own
  one owner, `com.legend.EngineText` (root layer): `engineText(type)` (toSQLString: H2/DB2/Composite),
  `enginePlanText(type, quote, tz)` (plan text: H2; DB2 and Composite → DB2-family) and `ENGINE_TEST_DATABASE`. Why
  apart: the first run put them in `Databases`, and `ArchitectureTest`'s invariant 4d (engine-style renderers are root
  layer only — an execution path reaching one would run engine-H2 TEXT against a real session) refused it; `exec`
  will depend on `Databases` in C3b, so the quarantine is kept, not widened. Consumers switched:
  `StatementExecutor` (`H2_DDL`, `ENGINE_TEXT`, the `toSQLString` switch, `planDialect` DELETED with its `default` → H2,
  the `PlanConn` "H2"s, the activity SQL), `PlanAllocations`, `PlanEnvelope`, `plan/InProtocol`, `SqlTextVerdicts`.
  The plan's database is now a `DatabaseType` read ONCE (`StatementExecutor.planDatabase`), not a string compared in
  five files. Found while switching: `planModel` (the plan NODE model) ignored the connection's declared type and
  always printed H2 — it now reads it through the same `planDatabase`. The `null` → H2 branches stay at their call
  sites marked "C4 (handed off)" and all spell `Databases.ENGINE_TEST_DATABASE`. Guard step 1: `DialectBoundaryTest`'s
  type census moves Compiler 1 → 0, Databases 0 → 2, EngineText 0 → 1; a new census of decisions by database NAME (`"DB2".equals`,
  `case "H2"`) is pinned at what is left: the grammar (3, out of scope) and `SystemDatabase` (3, C3b).
  `tools/census/RenderCensus` is left: a debug tool that renders every dialect by design.
- **C3b — the execution-side owner, REVISED 2026-10-03 after upstream homework (audit point 3's "narrow input from the
  parsed model" is WITHDRAWN: it would re-implement `ModelBuilder.ingestRuntime`'s connection classification — inline
  connections, model, model-chain and foreign connections — a second owner).**
  - **Upstream (traced, file:line):** the server compiles first (`Execute.java:404-419`; a concrete model is recompiled
    per request, `ModelManager.java:153-155`); plan generation picks the connection per store from the runtime
    (`connectionByElement`, `runtimeExtension.pure:79-93`, called at `relationalMappingExecution.pure:59`) and writes the
    definition INTO the plan (`SQLExecutionNode.connection`, `SQLExecutionNode.java:32`); the executor opens it only when
    it reaches the node (`RelationalExecutor.java:400-419` → `ConnectionManagerSelector`), throwing on any unknown spec
    or type — no fallback. `ExecuteInput`'s runtime is OPTIONAL (`ExecuteInput.java:28-34`; null passes through,
    `HelperRuntimeBuilder.java:277-279`): a storeless lambda is planned as a platform node and runs in Java
    (`executionPlan_generation.pure:48-60`); model data runs in memory (`storeContract.pure:86-92`). NOT copied:
    `connectionByElement`'s silent `at(0)` when no connection matches the store.
  - **The rule (user, 2026-10-03; SEMANTICS_REGISTER S27 widened):** a query that reads a declared database runs there;
    a query that reads NO database — model data only, or literals only — with no runtime runs on the platform DuckDB;
    a storeless query WITH a runtime runs on that runtime's database (upstream ignores the runtime there: recorded
    difference — PCT's lanes rely on it, and an explicit runtime is honoured). A query that reads a database with no
    runtime is refused by name, for relation accessors too (today only the class-query wall exists,
    `StoreResolver.java:1503`; an accessor is refused only by the blanket `NO_RUNTIME`, which this rule replaces).
  - **Steps:**
    1. `executesOn` returns the TARGET, not just a type: `Declared(ConnectionDefinition)` or `Platform`, decided once
       from the compiled model and, for no runtime, from whether the resolved query reads a database. Two DIFFERENT
       connection definitions in one runtime are refused by name (one query, one session) — measured first.
    2. The execution-side owner `com.legend.exec.Sessions`: the JDBC product name per type (the session check, moved
       off the dialect: `SqlDialect.jdbcProduct` and `AnsiSqlRenderer`'s constructor argument DELETED, with
       `CarrierDifferentialTest`'s); opening a session from a connection definition, exhaustive over type × spec with
       no `default` arm (unknown → refused by name); a private in-memory instance (DuckDB, H2, SQLite; Postgres refused
       by name); `PLATFORM`'s DuckDB.
    3. The server: the four `QueryService` paths that resolve a connection today hand the compiler an OPENER (the
       server's `ConnectionResolver`, keeping its content-keyed cache and leases) instead of a connection; the compiler
       compiles once, computes the target, and asks the opener for exactly it. `ConnectionResolver.resolve(source,
       runtimeName)` is DELETED with its parse-based lookup — the first-binding pick, the short-name match (dead: the
       compiler resolves runtimes by full name only, `SymbolTable.resolveId`, so a short name already fails at
       execution), and its `default ->` spec arms (silent in-memory fallbacks). Tests: a JSON-only runtime over the
       server (fails today, "Connection not found"); a storeless query with no runtime; a runtime with two different
       connections; an unknown spec kind.
    4. Direct callers that hand their own connection keep it, CHECKED against the target's type (`Platform` = DuckDB).
    5. `exec/SystemDatabase` opens by the target's type through `Sessions` (its product-name switch goes: the name census
       3 → 0).
    6. `test/StorelessRuntime` declares the REAL session (the embedded Postgres port from `PctBackend`).
    7. `ModelContext.isModelConnection`'s `default false` goes (every context answers; a default hid the question).
    8. `Compiler.NO_RUNTIME` is replaced by the rule's refusal; `MetamodelStoreTest`/`MetamodelMappingStoreTest` pin
       the new wall.
  - **Gate:** the full chain uncached; PCT (3 lanes), Channel B and both corpora unchanged; the server tests above.
  - **C3b audit (2026-10-03, before code) — corrections to the steps above:**
    1. **A second owner already exists: `CrossStoreGuard`** (root, called at `StatementExecutor:358`). It maps the
       stores a RESOLVED statement touches to their bound connections (upstream's `connectionByElement`, per store) and
       refuses two different connections. It also has three silent passes: no runtime → return; runtime not found →
       return; a touched store the runtime does not bind "rides the session connection" — upstream's `at(0)` in our
       code. **Fix:** one decision, two halves. `executesOn` (runtime level, before execution) chooses the session;
       the per-statement walk CHECKS every touched store is bound to that session's connection, and refuses, by name,
       a store the runtime does not bind and a store read with no runtime. Its silent returns are deleted; it moves
       beside `executesOn` as that decision's per-statement half.
    2. **"Reads no database" is not decided by a pre-walk.** Statements resolve one at a time inside the executor,
       after user-function inlining, so a walk before execution would miss a `getAll` or `#>{db.T}#` inside a called
       function. **Fix:** no runtime → the `Platform` target; the per-statement check (1) refuses a store read with no
       runtime when it meets one (a class query still meets the resolver's existing wall first,
       `StoreResolver.java:1503`). Step 1's "decided from whether the resolved query reads a database" is withdrawn.
    3. **Two different connections in one runtime.** Refusing the whole runtime would break the direct-caller path,
       which never opens anything (it only checks the type). **Fix:** the target carries the runtime's connection
       definitions; only the server's opener needs ONE and refuses more than one distinct definition by name. Measured
       before coding: how many corpus/test runtimes bind more than one distinct definition.
    4. **`exec/JdbcMetadata`** (the product/version read kept out of `Compiler` for `java.sql` linking) merges into
       `Sessions` — the product name and the read of it in one place; `JdbcMetadata` deleted (rule 15).
    5. **The server cache key** (`ConnectionResolver.storesKey`, a hash of the model's Database declarations) comes from
       the parsed model today; under the opener it is computed from the compiled model (`ModelBuilder.databases()`,
       to be exposed on `ModelContext`), so the server still parses once.
    Unchanged by the audit: the rule; `Sessions`; the opener; direct callers checked; `SystemDatabase`;
    `StorelessRuntime`; `isModelConnection`'s default; the server tests.
  - **C3b measurement (2026-10-03, `runs/conn/`; probe `runs/conn/probe.patch`, reverted; corpus H2 + DuckDB, core
    tests, PCT H2, Channel B, uncached, all green under the probe):**
    1. **No runtime binds more than one distinct database connection** — 0 of 47 runtime shapes in the core tests,
       0 in PCT, Channel B and both corpora. Audit point 3's server refusal affects nothing measured.
    2. **Scope limit — the corpus never decides a connection from a test's own runtime.** Every corpus statement
       executes under the harness's one runtime (`rcorpus::Rt`, `MinimalCorpus.java:113`); a test's own
       `->from(mapping, runtime)` is read only for its declared TEXT (the C4 reader). Upstream's in-query runtime IS
       the runtime. So the rule's "no runtime" must mean neither the caller nor the query declares one; until C4 reads
       the query's runtime, a store read under an in-query runtime with no caller runtime stays REFUSED by name (as
       today, via `NO_RUNTIME`) — never sent to the platform DuckDB.
    3. **`CrossStoreGuard` ran on ONE of five execution paths** (CORRECTED 2026-10-04 — first recorded as "blind to
       class queries", which was wrong: a resolved class query names its tables in `TypedTableReference`s built from
       the mapping, and a probe on `RelationalMappingIntegrationTest` saw `store::DB` in 248 statements). Queries
       resolve for execution at five sites: the statement path (`StatementExecutor` prepare — checked), `execute()`
       frames, legend-query frames, staged assertion sides, and the compiler's own `lowerParsed`/`lowerResolved`
       (the server's wire/streaming/plan paths) — those four unchecked. The corpus runs its class queries through
       `execute()` frames, hence 35 of ~256,000. **Fix (done in C3b):** one helper, `resolvedToExecute`, resolves
       and checks for every `StatementExecutor` execution path; `lowerParsed`/`lowerResolved` decide the runtime
       (`executesOn`) and then check.
    5. **(found 2026-10-04) `Compiler.target(model, query)` already reads a query's own `->from(.., runtime)`** (its
       typed `TypedFrom.runtime()`), taking the FIRST found. A starting point for the in-query runtime, not an answer:
       first-match is the shape this line removes; C4 owns reading it fully.
    4. **The one "unbound store" case seen is the platform's own metamodel store** (`meta::lite::metamodel::MetamodelStore`,
       73 statements, core tests, `storeless::Runtime`): routed to `SystemDatabase`, legitimately bound by no runtime.
       **Fix:** exempted by identity, with its reason — not a silent pass.

- **C3b — DONE 2026-10-04** (full chain `runs/c3b-full.log`). `exec/Sessions` owns the session side: the JDBC product
  per type and the check of a handed session (moved off the dialects: `SqlDialect.jdbcProduct` and the
  `AnsiSqlRenderer` constructor argument DELETED), opening a declared connection (`openingFor`: every type ×
  specification and every authentication listed — the in-memory folds of unknown specifications and the silent
  "nothing to apply" for unhandled authentications are gone, refused by name), a named (H2) vs held (DuckDB, SQLite)
  in-memory database, and private instances (`openPrivate`, Postgres refused by name); `exec/JdbcMetadata` deleted
  into it. `Compiler.executesOn` is public and returns the `com.legend.database.Target` (`Declared` with the
  connection definitions, or `Platform`); it now also reads a ModelStore's inline `JsonModelConnection`s as model
  data (found by the new JSON-only server test: such a runtime was refused, "binds no connection"). The server opens
  the compiler's target: four `Compiler` entries take a `Sessions.Source` (the connection overloads wrap
  `Sessions.given`, checked); `ConnectionResolver.resolve` and its first-binding pick and short-name match are DELETED;
  it is the server's source (`SOURCE`, `lease(ctx, runtime)`), refusing two different connection definitions by name,
  its cache key read from the compiled model (`ModelContext.databases()`, new). `SystemDatabase` opens by the declared
  type. `CrossStoreGuard`: its no-runtime / undefined-runtime passes are hard failures (unreached); a touched store the
  runtime does not bind is refused by name, a store INCLUDED by a bound database counting as bound (the stress suites'
  `store::DB` includes ten domain stores — the first full run refused them), the metamodel store exempt by identity;
  and every execution path now runs it (`StatementExecutor.resolvedToExecute`; `lowerParsed`/`lowerResolved` decide
  the runtime first). `ModelContext.isModelConnection` has no default. `StorelessRuntime.onServer` declares a server's
  real coordinates (`PctBackend.withStorelessRuntime`: the embedded Postgres port). Tests: `ServerSessionsTest` (a
  JSON-only runtime on the platform DuckDB, an unbuilt specification refused, two connections refused); the server
  test helpers lease through `ConnectionResolver.lease`. Guards: `DialectBoundaryTest` (Sessions 1; SystemDatabase's
  name decisions 3 → 0), `JdbcSurfaceCensusTest` and `JavaEvalLedgerTest` (JdbcMetadata → Sessions; StatementExecutor
  2377 → 2382, PctExecuteNative 106 → 105), the Postgres PCT roster's one message, `core-layers.txt` (exec,
  server_lib reach database; server_lib reaches compiler), SEMANTICS_REGISTER S27.
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
- **C6. The legend-engine text printers become TEST tools (user direction 2026-10-03, after the measurements below;
  REVERSES D16 of EXECUTION_PLAN_2026_09_26.md, "a backwards-compatible product dialect", and so W5.6).**
  - **Why.** `EngineStyleH2`/`EngineStyleDB2`/`EngineStyleComposite` (2,248 lines) were grown one golden at a time to
    match legend-engine's TEXT; their SQL is never run (a text assert passes on byte equality, else the rows appeal
    runs our REAL dialect). Measured 2026-10-03 (`runs/gap/gap2.tsv`; probe `runs/gap/probe.patch`, reverted): of
    1,699 sql-text asserts in the H2 corpus, our engine text replayed on the seeded H2 oracle (legend-engine's
    `legend_h2_extension_*` functions installed) does not RUN for 273 of the 1,087 whose text differs — 168 "column not
    found" (alias renames pointing nowhere), 98 DuckDB spellings inherited where no golden pinned an H2 one
    (`starts_with` 37, `ends_with` 31, `error` 14, `date_part` 12, `regexp_full_match` 2, `HUGEINT` 2), 7 syntax — and
    gives a different answer for 41 more (13 row counts, e.g. `inner join` where the engine has `left outer join`; 28
    values). Making them product dialects means fixing all of that for SQL nobody runs, and DB2/Composite have no
    database to check against. A user's `toSQLString` meanwhile returns that text.
  - **The verdict, rows first** (measured 2026-10-03, `runs/gap/rowsfirst.tsv`; probe `runs/gap/rowsfirst-probe.patch`,
    reverted; every assert's rows leg forced, 1,656 asserts / 1,541 tests, H2 corpus, host judge):
    1. ~~our real H2 text equals the golden~~ — DROPPED: 0 of 1,656 match (our dialect formats differently);
    2. **rows**: legend-engine's SQL replayed on the oracle vs OUR rows from the real dialect — 1,528 match (1,502 sql,
       26 plan); 9 diverge;
    3. **exact engine text, only where rows cannot run** — 119 asserts: legend-engine's SQL cannot replay alone (48),
       our rows underivable (26), DB2 (33) / Composite (7) goldens, plan parameters unbindable (10). 98 of them pass
       today on exact text.
    Versus today (text first, rows as the appeal) this changes ONE verdict:
    `meta::relational::tests::...::testToSQLStringForTDSStringJoin` passes today on exact text but its rows DIVERGE —
    a real product bug the text-first order hides (the other 8 divergences already fail on text today).
  - **Steps.** (a) `toSQLString`, execution-plan text, an `execute()` result's activity SQL and setup DDL text print the
    declared type's REAL dialect (`Databases.dialect`); DB2/Composite refused by name. (b) The judge
    (`SqlTextVerdicts`, already test-only — it walls without the harness's oracle) goes rows first; step 3 asks the
    harness for the engine text through an SPI it registers, the `SqlReplayOracle` pattern. (c) The three printers and
    the Lowerer's engine-only options (`withEngineText`, `withEngineExistsJoinForm`, `PlanEnumForm`) move to the test
    harness; `com.legend.EngineText` is deleted; invariant 4d becomes "no engine-style printer in main". (d) The fail
    roster gains `testToSQLStringForTDSStringJoin` with its reason; the bug is fixed or rostered.
  - **Not yet measured — before (a)/(c):** the plan-text consumers that do not reach this judge (only 53 plan asserts
    did), activity-SQL and setup-DDL text asserts, the DuckDB lane (same oracle, expected identical), and whether a
    test-side printer can reach the Lowerer's engine options without a hook in main.
  - **Order:** after C3 (C3b, C3c, the guard); `EngineText` stays until then, labelled test-only-to-be.

## 5. Other open work (recorded so it is not lost; NOT in this plan)

- Regex flags as plan data (a typed regex node; dialects spell flags) — includes a live bug: `regexpIndexOf` with a
  flag builds `(?i)(?s)^…`, and `Postgres.pattern()` strips only the first group, so ARE rejects it.
- Structs on Postgres arrive as jsonb `PGobject`: decode design "option A" (Postgres delivers struct roots as JSON
  text; the Executor gains one struct-from-JSON-text arm). A parked experiment: `runs/scratch/executor-json-decode.patch`.
- PCT on Postgres: 112 expected failures (46 refused constructs, 37 collections over jsonb, 19 different errors, 4
  Float rule, 2 identity rows shared with DuckDB, 4 Relation incl. 3 jsonb key order).
- Corpus tests on Postgres (P6): needs homework (the corpus seeds raw H2 SQL).
- (Corrected 2026-10-03: `//datacube:app` on Windows LANDED on main with PR #14, `23b441852`.)
- Cross-store queries (user, 2026-10-03: "we will have to support xstore queries"). Upstream plans one node per store,
  each with its own connection (`connectionByElement` per store), and joins the results (XStore graphFetch: relational +
  model data). Today lite refuses a query whose stores sit on different connections (`CrossStoreGuard`, one session
  per query), and C3b keeps that refusal; C3b's shape does not block the feature (the decision is per store, the
  opener can be asked for more than one connection). Open design, honouring "the database executes": e.g. DuckDB
  attaching the other databases and running the whole query (the Postgres pass-through strategy).

## 6. Process lessons from this session (apply throughout)

- Measure, don't sample: full-suite probes over the uncached chain; static call-site parsing over grep reading.
- Trace upstream's WHOLE flow for a seam before designing its fix — not the one function. C3b's first design (the
  server reading the parsed model) was written from an assumption; tracing `Execute` → plan → executor showed upstream
  decides the connection after compile, from the compiled model (2026-10-03, user catch).
- A probe insertion and its run are one chained command; probes never reach a commit.
- Read comments when judging code (a "silent default" had a written rationale — still wrong, for a different reason).
- When a guard claims a property, verify the guard sees what it claims (F1.3b).
