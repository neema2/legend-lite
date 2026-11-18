# PLANNER: verdict MIXED

## Summary
The decomposition underneath is sound. A typed query becomes prepared statements (`ProductStatements.prepare`), then lowered MIR, then plan nodes on a `Target`. Loads carry a cadence and a content identity, and a target's session facts come from one maker (`Targets.of`). The leaf types are the best part: sealed records, exhaustive switches, refusals that name the thing.

The orchestration is where the debt sits. `PlanMaker` drives a program through a 9-parameter, flag-driven `programmed(...)` that a writing query runs twice (once as a probe, once for real). The "lower a prepared statement for a target" sequence is rebuilt by hand at six or more sites. The product planner still branches on the test door.

The planner's API is not a type. 33 of 36 classes are public, with 125 public statics, and a test-side allowlist of 61 signature strings (`PLANNER_ENTRIES`) polices who calls what. The root (`Compiler`, `TypedQuery`) and the planner depend on each other.

The series added stringly-typed payload identity to the MIR. It also added about 3.5K lines of test-data-generator machinery, with structural duplication and modal state in the Lowerer.

Readability is hurt in every file. There are about 1,250 inline fully qualified names, and the javadoc is dense with process history (242 dates, 83 "audit", 43 "until then"). A new engineer would not be productive in `PlanMaker` or `TestDataPlanner` within a week.

The verdict is MIXED, at the WEAK end for `PlanMaker` and `testdatagen`. `setup/`, `Targets`, `LoadSteps` and the host-plan records are close to SOLID.

## Strengths
- **Sum types at the edges, with no `default` arms.**
  - Sealed hierarchies: `PlannedStep` (core/src/main/java/com/legend/planner/PlannedStep.java:17), `LoadPlanner.RemoteLocation`, `HostFileInto` (LoadPlanner.java:82, :316), `Synthesize.Knob` (Synthesize.java:51) and the series' `HostOperation` hierarchy (sql/dialect/HostOperation.java:21).
  - Exhaustive switches with no default: `ConnectionPlans.specification`/`authentication` (ConnectionPlans.java:38-85), `DataSources.read(SqlDml)` (DataSources.java:235-256), `PlanMaker.readsAnotherSource` (PlanMaker.java:1332-1345). A new variant fails to compile.
- **Invariants live in compact constructors, not prose.**
  - `RowLoad` checks width, types and columns (setup/RowLoad.java:51-68).
  - `LoadSteps` copies defensively (LoadSteps.java:31-34).
  - `TestDataPlanner.Comparison` checks its "either decided or two statements" rule (TestDataPlanner.java:1076-1080).
  - `PlainLocation` and `BearerLocation` refuse tokens and userinfo (LoadPlanner.java:86-110).
- **One identity scheme and one target maker.**
  - Every load is named by `LoadPlanner.identity` (LoadPlanner.java:420-422).
  - Every target's session setup, settings, raise read-back and transaction come from `Targets.of` (Targets.java:24-28). "Exec sends only what the plan carries" is honoured at one place.
- **Credentials stay out of the plan by construction.** `LoadSteps.held`/`Bind.Held` name a host's secret and never hold its value (LoadSteps.java:104-112; LoadPlanner.java:164-185).
- **Refusals carry the table, the column and the value.** Examples: `RowLoad.refused`/`unsupported` (RowLoad.java:262-270) and `QueryDoor.TestOnly`'s what/why pairs (QueryDoor.java:47-74).
- **`RowLoad.cell` writes typed literals by declared type.** It refuses sub-second digits the column cannot keep, instead of letting the database cut them (RowLoad.java:135-238).
- **Low textual copy-paste.** 1.48% at 5 lines/40 tokens; the duplication is structural (see findings).
- **Tests are mostly behavioural against real in-process databases.**
  - `TestDataPlannerTest` is parameterised over every in-memory database (core/src/test/java/com/legend/testdatagen/TestDataPlannerTest.java:62).
  - `SentStatementsAreThePlansTest` records the wire and compares it to the plan (core/src/test/java/com/legend/planner/SentStatementsAreThePlansTest.java:47-60).
  - `PlanMakerTest` asserts answers, not shapes, in most cases.

## Findings

### PLANNER-1 [S1] The planner's API is an allowlist of signature strings, not a type
- Where:
  - Surface: core/src/main/java/com/legend/planner/ (33/36 classes `public`, 125 `public static`).
  - Allowlist: core/src/test/java/com/legend/ArchitectureTest.java:1129-1240 (`PLANNER_ENTRIES`, 61 entries). It includes `"planner.OutputPlan$Mode#ordinal()"` and `#values()` (:1226-1229).
  - Leaky root type: core/src/main/java/com/legend/TypedQuery.java:72-77 and :41-46.
- What:
  - Almost every planner class is public, because `testplan` reaches its internals. Counting `com.legend.planner.*` references from `testplan`, `testexec` and `server`: `PlanEnv` ×12, `BodyPlanner` ×11, `FramePlanner` ×9, `ProductStatements` ×8, `PlanAllocations` ×6, and others.
  - What may be called is enforced by an ArchUnit rule that lists each permitted member by full JVM signature with its permitted caller classes, enum synthetic methods included.
  - `TypedQuery` documents that `specs()`, `resolved()`, `body()` and `withBody()` are "the planner's and the test planner's internals, public since step C". The "product API" is a subset described in prose.
- Why it matters:
  - Any refactor that changes a parameter type, adds an overload or moves a caller must edit a string table in another file.
  - A reader cannot tell the API from the internals by looking at the code.
  - The guard exists because Java visibility was given up, not because the invariant needs a test.
- Expert alternative:
  - Three narrow public types, with everything else package-private (or JPMS `exports`/Bazel `visibility` on a `//core:planner_api` target):
    - `planner.api.Planner { ExecutionPlan plan(TypedQuery, PlanRequest) }`
    - `planner.api.HostPlans` (the boundary's remoteMount, snapStream, frameLoad, hostFile, projectData, synthesize, snapCopy, output, authored)
    - `planner.spi.ProgramExtension` (what `testplan` needs)
  - `TypedQuery` becomes the product type only. The test planner gets the typed body through the SPI.
  - `PLANNER_ENTRIES` then has nothing to police and can be deleted.
- Confidence: verified

### PLANNER-2 [S1] Program planning is flag-driven, runs twice for a writing query, and still special-cases the test door
- Where:
  - The two passes: core/src/main/java/com/legend/planner/PlanMaker.java:107-113.
  - `programmed`: PlanMaker.java:416-492 (9 parameters: `door`, `kinds`, `systemRouted`, `projectDatabase`, `probe`). Nested ternaries choose the target (:429-436) and the catalog mode (:443-445).
  - Probe early returns in each planning method: :580, :615, :657.
  - Test-door branches: :429, :655, :669, :673, :694.
  - Mode enum and a check that only exists because two passes may disagree: ProgramState.java:66 (`CatalogReads { ANY, PROBE, RUNTIME, CATALOG }`) and :157-161.
- What:
  - A writing query is planned once as a "probe": every statement is prepared (inlined and resolved), and each planning method returns early under `state.probing()`. It is then planned again for real.
  - The routing decision (catalog or runtime) is a side effect collected in `ProgramState` during the probe.
  - The `ProgramKinds` extension point was meant to keep test kinds out of the product planner. Yet `planStatement` still branches on `ProgramState.Door.TEST` four times: answer shaping (`asATestAnswers`), handles, and the test door's refusals.
- Why it matters:
  - Every new statement kind must be written twice (probe and plan) and must remember the early return.
  - Preparation cost doubles for writing programs.
  - The flag combinations (door × systemRouted × probe × projectDatabase) live in nested ternaries no reader can check. Each new door would add another arm.
- Expert alternative:
  - Phase it explicitly:
    1. `List<Prepared> prepared = prepareAll(query)` (once).
    2. `Routing routing = Routing.of(prepared)`, a pure function of the resolved stores, returning `{target, dialect, catalogRegistration}`.
    3. `steps = plan(prepared, routing, policy)`.
  - Replace `Door` with a `ProgramPolicy` interface (`ProductPolicy`, `TestPolicy`) that owns the answer shape, handle answers and refusals.
  - Pass a `ProgramRequest` record instead of nine positional parameters.
- Confidence: verified

### PLANNER-3 [S2] "Lower a prepared statement for its target" is rebuilt by hand at six or more sites, and pulls are planned twice
- Where (hand-built lowering sequences):
  - PlanMaker.java:126-131 and :155-156 (query)
  - PlanMaker.java:206-221 (`copyInto`)
  - PlanMaker.java:623-628 (`stagedLet`)
  - PlanMaker.java:586-589 (`stagedGridLet`)
  - ProgramState.java:121-125 (`lowered`)
  - ProductStatements.java:421-425 (`lowerTestDataSeed`)
- Where (Lowerer configuration duplicated): ProductStatements.java:390-410 and Compiler.java:945-956.
- Where (re-plan and compare): SnapCopy.java:52 and :69-73; OutputPlan.java:84-95.
- What:
  - Each site chains a different subset of: `ProductStatements.lower`/`lowerer`, `seeded`, `postProcessed`, `namedColumns`, `EnumValueTables.apply`, `FrameCtes.attach`, `DataSources.read`, `stagedShared`, `TypedRows.statement`, `stampedIfRead`.
  - `Compiler.LoweredQuery` is constructed only to be destructured on the next line (PlanMaker.java:127, :207).
  - `SnapCopy` and `OutputPlan.make` plan the pull through `executionPlan(...)`, plan it again in `copyInto`, and then assert at run time that the two agree ("a copy is its pull's statement").
- Why it matters:
  - A new post-pass, say a dialect rewrite, has to be added in six places, and some will be missed. `plan()` already differs from `ProgramState.lowered` (no `FrameCtes.attach`).
  - The self-comparison in SnapCopy is a symptom: there is no value that represents "a statement planned for a target" to reuse.
- Expert alternative:
  - One `PlannedStatement lower(Prepared.Planned, LoweringOptions, Target)` returning `{SqlQuery tree, ExecutionPlan.Sql sql, TypedSpec root}`, used by every site. `LoweringOptions` is a record: streaming, slots, sites, sharing.
  - One `Lowerer` factory shared with `Compiler.lowerResolved`.
  - `copyInto` takes the `PlannedStatement` already made and wraps its tree in `SqlDml.CreateTableAs`.
- Confidence: verified

### PLANNER-4 [S2] The root and the planner depend on each other, the root keeps a second lowering road, and three types are called `Target`
- Where:
  - Planner calls the root: `Compiler.executesOn` ×9, `Compiler.lowerResolved` (PlanAllocations.java:500), `Compiler.LoweredQuery` (Compiler.java:716-719).
  - The root calls the planner: Compiler.java:938 (`QueryDoor.refuseRawSql`), :942 (`CrossStoreGuard`), :897 (`ProgramFacts.of`); TypedQuery.java:244-248 → `PlanMaker`.
  - The three `Target` types: database/Target.java:17, Compiler.java:729, executionplan/ExecutionPlan.java:956.
  - Two `DatabaseType` enums mapped by name: ConnectionPlans.java:30-37.
- What:
  - `Compiler.lowerResolved` is a public typing → inlining → resolving → lowering path outside the planner. It builds its own `Lowerer` and was patched by the series with the raw-SQL refusal.
  - `LoweredQuery`'s javadoc ("what execution renders with the session's dialect") is stale.
  - `PlanMaker` reads `Target decided`, `decided.target()` and `ExecutionPlan.Target target` within a few lines of each other.
  - `SystemCatalog`/`SystemRoutes` round-trip plan type → model type → dialect (`Databases.dialect(ConnectionPlans.model(engine))`, SystemCatalog.java:31, :37, :87).
- Why it matters:
  - A package cycle means neither side can be reasoned about, or extracted to a module, alone.
  - The second lowering road is exactly what G1 ("one planner") forbids.
  - Name collisions slow every read of `PlanMaker`.
- Expert alternative:
  - Move `executesOn` and the runtime decision into `planner` (`RuntimeDecision`).
  - Move `lowerResolved` into `planner` as the column-lineage lowering and route it through PLANNER-3's single lowering.
  - Delete `LoweredQuery`.
  - Rename `database.Target` to `ExecutionSite` or `Placement`.
  - Use one `DatabaseType` in the plan, with `Databases.dialect(executionplan.DatabaseType)`.
- Confidence: verified

### PLANNER-5 [S2] The series encoded payload identity in the MIR as URL strings and magic names
- Where:
  - core/src/main/java/com/legend/sql/SqlSource.java:78-112: `SourceUrl(String url, ...)`, `byDigest` returns `"data:application/json;sha256=" + digest + ","`, `namesByDigest(url)` is `startsWith`, and `placed()` scans outputs for the magic column `"u_frame_ord__"`.
  - DataSources.java:105-106, :223-233, :276-278, :385-392 (`content(url)` is a substring after the comma).
  - lowering/TableSwaps.java:38 and TestDataPlanner.java:367-369: table identity as `store + "|" + table`, split back with `indexOf('|')`.
- What: inline payload, payload-by-digest and file or remote URL are one `String` told apart by prefix. Whether a source carries row positions is inferred from a column name. A table's identity is a pipe-joined string.
- Why it matters:
  - AGENTS.md §3a demands typed MIR with no string-encoded operations. Here every reader (planner, `AnsiSqlRenderer`, dialects) re-parses the URL.
  - A new payload kind is a new prefix check everywhere, invisible to the compiler.
- Expert alternative:
  - `record Payload(PayloadRef ref, String alias, List<OutputCol> outputs, Form form, boolean placed) implements SqlSource`
  - `sealed interface PayloadRef permits Inline /*String json*/, ByDigest /*String sha256*/`
  - Keep `SourceUrl` for real URLs only.
  - `record TableIdentity(String store, String physical)` as the key in `TypedTableSwap.reads` and `TableSwaps`.
- Confidence: verified

### PLANNER-6 [S2] The raw-SQL refusal is policed door by door, and its exception class is a trap for catch sites
- Where:
  - QueryDoor.java:152, :166, :178, :189 (four refusal entry points plus an `instanceof` wrapper).
  - Callers: PlanMaker.java:94, ProductStatements.java:422, Compiler.java:938, server/TestDataRoutes.java:111 and :150.
  - error/RawSqlRefusal.java:15 (`extends NotImplementedException`).
  - PlanAllocations.java:249-260, :505-513, :441 (`if (!noLineageRows(e)) throw e;`).
- What:
  - "executeInDb's SQL is refused on every product door" is enforced by calling a refusal at each door. Audit rounds added `refuseRawSql(TypedQuery)`, then `refuseRawSql(List, SpecCompiler)` for the root's `lowerResolved`, then the test-data routes.
  - `RawSqlRefusal` subclasses the exception that the plan-model catches swallow, so each catch must re-throw it through a double-negative predicate.
- Why it matters: the next door, or the next catch of `NotImplementedException`, silently reopens the hole. Correctness depends on every author remembering two rules.
- Expert alternative:
  - Refuse once, where raw SQL becomes plannable: the product-mode `Lowerer` configuration rejects `TypedRawSqlRelation`. Every product road uses PLANNER-3's single lowering, so no door needs its own call.
  - Have the plan-model walls throw a dedicated `PlanModelUnsupported`, and catch only that. `RawSqlRefusal` then needs no filter.
- Confidence: verified

### PLANNER-7 [S2] testdatagen and setup duplicate the same logic, use a boolean mode flag, and build SQL with an 11-argument constructor
- Where:
  - testdatagen/TestDataPlanner.java:1673-1712 (`condition`) vs TestDataGenerator.java:640-670 (`planCondition`).
  - TestDataPlanner.java:1717-1755 (`milestoningFilter`) vs TestDataGenerator.java:673-709 (`planMilestoneFilter`, the same code with the two dimensions in the other order).
  - TestDataPlanner.java:1828 (`literal`) vs TestDataGenerator.java:713 (`planLiteral`).
  - setup/CsvSeed.java:298 (`schemaDeclared`) ≡ TestDataPlanner.java:1258 (`hasSchema`).
  - CsvSeed.java:574 (`stripMatchingQuotes`) ≡ TestDataPlanner.java:1280 (`unquoted`).
  - CsvSeed.java:580 (`types`) ≡ setup/Ddl.java:433 (`declaredTypes`).
  - `route` boolean: TestDataPlanner.java:152-181, :413, :616-631.
  - `new SqlSelect(` ×22 in TestDataPlanner, e.g. :600, :646, :672.
- What:
  - The series' 1,860-line `TestDataPlanner` re-implements three helpers that already existed in the same package, and they now diverge: one writes dates as typed, checked literals, the other as strings.
  - Schema, table and owner lookups across includes exist in four forms (CsvSeed, TestDataPlanner ×2, `ProjectData.declaringAll`).
  - A `boolean route` changes four behaviours: view refusal, the unseeded error, header order and block order.
  - SQL trees are built with the 11-positional-argument record constructor, `null`s included, although `SqlSelect.starOf(..).withWhere(..).withOrderBy(..).withLimit(..)` exists (sql/SqlSelect.java:106-261).
- Why it matters:
  - A fix to join conditions or milestoning must be made twice and kept in sync (it already wasn't for literals).
  - `new SqlSelect(ps, false, from, where, List.of(), null, null, order, FETCH_LIMIT, null, List.of())` hides which slot is which.
- Expert alternative:
  - One `FetchBuilder` parameterised by a `ParentRef` strategy (placeholder `${res}` vs CTE) and a `MilestoneOrder`.
  - `enum TextLayout { FUNCTION, ROUTE }` carrying header order, header case and block order as methods.
  - A shared `StoreLookup` (`schemaIn`, `tableIn`, `owner`, `declaring`) used by setup, testdatagen and ProjectData.
  - Use the `with*` builder throughout.
- Confidence: verified

### PLANNER-8 [S2] The test-data reload added modal Lowerer state and SQL-carrying HIR nodes to core layers
- Where:
  - lowering/Lowerer.java:600-626 (`swaps` deque, `swappedScans` map, `unswapped(...)` threaded through every relation arm) and :3591.
  - compiler/spec/typed/TypedPlannedValue.java:22 and TypedTableSwap.java:32 (typed HIR records holding `SqlQuery`).
  - compiler/spec/GeneratorSplice.java:335-420 (724 lines, all new).
- What:
  - The read-only reload of a generated slice is implemented as mutable state on the `Lowerer`, with an at-most-one runtime check.
  - A generic `<T> T unswapped(T source, String what)` is wrapped around six arms, with `source instanceof String url ? url : ""` inside it.
  - The compiler's spec package gained a 724-line pass that recognises upstream's `assertTestData`/`loadAndTestExecution` idioms in a body (`assertSameElements(setUpDataSQLs(..), setUpDataSQLs(..))`, `loads->map(sql | executeInDb(..))`) and rewrites them.
  - Altogether about 3.5K new lines (TestDataPlanner, GeneratorSplice, TableSwaps, two HIR nodes, GeneratedTestData, TestDataRoutes) serve one test-road feature.
- Why it matters:
  - Core layers every query passes through (`Lowerer`, typed HIR) now carry a feature's modes.
  - Typed HIR holding MIR blurs the HIR → MIR boundary AGENTS.md draws.
- Expert alternative:
  - Do the swap as a post-lowering `SqlRewriter` over `SqlSource.Table` keyed by `TableIdentity` (PLANNER-5). This needs no Lowerer state, and `TableSwaps.swapped` is already half of it.
  - Keep the idiom recognition in `testplan`, emitting plan-level nodes, rather than in `compiler/spec`.
  - If a planned value must stand inside an expression, lower it from a placeholder resolved in the planner, not from a `SqlQuery` inside a `TypedSpec`.
- Confidence: verified for the code; the placement alternative is a judgement.

### PLANNER-9 [S2] Payload ordering is a planner-level MIR repair, and DML traversal is hand-switched twice
- Where:
  - DataSources.java:64-77 (`inPayloadOrder`), :98-163 (`carried`, CCN 29).
  - :235-256 and :338-365 (two full switches over `SqlDml`).
  - :412-457 vs :497-517 (two JSON depth/quote scanners).
- What:
  - After lowering, the planner searches for any select "whose LIMIT or OFFSET picks a payload's records", walks through intermediate selects, and injects `ORDER BY place` (or `MIN(place)` for groups, rewriting `DISTINCT` into `GROUP BY`).
  - The `Lowerer`, which lowers `take`/`slice`/`drop`/`first` over the source, is where that order is known.
  - `read(SqlDml)` and `RunValues.in(SqlDml)` each enumerate every DML variant to reach its query and expressions. `RunValues` collects matches into a `List<Object>` only to test emptiness.
- Why it matters:
  - This is AGENTS.md "common mistake 5": a normalisation that reverse-engineers semantics the producer could state.
  - The 29-CCN predicate (`grouped && !groups || windows && !groups || distinct && (...)`) is where future bugs will hide.
  - A new DML variant needs both switches edited.
- Expert alternative:
  - The lowering sets an explicit order when a limit applies to a placed payload (see PLANNER-5's `Payload.placed`).
  - Add `SqlRewriter.rewrite(SqlDml)` (or `SqlDml.queries()`/`expressions()`).
  - Keep one JSON slice scanner that returns spans.
- Confidence: verified that the ordering lives only here (`git log -S inPayloadOrder`, b1f4359b8); the lowering placement is a judgement.

### PLANNER-10 [S2] Catalog registration depends on call order and is checked after the fact
- Where:
  - ProgramState.java:231-249 (`registerOnTheCatalog`: first call wins; `place` inserts the load).
  - PlanMaker.java:453-464 (three branches before the loop), :476-478 (per statement, a no-op after the first), :487-490 (post-hoc `IllegalStateException`).
- What:
  - Whether and where the model's catalog registration is placed depends on which of four calls happens first.
  - Steps placed before registration would silently miss it, which is why the end-of-program check exists.
- Why it matters:
  - Hidden ordering makes the loop unsafe to restructure. Moving a `state.add` above the registration compiles and breaks at run time.
- Expert alternative:
  - Decide the registration from `Routing` (PLANNER-2) and pass it to `ProgramState`'s constructor.
  - Insert it in one final pass over the finished steps, as `finished()` already does for the drops.
- Confidence: verified

### PLANNER-11 [S2] SystemCatalog rolls its own hash although the planner already has a portable SHA-256
- Where: core/src/main/java/com/legend/planner/SystemCatalog.java:188-220 (FNV-1a plus a custom mix, "the planner runs in the browser too ... which has no java.security").
- Contrast:
  - `com.legend.cache.Hash.ofUtf8` (portable `Sha256`, since 2026-09-23) is used in the same planner: PlanMaker.java:1006, :1249, :1292.
  - LoadSteps.java:46-47 says "the portable SHA-256 (the planner runs on WebAssembly, which has no MessageDigest)".
- What: the catalog's content key uses a weaker home-made 128-bit hash, introduced 2026-11-08 (6c72d970c), next to the SHA-256 used for every other content key.
- Why it matters: there are two content-identity schemes, one of them non-standard, and the reason given for it is false in this codebase.
- Expert alternative: `KEY = "platform:" + Hash.ofUtf8(content).hex()`.
- Confidence: verified

### PLANNER-12 [S2] Empty declared tables are made by building a CSV text and feeding it to the CSV parser
- Where: LoadPlanner.java:369-372; Synthesize.java:112-115 (`CsvSeed.steps(schema + "\n" + table + "\n" + String.join(",", columns), ...)`).
- What: to get the DDL for a declared table (create the schema, drop, create), two call sites build a fake CSV block and parse it back.
- Why it matters: it couples DDL generation to CSV parsing rules (a column name holding a comma or a quote would break it), and it is copy-pasted.
- Expert alternative: `Ddl.declaredTableSteps(TableDefinition def, @Nullable String schema)` returning the `CreateSchema`/`DropTable`/`CreateTable` steps that `CsvSeed.blockSqls` (CsvSeed.java:203-260) already emits. Both call sites use it.
- Confidence: verified

### PLANNER-13 [S3] Inline fully qualified names are everywhere
- Where (about 1,250 non-import FQN uses across the unit's main sources):
  - planner/EnginePlans.java (229)
  - Compiler.java (184)
  - planner/LoadPlanner.java (133), e.g. :145-196: `com.legend.sql.dialect.HostOperation.MakeRelation(com.legend.sql.dialect.HostOperation.Kind.VIEW, com.legend.sql.dialect.HostOperation.Name.of(...)`
  - testdatagen/TestDataPlanner.java (66), e.g. `java.util.Objects.requireNonNull` and `@com.legend.base.Nullable` in signatures (:125, :153, :248)
  - planner/SystemCatalog.java (57, and the file has no imports at all), e.g. :41-55
- Why it matters: lines run 120+ characters, and the reader sees package paths instead of the logic.
- Expert alternative: imports, and IntelliJ/Spotless "optimize imports" in the build.
- Confidence: verified

### PLANNER-14 [S3] Comments narrate process history, and some are stale or orphaned
- Where: across the unit's main sources, 242 dated references, 83 "audit", 43 "until then" and 34 quotes of user rulings. Representative:
  - PlanMaker.java:83-89 and :100-106 (rulings, audit findings and dates in the javadoc of `plan`).
  - PlanMaker.java:892-894: a comment about a method that was deleted.
  - PlanMaker.java:506-507: an orphaned javadoc stacked on another one.
  - Stale: MetamodelSeeds.java:14-20 still says `exec.SystemDatabase writes them`; Compiler.java:716-717 (`LoweredQuery`).
- Why it matters:
  - The prose style ("the X's Y, the Z's W") plus program codes (B2d.3, M3f, C5-J) make the "what and why" hard to find.
  - History belongs in git and the design docs, and it goes stale (as above).
- Expert alternative: javadoc that states the contract in present tense, with the decision log moved to the docs/commit messages, and a lint rule against date patterns in javadoc.
- Confidence: verified

### PLANNER-15 [S3] Positional booleans and telescoping overloads
- Where:
  - ProductStatements.java:83-120 (four `prepare` overloads, up to 8 parameters including `Map` and `boolean frameAnswered`).
  - PlanMaker.java:127 (`ProductStatements.lower(query, planned, st.temporal(), true, ...)`: the `true` is `streaming`).
  - LoadPlanner.java:25-56 (five `load` overloads).
  - Every host-plan method takes both `SqlDialect dialect` and `DatabaseType type` (LoadPlanner.java:145, :221, :274, :343; Synthesize.java:66), which can disagree.
  - 16 methods in the unit take a boolean flag.
- Why it matters: call sites are unreadable and easy to swap by mistake, and redundant pairs allow inconsistent inputs.
- Expert alternative:
  - `StatementRequest`/`LoweringOptions` records.
  - One `load(LoadSpec)`.
  - Host plans take `DatabaseType` and derive the dialect (or take a `Database` value that carries both).
- Confidence: verified

### PLANNER-16 [S3] Some tests pin text and counts, and nine planted classes exist only for guards
- Where:
  - core/src/test/java/com/legend/planner/DataSourcesDeliveryTest.java:201, :209 (full Databricks SQL pinned).
  - BoundaryRequestsAreClosedTest.java:171 (`assertEquals(52, refused, "the objects walked")`).
  - core/src/test/java/com/legend/planner/Planted*.java (9 classes) exist only so ArchUnit rules can show they bite.
  - core/src/test/java/com/legend/planner/Lowered.java:38-46 re-implements the product lowering sequence in test code (a seventh copy for PLANNER-3).
- Why it matters: incidental SQL changes break tests that pin no behaviour, and the planted fixtures grow with every guard.
- Expert alternative: assert results on a database (as most of `PlanMakerTest` does), or assert structure on the MIR tree. With PLANNER-1's visibility-based API, most planted fixtures are unnecessary.
- Confidence: verified

## Cross-unit observations
- **`SqlDialect` is becoming a grab-bag of capabilities** (about 40 methods: rendering, `deliver`/`inlineRows`, `raiseReadBack`, `databaseSettings`, `timestampColumnDigits`, `keysSnapshot`, staging flags, scripts, `verdictSavepoint`, catalog queries; sql/dialect/SqlDialect.java). The series added several. Database facts could be a `DatabaseProfile` record beside a pure renderer.
- **`RunScope.planning(...)` is sprinkled at about 70 call sites** (NameResolver alone has 8 identical strings, added in #101). `base/RunScope.java:35-44` keeps `ThreadLocal<int[]>` holders. That is the guards unit's call, but it costs every compiler entry point.
- **The connection description is a mirrored hierarchy in two packages.** `model.ConnectionSpecification`/`AuthenticationSpec` and `executionplan.*` are converted variant by variant (ConnectionPlans.java). This is forced by invariant 6j and well done (exhaustive). A shared leaf "connection" records module would remove it.
- **Out-parameters through one-element arrays.** `testplan/GeneratedTestData.java:206-210` calls `TestDataGenerationNatives.classifyArg(.., rowIds, dates, hash)` with `MilestoningDates[1]` and `boolean[1]`.
- **Pre-series residue next to series edits:**
  - PlanAllocations.java:308-315 prints to stderr under `System.getenv("LL_TMP_DEBUG")`.
  - Compiler.java:904-918 has two stacked javadoc blocks.
- **`TypedQuery.planned` reads a test flag from a JVM system property in the product's planning entry** (`Boolean.getBoolean("legend.plan.roundtrip")`, TypedQuery.java:249). It is cheap, but it is test behaviour in product code; a test-side decorator would do.

## Measurements
- Unit main size: 16,022 lines in `planner/`, `setup/`, `testdatagen/`, `Compiler`, `TypedQuery` and `HeldModels` (`cat … | wc -l`). `lizard`: 678 functions, average CCN 4.2, 22 over the limits (`python3 -m lizard -l java -L 80 -C 15 -w …`).
- Worst functions (lizard):

  | Function | CCN | NLOC | Params |
  |---|---|---|---|
  | `EnginePlans::planToString` | 33 | 120 | 4 |
  | `RowLoad::cell` | 30 | 55 | 3 |
  | `DataSources::carried` | 29 | 64 | 2 |
  | `Synthesize::formula` | 29 | 74 | 5 |
  | `BodyPlanner::prepare` | 27 | 80 | 2 |
  | `TestDataPlanner::same` | 22 | 123 | 5 |
  | `PlanMaker::programmed` | 21 | 59 | 9 |
  | `PlanMaker::planStatement` | 20 | 54 | 6 |

- Copy-paste (jscpd): 23 clones, 172 duplicated lines (1.48%) at `--min-lines 5 --min-tokens 40`; 2 clones (0.23%) at 8/70. The duplication is structural, not textual.
- Inline FQNs (non-import, non-comment): 1,254 across the unit (`grep -oE '\b(java\.util|java\.math|java\.nio|com\.legend)\.[a-z.]*[A-Z]\w*'`). Per file: EnginePlans 229, Compiler 184, LoadPlanner 133, TestDataPlanner 66, SystemCatalog 57.
- Comment history markers in the unit's main sources: 242 ISO dates, 83 "audit", 43 "until then", 34 "the user's ruling/go".
- Public surface:
  - 33/36 planner classes are `public`, with 125 `public static` methods (`grep -l '^public '`, `grep -c 'public static'`).
  - `PLANNER_ENTRIES` has 61 signature entries (ArchitectureTest.java:1129).
- `new SqlSelect(` with the 11-argument constructor: 22 in TestDataPlanner.java.
- The series' additions to this unit (from units/PLANNER.txt): `TestDataPlanner` 1,860 new lines, `PlanMaker` +577, `DataSources` 539 new, `LoadPlanner` 473 new, `CsvSeed` +429. In compiler, lowering and sql: `GeneratorSplice` 724 new, `AnsiSqlRenderer` 462 lines changed in #95, and `Lowerer` 110 lines changed in #95 (`git diff --stat <merge>^1 <merge> -- …`).
- The test-data generator feature is about 4,545 lines in total: testdatagen/*, GeneratorSplice, TableSwaps, TypedTableSwap, TypedPlannedValue, GeneratedTestData, TestDataRoutes (`wc -l`).
