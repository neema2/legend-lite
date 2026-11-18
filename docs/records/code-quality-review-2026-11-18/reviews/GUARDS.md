# GUARDS: verdict WEAK

## Summary
The guard layer has a sound core: ArchUnit dependency rules over the real product jars, checks that read the Bazel graph, a type-closure walk proving the plan is data, and an unusually rigorous habit of testing that each rule actually fails (planted violators, `bites()`). Most of its volume is something else, though: exact inventories of incidental structure. These include stripped line counts of 28 product files, a file-name register for 8 packages (154 names), 61 full JVM signatures of planner entry points, 88 TypeScript call sites pinned by their literal text, per-file counts of unrelated `as unknown as` casts, and per-test-file counts of `CREATE TABLE` literals. They are read by hand-written regex parsers of Java, TypeScript and Python that depend on indentation.
- **Cost.** 195 of the series' 556 product-changing commits also had to edit a guard file. A plain move commit (5300ea203) edited five guard files and still left two rules aimed at packages that no longer exist, where they now guard nothing.
- **History in the code.** Comments are 37% of the guard files' lines, with 1,392 date stamps. 33 of ArchitectureTest's 75 rule failure messages cite audit codes, rounds or dates ("J1 (2026-11-14), T3 ...").
- **Simpler tools left unused.** Bazel visibility and strict deps, package-private or JPMS access, Error Prone (already in the toolchain), typed request records and a capability token would each replace a large inventory.

The design is defensible in places. As built, it makes every change in the product more expensive, and in two cases it gives false assurance.

## Strengths
- **Rules are tested against planted violators.** `bites()` makes sure a rule fails *by a violation*, not by ArchUnit's empty-rule failure (core/src/test/java/com/legend/ArchitectureTest.java:2832-2841). There are 38 `bites` calls over 37 planted classes. This is mutation testing of guards, which few codebases do.
- **ArchUnit's method-reference blind spot is closed in one place.** `invokesWhere`, `constructs` and `reachesCodeUnitWhere` treat `X::new` and `Type::m` as accesses (ArchitectureTest.java:3920-3969). `theMethodRulesBiteAReference` checks 12 method rules against 22 planted references (ArchitectureTest.java:4122-4166).
- **Several registers check themselves and fail when an entry goes stale:**
  - `thePlannerEntriesAreTheMeasuredOnesExactly` (ArchitectureTest.java:1279-1282)
  - `everyLoadMakerOnTheListMakesOne` (ArchitectureTest.java:3981)
  - the BUILD_TIME_READERS "drop its name" check (ArchitectureTest.java:813-818)
  - the funnel registers, exact in both directions (JavaEvalLedgerTest.java:2041-2070)
- **PlanTypesAreDataTest is the model guard.** It walks the type closure of `ExecutionPlan` and `TestPlan` by reflection and allows only records, sealed types, enums and value leaves (core/src/test/java/com/legend/PlanTypesAreDataTest.java:75-166). It protects a real invariant (the plan is portable data read by three hosts), names almost no classes, and costs nothing when code is refactored.
- **The build graph is checked from Bazel's own answer:**
  - genquery output is compared with tools/deps/core-layers.txt (CoreLayeringTest) and with ts-layers.txt (tools/deps/TsLayeringTest.java:53-103)
  - each TypeScript layer is typechecked alone with the pinned `tsc` (tools/deps/BUILD.bazel:186-330)

  These are real mechanisms, not text scans.
- **The guard's own input is guarded.** `theImportIsEveryProductLibrary` (ArchitectureTest.java:118-128) fails if a product library silently drops out of the import.
- **Cheap targeted rules protect real constraints:**
  - `TEXT_WRITTEN_PORTABLY` bans `String.isBlank` because TeaVM differs (ArchitectureTest.java:2702-2711)
  - `REFLECTION_BANNED` (ArchitectureTest.java:2677)
  - `execPluginsAreExecClasses` closes the ServiceLoader hole (ArchitectureTest.java:1848)
- **Known limits are stated honestly**, for example the opaque-carrier "KNOWN BLIND SPOT" (ArchitectureTest.java:526-531) and HostSqlTest's "What it cannot see" (tools/guards/HostSqlTest.java:29-32).

## Findings

### GUARDS-1 [S1] Exact inventories of incidental structure make ordinary edits fail the build
- Where:
  - core/src/test/java/com/legend/JavaEvalLedgerTest.java:82, 2072-2104
  - core/src/test/java/com/legend/JavaEvalLedgerTest.java:1748, 2041
  - tools/guards/PlannedSqlTest.java:124-166, 244-276, 288-309, 73-97
  - core/src/test/java/com/legend/FixtureWriteCensusTest.java:91-94
  - core/src/test/java/com/legend/CodeShapeGuardrailTest.java:52
  - core/src/test/java/com/legend/ArchitectureTest.java:3523
- What:
  - **`EVICT_SIZE`** pins the exact comment-stripped line count of 28 product files, for example `PlanText.java` at 862 and `AssertVerdicts.java` at 1528. The check fails on *shrink* as well as growth ("re-pin to the measured count").
  - **`FUNNEL_PACKAGE_REGISTERS`** lists every `.java` file of root, planner, exec, testexec, result, server, testplan and testdatagen: 154 names, exact.
  - **PlannedSqlTest** pins:
    - 88 TypeScript call sites by their literal text, for example ``"fetch(`${link.baseUrl}/${at}&version=${version}${as}`"``
    - 101 `as unknown as` casts in 40 files, which its own javadoc says are each "to a type that holds no statement"
    - identifier counts in test files (`engine-client/test/plan-runner.test.ts`, 19)
  - **FixtureWriteCensusTest** pins literal `CREATE`/`INSERT` counts in 135 *test* files, under a ceiling of 950.
  - **`MUTABLE_FIELD_ALLOWLIST`** has 129 entries.
  - **`STATIC_STATE`** has 56.
- Why it matters:
  - A good refactor of these files has to edit a guard: extracting a class, deleting three lines, renaming a URL variable, or writing a new test that builds its own table. 195 of 556 product-changing commits in the series did, and JavaEvalLedgerTest alone was touched by 85 commits.
  - The pins measure the size and spelling of code, not the invariant. A file that grows by three lines of decoding is "the evaluator GREW". A file that computes a value in fewer lines passes.
- Expert alternative:
  - **State properties, not inventories.** Delete the line-count ledger. The evaluation invariant is already carried by the exec/testexec dependency walls and the plan-is-data walk.
  - **Make the funnel register a dependency rule.** Something like "no class in exec depends on X" is already ArchUnit's job. Drop the list of who exists.
  - **Narrow the TypeScript rules.** Use zero-tolerance bans where the risk is real (`any`, which exists already), and the narrow `HOLDER_CASTS` rule, which targets only types that hold the brand. Delete the per-file counts of unrelated casts and fetches. The warehouse's one SQL send is already pinned by `SUBMITS`.
  - **Delete FixtureWriteCensusTest.** Test fixtures writing their own tables is a preference, not a product invariant.
- Confidence: verified

### GUARDS-2 [S1] String-keyed rules silently stop guarding when code moves; two are vacuous now
- Where:
  - core/src/test/java/com/legend/ArchitectureTest.java:3420-3430 (target at 3427)
  - core/src/test/java/com/legend/ArchitectureTest.java:3432-3481 (target at 3475-3476)
  - core/src/test/java/com/legend/ArchitectureTest.java:2740-2743
  - core/src/test/java/com/legend/ArchitectureTest.java:2614, 2627-2628
- What:
  - `theCompatibilityJsonReaderIsTheJudgesAlone` checks `haveNameMatching("com\\.legend\\.driver\\.JudgeJson(\\$.*)?")`.
  - `hostVerdictIsReachableOnlyFromTheVerdictSeam` checks `com\\.legend\\.judge\\.(PureAsserts|CanonicalForm|CanonicalDivergence|Equality)`.
  - Neither package exists: `JudgeJson` now lives in `testexecutionplan`, and the four classes in `testexec`. Commit 5300ea203 moved them, rewrote the rules' *exemptions* to the new names, and left the *targets* as they were.
  - Pointed at the real classes, the host-verdict rule would fail. `testexec.AssertRuns` (line 264) and `DatabaseVerdicts` (line 46) call `CanonicalDivergence`, and `HostJudgments` (lines 89-106) calls `Equality` and `PureAsserts`; none of the three is exempt.
  - `theBodyDriverAndTheJudgesHoldNoJdbc` still names `driver.*` and `judge.*`.
  - `compileSideLayersAreDialectBlind` exempts `lowering.Scalars`, which references no dialect: its eight mentions are all in comments. The same rule's javadoc claims "ZERO exceptions".
- Why it matters: the guards report green while protecting nothing. The planted-violator discipline that would catch this is applied to some rules and not to these.
- Expert alternative:
  - Name targets and exemptions by class literal, for example `.should().dependOnClassesThat().belongToAnyOf(PureAsserts.class, CanonicalForm.class, ...)`, so a move is a compile error.
  - Add one meta-test that iterates every `ArchRule` constant (collect them in a `List<ArchRule> RULES`) and requires a planted violator for each.
  - For regex targets, assert that the pattern matches at least one imported class.
- Confidence: verified (package absence and the call sites confirmed by grep; the rules were not run)

### GUARDS-3 [S1] Hand-written source parsers are the analysis engine: fragile, duplicated, and they dictate code layout
- Where:
  - core/src/test/java/com/legend/PlannerSurfaceTest.java:466-468, 1109, 1293-1294, 1465
  - tools/guards/PlannedSqlTest.java:457-479, 738-808, 1028-1041, 1100-1159
  - core/src/test/java/com/legend/CodeShapeGuardrailTest.java:423-451
  - core/src/test/java/com/legend/JavaEvalLedgerTest.java:2085-2086 vs 2115-2117
- What:
  - **PlannerSurfaceTest finds Java methods and switch arms by fixed indentation:**
    - methods by `"\\n    (public |private )?static ..."`
    - a method's end by `"\n    }\n"`
    - host operations by `"\\n {12}case \"(\\w+)\" ->"`
  - **PlannerSurfaceTest reads Python dict rows by exact formatting:** `^    '(\w+)': \('(lite_\w+)', lambda m: \((.*)\)\),$`. Running `black` on `_planner.py` breaks the Java build.
  - **PlannedSqlTest carries its own parsers.** It has a TypeScript lexer (`codeOf`, cyclomatic complexity 43) and a fixed-point regex parser of type declarations. It caches every scan in a static map because "the guard timed out at its 60 s on Windows' CI".
  - **PlannedSqlTest pins product source lines verbatim**, for example `"return runAuthored(plan, { databaseType: database.databaseType, run: (sql) => database.engine.run(sql, 0) });"`.
  - **CodeShapeGuardrailTest's field regex dictates style.** It needs 4- or 8-space indentation and requires that nested-class fields "never [be] package-private", so the regex can tell a field from a local. Two allowlist entries are labelled "regex artifact".
  - **At least nine separate comment-strippers exist.** They do not agree: JavaEvalLedgerTest strips `/*...*/` before `//` in one check and after it in another, so `"http://..."` or `/* // */` gives different text to each.
- Why it matters:
  - Reformatting, a formatter run or an indentation change breaks guards with no change in behaviour.
  - Each parser is its own bug surface (it grew a stack-overflow fix, ArchitectureTest's regex history, and a cache).
  - A rule that dictates code layout for its own convenience inverts the purpose of a guard.
- Expert alternative:
  - For Java, use bytecode (ArchUnit, already loaded) or Error Prone, which is already in `LEGEND_JAVACOPTS` (tools/java/defs.bzl:17-20). For example, raise `UnusedMethod` to ERROR and enable `FieldCanBeFinal` in place of the regex scans.
  - For TypeScript, accept the stated constraint (TypeScript 7.0.2's API is an unstable bridge to its Go binary, tools/guards/HostSqlTest.java:25-27). Then keep exactly one shared lexer module, and push the rest onto the type checker, which already holds the brand, and the layer-alone typecheck.
  - For request fields, see GUARDS-5.
- Confidence: verified

### GUARDS-4 [S2] Registries stand in for access control and types (planner entries, root doors, request fields)
- Where:
  - core/src/test/java/com/legend/ArchitectureTest.java:1129-1239, 1293-1312
  - core/src/test/java/com/legend/PlannerSurfaceTest.java:80-134, 180-238, 338-384, 496-627, 790
- What:
  - **`PLANNER_ENTRIES`** lists 61 entries by full JVM signature, each with its callers. It includes:
    - record constructors, for example `planner.LoadPlanner$FrameLoad#<init>(java.lang.String, java.lang.String, boolean, java.lang.String, java.lang.String)`
    - an enum's synthetic members, `OutputPlan$Mode#ordinal()` and `#values()`
  - **PlannerSurfaceTest hand-lists:**
    - 38 boundary doors with 181 kind labels
    - 126 server JSON keys
    - 90 root-door signatures with a `Kind` per parameter
    - 39 query roads
    - 65 exports
  - **The `Kind` labels are human assertions.** The test cannot check that a field labelled `NAME` is not spliced into SQL; `UPSTREAM_PROTOCOL`'s javadoc says the real protection is `UpstreamProtocolFieldsTest`'s fuzz (lines 398-409). The automatic part is a name heuristic, `NAMES_SQL = "(?i).*(sql|statement).*"`.
  - **The keys mix two naming schemes.** The owner key drops `com.legend.`, so `planner.PlanMaker` means `com.legend.planner`, while the caller `planner.Boundary` is the wasm package literally named `planner` (line 1294 versus 1184).
- Why it matters:
  - Adding a record field, an overload or an accessor needs a guard row.
  - Commit 8d2571404, a good layering fix that moved seven helpers below the resolver, had to edit `PLANNER_ENTRIES` and core-layers.txt and left a three-line history note behind.
- Expert alternative:
  - Give the planner one public facade per client, for example `PlannerApi` with `plan`, `runtimeOf` and `facts`, and make the internals package-private. Where packages must share internals, use JPMS `exports` or Bazel `visibility` on `//core:lowering`. Then the compiler refuses a new route.
  - Decode each door's request into a record whose component *types* carry the kind (`Name`, `DataRef`, `ModelText`, `AuthoredSql`). The guard then becomes one reflective assertion, "only `AuthoredRequest` has an `AuthoredSql` component", in the style of PlanTypesAreDataTest.
- Confidence: verified

### GUARDS-5 [S2] The same invariant is encoded two or three times (Bazel deps, core-layers.txt, ArchUnit lists, source regex)
- Where:
  - core/src/test/java/com/legend/ArchitectureTest.java:1787-1789 vs core/BUILD.bazel:218 vs tools/deps/core-layers.txt (`exec` line)
  - core/src/test/java/com/legend/ArchitectureTest.java:1886-1895 (testexec), 2230-2240 (edge), 338-354 (sql)
  - core/src/test/java/com/legend/ArchitectureTest.java:2178-2228 vs core/src/test/java/com/legend/JavaEvalLedgerTest.java:1937-1995
  - core/src/test/java/com/legend/ArchitectureTest.java:2733-2748 vs core/src/test/java/com/legend/JavaEvalLedgerTest.java:1997-2016
  - core/src/test/java/com/legend/ArchitectureTest.java:2023-2085 vs core/src/test/java/com/legend/PlanTypesAreDataTest.java:75-166
- What:
  - **`EXEC_MAY_REACH`** is, by its own javadoc, "exactly `//core:exec`'s dependency list". `noProductClassReachesTheEdge` says "The build's twin is core-layers.txt".
  - **"The doors are thin"** is checked in bytecode (`theDoorsOnlyCompose`) and again in source by regex plus brace counting (`theDoorsHoldNoLogic`).
  - **"The judges hold no JDBC"** is checked in bytecode and again as a source spelling.
  - **Two reflective type walkers** (records, sealed types, collections) differ only in their leaf policy.
- Why it matters: a layer change edits the BUILD deps, core-layers.txt and an ArchUnit package list (8d2571404 touched all three). Duplicates drift apart: GUARDS-2's stale targets survived because nothing tied a rule to its twin.
- Expert alternative:
  - Let Bazel deps and `visibility` be the one source for target-level layering. Strict Java deps is the default, and the repository already uses `visibility`: 77 mentions in core/BUILD.bazel.
  - Keep ArchUnit for what Bazel cannot see: member-level rules, packages split across jars (`duckdb_cells`, `postgres_cells`), and ServiceLoader providers.
  - Keep one encoding per rule.
  - Extract `TypeClosure.walk(Type root, Predicate<Class<?>> leaf)` and use it in both walkers.
- Confidence: verified

### GUARDS-6 [S2] Guards measure names, spellings and size as stand-ins for design
- Where:
  - core/src/test/java/com/legend/ArchitectureTest.java:3374-3383, 152-158
  - core/src/test/java/com/legend/DataMechanismCensusTest.java:64-332, 337, 461-481
  - core/src/test/java/com/legend/TypeHoleDeletedTest.java:32
  - core/src/test/java/com/legend/PlannerSurfaceTest.java:790
  - core/src/test/java/com/legend/JavaEvalLedgerTest.java:82, 1849-1896
- What:
  - **`cachesAreFunneledToContentAddressedStore`** forbids class names ending in `Cache` or `Store` outside `com.legend.cache`. That is naming as a proxy for content addressing; the field-level version beside it is the real check.
  - **The deletion censuses** ban about 120 old spellings across every module forever, for example `"List<String> fill"`, `"p.fill()"`, `"keyOnly("` and `"csvStreamed"`.
    - `theDeletedRowsOnlyGrow` pins the number 12 under a 15-line changelog.
    - The `Pending`/`REMOVED_SO_FAR` machinery is dead: there are no Pending rows, and the map is empty.
  - **`verdictFilesJudgeOnly`** regex-bans `Math.`, `BigDecimal` and similar in three files. Its own javadoc admits primitive arithmetic passes.
  - **`EVICT_SIZE`** is line count as "evaluation" (GUARDS-1).
- Why it matters:
  - These guards are trivially evaded by renaming, so they give false assurance.
  - They also produce false positives on unrelated future code: a generic local named `fill`, or a class correctly named `DataStore`.
  - The deletion censuses keep a finished project's records in every build.
- Expert alternative:
  - Delete deletion censuses once the deletion lands; git history is the proof. Keep the behavioural witness that would fail if the mechanism returned, for example `SentStatementsAreThePlansTest`.
  - Replace name rules with the semantic checks that already exist beside them (`staticStateIsConstantOrRegistered`).
- Confidence: verified

### GUARDS-7 [S2] The guards dictate product code shape to suit their own analysis
- Where:
  - core/src/test/java/com/legend/ArchitectureTest.java:760-780, 988-1025, 2167-2228
  - core/src/test/java/com/legend/JavaEvalLedgerTest.java:1966-1991
- What:
  - **Parse doors are restricted.** A parse door may hold no lambda, no method reference and no nested class, and must call `RunScope.planning` on an earlier *line number* than its first parse. The reason is that "ArchUnit reads a lambda's calls as its enclosing method's".
  - **Execution doors are limited to one statement.** `Execution` and `TestExecution` must have exactly one statement per method, counted as `;` at brace depth 2.
- Why it matters:
  - The product gives up ordinary Java (a local variable, a helper lambda) to cover a checker's blind spot.
  - The check also depends on line numbers in the bytecode and on formatting.
- Expert alternative: make "asks first" a type-level fact. `RunScope.planning()` returns a `PlanningPermit`, and every lexer, parser and lowering entry takes one as a parameter, so it cannot be called without asking, in any code shape. The doors then need no shape rule at all.
- Confidence: verified

### GUARDS-8 [S2] Guard code quality: one 4,407-line class, structural duplication, repeated import cost, a silent skip
- Where:
  - core/src/test/java/com/legend/ArchitectureTest.java (whole file)
  - core/src/test/java/com/legend/ArchitectureTest.java:48-58, 832-842, 3179-3195
  - core/src/test/java/com/legend/ArchitectureTest.java:1822-1876
  - core/src/test/java/com/legend/ArchitectureTest.java:1256, 1531, 1800, 3303, 4397
  - core/src/test/java/com/legend/ArchitectureTest.java:3982-3998 vs 4069-4077
  - core/src/test/java/com/legend/ArchitectureTest.java:3608-3614
- What:
  - **One class, 122 tests, about 15 unrelated concerns:** layering, parse doors, JDBC, MongoDB, id mints, static state, temporal literals, loads, TeaVM portability, and more.
  - **838 inline fully-qualified type references** such as `com.tngtech.archunit.core.domain.JavaClass` and `java.util.List`, where imports would do.
  - **Repeated helpers:**
    - three copies of the jar importer
    - two copies of the ServiceLoader file reader
    - five spellings of "top-level class name" (`indexOf('$')` three times, `replaceAll("\\$.*")`, `split("\\$")[0]`)
    - five package-prefix matchers
  - **`assertMakes` exists and is not used** by the near-identical `everyLoadMakerOnTheListMakesOne`.
  - **`coreAndTheHostsDoors()` re-imports every product jar on each of its 8 calls.**
  - **`staticStateIsConstantOrRegistered` has two problems.** It runs `Class.forName` on every product class (initialising them all, side effects included). On `NoClassDefFoundError` or `ClassNotFoundException` it does `continue`: a silent skip in a codebase whose first invariant is "no fallbacks".
- Why it matters: a new engineer has to read thousands of lines to find the one rule that failed. The duplication drifts. Repeated imports slow the guard lane.
- Expert alternative:
  - Split the class by concern: `LayeringRules`, `ParseDoorRules`, `JdbcRules`, `DeterminismRules`, `StaticStateRules` and so on.
  - Give them a shared `ProductClasses` fixture (lazy static holders for the product import and the host-door import) and a small `ClassNames` helper (`topLevel`, `inPackage`).
  - Add a `ServiceFiles.providers(Class<?> spi)` helper and use imports.
  - Make an unloadable class a listed exemption, not a skip.
- Confidence: verified

### GUARDS-9 [S3] Process history and stale or misattached documentation dominate the guard files (pattern)
- Count: 5,347 comment lines against 8,400 code lines across 17 guard files; 1,392 dated stamps; 514 uses of audit, ruling, orchestrator, round, slice and similar; 33 of 75 ArchUnit `.as()` failure messages cite audit codes, rounds or dates (not counting plain "Invariant N" labels); 9 javadoc blocks attached to the wrong member in ArchitectureTest.
- Examples:
  - **The ledger is mostly history.** JavaEvalLedgerTest has 1,657 comment lines to 450 code lines. Its `EVICT_SIZE` block is 993 lines, 959 of them comments: a changelog of every pin bump (lines 83-1059).
  - **Failure messages cite process codes.** ArchitectureTest.java:1311-1312 reads `.as("J1 (2026-11-14), T3 (2026-11-14): the planners are reached ...")`; line 981 reads `"C5-J item 1 (2026-11-15) ... since its last round (A1, 2026-11-15)"`. They also cite commit hashes: "the final audit of 9af16fbe58's J2" appears 8 times.
  - **Javadoc is stale or attached to the wrong member.**
    - The class header still describes "the Strangler Fig migration ... from {@code engine/}" and allowed drivers "org.duckdb, org.sqlite" (ArchitectureTest.java:20-32). The engine module was deleted on 2026-08-11.
    - The reflection-ban javadoc sits on `compileSideLayersAreDialectBlind` (lines 2591-2618).
    - The wall's javadoc sits on `extensionCodecsAreTheEdges` (lines 1804-1823).
    - Other misattached blocks: lines 189, 326, 1490, 2156 and 3385-3411.
- Why it matters: when a rule fails, its message names an audit item a new engineer cannot look up. The present-tense reason is buried under the changelog.
- Expert alternative:
  - Give each rule one javadoc paragraph saying what is forbidden, why, and what to do instead.
  - Make failure messages actionable.
  - Keep the history in commit messages and the design docs. AGENTS.md's "moves ONLY with a dated justification comment" can be met by the commit message; it needs no in-code changelog.
- Confidence: verified

### GUARDS-10 [S3] Untyped tuples and if/else ladders in the guard code (pattern)
- Count: 4 sites.
- Examples:
  - `Map<String, Object[]> EVICT_NAMES` is read through `(Integer) e.getValue()[1]` (JavaEvalLedgerTest.java:1075, 2107).
  - PlanTypesAreDataTest's work queue holds `new Object[] {ExecutionPlan.class, "ExecutionPlan"}` pairs (PlanTypesAreDataTest.java:80).
  - `_LAYERS_ALONE` in tools/deps/BUILD.bazel:194 is a dict of positional 7-tuples documented only by a comment. `TsLayeringTest.ruleBreaches` is a 54-line if/else ladder over layer kinds (cyclomatic complexity 34, TsLayeringTest.java:201-254).
- Expert alternative:
  - Use `record NamePin(String regex, int count)` and `record Visit(Type type, String path)`.
  - Use Starlark `struct(layer=..., kind=..., never=..., ...)`.
  - Replace the ladder with a `Map<Kind, Predicate<String>> allowedDeps` table.
- Confidence: verified

## Cross-unit observations
- **The guards shape the product.** The door classes (`Execution`, `TestExecution`) are written to the one-statement rule, and the parse doors have no lambdas. Product javadoc probably carries the same dated-ruling narrative; other units should check.
- **The root cause is project policy.** AGENTS.md's rule "a ceiling, pin, or allowlist entry moves ONLY with a dated justification comment" is what turns guard files into changelogs. It is a process choice, not a code requirement.
- **Packages are split across test and main.** The 37 planted violators (1,674 lines) live in product packages under test sources (`com.legend.exec`, `planner`, `server`). That is acceptable for tests, but it means the test target compiles against product internals that the product's own Bazel deps forbid.
- **A package-name collision.** The wasm boundary sits in the package literally named `planner` (wasm/src/main/java/planner/Boundary.java), which collides in print with `com.legend.planner` in every guard key that drops the `com.legend.` prefix.

## Measurements

**Sampled guards: invariant, how it detects, cost, class**

| # | Guard | Invariant | Detection | Cost | Class | Simpler mechanism |
|---|---|---|---|---|---|---|
| 1 | `packageDependenciesAreAcyclic` (ArchitectureTest.java:197) | no package cycles | ArchUnit slices | trivial | essential | |
| 2 | `sqlLayerIsFullyStandalone` (:337) | MIR is pure data | ArchUnit allowlist | low | essential | duplicates the Bazel `sql: base` line |
| 3 | `execReachesNoPlanning` (:2503) | exec reaches no planning library (G4) | bytecode deps vs a package set | low | useful | Bazel deps already enforce it per target; ArchUnit adds only the split-package jars |
| 4 | `PLANNER_ENTRIES_ONLY` with the exact-match check (:1104, :1279) | no new route into lowering | 61 signatures with their callers | high | harmful | package-private facade, JPMS, Bazel visibility |
| 5 | `everyParseIsReachedThroughADoorThatAsks` (:781) | a run cannot parse | access rule plus line-order and lambda bans | high | useful | `PlanningPermit` capability parameter |
| 6 | `theDoorsOnlyCompose` and `theDoorsHoldNoLogic` (:2178, JavaEvalLedgerTest.java:1946) | the doors only compose plan and run | bytecode plus regex and brace count | medium | low-value | |
| 7 | `noCompiledModelReachesTheRun` (:2267) | the run reads only the plan | call-graph reachability | medium | useful | |
| 8 | `aRunTakesDataSealedSourcesAndNamedReceiversAlone` (:1988) | no callbacks into the run | reflective type walk | low | useful | |
| 9 | `PlanTypesAreDataTest` | the plan is data | reflective type closure | trivial | essential | |
| 10 | `staticStateIsConstantOrRegistered` (:3601) | no hidden mutable static state | `Class.forName` plus reflection, 56-row register | medium | useful | |
| 11 | `cachesAreFunneledToContentAddressedStore` (:3374) | caches are content-addressed | class-name suffix | low | low-value | |
| 12 | `textTheTabWritesDifferentlyIsWrittenPortably` (:2702) | TeaVM `isBlank` quirk | one access rule | trivial | essential | |
| 13 | `hostVerdictIsReachableOnlyFromTheVerdictSeam` and `theCompatibilityJsonReaderIsTheJudgesAlone` (:3432, :3419) | the verdict seam is isolated | name regex | low | harmful (vacuous) | |
| 14 | `javaEvaluationSurfaceOnlyShrinks` (JavaEvalLedgerTest.java:2072) | "Java evaluates nothing" | exact line counts of 28 files | high | harmful | |
| 15 | `theFunnelPackagesAreClosedRegisters` (JavaEvalLedgerTest.java:2041) | new classes are reviewed | 154 file names | high | harmful | |
| 16 | `thePlannerSurfaceIsClosed` (PlannerSurfaceTest.java:1422) | no request field carries SQL | regex on sources, 181 kind labels | high | low-value | typed request records |
| 17 | `everyTransportHasOneChokePoint` (PlannedSqlTest.java:1401) | one database send per transport | 88 call-site texts | high | harmful | keep `SUBMITS` and `DATABASE_SENDS`, drop `FETCHES` |
| 18 | `noAnyInTheProduct` (PlannedSqlTest.java:1533) | no `any` in TypeScript | regex | low | useful | a lint rule |
| 19 | `FixtureWriteCensusTest` | test fixtures seed through plans | per-test-file counts | high | harmful | |
| 20 | `aDeletedMechanismsSymbolsReadZero` (DataMechanismCensusTest.java:435) | deleted mechanisms stay deleted | grep for about 120 spellings | medium | low-value | |
| 21 | `mutableInstanceStateIsExplicit` (CodeShapeGuardrailTest.java:820) | instance fields are final | indentation regex, 129 allowlist entries | high | low-value | Error Prone `FieldCanBeFinal` |
| 22 | `CoreLayeringTest` with core-layers.txt | the Bazel graph is as declared | genquery diff | medium | useful | |
| 23 | `TsLayeringTest` plus the layer-alone typecheck | the TypeScript run/planner split | genquery plus `tsc` | medium | useful | Bazel `visibility` would cover the dependency half |

**Size of the layer (17 main guard files)**
- 14,467 lines: 8,400 code, 5,347 comment.
- 206 `@Test` methods; ArchitectureTest alone has 122 tests in 4,407 lines.
- 1,392 date stamps. Command: python script at review/guardstats.py.

**Hard-coded names and pins**
- 2,415 name-like string literals across 13 guard files: ArchitectureTest 774, PlannerSurfaceTest 513, PlannedSqlTest 213, JavaEvalLedgerTest 206, DatabaseOwners 94. Command: review/namepins.py.
- Registers and pins:

| Register | Size |
|---|---|
| `PLANNER_ENTRIES` | 61 signatures |
| `STATIC_STATE` | 56 |
| `EVICT_SIZE` | 28 line counts |
| funnel registers | 154 file names |
| `MUTABLE_FIELD_ALLOWLIST` | 129 |
| FixtureWriteCensusTest `PINNED` | 135 files |
| PlannedSqlTest integer pins | 65 |
| PlannedSqlTest call-site text pins | 88 |
| PlannerSurfaceTest `ROOT_DOORS` | 90 |
| PlannerSurfaceTest `BOUNDARY` | 38 doors, 181 kind labels |
| PlannerSurfaceTest `SERVER_FIELDS` | 126 keys |
| PlannerSurfaceTest `QUERY_ROADS` | 39 |
| PlannerSurfaceTest exports | 65 |

**Stale rules**
- 2 rules target packages that no longer exist; a third rule targets them in part. Command: review/fqncheck2.py plus `ls core/src/main/java/com/legend/{judge,driver}`.
- 1 dead exemption (`lowering.Scalars`).

**Refactor cost, measured on the series' move commits** (`git show --numstat`, guard files only)

| Commit | What it did | Product files | Guard files | Guard lines |
|---|---|---|---|---|
| 5300ea203 | moved 22 classes into testexec | 110 | 5 | 51+/49- |
| 8e59e9ec3 | deleted the driver package | 32 | 6 | 40+/36- |
| 8d2571404 | moved the resolver's helpers below both layers | 8 | 2 | 8+/15- |
| 4f268584f | moved `Census` from exec to diagnostics | 12+/8- lines | 2 | 6+/7- |
| 3dea62938 | moved the host-judgment types into the plan | (not counted) | 2 | 17+/8- |

5300ea203 also left the two vacuous rules behind. In 4f268584f the guard churn equalled the product churn.

**Commit coupling**
- Over the series, 195 of 556 product-changing commits (35%) also edited a guard file; 241 of 936 non-merge commits touched one.
- Guard files gained 10,629 lines and lost 2,568, against 56,649 added and 30,929 deleted in Java product code.
- Net over the series: product Java +25.4K lines, guards and deps +10.5K (`git diff --shortstat 770acc55^1 4dfbb5b7`).

**Complexity and duplication**
- lizard flags 16 guard methods with cyclomatic complexity of 15 or more. The worst are `PlannedSqlTest.codeOf` (43), `ArchitectureTest.mutableObject` (37), `TsLayeringTest.ruleBreaches` (34) and `PlannerSurfaceTest.rootViolations` (33). Command: `python3 -m lizard -l java -w -L 80 -C 15 ...`.
- jscpd finds 0.5% textual duplication in the large guard files. The duplication is structural: see GUARDS-5 and GUARDS-8.
- At least nine separate comment or code strippers (`grep -n "static String (sanitize|blankNonCode|codeOf|strip...)"`).
- Inline fully-qualified type references: 838 in ArchitectureTest, 1,146 across 17 guard files.
