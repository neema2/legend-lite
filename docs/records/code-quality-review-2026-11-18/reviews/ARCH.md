# ARCH: verdict WEAK

## Summary
The core decomposition is right for a push-down engine and close to legend-engine's own: a compiler and planner produce a closed, immutable, physical plan (rendered SQL per target, typed slots, loads), exec binds and sends it, and a streaming sink writes typed rows at the edge. The main wall (exec reaches no planning library, the planner reaches no exec) is enforced cheaply and correctly by the Bazel graph. Around that core the series built a lot of accidental structure, and that structure makes ordinary changes expensive.
- **Parallel step models and runners.** "A program of steps" has 4 representations and 2 Java walkers that re-derive the same rules (ARCH-2).
- **A hand-kept plan schema.** The schema is hand-kept in 3 languages and 5 codecs, versioned by exact match: 25 format bumps in the window, about 30 files per change (ARCH-1).
- **Guards as large as the product.** The guard layer (~24K lines plus 38 planted violators) is about the size of the plan/exec product code (~25K) and has shaped its API (ARCH-3).
- **Type holes.** The plan's types admit roots, nestings and judge combinations that the runners refuse at run (ARCH-4).
- **Process-log documentation.** Code and docs read as a process log: ~2,100 history references in the 7 packages, 455 docs, 5 "start here" pages (ARCH-8).

Is the plan/exec boundary well placed? Yes in kind, but session routing is spread across 4 routers and 2 source hierarchies over denormalised targets (ARCH-5).

Is the enforcement the right tool? Build boundaries and types would do most of it more cheaply.

The single most important improvement: make `ExecutionPlan` the one program IR, defined once (test steps as typed extension steps, invalid roots unrepresentable, targets normalised), run by one walker, with its codec and TypeScript and Python bindings generated. Estimated cost: 4 to 6 engineer-weeks; it deletes roughly 4-5K lines; a plan change drops from about 30 files to about 5.

## Strengths
- **A closed, immutable plan algebra.** The plan is sealed records, its invariants checked in constructors, read by exhaustive switches.
  - `core/src/main/java/com/legend/executionplan/ExecutionPlan.java:186` (sealed `Node`, 8 variants).
  - `:961-985` (`Target` validates the order and identity of its loads).
  - `:727-748` (`Sql` validates that payload digests match its content).
  - `core/src/main/java/com/legend/exec/PlanRunner.java:101-142` and `exec/ProductRun.java:128-137` switch over nodes with no `default`.
  - This is good Java 21 practice and the right base for an IR.
- **The wall is in the build, where it is cheapest and strongest.** `core/BUILD.bazel:210-219` gives `//core:exec` no compiler, lowering, dialect or planner dependency. `:270-285` keeps `//core:planner` without exec. `:394-401` makes `plan_side` the WebAssembly and native planners' closure. `:237-241` keeps `testexec` without testplan. These four rules alone prove most of G4's structural half.
- **A clean result boundary.** `core/src/main/java/com/legend/result/Output.java:20-42` is a sealed choice of format. `ResultSink` streams columns, then batches, then the end. `exec/ProductRun.java:44-48` makes the writer from the plan before any statement runs, so a refused format never leaves writes behind. That ordering is thoughtful.
- **Per-database variation is contained.** A grep for `DatabaseType.<X>` dispatch in exec, planner, testplan, testexec and lowering, outside the connectors, returns 0. A database is a dialect (`sql/dialect/*`) plus a connector (`exec/*Connector.java`, `exec/Connectors.java`).
- **The doors are genuinely thin and discoverable.**
  - `core/src/main/java/com/legend/Execution.java:39-108`.
  - `core/src/main/java/com/legend/TestExecution.java:42-88`.
  - Each method is one plan call and one run call, so a new engineer finds both halves of a road in one place.
- **Almost no copy-paste.** jscpd found 3 clones (50 lines, 0.16%) across 147 files of exec, testexec, testplan, planner and the two plan packages. The duplication that exists is structural (below), not textual.
- **Refusal by name is consistent (G7).** Unsupported shapes fail with a message naming the shape, never a silent default. Examples: `exec/ProductRun.java:109-112` and the arms of `exec/PlanRunner.java:135-142`.
- **One guard that is proportionate and strong.** `core/src/test/java/com/legend/PlanTypesAreDataTest.java` walks the plan's type graph reflectively and proves it is records of leaves. That is the right shape of guard for G11.

## Findings

### ARCH-1 [S1] The plan schema is hand-maintained in three languages and five codecs, versioned by exact match
- Where:
  - `core/src/main/java/com/legend/executionplan/PlanJson.java:22-151`: 1,423 lines; `VERSION = 51` at :151 under about 130 lines of per-version changelog javadoc.
  - `executionplan/MirJson.java` (1,146 lines).
  - `testexecutionplan/TestPlanJson.java:25-46`: version 21, with its own changelog.
  - `testexecutionplan/HostJson.java` (1,295 lines) and `testexecutionplan/JudgeJson.java` (351 lines).
  - `engine-client/src/plan-runner.ts:62-131`: about 70 lines of version changelog, then `const PLAN_VERSION = 51`.
  - `engine-client/src/plan-runner.ts:136-143`: hand-written loose step types ending in `| { readonly _type: string }`, plus `as` casts at :199-200 and :544.
  - `python/legend_lite/_runs.py:26`: `PLAN_VERSION = 51` and `dict.get('_type')` dispatch.
  - `core/src/test/java/com/legend/PlanVersionsAgreeTest.java:33`: a regex over the TypeScript source keeps the two constants equal.
  - `executionplan/ExecutionPlan.java:725-727` and `PlanJson.java:672`: every `Sql` carries `SqlQuery tree`, the typed SQL tree, serialised through `MirJson`.
- What:
  - **One schema, many hand copies.** The plan's shape is written once as Java records, then again by hand as each of 5 symmetric writer/reader codecs (about 4.6K lines). It is written yet again as loose TypeScript interfaces and as Python dict access.
  - **Exact-version handshake.** Hosts refuse any version but their own. So a planner change that no host reads still bumps all three constants, for example `5ee415fcf` ("48 -> 49 ... no host database's"), which touched 6 files.
  - **Change cost.** There were 25 bumps of `PlanJson.VERSION` in the series window, and the median such commit touched about 33 files (`fb15b1672`: one field on a database record, 23 files, 16 of them tests).
  - **Dead payload.** No runner reads `Sql.tree`. Its only reader is a test (`core/src/test/java/com/legend/testcases/PlanRoundTrip.java:99-120`). Yet it is in every plan's JSON, and it is why `//core:execution_plan`, and so exec, depends on the 6.6K-line MIR target `:sql` (`core/BUILD.bazel:80`).
- Why it matters:
  - The plan is the central contract between the planner, exec, three hosts and the test road, and every feature changes it.
  - Hand-kept copies drift. The project's answer is more pins (a regex over TypeScript, version constants), not a single source.
  - The TypeScript catch-all variant defeats exhaustiveness, so a new node kind fails only at run time.
  - The version changelog lives in javadoc, which is git's job.
- Expert alternative:
  - **One definition.** Treat the Java records as the single schema.
  - **Generate the codec.** Use an annotation processor over the sealed records, which stays TeaVM-safe because there is no runtime reflection. `PlanTypesAreDataTest` already proves the type graph is records of leaves, which is exactly the generator's precondition.
  - **Generate the host types.** Emit TypeScript discriminated unions (no catch-all) and Python `TypedDict`/dataclasses from the same model. Generate the version and the tag list too.
  - **Tag-level compatibility.** Replace the exact version match with a host refusing a node or binding tag it does not know, where it meets one.
  - **Take the MIR tree out of the portable plan.** Keep it beside the plan for diagnostics if wanted, so exec stops depending on `:sql`.
  - **Expected result.** This deletes about 3.5K net lines of codec and the changelog comments, and a plan change becomes record, generator run, the runner that reads it, and tests.
- Confidence: verified (codecs, constants, the regex pin, `tree` having no runtime reader, the commit file counts).

### ARCH-2 [S1] Four representations of "a program of steps" and two Java walkers that re-derive the same rules
- Where:
  - `core/src/main/java/com/legend/executionplan/ExecutionPlan.java:226` (`Sequence(List<Node>)`).
  - `planner/PlannedStep.java:18` (`Product | FrameRun | Extension(handle, declared)`).
  - `testexecutionplan/TestPlan.java:74` (`Run | FrameRun | Verdicts | Branch`).
  - `exec/ProgramRunner.java:35-75` (`Program`, and `Step = Node(node, origin) | Callback(id, declared, origin)`).
  - `testexec/TestRun.java:77-91` translates a `TestPlan` into a `ProgramRunner.Program` at run.
  - `exec/PlanRunner.java:378-474` (`sequence`) and `:499-516` (`steps`), against `exec/ProgramRunner.java:355-436` (`shapeOf`), `:459-485` (`bracketed`) and `:487-516` (`runSteps`).
  - `testexecutionplan/TestPlan.java:98-120` and `executionplan/ExecutionPlan.java:47-112` re-derive the answer: `answers()`, `answer()` and `answerShape()`. `TestPlan.answerShape` builds a throwaway `ExecutionPlan` to reuse one method.
- What:
  - **The same rules, written twice.** Both walkers compute the program's one target (deep `Target.equals`), its result count, whether it changes the database, the strictest keyed snapshot, and the clash between a count's name and a parameter's. Both then choose the same snapshot-or-transaction bracket, with the same `UncheckedIOException` tunnel (`PlanRunner.java:455-474`, `ProgramRunner.java:459-485`).
  - **The copies already disagree.** The product walker requires exactly one result (`PlanRunner.java:449`); the program walker allows at most one (`ProgramRunner.java:429`). Only the program walker routes the system database and a "user beside" session.
  - **Why the split exists.** `ProgramRunner` is a separate Bazel target, so that "a product caller reaches none of it" (`core/BUILD.bazel:221-230`). That separation of build targets is the cause of the duplication. A callback interface is not test machinery.
- Why it matters:
  - Every rule about a program (targets, transactions, snapshots, loads) has to be changed in two places. It has already diverged.
  - A reader has to learn four isomorphic sum types and the translations between them.
- Expert alternative:
  - **One step type.** Define it in `executionplan`: `record Program(List<Parameter> parameters, List<Step> steps)` and `sealed interface Step permits Node, FrameRun, Extension`, with `record Extension(int id, List<Read> declared)`.
  - **The product.** A product plan is a `Program` with no extensions.
  - **The test plan.** A test plan is `record TestPlan(Program program, Map<Integer, TestStep> extensions, ...)`, where `TestStep = Verdicts | Branch`.
  - **One walker.** Exec walks it, with `Callbacks` as an optional, ordinary interface on that walker. Delete `PlanRunner.sequence/steps`, `PlannedStep`, `ProgramRunner.Program/Step` and `TestRun.program`.
  - **One answer rule.** The answer rule lives once, on `Program`.
- Confidence: verified.

### ARCH-3 [S1] The guard layer is about the size of the code it guards, and it has shaped the product's API
- Where:
  - **The size of the guard layer:**
    - The GUARDS unit: 103 files, 26,652 lines (85 test-ish files, 23.9K lines), against about 25.4K lines in planner, exec, executionplan and result.
    - `core/src/test/java/com/legend/ArchitectureTest.java`: 4,407 lines, 122 tests, and `PLANNER_ENTRIES` at :1129 (76 entries mapping a full method signature to its allowed caller classes) used by the rule at :1303.
    - 38 `Planted*.java` violator classes (1,700 lines) that exist to prove the guards bite.
    - `JavaEvalLedgerTest.java:1947-1990` parses door sources with regexes and brace counting to check "one statement a method".
    - `tools/guards/PlannedSqlTest.java` (1,652 lines of Java) scans TypeScript sources.
    - Host-side guard tests: `python/tests/test_planned_guard.py`, `python/tests/test_run_closure.py`, `tools/deps/layer-alone.test.ts`, `tools/deps/app-runs.test.ts` and others, about 2.6K lines.
  - **Product code that exists for the guards:**
    - `base/src/main/java/com/legend/base/RunScope.java:35-141`: four `ThreadLocal`s, two of them `int[]` holders. RunScope is referenced 112 times in 38 main files, including 70 `RunScope.planning/plans` checks spread through planning code.
    - `exec/PlanRunner.java:92-97`: the execution id is passed as a parameter, stored in a `ThreadLocal` by `RunScope.enter(execution)`, and read back by `RunScope.execution()` inside the run (also at :826). The parameter exists because `execMakesNoValueOfItsOwn` forbids exec from minting one.
    - `executionplan/PlannedSql.java:34`: a public `fromPlan(String)` that anyone can call, while the plan stores plain `String`s (`ExecutionPlan.Sql.statement`, `Target.session`, `TestPlan.Verdicts.Savepoint`). Its provenance holds only by an ArchUnit list of who may mint one.
    - `testplan/TestPlanner.java:167`: `TestPlanJson.requireRoundTrip(plan)` runs on every planned test body in production code. The whole test-plan codec (about 2,050 lines: TestPlanJson, HostJson, JudgeJson) has no production reader. Its other callers are a diagnostics switch (`:172`, `:176`) and `ServiceTestRunner.planJson`, which only tests call.
- What: the separation is proven three times over: by the build graph, by ArchUnit (including signature-level allowlists that re-implement Java's access control), and by a run-time wall. Each layer is mirrored in TypeScript and Python. The guards also changed the code they guard: ids threaded twice, a wrapper type without a type-level guarantee, a round trip on every test, and a split of build targets that caused ARCH-2.
- Why it matters:
  - Every legitimate refactor (a new entry point, a moved method, a new connector) means editing allowlists. AGENTS.md requires a dated justification for each edit.
  - Text-scanning guards break on formatting.
  - New engineers have to learn the guards to make any change.
  - A run-time wall at 70 planning sites protects against callbacks that could plan, a risk that a handful of callback seams create.
- Expert alternative:
  - **Keep the build graph.** It already proves G4's structure.
  - **Use the language.** Replace `PLANNER_ENTRIES` with package-private visibility and one public facade per package.
  - **Make `PlannedSql` real.** Store it in the plan's records, so a statement is typed from the dialect to the session. Make its factory package-private to `executionplan`, called by the records and the codec.
  - **Shrink the run-time wall.** Keep `RunScope` only at the callback seams the caller supplies (`Output.Rows`, `AssertListener`, `Sessions.Opener`). Pass the execution id explicitly or not at all.
  - **Move the round-trip proof.** Make it a test-scope property test over the lanes' plans.
  - **Delete the source-text guards** (regex over Java and over TypeScript). Use ESLint `no-restricted-imports` or package boundaries for TypeScript.
  - **Expected result.** About 10-15K lines of guards and planted classes could go.
- Confidence: verified (counts, the call sites, the codec having no production reader). The size estimate for deletion is suspected.

### ARCH-4 [S1] The plan's type model admits states the runners refuse at run time
- Where:
  - `executionplan/ExecutionPlan.java:26` and `:186`: the root is any `Node`. `exec/PlanRunner.java:135-142` throws for `Update`, `Check`, `Effect` and `Load` roots.
  - `ExecutionPlan.Sequence` can nest. The nesting is refused in 11 places ("a sequence inside a sequence", "inside a program").
  - The rule "a plan answers through one result step" is checked by scanning: `ExecutionPlan.java:93-112`, `PlanRunner.java:449`, `ProgramRunner.java:429`, `TestPlan.java:103-111`.
  - `testexecutionplan/TestPlan.java:444`: `Branch(Side condition, List<TestStep> then, List<TestStep> otherwise)`. Only `Verdicts` and `Branch` are valid there (`testexec/TestRun.java:129-134` throws for the others).
  - `ProgramRunner.Callback.id` is an index into `plan.steps()`, and its kind is checked at run (`TestRun.java:112-115`).
  - `TestPlan.java:313`: `Verdict(int id, String name, String arm, List<Side> base, @Nullable Node wrapper, @Nullable Side message, boolean raises, @Nullable Host host)` and `TestPlan.java:185`: `Verdicts(..., @Nullable TdsResult fused, @Nullable Savepoint, ..., @Nullable RootLoop roots, @Nullable Agreement agreement)`. Which nullables go with which of the three judges is written in prose.
  - Two enums name the same concept: `testplan/PlanningOptions.java:56` (`JudgeMode { HOST, DATABASE, BOTH }`) and `TestPlan.java:71` (`Judge { HOST, DATABASE, AGREEMENT }`).
  - `testplan/AgreementAsserts.java:45-56` splices a host verdict's name, arm, base and message into the database's first verdict by hand, because there is no sum type to compose.
- What: exec has 101 `IllegalStateException` throws and testexec 62. A large share reject structures the types could have excluded.
- Why it matters: every reader of a plan (three hosts, two walkers, two codecs) has to re-implement the same refusals. A planner bug surfaces as a run-time error far from its cause. The many nullable components produce telescoping constructors (ARCH-10) and agreement logic built by splicing records.
- Expert alternative:
  - **Plan roots:** `sealed interface Root permits Query(Result), Program(List<Step>), Constant`, where `Result = TdsResult | Keyed`. `Step` excludes `Program`, and a writing program is its own variant rather than `boolean writes` plus scanning.
  - **Branches:** `Branch(Side condition, List<JudgedStep> then, List<JudgedStep> otherwise)`, with `JudgedStep = Verdicts | Branch`.
  - **Verdicts:** `sealed interface Judged permits Host(HostFacts), Database(Node wrapper, @Nullable TdsResult fused), Agreement(Host host, Database database)` on each verdict. The plan-level judge enum, and one of the two enums, then disappear.
- Confidence: verified.

### ARCH-5 [S1] Session routing is spread across two source hierarchies and four routers over denormalised targets
- Where:
  - `exec/SessionSource.java:25`: 5 variants (`Given`, `None`, `ProjectDatabases`, `Fresh`, `Declared`).
  - `exec/PlanSessions.java:27`: 8 variants, including the decorators `Marked` and `Admitting`, with `Acquired` holding `boolean[] state = {false, false}`.
  - `exec/ProductRun.java:36` (`Establish`) and `:73-120`: the decision between system, write and given sessions.
  - `exec/ProgramRunner.java:575-640` (`Routes`, `UserBeside`).
  - `testexec/TestRunner.java:130-134` (`sessionsByTarget`).
  - `exec/PlanSessions.java:191-205` (`Given.open`).
  - The split between the system database and the user's database is re-decided at `ProductRun.java:90`, `PlanSessions.java:196`, `ProgramRunner.java:582` and `TestRunner.java:133`.
  - Deep `Target.equals` comparisons: `ProductRun.home`, `PlanRunner.java:401`, `:444`, `ProgramRunner.sameTarget:450`, `UserBeside.on:628`.
  - `PlanJson.hoistLoads:371` de-duplicates loads that the in-memory form repeats on every statement's target.
  - The product door exposes these knobs publicly. `Execution.java:39-108` has six `run` overloads taking `SessionSource`, `PlanSessions.Source` and `Establish`. The only production caller uses `runOnProjectDatabases` (`server/ConnectionResolver.java:41`); the other overloads serve tests and PCT.
- What:
  - **The question every runner answers.** "Which session does this target's statement go to?" is the one run-time binding decision exec owns, and it is answered in four places with slightly different rules.
  - **Targets are values, not identities.** They are embedded in every `Sql` and compared structurally.
- Why it matters: adding a database role (a second store, a catalog, a read replica) means editing every router. The test and server configuration surface leaks into the product's public API.
- Expert alternative:
  - **Normalise the plan.** `ExecutionPlan(parameters, Map<TargetId, Target> targets, Map<LoadId, Load> loads, Root root)`, with `Sql.target` a `TargetId`.
  - **One binder.** A single exec `SessionBinder`, built once per run from a `RunConfig` (`Map<TargetId, SessionPolicy>`, plus admission and marks), which every walker asks.
  - **A narrow public door.** `Execution.run(TypedQuery, runtime, Output)` plus one `run(..., RunConfig)` for test harnesses.
- Confidence: verified (the four routers, the API's callers). The binder design is a proposal.

### ARCH-6 [S1] Three host runners and three walls re-implement plan semantics; the host boundary is an RPC spelled in five places
- Where:
  - **The runners:**
    - Java: `core/src/main/java/com/legend/exec/PlanRunner.java` (1,209 lines).
    - TypeScript: `engine-client/src/plan-runner.ts` (810 lines).
    - Python: `python/legend_lite/_runs.py` (335 lines).
    - Each runs a different subset and refuses the rest by name: `plan-runner.ts:1-12`, `_runs.py:51-60`.
  - **The walls:**
    - `RunScope`, mirrored by `engine-client/src/run-frame.ts` and `plan-guard.ts`, and by `python/legend_lite/_runframe.py` (23 Python and 31 TypeScript call sites).
    - `PlannedSql`, mirrored by `engine-client/src/planned-sql.ts` and by `python/legend_lite/_planned.py:30` (a `str` subclass).
  - **The protocol:**
    - Request kinds are spelled in `engine-client/src/legend/planner-protocol.ts`, in `planner-answer.ts` (23 cases) and in `python/legend_lite/_planner.py:17-40` (`KINDS`, 22 entries).
    - They are answered by `wasm/src/main/java/planner/Boundary.java` (38 statics), re-exported by `TabExports.java` (72 exports) and by `native/src/main/java/com/legend/nativelib/Compiler.java` (34 entry points).
    - Answer shapes vary by kind:
      - SQL alone (`planSql`).
      - `{sql, type, plan?}` (`Boundary.planned`, :175).
      - A PlanJson plan.
      - The engine-JSON plan half (`PureV1Api.jsonPlan`).
      - Bare statement lists (`sessionSetup`, `hostOperations`, `catalogColumnsSql`).
- What: G11's "one portable plan run by any runner" is realised as several answer shapes and several partial runners. Each runner is kept in step by guard tests and conformance fixtures rather than by sharing code.
- Why it matters: a new node, binding or load kind is a change in three languages plus their guards. A new protocol kind touches about 6 files (`e376eec90` added `outputPlan` across 37 files).
- Expert alternative:
  - **Constraint.** Hosts run SQL on their own DuckDB (WebAssembly or Python), and the planner is already compiled to JS (TeaVM) and to a native library.
  - **Option (a): one runner core.** Compile a host-neutral runner core (Java, over a 2-method `HostSession { rows(PlannedSql, params); }`, with the plan's transaction statements) into the same TeaVM and native artefacts, so a host implements only the statement call. The wall then exists once.
  - **Option (b): a host profile.** Define an explicit host profile of the plan (statements, binds, one result, the transaction), validated by the planner, with a runner of about 100 lines per host.
  - **Either way: one protocol table.** Generate the protocol table (kind, export, arguments) from one source.
- Confidence: verified for the duplication. Option (a)'s feasibility under TeaVM is suspected; it needs `PlanRunner` decoupled from JDBC's `Session` class.

### ARCH-7 [S2] The test road is as large as the product road, and the judges multiply every assert by about 14 files
- Where:
  - Size: testplan, testexec and testexecutionplan are 23.5K lines; planner, exec, executionplan and result are 25.4K.
  - A grep for `assertSameElements` hits 14 main files in 6 packages:
    - `builtin/NativeFn`, `builtin/Pure`.
    - `testplan/VerdictArm`, `AssertVerdicts`, `DatabaseJudge`.
    - `lowering/VerdictSql`, `CanonicalRenderSql`.
    - `testexecutionplan/TestPlan`.
    - `testexec/HostJudgments`, `PureAsserts`, `Equality`, `CanonicalDivergence`.
    - `compiler/spec/GeneratorSplice`, `testdatagen/TestDataPlanner`.
  - Hotspots (lizard):
    - `testplan/DatabaseJudge::databaseVerdict`: CCN 92, 13 parameters.
    - `testplan/BodyClass::frameProgram`: CCN 76.
    - `testplan/AssertVerdicts::adjudicate`: CCN 65, 313 lines.
    - `testplan/DatabaseAsserts::plan`: CCN 39, 208 lines.
  - Each judge has its own planner and runner: `HostAsserts`/`HostJudgments`, `DatabaseAsserts`/`DatabaseVerdicts`/`RootLoops`, `AgreementAsserts`/`AgreementRuns`/`AgreementArm`, plus `Appeals`/`SqlTextRuns` for the referee.
- What: G2 makes two comparison implementations per assert essential. The accidental parts are the third judge, which has a planner and runner of its own rather than being a composition, and the absence of an assert-arm abstraction. As a result an assert's host planning, database SQL, run-time comparison and census are scattered across packages by layer rather than grouped by assert.
- Why it matters: adding an assert means learning about 15 concepts: body class, arm, host judgment, side shape, judging rules, wrapper, fused group, root loop, agreement pairing, appeal, census and others.
- Expert alternative:
  - **One arm per assert:** `interface AssertArm { HostJudgment planHost(...); @Nullable Node planDatabase(...); }`, one implementation per assert family, registered in one table.
  - **Agreement as composition:** the agreement judge is `Agreement(Host, Database)`, built in the planner and in the runner, not a third pipeline (pairs with ARCH-4).
- Confidence: verified (counts, hotspots). The redesign is a proposal.

### ARCH-8 [S2] Code comments and the doc layer read as a process log
- Where:
  - About 2,100 process-history references in exec, executionplan, planner, testplan, testexec, testexecutionplan and result (6.1 per 100 lines in exec): 149 "until then", 111 "the user's ruling/go/question", 300 "audit", plus dated slice, hole and step labels.
  - Examples: `exec/ProductRun.java:14-26` ("the plan-as-data design's slice 5, 2026-11-09: the door's runPlan until then"), `executionplan/PlanJson.java:22-150`, `engine-client/src/plan-runner.ts:62-131`.
  - The docs: `docs/` holds 455 files (336K lines), and the series added 29K lines. Main code cites 107 distinct dated design docs.
  - `AGENTS.md` (536 lines) lists 5 "start here" documents, and its entry-point table rows are paragraphs.
  - Stale references: `core/BUILD.bazel:203` cites `SetupRunner`, which was deleted. `docs/GOALS_OF_RECORD_2026_11_07.md` §G1 allows "`BodyRouter`", which does not exist in main code.
- What: javadoc explains how the code got here, who ruled what and which audit found what, instead of what it does now. A reader has to filter about one line in six.
- Why it matters: a capable new engineer cannot be productive in a week. Finding the current design among superseded "programs" is itself a task, and the narratives go stale as soon as the next slice lands.
- Expert alternative:
  - Javadoc states the contract and the reason in the present tense.
  - History goes in commit messages and an ADR log; the plan format is documented by its (generated) schema.
  - One living architecture page (`core/README.md`) replaces the chain of start-here documents.
  - This is a mechanical sweep, about 1-2 weeks with review.
- Confidence: verified (counts by grep; see Measurements).

### ARCH-9 [S2] Measurement metadata is threaded through exec's core API
- Where:
  - `exec/StatementOrigin.java:7-35`: 15 values, documented as "measurement only, ... read by no verdict".
  - Threaded through:
    - `ProgramRunner.Node(node, origin)` (`ProgramRunner.java:50`), `Callback(id, declared, origin)` (:64), `OpenProgram.read(read, origin, bindings)`.
    - `PlanSessions.Source.mark()` and the `Marked` decorator, `Statements.marked`.
    - `TestRunner.run(..., origin, ...)`, `TestExecution.runFixture` (`StatementOrigin.SEED`), `TestRun.program` (`:82-87`).
  - 39 references in main code.
- What: a census counter has become a parameter of the runtime's step model and session API.
- Why it matters: every runner signature carries it, and the decorator layer in `PlanSessions` exists partly for it.
- Expert alternative: the planner knows why each statement exists. Put an enum `purpose` on `ExecutionPlan.Sql` (or on the step) and count it in one place (`JdbcStatements` already counts). Alternatively, attach a statement listener once, where a session is made.
- Confidence: verified.

### ARCH-10 [S2] Positional many-field records with telescoping constructors make every plan field addition a sweep
- Where:
  - `executionplan/ExecutionPlan.java:725-800`: `Sql` has 7 components and 4 constructors, plus `stamped` and `unstamped`.
  - `testexecutionplan/TestPlan.java:313-376`: `Verdict` has 8 components and 4 constructors, plus `withHost`.
  - `TestPlan.java:185-228`: `Verdicts` has 6 components and 4 constructors, plus `withRoots` and `withAgreement`.
  - `ExecutionPlan.java:956`: `Target` has 6 components.
  - Commit `fb15b1672` touched 23 files to add one component; 16 of them were tests constructing records positionally.
- What: there are no fixture factories, and the nullable components (ARCH-4) are what produce the telescoping constructors.
- Why it matters: it multiplies ARCH-1's change cost and clutters every call site with `null, null, List.of()`.
- Expert alternative:
  - Static factories named for the valid shapes, such as `Sql.query(text, slots, target)` and `Verdict.host(...)`.
  - A test-fixtures class (for example `Plans.duckDbTarget()`).
  - Fewer nullable components once ARCH-4's sum types land.
- Confidence: verified.

### ARCH-11 [S3] Inline fully-qualified names instead of imports (pattern)
- Where: about 4,300 in code lines (comments excluded) across the 7 plan/exec packages: testplan 1,842, planner 1,016, testexec 768, exec 372. `com.legend.base.@Nullable` alone appears 5,004 times in core main. Representative examples:
  - `Execution.java:43` (`@com.legend.base.Nullable String runtimeFqn`).
  - `TypedQuery.java` (`com.legend.executionplan.ExecutionPlan` throughout).
  - `testexec/TestRunner.java:58-62`.
- What, and why it matters: signatures roughly double in length, and it hides each class's real dependency list from a reader.
- Expert alternative: import the types. One sweep with an IDE organize-imports pass.
- Confidence: verified.

## Cross-unit observations
- **TESTPLAN:**
  - `testplan/DatabaseJudge.java::databaseVerdict` (CCN 92, 13 parameters), `BodyClass::frameProgram` (CCN 76), `AssertVerdicts::adjudicate` (313 lines).
  - `TestPlanner.plan` (`testplan/TestPlanner.java:71-178`) mixes the body-class dispatch, the judge choice, system routing, appeals, a JSON round trip and two diagnostics modes in one method.
- **EXEC:**
  - `PlanSessions.Acquired`'s `boolean[] state = {false, false}` (`exec/PlanSessions.java:246-252`).
  - `PlanRunner.streamRun` uses a one-element `Streamed[]` holder (`exec/PlanRunner.java:104-109`).
  - The `UncheckedIOException` tunnel is written twice (ARCH-2).
- **TESTEXEC:** `Appeals.Decider` has 7 parameters (`testexec/Appeals.java:233-237`).
- **TESTEXEC/TESTPLAN:** the `Edge` interface mixes planning methods (`effect`, `seed`, called at `testplan/TestDoor.java:140-144`) with run-time notifications (`seeded`, `effectRan`, `testexec/TestRunner.java:467-493`). The same stateful instance crosses the plan/exec boundary through `ExecuteOptions`.
- **GUARDS:**
  - `JavaEvalLedgerTest.theDoorsHoldNoLogic` checks Java source by regex and brace counting (`:1947-1990`).
  - `PlannedSqlTest` is a TypeScript import lint written as a 1,652-line Java test.
  - The guards register databases per class: `JdbcSurfaceCensusTest.REGISTER`, `DatabaseOwners.java:80` and the connector list at `JavaEvalLedgerTest.java:1255`. These add friction to adding a database, a change that is otherwise well contained.
- **TYPESCRIPT:** the `| { readonly _type: string }` catch-all in `engine-client/src/plan-runner.ts:143` and `:161` defeats exhaustiveness. The plan-runner header is a version changelog.
- **PYTHON:** `PlannedSql(str)` (`python/legend_lite/_planned.py:30`) is a convention, not a guarantee: anyone can call `PlannedSql(x)` or `from_plan(x)`, and the guarantee rests on a test that pins callers.
- **PLANMODEL:** `testexecutionplan/Column.java` duplicates `ExecutionPlan.Column` with weaker types. There are two `Savepoint` records: `TestPlan.Verdicts.Savepoint` holds `String`s and `ProgramRunner.Savepoint` holds `PlannedSql`s.

## Measurements
- **Series size.** +117,745 / -32,938 lines over the 7 merges' first-parent diffs:
  - core main +33,327 / -19,935.
  - core test +29,321 / -4,274.
  - docs +29,339.
  - Python +8,138 (source and tests); TypeScript +5,803 (source and tests).
  - Command: `for m in 770acc55 c4a39e06 981a24b8 cf865632 f4baf1d1 0c9032f5 4dfbb5b7; do git diff --numstat $m^1 $m; done`, then summed by area with awk.
- **Package sizes** (`find <pkg> -name '*.java' | xargs cat | wc -l`):

  | Road | Package | Lines |
  |---|---|---|
  | Product | exec | 7,487 |
  | Product | executionplan | 5,146 |
  | Product | planner | 9,844 |
  | Product | result | 3,032 |
  | Product | **Total** | **25.4K** |
  | Test | testplan | 13,345 |
  | Test | testexec | 6,092 |
  | Test | testexecutionplan | 4,143 |
  | Test | **Total** | **23.5K** |

- **Guards.** The GUARDS unit is 103 files and 26,652 lines now (85 test-ish files, 23,905 lines). There are 38 `Planted*.java` classes (1,700 lines). `ArchitectureTest` has 122 tests and `PLANNER_ENTRIES` 76 entries (`grep -c`, `find -name 'Planted*.java' | xargs wc -l`).
- **The run-time wall.** RunScope is referenced 112 times in 38 main files; 70 of those are `planning` or `plans` checks (`grep -rn "RunScope\." core/src/main`). `PlannedSql` is referenced 100 times; it is minted 16 times.
- **Plan codecs (Java).**

  | File | Lines | Note |
  |---|---|---|
  | PlanJson | 1,423 | |
  | HostJson | 1,295 | |
  | MirJson | 1,146 | the tree it serialises has no runtime reader |
  | TestPlanJson | 405 | |
  | JudgeJson | 351 | |
  | **Total** | **4,620** | |

  - Plan format `VERSION` = 51; the test-plan format is at 21.
  - 25 bumps of `PlanJson.VERSION` within the series window (`git log -G "public static final int VERSION = " 770acc55^1..4dfbb5b7 -- .../PlanJson.java`). The median bump commit touched about 33 files (`git show --stat`).
- **Step representations: 4.** `ExecutionPlan.Sequence`, `planner.PlannedStep`, `TestPlan.TestStep`, `ProgramRunner.Step`.
  - Java walkers of a step list: 2 (`PlanRunner.sequence`, `ProgramRunner.programRun`).
  - Host runners: 2 more (TypeScript, Python).
- **Session-routing types.**
  - `SessionSource`: 5 variants.
  - `PlanSessions.Source`: 8 variants.
  - `Establish`: 2.
  - The system-versus-user split is decided at 4 sites.
- **Host boundary.**

  | Artefact | Count |
  |---|---|
  | `Boundary.java` public statics | 38 |
  | `TabExports` exports | 72 |
  | Native `Compiler` entry points | 34 |
  | Python `KINDS` | 22 |
  | TypeScript `planner-answer` cases | 23 |

- **Text duplication.** jscpd `--min-lines 12 --min-tokens 90` over exec, testexec, testplan, planner, executionplan and testexecutionplan found 3 clones and 50 lines (0.16%).
- **Complexity** (`python3 -m lizard -l java` over the 7 packages): 2,451 functions, average CCN 4.0, 93 warnings, 21 functions with 8 or more parameters. Hotspots are listed in ARCH-7.
- **Comment content** (a script counting comment lines, dated lines and process words per package):
  - The comment-to-code ratio is 0.23-0.39.
  - Process-history references per 100 lines: exec 6.1, executionplan 5.1, testplan 4.6, testexecutionplan 4.1, testexec 3.7, planner 3.1, result 2.5. Total about 2,100.
- **Inline fully-qualified names in code lines:** about 4,300 across the 7 packages (`grep -v` comment and import lines, then `grep -oE "(com\.legend|java\.util|java\.io)\.[a-z.]*[A-Z]"`).
- **Concepts to learn for an ordinary change.**
  - **A new assert:** about 14 main files in 6 packages. Concepts: native signature, body class, verdict arm, host judgment record and its codec arm, side shape, judging rules, database wrapper SQL, fused group, root loop, agreement pairing, appeal (engine-text), census.
  - **A new database:** dialect, connector, `DatabaseType`, the `Databases` registry, the driver jar, and three guard registrations. The core part (dialect plus connector) is well contained.
  - **A new plan field:** the record, PlanJson writer and reader, possibly the TestPlan codecs, the TypeScript and Python runners if they read it, three version constants and their tests, and every positional test constructor (about 20-30 files).
- **Docs.** `docs/` holds 455 files and 336,067 lines. Main code cites 107 distinct `docs/*.md` files. `AGENTS.md` (536 lines) lists 5 "start here" documents.
