# TESTPLAN: verdict WEAK

## Summary
The unit's output model is sound. A test plan is typed, sealed data, judgments are records rather than closures, verdict and DDL statements are built as MIR through `lowering.VerdictSql` and `SqlDdl`, and planning is fenced off from sending (`RunScope.plans`). The planner that produces that data is an accretion, however.
- **Q1, BodyClass.** It is not a clear model. Its 4 enum values drive 3 booleans in production (`TestPlanner.java:95-137`). The remaining ~1,200 lines are interleaved predicates that re-simulate planner state, each rule added for a named corpus test and date.
- **Q2, the three judges.** The placement seam (`TestDoor.Asserts`) and the agreement composition are clean. Underneath, the host and database judges each carry their own body walk (4 copies of the let ladder) and their own classifier: two family switches, plus a gate that runs the host classifier. `VerdictArm` has one implementation.
- **Q3, the large classes.** `AssertVerdicts`, `SqlTextVerdicts`, `DatabaseJudge` and `BodyDriver` are god classes: CCN 92/76/65, methods of 200-313 lines, signatures with 13 parameters and 5 boolean flags. `VerdictBatch` is cohesive, but it is a stateful open/defer/orElse/close protocol.
- **The database and engine-text planners** are the deleted evaluator re-run under mutable recording collectors. Arms return a vestigial `TypedSide`, text sides are sentinel `String`s compared by identity, and planning failures (NPEs included) become planned test outcomes.
- **Q4, SQL and the layer rule.** The rule is largely respected. The exceptions are `"__canon"+i` and `"frame_"+name` naming contracts, regex parsing of engine plan text, and rendering a statement to find out whether the dialect refuses it.
- **Q5, vocabulary.** It needs a glossary. arm, side, wall, road, door, lane and rider are each overloaded. "The walk", a deleted subsystem, is the reference for behaviour 283 times. There are also ~1,600 inline FQNs.

## Strengths
- **The plan's output is typed, sealed data.**
  - `HostJudgment` is a sealed interface (`testexecutionplan/HostJudgment.java:25`), as are `TestPlan.TestStep` and `TestPlan.Side` (`testexecutionplan/TestPlan.java:74,388`).
  - The host judge's planners are small factories that pair the asks with a judgment record (`HostJudge.java:56-336`, e.g. `planSize` at :79-83). A judgment is data, never a closure.
  - Every plan is checked to round-trip through its JSON (`TestPlanner.java:167`).
- **A narrow placement seam with a sensible composition.** `TestDoor.Asserts` has one required method (`TestDoor.java:59-90`). `AgreementAsserts` composes the host plan with the database half by verdict id, instead of writing a third arm (`AgreementAsserts.java:39-89,124-154`).
- **The MIR layer rule is largely kept.**
  - Verdict statements are composed through `lowering.VerdictSql`: 71 calls, 45 distinct functions.
  - DDL and CSV loads are built as `SqlDdl`/`SqlDml` MIR and rendered by the dialect (`EffectPlanner.java:114-153,209-213,251-253`).
  - A grep for SQL keyword literals finds only the golden-detection in `SqlTextVerdicts.planReplaySql`.
- **Planning is fenced from the database at run time.** Planning runs inside `RunScope.plans` (`TestPlanner.java:86,151`).
- **Several small classes are clear and well documented.**
  - `AssertBranch` (104 lines): a `Parts` record and three short rules (`AssertBranch.java:37-103`).
  - `VerdictIds` and `EngineTextAppeals`.
  - `LiteralFold`, whose javadoc explains why the admission rule is what it is (`LiteralFold.java:13-40`).
  - `EffectPlanner`'s data-type table, in place of a switch ladder (`EffectPlanner.java:281-304`).
- **`VerdictBatch.plan` documents its choice.** It decides between a generic row and a ruled row by enumerating its four cases explicitly, and refuses impossible compositions loudly (`VerdictBatch.java:395-449`).
- **Error messages usually name the refused construct**, e.g. `TestKinds.java:397-399` and `HostSides.java:132-133`.
- **Some tests check behaviour, not structure.**
  - `AssertBarTest` probes assert bodies against upstream's exact messages under all three judges, with a shrink-only `GAPS` ratchet (`AssertBarTest.java:26-50`).
  - `VerdictBatchSplitRungTest` unit-tests the split rung with a stubbed `Runner` (`VerdictBatchSplitRungTest.java:33-60`).
- **Textual copy-paste is low:** jscpd finds 0.81%. The duplication is structural (TESTPLAN-1, -3).

## Findings

### TESTPLAN-1 [S1] Two parallel assert classifiers (and a one-implementation strategy interface)
- Where:
  - The database road's switch: `AssertVerdicts.java:369-681` (`adjudicate`, 313 lines, CCN 65).
  - The host road's switches: `AssertVerdicts.java:779-887` (`planHost`) and `:898-993` (`planArm`, CCN 47).
  - The database gate: `AssertVerdicts.java:156-271` (`databaseGate`/`gateOf`, which classifies by calling `planHost` at :197).
  - Quantified forms, planned twice: `:1444-1518` (`planQuantified`) against `:1326-1377` and `:1520-1560` (`unrolled`/`quantified`).
  - The strategy: `VerdictArm.java:21-115` (18 methods), implemented only by `DatabaseJudge.java:770-962`. `AssertVerdicts.java:1572-1581` throws unless the judge mode is DATABASE.
- What:
  - Every family's routing is written twice, as two switches over `NativeFn.Verdict` in the same order. The routing covers the sql-text arm, rendered text, grid pair, order view, arity checks, the static `assertIs` gate and the quantified forms.
  - The two switches differ subtly. `planArm` refuses an `assertInstanceOf` whose type is not a reference (:974); `adjudicate` does not (:640-649). `planArm` sends any two relations to the grid owner (:940-943); `adjudicate` requires `sqlGridPair`, else unjudged (:545-559).
  - The database judge decides whether it can plan an assert by running the host judge's classifier.
  - `VerdictArm`'s javadoc still describes a host arm that was deleted.
- Why it matters:
  - A new assert family, overload or route has to be added in two or three places.
  - Judge parity (G2) depends on two dispatchers kept in step by hand, so a drift shows up as a disagreement on a lane rather than as a compile error.
- Expert alternative:
  - Classify once, into a judge-neutral sealed shape: `sealed interface AssertShape { Equality(TypedSpec e, TypedSpec a, OrderView order, boolean grid); Rendered(RenderForm form, ...); GridPair(...); Size(...); Contains(...); Condition(...); Quantified(...); EngineText(...); StaticallyDecided(HostJudgment.Decided) }`, produced by `AssertShapes.classify(bare, lets, specs)`.
  - Then write `HostPlanner.plan(AssertShape) -> HostSides.Check` and `DatabasePlanner.plan(AssertShape) -> DbVerdict`, each an exhaustive switch over the sealed type. Judge-specific refusals live only in the judge's planner.
  - Delete `VerdictArm`.
- Confidence: verified

### TESTPLAN-2 [S1] The database and engine-text planners are evaluators re-run under mutable recording collectors
- Where:
  - `DatabaseJudge.java:405-422`: `runVerdict`. Its javadoc says "Execute a verdict statement on {@code on} and read its one row". The code defers into the batch and returns `ok()`, or throws.
  - `DatabaseJudge.java:475-484`: `unjudged` is typed `TypedSide` and always throws.
  - `AssertVerdicts.java:67-115`: `planDatabase` runs `batch.open`, then `adjudicate`. A caught `AssertFailed` or `DataError` becomes `resolve`; a caught `RuntimeException` becomes `wall`; a null answer leads to `discard`.
  - `VerdictBatch.java:193-369` is the open/defer/resolve/wall/close/discard protocol. `orElse` pushes onto a deque (:299,:305-321) that `defer` consults through `orElseOf.peek()` (:249-262).
  - `SqlTextVerdicts.java:76-114`: the mutable flags `readingOurs`, `readingGolden` and `originInForce`, with `marking`/`restoreMarking`.
  - `SqlTextVerdicts.java:170-171,232`: sentinel texts `new String("\u0000 our text, read at the verdict step")`, compared by reference at :176, :394 and :402.
  - `SqlTextVerdicts.java:378-432`: `decide` picks one of three roads by which collector or batch the environment happens to carry.
  - `AssertVerdicts.java:1936-1957`: `textFirst` calls `batch.defer(...)` then `batch.orElse(rows::get)` and returns `ok()`. The `TypedSide` that `rows` produces is discarded.
- What:
  - The arms keep the shape of the deleted walk's evaluators. They "evaluate" sides and "return verdicts".
  - The plan is whatever the call pushed, as a side effect, into `env.verdictBatch()` or `env.engineText()`.
  - Exceptions double as planning outcomes.
- Why it matters:
  - An arm's contract can only be learnt by tracing environment state.
  - A route that returns a `TypedSide` without deferring silently plans nothing.
  - A sentinel string can flow into ordinary string logic (`golden.equals(ours)` at `SqlTextVerdicts.java:609`).
  - The deque-plus-peek protocol is hidden temporal coupling.
- Expert alternative:
  - Arms return their plan: `sealed interface DbVerdict { Row(SqlQuery row, Target on, Optional<FailureMessage> m); WithRuled(Row generic, DbVerdict ruled); Decided(Failure f); Unjudged(String why) }`.
  - The segment planner collects the returned values: `batch.add(rootName, List<DbVerdict>)`, with no open/close/orElse protocol.
  - Text sides become a type: `sealed interface Text { Constant(String s); Deferred(HostSides.Ask ask, Origin origin) }`, with `Origin` an enum passed explicitly.
- Constraint: G4 (exec runs only the plan) and verdict parity motivated reusing the walk's arms under a collector. That explains the reuse, but the end state keeps the evaluator's shape wrapped around planner output.
- Confidence: verified

### TESTPLAN-3 [S1] The body walk is re-implemented four times
- Where:
  - `HostAsserts.java:110-254`, with the let ladder at :140-176.
  - `DatabaseAsserts.java:100-307`, with the let ladder at :133-165.
  - `DatabaseAsserts.java:435-540`, the static pass, with the let ladder at :450-481.
  - `DatabaseAsserts.java:569-668` (`planEach`), with the let ladder at :589-625. jscpd confirms this ladder is an exact clone of :148-164.
  - `steps(int)` appears three times: `HostAsserts.java:276`, `DatabaseAsserts.java:413`, `AgreementAsserts.java:192`.
  - The savepoint conversion appears twice: `HostAsserts.java:485-488`, `JudgePlans.java:134-137`.
- What:
  - Each copy has the same FRAME / ALIAS / STAGED_GRID / handle-registration ladder, and the same handling of branches, `assertError` and statement-root asserts.
  - Each copy has its own exception policy: `pendingLet` in the host walk, `return false` in the static pass, `break` in `planEach`.
  - The static pass exists only because the main pass has side effects: census counts and environment accumulators (javadoc at `DatabaseAsserts.java:68-75`).
- Why it matters:
  - A new let kind, or a change to frame handling, must be made in four places with four different failure semantics.
  - The two passes must also agree on how they number segments (`DatabaseAsserts.java:487-493`).
- Expert alternative:
  - Write one `BodyWalk`. It iterates the statements, owns a `LetScope` (`bind(TypedLet let, int index)`, `prefix()`, `frames()`), and calls a judge strategy: `interface AssertPlanner { List<TestStep> planAssert(...); List<TestStep> planAssertError(...); void closeSegment(); }`.
  - Keep planning side-effect free and count from the finished plan. The static pass then disappears.
- Confidence: verified

### TESTPLAN-4 [S1] BodyClass predicts what the planners can do, one accreted rule at a time
- Where:
  - `BodyClass.java:145-173` (`of`).
  - `:431-651` (`frameProgram`: 221 lines, CCN 76, 6 memo maps, 7 mutable sets and flags).
  - `:754-825` (`assertProgram`), `:872-955` (`queryProgram`), `:1182-1247` (`effectProgram`).
  - `TestPlanner.java:95-137`: the only production reads of the enum values.
  - `BodyKinds.java:41-155`: a fifth body walk that restates the same rules for the census.
- What:
  - The classifier re-simulates planner state to predict whether the planners will succeed. The `seeded` set mirrors `TestKinds.established` (`BodyClass.java:461-505`), and frame staleness is tracked at :464-485.
  - Rules are justified by individual corpus tests and dates: "paginate::testPaginated" (:502) and "testConvertAlias" (:783).
  - EFFECT_PROGRAM and QUERY_PROGRAM lead to the same code path. The enum's other job is a test that pins its names (`BodyClassTest.java:23-37`).
  - The original purpose died with the walk: routing between the walk and the program road without try-and-fallback.
  - The `oneJudge=false` branch of `of(body, runtime)` (:163) is reached only from tests. Production always passes `true` (`TestPlanner.java:90`).
- Why it matters:
  - Every planner capability change needs a matching predicate in a 1,316-line file.
  - A mismatch surfaces as a body refused, or planned wrongly.
  - A reader cannot tell which rules still carry weight.
- Expert alternative:
  - Let the planners refuse by name where they meet a shape they do not plan. They already do: `TestPlanner.java:139-147`.
  - Reduce classification to the facts the planner consumes: `record BodyFacts(boolean hasAsserts, Stores stores, boolean systemRouted)`.
  - Keep any genuine pre-checks as a table of named rules, `record BodyRule(String name, Function<TypedQuery, Optional<String>> refuses)`, evaluated in one pass with one memo per predicate. That replaces four interleaved ladders.
- Confidence: verified (the mirroring, and the enum's production use); suspected (how many rules could be deleted)

### TESTPLAN-5 [S2] Planning failures, Java defects included, are caught broadly and replayed as test outcomes
- Where:
  - 18 `catch (RuntimeException ...)` blocks:
    - `HostAsserts.java:171,312,320,360,452,464`.
    - `DatabaseAsserts.java:275,474,504,531,616,643`; :531-534 has an empty body and :616-618 swallows the exception.
    - `AssertVerdicts.java:95,476,851`.
    - `HostSides.java:68`.
    - `SqlTextVerdicts.java:1296`, which returns null; and `:1561`.
  - `testexecutionplan/Failure.java` `Kind` lists `NULL_POINTER`, `CLASS_CAST`, `INDEX_OUT_OF_BOUNDS` and `ILLEGAL_STATE` as plan failure kinds.
- What:
  - A lite bug while an assert is planned (an NPE, a ClassCastException) becomes a "planned raise". It is reported at the assert's turn as that test's failure, after the asserts before it have run.
  - Some sites swallow the exception, or turn it into "no producer, as before".
- Why it matters:
  - Planner defects masquerade as test failures in PCT and corpus rosters.
  - `catch -> return null` is the fallback G7 forbids.
- Constraint: upstream raises at the assert's turn, after the asserts before it ran, so user-level errors must be deferred as data. That holds for Legend's own error hierarchy, not for JDK defects.
- Expert alternative:
  - Catch only the closed user-level hierarchy: `LegendCompileException`, `AssertFailed`, `DataError`, `DialectCapability`, `NotImplementedException`.
  - Let `IllegalStateException`, `NullPointerException` and `ClassCastException` propagate as planner defects.
  - Remove the JDK kinds from `Failure.Kind`.
- Confidence: verified

### TESTPLAN-6 [S2] God classes and flag-laden telescoping signatures
- Where:
  - `DatabaseJudge.java:40-278`: five overloads of `databaseVerdict`. The last has 13 parameters, five of them trailing booleans (`canonicalOrder, cellPool, csvStrings, renderedStrings, gridPairArm`), and CCN 92.
  - Call sites read like `AssertVerdicts.java:1836-1837`: `..., hook, true, false, false, true)`.
  - `planSide` has four overloads (`DatabaseJudge.java:424-469`).
  - `JudgePlans.java:319-325`: `sideShape` takes 11 positional arguments, 6 of them booleans.
  - `SqlTextVerdicts.java:1418-1471`: three overloads of `rowsLegAndVerdict`, up to 18 parameters.
  - `AssertVerdicts` is 2,146 lines with 75 functions.
- What: `AssertVerdicts` mixes six concerns:
  - classification;
  - quantified-form analysis;
  - the rendered-text grammar (:1792-2006);
  - static type identity (:1007-1193);
  - referee replay facts (:1633-1756);
  - let chasing.
- Why it matters: call sites cannot be read without counting arguments. Every new variant adds another overload or another boolean.
- Expert alternative:
  - For the flags: `record EqualityForm(Order order, Pool pool, Strings strings, boolean gridPairArm)`, with `enum Order { DEFINITION, CANONICAL }`, `enum Pool { ROWS, CELLS }` and `enum Strings { RAW, CSV, RENDERED }`.
  - Split `AssertVerdicts` into `AssertShapes`, `QuantifiedAsserts`, `RenderedText`, `StaticIdentity` and `ReplayFacts` (the last belongs at the referee edge).
  - Split `SqlTextVerdicts` into three parts: producer finders, text collection and the rows leg.
- Confidence: verified

### TESTPLAN-7 [S2] ExecEnv looks immutable but carries shared mutable planning state, and some parameters are redundant
- Where:
  - `BodyDriver.java:59-217`: a record of 9 components, 4 of them nullable mode slots: `verdictBatch`, `engineText`, `failureMessage`, `appealOn`.
  - Its `VerdictIds` and `EngineTextAppeals` components are mutable, and every copy shares them.
  - Each `with*` method re-lists all 9 fields (:128-188).
  - `BodyDriver.java:304`: `env.queryLets().putAll(...)` mutates a map read off a record accessor.
  - `BodyDriver.java:395`: `env.plan().stamp(stamp)`.
  - `HostAsserts.java:95-101`: the javadoc says the assert is planned twice and "the discarded pass ... writes the plan environment's run-scoped accumulators".
  - Redundant parameters: a `systemRouted` boolean travels beside `bodyEnv`, although it equals `bodyEnv != null` (`TestPlanner.java:128-133`, `AgreementAsserts.java:108-109`, `DatabaseAsserts.java:83-95`).
- Why it matters:
  - Copying an environment does not isolate it, so a discarded planning pass leaves residue.
  - Modes are encoded as nullable fields: the database judge is `verdictBatch != null`, collecting is `engineText != null`.
- Expert alternative:
  - Split the environment in two. `BodyPlanning` is one explicit, owned, mutable per-body context holding the ids, appeals and accumulators. `SidePlan(PlanEnv plan, Target target, Judging judging)` is immutable.
  - Make the mode a type: `sealed interface Judging { Host; Database(VerdictBatch batch); Collecting(Collector c) }`.
- Confidence: verified

### TESTPLAN-8 [S2] Comments explain present behaviour by a deleted subsystem, and many are stale
- Where:
  - `BodyClass.java:63-64`: "the walk runs every other body until M10 moves the last class".
  - `BodyClass.java:110`: "The database judge's bodies stay on the walk".
  - `BodyClass.java:136-145`: "or null when the walk runs it". Null now means the body is refused.
  - `BodyDriver.java:14-30`, the class doc: "statement sequencing ... the K-native dispatch arms ... it runs already-typed statements"; "walks here every body of a class not yet moved".
  - `VerdictBatch.java:26-45`: "the batch is sent as ONE statement ... This class sends and orders". It plans and sends nothing.
  - `DatabaseJudge.java:405-407`: "Execute a verdict statement".
  - `VerdictArm.java:11-20`: "the host arm ... or the database arm".
  - `EffectPlanner.java:40-44`: "a computed one evaluated by the database". It is now refused.
  - "the walk" is mentioned 283 times across 12 files.
  - 14 dangling in-package `{@link}`s, e.g. `HostJudge.java` links to `#tdsEquivalent`, `#is`, `#allRowsOf`, `#quantified`, `#rowValue` and `#sideCanonOf`, none of which exist; `HostSides` links `#run`; `AssertVerdicts` links `#batched` and `#orderView`.
  - Test names: `BodyClassTest.aBodyWithAnAssertStaysOnTheWalk` (:58), and `BodyRouterParityTest`, which no longer compares two roads.
- Why it matters:
  - A new engineer has to reconstruct a deleted interpreter to understand what the code does today.
  - Several docs now state the opposite of the behaviour.
- Expert alternative:
  - Rewrite each class and method doc as a present-tense contract, and move history to git and the design docs.
  - Enable `-Xdoclint:reference` so that links to deleted methods fail the build.
- Confidence: verified

### TESTPLAN-9 [S2] Dead code, residue and stubs kept alive for a guard
- Where:
  - Methods with no callers: `AssertVerdicts.java:1566` `databaseMode`, `:2078` `containsTreeMarker` and `:2084` `concat`; `VerdictBatch.java:163` `frameCensus`.
  - `BodyDriver.java:539-599` (`EFFECT_ARMS`): 6 of its 10 entries throw (`effectIsAPlanStep`).
  - `registeredRoutineKeys()` (:592-599) is a hard-coded set that routes nothing. Both exist so that `NativeRegistryGovernanceTest.java:34,42` has something to compare with the catalog.
  - Unused parameters: `BodyDriver.java:658-659` (`answer`'s `rider` and `identityLane`) and `:834` (`evalStringArg`'s `env`).
  - `BodyDriver.java:343` is an empty section header.
  - `SqlTextVerdicts.java:572-578`: `dbType = "unresolved"` is followed by an unreachable `if (dbType == null) return null;`.
  - `BodyClass.java:866-869`: `refused(String why) { return why; }`.
  - 23 orphaned or stacked javadoc blocks, e.g. `BodyDriver.java:374-381,601-611,813-830`, `AssertVerdicts.java:54-61,1153-1157,1195-1213`, `DatabaseJudge.java:369-372`.
- Expert alternative:
  - Delete them.
  - Dispatch effects off the catalog itself (`NativeFn.Effect` in `EffectPlanner`) and drop the registry half of the governance test.
- Confidence: verified

### TESTPLAN-10 [S2] Planning writes process-global counters and reads system properties
- Where:
  - `AssertVerdicts.java:494,524,535,589,679`: `VerdictCensus`, which is a static `ConcurrentHashMap`.
  - `DatabaseJudge.java:94,583,766,1021,1034`.
  - `TestPlanner.java:149`: `Census.inc`.
  - `BodyClass.java:747-752` reads `System.getProperty("legend.diagnostics")` and `legend.test.current`, and prints to `System.err`. `TestPlanner.java:168` uses `Diagnostics.on` instead.
  - `TestPlanner.java:176`: `System.out.println`.
- What: planning is not a pure function of its inputs. Arms planned more than once (the gate, the static pass, the re-plan in `HostAsserts.java:220-233`) interact with the counters. The javadoc claims "Planning sends nothing and counts no verdict's outcome" (`HostAsserts.java:33`).
- Why it matters:
  - It works against G10 (determinism) and G4 (plan only plans).
  - It is the root cause of the static pass in TESTPLAN-3.
- Expert alternative:
  - Derive the census from the finished plan: `PlanCensus.of(TestPlan plan)`, a pure function. If a sink is needed, pass a `CensusSink` in explicitly.
  - Use one diagnostics facade.
- Confidence: verified

### TESTPLAN-11 [S2] Stringly-typed forms and cross-layer naming contracts
- Where:
  - Render forms `"CSVTEXT"`, `"TDSTEXT"` and `"CSVJOIN:"+sep` (`AssertVerdicts.java:1966-2006`) travel as `String form, eForm, aForm` through `VerdictArm.rendered` and `HostJudge.planRendered` into `HostJudgment.Rendered`.
  - Statement origins `"SIDE"`, `"REFEREE_OURS"` and `"STATEMENT"` (`BodyDriver.java:749`, `SqlTextVerdicts.java:104,593`).
  - `"__canon"+i` column names at 7 sites in `DatabaseJudge` (:197,236,241,522,553,723,748). They must match `lowering/CanonicalRenderSql.java:322`, and that is only a convention.
  - `"frame_"+name` at three sites (`BodyDriver.java:496,500`, `DatabaseAsserts.java:148`).
  - Test-data generator flavours `"plan"` and `"seedString"` (`BodyClass.java:1070`, `GeneratedTestData.java:92`).
  - `dbType` compared as text against `REPLAY_REFEREE.name()` (`SqlTextVerdicts.java:610`).
  - `VerdictBatch.armOf` parses a verdict name by `"::"` and `"_"` (:106-111).
  - `fqn.startsWith(PKG)` prefix matches (`AssertVerdicts.java:1273,1531`) sit beside the "identity, never a name's prefix" guardrail used elsewhere.
  - Literal FQNs for forAll, contains, type and at (`AssertVerdicts.java:1102,1160,2046,2054`; `BodyDriver.java:458`).
- Expert alternative:
  - `sealed interface RenderForm { CsvText; TdsText; CsvJoin(String sep) }`.
  - A shared `enum Origin`.
  - `CanonRider.canonColumn(int i) -> OutputCol`, owned by the lowering.
  - `NativeFn` ids for the platform functions.
  - An enum for generator flavours.
- Confidence: verified

### TESTPLAN-12 [S2] Dialect capability is probed with exceptions, including one fallback
- Where:
  - `DatabaseJudge.java:1013-1019`: renders the JSON multiset statement and, on `DialectCapability`, falls back to a byte compare. That is the "rejected statement retried another way" that G7 forbids, decided at planning.
  - `DatabaseJudge.java:464`: renders only to provoke a refusal.
  - `VerdictBatch.java:426-447,503-519,654-660`: `planAhead` catches `DialectCapability` to choose a shape.
  - `JudgePlans.java:139-142`: renders a statement to count its characters.
- Why it matters: control flow is driven by exceptions. A dialect bug that throws `DialectCapability` silently changes which verdict form is planned.
- Expert alternative: expose capability as a query, `SqlDialect.refusal(SqlQuery q) : Optional<DialectCapability>` or `spells(...)`, and choose among alternative forms explicitly in one place, each choice named.
- Confidence: verified

### TESTPLAN-13 [S2] Test-suite structure: pinning, legacy names, thread-local state
- Where:
  - `BodyClassTest.java:23-37` pins the enum's names with an append-only, dated list.
  - `BodyRouterParityTest.java` is 2,177 lines, with 54 `assertParity` calls that no longer compare anything (doc at :41-45). It carries a `ThreadLocal<Boolean> COMPAT` toggle (:51-63).
  - Assertions on message prefixes, e.g. `:1760`.
  - Contrast: `AssertBarTest` and `VerdictBatchSplitRungTest` are good behavioural tests.
- Why it matters:
  - The largest test file in the unit is named for a property it no longer checks, so readers misjudge what is covered.
  - The thread-local mode makes test helpers order-sensitive.
- Expert alternative:
  - Rename `BodyRouterParityTest` to `BodyPlanningTest` and split it by class (effect, query, assert, frame).
  - Pass the judging rules as a helper parameter.
  - Drop the name-pinning test, or replace it with behavioural examples per class.
- Confidence: verified

### TESTPLAN-14 [S3] Inline fully qualified names and unused imports (pattern)
- Count: 1,603 inline `com.legend.` names and 334 `java.util.` names on code lines, while 9 imports go unused (for example `TestPlanner.java` imports `IOException`, `UncheckedIOException` and `Map` and uses none of them).
- Examples:
  - `TestPlanner.java:40-52`.
  - The `ExecEnv` record header at `BodyDriver.java:59-71`, e.g. `com.legend.testplan.@com.legend.base.Nullable VerdictBatch verdictBatch`.
  - The `instanceof` ladder at `AssertVerdicts.java:1738-1751`, with 14 `com.legend.compiler.spec.typed.` prefixes.
- Expert alternative: import the types and write `@Nullable VerdictBatch`, which is legal once the type is imported.
- Confidence: verified

### TESTPLAN-15 [S3] Process history in comments, identifiers and runtime error messages (pattern)
- Count: 341 ISO dates and about 409 milestone or audit codes (M10c item 8, slice 5, the M9b audit's F2, ...) in the unit's comments. The comment-to-code ratio is 0.38.
- Examples:
  - `BodyClass.java:487-505`.
  - User-facing error messages that carry milestones: `TestKinds.java:206-217` ("not planned yet (the test-is-the-product plan's M6-M8 ...)") and `TestKinds.java:453-457` ("the T9 kind the test-is-the-product plan's M9 plans").
  - `AssertVerdicts.java:1955-1956` ("(I2, 2026-11-07: the walk's unbatched road deleted)").
- Expert alternative: keep the "why" in the javadoc, and move dates, rulings and audit references to git and the design docs. Error messages should name the construct and what to do.
- Confidence: verified

### TESTPLAN-16 [S3] Hand-rolled tree rewrites, let chasing and duplicated small types (pattern)
- Where:
  - Five copies of the children → map → `withChildren` loop: `BodyClass.java:240-259,273-297,691-721` and `GeneratedTestData.java:92-120,151-167`. `TypedSpec.mapChildren` already exists (`compiler/spec/typed/TypedSpec.java:120`).
  - `AssertVerdicts.java:1770-1790` (`chaseLets`) duplicates `Lets.bound` with different, non-lexical scoping.
  - Depth caps that stop silently: `BodyClass.java:274` (`depth > 32` returns the node unsubstituted) and `BodyDriver.java:257` (`depth > 8` means "not an assert helper").
  - `JudgePlans.Kind` (:203) duplicates `SideShape.Kind`.
  - `JudgePlans.sideOf` (:73) duplicates `VerdictBatch.sideOf` (:673).
- Expert alternative: use `mapChildren` and `Lets.bound`. Throw where a depth bound is reached. Reuse `SideShape.Kind` and keep one `sideOf`.
- Confidence: verified

## Cross-unit observations
- **testexecutionplan:**
  - `Failure.Kind` names exception classes by string, including a planner inner class (`"com.legend.testplan.SqlTextVerdicts$NotAConstant"`) and JDK defect types (NPE, CCE, IOOBE). The plan-data package therefore knows planner internals and treats defects as test outcomes.
  - `SideShape` is a kind plus 10 fields whose valid combinations depend on the kind. It should be a sealed type.
- **planner:** `PlanEnv` is a record whose `queryLets` and `planRows` are shared mutable accumulators, plus a `stamp()` mutator. Copies are not isolated.
- **lowering:**
  - `CanonRider` is a mutable out-parameter (`decline`, `wrap`) threaded through the judges.
  - The `"__canon"+i` column-name contract is spelled independently in `CanonicalRenderSql`, `ClassLayouts.SYNTHETIC_CANON`, `result/CellText` and testplan.
- **diagnostics:** `VerdictCensus` and `Census` are process-global static counters written during planning.
- **compiler:** `TypedTestDataGen.flavor()` is a `String` that consumers dispatch on.
- **guards:** `NativeRegistryGovernanceTest` keeps a throwing `EFFECT_ARMS` registry and a hand-written routine-key set alive in `BodyDriver`. It is a guard that preserves residue.

## Measurements
- **Size:** 13,345 lines of main code in 30 files and 4,430 lines of tests in 16 files. Command: `wc -l core/src/{main,test}/java/com/legend/testplan/*.java`.
- **lizard** (`python3 -m lizard -l java core/src/main/java/com/legend/testplan/ -s nloc`): 9,116 NLOC, 559 functions, 34 warnings. Top methods:

  | Method | CCN | Lines | Params |
  |---|---|---|---|
  | `DatabaseJudge.databaseVerdict` | 92 | 204 | 13 |
  | `BodyClass.frameProgram` | 76 | 221 | 3 |
  | `AssertVerdicts.adjudicate` | 65 | 313 | 5 |
  | `AssertVerdicts.planArm` | 47 | 96 | 7 |
  | `BodyKinds.of` | 40 | 115 | 3 |
  | `DatabaseAsserts.plan` | 39 | 208 | 7 |
  | `AssertVerdicts.planHost` | 38 | 109 | 5 |
  | `VerdictBatch.plan` | 33 | 159 | 0 |
  | `HostAsserts.plan` | 33 | 145 | 6 |

- **jscpd** (`jscpd --min-lines 8 --min-tokens 60 core/src/main/java/com/legend/testplan`): 4 exact clones, 0.81% of lines. Textual copy-paste is low; the duplication is structural (TESTPLAN-1, -3).
- **Inline FQNs on code lines** (grep, excluding import and comment lines): 1,603 `com.legend.` and 334 `java.util.`. The heaviest files are `AssertVerdicts` (323), `SqlTextVerdicts` (285), `DatabaseJudge` (263) and `BodyDriver` (218). There are 9 unused imports.
- **Comments:** 3,426 comment lines against 9,116 code lines. They contain 341 ISO dates and 283 mentions of "the walk" (`grep -oiE 'the walk'`), most in `BodyClass` (75), `AssertVerdicts` (50), `DatabaseAsserts` (37) and `HostAsserts` (34).
- **Javadoc defects** (small Python scans over the package): 23 stacked or orphaned javadoc blocks and 14 dangling in-package `{@link}`s.
- **`catch (RuntimeException`:** 18 sites in the unit.
- **BodyClass values in production:** read only in `TestPlanner.java:95,100,126,137`, where they drive three booleans; tests reference them 26 times.
- **VerdictArm:** 18 methods, 1 implementation (`DatabaseJudge.ARM`).
- **Verdict SQL:** 71 `VerdictSql.*(` calls and 45 distinct functions. SQL keyword string literals: only `SqlTextVerdicts.java:1255-1256`.
