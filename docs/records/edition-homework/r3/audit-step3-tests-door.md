# Audit of d69cd2a0: the edition program's step 3, legend-engine's own tests' door

I read the code as it is at d69cd2a0. Nothing was built or run.

**Verdict.** I found no blocker on the product road or on another door: claim 1 holds. At the corpus door the headline claim does not hold for the corpus's most common query shape, a query lambda bound by a `let` (finding 1). There are two smaller places where the test's own code gets legend-engine's meaning (findings 2 and 3). Two pinned guards will fail as the commit stands (finding 4).

Files read in full:
- the commit's diff
- `edge/EngineTable.java`
- `typed/TypedNativeCall.java`
- `EngineTestsStyleTest.java`
- `LeanSqlCorpusTest.java`

Read by region:
- `UserCallInliner` (constructors, stamp, `inlined`, `Scope`, `rewrite`, the native-call arm)
- `ResultEnvelopeSplice` (hook, rewrite, valuesRead/executedExtent)
- `ProductStatements` (55-300)
- `BodyPlanner` (170-225, 430-450)
- `BodyDriver` (240-330, 470-560, 700-810)
- `ExecuteChainAssembly` (55-230, 500-655)
- `FramePlanner` (80-320)
- `TestKinds` (215-470)
- `TypedSubst` (1-200)
- `TypedSpec.mapChildren`
- `PureText` (40-240)
- `PlanEnv.editionTable`
- `PlanAllocations` (180-215)
- `EnginePlans` (30-175)
- `SqlTextVerdicts`: the plan-text arm, rowsAhead, readText, lookThrough
- `AssertVerdicts`: the hook adaptations, unrolledElement callers
- `JavaEvalLedgerTest`: EVICT_SIZE and its rule
- `HookRulingTest`, `HookLambdasTest`, `ArchitectureTest`: the rules named below

legend-engine's sources were grepped read-only in the Bazel cache (`core_relational`).

## Findings, by severity

### F1. MAJOR: a query lambda bound by a test's `let` is stamped, so the query gets Pure's meaning
Location: `UserCallInliner.java:201-213`, reached from `:267-269` and `:415`.

**What happens.** `stampAroundFrames` has no exemption for code as a value. Compare `stampedIfStoreFree`, where `holdsCode` returns the value unstamped where it is bound. So `let q = {|...}` has every call in its body stamped where it is bound. The chain from there:
- `TypedSubst` substitutes `execute`'s first argument. Only the runtime argument is kept as written (`TypedSubst.java`, the orchestration arm).
- So `toSQLString($q, ...)`, `executionPlan($q, ...)` and an inline `execute($q, ...)` are handed the already-stamped lambda.
- `handsEngineAQuery` leaves the call alone, but its argument is already stamped.
- `EnginePlans.toSqlString` reads `in.query()`, which is `call.args().get(0)`. `SqlTextInputs` reads through the lets only for the receiver form (`:54`).
- `engineSql`'s inliner keeps the stamps.
- `EngineTable.lowersAs` returns no row for a stamped call, so the call lowers with Pure's meaning.

The same happens to a lambda `let` applied inside a frame's query. `inlineFrameQuery` stamps the test lets (`:415`), so `let f = {p:Person[1]|$p.name->substring(1,3)}; execute(|Person.all()->project($f,'x'), ...)` writes Pure's substring into the SQL. It also happens to a lambda held in an instance: `testToSQLString.pure:70` writes `toSQLString($testCase.query, ...)`.

**Concrete corpus case.** `meta::pure::executionPlan::tests::testSubstringIndexingCorrectedByFeatureFlag`:
```
let query = {|Person.all()->project(p|substring($p.firstName, 4, 9), 'slice')};
executionPlan($query, ...)->planToString(...)
```
It expects the uncorrected plan to contain `substring("root".FIRSTNAME, 4, 9)`. It is on both lanes' fail rosters before and after the commit, so the measurement could not see this. The corpus holds about 427 `let x = {|...}` lambdas and about 29 `toSQLString($var...)` calls in test files. The three SQL-text tests that now pass use inline lambdas (for example `testToSQLStringSubstringIsUncorrectedByDefault`).

The javadoc at `:192-200` says function values are stamped where applied. The code stamps them where bound too.

**Fix.** In `stampAroundFrames`, leave code that is a value unstamped: a node whose type `holdsCode`, and a `TypedLambda` that is not an argument of a native call. The existing `reducedTerms && holdsCode` branch already stamps such a value where test code applies it. Add an `EngineTestsStyleTest` case: `let q = {|...substring...}; toSQLString($q, ...)`, and the same case through `executionPlan`.

### F2. MAJOR: the identity set can leave the test's own code unstamped (program road)
Location: `ResultEnvelopeSplice.java:193-197`, read at `UserCallInliner.java:202`.

**What happens.**
- `NotingHook.note` records every descendant of each frame chain by identity.
- The inliner keeps untouched subtrees by identity (the native-call arm: "untouched subtrees keep identity").
- Callee bodies are compiled once and shared.

So a parameter-free subtree of a helper keeps the same node object in the frame's chain and in the statement's own code. Example:
```
function x::code():String[1] { 'ABCDEF'->substring(1, 3) }
{| let r = execute(|Firm.all()->filter(f|$f.name == x::code())->project(...), M, RT, ext);
   $r.values->filter(x|$x.getString('name') == x::code()); }
```
The statement's own `substring` node is a noted frame node. It is returned unstamped and gets legend-engine's `substringAsGiven` in the test's own code. By the brief's definition this is blocker-class if any body reaches it. None was found by grep.

This affects the program road only: the judges adapt the hook through `AssertVerdicts.SpliceHook` (`rawHook::apply`, `AssertVerdicts.java:196/355/775/1471`, `DatabaseAsserts.java:307/434/543`), which drops the `NotingHook`.

**Fix.** Note only the root of each view the hook hands out. The walk stops at the first frame node and never descends, and the `TypedFrom` rule already covers a chain whose root was rebuilt. Or drop the identity set: on the judges' road the shape rules alone already cover every frame shape.

### F3. MAJOR: the test's own impossible casts and inverted slices are written plain at the corpus door
Locations: `EngineTable.java` `checks`, `CastPolicy.java:179`, `ListEncodings.java:265`.

**What happens.** A cast and a slice ask `checks(kind, null)`, with no call and so no stamp. `CAST_IMPOSSIBLE` and `SLICE_BOUNDS_INVERTED` are in `ENGINE_UNCHECKED`, so they are plain wherever they are written, including the test's own code. Example: `let v = $r.values->at(0).rows->map(r|$r.getInteger('id'))->slice(2, 1);` in a harness. legend-pure raises there. lite now writes the database's slice. Before the commit (`UPSTREAM`, lite's edition) both checks were written.

This is the test's own code getting legend-engine's meaning on a road that plans SQL. Its incidence is unmeasured.

**Fix.** Pass the slice's call to `checks`, as `Scalars` does. Give the cast a stamp, or read the stamp of the enclosing call.

### F4. MAJOR: three guards will fail as the commit stands
**`JavaEvalLedgerTest.javaEvaluationSurfaceOnlyShrinks`** has exact pins on comment-stripped, non-blank lines. I measured with the test's own regexes; the parent commit equals the pins.
- `BodyDriver.java`: 547 becomes 544.
- `BodyPlanner.java`: 318 becomes 316.
- `FramePlanner.java` stays at 212.

Each needs a dated re-pin downwards.

**`MinimalCorpusTest`** holds each lane's fail roster equal to the failing set. The six tests that now pass per lane must leave `duckdb-fail-roster.txt` and `h2-fail-roster.txt`.

**`LeanSqlCorpusTest`** now plans under `ENGINE_TESTS`. Plain checks and legend-engine's rows inside frame runs shrink SQL, and a family that shrinks by more than 1% asks for a re-pin. `lean-sql-baseline.tsv` is not re-measured in the commit.

Guards that will not trip:
- `PLANNER_ENTRIES_ONLY`: `ExecuteChainAssembly` is not a planner package and `chain` has only `FramePlanner` as a caller.
- `HookLambdasTest`, `EdgeOnlyMembersTest`: the edge is excluded.
- `NativeCallStampGuardTest`: the commit builds no new native call.
- `thePureV1StyleIsSelectedByThePureV1RoutesAlone`.
- `NoSaltedIterationOrderTest`.

### F5. MINOR: a store-free `let` inside an executed lambda is not stamped, unlike the same query at pure/v1
Locations: `UserCallInliner.java:388-390`, `ExecuteChainAssembly.java:531-532`.

`inlineFrameQuery` passes `stampStoreFree=false` for the query's own statements. Consider `execute({|let cut = 'Acme'->substring(1,3); Firm.all()->filter(f|$f.name == $cut)...}, ...)`:
- **corpus:** `cut` gets the database's row.
- **pure/v1:** the same query stamps `cut` (Pure's meaning: legend-engine's own Java computes the allocation).
- **plan-text arm's appeal:** `SqlTextVerdicts` adds the query's own lets to a judged side's prefix (`bound.addAll(lam.body().subList(...))`), and `inlinedSide` stamps them there. That is a third answer for the same `let`.

**Fix.** Ask the engine runner what legend-engine does. Then either pass the edition's `stampsOutsideStore` for the query's own statements, or record a register row.

### F6. MINOR: test values that reach a frame's query by roads other than a `let` are not stamped
- **`executeLegendQuery`'s vars values** become the query's leading lets (`ExecuteChainAssembly.java:224`). They sit at positions past `testLets`, so they are not stamped. Example: `[pair('d', 'ABCDEF'->substring(1,3))]`. `coerceVar`'s minted parse is plain too.
- **The arguments of a helper that builds a lambda** are inlined by plain inliners (`ExecuteChainAssembly.java:89`, `:177`). Example: `execute(buildQuery($s->substring(0,2)), ...)`.

Values read through a variable are fine.

**Fix.** Count the vars lets as test lets. Stamp a helper's arguments before inlining it.

### F7. MINOR: the stamp on mapping code is decided once for a whole judged side
Locations: `ProductStatements.java:208-209`, `BodyPlanner.java:215`.

`sideStamp` reads "fetches a graph" and "reads through a chain" off the whole side. A side that splices a graph-fetch frame and a TDS frame over the same model-to-model chain therefore stamps both frames' transforms. legend-engine pushes the TDS frame's transforms into its SQL. This is pure/v1's recorded statement-wide rule, newly exposed: a corpus side often splices several frames.

### F8. MINOR: no refusal by construction, and the table's database is the harness's
Location: `PlanEnv.editionTable`, with `rcorpus::Rt` at `MinimalCorpus.java:304`.

Every corpus statement plans with `rcorpus::Rt`, which binds every database to the lane's connection. So `runtimeFqn` is never null and the table is always DuckDB's or H2's. That is why the run saw no refusal. The `execute()`'s own runtime never chooses the table.

Consequences:
- A query run over a model-data runtime gets the lane's rows. pure/v1 refuses such a query, and legend-engine runs it in its own Java.
- `//spec:corpus_on_postgres` will now refuse every query-side row, because Postgres is not in `CHECKED`. That is a measurement move.

### F9. MINOR: the tests pin less than the claims
**`EngineTestsStyleTest`:**
- Case 1 only asserts the two plans differ. Any difference passes, including a wrong one on the test's side.
- All three cases are program-road last statements under the host judge.

**No test for:**
- the judges' road
- the database judge
- a class-rooted frame
- `toSQLString` or `executionPlan` text
- a lambda bound by a `let` (F1)
- `executeLegendQuery`
- a frame's own `let`
- a cast or a slice

**`HookRulingTest`** has no `plainOf(ENGINE_TESTS)` row.

### F10. NOTE: stale comments
- `PureText.java:48-49`: the `isEdgeStyle` javadoc lists two styles.
- `PureText.java:139-140`: `UPSTREAM.twoForm` says "PCT's and the corpus's doors hand none yet".
- `PureText.java:186` and `ArchitectureTest.java:3638`: "PCT and the corpus keep UPSTREAM".
- `PlanStyleSurfaceTest.java:80`: "the edge's two".
- `UserCallInliner.java:192-200`: the javadoc contradicts the code (see F1).
- `ResultEnvelopeSplice.java:168-170` says the splice's wrappers "hold no call". The string entry's `values` envelope concatenates with calls, and the size and at/toOne/first arms re-mint the test's own calls.
- `JavaEvalLedgerTest`: Edition is "an interface of five questions"; it has seven methods now.

### F11. NOTE: the "fragment" rule is absent from the test-body walk
A test-level `let` bound to a relation that is not yet executed, read by a later query, is stamped. pure/v1's rule makes such a `let` part of that query. grep found no corpus instance at test level.

### F12. NOTE: a copy rebuilt by the stamp hides a `random()` read twice
A function-valued `let` term is rebuilt at each read. `readOnce` counts reads by identity, so a lambda holding `random()` or `generateGuid()` read twice could escape its refusal. This only happens at this door.

### F13. NOTE: moved answers are not yet posted or recorded
Answers that move at the corpus door, which AGENTS.md asks to post to the user and record as register rows:
- plain checks inside corpus queries (division, parse, toOne over several)
- transforms under a judged graph-fetch side
- harness casts and slices (F3)

## The four questions

### 1. Is the product road untouched, and the other doors?
Yes. I found no counterexample.

- **`LiteEdition` and `EngineTable.DOOR`** answer `stampsTestBodies() == false`.
- **`ProductStatements`:** the 6- and 7-argument `prepare` now reach `prepared(frame=false)`, and the 8-argument one reaches `frame=true`. Before, all three ended in the same 9-argument call with `frameAnswered=false`. `frame` only gates `testBody`, which is false for both editions.
- **`inline`:** `ofTestBody(specs, hook(recorder,false), false)` equals the old `new UserCallInliner(specs, ResultEnvelopeSplice.hook(recorder))`.
- **`BodyPlanner.inlinedSide`:** with a null hook the old code built `UserCallInliner(specs)`, which is the same as `UserCallInliner(specs, null)`.
- **`BodyPlanner.stagedSide`:** `sideStamp` gives `NONE`, as before.
- **`BodyDriver`:** `env.queryLets()` is `plan.queryLets()` (`BodyDriver.java:105-106`), the same map, and `letPrefix.size()+1` equals `single.size()`.
- **`BodyDriver.spliceHook`:** `hook(false, frames)` is `hook(frames)`.
- **`ExecuteChainAssembly.chain`:** `inlineFrameQuery(qb, 0)` is `inlined(qb, null, false, 0)`, which is `inlineBody(qb)`.
- **`UserCallInliner.inlined`:** with `testLets == 0` and `testBody` false, the `let` loop evaluates `reduce` and then `stampedIfStoreFree` with the same arguments in the same order. Java evaluated the old call's arguments left to right, so the order is unchanged.
- **`stampedIfStoreFree`:** the new early return needs `testBody`.
- **A `NotingHook` handed to an ordinary inliner** (`VerdictQueries.unrolledElement`, `forVerdictSource`) sets `noting`, but is never read without `testBody` or test lets.

### 2. Is the stamp rule right?
Counterexamples: F1, F2, F3, F5, F6 and F7. Everything else checked out:

- **What the shape rules cover.** A frame chain is always under a `TypedFrom`: `chain()` wraps it, or it carries one inside. The splice's wrappers are `From`s too (executedExtent, rung 12, readThroughExecuteValues). CTE reads are `From(TypedFrameRef)`. `$r.activities` re-roots at the `execute` call, which `handsEngineAQuery` keeps.
- **What the shape rules miss on the judges' road**, where the identity set is lost: the string entry's `values` envelope (`plus` calls, no row, no check) and code above an inner `from` (`testPreeval`'s join; a serialize over `from`). All are harmless today.
- **The test's own re-minted calls.** `toOne`, `at` and `first` over a class frame, and `$result.rows->size()`, are correctly stamped (Pure's).
- **A test's own `->from()`** is a query, so it is not stamped. That is right.
- **A test `let` read inside a frame** is stamped where it is bound, on both roads. That is right. So is a test `let` built from an earlier frame's values: the `execute` call inside it is kept and the rest is stamped.
- **Calls the judges mint** (`not`, `equal`, `abs`, `minus`, `lessThanEqual`) enter the side before it is inlined, so they are stamped. None is a table row or a checked kind.
- **Other inliners on the test road** are analysis only, or plan the frame's own code: `ExecuteChainAssembly` 89/177/472/551/645, `FramePlanner:155/296`, `PlanAllocations:203`, `EnginePlans:63/155`, `RuntimeContexts`, `SqlTextVerdicts:465/1198`, `EdgeEngineTestText:88`, `PlanMaker:1212`, `TestKinds:306`. `VerdictQueries.unrolledElement`'s output and `lookThrough`'s scope are inlined again by `inlinedSide`, where they are stamped. Exceptions: F6.

### 3. Identity and sharing
- `mapChildren` returns the same node when no child changed (`TypedSpec.java:122-132`). An unchanged subtree with no call, and frame code, keep their identity.
- Every ancestor of a stamped call is rebuilt.
- `reducedTerms` holds the stamped term itself (`Scope.put`).
- The `pastes` count and `again` see the frame roots unchanged.
- Risks: F2, where shared nodes make the identity set unsafe, and F12.

### 4. The new style
- `isEdgeStyle` is updated.
- `PlanStyleFacetsTest` and `PlanStyleSurfaceTest` are updated: `PureText$3` is `ENGINE_TESTS`.
- `MinimalCorpus.java:526` opens sessions under `UPSTREAM`. They match, because the dialect and settings are delegated to `UPSTREAM`.
- `SqlTextVerdicts:1448` (`shapeOf(p, UPSTREAM)`) reads only presentation, which is delegated.
- PCT keeps `UPSTREAM`.
- `EdgeServer` and the hosts use `PURE_V1`.

No identity comparison was missed. The gaps are the stale comments (F10) and the lane measurement move (F8).
