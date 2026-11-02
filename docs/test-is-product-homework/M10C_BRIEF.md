# M10c brief — the remaining shapes the two planners refuse (2026-11-02)

The plan's §7 M10c (the revalidation's split of "the last class": "the remaining host shapes `planHost` refuses: the
quantified forms, the cell pool, the lineage lines, `assertIs` over non-elements, the grid pair and the class-instance
`assertEq` under the database judge -- each a form or a named refusal"). Written before the code (R15) from a
MEASUREMENT of the DuckDB corpus after M10b (5ce2cf5d): every body still walked whose class the router admits but whose
judge's planner refuses, attributed to the planner's refusal site by a temporary probe (the first refusal after the
test's trace marker; passing bodies). Its audit checks the build against it.

## What is still walked, and why (DuckDB, passing bodies)

The host pass walks 460 passing bodies, the database pass 992. By cause:

| Cause | Host | Database | Where |
|---|---|---|---|
| a user store's read beside a system read, or a shape the assert class refuses (M10a's residue (f)) | 137 | 139 | `BodyClass` (`assert,system-read`) |
| effects, seeded or graph frames, the generator carrier mixed with system reads (M10d's) | ~140 | ~140 | `BodyClass` |
| **the planners' refusals (this step)** | **153** | **683** | below |

The planners' refusals, by site:

| Refusal | Host | Database | Shape, with a corpus witness |
|---|---|---|---|
| `DatabaseAsserts.plannable`: a raw grid read or a lineage scan reached through the body's helpers | -- | ~370 (161 over temporal frames, 127 over frames, 49 with system reads, 30 plain) | `assertEqualsH2Compatible(planText, $result->planToString(...))` (`testClassPropertyOpenVariable`). Measured by a probe printing the predicate's root and its hit (one scoped `corpus_one` run): 321 bodies hit `raw<SELECT H2VERSION()>` through the ROOT `assertEqualsH2Compatible` -- the scan followed the user call into the engine's helper `getH2Versions` (h2Extension.pure:44-49), a body no road runs; 49 through a `scanRelations` call and 6 through `scanColumns` (the lineage scans, a system read). **Item 1a, BUILT 2026-11-02.** |
| `databaseGate`'s switch: a GRID PAIR (both sides relations) or a RENDERED TEXT (`renderedPlan`) under `assertEquals`/`assertSameElements` -- M7's "the shapes whose database arm reads a side on the host" | -- | ~221 (209 equals, 12 same-elements) | `assertEquals($result.values, ...)` grid pairs over frames (`columnValueDifferenceTest`, `isolationTest`); `relationalResultSourcingOfList` |
| `LineageTreeLines.asLines`: a lineage tree print against a golden | 49 | (inside the ~370 and `db:host-null`) | `scanRelations($query, $mapping, ext)->relationTreeAsString()` against a text (`scanRelationsTests::testConstant`) |
| the equality arm declines: a plan-text read through a HANDLE is no constant of the plan (`NOT_A_CONSTANT`) | 43 | 39 | `assertEquals('select ...', $xstoreResult.rootExecutionNode.executionNodes->at(0)->cast(@SQLExecutionNode).sqlQuery)` with `let xstoreResult = executionPlan(...)` (`testJoinWithConstantDate`) |
| a user-written assert helper at the root (`fn == null`: not a verdict native, not a root arm) | 20 | 20 | `assertConversion(...)` (`toPostgresModel::tests`) |
| the quantified forms (`->map(x|assert(...))`, `forAll`) | 18 | 17 | `testCbrt`, `testDayOfYear`, `testPad` |
| M10b's S1 (an engine-text assert in a system-routed body: the appeal needs the user session) | 9 | 9 | `planProjectWithDerivedProperty` |
| a side the walk routes to the system session inside a body that is not system-routed (a lineage scan's rows) | 5 | -- | `testAssociationMapping`, `testQualifier` |
| the dual-golden arm declines its route (the rewrite planned, null) | 6 | -- | `testSqlGenerationForAdjustStrictDateUsageInFiltersForH2` |
| a text side that is no constant (`assertEqualsH2Compatible`, `assertSameSQL`) | 5 | 1 | `testToSQLStringWithCodeBlock` |
| no callee at the root | 2 | 2 | -- |

## The design, item by item (largest first)

1a. **The static scan reads what the program road runs (database `plannable`: 321 bodies). BUILT 2026-11-02.** As
   first drafted this item attributed the 321 to the handle's rows read as a raw relation; the code says otherwise
   (`TypedRawSqlRelation` is minted for `executeInDb` text alone, `Overloads.rawGridOrSelf`) and the probe confirmed
   it: the refused statement is the root `assertEqualsH2Compatible`, and the hit is the `executeInDb('SELECT
   H2VERSION();')` in the engine's helper `getH2Versions`, which `StatementEffects.reaches` found by following the user
   call into its body. The program road never runs that body: the arm plans the two texts from the call's arguments
   (`SqlTextVerdicts.plan`), a declined dual-golden is the function's own meaning, `assertEquals(upgraded, actual)`
   (`planHost`), and any other decline leaves the body to the walk -- where the dual-golden arm is total and the other
   two roots' declines inline the callee, as they always did (the audit's correction: "either road" was too strong). The rule: `plannable` scans what the program
   road would RUN of a statement (`scanned`): a root engine-text assert's ARGUMENTS alone, every other statement whole.
   The witness: the frame model's `assertEqualsH2Compatible` carries the engine's own body (the helper's `executeInDb`
   read, spelled by FQN), so the DATABASE-judge witness in `engineTextPrograms` pins that such a body moves. Measured:
   DuckDB database pass moved 2,122 -> 2,398, H2 2,055 -> 2,331, outcomes identical, every register green.
1b. **The handle's text through a navigation (host `NOT_A_CONSTANT` 43, database 39).** An orchestration handle bound
   by a let (`executionPlan(...)`, `scanRelations(...)`) registers its rows at planning (`PlanAllocations.registerHandlesIn`:
   the plan's nodes, the lineage rows) and THE ROWS RIDE THE QUERY (the user's ruling of 2026-09-02): a read of them
   (`$p.rootExecutionNode.executionNodes->at(0)->cast(@SQLExecutionNode).sqlQuery`) is a statement over inline rows the
   database answers, so under M10b's design (the two texts constants of the verdict step) the side is no constant and
   the body stays on the walk. Two forms, to decide at this item's turn after a measurement of the navigations the
   corpus spells: (i) the text side a STEP of the plan whose one row binds the verdict step's text parameter (the frame
   stamp's own mechanism, M9c; R17 C4), the appeal reading the bound text at the step -- the tenet's form (the database
   computes the navigation); (ii) folding the navigation over the registered rows in Java -- a shadow evaluator of Pure
   navigation, which `docs/WORLD_MAP.md` refuses (compare spelled tokens, never compute a value): not an option. A
   navigation the planning cannot bind (a join with a user store) stays the walk's, by name.
2. **The grid pair and the rendered text under the database judge (~221 bodies). BUILT 2026-11-02: DuckDB database pass
   moved 2,398 -> 2,662, H2 2,331 -> 2,595, outcomes identical on every pass, the rosters byte-identical.**
   What the code says (read before the build; the first draft of this item had the mechanics wrong): under the
   DATABASE judge the walk's `adjudicate` routed a pair of two relation-typed sides to the HOST grid owner before any
   arm (`HostJudge.planGridPair`, D3: both grids executed, `TdsCompare.grids` judges), so M7's static gate refused the
   pair as "a side read on the host at the assert"; the rendered text's database arm (`renderedValueVerdict`) already
   plans both sides and brings the golden to rows -- it reads nothing, but its late-bound branch (a raw grid's text:
   a keyed `Render.gridCsv` statement) is a shape the program road does not place, so the gate refused every rendered
   pair. The build: the arm gains `gridPair` (the one dispatch: the router classifies, the judge's arm judges) -- the
   host arm the grid owner as before, the database arm `VerdictSql.gridPair` over the two planned grids (the cells
   walked for the declared-Float leniency where the schemas agree), a row multiset unless the actual side's order view
   is SORTED (the grid owner's own rule), the agreement arm both; `databaseGate` admits the pair when both grids are
   `staticGrid` (a relation type with named columns, none dynamic or late-bound) and the rendered text when its rendered
   grid is (`renderedStatic`). A late-bound grid (a raw grid, a dynamic pivot) stays the walk's, by name. The witness
   (`gridPairsAndRenderedTextsUnderTheDatabaseJudge`): equal and differing pairs, `assertNotEquals`, a defined order
   (a multiset) against a sorted actual (in order), toCSV against its golden equal and not, a frame's grid rendered, the
   rendered text under `assertSameElements` -- outcomes pinned absolutely, the same on both roads. The independent audit
   (COMMIT WITH FIXES, folded): the SQL grid pair compares row canons alone where the host grid owner decides three
   facts from the grids first -- the column names, the width (two empty grids of different widths), the expected
   grid's declared kinds typing both cells (a Float-declared expected against an Integer-declared actual) -- all facts
   of the two declared schemas; so ONE predicate, `sqlGridPair` (both grids declared, one width, named and typed alike
   position by position), both gates the database road and routes the walk's pair, and a pair it refuses keeps the
   host grid owner on every road (the walk under the DATABASE judge included: the first build had routed every pair
   to the database arm, which declines a late-bound grid where the host judged its cells); the three mismatches are
   witnesses that stay on the walk with the grid owner's verdicts; the ledger's exact pins re-dated (AssertVerdicts
   1511 -> 1559, DatabaseJudge 595 -> 600, HostJudge 1128 -> 1132). Residue: the declared-Float 2-ULP leniency under
   the multiset view joins cells by arrival where the host pairs rows as a multiset first (ULP-differing floats in
   another arrival order: the host holds, the statement fails; no corpus body); `renderedStatic` keys on
   `renderedSide` where the route keys on `renderForm` (a render-form-only shape is admitted and lands unjudged at
   planning, the walk's own outcome, nothing sent); the database arm counts no `grid-pair` route (the verdict row's
   `sqlJudgedInDatabase` is its census); the agreement lane (BOTH) now runs the SQL pair beside the host owner, one
   statement more per pair -- measured: `bazel run //spec:corpus_one -- duckdb both` on the final predicate, JUnit exit 0,
   2471 / 108 as the host pass, `disagree=0` (the agreement gate's answer).
3. **The lineage lines (49 + the database's).** `LineageTreeLines.asLines` already rewrites the assert into an ordinary
   collection equality (the golden's lines a String collection literal; ours the prelude's Pure body over the handle's
   lineage rows) -- the host checks plan the rewritten assert (`planHost(lines)` recursively, as the walk's `adjudicate`
   continues with it) and the database gate the same. The lineage scan's rows join the metamodel tables: the scan is a
   SYSTEM READ (`anotherWay`: `SCAN_RELATIONS`, `SCAN_COLUMNS` → `SYSTEM_READ`), so a body whose only store read is the
   scan is system-routed (M10a) and its side's session is the program's one session; a scan beside a user read stays the
   walk's (two sessions: the 5 `testAssociationMapping`-shaped bodies).
4. **A user-written assert helper at the root (20).** `assertConversion(a, b)` is a user function whose body asserts;
   the walk inlines it (`UserCallInliner.inlineBody`, `BodyCompiler` ~305) and adjudicates the inlined statement. The
   test door does the same: a statement-root user call whose callee's body is statements ending in an assert is
   inlined into the body at its place (the inliner, α-renamed lets), then planned as the body's own statements -- a
   named rule over the callee's body shape (every statement a let or an assert; no effect, no frame), the rest the
   walk's.
5. **The quantified forms (18).** Two shapes today: the VECTOR form (`xs->map(x|assert(pred))`: the predicate
   vectorizes in the database, the boolean vector judged on the host, the first failure raising) and the UNROLL form
   (a literal collection whose lambda carries lets or a non-boolean assert: each element bound as a let, the inliner
   reduces, each final assert a statement-root verdict). On the program road: the vector form is one verdict step whose
   side is the boolean vector and whose judgment is `HostJudgment.Condition` over every row (`ROWS_ALL`: holds when no
   row fails; T3's database form the predicate a column over the rows, holding when no row is false); the unroll form
   over a LITERAL collection is unrolled at planning into N statement-root asserts (the walk's own reduction, the
   inliner), each planned as any assert. A quantified assert over a runtime collection with lets stays the walk's, by
   name.
6. **The tails.** The dual-golden route null (6: the rewrite to `assertEquals(upgraded, actual)` declines too -- measure
   which arm), the non-constant text sides (5), no callee (2): each measured and named; none moves without a witness.
7. **Not this step:** M10b's S1 bodies (an engine-text assert in a system-routed body: 9 -- M10d, where the walk's two
   sessions go), the class-rule residue (M10a (f), the effect and frame mixes: M10d).

## Movement and measurement

Each item its own commit, measured whole on the DuckDB corpus (both judges) and H2; the parity witnesses on the frame
model gain a body per shape (a plan handle's SQL text under `assertEquals`, a grid pair over a frame under the database
judge, a lineage print, a helper at the root, a vector and an unrolled quantified assert); outcomes identical, every
register test green; the chain at M10's end.

## Residue, expected

Shapes the planning cannot know (a dynamic pivot's columns, a computed handle navigation, a quantified assert over a
runtime collection with lets), each a named refusal; the walk's two-session bodies (M10d); the engine-text appeals and
the referee (T13, for good).
