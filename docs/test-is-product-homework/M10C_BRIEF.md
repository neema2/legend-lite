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
| `DatabaseAsserts.plannable`: a raw grid read or a lineage scan reached through the body's helpers | -- | ~370 (161 over temporal frames, 127 over frames, 49 with system reads, 30 plain) | `let result = executionPlan({...}, myMapping, testRuntime(), ext); assertEqualsH2Compatible(planText, $result->planToString(...))` (`testClassPropertyOpenVariable`); the predicate that fires, measured: 321 bodies through `TypedRawSqlRelation` (the handle's rows read as a raw relation -- the plan's nodes, the lineage rows, a `VALUES` source at planning), 49 through a `scanRelations` call and 6 through a `scanColumns` call (the lineage scans, a system read) |
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

1. **The handle's rows are constants of the plan (database `plannable`, host `NOT_A_CONSTANT`: ~410 bodies).** An
   orchestration handle bound by a let (`executionPlan(...)`, `scanRelations(...)`, `toSQL(...)`) registers its rows at
   planning (`PlanAllocations.registerHandlesIn`: the plan's nodes, the lineage rows, from the handle's arguments) and a
   read of them (`$p.rootExecutionNode.executionNodes->at(0)->cast(@SQLExecutionNode).sqlQuery`, `$p->planToString(ext)`)
   is planned today as a STATEMENT over those rows (a `VALUES` source the database answers) -- so the engine-text arm
   sees no constant and the database planner's `plannable` gate meets the handle's raw-relation form. Both roads already
   hold the rows as data at planning; M10c folds a navigation over registered handle rows to the constant it is
   (`LiteralFold` over `PlanRows`/`LineageRows`, the compiler's own fold) where every input is registered, so the text
   is a constant of the plan, the arm plans it (M10b) and `plannable` sees no raw relation. Where the navigation needs
   the database (a computed index, a join with a user store), the shape stays a statement side and the body the walk's,
   by name. Measured first: which navigations the fold covers (the corpus's `sqlQuery`, `planToString`, `->at(k)`,
   `->cast`, `.executionNodes`).
2. **The grid pair and the rendered text under the database judge (~221 bodies).** M7 left "the shapes whose database
   arm reads a side on the host at its turn": the grid pair's `VerdictSql.gridPair` needs both grids' schemas (today
   read from a probe of the side), the rendered text (`TdsCompare.renderedText`'s line multiset) the grid's rendering.
   The schemas are the typed sides' relation types (`TypedRows.schema`), known at planning; `databasePlansWithoutReading`
   admits the pair when both are relations whose schemas the lowering declares, and the rendered text when its grid's
   is; the cell tolerance and the line multiset as `VerdictSql` forms (T3's rows). A grid whose schema the planning
   cannot declare (a dynamic pivot, a late-bound grid: `JudgePlans.tabular`'s keyed forms) stays the walk's, by name.
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
