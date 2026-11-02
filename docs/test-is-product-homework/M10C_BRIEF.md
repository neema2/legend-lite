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
3. **The lineage lines (49 + the database's). BUILT 2026-11-02: DuckDB host pass moved 2,690 -> 2,744, database
   2,662 -> 2,716; H2 host 2,690 -> 2,744, database 2,595 -> 2,649; outcomes identical on every pass, the rosters
   byte-identical.** `LineageTreeLines.asLines` rewrites
   the assert into an ordinary collection equality (the golden's lines a String collection literal; ours the prelude's
   `meta::lite::lineage::relationTreeLines` over the scan's handle, whose rows ride the query as inline relations and
   join the metamodel tables): the walk's `adjudicate` continued with the rewritten assert where `planHost` refused the
   shape outright -- now `planHost` plans the rewritten assert recursively, reported under the call the body spells.
   The scan is a read of the SYSTEM store (`BodyClass.anotherWay`: a `scanRelations` or `scanColumns` call is
   `Kind.SYSTEM_READ`), so a body whose only store read is the scan is system-routed (M10a) and its program runs on the
   graph's system database, the one session the walk routes the side to. The new kind decides the ROUTING alone: the
   class rules see a handle as its query (`framesAndHandlesAsTheirQueries`) and never meet the scan's kind, so a scan
   beside a user read is admitted by the class and refused by the planners as before -- the host's session check
   (M9d), the database's `plannable` (admitting the scan in a system-routed body now) -- each a named static rule
   (the audit's correction: the first draft said the side-kind rule kept such a body). The 5 "testAssociationMapping"
   bodies the inventory took for scan-beside-user-read bodies are `meta::pure::lineage::scanColumns::test::*`
   (`testQualifier`, `testAssociationMapping`, `testSubType`, `testEmbeddedMapping`, `testView`): a `scanColumns`
   chain and an ordinary equality over its rows, no user read, refused at the host's session check because the body
   was not system-routed -- system-routed now, they MOVE: the host's +54 is 49 prints + 5. The witness
   (`lineagePrograms`): the print's golden read off a run, equal and differing goldens, an equality over the scan's
   rows, the corpus's `scanColumns` chain -- pinned on both roads under both judges, every boundary the same but the
   walk's one body-session setup statement (the scan's let evaluated on the body's session; the program's one target
   the system database -- the M10a difference `systemRoutedPrograms`' store-free case allows).
4. **A user-written assert helper at the root (20). BUILT 2026-11-02: DuckDB host pass moved 2,744 -> 2,764, database
   2,716 -> 2,734; H2 host 2,744 -> 2,764, database 2,649 -> 2,650 (the struct-less dialect walls the class-value pairs
   at planning: the bodies given back, see (i)); outcomes identical on every pass, the rosters byte-identical.** `assertConversion(expected,
   input)` is a user function whose body is lets then an assert over class values; the walk's `prepareValue` inlines the
   statement with the lets before it (`UserCallInliner.inlineBody`, G½), stages it, reads its root and -- where the
   original is a user call and the root a verdict native asserting a CLASS VALUE or reading a STRING ENTRY
   (`VerdictRoutes.assertsClassValue`, `readsStringEntry`: the corpus's `runLegendTest` wrappers and `assertConversion`)
   -- adjudicates that root; every other helper root is a value. The rule named once (`BodyDriver.isHelperAssertRoot`,
   `helperAssertRoot`: a cheap necessary condition first -- the callee's body reaches an asserts-package call or a
   string entry's read -- then the same inlining, the reduced root or null) and shared by the program road through one
   predicate (`isHelperCall`): `planHost` plans the reduced root recursively, reported under the helper's own name;
   `databaseGate` and `planDatabase` reduce the helper once up front and gate and adjudicate the reduced assert under
   its own name (the walk's `batched` opens the batch root for the reduced assert under the DATABASE judge). The report's
   LABEL under the HOST judge changes with this item, deliberately (the audit's reading): the walk's `tryAdjudicate`
   names a statement's verdict by its root, and since `planHost` plans the helper the walk takes that plan first, so
   both roads report `x::assertSame` where the walk's `prepareValue` reported the reduced root's `assertEquals` before
   -- as the lineage-lines and dual-golden rewraps; the census family (`adjudicated assertEquals`) is the reduced
   root's on both roads under both judges, unchanged. The witness (`helperAssertPrograms`): a keyed class `x::Key` and a helper
   `x::assertSame(expected, actual) { let a = $actual; assertEquals($expected, $a); }`; keys equal (held), keys differing
   (failed), after a let -- on both roads under both judges, the outcomes pinned absolutely on DuckDB. Two things the
   witness found and pins as the walk's own until M10d: (i) on a dialect without struct and list encodings the keyed
   pair walls (`DialectCapability`) on both roads alike, raised by the root loop at the flush; under the DATABASE judge
   the planning pass defers the row (nothing rendered: PLANNED, the family counted), then the segment's close finds no
   member the dialect spells (`VerdictBatch.plan`'s split rung catches the capability; `DatabaseAsserts.close`: no step)
   and gives the body back to the walk, which counts the family again -- M8's note in `close` ("every member's canon the
   dialect cannot spell ... M8 carries the refusal as a step"), the same path a DIRECT class-value `assertEquals` takes
   on such a dialect (pre-existing; on H2 the 22 toPostgresModel tests fail on both lanes already) -- M10d, where a
   planning failure is the body's outcome (R14), carries it as the step; (ii) under the DATABASE judge a helper whose deferred verdict fails is ANSWERED
   `true` by the walk (`prepareValue` streams the inlined assert's value as the statement's value before the batch's
   flush judges it) where a statement-root assert and the program road answer nothing -- a quirk the walk's deletion
   removes. The audit (COMMIT WITH FIXES, folded): the naming rationale and (i)'s mechanism as above; the helper
   reduced once per road (seven inline passes before); one predicate for the three sites; the pre-check.
5. **The quantified forms (18). BUILT 2026-11-02: DuckDB host pass moved 2,764 -> 2,782, database 2,734 -> 2,752; H2
   host 2,764 -> 2,782, database 2,650 -> 2,668; outcomes identical on every pass, the rosters as registered.** The walk's `adjudicate` meets three spellings
   (`xs->map(x| <asserts>)`, the same under distinct/true wrappers, `xs->forAll(x| <assert>)`) and two forms: the
   VECTOR form (the predicate vector the database computes over the source's elements, judged on the host over every
   row: the vector contract's `assert`/`assertFalse` over a row source, and the simple predicate with a literal message
   over any source) and the UNROLL form (a literal collection whose lambda carries lets or another assert family, each
   element's lambda body β-reduced to one statement and adjudicated in turn, the statement reported once). The first half
   of the walk's `unrolled` is factored out as `quantifiedShape` (the source through the lets and the substitution
   engine, the lambda's root, the vector contract, the simple predicate) and shared; `planQuantified` plans the vector
   forms as one host check whose one side is the predicate vector and whose judgment is `HostJudgment.AllRows` (every
   row the wanted truth, else the element arm's own words), and the unroll as one host check per statement -- every
   element's reduced root planned by `planHost` as any root, their asks in order, the judgment
   `HostJudgment.Unrolled` judging each element over its slice (entered and counted as the walk counts an element; the
   first failure raising) -- so the statement reports ONE verdict as the walk's `tryAdjudicate` does. `planHost`
   answers them (the walk under the HOST judge now takes the plan first, the same judgment); `databaseGate` admits a
   vector form (the database arm's predicate vector reads nothing) and gates an unroll element by element; the
   planning pass is the walk's own path (one batch root, one row per element). Left to the walk by name: a nested
   quantification, the vector contract's `assertEquals` and tolerance families (the walk's row-by-row judgment), a
   runtime collection the substitution engine does not reduce. The witness (`quantifiedPrograms`): the unroll with
   `assertEquals`, with lets in the lambda, the corpus's pairs (`testCbrt`, `testPad`: held and failing), `forAll`, the
   vector form held and failing, `assertFalse` -- on both roads under both judges; where a dialect walls the predicate
   vector's unnest, the outcome and the verdicts are the walk's (the host road moved, the database road given back:
   item 4's (i)). The independent audit (COMMIT WITH FIXES, folded) found two defects of the WALK's own beside my
   change: the `forAll` spelling of the simple predicate fell through to the generic path (the map spelling judged
   the vector; the Lowerer has no arm for an assert, so a forAll walled) -- `adjudicate`'s forAll arm now takes the
   vector form as the map arm does, on both judges (a behaviour change: a walled shape judges; no corpus body was
   found walled by it, the lanes decide); and the vector contract's `assertFalse` was INVERTED on the host (the
   contract's vector already carries the polarity, `assertAsPredicate` negating an assertFalse's condition, and the
   host arm tested it against the family's truth where the database arm reads every row true) -- fixed in the walk's
   `quantifiedRows` and in the planned form alike, witnessed over a row source under both judges. Also folded: an
   engine-text part inside a composite refused in a system-routed body (`holdsEngineText`); one family spelling for
   both vector forms (a NEW census row per vector assert: the walk's arms counted no family; deliberate); the walk's
   `run` judges a composite element by element (fetch, judge, stop at the first failure) as its unroll did, the
   program road reading every element's sides in one step (a named difference on a failing body); the gate reads the
   elements' planned judgments instead of re-planning each; the javadocs. One register row: the generator body
   `testAlloyTestDatGenWithQuotedColumnsForViews` moves on both database lanes (its helper's guard, a `map(assert)` with
   a computed message spliced into the body, is item 5's unroll) and its plan-text assert walls at planning on the
   generator's known gap, the wall carried as a step after the guard's rows -- decided outside a verdict row where the
   walk raised it at the assert's turn (M10b's registered mechanism); FAIL on both roads as before.
6. **The tails.** The dual-golden route null (6: the rewrite to `assertEquals(upgraded, actual)` declines too -- measure
   which arm), the non-constant text sides (5), no callee (2): each measured and named; none moves without a witness.
7. **The spurious system reads (about 130 bodies per lane). BUILT 2026-11-02: DuckDB host pass moved 2,782 -> 2,904,
   database 2,752 -> 2,875; H2 host 2,904, database 2,668 -> 2,781; outcomes identical on every pass, no register changed.** The mixed-bodies
   design note (`MIXED_SYSTEM_USER_BODIES_DESIGN.md`, written at the user's ask) measured the largest walked bucket body
   by body: 136 of the 145 `assert,system-read` bodies are graphFetch tests whose only "system read" is a store element
   named inside `testRuntime()`'s `^Runtime(...)` construction (or an inline Runtime literal), consumed by `execute`'s
   orchestration argument, which the walk reads in source form at planning -- one such body sends zero statements to the
   system session on both lanes (measured). The rule (the note's (A)): `BodyClass.bindsSystemElementsOnly` -- a let
   before the last statement whose value's reads of the system store are element references alone, through the user
   functions it calls and the constructions it builds, and no other read (no system class's `all()`, no system table, no
   system function, no navigation of an element's properties) -- generalizes M10b's direct-element rule; such a let, when
   consumed (`bindingConsumed`), is no system read at the three sites (the routing, the assert program, the frame
   program). A real metamodel read beside a user read keeps the body on the walk until M4 (the note's (B)). The witness
   (`consumedElementReferencePrograms`): the helper-built Runtime, the inline literal, the mapping as a let too, in the
   corpus's `.values` shape (an assert program) and the frame shape (a frame program) -- not system-routed, moved on both
   roads under both judges, zero system statements; a body with no user read not system-routed either; the element's
   navigation consumed inside a frame's lambda no class; a real metamodel read beside the frame stays. The independent
   audit (COMMIT WITH FIXES, folded): the "no other read" half named only the side kinds, which name the REFERENCE inside
   a navigation (`x::DB.schemas.tables.name`) and not the navigation -- a navigation consumed inside a frame's lambda
   would have passed as no read (`BodyClass.elementNavigation` added, the witness above); the first draft's witnesses were
   all frame-shaped while the 136 bodies are `.values`-shaped (the assert program's site: witnessed); the kinds census
   tagged the consumed let the class rules exclude (`BodyKinds` skips it as a handle's let).
7b. **Consumption through a chain and through a helper (the design note's §6.4 rules 1-3; written before the code,
   2026-11-02). BUILT 2026-11-02: DuckDB host pass moved 2,904 -> 2,929, database 2,875 -> 2,898; H2 2,929 / 2,789;
   outcomes identical on every pass, no register changed; stayed 254 -> 231 on the DuckDB host pass; `system=121` on
   both DuckDB passes before and after (no body flipped to the system database).** The run-time census (`MIXED_BODIES_RUNTIME_CENSUS_2026_11_02.tsv`) read 146 of the 204 stayed bodies
   carrying a system-read tag sending NOTHING to the system store: the element is read in source form on the walk and
   the class rule's consumption test is too short-sighted to say so. Three shapes, one analysis
   (`testplan/ElementConsumption`, replacing `bindsSystemElementsOnly` + `bindingConsumed`): a let is an ELEMENT-ONLY
   let when its value mentions a tracked element reference or the variable of an element-only let before it, and
   evaluates no other read (no navigation of an element, no system class's `all()`, no system table, no system function,
   no store write); a binding is CONSUMED when no later statement reads its variable as a VALUE -- a read being an
   occurrence outside a lambda literal, outside the arguments of a planning-time native (a handle's, a Java routine's,
   and now the three planning-time EFFECT natives `connectionByElement`, `setUpDataSQLs`, `setUpDataSQLsV2`, which read
   the runtime and the model at planning: `NativeFn.Effect.readsArgumentsAtPlanning`), outside an effect's orchestration
   arguments (a connection, a store, a table: a handle's or a tracked element's type), outside a `from()`'s mapping and
   runtime, and outside the argument position of a user call whose PARAMETER is itself consumed in the callee's body
   (the same analysis over the callee, the parameter seeded as carried; the last statement's value is the caller's, so a
   read there is a read; a cycle or an un-typeable callee scores not consumed); and a read inside the value of a CARRIER
   let -- an element-only or an orchestration-handle let whose own binding is consumed -- is no read (rule 1: the walk
   splices the whole chain into the orchestration argument it ends in). The three sites and the kinds census take the
   one predicate. Expected: the runtime-copy chains (F1, 23) move; the `setupTestData` bodies (F2, 48) have their lets
   consumed but stay on their EFFECT HELPER statement (a user call wrapping effects, which no class admits yet: the next
   item, measured first); `from`'s arguments were already read as the context by the frame scan, so F6's 13 are
   re-measured, not promised. Witnesses: a two-let copy chain and a four-let navigation chain on the frame model
   (`.values` shape, an assert program; parity under both judges, zero system statements), the chain's end read as a
   value (system-routed as before), and the analysis's own unit witnesses in `ElementConsumptionTest` (a helper whose
   parameter is consumed through `connectionByElement` and `executeInDb`'s connection; a helper that navigates its
   parameter: not consumed). Not built: the replaceTables navigation (F4, 6 bodies: one reader's special case). The
   independent audit (COMMIT WITH FIXES, folded; GATES item 24 has the five findings): the first draft's "mentions a
   carried variable" was wider than the value-read walk (a let handing the runtime to a planning-time native flipped a
   store-free body to system-routed: fixed, witnessed); the seed forms' data argument is a value, not planning-time
   (fixed, witnessed); the unpinned branches witnessed with the corpus's `setupTestData` whole and an effect's element
   argument; the inherited lambda hole closed (a mapped lambda's navigation is a read); nits.
7c. **The effect helpers unfolded: the seed-form effectful map on the program road (M9's T9 kind), and a subsumed call's
   arguments (written before the code, 2026-11-02). BUILT 2026-11-02: DuckDB host pass moved 2,929 -> 2,978, database
   2,898 -> 2,945; H2 2,978 / 2,836; outcomes identical on every pass (skipped 14 unchanged), no register changed; stayed
   231 -> 184 on the DuckDB host pass. The code found a third blocker the brief did not name: the subsumed call's BODY
   reached by the kinds scan (its assert tagged the config let), fixed in `StatementEffects.reaches` (GATES item 27);
   the audit's folds in GATES item 29.** The probe (`bazel run //spec:consumption_probe -- <fqn>`, a
   measurement over corpus bodies: each statement's element-only and consumed verdicts, the kinds, the class, every
   call's catalog row) read a `setupTestData` body as the walk sees it: the resolver's statement inliner
   (`compiler/StatementInline`, `Compiler.resolveQuery`) has already UNFOLDED the helper into the body -- 13 statements:
   the CSV lets, `let db = Db`, `let runtime = testRuntime($db)`, then the helper's own `let _s7_connection =
   $runtime.connectionByElement($db)->cast(...)` (the qualified property `Runtime$prop$connectionByElement`, a user call,
   not the effect native; a handle by its type), `let _s8_dbConfig = createDbConfig($_s7_connection.type)` (SUBSUMED:
   an opaque typed value never evaluated on any road, `builtin/Subsumed`), `let _s9_sqls =
   $csvs->joinStrings('\n-\n')->setUpDataSQLsV2($db, $dbConfig)`, the statement `$sqls->map(sql |
   executeInDb($sql, $connection))`, `true`, then the frame, the expected text and two asserts. Two things keep it on
   the walk: (i) `connection` is not consumed because the subsumed `createDbConfig($connection.type)` counts as a read --
   a SUBSUMED call's arguments are never evaluated, so they are no read (`ElementConsumption.valueRead`); with that,
   `connection` is a consumed handle carrier and `db` and `runtime` are consumed; (ii) the MAP statement: a map over an
   effectful lambda, the T9 kind the test door refuses (`PlanMaker.unplannedOnTheTestDoor`) and no class admits. The walk
   runs it as `SeedSqlForms.mappedExecutionForm`: the source a seed form (`setUpDataSQLs`, `setUpDataSQLsV2`), its data
   argument folded over the lets (`Literals.fold`: literals joined by `+` and `joinStrings`) or, failing that, evaluated
   as a statement, its store argument a reference's name or null, the texts `CsvSeed.sqls(csv, dbFqn, ctx, dialect)`,
   each text bound into the lambda's body (`UserCallInliner.bindStringParam`) and run as the effect `executeInDb(text,
   $connection)`. The program road plans the same: ONE reader of the seed form for both roads (`testplan/Seeds.seedForm`:
   the folded data and the store's name, or null where the data does not fold -- the walk keeps its statement path
   there, the program road refuses by name, R14), the test door's `TestKinds.effect` accepting an effectful map whose
   source is a seed form and whose lambda body is one db effect, each bound statement prepared as the effect it is and
   added to the open script (one segment, the map's index), and the class rules admitting such a statement where they
   admit a literal effect (`BodyClass.seedFormEffectMap`). Witnesses: the frame model's `x::setupData(csvs, db,
   runtime)` spelled as the corpus's helper (the qualified property, `setUpDataSQLs($db)`, the map over `executeInDb`),
   unfolded by the resolver, under both judges (the table dropped and recreated by the seed: the rows are the CSV's);
   a data argument that does not fold stays; the unit witness of the subsumed argument. Expected: the 48 `setupTestData`
   bodies (modelJoin advanced, simple, milestoning, union) move or name their next blocker (the TDS `toString` compare,
   the `->sql()` text); measured, not promised.
8. **Not this step:** M10b's S1 bodies (an engine-text assert in a system-routed body: 9 -- M10d, where the walk's two
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
