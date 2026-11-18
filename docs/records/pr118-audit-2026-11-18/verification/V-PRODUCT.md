# V-PRODUCT verification

All `path:line` are at the PR head `68f20eb4d` (worktree `scratchpad/pr118`). `core/src/main/java/com/legend/` is
shortened to `cm/`. Nothing was executed (no Bazel, per the brief). Every walk below is from reading the code.

| Finding | Claimed | Verdict | New in PR? | Ruled? | Covered by a test? | Fix size |
|---|---|---|---|---|---|---|
| PRODUCT-1 | lite's `/api/lite/v1/queries` answers malformed bodies `internal`/500 with legend-engine class names; an unknown path is 404 `request` | **FIX-AFTER** (confirmed; the spec is unambiguous) | Yes. The route is new (H6). On main the same 500s existed only at legend-engine's `/api/pure/v1/query`, where they are upstream's own statuses | No user ruling. The code's own cited ruling ("upstream's 500 is the edge's alone") argues against the 500s | Pinned in the WRONG direction: `datacube/test/saved-pages/conformance.ts:123-140` (every host) and `python/tests/test_engine.py:851-853` | ~50 lines in 5 files plus re-pins |
| PRODUCT-3 | legend-pure's "Assert failed" is the product default on two paths; elsewhere lite says "expected true, got false" | **FIX-AFTER** (confirmed; one of the auditor's two inputs is wrong) | The split is new: main said "Assert failed" on every path. The literal is main's | Orchestrator STAYS (J29, 2026-11-18), not the user's. That ruling contradicts the PR's own word tables | No. `CompatSpellingGuardTest`/`TerminalRowsFrozenTest` freeze the literals | about 5 lines (vector path) plus 20-40 lines (LambdaBodies, two-form) |
| PRODUCT-9 | DB and connection failures answer `data` (422) with a non-raise `raise`; `DataError` drops `where`; compile messages repeat the position | **FIX-AFTER** (all three confirmed) | Yes: `LiteV1Api`, `ErrorKind`, the position field are all new | DATABASE_ERROR and CONNECTION_ERROR as kinds: the user ruled them (2026-11-16). Their wire mapping: no ruling | The duplication is pinned: `engine-client/test/fixtures/lite-api-cases.json:431-436`. The other two are not covered | ~25 lines plus fixture re-pins; the wire kind needs the user |
| JUDGES-2 | `assertEq`/`assertNotEq`/`assertIsNot`: the host compares by carrier kind, the database judge by declared kind | **FIX-AFTER**, borderline (confirmed by reading) | Mixed. For `assertEq`/`assertIs` it is main's. For `assertNotEq`/`assertIsNot` the PR introduced a NEW G2 split (on main both judges read one database Boolean) | S127 claims the two judges "agree by construction"; this contradicts it. No ruling covers it | No test has the shape. The BOTH lanes would flag it if one did | 3 lines (`eqOf` typed by declaration) plus about 25 lines of test |
| JUDGES-1 | a raising `let` is held differently by the two judges; the PR's B1 fix added `let <model error>; <effect>; assert` | **FIX-AFTER** (confirmed) | (i) main's, narrowed by the PR. (ii) new in the PR | None | No. `DatabaseHalfDe5Test` pins B1's two other shapes, not the effect one | ~6 lines in `DatabaseAsserts` plus about 15 lines of test |
| LOWERSQL-3 | WHERE conjunct order moved from zone order to encounter order, unmeasured | **FIX-AFTER**, javadoc and a pin only. The wrong-answer concern is **REFUTED** | Yes (L10, `61e5205d1`) | None needed for values. A register row is optional (the move is toward Pure, away from legend-engine's text) | No. Lean SQL counts are order-blind, and the temporal tests use `contains` | 2 lines of javadoc plus a 20-line SQL pin and an optional raise probe |
| COMPLETENESS-2 | the prelude ships legend-pure's print bodies; the product writer prints a Pair `<a, b>` | **FIX-AFTER** (confirmed; one citation wrong) | No: main's (the PR moved List and Map to JSON and left Pair) | No ruling names Pair. The print-form table says "Objects ... are refused", and S363's reason ("the same on every output") is broken: CSV `<1, a>` against JSON `{"first":1,"second":"a"}` | Pinned in the WRONG direction: `ResultWritersTest.java:177-183` | ~60-100 lines (the edge must take legend-pure's Pair/List text for PCT); needs a user ruling |
| ARCH-9 | the JVM property `legend.exec.engineScanOrder` changes planned SQL outside the style and the cache key | **NOTE**. A product run cannot be affected (refuted for the product) | No. Moved from the product's DuckDB (main `sql/dialect/DuckDb.java:739`) to the edge's: an improvement | User ruling 2026-09-20 (order emulation is test-lane only) | Yes: `TestLaneOrderGuardrailTest.emulationIsInstalledOnlyBehindTheSwitch` pins the switch to the edge's DuckDB | ~15 lines to make it a corpus style |

**Blockers confirmed: none.** JUDGES-2 is the only one that meets the letter of "a goal of record broken by this PR"
(G2, for the two new natives). The reasons I do not grade it BLOCKER are in its section. It is the cheapest fix in this
batch: 3 lines, no format change.

## Details

### PRODUCT-1: lite's saved-query route answers malformed requests `internal`/500

**Facts.**
- `cm/server/LegendHttpServer.java:193` mounts `storeRoutes("/api/lite/v1/queries", ...)`. Its wrapper (`:264-300`) maps a
  store refusal as follows:
  - a missing record (`MISSING`, `:249-252`) → `notFound`/404;
  - anything else keeps the store's status, with kind `a.status() >= 500 ? INTERNAL : REQUEST` (`:296`).
- The store's statuses are legend-engine's ApplicationQueryException statuses:
  - `cm/queries/SavedQueries.java:241-245` (`failed`: any `IllegalArgumentException`/`ClassCastException` → `MALFORMED`, 500);
  - `:497` and `SavedCubes.java:276` (`NO_SEARCH_TERM`, 500);
  - `:253` (`NO_SUCH_ROUTE`, 404).
- The shape checks throw with legend-engine's class names: `SavedQueries.java:626-660`, `SavedCubes.java:245-284`
  (`QuerySearchSpecification`, `QuerySearchTermSpecification`, `DataCubeQuery`, ...).

**Walk.** `POST /api/lite/v1/queries/dataCube/search` with body `{"limit":"ten"}`:
1. `LegendHttpServer:193` → the lite wrapper `:283` → `storeRoutes(..., LITES_WORDS)` `:321-326` → `SavedCubes.answer`.
2. → `SavedQueries.served` (`SavedCubes.java:86`) → `searchShape` (`SavedCubes.java:282`) throws
   `IllegalArgumentException("QuerySearchSpecification.limit is not a number")`.
3. → `served` catches it (`SavedQueries.java:224-229`) → `failed` → `MALFORMED`, 500 → `refused` writes
   `{"kind":"malformed","message":"the request could not be served: QuerySearchSpecification.limit is not a number"}`.
4. → back in the wrapper: `malformed` is not in `MISSING`, status 500 → kind INTERNAL.

Output: HTTP 500 `{"kind":"internal","message":"the request could not be served: QuerySearchSpecification.limit is not
a number"}`.

Other inputs:
- `{"searchTermSpecification":{}}` → `NO_SEARCH_TERM` 500 → `internal`.
- `GET .../dataCube/a/nothing` → `NO_SUCH_ROUTE` 404 → kind `request` with status 404.

**Should be.** The spec, `docs/records/lite-api-v1-2026-11-17.md` §3, is unambiguous:
- "Every refusal, on every binding, is `{kind, message, where?, raise?}`".
- `request` is "the request itself is malformed: a key it does not read, a wrong type, ...".
- `internal` is "anything else: a defect, said as one".
- "Over HTTP the status follows the kind: 400 for ... `request`".
- §2's "`queries/...` | as today" governs the records ("the same store and the same records"), not the refusals.

So both bodies should get 400 `request`, worded with the request's own field names, not legend-engine's class names.
The unknown path should be 400 `request`, as `LiteV1Api` answers an unknown operation (`LiteV1Api.java:184`,
`MalformedRequest`). The code's own comment (`LegendHttpServer.java:247`) quotes the orchestrator's ruling "upstream's
500 is the edge's alone", applied to only one of the cases.

**New?** Yes. On main the only mount was legend-engine's `/api/pure/v1/query` (main `LegendHttpServer.java:144`), whose
500s are upstream's own. The PR added lite's route and moved DataCube onto it (`datacube/src/cube-store.ts:3-4,193`).

**Covered?** Pinned in the wrong direction:
- `datacube/test/saved-pages/conformance.ts:123-140` ("answers a body of the wrong shape with lite's internal
  refusal"), run on every host;
- `python/tests/test_engine.py:851-853` (404 `request`).

Probe: flip conformance.ts:126-138 to `assert.equal(r.status, 400); assert.equal(r.json.kind, 'request')`.

**Grade: FIX-AFTER.** A valid request never gets a wrong answer. The malformed one is still refused, and its message
names the problem. Only the kind and status are wrong, and no format or plan field is involved.

**Fix (~50 lines).**
- `SavedQueries`/`SavedCubes`: split `failed` into a shape refusal (400; its message names the request field, not the
  class) and a disk failure (500). `NO_SEARCH_TERM` → 400.
- `LegendHttpServer.storeRoutes` (lite wrapper): classify by the store's kind, not its status.
- `edge/StoreWords.java:90-106`: give the edge its own status map, keeping legend-engine's 500s at `/api/pure/v1/query`.
- Re-pin the conformance suite and the Python test.

### PRODUCT-3: "Assert failed" against "expected true, got false"

**Facts.**
- `cm/compiler/spec/VerdictQueries.java:154-158`: `assertMessage` returns "Assert failed" whatever the style. It is used
  by the host vector path (`cm/testplan/AssertVerdicts.java:1488-1489`) and the database vector path
  (`cm/testplan/DatabaseJudge.java:927`), so the two judges agree here.
- The simple-predicate path (`AssertVerdicts.java:1503-1505`) and the quantified path (`:1567`) use
  `defaultConditionMessage` → `DatabaseJudge.words(env).conditionFailed` → `cm/lowering/LiteVerdictWords.java:172-173` →
  `AssertSentence.TRUE` "expected true, got false". The edge's `UpstreamVerdictWords.java:184-186` says "Assert failed".
  So in this PR's own word tables "Assert failed" is legend-pure's form.
- `cm/compiler/spec/LambdaBodies.java:128-131`: `assert(c); rest` in a multi-statement lambda becomes
  `if(c, |rest, |fail('Assert failed'))`.

**Walk 1, test road (vector contract).** Body `{|[1, 2]->zip([2, 1])->forAll(p | assert($p.first < $p.second))}`:
1. `quantifiedShape` (`AssertVerdicts.java:1239-1308`): the source is a zip of literal arms, so `rowSource`
   (`VerdictQueries.java:161-170`) holds.
2. The predicate is row-local and there is no message, so `literalMessage` holds and `vectorPredMap != null`.
3. → `:1481-1489` → message "Assert failed".

The same assert over a literal list, `{|[2, 1]->forAll(x | assert($x < 2))}`, is not a row source. It takes the simple
path (`:1502-1505`) and fails with "expected true, got false", as a plain `{|assert(1 == 2)}` does
(`AssertVerdictsTest.java:97`).

**Walk 2, product road (a `query/run`).** The auditor's example, a model function `f(x){ assert($x > 0); $x; }` with
`|f(-1)`, is **wrong**. A function body is typed statement by statement (`SpecCompiler.java:229-272`), not through
`LambdaBodies`, so the assert survives as a call. The product's query door then refuses it by name ("an assert in a
query: asserts are a test's ..."; `QueryDoorTest.anAssertInsideAUserFunctionIsRefused`, `QueryDoorTest.java:170-178`).

A product query that does reach `LambdaBodies`, modelled on `PostgresDialectTest.java:233`:

```
|#TDS
  x:Integer
  -1
#->extend(~y: r|if($r.x > 0, |$r.x, {|assert($r.x == 0); 0;}))->from(<runtime>)
```

1. `IfChecker.java:67-69` → `thunkBody` (`:133-139`) → `LambdaBodies.synthBody` → `assertThenValue` (`:129-131`)
   produces `if($r.x == 0, |0, |fail('Assert failed'))`. No assert call is left in the typed tree.
2. The query door's `isAssert` excludes `fail` (`cm/planner/QueryDoor.java:236-240`), so the query is planned.
3. `fail` lowers to a `USER_FAIL` raise whose operand is the message (`cm/lowering/Scalars.java:267-276`). A user's fail is
   never a site, so the message is the user's text (`UserFailIsNeverASiteTest`).
4. The run raises `DataError("Assert failed")`. Lite's API answers `{kind:"data", message:"Assert failed"}`.

**New?** On main every path said "Assert failed" (main `AssertVerdicts.java:1485,1547`, `HostJudgments.java:492`,
`VerdictQueries.java:318`, `LambdaBodies.java:130`). The PR introduced lite's sentence on two paths and left two. The
inconsistency is new; the literal is main's.

**Ruled?** `CompatSpellingGuardTest.java:148-155` and `TerminalRowsFrozenTest.java:62-67`: "RULED STAYS by the orchestrator
(2026-11-18) ... the orchestrator's, not the user's". That ruling calls "Assert failed" lite's own sentence. The PR's
`LiteVerdictWords` and `UpstreamVerdictWords` say the opposite.

**Probes.**
- `QuantifiedRowsTest`: `assertEquals(run("{|[2, 1]->forAll(x | assert($x < 2))}") message, run("{|[1, 2]->zip([2,
  1])->forAll(p | assert($p.first < $p.second))}") message)` fails today.
- A `UserFailIsNeverASiteTest`-style test runs the TDS query above and asserts the `DataError`'s message. Today it is
  "Assert failed".

**Grade: FIX-AFTER.** A message text, not a value or a verdict.

**Fix.**
- `VerdictQueries.assertMessage(root, VerdictWords)` and its 2 callers (about 5 lines).
- `LambdaBodies` must stay style-free, because the edge needs legend-pure's "Assert failed". Raise an assert kind
  worded per style, through a two-form kind (20-40 lines). Or the user rules that "Assert failed" is lite's sentence too.

### PRODUCT-9: lite's error shape

**Facts and walks.**
1. **Database and connection failures.** `cm/server/LiteV1Api.java:404-407` maps every `DataError` to `DATA` (422) with
   `raise: lowerCamel(kind)`.
   - A database's own failure: `cm/exec/RaisedErrors.java:263-265` builds `DataError.database(DATABASE_ERROR, ...)`, which
     answers `{kind:"data", message:<the database's text>, raise:"databaseError"}`.
   - A lost connection (`RaisedErrors.java:284,299`) answers `raise:"connectionError"`.
   - An oversized load (`exec/DataLoads.java:58`) answers `raise:"fileTooLarge"`.
   - `ErrorKind.isRaise()` is false for all three (`cm/error/ErrorKind.java:52-66`), and `sql/RaiseKind` has none of
     them. The spec (§3) says `raise` is "one name of a closed list ... the product plan's raise-site kinds" and that
     `data` is "a run's raise". A lost connection is `internal` by the spec.
   - The kinds themselves were ruled by the user (plan of record §12, 2026-11-16 21:20, item (4)). That ruling also put
     the database's message "in a separate field", and `refusal` writes no such field. The wire mapping is unruled.
2. **`where` dropped.** `cm/error/DataError.java:57-62`: the planned-site constructor takes `where` and keeps no field
   or accessor for it. `RaisedErrors.java:256-257,273-274` pass `site.where()` into it, and `LiteV1Api.refusal` writes
   `where: null`. The where text survives only inside the message (`ExecutionPlan.Site.sentence`, "sentence: where").
   The spec asks for `where` "where there is one".
3. **Position repeated.** `cm/error/LegendCompileException.java:43,70`: the message is `position + " " + sentence`. The
   refusal adds `where = position` (`LiteV1Api.java:395-397`). This is pinned in
   `engine-client/test/fixtures/lite-api-cases.json:431-436` (`"[1:1] Unknown type: ..."`, `"where":"[1:1]"`).

**New?** Yes. Main has no `LiteV1Api`, no `ErrorKind`, and a two-constructor `DataError`.

**Probes** (a `LiteV1ApiTest` method each):
- `assertEquals(LiteV1Api.Kind.INTERNAL, LiteV1Api.refusal(DataError.database(ErrorKind.CONNECTION_ERROR, "08001", "gone",
  "gone", new RuntimeException())).kind())`. Today the kind is DATA, with `raise` "connectionError".
- `assertEquals("p", LiteV1Api.refusal(new DataError(ErrorKind.DIVIDE_BY_ZERO, "p", "division by zero: p",
  null)).where())`. Today it is null.

**Grade: FIX-AFTER.** These are classification and shape on refusals only.

**Fix (~25 lines).**
- `refusal`: `isRaise()` kinds → DATA with a `raise`; `isDatabases()` → INTERNAL (or a new kind, after a user ruling),
  writing `sqlState` and the database's message as fields.
- `DataError` keeps `where`, and the refusal writes it.
- A compile refusal writes the sentence without its position prefix (re-pin the fixture).

### JUDGES-2: carrier kind on the host, declared kind on the database judge

**Facts.**
- `cm/testexec/HostJudgments.java:563-575` (`eqOf`) → `cm/testexec/PureAsserts.java:56-75` (`eqHolds`) →
  `Equality.same(Typed.of(e), Typed.of(a))`. Kind null means `effectiveKind` is the carrier's (`Equality.java:66-68,
  125-133`).
- `sameElementsOf` and `equalsOf` use `typedSide(values, j.e())`, the declared kind (`:252-255,630,816-817`). So the
  host disagrees with ITSELF across `assertEq` and `assertEquals`.
- Host plans for the new natives: `AssertVerdicts.java:990-992` (`negated(planEq)`). `planEq` has no kind gate
  (`HostJudge.java:140-147`).
- Database judge: `AssertVerdicts.java:653-666` → `DatabaseJudge.databaseVerdict(name, false, ...)` → the static gate
  `DatabaseJudge.java:91-101` (declared kinds differ: `wantEqual ? fail : ok`).

**Premise check** (the carrier really is a Long):
- The slot is the wire: `sql/SqlTyping.java:194-209`, and the slot comes from the lowered output (`ColumnFacts.java:63`).
- A cell decodes by its slot: `exec/JdbcDecode.java:258-275`, INTEGRAL → Long.
- Only a Float-declared root is converted: `lowering/Fold.java:1399-1410`, "A Number- or Decimal-declared root keeps the
  wire kind".

So a `Decimal` property mapped to an `INTEGER` column arrives as a `Long`. (A Float-declared one would be converted, so
Decimal is the case that reaches this.)

**Walk.** Model `Class m::P { id: Integer[1]; d: Decimal[1]; }`, mapped to `T (ID INTEGER, D INTEGER)` holding row
(1, 1). Body `{|assertEq(1, <one P>.d)}`:
- HOST: `eqHolds(Long 1, Long 1)` → INTEGER = INTEGER → holds → **PASS**.
- DATABASE: Integer against Decimal → "kinds differ (Integer vs Decimal)" → **FAIL**.
- S127's rule (different kinds are never equal) says FAIL.

Body `{|assertNotEq(1, <one P>.d)}`: HOST fails ("expected a value other than 1"); DATABASE passes. Under BOTH the
isolated comparison reports a disagreement.

**New or extended?**
- `assertEq`/`assertIs`: main has the same `eqOf` (main `HostJudgments.java:525-536`, `PureAsserts.java:236`).
- `assertNotEq`/`assertIsNot`: on main these were prelude bodies, `assert(!eq($notExpected, $actual), ...)` (main
  `prelude.pure:1298-1306, 1367-1385`). Both judges read one database-computed Boolean, so they agreed by construction.
  `e9363946f` (#28) made them natives with a host arm reusing `eqOf`, which created a new G2 split.
- The product's default judge (HOST, `ExecuteOptions` `:30-34`) answers as on main. Only the DATABASE judge's answer
  moved, to the S127-correct one.

**Ruled?** S127 (`docs/SEMANTICS_REGISTER.md:159`) claims "the two agree by construction"; this falsifies that. No row
covers carrier-kind equality.

**Covered?** No test, PCT or corpus body with this shape was found. The DATABASE and BOTH lanes
(`core/BUILD.bazel:1005-1045`) would catch one.

**Probe** (in `DecimalKindsTest`, using its seeding style): create `T (ID INTEGER PRIMARY KEY, D INTEGER)` with
`(1, 1)`, map `d: Decimal[1]` to `T.D`, and for each of `assertEq(1, $p.d)` and `assertNotEq(1, $p.d)` assert that
`Program.run` under HOST and under DATABASE give the same outcome. Expected today: they differ.

**Grade: FIX-AFTER, borderline.** By the letter of the rubric, the two new natives are "a goal of record broken by this
PR". I do not grade it BLOCKER, for three reasons:
1. No product answer changes versus main.
2. The trigger needs a numeric declaration that differs from its column's SQL kind (Decimal over an integer column),
   and no known body has it.
3. The fix is 3 lines and touches no plan or test-plan format.

If the owner reads G2 strictly, this is the cheapest blocker to clear.

**Fix.** In `eqOf`, call `Equality.same(typedSide(List.of(ee), j.e()).get(0), typedSide(List.of(aa), j.a()).get(0),
rules)`, keeping the non-primitive identity branch (3-6 lines). Then add the probe above.

### JUDGES-1: a raising `let` held differently

**Facts** (all confirmed):
- Host: `cm/testplan/HostAsserts.java:174-176` holds any `RuntimeException`. `:133-139` refuses the body (returns null:
  refused by name) when an EFFECT or LET_EXECUTE follows a pending let.
- Database main pass: `cm/testplan/DatabaseAsserts.java:166-170` holds only `LegendCompileException`. `:172-184` closes the
  segment at an EFFECT with no pending-let check.
- Database static pass: `:510-526`. A `LegendCompileException` with an assert after it admits the WHOLE body
  (`return true`, without looking at the statements between). Any other `RuntimeException` refuses the body.

**Walk (ii).** `{| let p = <a let whose planning raises a MappingResolutionException, e.g. DatabaseHalfDe5Test's
executionPlan over the unmapped p::B>; meta::relational::metamodel::execute::executeInDb('Create Table j1_w(i INT);',
^meta::external::store::relational::runtime::TestDatabaseConnection(type=meta::relational::runtime::DatabaseType.DuckDB),
0, 1000); assertEquals(1, 1);}`
- HOST: pending let, then the EFFECT → null → refused by name ("the program road does not plan this body").
- DATABASE: the static pass admits it. The main pass holds the let's error, sends `CREATE TABLE` at the effect step,
  and raises the `MappingResolutionException` at the assert.
- BOTH: `TestPlanner.planIsolated` (`TestPlanner.java:81-95`) plans HOST first, so it refuses like HOST.

The two outcomes differ only between the HOST and DATABASE lanes. Both fail the body, and the database lane sends a
write that HOST (and upstream) do not.

**Walk (i).** A frame let whose planning raises a `NotImplementedException`:
- HOST raises it at the next assert, after the earlier asserts have reported.
- DATABASE refuses the body.
- BOTH refuses (the database plan throws in `planIsolated`).

**New?**
- (i) is main's: main's static pass refused every raising let (main `DatabaseAsserts.java:474-480`). The PR narrowed it.
- (ii) is new with `776984dd6` (B1, in the PR, not on main). On main both lanes refused the body.

**Product?** No. The product door is HOST, and HOST behaves as on main.

**Probe.** In `DatabaseHalfDe5Test`, the body above on `onTheEdge(UNMAPPED, body, mode)`. Assert that every
`JudgeMode` gives the HOST outcome. Expected today: DATABASE differs.

**Grade: FIX-AFTER.**

**Fix (~6 lines plus about 15 of test).**
- In the main pass, return null on EFFECT or LET_EXECUTE while `pendingLet != null`.
- In the static pass, make `anAssertFollows` stop at an EFFECT or LET_EXECUTE.
- Hold the same exception classes on both sides.

### LOWERSQL-3: WHERE conjunct order

**Facts.**
- Main applied `lowering/WhereMerge` unconditionally: user, then correlation, then temporal (main `WhereMerge.java:33-52`).
- The PR's product appends in encounter order (`cm/lowering/ProductLoweringForms.java:110-123`;
  `Lowerer.java:2326-2340`).
- The stamps:
  - `resolver/TemporalFrame.java:1593,1998` (TEMPORAL, the inner filter);
  - `resolver/Substitution.java:2819-2824` (CORRELATION, then the member predicate NONE).
- The edge keeps the zones.
- Commit `61e5205d1` claims "no product SQL moved in the measured corpus". The corpus measures counts, which are
  order-blind.

**Can it change an answer?** No value can change: AND is commutative under SQL's three-valued logic. Order can matter
only when an operand raises.
- **Temporal operands are raise-free.** They are column comparisons against a literal or a typed parameter.
- **A computed temporal date that raises** (e.g. `Person.all('x'->parseDate())`): Pure evaluates the argument eagerly
  and raises. The PR's temporal-first order raises too; main's could skip it.
- **Correlation operands** are verbatim key equalities (`Lowerer.java:2215`).
- **A correlation that itself raises** (a join written with a raising dynaFunction): the PR evaluates it before the
  user predicate, as Pure's navigation does (the join decides the related rows before the predicate sees them).

In every case I could construct, the PR's order is Pure's evaluation order. The user's predicate is never evaluated on
more rows than on main.

**By database:**
- **Databases that honour written order with short-circuit:** SQLite; MySQL and MariaDB, after their index and range
  conditions; ClickHouse with `short_circuit_function_evaluation`, though PREWHERE can move cheap column conditions first
  anyway; H2, modulo its optimizer.
  - On these, the PR can only remove a spurious raise. Example: a milestoned row outside the date with `x = 0`, and
    `Person.all(%2020-01-01)->filter(p|10/$p.x > 1)`. Main can raise DIVIDE_BY_ZERO; the PR answers the in-range rows.
- **Databases that reorder by cost or adapt at run time:** Postgres (`order_qual_clauses` sorts by cost, written order
  only breaks ties); DuckDB (pushdown plus runtime adaptive filter reordering); Trino; Snowflake; Databricks; BigQuery
  (documents no evaluation order); MongoDB (`$and` short-circuits but the planner may use indexes first).
  - On these, neither order guarantees anything, and a guarded raise is the CASE's job. The PR is at worst neutral (a
    tie-break toward the cheap structural predicate).
- **Row order:** an unsorted query's row order could only move through a tie-break of a plan choice. The product
  promises none (the 2026-09-20 order ruling).

The auditor's reading holds: this can only remove spurious errors.

**Grade: FIX-AFTER** (small): the javadoc `ProductLoweringForms.java:110` ("conjunct order is no answer's") is
inaccurate, and the product order is unpinned. The wrong-answer concern is refuted.

**Fix.**
- Reword the javadoc: "AND commutes in value; a raising operand is guarded by its own CASE; inner (source) filters come
  first, as Pure evaluates them."
- One SQL-text pin each for the temporal and EXISTS-correlation forms (about 20 lines).
- Optionally, the example above as a `//core:product_raise_probe` case on SQLite and MariaDB.

### COMPLETENESS-2: legend-pure's Pair, List, SQLNull and TDSColumn print bodies

**Facts.**
- `core/src/main/resources/com/legend/builtin/prelude.pure` has these bodies:
  - `:16-24` (Pair `<first, second>`);
  - `:31-38` (List `joinStrings('[', ', ', ']')`);
  - `:3540-3550` (SQLNull);
  - `:4950-4975` (TDSColumn).
- They are identical on main. Only TDSNull's body was deleted (J27).
- `cm/result/CellText.java:89-94` prints a Pair as `<first, second>`, while List and Map go through the forms (JSON).
  The JSON writer prints the same Pair cell as `{"first":1,"second":"a"}` (`CellText.java:236` → `structure` `:166-173`).
- So one Pair cell has two product forms: CSV and Arrow PURE text `<1, a>` (`CsvWriter.java:51`, `ArrowWriter.java:431`),
  against the JSON object.
- A `toString` in a product query runs the prelude body (`MongoDbDifferentialTest`'s `pair_to_string`, `'(' + [pair('a',
  'b'), pair('a', 1)]->map(p | $p->toString())->joinStrings(',') + ')'`), giving `(<a, b>,<a, 1>)` computed in SQL.
- `ResultWritersTest.java:177-183` pins `<1, a>` and `<<1, a>, 4>`.
- Citation error: `LiteralsParityTest.java:75` is a `joinStrings('<', ', ', '>')` fold, not a Pair.

**Against the rulings?**
- No row names Pair. The user's product print forms (`docs/TOTAL_WALL_START_HERE_2026_11_11.md` §5.4) list "list: a
  JSON array; map: a JSON object", and say "Objects and relations are refused".
- A Pair is an instance of a platform class, so the product should refuse it or give it JSON. It should not use
  legend-pure's `<a, b>`.
- S363's reason ("the product's own definition, the same on every output; legend-pure's forms stay at the edge") is
  broken by CSV against JSON.
- The prelude bodies also break AGENTS.md's world-map tenet ("no other Pure bodies"). That predates the PR.

**New?** No, it is main's. The PR completed List and Map (K13) but not Pair.

**Probe** (`ResultWritersTest`): `assertEquals(Json.toCompact(CellText.pureJson(pair, col)), CellText.pure(pair, col))`
fails today (`<1, a>` against `{"first":1,"second":"a"}`).

**Grade: FIX-AFTER.** This is an incomplete move, not a regression. It needs the user's choice of Pair's product form.

**Fix (~60-100 lines).**
- The Pair arm → `dates.structure(m, ...)`, with legend-pure's `<a, b>` moved into `edge/Presenter.LEGEND_PURE`.
- Delete the four prelude bodies, giving the edge legend-pure's `toString` texts. PCT needs them.
- Re-pin `ResultWritersTest` and the MongoDB case.

### ARCH-9: `legend.exec.engineScanOrder`

**Facts.**
- `cm/edge/PureText.java:364-369`: only the EDGE's DuckDB (`PureText.DuckDb`) reads the property, to install
  `StableScanOrder`.
- The product's `sql/dialect/DuckDb.java:745-747` returns no scan-order passes. On main the switch was in the product's
  own `DuckDb.java:739`.
- It is set only by `spec/corpus.bzl:13` and the corpus harnesses (`CorpusOn.java:25`, `CorpusOne.java:30`).
- `TestLaneOrderGuardrailTest.java:72-92` pins that it is named, read and installed only in the edge's DuckDB, and set
  nowhere in main code.

**Can a product run be affected?** No. A product plan uses `PlanStyle.PRODUCT`, and so the product's DuckDb, which never
reads the property. Even a server JVM launched with the flag plans the product identically. Only an edge (legend-engine
or corpus) plan on DuckDB would change.

**Grade: NOTE.** The residue is edge-only design debt: the edge's plan depends on a JVM global the plan-cache key does
not see. It is set at JVM start, except by two corpus test harnesses, so no product exposure. The PR improved this
(the switch left the product).

**Fix (optional, ~15 lines).** Make it a component of `PureText.CORPUS`, chosen by the corpus door, and re-pin the
guardrail.
