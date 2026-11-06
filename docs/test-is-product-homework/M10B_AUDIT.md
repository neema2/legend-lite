# M10b audit — the engine-text asserts: the test in the plan on every lane, the appeal a harness (2026-11-02)

The independent audit of the test-is-the-product plan's M10b (2026-11-02), as delivered on the working tree before the folds the plan's §9 "M10b" records; its line numbers are that tree's, not the commit's.

Independent read-only audit of the uncommitted working tree on `claude/wizardly-dirac-rw6tim-seam` (HEAD 30a0136f; 11
files, `git diff --stat`: 524 insertions, 108 deletions), against `docs/test-is-product-homework/M10B_BRIEF.md`, its code
map (aec8b44b's lines) and the plan's §1 rulings. No bazel, no state-changing git. Every line number below is the WORKING
TREE's as it stands after the coordinator's `decide` fix (STV = `core/src/main/java/com/legend/driver/SqlTextVerdicts.java`,
AV = `AssertVerdicts.java`, DA = `DatabaseAsserts.java`, JP = `JudgePlans.java`, BC = `core/src/main/java/com/legend/testplan/BodyClass.java`,
PT = `core/src/test/java/com/legend/driver/BodyRouterParityTest.java`).

## Findings

### BLOCKER

**B1 — found by the corpus, fixed before commit (record it honestly).** `SqlTextVerdicts.decide` tested `env.engineText() != null`
alone; `Collected.env` (STV:201) is the arm's environment and still carries the `Collecting` object, so `judge` (STV:171-186) →
`decide(c.env(), …)` re-collected at the verdict step and returned `ok()` without judging. The HOST WALK was equally affected:
since M10b `adjudicate` under the host judge tries `planHost` first (AV:276-283) and `planHost` now plans the engine-text
shapes (AV:744-756, 791-804), so walk and road were both vacuous and the walk-vs-road witnesses passed. Corpus evidence (the
coordinator): 2 referee replays instead of 1,188; 7 expected-failing tests passed. The tree now reads
`if (collector != null && collector.planning())` (STV:198) with a comment (STV:199-201); I verified the fix and that no other
site consults the collector unguarded (`grep engineText()`: STV:197, JP:386 — the latter guarded by `planning()` at JP:387).
See S2 for the structural fold that makes this class of defect impossible.

**B2 — the let rule un-routes the bodies M10b is meant to move, and the class then refuses them: zero movement of the
brief's 94.** `systemRouted` (BC:194-204) now `continue`s past a let whose value is a system element reference unless a later
statement reads the variable outside a lambda and outside a planning-time native's arguments. For the corpus shape the brief
names (`let mapping = x::M; … assertEquals(golden, toSQLString($q, $mapping, DatabaseType.H2, []))`, the 94
`assert,host-unplanned-assert,system-read` bodies, brief "Today" ¶2) the body is therefore NOT system-routed any more. But
`assertProgram` (BC:475-479) still scans every statement with `sideKind(n, …, systemRouted)` and the let's own value — the
`TypedPackageableRef` — is `Kind.SYSTEM_READ` (`anotherWay` BC:632-636 via `systemElementReference` BC:650-653), which
`sideKind` (BC:666-680) returns as a refusing kind whenever `systemRouted` is false. `frameProgram` refuses the same let at
BC:368-372; `queryProgram` refuses an assert; `effectProgram` does not apply. Result: those bodies fall from
`host-unplanned-assert,system-read` (admitted by the class, refused by the planner) to `assert,system-read` (refused by the
class) — a tag change with no movement, and M10a's routing of them is undone. The brief's point 5 ("The class rule otherwise
changes nothing") is contradicted; nothing in the brief asked for this rule. Witnesses do not cover it (PT:727-734 only asserts
the census carrier is no longer system-routed; it stays for another reason). Fix (either): (a) drop the let rule — M10a's
routing stands, the program runs on the system database and the appeal's rows side routes itself to the user session through
`planHostSide`/`openRoute` as the walk's does (but see S1); or (b) keep it and apply the SAME let-level predicate where
`sideKind` is consulted (BC:475-479, 368-372, `BodyKinds` 127-134): a let-bound element whose variable is consumed only by
planning-time natives is no kind, so the body moves on the USER runtime (no system database needed — the better design).
Confirm on the DuckDB histogram the coordinator already has: if `host-unplanned-assert,system-read` → 0 while `assert,system-read`
grew by ≈94 and `body-router moved` did not grow by them, this is why.

### SHOULD-FIX

**S1 — the appeal hands the referee the SYSTEM session for a system-routed body.** `BodyRouter` plans a routed body's asserts on
`env.withSession(systemRoute.session())` (`BodyRouter.java:72-81`); `Collected.env` (STV:201) keeps that env; `rowsLegNow`
calls `referee.verify(env.session(), …)` (STV:1606-1618) and `tdgRowsNow` likewise (STV:1211-1216). The harness's
`connectionOf` declines a session the test side did not make (the code map §2.2, RR:898-906) → `oracle-declined` → the text
decides → FAIL, where the walk's `env.session()` is the body's user session → MATCH → pass. Unwitnessed (no system-routed
engine-text witness). Fix: capture the body's user session for the appeal (the env before `withSession`, or
`Collected.env.withSession(userSession)` recorded by the router), or keep system-routed engine-text bodies on the walk by name
until settled. (Moot for the let-bound shapes while B2 stands; live for any body routed by a real `$m` read.)

**S2 — R20: a mutable flag escapes `plan` and is read later.** `Collecting.planning` (STV:85-93) is flipped in `plan`'s
`finally` (STV:152) but the SAME object travels in `Collected.env` into the sink's `HostJudgment.EngineText` and is consulted at
the step (STV:197-198) and by the appeal's every `evalSide` (JP:386-387). Fold: store `env.withEngineText(null)` in `Collected`
(STV:201) so no collector leaves `plan`; then `decide` needs no `planning()` test, `Collector` shrinks to `collect`, and B1's
class of defect cannot recur. `Collecting.collected` as a one-element list with a size check (STV:157-160) can be a single
nullable field.

**S3 — the dual-golden rewrite loses the original listener name on the program road.** `planHost` recurses
`planHost(asEquals, …)` (AV:764-772) and returns that `HostAssert`, whose `listenerName` is `assertEquals`'s FQN;
`judgedAtItsStep` reports `a.listenerName()`. The walk reports `listenerName(bare)` = `assertEqualsH2Compatible` (the recursion
at AV:352-358 is inside `tryAdjudicate`'s one listener call, AV:71-81). Fix: re-wrap the recursive result with
`listenerName(bare)` (keep its family, check, wantEqual). Unwitnessed (PT's dual-golden witness takes the text arm).

**S4 — the HOST WALK's path changed for a text side the compiler cannot fold.** `adjudicate` under HOST → `planHost` →
`plan` → `evalSide` → `NotAConstant` (JP:390-393) → `plan` null (STV:149-150) → `planArm` FALLS THROUGH to
`renderedPlan`/the generic equality (AV:805-826) and the walk runs that in place; before M10b the walk's arm (AV:403-407) ran
with the statement side and its rows appeal. Bounded to non-constant text sides (a TDG carrier not folded to a literal, a text
read by a query); the per-pass `verifyFetchTexts`/`verifyFetchChain`/`plan-params-unbindable` counts are the check. Fix: on
`NotAConstant` make `planHost` answer null outright (the walk keeps its arm; the road refuses by name), instead of continuing
down `planArm`.

**S5 — `databaseGate`'s RAISES proxy counts any earlier assert, not an earlier ROW.** DA:275 refuses a RAISES gate only when
`segmentsWithAssert` lacks the segment; an earlier assert that resolved at planning (`batch.resolve` on an AssertFailed/DataError,
AV:122-125) yields no `Pending`, `VerdictBatch.plan` builds no group for it (VerdictBatch:369-375), and `close` returns false on
`steps.isEmpty()` (DA:376-381) AFTER `planDatabase` counted both roots — the double count the gate exists to prevent. Narrow.
Fix: record `segmentsWithRow` when the gate answers PLANS (or carry a wall-only segment as a step, M8's shape). No un-move of
M7-M10a bodies: a first-of-segment RAISES was "admitted then given back after counting" before, never moved.

**S6 — the witnesses are weaker than the brief's §7 and than the defect they let through.**
(a) `assertParity`/`assertWalkedAlike` (PT:908-930, 804-815) compare walk to road and never assert the EXPECTED outcome; B1
passed them with both sides vacuous. Assert per body: `ok` (equal text, MATCH), `AssertFailed` with the DIVERGED message
(`otherRows`), the raise (`foreign`). (b) The test listener (PT:988-1000) records `verdict`/`unjudged` only; `declined` and
`refereed` — the appeal's whole census — are not compared. (c) The javadoc's "a foreign dialect (declined: the text is the
contract)" (PT:736-742) is a RAISE witness: `EngineText.engineText` throws for Snowflake (`planner/EngineText.java:40-43`); the
decline path (`decide(…, "foreign-dialect:…")`, STV:346) is reachable with `DatabaseType.DB2`/`Composite`/an `unresolved`
runtime and has no witness; nor does `plan-params-unbindable` (STV:1018). (d) No frame witness with a DIFFERING text (the
appeal over `$r.values` on the program road is unexercised), no `executionPlan`/`planToString` SQL-node witness (brief §7), no
TDG witness (acknowledged, PT:1052-1055). (e) Under the DATABASE judge the road reports `declined` at PLANNING
(DA:160 → `planDatabase` → `adjudicate` → `decide` STV:206) and the walk at the assert's turn; for a multi-segment body the
listener's event order differs — unobserved because of (b).

**S7 — records.** The diff carries no document. R12 ("records land with the code"), the brief's residue (the charter §0
sentence corrected when M10b's records land), the plan's §10 status entry, and this step's deviations (§9 below) are due with
the commit. STV's class javadoc (STV:17-42) still says "judges on ROWS, never on text" and links `SqlTextEmission`, a class
that does not exist.

### NIT

**N1 — stale javadocs:** AV:713-727 (`planHost`: "Null where … the engine-text arms"), AV:145, STV:220-223 (`mayArm`: "The host
checks leave such an assert to the walk"), `HostJudge.java:652` ("no environment: the sink holds data" — false for
`EngineText`, which `HostJudgment.java:27-29` correctly names as the one exception), BC:108.
**N2 — a double census tag:** `BodyKinds` already tags `tdg` (`BodyKinds.java:66-67`); `sideKind` now adds `tdg-generator`
(BodyKinds:127-134 via BC:600-602) → every generator body's histogram key changes (`assert,tdg` → `assert,tdg,tdg-generator`),
breaking pass-to-pass comparability. One tag.
**N3 — planning plans before it tests constancy:** `plannedSide` (JP:485-495) → `planHostSide` → `planStaged`
(`BodyDriver.java:1205-1208`, which opens the side's route) runs BEFORE `NotAConstant` is thrown (JP:390-393). No statement is
sent, but a first system-route open could land inside the `StatementOrigin.REFEREE_OURS` bracket (STV:320). Test `foldedSide`
first, or document.
**N4 — the texts are folded three times per assert on the database road:** `databaseGate` (static pass, DA:268) → `plan`;
`planDatabase` → `databasePlansWithoutReading` → `planHost` → `plan` (AV:114, 151-155); then `adjudicate` (AV:120). Cost only.
**N5 — `instanceof` dispatch on `HostJudgment`** at AV:173, 176 and AV:614 beside the one switch in `HostJudge.judge`
(HostJudge:655-676, no `default`, the `EngineText` arm at :675). Acceptable for a gate reading a record's kind.
**N6 — test helpers:** `text(Run)` (PT:822-830) splits on `" row["`/`"] end"` — fine for one-line SQL, fragile for a text
containing `] end`; `pure()` (PT:817-819) escapes `\` and `'` correctly; a text with a newline would need `\n`.
**N7 — pre-existing unused imports** (not this step's): AV `ModelContext`; `BodyDriver` `CrossStoreGuard`, `PlanMaker`,
`QueryParameters`; `HostJudge` `TypedUserCall` (all present at HEAD).
**N8 — `NotAConstant` is exception-as-tri-state** (JP:392 thrown, STV:149 caught, `return null`). The guardrails accept it
(`return null` is a sentinel, `ErrorShapeGuardrailTest.java:280-281`; not a broad type, :195-196); R16's letter is honoured
(a named signal, not a failure). A tri-state answer from `plannedSide` would be cleaner.

## 1. Fidelity of the walk (the three policy sites folded into `decide`)

`decide` (STV:194-214): collector-while-planning → collect, `ok()`; else `declined(env, name, decline)` if a decline;
else active batch → `batch.defer(name, true, VerdictSql.textEquals(golden, ours), env.session(), appeal)`, `ok()`; else
`textEqual ? ok() : appeal.appeal()`.

| Site | Old order (aec8b44b, code map) | New | Verdict |
|---|---|---|---|
| rows tail, 19-param `rowsLegAndVerdict` (STV:1522-1543, call :1541) | batch? defer(row, `() -> rowsLegNow(…)`) : textEqual? ok : rowsLegNow (old :1380-1405) | same order; the same 19-argument closure; AssertFailed from `rowsLegNow` unchanged | byte-identical on the walk |
| TDG fetch text `tryArmTdgSql` (STV:1197-1198) | batch? defer(row, `() -> tdgRowsNow(…)`) : textEqual? ok : tdgRowsNow (old :1028-1043) | same | byte-identical |
| foreign dialect (STV:346-348), plan params (STV:1018-1020) | `declined(...)` THEN `textVerdict`: batch? defer(row, `() -> fail(msg)`) : textEqual? ok : fail(msg) (old :177-181, :850-855, :1704-1714) | `declined` first inside `decide`, then the same batch/text split, the appeal `() -> fail(msg)` with the same `g`/`ours`/`dbType` text | byte-identical; `textVerdict` deleted (grep: no residue in code; two historical docs mention it) |

Under the HOST judge, however, the walk no longer reaches these arms from `adjudicate`'s root/`tryArm` sites: it reaches them
through `planHost` → `plan` → the collector, then `HostAssert.run` → `judge` → `decide` (AV:276-283, 621-628, 612-618;
STV:171-186). Where every text side is a constant this is the same decision in the same order (declined → text → appeal), the
same census (§4) and the same appeal closure; where a side is not a constant see S4. Under the DATABASE judge the walk is
untouched (`adjudicate` skips `planHost` when the mode is not HOST).

## 2. The program road's planning: observable effects before `decide`

Traced every path of `plan` (STV:119-168): `tryArm` (STV:237-398: `findProducer`, `SqlTextInputs.of`, `Lets.bound`,
`UserCallInliner.inlineBody`, `ExecutionContext.reader().read`, immutable `env.withFrame/withTableReplace`, the thread-local
`StatementOrigin.enter(REFEREE_OURS)` bracket :320 restored by try-with-resources, two `evalSide`s, `stripChainedPlanWarning`,
the referee-null WALL :327-333 — a raise, planned by `planHost`'s catch as the assert's raise, as the walk raises it at its
turn), `tryArmSameSql` (:421: `sqlStripRead`, `frameMappingAndClass` through the splice hook — splices, sends nothing),
`tryArmDualGolden` (:493), `tryArmExecRead` (:638: `populationShape`, `inListTemps`, `statementRoute` — pure), `tryArmPlanText`
(:949: `refereeBindings`, `planReplaySql`, the pre-existing `lookThrough` catch), `tryArmTdgRoot/Sql` (:1144, :1171), the
three `rowsLegAndVerdict` overloads (`fromWrapped`, `replayFacts`, `unpagedRead` — tree work). No statement is sent, no
`CanonicalDivergence.*`, no `declined/refereed/provideStore`, no `SqlTypeCensus.probeSuspend` (those live only in
`rowsLegNow`/`tdgRowsNow`, STV:1548-1647, 1202-1245 — the appeal). `JudgePlans.evalSide`'s guard (JP:386-395) admits only
`Kind.CONSTANT` and throws `NotAConstant` otherwise; `plannedSide` itself sends nothing but plans the side first (N3). The
collector is consulted at exactly two sites (STV:197, JP:386). Mutation of shared state: none beyond the `Collecting` object
(S2). `routed` (STV:146-147) is a local, counted at the step only on an answered judgment (AV:614-616).

## 3. The appeal at the step

`HostJudgment.EngineText(Collected, routed)` (`HostJudgment.java:245-250`) holds `Collected.env` = the arm's planning env
(`withEngineText(collecting)` STV:123, possibly `withFrame`/`withTableReplace`, both preserving `engineText`,
`BodyDriver.java:170-213`). At the step: `judge` checks the two constant sides against the planned texts (STV:177-184) and calls
`decide(c.env(), …)` → with the fix the collector branch is skipped (planning false) → `declined` → `c.env().verdictBatch()`
(null on the host road) → `textEqual ? ok : appeal` — correct. The appeal (`rowsLegNow` STV:1548-1647, `tdgRowsNow`
STV:1202-1245) consults `env.engineText()` only through `evalSide` (JP:386-387, guarded). State between planning and the step:
`letPrefix`, the splice hook and the frames are captured as they stood at the assert's turn (M6's design, same as the host
checks); the rows side is the producer's lambda or `$r.values` (the frame's typed chain re-lowered — no stamp slot;
`JudgePlans.run` binds `Map.of()`, JP:337-343, so a side reading a frame's ACTIVITY comment would carry an unbound stamp —
none does today); the session is `env.session()` as captured — the user session on an ordinary body, the SYSTEM session on a
routed one (S1). `Collecting.planning` is the only mutable state; it is scoped to one `plan` call but the object outlives it
(S2).

## 4. `AssertVerdicts.planHost` and the census

Roots: `isRoot(fqn)` (STV:103-107) → `fn = null`, `name = null` → `plan` → `HostAssert(listenerName, family null, …)`;
`enter()` counts no family (AV:605-609), as `adjudicate` counts none before a root arm (AV:326-361). Equality arm: family
`assertEquals`/`assertNotEquals` counted by `enter()` before the sides (AV:621-628) as at AV:376; the `sql-text` route counted
by `HostAssert.judge` only when the judgment ANSWERED (AV:612-618, `sqlTextRouted` :638-640) — `adjudicate` counts it at
AV:406 only after `tryArm` returned, never on a raise: identical on success and failure. Raise inside the arm: `planHost`'s one
catch (AV:758-762) → `HostAssert.raising(listenerName, name, e)` → family then raise at the turn; the walk: family at :376,
the raise out of `tryArm`, `sqlRaised` in `tryAdjudicate`'s finally (AV:91-96); the road: `judgedAtItsStep` counts `sqlRaised`
(AV:703-712). `sqlEnter` once on both. `planHost` is reached on the walk under HOST for these shapes (AV:276-283) and on the
database road from `databaseGate` (AV:170) — the latter discards the check and keeps only the gate's answer (AV:173-183), so
nothing is counted there. The dual-golden recursion (AV:764-772) counts `assertEquals`'s family as the walk's recursion
(AV:352-358) does; the listener NAME differs (S3). `databasePlansWithoutReading` (AV:151-155) now admits the engine-text
shapes (the `EngineText` arm, AV:176-183) — brief 3 ✓.

## 5. `databaseGate` / `everyAssertPlansStatically`

The RAISES refusal (DA:275-281) is right in intent (the planning pass would wall a batch with no row and `close` would give the
body back after `planDatabase` counted, DA:376-381 + AV:117-129). The proxy `segmentsWithAssert.contains(segment)` means "an
earlier assert statement in this segment", not "an earlier row": an earlier UNPLANNED assert never reaches here (the pass
returns false at DA:270-272 — fine); an earlier walled assert stops the planning loop (DA:151-154 — fine, and such a first
wall is itself refused); an earlier assert RESOLVED at planning leaves no `Pending` → S5. Frames/effect boundaries: `segment`
increments on an EFFECT (DA:244-247), and `segmentsWithAssert` is per segment — correct. Un-move of M7-M10a bodies: none; a
first-of-segment RAISES body was admitted-then-returned (counted twice) before M10b, never moved; the shapes affected are only
those.

## 6. `BodyClass`

(a) `NativeFn.JavaRoutine` (`builtin/NativeFn.java:761-777`): `PLAN_TO_STRING(plan, ext)`, `PLAN_TO_STRING_WITHOUT_FORMATTING(plan, ext)`,
`TO_SQL_STRING(fn, mapping, DatabaseType, ext)` (also the lifted derived `SQLResult.toSQLString`, whose receiver `toSQL(fn, …)`
is itself a handle reduced to ITS first argument), `TO_SQL_STRING_PRETTY(fn, mapping, DatabaseType|Runtime, ext)`,
`TO_NON_EXECUTABLE_SQL_STRING(fn, mapping, DatabaseType, ext)`: the first argument is the query or the plan in every member —
`framesAndHandlesAsTheirQueries` (BC:421-428) is right to take `args().get(0)`.
(b) The let rule (BC:194-204) — counter-examples: `let m = x::M; let n = $m; …$n.name` → `$m` is read in statement j=1 → routed
(conservative, correct). `f($m)` → `readsStore` descends the call's children after the callee walk (BC:253-257) → found →
routed ✓; but inside the callee's body the SAME predicate matches any variable spelled like the let (a parameter `mapping` in
a helper) → a false positive (harmless: routes a body that reads no store). `[1]->map(x | $m.name)` → `readsStore` skips every
`TypedLambda` (BC:223-225) → MISSED → not routed → the let is a `SYSTEM_READ` kind for `assertProgram` (BC:475-479) → the body
stays on the walk (it moved at M10a): an un-move, safe but unnamed. Two lets of one name: Pure forbids re-binding; not
reachable. The memo: fresh per statement j (BC:201) ✓; `userMemo`/`systemMemo` keyed by callee with one predicate each ✓
(the M9b audit's F1 rule honoured). The big one is B2: the rule un-routes exactly the shape M10b targets and the class then
refuses it.
(c) `Kind.TDG_GENERATOR` (BC:573-578, :600-602): before, `anotherWay(TypedTestDataGen)` was null and generator bodies were kept
off every class by `StatementEffects.containsTdgGenerator` (BC:320, 475, 503) — so no class relied on the old null;
`frameQueryKind` (BC:688-691) now refuses a generator inside a frame's query (none exist); `queryProgram`'s
`plannedAnotherWay` (BC:558) too (already refused by the generator test). The M9e `TestDatGenForNestedViews` bodies are effect
programs (BC:52-53), whose rule reads no `sideKind` → structurally unaffected; confirm on the corpus. `BodyKinds` double-tags
(N2).

## 7. The witnesses (PT)

`frameModel` adds `sqlRemoveFormatting(Result)`, two `assertSameSQL` overloads and `assertEqualsH2Compatible` under the library's
FQNs (PT:342-353) — the arms detect by FQN and the Result overloads spell the library's body ✓. `engineTextPrograms`
(PT:745-800) covers: equal text under `assertSameSQL(String)` and plain `assertEquals` (no appeal), the same rows under another
spelling (MATCH), other rows (DIVERGED), the frame's SQL under `assertEquals`, `assertEqualsH2Compatible` and
`assertSameSQL(Result)` (all EQUAL texts), the Snowflake RAISE under both judges (host: parity; database: `assertWalkedAlike`).
Not covered from the brief's §7: the foreign-dialect DECLINE, `plan-params-unbindable`, a frame with a differing text, the
`executionPlan`/`planToString` golden, the TDG forms, the listener's `declined`/`refereed` events, the expected outcomes (S6).
`StubReferee` (PT:1052-1127) is an honest DOUBLE of the SPI, not of the policy: it replays the golden on the body's OWN
connection (no seed ledger, no mirror), compares `String.valueOf` cells joined by `|` as SORTED multisets — no types, no column
labels, no order, nulls as the text `null`, a SQL failure a decline; `verifyFetchTexts/Chain` throw. `text(Run)` and `pure()`
are adequate for these one-line texts (N6). `assertWalkedAlike` (PT:804-815) is a sound "moved class, named refusal" witness.

## 8. Guards

`JavaEvalLedgerTest` pins, each with a dated justification (quoted) and each EXACT against the test's own stripped count
(`/* */` and `//` removed, blank lines dropped), recomputed on the tree as it stands:
- `HostJudgment.java` 152 → 154 (:579): "(2026-11-01, M10b): EngineText, the engine-text asserts' judgment (the comparison the
  arm planned, the appeal); nothing evaluated" — measured 154 ✓; accurate (a record).
- `HostJudge.java` 1127 → 1128 (:580): "the EngineText arm of the one dispatch; nothing evaluated" — 1128 ✓; accurate.
- `AssertVerdicts.java` 1470 → 1503 (:582): "the database judge's static gate in three …; planHost plans the engine-text asserts
  … through SqlTextVerdicts.plan; … the equality arm's census route counted once the planned judgment answered; nothing
  evaluated" — 1503 ✓; accurate.
- `BodyDriver.java` 954 → 961 (:832): "ExecEnv carries the engine-text arms' collector … (withEngineText); nothing evaluated" — 961 ✓.
- `SqlTextVerdicts.java` 1254 → 1329 (:1012): "the arms' PLANNING ENTRY … the step's judge, and decide — the one verdict site
  …; the texts are constants and the rows leg is unchanged: nothing evaluated here that was not" — 1329 ✓ (the fix added
  comment lines only); accurate: `judge`'s `c.golden().equals(golden)` is a self-check, `golden.equals(ours)` pre-existed.
`ErrorShapeGuardrailTest`: `catch (NotAConstant e)` is not a broad type (:195-196) and `return null` is a sentinel (:280-281) —
`SqlTextVerdicts.java` 2, `AssertVerdicts.java` 2, `DatabaseAsserts.java` 4 unchanged (planHost's catch and DA's gate catch
pre-date the step). `CodeShapeGuardrailTest`: `plan`/`decide`/`judge` are short; N7's SIG hole still skips annotated returns.
`ArchitectureTest.hostVerdictIsReachableOnlyFromTheVerdictSeam` (:1366-1405): STV references none of
`PureAsserts|CanonicalForm|CanonicalDivergence|Equality`; `HostJudgment → SqlTextVerdicts.Collected → VerdictBatch.Appeal` is
outside the rule's set — no new dependent. `HostJudge.judge` stays a switch expression with no `default` (:655-676). New
`instanceof` dispatch: AV:173, 176, 614 (N5).

## 9. Deviations from `M10B_BRIEF.md`

1. "The arms split in two halves (planX + judge)" → the arm run ONCE under a collector, the comparison handed over
   (STV:52-168). Fair simplification: the walk's arm code is untouched where texts are constants; costs: the collector escapes
   (S2), three folds per assert on the database road (N4), and the walk's HOST path now goes through `planHost` (S4).
2. A sealed `SqlTextVerdicts.Appeal` record of facts → `VerdictBatch.Appeal` closure (STV:66-69). Fair: the brief's record would
   carry the same harness objects; the closure is the walk's own.
3. Witnesses in core's `BodyRouterParityTest` with a stub referee, not spec's `EngineTextParityTest` with `ReplayReferee`. A
   gap by the brief's own reason (the compare policy and the seed ledger are the harness's): the corpus measurement must stand in
   for the policy; the core witnesses need S6's folds to be witnesses of the verdict at all.
4. `HostJudgment.EngineText(name, decline, appeal)` → `EngineText(Collected, routed)`: fair; the texts are re-checked at the
   step (STV:177-184), a good addition.
5. Brief 3's "a text side read from the database is a statement side, admitted by the host road, refused on the database road"
   → refused on BOTH roads by `NotAConstant`. Narrower and fair; S4 is its walk-side cost.
6. Brief 5 "the class rule otherwise changes nothing" → the `systemRouted` let rule (BC:194-204) and the `JavaRoutine`
   reduction in `framesAndHandlesAsTheirQueries` (BC:423-427). The reduction is fair and consistent with `readsStore`; the let
   rule is B2.
7. `DatabaseGate` tri-state and the RAISES refusal (AV:161-206, DA:275-281): not in the brief, found by the witness; fair, S5.
8. Brief 4 (`TDG_GENERATOR`) ✓; the brief's build-time confirmation (TestDatGenForNestedViews still moved) is structurally
   plausible, to be read off the histogram; N2 on the tag.
9. Brief 6 (census identical, the appeal's statements under `REFEREE_OURS`/`REFEREE_GOLDEN`) ✓ by code (STV:1568, 1589, 1606,
   1211); the DuckDB numbers are the proof.
10. Brief "Residue": the charter §0 correction and the class javadoc not done (S7).

## 10. Other

`AssertVerdicts.calleeFqn` widened to package-private (AV:1825) for `plan` — fine. `VerdictQueries.text` (:54-56) is a
one-line mint; fine. NullAway: `Collected.decline` is the only `@Nullable` component and is tested before use (STV:205); the
locals `golden`/`ours` in `judge` (STV:181-182) are nullable-typed locals passed to `equals` — allowed; `HostAssert.family` null
for a root (AV:751) is declared `@Nullable` ✓; `fqn` null in the ISE message (STV:158) is string concatenation ✓. Naming: `plan`
and `judge` on STV mirror the host checks ✓; `decide` is well named as "the one verdict site"; `Collecting.planning` as a
field name reads as a verb (`isPlanning`/`collecting`) — moot after S2. Dead code: `textVerdict` is gone; `Supplier` no longer
imported in STV ✓; no unused import introduced by this step (N7 are HEAD's).

## Verdict

**DO NOT COMMIT** as the tree stands — blocker **B2** (the `systemRouted` let rule un-routes the let-bound-element bodies and
`assertProgram`/`frameProgram` then refuse them at the let: the brief's 94 cannot move; confirm on the DuckDB histogram already
in hand — if it shows them moved, downgrade B2 to SHOULD-FIX and the verdict becomes COMMIT AFTER FOLDS). B1 is fixed in the
tree (found by the corpus; record it so). Once B2 is settled: **COMMIT AFTER FOLDS** S1 (the appeal's session on a
system-routed body), S2 (no collector escapes `plan`), S3 (the dual-golden listener name), S6a-c (expected outcomes, the
`declined`/`refereed` events, a real decline witness) and S7 (the records); S4, S5 and the NITs may follow in the step's audit
fold-in or be ledgered.
