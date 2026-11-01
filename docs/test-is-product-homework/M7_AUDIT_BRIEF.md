# M7 audit brief — the database judge's assert and frame programs move (2026-11-01)

The independent audit of the test-is-the-product plan's M7 (R15/R22). Read-only; findings as blockers / should-fix /
nits with file:line and a concrete Pure body each; fold nothing. The user's ruling in force (the revalidation, B3
struck): an assert under the database judge is a regular lowered statement answering the verdict row; the plan carries
its SQL and no assert metadata; M8 makes the wrapper self-contained (polarity, message). M7 moves the bodies with
TODAY's wrappers, judged by today's root loop, and must change no outcome, statement text or census.

## What M7 built

1. **`judge/VerdictBatch` split** (the walk's `flush` = `plan()` + run + `judge()`): `plan()` is the former first half
   (VERDICT_FLUSHES, groups per session, the keyed members a `Group` of their own, `fusion.fuse` over the frames,
   DialectCapability → `spelled` → `fallback` at planning) answering `Planned` (`groups()`: `Group(step, on, keyed)`;
   `fused(group, rejected)` the census; `received(verdict, row)` the rows by member index; `judge(listener)` the root
   loop, `executeOne` lazily where a row is absent). A fused group's verdicts carry NO wrapper (as before: the split
   rung's wrapper is planned and run at its place by the root loop, under FALLBACK, never past the first failure).
   `Resolved(failure, reported)`: `wall(e)` records a wall the arm left through as a step raised at the root's turn,
   told to no listener (the walk's `batched`: `sqlRaised`, discard, flush, surface). `flush(l)` runs each group
   through `PlanRunner.program` on its session under BODY, swallowing a keyed group's DataError as before, then judges.
2. **`driver/AssertVerdicts.planDatabase(batch, bare, letPrefix, specs, env, hook)`**: the `batched` protocol with no
   flush (open, adjudicate under the batch, resolve/wall/close; discard → false), gated first by
   `databasePlansWithoutReading`: `planHost(...) != null` (the shapes neither judge plans statically), and not the
   grid pair of two relation-typed sides (D3: `HostSides.judged` reads them on the host at the arm), not a
   rendered-text pair (`renderedPlan`), not `assertEq` over a class-kind side (`HostJudge.eq`, D43). Claim: every
   other database arm plans its verdict statement without sending anything (`runVerdict` defers; `databaseVerdict`,
   `databaseSize`, `databaseCondition`, `databaseTdsEquivalent`, `quantified`'s `planSide`, `cellPool`).
3. **`driver/DatabaseAsserts implements TestDoor.Asserts`** (the twin of `HostAsserts`): the body walked with a
   collecting batch on the environment; frame lets built with `BodyDriver.plannedFrame` under the batch (eager build →
   not run, CTE defined when a reader splices it; the activity comment the SEGMENT's fused statement's stamp
   `#stamp.fused<k>`, a parameter the fused step binds: `stamped(step, stamp)`); aliases; handle lets registered; at
   each EFFECT statement the segment closes (`close`: `batch.plan()`, every group on the body's session and none keyed,
   else null → walk; the steps placed at the segment's last assert, the asserts before it `at() == List.of()`
   absorbed) and a fresh batch opens; at the body's end the last segment closes. A frame-environment raise or the
   gate's own raise before the arm → `wall` at the root (S1). `runsFrames()` false; `frameStamp(i)`.
4. **`testplan/TestDoor.Asserts`**: `at(int)` answers a LIST (one step, several, empty = absorbed, null = refused);
   `runsFrames()` default true. `TestKinds.plan` places every step; `TestKinds.frame` under `!runsFrames()` adopts the
   stamp and places no check (no CTE, no `ProgramFrames` registration).
5. **`driver/BodyRouter`**: `BodyClass.of(typed, runtime, mode != BOTH)`; the planner by mode (HOST → `HostAsserts`,
   else `DatabaseAsserts`); `PlanRunner.program(..., database != null ? Judging.DATABASE : Judging.HOST, moved)`;
   `Moved.verdict` → `database.received(verdict, row, listener)` (the segment judged at its last verdict),
   `Moved.fused` → the batch's census; `originOf` BODY for the verdict-group steps; `lastVerdict = ok()` for a body
   ending on an assert. The census tag `<mode>-unplanned-assert`.
6. **`testplan/BodyClass.of(body, runtime, oneJudge)`**: the assert and frame classes for HOST and DATABASE; BOTH
   (channel B) walks every assert body until M8.
7. **Parity** (`BodyRouterParityTest`): `assertProgramsUnderTheDatabaseJudge`, `framesUnderTheDatabaseJudge`
   (frames, aliases, inline, an effect between two asserts = two segments, class-rooted, activities `.sql` and
   `.comment` (62 chars, the fused statement's own stamp), the plan handle, a temporal frame),
   `theAssertShapesTheDatabasePlannerLeavesOnTheWalk` (grid pair, class-kind `assertEq`, `toCSV` rendered form),
   each comparing outcome, ledger, answer, statement-origin census, statements (the stamp's two spellings
   normalized), verdicts, the verdict census.

## Questions

- **A. The gate's completeness (claim 2).** Walk every arm reachable from `AssertVerdicts.adjudicate` in DATABASE
  mode (`arm = DatabaseJudge.ARM`): which can SEND a statement or read rows before `runVerdict` defers? Candidates:
  `renderedArm` (gated), the grid pair (gated), `HostJudge.eq` (gated), `cellPool` (`databaseVerdict`: defers?),
  `quantified` (`planSide` then `databaseVerdict`?), `tdsEquivalent` (`databaseTdsEquivalent`: the tolerances' group
  on the walk was read AFTER the grids — M6's N1 — does the database path read anything?), `instanceOf`, `is`,
  `contains`, `jsonStringsEqual`, `size`, `empty`, `condition`; the lineage-lines transform; `SqlTextVerdicts.tryArm*`
  (the engine-text arms: `mayArm` gate → `planHost` null → gated); `AssertErrorNative` (not a verdict). For each, cite
  the line that defers or the line that would run. A body for any arm that runs at planning is a blocker (the
  statements would be sent at planning, before earlier steps, and then the body walks or moves with a double send).
- **B. Segment placement.** The walk flushes at `closeVerdicts()`: before an effect (`collectEffect`), before a TDG
  generator, at the body's end; a deferred VALUE statement runs before the fused statement at the close. In the
  admitted classes no value statement sends (plansNoStep) — confirm nothing else closes a segment on the walk
  (`BodyCompiler.executeBody`: STAGED lets `seg.value(...)` → `values` deferred, not a close; the let kinds are
  refused anyway). Confirm the program road's step order per segment: [frames: nothing] [effect scripts] [verdict
  groups at the last assert]: the effect statement that CLOSES segment k is placed by TestKinds AFTER the groups of
  segment k? (`DatabaseAsserts.close` puts the steps at `lastAssert < effect index`; TestKinds places them at that
  assert's turn, before the effect — the walk's order: flush then effect. ✓?) A segment with NO assert before an
  effect: `close` → `plan()` null → fine.
- **C. The stamp.** The fused statement `.stamped("#stamp.fused<k>")`: `PlanRunner.result()` binds its own stamp
  before `statement()` (the `?` inside the fused text filled); `PlanMaker.everyReadStampBound` satisfied by the fused
  step's `sql.stamp()`; `ProgramFrames.adoptStamp` makes every program statement bind the slot; the walk's
  `reserve()`/consume protocol (`JdbcStatements`) gives the fused statement the reserved comment — equal in shape
  (`-- "executionTraceID" : "<uuid>"`). A frame whose segment has no fused step (no assert after it before an effect)
  but whose activities a LATER segment reads: `#stamp.fused<k>` read by segment k+1's statement, bound by no step →
  `everyReadStampBound` raises at `TestDoor.planned` → R14 failure where the walk passes. Is that reachable
  (`let r = execute(...); <effect>; assertEquals(..., $r.activities...)`)? Recommend the refusal (walk) in
  `DatabaseAsserts.plan`.
- **D. Keyed and unspelled members.** `close` returns false (walk) for a keyed group and for a segment whose
  members the dialect spells none of; a PARTIALLY spelled segment: the fused step holds the spelled members, the
  unspelled ones have no step and no row → judged by `executeOne` at their place (DialectCapability → UNJUDGED →
  raise) — identical to the walk? The walk's `spelled` path: same. ✓? Where is `Census.VERDICT_FALLBACKS` counted
  for the unspelled: `fallback(e)` at planning in both.
- **E. The listener protocol and census.** `sqlEnter` at planning (`planDatabase`) vs the walk's at the assert's
  turn — same count per test; `sqlFamily` at the arm (planning, both); `sqlJudgedInDatabase` in `verdictOf` (judge
  time, both); `sqlRaised` once per failing root; a wall `sqlRaised` once and no listener verdict. VERDICT_FLUSHES
  per segment with roots. VERDICT_HOST_DECIDED per root with no pending. Verify `received` for a verdict whose row is
  null (the fused statement rejected: `databaseVerdicts` passes null rows for each verdict with no wrapper) → the
  root loop runs `executeOne` → FALLBACK. The runner's `rejected` savepoint path (F11, Postgres) unchanged.
- **F. R16 and the guards.** `DatabaseAsserts` decides by `planDatabase`'s boolean and `close`'s route checks — no
  catch routes (the two `catch (RuntimeException e)` make planned raises, not routes). `JavaEvalLedgerTest`: register
  `DatabaseAsserts.java` in DRIVER_CLASSES; pins for `AssertVerdicts` (1449 → ?), `VerdictBatch` (if pinned),
  `BodyDriver` (945 → ?), `BodyRouter`; `ErrorShapeGuardrailTest.BROAD_CATCH_COUNTS` (`DatabaseAsserts.java` 2,
  `AssertVerdicts.java` +1, `VerdictBatch.java` +1?); `hostVerdictIsReachableOnlyFromTheVerdictSeam` for the new
  sink paths; `theDriverHasOneCaller`; `CodeShapeGuardrailTest` method length (`DatabaseAsserts.plan` ~80 lines).
- **G. BOTH mode.** `BodyClass.of(..., false)` for BOTH → the assert classes null → every assert body walks under
  channel B (the AgreementArm). Confirm `BodyRouter` passes `mode != BOTH` and that the effect/query classes still
  move under BOTH as before (M5's 604).
- **H. The answer.** A body ending on an assert under the database judge: `lastVerdict = ok()` set in
  `Moved.verdict` after the segment judged (the judge raises before it on a failure) → `JudgePlans.host(ok, answer)`;
  the walk: `seg.last = pendingVerdict = ok()`. A body ending on a literal: the constant. ✓?

## Files

`core/src/main/java/com/legend/judge/VerdictBatch.java`, `core/src/main/java/com/legend/driver/{DatabaseAsserts,AssertVerdicts,BodyRouter,BodyDriver,HostAsserts,DatabaseJudge}.java`,
`core/src/main/java/com/legend/testplan/{TestDoor,TestKinds,BodyClass}.java`, `core/src/main/java/com/legend/exec/PlanRunner.java` (`databaseVerdicts`, `verdicts`),
`core/src/main/java/com/legend/planner/PlanMaker.java` (`everyReadStampBound`, `withoutUnreadStamps`), `core/src/test/java/com/legend/driver/BodyRouterParityTest.java`.
Context: the plan's §6 B3 (struck), §7 M7, §9; `docs/test-is-product-homework/M9C_AUDIT_BRIEF.md`; the scratch design
`m7-brief.md` is not in the tree (its v2 is summarized above).
