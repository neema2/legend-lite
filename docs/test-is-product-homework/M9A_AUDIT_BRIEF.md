# M9a's independent audit: the brief (2026-11-01)

The step: `docs/TEST_IS_THE_PRODUCT_PLAN_2026_10_31.md` §7 M9 (frames read by asserts, under the host judge, the
first sub-step), the revalidation `REVALIDATION_2026_11_01.md`. The code: the working tree's diff since `c9847ebe`
(M6): `testplan/BodyClass.FRAME_PROGRAM`, `driver/HostAsserts` (the walk's frames built without their eager run),
`driver/BodyRouter` (the class routed; the frame run's origin named to the runner), `exec/PlanRunner` (a step's origin
named by the sink), `testplan/TestKinds` and `TestDoor.Planned` (the frame-run steps beside the plan),
`BodyRouterParityTest.framePrograms` and `theFrameShapesTheRuleKeepsOnTheWalk`. Read-only; the auditor runs no Bazel
while a chain runs.

## The design in one paragraph

A FRAME PROGRAM is an assert program (M6's class) whose lets may also bind frames -- `let r = execute(...)`
(`LetPlanner.Kind.FRAME`, the statement's one kind `LET_EXECUTE`), the query argument holding no kind planned another
way -- read by the asserts and the value lets after it. On the program road the frame let is the test door's own step
since M3f (the eager run as a `Check` at the let, product lowering); the asserts are planned before the program by
`HostAsserts`, which now builds each frame as the walk builds it (`BodyDriver.buildFrame(ec, letPrefix, eager=false,
specs, env)`: the chain, no run, no CTE under the host judge) and splices the asserts' reads through the walk's own
hook, so a side is the walk's statement byte for byte. The eager run is sent under the walk's origin LET: the plan does
not say which check is a frame's run, so the test library records the steps (`TestDoor.Planned.frameRuns`) and the
router's sink names the origin to the runner (`ProgramSink.originOf`), which enters it where none is marked. Left on
the walk by the rule: an alias of a frame, an inline `execute(...)` inside a statement, a read of a frame's
`activities` (the pre-planned frame registers no run comment), staged rows and raw grids, the test-data generator,
effects, `assertError`, a seeded runtime.

## What to audit (find real defects; rank them blocker / should-fix / nit)

A. **Walk equivalence of the sides.** `buildFrame(eager=false)` against the walk's `buildFrame(eager=true)` under the
   host judge: does anything but the run differ (the activity rows registered with a null comment; `fc.env()`;
   `tableReplace`; the `legendQuery` branch; `registerActivityRows` into `env.plan()` -- the same `PlanEnv` the
   walk's sides are planned against)? A side reading `$r.values...` through the hook: identical SQL to the walk's?
   A VALUE let reading a frame, then an assert reading the let.
B. **The eager run.** The test door's `Check` (product lowering, `TestKinds.eagerRun`) against the walk's
   `executeTyped(resolvedToExecute(runChain))`: where do the texts differ (the identity lane, the object layout,
   ids: MV6), and is each difference inside an approved movement class (the plan's §4)? The statement origin LET on
   both roads. The stamp: when a later statement reads the run's comment the check is stamped; the class excludes
   reads of `activities` -- is that exclusion complete (`relationalActivitySql`, `$r.activities->at(0).sql`, a
   helper function reading them)?
C. **The class rule's soundness (R16).** Can a body reach FRAME_PROGRAM that the walk runs differently? A frame whose
   query has a `from()` of another runtime; a frame let AFTER the asserts that read it (an unbound `$r`); two frames
   of the same name; a frame let as the last statement; a frame read by a non-assert, non-let statement
   (`print($r...)`, a handle); `executeLegendQuery` (the string entry); the mapping/runtime arguments (packageable
   refs are admitted there alone -- is anything else admitted by the first-argument-only check?); a frame whose query
   reads the system store or a temporal class.
D. **The origin hook.** `PlanRunner.runSteps`: `enterIfUnmarked(LET)` under a mark in force (a fixture's, a
   judge's); the no-op scope where the sink names none; the hook's default on every other sink (`VerdictBatch`'s
   sinks, the product's).
E. **Guards and invariants.** NullAway; `JavaEvalLedgerTest` pins; `plannerIsExecutionFree`;
   `theTestPlanningLibraryIsTheDriversAlone` (`TestDoor.Planned.frameRuns` is a set of indexes, no exec type);
   AGENTS.md invariant 4 (no fallbacks).
F. **The parity test.** Does `framePrograms` observe the eager run's statement and label? Cases missing: a frame read
   by two asserts with a failing first; a frame whose query fails to plan (the body fails at planning: the M5 audit's
   S6); a let-bound frame read only by a value let; `assertSize($r.values.rows, ...)` vs `$r.values.rows->size()`.

Report: a concise list of findings, each with file:line, severity, the concrete failure scenario (input -> wrong
result) and a suggested fix; then the residue items for the records.
