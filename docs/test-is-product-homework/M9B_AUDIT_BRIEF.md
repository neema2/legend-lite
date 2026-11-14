# M9b's independent audit: the brief (2026-11-01)

The step: M9's second sub-step, the frame class widened (`docs/TEST_IS_THE_PRODUCT_PLAN_2026_10_31.md` §7 M9; M9a's
brief `M9A_AUDIT_BRIEF.md` and its audit's findings, folded in this same diff). The code: the working tree's diff since
`c9847ebe` (M6) -- M9a's and M9b's together: `testplan/BodyClass.FRAME_PROGRAM` (`frameProgram`: frames by their
relation-rootedness from `FramePlanner.chain`; aliases of relation frames; lazy inline `execute(...)` reads admitted,
the eager `size(execute(...))` shape and a read of `activities` refused; a database effect as a statement with literal
arguments; `LetPlanner.badAliasIndex` and a frame reading a frame refused by name), `planner/LetPlanner.badAliasIndex`
(new), `driver/HostAsserts` (the walk's frames built without their run; aliases), `driver/BodyRouter`,
`exec/PlanRunner` (`ProgramSink.originOf`), `testplan/TestKinds`/`TestDoor` (the frame runs' places),
`planner/ProgramState.size`, `BodyRouterParityTest.framePrograms` and `theFrameShapesTheRuleKeepsOnTheWalk`.
Read-only; run no Bazel.

## What to audit (find real defects; rank them blocker / should-fix / nit)

A. **The three relaxations against the walk.** (1) An alias let: `HostAsserts` puts the aliased frame under the let's
   name; the walk does the same (`BodyCompiler` ALIAS). The predicate (`relationRooted`) identical on both; a class
   frame's `$r.values->at(0)` is a VALUE let on both. (2) An inline `execute(...)` inside an assert's side or a value
   let: on the walk the side's planning splices it lazily (D15) -- does `BodyDriver.planHostSide` ever RUN an inline
   execute (the hook's `inlineExecute(ec, eager=true)` paths: `size(execute)` is refused by name; any other?)
   `relationalActivitySql(ec)` for an inline execute (`sql(execute(...))`)? (3) A database effect statement between
   frames and asserts: the eager run before it, the sides after it (both roads re-run the pasted chain), the effect's
   script closed before the next statement, `markWriting`; the effect program's exclusions carried (`literalEffect`,
   not a let, no seeds). An effect's connection let (`let c = ^TestDatabaseConnection(...)`) a VALUE let on both roads?
B. **`badAliasIndex`** against `aliasOf`: the same walk; a shape where they disagree.
C. **The class rule's completeness.** With frames, aliases, inline executes and effects admitted: a body the walk runs
   differently that passes the rule -- an effect whose arguments read a frame (`executeInDb($r.values...->toString(),
   ...)`: `literalEffect`?), an effect before a frame let on the same table, an alias of an alias, an alias read by an
   alias, a frame reading a VALUE let that reads a frame, `print($r...)` between.
D. **Guards and invariants.** NullAway; the ledger pins (`JavaEvalLedgerTest`: BodyClass is not pinned? LetPlanner?);
   `plannerIsExecutionFree` (BodyClass calls `FramePlanner.chain`: planning only); AGENTS.md invariant 4.
E. **The parity test.** Cases that compare statements for every admitted shape; a case missing per relaxation.

Report: findings with file:line, severity, the concrete failure scenario (a body -> walk vs program road), a fix;
then the residue for the records. Under 1,500 words.
