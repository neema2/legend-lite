# C3f. M3f: frames, the eager run and the trace stamp on the program road (the test-is-the-product plan's step M3f, 2026-10-31)

M3's brief (`docs/TEST_IS_THE_PRODUCT_PLAN_2026_10_31.md` §6 B12, items 8 and 10; D15, B10, R17 C4) plans the results of
`execute(...)` calls a program reads: on both doors an inline, lazy read (`execute(...).values`); on the test door a
frame bound by a let, run at the let. An activity's comment is the execution-trace stamp of the statement that ran its
frame, bound when the plan runs.

## What was built

- **The stamp (R17 C4).** `ExecutionPlan.Sql.stamp`: the parameter a statement's own execution-trace stamp binds. The
  runner mints the comment the statement goes out under (`Statements.nextStamp`: the trace's reservation, or the
  session's own when no trace is given), binds it under that name -- for the statement's own slots and the steps
  after -- and sends the statement under it. A keyed or effect statement never binds one (refused by name as a planner
  defect). The TypeScript runner refuses a plan that binds one by name (a host's session sends no trace comment).
  `PlanJson` writes `stamp` where given (format 24, M3's).
- **A parameter cell.** The activity rows' comment cell is a plan parameter, never text the planner wrote: typed data
  carried from the rows (`LazyRows.Parameter`) through the resolver's inline rows (`ConstructedInstances`,
  `ClassSources`) into the TDS literal (`TypedTds.parameters`), which the lowerer writes as the statement's bound
  parameter (an unbound one is refused by name).
- **The frames (`planner/ProgramFrames`).** A program's frames, its plan rows (shared by its statements) and the stamp
  names it mints (`#stamp<n>`, as `#count`). `ProductStatements.prepare` inlines every statement with the splice hook
  over them, and widens its environment, after the resolver, with the renames of the frames it reads
  (`FramePlanner.renames`, moved from the body driver's `frameReplaceEnv`, which delegates). An inline lazy read
  registers its activity rows with the reading statement's own stamp; the statement binds it where it reads it
  (`PlanMaker.stampedIfRead`). An `execute(...)` run where it stands is refused by name (M9).
- **The let kinds.** `PlanMaker.program` dispatches a let on `LetPlanner.kind` (the walk's rule): ALIAS reads the same
  frame; VALUE rides the lets; STAGED as before; STAGED_GRID refused by name (T12, M9); FRAME is the test-planning
  library's.
- **The frame let (test door; decision 1, MV7).** `TestKinds.frame`: the frame's chain (`FramePlanner.chain`), its
  activity rows registered with a fresh stamp, and its eager run a `Check` step at the let whose statement binds that
  stamp; its readers paste the chain (a CTE is M3g's).

## Measured

- The tests: a frame let on the test door plans `Setup`, the stamped `Check` of its eager run, and its reader
  (`TestDoorTest.aFrameLetIsRunAtTheLetAndReadAfter`), and runs on DuckDB; an activity's comment is read back through
  its stamp, the run's comment matching upstream's own test's pattern (`anActivitysCommentIsTheStampOfTheRunBound...`,
  the corpus's `testSQLComments` regex); the query door plans an inline lazy read, stamped only where the reader
  binds the stamp (`QueryDoorTest.anInlineLazyExecuteReadIsPlanned`).
- Every core package's sent statements against M3e (the M1 method): 28/28 green; identical but the test door's own
  new tests (`testplan`, +10 lines).
- The user's question (2026-11-01, "polluting or better?") put the stamp's cost to them: 15 files, +217/-37 lines, a
  good part of it documentation; an ordinary `?` riding the count's binding, its plumbing the cell inside the activity
  rows' inline table. Kept (the user: "Keep it, fix the guard, and continue"); the typed-node guard
  (`TypedSpecChildrenTest`) gained the cell's dummy.
- The front-half census re-run (M2's, frames now on the product road): recorded with M3i's census.
