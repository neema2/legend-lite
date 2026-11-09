# The plan-as-data design's step 9 and step 10 drafts (2026-11-08)

Work done outside the tree, in a scratch copy compiled with plain `javac`, while slice 2's lanes ran. None of it is
committed code. It is kept so that nothing done has to be done twice.

| File | What | State |
|---|---|---|
| `step-9-draft.patch.txt` | **step 9**: a window frame's offset as a closed type (`Frame.Offset`: an integer, a float or a decimal; `Preceding.of`/`Following.of`, `n()` kept), so guard 1's last pin goes and guard 1 is at zero; guard 3 hard on the DuckDB corpus lane (`spec/BUILD.bazel`: the lane runs the plan read back from its JSON) | compiles, with core's tests, spec and PCT; frame bounds render the same text (every renderer reads `n()`); **not run under Bazel, the guards or the lanes**; `git apply` cleanly onto `9de03396` |
| `step-10-cuts-draft.patch.txt` | **step 10's class-level cuts, before any package move** (10.1 to 10.6c; listed below). Every mixed class is split into its planning half and its run half, still inside `com.legend.driver` and `com.legend.judge` | compiles, with core's tests, spec and PCT; `jdeps` over the compiled classes shows **zero planning-to-run and zero run-to-planning references**; **not run under Bazel, the guards or the lanes**; applies after the step 9 draft |
| `step-10-split-map.md` | the planning agent's read-only map for step 10, verbatim, with a note on where it departs from the design | evidence |

## The cuts in the step 10 draft

- **10.1 In place:**
  - `ExecutionPlan.Multiplicity.text()`;
  - a verdict row's columns by position (`testexecutionplan.VerdictRow`, pinned against `VerdictSql.schema` by
    `VerdictRowOrderTest`);
  - the class layout's synthetic column names in `sql.SqlExpr` (`ClassLayouts` aliases them);
  - a message's planning-failure text decided at planning;
  - the pivot's naming rule in `sql.PivotNames` (`Type.RelationType` delegates);
  - a raised error's source position through `exec.RaisedErrors.position` (no `java.sql` type outside exec);
  - unused imports.
- **10.2:** `TypeInferenceException`, `SchemaInvariantException` and `DialectCapability` move to `com.legend.error`
  (about 125 files; `Failure.Kind`'s binary names follow).
- **10.3:** the SQL verdict census moves out of `CanonicalDivergence` into `diagnostics.VerdictCensus`, its never-set mute
  switch left behind. The root loop's `Judge` and `Appeal` move into `RootLoops`. `PrepTrace` moves to `diagnostics`.
  The batch's unread trace field is deleted.
- **10.4:** `TypedSide`, `Column`, `Row`, `AssertListener` and `Failures` move to `testexecutionplan`.
- **10.5:** each mixed class is split. Run halves: `HostJudge`, `AssertRuns`, `SqlTextRuns`, `DatabaseRows`,
  `SideShapes`, `HostSideRuns`, `AssertErrorRuns`. Planning halves: `HostPlans`, `AssertVerdicts`, `SqlTextVerdicts`,
  `DatabaseJudge`, `JudgePlans`, `HostSides`, `AssertErrorNative`. The helpers both halves read go below both:
  - `testexecutionplan.Outcomes` (`ok`, `fail`, `unjudged`, `excerpt`, `firstTextDiff`);
  - `EngineTextRules`;
  - `TypedSide.decodeSideValues` and `scalarString`;
  - `SideShape.constantValue` and `SideShape.PRIMITIVES` (pinned by `SideShapePrimitivesTest`);
  - `PlannedAppeals`;
  - `JudgeJson`, its compatibility grammar the edge's (`edge.JsonSimple`) through the `CompatJson` seam;
  - `HostJudgment.oursOrigin`.

  Planning gets its own origin marks (`OriginMarks`), its own activity stamp (`ActivityStamp`) and its own options
  (`PlanningOptions`, converted from `ExecuteOptions` at the door); it knows only whether a referee decides appeals.
- **10.6:** the edge inversion:
  - the rows leg of an appeal moves to the edge (`edge.RowsLeg`, behind `AppealDecider`, which `CompatEdge` implements);
  - the referee's SPI moves to the edge;
  - the run hands a decision explicit services (`AppealRun`) where `RunReader`'s thread-local stood (deleted);
  - a contract's and the generator's appeals stay the run's.
- **10.6c:** the body's run sink becomes a class of its own (`BodyRun`, `BodyRouter`'s `Moved`); the program's answer
  sink becomes `Answer`.

**To revisit before reuse:**
- **`AppealDecider` sits at the door, and `runProgram` keeps a decider parameter in place of the referee.** The
  parameter was kept because lite's own runs check that a referee is present without any edge (`BodyRouterParityTest`'s
  lite half). Both choices predate the catalog design and the thin door.
- **The package move (10.7) was attempted and abandoned** (PLAN_AS_DATA section 10): it created `com.legend.testdoor`
  and widened visibility to whatever the compiler asked for.
