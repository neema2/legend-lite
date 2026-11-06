# M9c audit brief — activities reads, the side-kind rule, the kind census (2026-11-01)

The independent audit of the test-is-the-product plan's M9c (R15/R22: one audit per step and sub-step). Read-only;
report findings as blockers / should-fix / nits with file:line and a scenario each; fold nothing.

## What M9c built (the diff since the M9b audit's folds)

1. **Activities reads admitted in the frame class** (`testplan/BodyClass.frameProgram`: the `readsActivities` refusal
   deleted). The walk registers a let frame's activity rows AFTER its eager run with the run's trace comment
   (`BodyDriver.buildFrame` → `PlanAllocations.registerActivityRows(..., activityComment(run, env), env)`), a
   literal in the rows. A moved body's frame is pre-planned before the program runs, so its comment cannot be a
   literal: `BodyDriver.plannedFrame(ec, letPrefix, specs, env, stamp)` registers the rows with
   `commentParameter = stamp` (a `LazyRows.Parameter` cell → `TypedTds.Parameter` → the Lowerer's
   `bindPlanParam` → a `?` slot named `#stamp.let<i>`), and adds the name to `PlanEnv.stamps()` (a new run-scoped
   accumulator, the third beside `queryLets` and `planRows`), which `BodyPlanner.lowererFor` binds as a typed
   String plan parameter for every statement lowered on that environment.
2. **The stamp's binding**: `HostAsserts.plan` mints `#stamp.let<i>` for each frame let (`stampOfFrameLet`) and
   answers it through the new hook `TestDoor.Asserts.frameStamp(int)`; `TestKinds.frame` adopts it
   (`ProgramFrames.adoptStamp`) for the frame's eager run `Check`, which is `.stamped(stamp)`; the runner's
   `check(...)` puts the minted comment into the program's shared `values` under that name, so a later verdict
   side's `?` binds it (`PlanRunner.hostSides → result → send`). `PlanMaker.everyReadStampBound` must see the
   adopted name bound by the Check.
3. **The side-kind rule** (`BodyClass.sideKind`): for an ASSERT statement or a VALUE let of the ASSERT and FRAME
   classes, the another-way refusal (`plannedAnotherWay`: execute, staged, raw grid, store write, system read,
   element reference, object, graph, temporal) is replaced by `system-read || eager-inline-execute`. Claim: the
   asserts' sides are planned by `HostAsserts` with the walk's own planner, let prefix and frames
   (`AssertVerdicts.planHost` → `HostSides.plan` → `JudgePlans.plannedSide`), and a VALUE let sends nothing on
   either road (`PlanMaker.planLet: case VALUE -> state.let(let)`), so an object, a graph, a temporal read or a
   class reference inside one diverges nowhere. The frame's QUERY keeps the full `plannedAnotherWay` test (its
   eager run is the product's lowering, `TestKinds.eagerRun → ProductStatements`).
4. **The element-reference rule** (`BodyClass.systemElementReference`): a `TypedPackageableRef` is a system-store
   read only when typed by a bare `Type.ClassType` the context tracks and the system mapping binds — the
   resolver's own criterion (`ElementReferences.trackedElementClass`). A class reference typed `Class<X>`
   (`instanceOf`'s type argument, the corpus's `sql()` helper) is a value and reads no store. `anotherWay` folds
   the former into `"system-read"`; `"element-ref"` is no longer a kind.
5. **The kind census** (`testplan/BodyKinds`, `exec/Census.STAYED_KINDS`, the corpus's
   `[corpus2] body-router stayed <count> <kinds>` histogram over PASSING tests and the per-test TSV
   `corpus2-body-stayed.tsv`): a measurement carrier; it reads a statement where the rule reads it (a frame let's
   query argument alone, prefixed `frame-`; a handle let and an alias nothing; every other statement with its inline
   executes as their queries, through `sideKind`). It decides nothing. Registered in `JavaEvalLedgerTest`'s
   testplan set; `HarnessDisciplineTest`'s MinimalCorpusTest sort sites 2 → 3 (a display ordering).
6. **Parity** (`BodyRouterParityTest`): `framePrograms` gained activities reads (`assertSize($r.activities, 1)`, the
   filter-and-cast `.sql` read, through a value let, inline); `theActivityCommentIsTheRunsStamp` asserts the comment
   is the run's stamp on both roads (`startsWith('-- "executionTraceID"')`, `length() == 62`) and that the walk
   spells it as a literal while the moved side binds it; `assertParity` now compares statements with the stamp's two
   spellings normalized (`STAMP_LITERAL`, `STAMP_PARAMETER` → `<stamp>`) — the one designed text difference (R17
   C4). `assertInstanceOf(1, Integer)` moved from the walked pins to `assertPrograms` (the host checks plan it).

## Questions to answer

- **A. Soundness of claim 3.** Find any kind inside an assert statement or a value let for which the two roads
  differ although the side is planned by the same functions: a side whose session is not the body's (the one-target
  rule in `HostSides.plan`; the system route; a `from()` to another runtime — `RUNTIME_SEEDS`/`INLINE_SEEDS` kinds
  are refused by `kinds.isEmpty()`, check that); a value let the program road treats other than `state.let`
  (`PlanMaker.planLet`, `ProgramState.let`, `registerHandlesIn`); a let whose handle registration raises (S1
  pending-let path) now reachable with a kind that was refused before (an `executionPlan(...)` handle holding a
  graph fetch — does `registerHandleRows` plan it with the product's lowering? `PlanAllocations.registerPlanRows`).
  Also: `TypedRawSqlRelation` in a side without an EFFECT kind — is that possible (a raw grid read from a let bound
  before: `let x = executeInDb(...)` is an effect let, refused; a raw grid through a user function?).
- **B. The stamp parameter's flow.** (i) `PlanEnv.stamps` is a shared mutable list across derived environments: does
  any environment derivation copy it (then a later `add` is unseen)? `withPlan`/`BodyDriver.ExecEnv.withPlan`
  (`fc.env()` from `FramePlanner.chain` — a derived PlanEnv via `withTableReplace`/`withFrame`: shared list by
  the record copy — confirm). (ii) A side planned BEFORE the frame let that nonetheless reads `#stamp.let<i>`:
  impossible by scope? (iii) Two bodies in one session: the `#stamp.let<i>` names are per program; the runner's
  `values` is per `program(...)` call — confirm no leak. (iv) The `Check` is `.stamped(stamp)` only where
  `stampedIfRead`/`eagerRun` reads it? `TestKinds.eagerRun` stamps unconditionally: `.stamped(stamp)` — good; but
  `PlanMaker.everyReadStampBound` requires `bound` to contain the name: a side reading `#stamp.let3` whose frame
  let is at index 3 — the Check's `sql.stamp()` is the adopted name: verify `ExecutionPlan.Sql.stamped(name)`
  keeps the name verbatim. (v) `PlanRunner.unstamped`: a side that is a `Keyed`/`Update` cannot bind — can an
  activities read be a keyed side? (vi) The walk's `theActivityCommentIsTheRunsStamp` expectation `length() == 62`:
  `-- "executionTraceID" : "<uuid>"` = 25 + 36 + 1 = 62 — verify `ExecutionTrace.comment()`.
- **C. The census.** `BodyKinds.of` must never raise (it runs for every walked body in the corpus): `LetPlanner.kind`
  with a `relationFrame` predicate over the census's own frames; `FramePlanner.relationRooted` (non-raising by
  design — `ExecuteChainAssembly.queryLambda` — but `peelSelections`/`concatenateFold` may raise on odd shapes?);
  `BodyClass.literalEffect` → `Literals.fold`; `BodyClass.plansNoStep` → `BodyPlanner.handleType`. A raise here
  would fail a walked body that passed before (the router calls it before `BodyDriver.execute`). Recommend a guard
  if any path can raise.
- **D. The relaxed classes and R16.** `assertProgram` and `frameProgram` now admit bodies they refused at M6/M9b.
  List the shapes newly admitted that the parity test does not cover (an object literal side, a graph-fetch side,
  a temporal `.all()` side, a `let m = x::M` — should be refused as system-read: confirm `tracksClassifier` holds
  for `Mapping` in a test model; a `TypedTypeRef` vs `TypedPackageableRef` for `cast(@T)`).
- **E. Guards.** `JavaEvalLedgerTest` pins moved: BodyPlanner 336 → 341, BodyDriver 939 → 945, LetPlanner 123 →
  131 (M9b), FramePlanner 199 → 219 (M9b); each with a dated justification — check the counts and that nothing
  evaluated moved into Java. `HarnessDisciplineTest` 2 → 3. ArchUnit: `BodyKinds` in testplan reaches planner and
  compiler only; `Census.STAYED_KINDS` written by the driver (`BodyRouter`), read by the corpus harness.
- **F. Records.** The plan's §9 should name: the stamp-as-parameter difference (R17 C4) as the one designed text
  difference in a side; `sideKind` as the rule for sides; `element-ref` retired as a kind; the census carrier going
  with the router at M10.

## Files

`core/src/main/java/com/legend/testplan/{BodyClass,BodyKinds,TestDoor,TestKinds}.java`,
`core/src/main/java/com/legend/driver/{HostAsserts,BodyDriver,BodyRouter}.java`,
`core/src/main/java/com/legend/planner/{PlanEnv,BodyPlanner,ProgramFrames,PlanAllocations,FramePlanner}.java`,
`core/src/main/java/com/legend/compiler/spec/ExecuteChainAssembly.java`, `core/src/main/java/com/legend/exec/{Census,PlanRunner}.java`,
`core/src/test/java/com/legend/driver/BodyRouterParityTest.java`, `core/src/test/java/com/legend/{JavaEvalLedgerTest,HarnessDisciplineTest}.java`,
`spec/src/test/java/com/legend/rcorpus/MinimalCorpusTest.java`. Context: `docs/TEST_IS_THE_PRODUCT_PLAN_2026_10_31.md`
§6-§9, `docs/test-is-product-homework/M9B_AUDIT_BRIEF.md`, the M9b audit's findings recorded in the plan's §9.
