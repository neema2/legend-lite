# E-SQL Phase 6: legend-engine's plan text to the edge (note, 2026-11-17)

Scope (the orchestrator, 2026-11-16, the user's ask): every planner- and plan-side piece of legend-engine's text moves into
`com.legend.edge`. Done when no class outside `com.legend.edge` names any of them (their `CompatSpellingGuardTest` rows
gone), a plan-only consumer reaches upstream text only through the edge, and the edge golden net stays empty.
Measured on claude/total-wall-carriers (d-stage 7d9a3518b merged). Paths under `core/src/main/java/com/legend/`.

## The pieces and who names them outside the edge

| # | Piece (lines) | Named today by (main) | Guard row |
|---|---|---|---|
| 1 | `plan/PlanText` (1,118): planToString | `model/FromProtocol`, `platform/Feature`, `planner/PlanAllocations`, `planner/QueryParameters`, `plan/PurePrint`, `plan/InProtocol`, `testplan/JudgmentFacts`, `testdatagen/TestDataPlanner`, `testdatagen/TestDataGenerator` | none yet (add) |
| 2 | `plan/PurePrint` (101) | `planner/PlanAllocations`, `planner/QueryParameters` | none yet |
| 3 | `plan/UpstreamRelationType` (141) | `server/PureV1Api` | none yet |
| 4 | `planner/UpstreamPlans` (56), the facade | `database/PlanStyle`, `planner/{PlanAllocations,ProductStatements,BodyPlanner}`, `testplan/{PlanStyleFacets,Seeds,BodyDriver}` | none yet |
| 5 | `planner/PlanAllocations` (487): its engine-text rows | 15 planner/testplan/lowering/resolver classes name the class; only its engine-text rows move | none yet |
| 6 | `sql/dialect/RawSqlBoundary` (339) | `AnsiSqlRenderer`, `DuckDb`, `Postgres`, `setup/Ddl` | D1.7 x4 |
| 7 | `sql/dialect/EngineTextDialect` (33) + the `PlanStyle` hook | `database/PlanStyle` (8), `testdatagen/TestDataGenerationNatives` (2), `testdatagen/TestDataGenerator` (3) | HOOK, D1.7 |
| 8 | `Lowerer.withEngineText` | `lowering/Lowerer` | D1.7 |
| 9 | `PlanNode.EXEC_TRACE_COMMENT` (executionTraceID) | `plan/PlanNode` (also `sql/SqlFn` names the word) | D1.7 |
| 10 | `JsonEmission`'s tdsBuilder | `lowering/JsonEmission` (`compiler/spec/ExecuteChainAssembly`, `typed/TypedJsonResult` name the word) | D1.7 |

`database/PlanStyle` (188 lines) is the widest: about 25 classes in `planner/`, `testplan/`, the root doors (`Execution`,
`TypedQuery`, `ExecuteOptions`) and `server/` read it. It is the style switch itself, not only engine text.

## Order (each its own commit: the fast loop, census, guardrails, golden net, --nobuild //...)

1. Pure moves with no product reader left behind: 3 (`UpstreamRelationType`: its one reader is the server, already
   edge-side), 9 (the trace comment is content, not plan schema; no format bump), 10 (tdsBuilder's spelling).
2. 1 + 2 (`PlanText`, `PurePrint`) and 4 (`UpstreamPlans`): the planner calls the edge through the facade's existing
   seam. `FromProtocol`/`InProtocol`/`Feature` read PlanText today; each read is checked for whether it is engine text
   (moves) or the plan's own data (stays, renamed off PlanText).
3. 5: PlanAllocations' engine-text rows split out to the edge; its product rows stay.
4. 6, 7, 8: RawSqlBoundary, EngineTextDialect and withEngineText. These are dialect- and lowering-side.

## Where a new mechanism may be needed (I stop and ask before building)

- **PlanStyle (7).** If the engine style must still be chosen at planning by product classes, the edge needs a typed
  extension point the planner calls without naming engine text. `docs/PLAN_AS_DATA_2026_11_07.md` names "the edge's typed
  extension point"; if it already fits, no new mechanism. If not, I bring a design first.
- **RawSqlBoundary (6).** It sits in the dialects' render path (`AnsiSqlRenderer`, `DuckDb`, `Postgres`). Moving it to
  the edge means `sql/dialect` must not depend on `edge` (the layer rules), so the boundary becomes a hook the edge
  supplies. That is the same question as PlanStyle's.

## Overlap with E-Java's test-side move (please confirm the split)

Not mine, by this reading: the test side's EngineText rows: `testexec/{AssertRuns,HostJudgments,SqlTextRuns}`,
`testexecutionplan/{HostJson,HostJudgment,PlannedAppeals,Verdicts}`, `testplan/{AgreementAsserts,AssertVerdicts,
JudgmentFacts}`. **Shared, unassigned:** `testdatagen/{TestDataPlanner,TestDataGenerator,TestDataGenerationNatives}`
read PlanText and EngineTextDialect. They are planning-side but test-data, so either stream could take them; I propose
E-SQL takes them with pieces 1 and 7.
