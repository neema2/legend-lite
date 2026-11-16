# E-Java: W9's judge half, the slices (2026-11-16)

**Goal (D's plan, W9; the orchestrator's split: the judge half is E-Java's).** The test planner and the test runner
name none of legend-engine's text. The engine-text assert (a golden `toSQLString`/plan text compared with ours) is
compatibility, so it is the edge's. The shape is the one ruled in `docs/PLAN_AS_DATA_2026_11_07.md` §2.7 (2026-11-08),
confirmed by D on 2026-11-16:

- the edge's record implements `testexecutionplan.Extension`;
- the test plan holds it by verdict (`TestPlan.extensionsByVerdict`);
- its JSON is the edge's codec (`edge.EdgeCodec`, registered under `core/src/main/edge/META-INF/services`);
- the sealed `HostJudgment` family keeps one product arm, which names the verdict and its part only;
- under lite's own rules nothing is planned for the edge.

## Measured at the start (D's stage, `9aec31d18f`)

| Piece | Lines | Where |
|---|---|---|
| `testplan/SqlTextVerdicts` (the engine-text arms' planner) | 1,694 | product |
| `testexec/SqlTextRuns` (the engine-text verdict at its step) | 295 | product |
| `testexecutionplan/Appeal` (already an `Extension`), `PlannedAppeals` | 181 + 171 | product |
| `HostJudgment.EngineText` readers | 10 files | product |

## Slices

| # | Slice | State |
|---|---|---|
| S1 | The judges' engine-test text from the one hook: `PlanStyle.engineTestText`, `engineTestPlanText`, `engineTestConnection` (product refuses by name, the edge implements); the test planner names no `EngineText` | **done**, `9aec31d18f` |
| S2 | The judgment record to the edge. `HostJudgment.EngineText` becomes `edge.EngineTextJudgment implements Extension`, by verdict; the product arm `HostJudgment.Extended` names the verdict. A run-side seam (`testexecutionplan.ExtensionJudge`, found by the same `ServiceLoader` seam as the codec) judges it; `SqlTextRuns` moves behind it to the edge | next |
| S3 | The planning to the edge. `SqlTextVerdicts` is reached through a planning seam on the style (product refuses by name); what it needs of the test planner's package becomes that seam's typed arguments | after S2; sized after S2 (the package-private surface it reads is the cost) |
| S4 | `Appeal` and `PlannedAppeals`, already extension data, move with S2/S3 | with S3 |

## Rules kept

- No product class names an edge type (G5); `ArchitectureTest.onlyTheEdgeImplementsTheHooksUpstreamSide` and the
  planner-entries rule stay green at every slice.
- Each slice proves no verdict moves: core testplan (every judge), integration, both corpus verdicts, channel B's five
  DuckDB suites; a guard pin moves only with a dated reason.
