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

## Finding at S2's start (2026-11-16): S2 and S3 are one move

The judgment record cannot leave alone. The test planner's own routing reads its fields: `AssertVerdicts`' database
gate (`golden`, `ours`, `textEqual`), `AgreementAsserts` (`appeal`), `PlannedAppeals` (`appeal`), `Verdicts`
(`oursOrigin`), `AssertRuns` (`routed`). So the engine-text family moves as a unit: its planner arm
(`SqlTextVerdicts`), its gates, its runner (`SqlTextRuns`) and its appeals.

What the planner arm reaches inside the test planner (measured): `BodyDriver.ExecEnv` (32 uses), `AssertVerdicts`'
replay helpers (`replayFacts`, `ReplayGates`, `unpagedRead`, `adjudicate`, `calleeFqn`, `planHost`, `planDatabase`,
`gateOf`: 28), `JudgePlans` sides (9), `HostSides` (9), `DatabaseJudge.planSide`/`SideRows` (2), `JudgmentFacts.enumsOf`
(1); of D's planner, only `BodyPlanner.boundContext` and `SqlTextInputs.of`.

**Proposed shape (sent to D and the orchestrator before it is built):**

- A test-planning seam declared by the product in `testplan` (`EngineTextArm`): the planning entries the test planner
  calls today on `SqlTextVerdicts`, over a typed facade of the ~20 internals above. The product's arm refuses by name;
  the edge implements it (the moved `SqlTextVerdicts`).
- The arm is handed with the style (the edge's `PureText.UPSTREAM` carries it), never found by the product.
- The plan holds the edge's judgment by verdict (`extensionsByVerdict`); `HostJudgment` keeps one product arm naming the
  verdict; the gates ask the arm, not the record.
- The runner judges it through a `ServiceLoader` seam (`ExtensionJudge`, beside `ExtensionCodec`), so a run needs no
  caller-supplied `Edge`; `SqlTextRuns` and the appeals' deciding move behind it.
- Size: the largest single item of W9's judge half; estimated 2-3 working days, done in compile-green steps.

## S2c: the arm's code to the edge (planned 2026-11-16, after S2b `e2417f667e`)

**The orchestrator's condition:** when S2c lands, `AssertVerdicts` is at or below 1,520 lines and `SqlTextVerdicts`
at or below 1,157 (both stripped), and `ArchitectureTest`'s allowance for `EngineTestText#arm` is gone or names what
still needs it. Moving lines between product files to meet a pin is not a move; the arm's code goes to the edge.

**The surface, measured (the arm's callers in the test planner, and what the arm reads of it):**

| From | Calls into the arm |
|---|---|
| `AssertVerdicts` | `engineTextArm` (the facet, S2b); `tryArm`, `tryArmSameSql`, `tryArmDualGolden` (the database judge's path, lines ~441-534); `isRoot`, `mayArm`, `notAConstant`, `why`; `BatchTexts` |
| `BodyDriver` | `ExecEnv`'s `engineText` collector field and `withEngineText`; `marking`/`restoreMarking` around a side's planning |
| `JudgePlans` | `env.engineText()` (the collector), `NotAConstant` |
| `DatabaseAsserts` | `isRoot` |

| The arm reads | Uses |
|---|---|
| `BodyDriver.ExecEnv` (`ctx`, `target`, `frame`, `engineText`, `appealOn`, `appeals`, `verdictBatch`, `options`, `postProcessors`, `withEngineText`, `withFrame`, `withTableReplace`) | 32 |
| `AssertVerdicts` (`replayFacts`, `ReplayGates`, `unpagedRead`, `adjudicate`, `calleeFqn`, `planHost`, `planDatabase`, `gateOf`) | 28 |
| `JudgePlans` (`PlannedSide`, `plannedSide`, `evalSide`, `shapeOf`, `Kind`) | 9 |
| `HostSides` (`Ask`, `Check`) | 9 |
| `DatabaseJudge.planSide`/`SideRows`, `JudgmentFacts.enumsOf` | 3 |

**Order (each compile-green and pushed; the pins stay transitional until the last):**

1. **S2c-1, the facade.** A public context in `testplan` (`EngineTextPlanning`) that the edge's arm calls for each
   read above, implemented by the test planner over `ExecEnv`; the arm's state types (the collector, `Collected`,
   `NotAConstant`, `BatchTexts`) become the facet's own types.
2. **S2c-2, the callers.** Every call into the arm listed above goes through the facet (`env`'s style), so the test
   planner names no `SqlTextVerdicts` member.
3. **S2c-3, the move.** `SqlTextVerdicts` moves to `com.legend.edge` over the facade; the bridge `EngineTestText.arm`
   and its `ArchitectureTest` entry go; the pins return below 1,520 and 1,157.

## Rules kept

- No product class names an edge type (G5); `ArchitectureTest.onlyTheEdgeImplementsTheHooksUpstreamSide` and the
  planner-entries rule stay green at every slice.
- Each slice proves no verdict moves: core testplan (every judge), integration, both corpus verdicts, channel B's five
  DuckDB suites; a guard pin moves only with a dated reason.
