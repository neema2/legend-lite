# C3d. M3d: a test's effects planned on the test door (the test-is-the-product plan's step M3d, 2026-10-31)

M3's brief (`docs/TEST_IS_THE_PRODUCT_PLAN_2026_10_31.md` §6 B12, item 4) plans a test's database effects as
`Effect` steps of the test door's program: consecutive effect statements one script, as the walk sends them. The
brief's measurement for this sub-step: every corpus program holding an effect planned whole, its scripts compared
with the texts the walk sends, byte-identical expected.

## What was built

- `testplan/EffectPlanner` (moved whole from `planner/`, R17 C7: a test's effects ship in no planner of the plan
  side; the driver's effect arms call it there).
- `testplan/TestKinds`: an effect statement is prepared on the product's statement road, each String argument a
  literal folded (`Literals.fold`, the walk's own fold); a computed one is refused by name (the T9 kind M9 plans).
  The statements `EffectPlanner` writes join the open script; any other statement closes it, as the walk's segment
  rule does. A closed script is an `ExecutionPlan.Effect` whose script holds the statements, the dialect's bracket and
  abort, and whose text is the statements joined bare (the walk's `sendScript`). An effect let keeps the walk's guard
  (only connection reads may read it). An effect as the program's last statement answers as the walk's arm does
  (R17 C11): `true` for the DDL that drops and creates, no value otherwise. `loadCsvToDbTable` is refused by name
  (R17 C10); an effect under another expression (an effectful map) is refused by name (T9, M9).
- On both doors (R18, the planner's): `print`/`println` plan no step, a no-value constant when last;
  `connectionByElement` is a handle (no step before the last statement). On the test door a program ending on a
  handle (a setup returning its runtime) answers no value, as the walk does; the query door refuses a handle as an
  answer.

## The census

A research probe (`probes/M3dCensus.java.txt`, hooks `probes/m3d-hooks.diff.txt`; never committed as code) planned
every body the walk runs that holds an effect through `TestDoor.plan`, with the walk's runtime, features and server
version, and compared the planned `Effect` scripts with the scripts the walk's segments send, statement by
statement. Both corpora, both passes; both corpora 6/6 green with the probe on.

| per pass | DuckDB host | DuckDB database | H2 host | H2 database |
|---|---|---|---|---|
| bodies holding an effect | 754 | 754 | 754 | 754 |
| planned whole by the test door | 597 | 597 | 597 | 597 |
| ... scripts **byte-identical** to the walk's | **597** | **597** | **597** | **597** |
| ... scripts compared | 3,603 | 3,603 | 3,603 | 3,603 |
| ... statements compared | 109,162 | 109,162 | 109,162 | 109,162 |
| ... scripts differing | 0 | 0 | 0 | 0 |
| ... a planned body the walk failed | 0 | 0 | 0 | 0 |
| ... a bracket other than the walk dialect's | 0 | 0 | 0 | 0 |
| refused by name | 157 | 157 | 157 | 157 |

The refusals, each a kind a later sub-step plans:

| refusal | per pass | planned at |
|---|---|---|
| an assert | 116 | M6 (the verdict record) |
| a program ending on a runtime handle | 19 | **this sub-step** (folded after the census: the test door answers it no value) |
| a let-bound `execute(...)` | 17 | M3f |
| an effectful map | 4 | M9 (T9) |
| `loadCsvToDbTable` | 1 | its body's class (R17 C10) |

The 19 handle-ended programs (setups such as `meta::relational::graphFetch::tests::milestoning::setup`, which return
their runtime) were folded after the census ran; their scripts are compared in M3i's census re-run.

## Reading

- The test door plans the corpus's effects exactly as the walk sends them: on both databases, in both passes, every
  script of every body it plans is the walk's, statement for statement.
- No planned body's walk failed, so no failure ordering was exercised here; the runner's script failure path is
  pinned by `ProgramRunnerTest` (M3b).
- The core packages' sent statements are unchanged by M3d (the M1 method, recorded in the plan's §9).
