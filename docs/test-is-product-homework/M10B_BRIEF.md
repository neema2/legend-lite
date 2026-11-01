# M10b brief — the engine-text asserts: the test in the plan on every lane, the appeal a harness (2026-11-01)

The plan's §7 M10b (`REVALIDATION_2026_11_01.md` §2.4; the user, 2026-11-01: "I don't think we should bifurcate the h2
lane ... the test itself should be fully run in platform/plan ... then if test doesn't match everything else can be
done in java"; T11, T13; R6 the H2 referee replay stays the verdict source where our text differs from the golden; R10).
Written before the code (R15) from the code map `M10B_CODE_MAP.md` (every line number is aec8b44b's); its audit checks
the build against it.

## Today

- **The verdict is the TEXT; the rows are the appeal.** `SqlTextVerdicts` (1,720 lines) judges every engine-text
  assert the same way under either judge, because `AssertVerdicts.adjudicate` hands the three root arms
  (`assertSameSQL`, `assertSqlEquals` of the test-data generator, `assertEqualsH2Compatible`) and the `assertEquals`
  arm's producer shapes (`toSQLString`-family producer on one side; the frame's `sqlRemoveFormatting($result)`; the
  plan text of `executionPlan(...)`; a generator's fetch text) to it BEFORE the judge-mode dispatch (AV:300-384). The arm
  reads the two texts by ordinary evaluation (`JudgePlans.evalSide`, origin `REFEREE_OURS`: our text is the K-routine
  `EnginePlans.toSqlString`/`planToString` or the frame's executed SQL, folded at planning; the golden a literal),
  compares them byte for byte, and **a byte-equal text IS the verdict** (the user's ruling of 2026-09-21, option 1:
  STV:1394-1401, the TDG arm STV:1042-1045). On a failed text the ROWS LEG is the appeal: our rows by running the
  producer's own query on the lane's database (`VerdictQueries.fromWrapped(query, mapping, legCtx)` through
  `evalSide`), the golden replayed on the H2 referee seeded from the run's recorded ledger (`SqlReplayReferee.verify`,
  the harness's `spec/.../harness/ReplayReferee`), compared under the charter's §6 policy; MATCH passes whatever the
  text said, DIVERGED and FAULT fail, a decline (`oracle-declined`, `rows-underivable`) falls back to the text (failed
  here). Two declines are decided BEFORE the text compare and counted whether or not the texts agree: a foreign
  dialect (`dbType != H2`: text is the contract, STV:173-181) and a plan whose parameters cannot be bound
  (STV:866-872). No referee registered → the arm WALLS (production: no goldens). Under the DATABASE judge's batch the
  text equality is a ROW of the fused statement (`VerdictBatch.defer(name, true, VerdictSql.textEquals(golden, ours),
  session, appeal)`, STV:1381-1393) and the appeal runs at the flush where the row failed (`VerdictBatch.java:517-522`;
  M7's planning half keeps `Pending.appeal`). The charter's §0 ("rows, never text") describes the design before that
  ruling; the code is the ruling's.
- **The program road refuses them by name.** `AssertVerdicts.planHost` answers null for the three root FQNs
  (AV:702-706) and wherever `SqlTextVerdicts.mayArm` holds (AV:747), so `HostAsserts.plan` is null and the body stays on
  the walk tagged `host-unplanned-assert`; `DatabaseAsserts` through `databasePlansWithoutReading`, whose first gate is
  `planHost != null` (AV:154-157), tags `database-unplanned-assert`. Measured on the DuckDB host pass after M10a
  (`corpus2-body-stayed.tsv`, passing bodies): 1,131 `activities,assert,host-unplanned-assert,let-execute` (frame
  programs whose asserts compare the frame's SQL against a golden), 181 the same over temporal frames, 94
  `assert,host-unplanned-assert,system-read` (M10a's routed bodies), and part of 102 `assert,host-unplanned-assert`;
  the referee's census per pass: `verify8` MATCH 1,065 / DIVERGED 9 / DECLINED 6, `verifyFetchChain` MATCH 49,
  `verifyFetchTexts` MATCH 23, `verifyPlan` MATCH 7; statements `referee_ours` 1,188 over 1,066 tests,
  `referee_golden` 1,258 over 1,028.
- **The arms** (the map's §1, §6): `tryArm` (a `toSQLString`/`toSQLStringPretty`/`toNonExecutableSQLString` producer
  or the `toSQL(...).toSQLString(...)` receiver form on exactly one side; the dialect from the enum, the runtime or
  `unresolved`), `tryArmSameSql` (`assertSameSQL(golden, $result)`: our text the frame's executed SQL through
  `ResultEnvelopeSplice`, our rows `$result.values`), `tryArmDualGolden` (`assertEqualsH2Compatible(legacy, upgraded,
  actual)`: the upgraded golden alone; a declined route re-adjudicated as `assertEquals(upgraded, actual)` by AV:330-337),
  `tryArmExecRead` (`$res->sqlRemoveFormatting()`, `sql($res)`, indexed reads: the k-th statement let's rows, a
  population let's, or `$res.values` with in-list temp tables), `tryArmPlanText` (`executionPlan(...)`'s SQL node
  against a golden plan text; referee bindings for the parameters; `verifyPlan` for a multi-node plan),
  `tryArmTdgRoot`/`tryArmTdgSql`/`tdgRowsNow` (fetch text against fetch text; the rows leg entirely the referee's:
  `verifyFetchTexts`, `verifyFetchChain` with the generator's transcript re-run). Every finder is typed-tree navigation
  over the let prefix (`findProducer`, `findSqlRead`, `findPlanProducer`, `lookThroughPlanProducer`, `hasTdgProducer`,
  `frameMappingAndClass` through the splice hook); the rows leg needs `env` (the referee, the session, the trace, the
  listener, the batch) and the let prefix, the spec compiler and the hook (the map's §3).

## The ruling, restated as a design

On EVERY lane the same plan: the body's statements as the test door plans them; the query under test a plan-time text
(the routines `toSQLString`/`planToString`, the frame's activity SQL: constants the planner writes); our text against
the golden text a VERDICT STEP of the plan -- under the host judge two constant sides judged equal or not; under the
database judge the row `VerdictSql.textEquals(golden, ours)` fused into the segment's statement, as today's batch
already composes it. The verdict of the test is that comparison, as today (option 1). Where it FAILS, the APPEAL is the
harness's, outside the plan, at the verdict step: the rows leg exactly as today's (`rowsLegNow`, `tdgRowsNow`, the
plan replay), its outcome the verdict, its counting (`declined`, `refereed`, `sqlRoute`) at the step as the walk's at
the assert's turn. A matching text does not reach the appeal -- the pinned detail, corrected from the code: today's rule
is already the user's option 1, and it stays.

1. **The arms split in two halves (M6's shape for the host arms).** Each `tryArmX` becomes `planX` (the detection, the
   input extraction, the two text sides as `HostSides.Ask`s with `fold`, the pre-decided decline, and the APPEAL -- a
   sealed `SqlTextVerdicts.Appeal` holding the facts the rows leg needs: the read `fromWrapped(...)`, the replay SQL or
   plan, the mapping and class FQNs, the replay facts, the unpaged read, the temps, the lets' scope, the TDG hop; and
   the harness objects: the environment, the spec compiler, the splice hook) plus the shared verdict
   `SqlTextVerdicts.judge(golden, ours, decline, appeal)`: decline → counted, the text decides; equal → `ok()`; else
   `appeal.judge()`. The walk's `tryArmX` = `planX`, the sides fetched (`HostSides.fetch`), `judge` -- byte-identical in
   verdict, message and census. Where a text side evaluates to no string the arm declines as today (null) or walls (the
   two root arms).
2. **The host checks plan them.** `AssertVerdicts.planHost`: where the callee is a root FQN or `mayArm` holds,
   `SqlTextVerdicts.plan(fn, bare, letPrefix, specs, env, hook)` answers the `HostSides.Check(asks, HostJudgment.EngineText
   (name, decline, appeal))` or null (the arm's own null: the generic path, as today). `HostJudge.judge(EngineText e,
   sides)`: the two texts read off the sides (a constant's cell, or the one string cell a statement side answered),
   then `SqlTextVerdicts.judge(...)`. The appeal is the one named exception to "the host judgment is data" (R3: the
   host judge is the exception to the Java rule, not to the plan-execution split; T13: the referee replay outside the
   body's plan); it is built when the assert is PLANNED and run only at the step, only on a failed text.
3. **The database judge's road is today's batch.** `databasePlansWithoutReading` gains the engine-text branch ahead of
   the `planHost` gate: an assert of these shapes plans without reading (its texts are constants or planned sides; the
   appeal is deferred), so `DatabaseAsserts.plan`'s collecting batch takes the row `textEquals` with its appeal as the
   walk's does (`AssertVerdicts.planDatabase` → `adjudicate` → the arm → `batch.defer`), the segment's fused statement
   carries it, and `VerdictBatch.Planned.judge` runs the appeal where the row failed -- the walk's code path, unchanged.
   The planning-time evaluation of the two texts under the collecting batch sends nothing (constants; a frame's SQL the
   planned frame's chain rendered). A side the Pure computes by a query (a text read from the database) is a
   statement side, admitted by the host road and refused on the database road by name (the batch cannot read it).
4. **The generator carrier is a kind of its own.** `TypedTestDataGen` of the `generate` and `seedString` flavours is
   folded by the walk alone (`TestDataGenerationNatives.foldCensus` → `transcript` through the database); the program
   road has no fold. `BodyClass.anotherWay` answers `Kind.TDG_GENERATOR` (census tag `tdg-generator`) for it, as
   `CSV_CENSUS` since M10a; `sideKind` keeps it. T8 (the generator as a Keyed step) stays residue for M10c/M10d. The
   `plan` flavour is a value (`generatorPlanText`) and stays admitted. To confirm at build time: the two
   `TestDatGenForNestedViews` bodies moved at M9e and pass -- their generator result reaches an effect native's
   argument (`setUpDataSQLs`), not a verdict side; the new kind must not un-move a passing body without a reason named.
5. **The class rule otherwise changes nothing.** The assert program and the frame program already admit these bodies;
   the refusal was the planners'. The `*-unplanned-assert` tags shrink to the shapes M10c owns.
6. **The census.** The appeal's statements are sent by the harness under `REFEREE_OURS` / `REFEREE_GOLDEN`
   (`StatementOrigin.enter` inside the arm, as today), so the statement-origin census is identical; `body-router moved`
   grows by the bodies the planners now plan; `sql-text` routes, declines and referee outcomes identical per pass.
7. **The parity witness lives in `spec`**, where the real referee is (`harness/ReplayReferee`; core's test tree has
   none and should not grow a second compare policy): `EngineTextParityTest` runs the frame model's bodies through
   `Execution.runProgram` on the walk and on the program road under both judges with `ReplayReferee` registered --
   `assertSameSQL(golden, toSQLString({|x::Firm.all()->project(...)}, x::M, DatabaseType.H2, []))` with the golden our
   own text (equal: no replay), a golden spelled differently with the same rows (MATCH: pass, the decline/referee census
   identical), a golden with different rows (DIVERGED: fail, the message byte-identical), a foreign dialect
   (`DatabaseType.Snowflake`: declined, text the contract), a frame's `$result->sqlRemoveFormatting()` under
   `assertEquals` and `assertEqualsH2Compatible`, a `planToString`/`executionPlan` SQL-node golden -- outcomes,
   verdicts, listener events and the statement-origin census compared as `BodyRouterParityTest` compares them.

## What moves and what is measured

The host pass: the `host-unplanned-assert` bodies whose other kinds are admitted (the 1,131 + 181 + 94 above and the
engine-text part of the 102), less the generator-carrier bodies; the database pass likewise. Outcomes identical on
DuckDB and H2 (both judges, every register test); the referee-outcome census and the declines identical per pass; the
statement-origin census identical but for M10a's pattern. Measured on both corpora after the step; the chain (core ids,
PCT DuckDB, both corpora, channel B, `//gates:local`, Postgres unprivileged) at M10's end.

## Residue, expected

The referee replay, the comparison policy, the plan replayer and the TDG transcript stay the harness's (T13, R6); the
foreign-dialect and `unresolved`-dialect texts stay the contract, counted (the charter's §4); a declined golden a
counted decline; the generator carrier's bodies on the walk (T8, M10c/M10d). `SqlTextVerdicts` keeps its detection (now
a planning half) and its rows leg (now the appeal); it shrinks by nothing M10b names. The engine-text asserts inside
shapes M10c owns (a quantified form, the cell pool) wait for M10c. The charter's §0 sentence is corrected to the ruling
of 2026-09-21 when M10b's records land.
