# The result boundary program: start here (rewritten 2026-10-28)

For a session picking this up with no context, from any account. This page is the whole roadmap: what the program is,
exactly where it stands, every remaining piece of work in order with a brief for each, how we work, how to run every
check, how to raise the lanes that run the other databases, the traps already hit, what was never in the
repository, and the message that opens a new session (§12). Its evidence (the audit, the maps of the code, the
measurements, every probe's source) is `docs/result-boundary-homework/README.md`; its rulings are the program,
`docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md` §8. The handoff of 2026-10-26 this page replaces is in git history
(`f631cffc`).

## 0. In one minute

- **The goal.** A query answers typed values computed by the database; nothing in SQL formats output; each consumer
  writes its own form at the edge: legend-engine's exact JSON for Studio and legacy clients (the LEGACY renderer), lite's
  lossless JSON, CSV and Arrow for its own (the LEAN renderer). One way to run a query or a test body: a plan, run by
  the plan runner, into a writer. Every other path deleted.
- **Where it stands (2026-10-29).** Every route runs on plans: production, PCT's channels A and B (a relation crosses
  exactly as legend-engine's own PCT adapter reads it), our own core tests (the LEAN twin, per lane: track T), the
  core_relational corpus and the service test runner (track C) -- a query through `Execution.run`, a test body
  through `Execution.runProgram` (track B). The old door (`Execution.execute*`, `StatementExecutor`'s JDBC, the
  materialized `ExecutionResult`) is deleted (track D); mixed lists are typed values (L); the Arrow writer is at the boundary (A); the open findings are settled or posted (F); the final audit is folded (Z, 2026-10-29). Left: nothing of the tracks -- the user's rulings (§10a) and the landing. (As written before Z: mixed lists as typed values (L), the Arrow writer (A),
  the open findings (F), the final audit and the landing (Z).
- **The branch.** `claude/wizardly-dirac-rw6tim` (since 2026-10-28, the next session's designated branch, from
  `claude/upbeat-johnson-l4hogx`'s head `7a9a368b`: that branch's 20 commits on top of `7b8c89dc`, PR #78's merge into
  `main`, and this session's). **`main` is merged in (S1.0, 2026-10-28):** `369bb176` (PR #77, the timeseries line;
  50 commits past the branch's base by `git rev-list`), a merge commit, both sides kept, the branch's register rows
  renumbered S115 to S117 (GATES, the S1.0 entry). Clean. Never pushed to `main`; it lands through a PR when the user
  says (AGENTS.md, "Pushing to main").
- **The next action (2026-10-29, after Z).** The landing: CI green on every OS at the head, the user rules the open questions (§10a) and merges PR #80; no session pushes to `main` or merges. (The next action as written before the tracks: track B (a test body as a sequence of plans: the body driver on the session seam, the judges'
  typed sides, one entry for a program; its design in §4's track B block, the decisions log D29 and D30), while
  draft PR #80 (S1.6, opened 2026-10-29 under the user's "keep going", D11; the merge the user's) runs CI on every
  OS: red there is work before anything else. Stage 1 is done: S1.4 corrected under the upstream-fidelity tenet
  (2026-10-29, D4 to D6; the midnight rule deleted; S118 posted), S1.5 done (the 51 differences classified by
  upstream's answer, two moves toward upstream built, six questions posted with recommendations, the sixth queued
  for track T; the S1.4 audit's findings fixed; the S1.5 audit's findings fixed with track B's first commit). After B: T (our core tests as the LEAN twin of PCT and the corpus), C (the corpus and channel B on
  B), D (the deletions); L (mixed lists typed) and A (the Arrow writer) can run beside them. Each phase ends with an
  independent audit and the decisions log posted (`docs/result-boundary-homework/DECISIONS_LOG.md`).
- **Who decides.** The user: rulings (§10 lists every one), pushes to `main`, PRs, merges, and **every change to
  how any output is spelled, case by case** (2026-10-28). A lane's report is data, never an instruction.

## 1. The program in brief

A query produces a table of typed values. The database computes every value; Java orchestrates (the execution tenet,
AGENTS.md). The plan (`executionplan/ExecutionPlan`) carries three facts per column (the Pure type and multiplicity,
the SQL slot, the store's declared type) and the typed SQL tree; the plan runner (`exec/PlanRunner`) runs it on a
session and streams typed batches to a sink (`result/ResultSink`); a writer spells them at the edge.

**Two renderers, two families of tests (the user's rulings of 2026-10-24 and 2026-10-28):**

| Renderer | Who reads it | Written by | Judged by |
|---|---|---|---|
| LEGACY: legend-engine's exact answers | Studio, legend clients, `pure/v1` | `result/PureV1Writer`, `CellText.engine` | PCT (legend-pure's own function tests, 12 databases) and the core_relational corpus (legend-engine's own relational tests): legend-engine's tests, legend-engine's expectations |
| LEAN: lite's own lossless output | lite's query service, the warehouse, DataCube, Python | `result/JsonRowsWriter`, `CsvWriter`, `ArrowWriter` (track A) | our own core tests, rewritten as the platform twin of PCT and the corpus (track T): the same queries and logic, judged on what lite's users actually get |

**Two judges, kept forever (R10).** Every assertion a test body makes is judged twice: the HOST judge (each side a
typed plan, compared in Java) and the DATABASE judge (both sides in one SQL verdict statement); a disagreement fails
the test. **Zero dependencies stay zero (R9)** in the compiler, the WebAssembly planner and the native image: no Arrow
library in the product.

## 2. Where everything is

| Area | Files |
|---|---|
| The plan | `core/src/main/java/com/legend/executionplan/ExecutionPlan.java` (nodes `Sequence`, `TdsResult`, `Keyed`, `Update`, `Check`, `Effect`; `Column`), `ColumnFacts.java`, `PlanJson.java` (format 18; `PlanJsonTest` pins it), `MirJson.java`; `com/legend/PlanMaker.java`, `TypedQuery.java` (`executionPlan(...)`), `plan/QueryPlan.java`, `plan/UpstreamRelationType.java` (legend-engine's spellings of a column) |
| Running a plan | `exec/PlanRunner.java` (`stream`, `rows`, `update`, `effect`), `exec/JdbcStatements.java` (the JDBC boundary; stamps the trace comment), `exec/JdbcDecode.java` (a JDBC cell by the plan's facts; `Driver` holds the drivers' quirks as facts), `exec/PlanSessions.java`, `exec/NonFiniteJson.java`, `core/src/main/mongodb/.../MongoDbDecode.java` |
| The result and the writers | `result/`: `ResultSink`, `Batch`, `Batcher`, `Collector` (collect into memory), `RowDecode` (a cell from a JSON row), `HostRows` (rows a host ran), `CellText` (every cell spelling: Pure's print forms, legend-engine's tokens, lite's), `PureV1Writer` (LEGACY), `JsonRowsWriter` and `CsvWriter` (LEAN) |
| The production entry | `Execution.run(...)` (plan, run, stream to the sink a caller makes from the plan); `server/PureV1Api.java` (`engineWriter`, `jsonPlan`, `arrowPlan`, `execute`); the HTTP door's runner `server/ConnectionResolver.RUNNER` over `Execution.run` (`QueryService` and its streaming text routes deleted 2026-10-30, D67) |
| The old door (DELETED, track D, 2026-10-29) | `Execution.execute*`, `executeResolved`, the materialized `ExecutionResult`, `QueryService`'s three materialized `execute` methods: gone; `judge/TypedSide` is the judges' collected side, `driver/BodyDriver` (`StatementExecutor` until then; the root's until step C) the body driver behind `Execution.runProgram` |
| The judges | `driver/HostJudge`, `driver/DatabaseJudge`, `driver/AgreementArm`, `driver/AssertVerdicts`, `driver/VerdictArm`, `driver/JudgePlans` (each side a plan), `driver/SqlTextVerdicts` (the root's until step C, D73), `judge/VerdictBatch`, `judge/TdsCompare`, `lowering/CanonicalRenderSql` (the database judge's canon; stays) |
| The PCT harness | `pct/src/test/java/org/finos/legend/lite/pct/extension/`: `PctExecuteNative` (the native the reference calls), `PctPlanRoute` (plan, run; a relation through `PureV1Api.engineWriter`), `ValueBridge` (`engineTDS` for a relation, `values` for a value), `PctBackend` (the lane's sessions, `LEGENDLITE_PCT_BACKEND`); `pct/src/main/resources/core_legend_lite_pct/pct_adapter.pure` (the Pure adapter; `engineTDS` = legend-engine's `getTDSResultFromProtocol`), `pct_types.pure`; `pct/.../channelb/ChannelB.java` (channel B); the ledgers `Test_LegendLite_<Lane>_<Suite>_PCT.java` |
| The corpus | `spec/src/test/java/com/legend/rcorpus/` (`MinimalCorpus`, `JudgeLedger`), its rosters `spec/src/test/resources/rcorpus/`, the referee `spec/src/test/java/com/legend/harness/ReplayReferee.java`, `RefereeVerify.java` |
| What the twin (track T) changes | `core/src/test/java/com/legend/integration/AbstractDatabaseTest.java` (28 subclasses), and the other helpers on the old door (`CheckerResults.collect`, `SqlParity`, `CypherParity`, `IqParity`, `PlanCases`, `testing/src/main/java/com/legend/testing/Rows.java` (the `//testing` module; 23 call sites in 7 test files -- a row-sorting and multiset-comparing helper that executes nothing, the Z audit's correction); the census `docs/result-boundary-homework/test-census-2026-10-28.tsv` |
| Mixed values (track L, built) | `sql/SqlExpr.java` (`TaggedList`) and `SqlType.java` (`Tagged`), `lowering/TaggedLists.java` (every operation), `lowering/MixedEncoding.java`, `lowering/ValueCollections.java` (a relation's cells), `lowering/Scalars.java`, `lowering/Lowerer.java`, `lowering/Fold.java` (`conformValueEgress`, kept by D51), `result/TaggedCell.java` and `Batch.tag` (track F), `exec/TaggedListTest`, `exec/TaggedSelectionTest`. (Before L: `lowering/LiteralSpelling.java`'s carrier, `LiteralText`, the `LITERAL` slot's 94 sites -- deleted at L2) |
| The Arrow framer and writer (track A, built) | `arrow_ipc/` (`FlatBuilder`, `ArrowType`, `ArrowField`, `ArrowIpc`, `ZstdFrame`; on `//base` alone), `result/ArrowWriter.java`, `server/PureV1Api.java` (`executeArrow`, `arrowAnswer`), the hosts' `executeArrowAnswer`; the gates `core/src/test/java/com/legend/result/ArrowWriterTest.java` with `core/src/test/python/arrow_matches_rows.py`, `tools/deps/ArrowClosureTest.java`; the warehouse's `ArrowStreams.java` delegates its framing (its judge `warehouse/src/test/python/arrow_matches_json.py` unchanged) |
| legend-pure's source, readable (spec only, never a runtime input) | Bazel's external repository `$(bazel info output_base)/external/+http_archive+legend_pure_src` (5.99.0): e.g. `legend-pure-runtime/legend-pure-runtime-java-engine-compiled/.../CompiledSupport.java` (a Float's text), `legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure/` (the PCT functions and their tests); what it says: the homework README §7 |
| legend-engine's answers, recorded | `core/src/test/resources/upstream-api/` (e1, e5, e8, e9 answers; the Variant probe's `e8-variant-*.json` and `variant-probe.pure`), judged by `server/PureV1ApiTest` |
| The engine itself, runnable | `tools/engine-runner` (legend-engine 4.145.0's jars, `@maven_runner`); how to ask it anything: the homework README §7 |
| The guards that pin this line | `JavaEvalLedgerTest` (exact line pins, the exec register), `ArchitectureTest`, `CodeShapeGuardrailTest`, `TenetRatchetTest`, `DialectLeakRatchetTest` with `DatabaseOwners` (no database named outside its owners), `ParkedWorkLedgerTest`, `NoSaltedIterationOrderTest` |
| Documents | this page; the homework folder; the program (§8 rulings, §9 status); its design `docs/RESULTS_AND_RENDERERS_2026_10_22.md`; `docs/PLAN_AND_RESULT_MODEL_2026_10_18.md`; `docs/PLAN_PATH_PROGRAM_2026_10_17.md` (§5 lets, §6 judges, §7 steps 8 to 11 are tracks B, C, D here); `docs/SEMANTICS_REGISTER.md`; `docs/GATES.md`; `docs/PARKED_WORK_LEDGER.md`; `docs/CLOUD_BACKENDS_PCT_2026_10_11.md`; `docs/CLOUD_SESSIONS.md` |

## 3. Where it stands (2026-10-29)

**The program's steps, the phases that did them, and the commits** (the phases are the user's 2026-10-27 execution
order; the steps are the program's §5):

| Program step | Phase / stage item | Commits | State |
|---|---|---|---|
| 0. Measure | (the clouds' M5 to M8, 2026-10-26) | GATES 2026-10-26 | half: M1 to M4 never recorded; the lanes' per-test baselines of S1.1 replaced them as the gate |
| 1. Typed results, the batch stream | phase 1 | `51688f36` | done |
| 2. The shared Arrow framer | track A | `1e3065d4` | done 2026-10-29: `//arrow_ipc`, the warehouse delegating; no Arrow library in any product closure |
| 3. The legacy writer in Java; the SQL-written wires deleted | phases 2, 3 | `12b89c7d`, `1b0305f7`, `53cf5b6d` | done |
| 4. PCT from typed rows; the TDS text deleted | phase 4; S1.3 | `b3311cd6`; `ac4bb7d3`, `6811256b`, `4eaf1b07`, `d8ab8434` | done: a relation crosses as legend-engine's adapter reads it |
| 5. The lean writers | phase 2 (JSON, CSV); track A (Arrow) | `12b89c7d`; `1e3065d4` | JSON, CSV and Arrow done; `conformValueEgress` not deleted (D51, the user's call) |
| 6. Mixed lists typed | track L | `63ddd3f4`, `a93806d3`, `38f5e367` | done 2026-10-29: tagged lists; the LITERAL carrier deleted; plan format 19 |
| 7. The old executor goes | phase 5; S1.2; track D | `0fe63b42`, `f883d42b`; `6f4f654b`, `132a4ff2` | done 2026-10-29 (track D): `exec.Executor` deleted (phase 5); the old door, the materialized result and the service's materialized `execute` deleted; `StatementExecutor` is `BodyDriver`, the judges' orchestration behind `Execution.runProgram`, every statement through its plan (track B) |
| 8. Optional deletions | -- | -- | parked for the user (the engine-style SQL-text dialects) |

**The audit's three stages** (2026-10-27, accepted with "keep it, and keep going"): stage 1 S1.1 to S1.3 done, S1.4 to
S1.6 next (§4); stage 2 is tracks B, C and D; stage 3 is tracks L and A and the final audit (Z). All three stages'
work is on the branch as of `1e3065d4` (2026-10-29: tracks B, T, C, D, L, A and F landed; Z's audit running); the
audit itself and what it found: the homework README §1.

**The last measurements.**
- The DuckDB driver bump (2026-10-29, D60; GATES, the D60 entry): the full local chain on 1.5.6.0 alone, green, no
  PCT or corpus pin flipped; the as-of test asserts the spec on every lane.
- Doors 3, 4 and 5 (2026-10-29, D61; GATES, the doors 3-5 entry): the full local chain on the patch alone.
- Door 2 (2026-10-29, D62; GATES, the door 2 entry): the full local chain on the patch alone.
- Door 1 (2026-10-29, D63; GATES, the door 1 entry): the full local chain on the patch alone.
- The doors' audit fold (2026-10-29, D64; GATES, the fold entry): the full local chain on the patch alone.
- The three routings (2026-10-29, D65; GATES, the three routings' entry): the full local chain on the patch alone.
- The java.sql revamp, step 1 (2026-10-30, D66; GATES, the step 1 entry): the full local chain on the patch alone.
- The java.sql revamp, step 2a (2026-10-30, D67; GATES, the step 2a entry): the full local chain on the patch alone.
- The java.sql revamp, step 2b (2026-10-30, D68; GATES, the step 2b entry): the full local chain on the patch alone.
- The java.sql revamp, step 3 (2026-10-30, D69; GATES, the step 3 entry): the full local chain on the patch alone.
- The seam's step A (2026-10-30, D70; GATES, step A's entry): the full local chain on the patch alone.
- `f883d42b` (phase 5 green), six lane sessions against `7b8c89dc`, per test: channel A exact on Postgres, SQLite,
  MariaDB, MySQL, ClickHouse, Trino, H2, BigQuery, Databricks and Snowflake; channel B identical (one MySQL gain); one
  pin open (Snowflake's `testAdjustByMinutesBigNumber`, a driver limit).
- `e8c341d0`, the engine crossing (channel A, every suite, every lane): exact everywhere; each lane's log proves the
  route (460 or 459 relation crossings, 64 standard). Switched at `d8ab8434`.
- Locally at `d8ab8434`: PCT relation and standard on DuckDB, SQLite and MongoDB, `//pct:pct_discipline`,
  `//core:census`, `//core:guardrails`; at `4eaf1b07`/`0b06545e`: `//core:core_tests_server`, `//core:core_tests_root`,
  `//datacube:python_execute_test`. **Not run since phase 5: the local gate, CI on any OS.**
- `b944a42e` (the negative zero) locally: `//core:core_tests` (all 25 packages), `//core:guardrails`, `//core:census`
  green. Not run: `//datacube:python_execute_test` (it reads LEAN JSON), PCT and the lanes (neither PCT channel, the
  corpus nor the judges read the LEAN writers: channel A reads legend-engine's JSON and typed values).
- S1.4 (2026-10-28, `f58f37ed`) locally: `//core:core_tests` (all 25 packages), `//core:guardrails`, `//core:census`, `//pct:pct_discipline` (28 of 28); PCT channel A, all five suites, on DuckDB, SQLite and MongoDB, exact, the engine crossing counted (460 / 64, 459 / 64, 459 / 64); `//datacube:python_execute_test`. Not the lanes (unreachable), not the local gate (GATES, the S1.4 entry).
- S1.0's merge commit (2026-10-28, `main` at `369bb176` in) locally: `//core:core_tests` (all 25 packages),
  `//core:guardrails`, `//core:census`, `//pct:pct_discipline`; PCT channel A, all five suites, on DuckDB, SQLite and
  MongoDB, exact, the engine crossing counted (460 / 64, 459 / 64, 459 / 64); `//datacube:python_execute_test`; the
  local gate (GATES, the S1.0 entry). **Not the lanes:** the six lane sessions of §7 answer "not found" from the next
  session, and no environment with the clouds' credentials is visible to it (the merge changes nothing they measure).
- S1.4's correction (2026-10-29, `d102b35c`) locally, as root: `//core:core_tests` (all 25 packages), `//core:guardrails`,
  `//core:census`, `//pct:pct_discipline` (28 of 28); PCT channel A, all five suites, on DuckDB, SQLite and MongoDB
  (engine-JSON crossings 524 / 523 / 523; the relation suite 460 / 459 / 459); `//datacube:python_execute_test`. Not
  the local gate (last run at S1.0's merge commit: the S1.4 audit caught a GATES sentence claiming otherwise).
- S1.5's code commit (2026-10-29, `d314455a`): GATES, "S1.5's gate, measured at d314455a" (the local gate 446 targets,
  436 pass, 1 skipped, the 9 Postgres targets unprivileged 9 of 9; PCT channel A on DuckDB, SQLite and MongoDB 16 of 16;
  the Python route).
- Track B's B1a (`df91f816`) and B1b (`6cf0e83b`), 2026-10-29, each: `//core:core_tests`, `//core:guardrails`,
  `//core:census`, `//pct:pct_discipline`, `//core:mongodb_plan_test` 29 of 29; `//spec:judge_lanes`,
  `//gates:corpus_duckdb`, `//gates:corpus_h2` 12 of 12 (the rosters unchanged); PCT channel B x5 on DuckDB,
  `//pct:pct_h2`, channel A x5 on DuckDB, SQLite and MongoDB 21 of 21; the nine Postgres targets unprivileged 9 of 9;
  `//datacube:python_execute_test` (GATES, "B1a's gate" and "B1b's gate"). The second merge of `main` (`f4ce65b2`): the
  local gate with the parser-equivalence lane, 454 targets (GATES, "the second merge's gate recorded"); CI on PR #80
  green on every finished lane at that head (47 of 50 when the B1b push superseded the run).
- Tracks B2, T, C, D, L, A and F (2026-10-29; B2 `cda191e8`; T-a to T-d2 `39a2a657`, `9ca7482f`, `62837d67`, `32243d80`,
  `e767c57b`, `d8d05bad`; C `643f1bbe`; D `6f4f654b`, `132a4ff2`; `L1` `63ddd3f4`, `L2` `a93806d3`, the L2 audit's fold
  `38f5e367`; tracks A and F `1e3065d4`; Z's fold the commit after), each: the full local chain (GATES, each entry's "Measured locally before the commit"): `//json:tests`,
  `//:generated`, `//gates:checks`, `//core:core_tests`, `//core:core_tests_lanes`, `//core:guardrails`, `//core:census`,
  `//pct:pct_discipline`, `//core:mongodb_plan_test`, the four DuckDB parity lanes; the nine Postgres targets; the two
  corpus gates and `//spec:judge_lanes`; the 21 PCT lanes; the five Postgres PCT lanes; the readers (`//engine-client`,
  `//datacube`, `//wasm`, `//python:bindings_test`, `//warehouse:tests`, the Arrow gate and the two closure tests);
  `//datacube:python_execute_test`; the unlaned-tests guard. `//python:engine_test` fails in the cloud container at the
  headless host's V8 (a segmentation fault before the test body; CI judges it). CI on PR #80 at `38f5e367`: green on
  every Linux, Windows and ARM lane; macOS lanes cancelled runner-less (infrastructure).

**Who still entered through the old door** (measured 2026-10-28, before tracks C, T and D moved them and deleted
the door; production: nobody):

| Caller | Where |
|---|---|
| the core_relational corpus | `MinimalCorpus` -> `PureTestRunner` -> `Execution.executeResolved` (bodies and setups) |
| PCT channel B | `ChannelB.java` -> `Execution.executeResolved(..., JudgeMode.BOTH)` |
| the service test runner | `ServiceTestRunner.runOne` -> `Execution.executeResolved`; `serialize(ExecutionResult, ...)` |
| the corpus referee | `ReplayReferee`, `RefereeVerify` read `ExecutionResult` |
| core's tests | 140 files, 2,304 test methods (1,450 assert values), 609 direct call sites; most through `AbstractDatabaseTest` |
| main code naming `ExecutionResult` | 25 files: the judges, `StatementExecutor`, `BodyCompiler`, `PlanEnvelope`, `CsvLoad`, `SeedSqlForms`, `LiteralFold`, `AssertErrorNative`, `TestDataGenerationNatives.planTextResult`, `exec/VerdictBatch`, `exec/SqlReplayReferee`, `exec/TdsCompare`, `plan/ResultShape`, `server/QueryService` |

## 4. The roadmap, in order

Dependencies: S1.0 -> S1.4 -> S1.5 -> S1.6, first. Then B -> C -> D (D needs T as well). T can start right after
S1.6: of its 1,450 value tests only 20 use a `let` (up to 145 hold a `;`, mostly inside data strings: check those per
file), so nearly all are single queries that need nothing from B. L, A and F can run beside the others. Z last.
Every item follows §5's rules: announce in `docs/IN_FLIGHT.md`, measure first, run both, switch, delete in the same
step, records with the code, every lane per test.

### Stage 1, finish

**S1.0 Bring `main` in; renumber the branch's register rows (first; added 2026-10-28). DONE 2026-10-28** (GATES, the
S1.0 entry: the merge commit, two content conflicts and one unmarked, both sides kept; S112 to S114 → S115 to S117,
28 citations; the gate below green but for the lanes, unreachable from the next session). Why: `main` is 105 commits past
the branch's base (`7b8c89dc` -> `369bb176`, PR #77's timeseries line, which had itself merged `7b8c89dc`), the merge
only grows, and S1.6's PR needs it anyway. Build:
1. `git fetch origin main`, then `git merge origin/main` into the branch (a merge commit, never a rebase: the lanes
   check out the branch's commits). Files changed on both sides (2026-10-28): `core/BUILD.bazel`, `lowering/Fold.java`,
   `lowering/Lowerer.java`, `server/PureV1Api.java`, `ArchitectureTest.java`, `CodeShapeGuardrailTest.java`,
   `integration/SelectDistinctCheckerTest.java`, `server/PureV1ApiTest.java`, `docs/IN_FLIGHT.md`,
   `docs/SEMANTICS_REGISTER.md`, `python/legend_lite/engine.py`, `wasm/src/main/java/planner/Boundary.java`. Keep both
   sides' intent; regenerate generated files with the repository's tooling, never by hand; a guard pin the merge moves
   (`JavaEvalLedgerTest`'s exact line pins, `ArchitectureTest`'s and `CodeShapeGuardrailTest`'s allowlists) moves with
   a dated note naming the merge.
2. **The register ids collide.** `main` took S112 (a user function generic over relations), S113 (`without`) and S114
   (`read`), cited across `compiler/spec/`, `builtin/Pure.java`, DataCube and its documents. The branch's S112 (a
   Variant cell keeps the database's digits), S113 (`toVariant()` of an empty value, pending) and S114 (the negative
   zero) take the next free ids (S115 to S117 if `main` has added none since), and every citation of the branch's moves
   with them: `result/CellText.java` (two javadocs), `ResultWritersTest`, `server/PureV1ApiTest`,
   `datacube/test/python-engine/execute.test.ts`, `docs/GATES.md` (the PARK-34 and S1.4 entries),
   `docs/PARKED_WORK_LEDGER.md` (PARK-34), the program doc, this page, the homework README. Run
   `git grep -nE '\bS11[234]\b'` on the merged tree, read each hit, renumber only the branch's.
Gate: `//core:core_tests`, `//core:guardrails`, `//core:census`, `//pct:pct_discipline`, PCT on DuckDB, SQLite and
MongoDB, `//datacube:python_execute_test`; then the local gate; then the lanes, channels A and B per test against their
last logs (§7), before S1.4's code. Record: a GATES entry (the merge, the conflicts and how each was kept, the
renumbering), IN_FLIGHT.

**S1.4 The writers carry, never decide. DONE 2026-10-28** (GATES, the S1.4 entry: the `0.00010` fix; the
written-precision fact `ExecutionPlan.Column.precision`, plan format 17, the writer's slot-word matches gone, the
decoders' slot dispatch kept as their decode instruction; `ArchitectureTest.writersAreCarriageOnly`,
`WriterCarriageGuardTest` with its register of 58 named representation operations, the result package a closed
register in `JavaEvalLedgerTest`; the nits). **Corrected 2026-10-29 under the upstream-fidelity tenet** (the user: a
move toward upstream needs no asking; and "have you checked pct and core-relational? Are you sure it's always 9
digits expected?" -- checked, and no: the decisions log D4 to D6, `docs/result-boundary-homework/DECISIONS_LOG.md`):
- **A relation's time-bearing date cell in lite's own outputs is spelled as legend-engine's TDS spells it, nine
  fraction digits whatever carried it** (INSTANT, `PureDateLiteral.toEngineJson`; a literal written into a column
  too):
  legend-engine's `ValueTransformer` (its JSON and transformed CSV) and legend-pure's `fromSQLTimestamp` spell a SQL
  timestamp in full, and the engine's own `core_relational` tests expect it so for database-read timestamps (138
  places). The writer's trim for relation cells and its midnight rule (a Date's midnight as its day: no upstream prints
  a DateTime so; the PCT relation tests' `Date[1]` midnights read `T00:00:00...`) are deleted. What the PCT relation
  suite asserts textually -- three digits (`over.pure` 416 of them, `asofjoin.pure` 82) -- is its harness's own:
  legend-engine's `TestTDS` holds a date column as millisecond `java.util.Date`s (`TestTDS.java:209`), so its text
  shows three digits whatever the adapter sends; lite's deleted `PureTdsText` had copied that format.
- **A value root's TIMESTAMP cell stays at the precision its digits show** (MINIMAL, `PureDateLiteral.trimmed`), as the
  value lane's canon (`JudgePlans.valueLane`, `ValueBridge.values`) has carried it since 2026-10-18: the written
  precision legend-pure prints is lost in the database, and the trim is its text wherever no trailing zero was written
  (every PCT value test, exact on every lane); legend-engine's SQL answer would carry nine digits. Register S118; the
  user may rule nine digits instead (one line in `ColumnFacts.precision`).
- Plan format 18 (`MINIMAL`); `ColumnFacts.precision` takes the result's shape; `PlanColumns.shapeOf`.
- **Still the user's:** the tab's negative zero (D8); LEAN's nested dates against the engine's graph-fetch JSON (nine
  digits, no zone: D9) and LEAN's Float exponent layout against the engine's JSON (D10), for S1.5.
Why: rule R3; the audit found the writers comparing values and choosing
spellings by rules, unguarded. Build:
1. One typed fact on the plan column for a temporal value's written precision, replacing the slot-name strings
   (`"TEMPORAL_TEXT"`, `"LITERAL"`) matched in `exec/JdbcDecode`, `result/RowDecode`, `result/CellText` and
   `MongoDbDecode`: a field of `ExecutionPlan.Column` (a plan format bump; `PlanJsonTest`, `MirJson`), set by
   `ColumnFacts`, read by the decoders and the writers.
2. A Float's text in lite's own outputs (`CellText.pureFloat`, which LEAN JSON and CSV use; not LEGACY):
   - **`-0.0`: ruled and built 2026-10-28** (`b944a42e`; the user's "okay"; register S117, S114 until S1.0). A
     negative zero is written `-0.0`, the shortest text that reads back as the same double (the Float rule of
     2026-10-26), as the LEGACY writer and Arrow carry it; Pure's `toString` of a Float stays `0.0` for either sign, as
     legend-pure's own and lite's SQL (`FloatCanonText.ofText`, "ZEROS UNIFY") write it. `pureFloat` has no zero
     branch now.
   - **`0.00010`: ruled a defect to fix, 2026-10-28** ("keep the fix, fix the plan wording including adding that we
     need to fix 0.00010"); **built 2026-10-28** (GATES, the S1.4 entry, item 1). `pureFloat` laid out `Double.toString`'s exponent form with
     `new BigDecimal(shortest).toPlainString()`, which keeps the mantissa's scale, so a double below `1e-3` whose
     shortest digits are one digit gets a trailing zero: `1.0E-4` is written `0.00010`, `1.0E-5` `0.000010`, `-1.0E-4`
     `-0.00010`. legend-pure writes `0.0001` (`CompiledSupport.primitiveToString`'s `DecimalFormat`), and `0.00010` is
     not the Float rule's shortest digits. The fix, measured in a scratch program (2026-10-28,
     `docs/result-boundary-homework/probes/FloatTextProbe.java.txt`) against legend-pure's own formatter over `1e-4`,
     `1e-5`, `2e-5`, `1.2345e-5`, `-1e-4`, `1e7`, `1.5e15`, `2.5e-7`, `0.001`, `0.1` and a third:
     `new BigDecimal(shortest).stripTrailingZeros().toPlainString()`, then `.0` where there is no point; equal to
     legend-pure on every one but the negative zero (ruled above). Pin those values in
     `ResultWritersTest.aFloatPrintsItsShortestDigitsWithAPoint_neverAnExponent`. `stripTrailingZeros` here is exact (a
     representation operation, no rounding) and item 3's scan lists it by name. LEGACY is untouched: `CellText.engine`
     writes `Double.toString`'s own `1.0E-4`. Records: the GATES entry; no register row (it restores legend-pure's
     text).
3. The two guards the program names (R3), **approved by the user 2026-10-28** ("yes add this to the plan", after the
   explanation below), built right after item 1 so the precision fact removes the slot-word matches they would flag:
   `ArchitectureTest.writersAreCarriageOnly`, a dependency rule (the `result` package reads the plan's column facts
   and the typed batch, and no `compiler`, `lowering`, `sql` or `exec` class: true today, the rule keeps it true), and
   `WriterCarriageGuardTest`, a source scan of `result/` for arithmetic and comparison on numbers and dates, zero
   allowed, with representation operations (a scale set without rounding, a decimal's plain digits) listed by name.
   What the scan finds today, each to be fixed by a typed fact or put to the user: `dateOnly`'s `midnight(d)` (an
   abstract Date at midnight printed as its day: the writer guessing the written precision a typed fact should carry,
   item 1); `engineDay`'s and `dateOnly`'s `atLeast(HOUR)` and slot words compared as strings; `stripTrailingZeros` on
   a Float held as a decimal (exact, like item 2's: a representation operation to list by name). Also the `result`
   package as a closed register in `JavaEvalLedgerTest`, like `exec`. What they are for, as put to the user: R3 says
   a writer only spells the typed value it was handed, so swapping one writer for another changes no value; a writer
   that compares or computes a value is deciding output the plan never typed, and nothing would show it but a judge
   that happens to cover the case (the midnight rule `6811256b` fixed was caught by PCT only by luck of a fixture).
   Limits: the scan catches drift, not a determined workaround, like every guard here. An allowlist entry moves only
   with a dated justification (AGENTS.md, "Guard files are load-bearing").
4. The phase 3 audit's nits (homework §9); any that changes an output's spelling goes to the user first.
Gate: `//core:core_tests`, the guards, PCT on DuckDB, SQLite, MongoDB, `//datacube:python_execute_test`; the lanes
channel A. Trap: a guard that rejects the change is usually right. Order: item 2's `0.00010` fix can go first (one
function and its test); item 1 before item 3, so the guards land on writers that already pass them.

**S1.5 The missing records. DONE 2026-10-29** (the records commit `3e9e739a` and the code commit after it; GATES, the
S1.5 entry; the classification `docs/result-boundary-homework/WRITER_DIFFERENCES_BY_UPSTREAM_2026_10_29.md`; the
decisions log D12 to D22 and the S1.4 audit's findings D23 to D27; register S119, S120; two moves toward upstream
built: an hour-only date without a zone, a LEGACY NaN cell as `null`). The brief as it was: GATES entries for phases
1 to 4 (each commit's built, deleted, measured; the lanes' S1.1
numbers are in GATES 2026-10-27's phase 5 entry, item 3); the 51 writer differences
(`docs/result-boundary-homework/wire-differences-2026-10-27.tsv`), **classified by upstream's answer (the tenet of
2026-10-29)**: for each, what legend-pure prints (`CompiledSupport.primitiveToString`, `PureDate.toString`) and what
legend-engine serializes (its JSON `DEFAULT`, `ValueTransformer`; its CSV, `RelationalResultToCSVSerializer` --
commons-csv `CSVFormat.DEFAULT` over the driver's own text, no header unless asked -- and `CSV_TRANSFORMED`, the same
with `ValueTransformer`'s dates), read from the pinned sources or asked of the engine runner (the homework README §7);
a difference whose new text is upstream's is a correction (recorded, no row); one whose new text is neither upstream's
is put to the user case by case with its expression, upstream's text and lite's, and the user's answer decides each (a
register row with the ruling, or a revert in code). LEAN's own spellings are measured the same way, the renderer's
design (D6: Pure's print forms) read under the tenet: where legend-pure's print and legend-engine's serializer differ
(a Float with an exponent: `0.00000025` against `2.5E-7`; a negative zero: `0.0` against `-0.0`, ruled S117), the user
picks the twin per case. Register rows for the PCT message changes (`testBigFloatAbs`'s digits, the NaN refusals of
`testArcCosineError`/`testArcSineError`); the program's §3 table brought up to date.

**S1.6 A draft PR. OPEN 2026-10-29: PR #80** (`https://github.com/neema2/legend-lite/pull/80`, from `a0fe9d21`;
GATES, the S1.6 entry; the session subscribed to its events). So CI runs on every OS (the local gate covers neither
Windows, arm, macOS, the corpus lanes nor the product build: the 2026-10-26 incident). Opened under the user's "keep
going all the way until done with whole program" (2026-10-29; the decisions log D11: a draft PR is not a merge; a
reading the user may reverse -- until 2026-10-29 this item read "with the user's word"). Template
`.github/pull_request_template.md`. The local gate ran first (`bazel test --lockfile_mode=error //gates:local`,
Postgres targets unprivileged, §6: GATES "S1.5's gate"). Drive CI to green, recording each result in GATES; the user
merges. The branch keeps moving (track B lands on it); the PR follows the branch.

### Track B. A test body is a sequence of plans

Why: the corpus, channel B, the service tests and the 20 `let` tests need one entry for a whole body; it is what lets
`StatementExecutor` stop sending anything itself and `ExecutionResult` leave main. It is the plan path program's step 8
(`docs/PLAN_PATH_PROGRAM_2026_10_17.md` §7) and the end state's item 3 (homework README §6).

What exists: the plan nodes `Sequence`, `Update`, `Effect`, `Check`, `TdsResult`, `Keyed`; `PlanRunner.update`/`effect`
(phase 5: every effect of a body is already a plan node run on the body's session); `JudgePlans` (each judge side and
each verdict statement already a plan); the `AssertListener` verdict channel.

Build:
1. The body driver sends nothing itself: the three schema probes become plan steps through the session seam; a guard
   forbids `StatementExecutor`/`BodyCompiler` from `java.sql` (`ArchitectureTest`).
2. One entry for a program: `Execution.runProgram(...)` (name it) plans a body (lets per the plan path program §5: in
   the database or nowhere), runs it, reports every verdict to the listener, returns nothing materialized.
3. The judges take typed sides (collected typed rows plus a shape) instead of `ExecutionResult`; the host judge's
   `Equality.ordered` unchanged.
4. Put to the user before building: what the body driver keeps in Java (call frames, `assertError`, `$plan` navigation,
   `print`, the test-data generator; homework README §6 lists every kind with its size).
Gate: the corpus rosters unchanged (`//spec:judge_lanes`), channel B identical per test on every lane, PCT exact.

**The design, from the map read 2026-10-29 (the decisions log D29; the Java residue D30, posted to the user and built
under "keep going").** What the map says: the body driver (`StatementExecutor`, 3,270 lines) holds a
`java.sql.Connection` in its environment (`ExecEnv.connection`, 15 reads) and switches it per side (`withConnection`,
`withSystemRoute`, `WrappedSide.connection`, the database judge picking the store-bound side's); everything it SENDS
already goes through `JudgePlans.run` (`PlanRunner.rows` on `PlanSessions.given(connection, trace)`) or
`PlanRunner.update`/`effect`; what still touches JDBC directly is the three schema probes (`ColumnProbe.probe`, a
LIMIT-0 metadata read for a deferred relation `toString`; `GridProbe.probeTypedColumns`, a raw grid's schema through
`gridReferee`; `WireTypes.staticized`/`reconcile`, a late-bound grid's and a wire-decided kind's reported types),
`DynamicPivot.staticize` (the same probe for a static pivot), `SetupRunner.run` (a context's declared seeds,
`establishContexts`, keyed by connection in `ESTABLISHED`), `Sessions.inTransaction`/`ownsTransaction`,
`SystemDatabase.of`'s route and the test-data generator (`TestDataGenerationNatives`, a `Connection` argument). The
judges' sides are already typed: `JudgePlans.evalSide` -> `PlanRunner.Rows` (typed, collected) re-wrapped as
`ExecutionResult.{Tabular,Scalar,Collection,Graph}` and flattened back by `AssertVerdicts.decodeSide`/`cells`.
26 main files name `ExecutionResult` (359 references); the callers outside core's tests are `QueryService`'s three
materialized `execute`s, `PureTestRunner`, `ServiceTestRunner`, `ChannelB`, and the referee.
- **B1 (first, two commits).** (a) The session seam answers a statement's REPORTED COLUMNS: `Statements.columns(String
  statement)` -> (name, driver type name, nullable), the JDBC session from `ResultSetMetaData` of a prepared
  statement (LIMIT 0 where a probe needs it stays the caller's text), a connector session from its own; the three
  probes and `DynamicPivot.staticize` take a session (`PlanSessions.Source` or `Sessions.Session`) and read through
  it; `SetupRunner.run` and `TestDataGenerationNatives` the same. (b) `ExecEnv.connection` becomes `ExecEnv.session`
  (a `Sessions.Session`: its `statements()` for every send, its identity for `ESTABLISHED`, `withConnection` ->
  `withSession`, `WrappedSide.session`); `Sessions.inTransaction` moves behind `Statements.inTransaction` (already
  there); `Execution` stays the one JDBC-aware caller (it opens or is handed the connection and wraps it as the
  session). Guard: `ArchitectureTest`: `StatementExecutor`, `BodyCompiler`, the judges (`HostJudge`, `DatabaseJudge`,
  `JudgePlans`, `AssertVerdicts`, `SqlTextVerdicts`, `AgreementArm`, `VerdictArm`), `CsvLoad`, `SeedSqlForms`,
  `AssertErrorNative`, `TestDataGenerationNatives` depend on no `java.sql` (the JDBC funnel's allowlist shrinks);
  `TenetRatchetTest`'s accessor pin and `JavaEvalLedgerTest`'s pins move with dated notes.
- **B3 (second, the mechanical bulk).** `JudgePlans.evalSide` returns a typed side -- `PlanRunner.Rows` with the
  side's `ResultShape` and type (`TypedSide`) -- and `AssertVerdicts.decodeSide`/`cells`, `HostJudge`,
  `AgreementArm`, `VerdictArm`, `exec/TdsCompare`, `PlanEnvelope`, `LiteralFold`, `AssertErrorNative`,
  `SqlTextVerdicts` read it; `ExecutionResult` leaves the judges and survives only at B2's shims and `QueryService`.
- **B2 (third).** `Execution.runProgram(TypedQuery, runtimeFqn, PlanSessions.Source, AssertListener,
  SqlReplayReferee, ExecuteOptions)`: the body driver on the session seam, nothing materialized returned (a
  `ProgramReport`: verdict count, and the last statement's typed side for `ServiceTestRunner`, which serializes it);
  `execute*`/`executeResolved` become shims over it for core's tests until tracks T and D delete them. (As built,
  2026-10-29: a `Sessions.Source`, since the driver holds a `Sessions.Session` after B1b; no verdict count, the
  verdicts are the listener's -- D37; the B2 audit, D40.)
- **B4, the Java residue (D30; the user may reverse):** call frames (`buildFrame`, `spliceHook`, `aliasFrame`: the
  compiler inlining a test helper -- planning), `assertError` (a verdict over the error a database raised -- carriage
  of an error), `$plan` navigation, `planToString`/`toSQLString` (plan-time values, the plan path program §5),
  `print` (an effect on stdout), the test-data generator's fold (the one place Java reads a session's state to plan
  the next statement: the plan path program's step 9 residue). Each pinned by name in `JavaEvalLedgerTest`; the
  generator's fold is the one to revisit (a two-plan body).
- **Per commit:** `//core:core_tests`, the guards; `//spec:judge_lanes`, `//gates:corpus_duckdb`, `//gates:corpus_h2`
  (the rosters unchanged); PCT channel B x5 on DuckDB identical per test; channel A exact on DuckDB, SQLite and
  MongoDB; the Postgres targets unprivileged; `//datacube:python_execute_test`. Order: B1a, B1b, B3, B2, then C.
- **B1a built 2026-10-29** (GATES, the B1a entry; the decisions log D33): `Statements.columns` (a statement run for its
  columns, the LIMIT-0 probe, no row read), `Statements.describe` (the prepared statement's metadata, none where the
  driver withholds it: the wire-type reader's reading, kept as it was) and `Statements.load` (the bulk loader where the
  database has one); `ColumnProbe`, `GridProbe`, `WireTypes`, `SetupRunner`, `SystemDatabase`'s seeding and
  `Execution`'s session setup read and send through them; `DynamicPivot.staticize` takes the session and a
  `KeyReader` (the keys as typed rows through `PlanRunner`, a key the lowering did not type typed by the database's
  report); the body driver's `ExecEnv.statements()`; `gridReferee(env)`; `lowerAndPrepare` without its connection
  parameter. The JDBC census loses `SetupRunner`, `GridProbe`, `ColumnProbe`, `DynamicPivot`; the ledger re-pins
  `StatementExecutor` 2316 -> 2313, `GridProbe` 53 -> 46, `DynamicPivot` 112 -> 111, dated. Not yet (B1b): the
  `Connection` in `ExecEnv`, `WrappedSide`, `ESTABLISHED`, `wireMemo`, `SystemDatabase.Route`, the transaction
  bracket, the test-data generator; the `java.sql` guard on the driver and the judges.
- **B1b built 2026-10-29** (GATES, the B1b entry; the decisions log D35): the `Connection` leaves the body driver, the
  judges, the verdict batch, the referee's SPI and the generator's natives. `ExecEnv.session` (a `Sessions.Session`:
  `statements(trace)`, `state()`, `version()`), `withSession`, `WrappedSide.session`; `SessionState` on the seam (the
  seeds established, the wire-type memo, the write mark: the old `ESTABLISHED`, still keyed by the connection so a
  shared lane keeps one state); `Statements.ownsTransaction`; `SystemDatabase.Route.session`; `VerdictBatch` by
  session; `SqlReplayReferee` by session; `TestDataGenerator.generate`/`seedDataString` by session, their
  `SQLException` a `DataError` at the generator's door. `Execution` is the one JDBC-aware caller (its execute entries
  wrap the connection: `PlanSessions.session`). Guards: root's `java.sql` pin shrinks to `{Execution}`;
  `ArchitectureTest.theBodyDriverAndTheJudgesHoldNoJdbc` names the thirteen; the JDBC census loses three files and
  gains `SessionState`; the ledger re-pins `StatementExecutor` 2313 -> 2290. Not yet: B3 (typed sides), B2
  (`Execution.runProgram`), B4 (the residue: the generator still runs JDBC inside `testdatagen` on the session it is
  handed). Audited (GATES, "B1b's gate"; the decisions log D38): confirmed; its three findings fixed with B2.
- **The order changed 2026-10-29 (the decisions log D36; the user may reverse):** B3 is deferred to track D as a
  rename. `ExecutionResult` is already the typed, collected side B3 wants; its other two roles (the verdict, the old
  door's materialized result) are what C, T and D remove, and building `TypedSide` beside it now would rewrite ~350
  references twice. Order: B1a, B1b, B2, C, T, D (the deletions, then `ExecutionResult` -> `TypedSide` in the judges).
- **B2 built 2026-10-29** (GATES, the B2 entry; the decisions log D37): `Execution.runProgram(TypedQuery, runtimeFqn,
  Sessions.Source, AssertListener, SqlReplayReferee, ExecuteOptions) -> ProgramReport(last)`, the one entry for a
  program: the body typed once, run on the session the source opens for the runtime's target, every verdict to the
  listener, the last statement's side for a serializer and nothing else. `StatementExecutor.execute` takes the typed
  query (the three resolved-AST overloads deleted); the old door's nine public entries reach `runProgram` (five as
  direct shims, four through the 7-arg `executeResolved`) until D. Audited (D40): confirmed; three record fixes.
  Next: track C (the corpus runner, channel B and the service test runner on `runProgram` and typed plans).

### Track T. Our core tests become the LEAN twin of PCT and the corpus

The user's ruling, 2026-10-28: "i would prefer not to delete the overlapping tests but use the exact same logic but
with platform output that we think users would actually run/use instead of backwards compat mode - so its almost the
platform twin of PCT/core-relational". Nothing is deleted; the same queries and logic are judged on lite's own output.

Build:
1. A test helper `lean(model, query)` (and its runtime overload) that plans with `Compiler.query`, runs with
   `Execution.run`, writes lite's JSON rows (`result/JsonRowsWriter`, what the query service serves) and reads them
   back as a client does: columns (name, Pure type, multiplicity) and rows of JSON values. It may keep the accessor
   names tests use (`rows().get(0).get(0)`, `columns()`, `rowCount()`): it is a reader of the product's output, not a
   copy of `ExecutionResult`. Never in main.
2. Switch `AbstractDatabaseTest` (`queryService.execute`, `executeRelation`, `executePureQuery`) and the other helpers
   (§2) to it. Every expectation that moves is decided one by one and listed (the old decoder's Java carrier, an
   `Integer` for a `Long`, a Decimal's scale, a date's precision: a correction recorded, or a bug in the new path
   fixed), as phase 5 did for its 93.
3. Run per lane like PCT: `AbstractDatabaseTest.backend()` is `TestBackends.platform()` (DuckDB) today; parameterize
   by `TestBackends` and give each lane a Bazel target beside PCT's (DuckDB, SQLite, H2, MongoDB here; the servers and
   the clouds through the lanes).
4. The ~850 tests that only reach the old door to compile or plan (homework README §5) call `Compiler.query` or the
   plan instead.
5. The LEAN judge is lite's JSON rows first; Arrow joins it when track A lands, with a gate that the two agree.
Gate: the census's 1,450 value tests pass on DuckDB with every moved expectation listed; then each lane at a recorded
floor. Census: `python3 -I docs/result-boundary-homework/probes/test_census.py.txt`.

- **T-a built 2026-10-29** (GATES, the T-a entry; the decisions log D42; the ledger
  `docs/result-boundary-homework/T_EXPECTATIONS_2026_10_29.md`): the reader `testing/Lean.java` (the product's route,
  lite's JSON rows read back by the product's own row decoder; three rules: the ran plan's columns, presented names,
  a tagged column by its declared type); `AbstractDatabaseTest`, `CheckerResults` and the 28 subclasses on it;
  `ExecutionResultIntegrationTest` on the answer's shapes. 2,232 of 2,232 integration tests pass; no expectation
  moved. Audited (D43): the code confirmed; the records corrected with T-b.
- **T-b built 2026-10-29** (GATES, the T-b entry; the decisions log D44; the ledger, batch T-b): the integration
  package's remaining 31 files on `Lean.run`; `testing/Program.java` (a body through `Execution.runProgram`, its
  verdicts, its last side through `Lean.of`) for the six files whose bodies only the program entry runs (the
  metamodel store's four, the DDL natives, a staged let); two reader rules (a Variant cell, the view's empty value).
  One plan-path bug fixed in main (a navigate's slot column: `TypedRows.flat`, one flatten for the plan's columns and
  the judges'; `ColumnFacts`' tolerance gone). 2,232 of 2,232. Findings (D44): the plan path does not route a
  system metamodel store read on a given session (track D must carry `SystemDatabase.Route` before the driver's
  goes); `PureV1Api`'s tdsColumns index unflattened typed columns against the plan's by position. Next: T-b2, the
  other packages (85 files, 149 lines by the ledger's counting rule: lowering 16, exec 16, resolver 12, the root 13,
  ...); T-c the helpers (`SqlParity`, `CypherParity`, `IqParity`, `PlanCases`, `testing/Rows`) and the parity-lane
  files; T-d the compile-only tests and the per-lane targets.
- **T-b2 and T-c built 2026-10-29** (GATES, the T-b2/T-c entry; the decisions log D45, D46; the ledger, batch T-b2 and
  T-c): the other packages' 68 files, the six integration files the first counting rule missed (the T-b audit, D45), the
  parity harnesses and their eleven lane tests on the twin; `Lean.value()`, `Lean.service` (a new product entry,
  `QueryService.execute(..., sinkOf)`), `Lean.established`; `Program` typed as the old door types; the reader in
  `:test_cases` (the T-b gate's build failure fixed). Two product bugs the twin found, fixed in main: `Json.parse`'s
  negative zero, `RowDecode`'s identity fields. By the census (`probes/old_door_census.py.txt`) 7 files, 21 lines remain,
  every one track D's. Findings for F: the plan path's egress lower bound (Pure raises, the plan path writes `[]`: the
  user to rule), the metamodel-store route, the API's unflattened typed columns. Next: T-d (the per-lane targets for
  core's tests beside PCT's: DuckDB, SQLite, H2, MongoDB), then track D.
- **T-d built 2026-10-29** (GATES, the T-d entry; the decisions log D47; the ledger, batch T-d): the twin per lane --
  `testing/LaneFloor` holds each lane's run to its recorded floor (`core/src/test/resources/lanes/<lane>.txt`, which
  only shrinks); `//core:core_tests_integration_sqlite` (15 on the floor) and `_h2` (169), in `//gates:core`; four
  harness assumptions corrected, no value moved. No MongoDB lane yet (the base opens a JDBC session; MongoDB has
  none): with track D's session-seam move. Item 4 (the compile-only tests) closed by T-b2/T-c. Track T is built; its
  remaining item is Arrow's judge (track A). Next: track D.
- **T-d2 built 2026-10-29** (GATES, the T-d2 entry; the decisions log D49; the ledger, batch T-d2): every integration
  test on the lanes (the 37 classes the base did not cover, under `LaneFloor`; the exclusions named), the floors
  re-measured at the lane's full scope -- sqlite 89, h2 269 and one NOT-RUN pin (268 since track F's H2 parse; a new floor-line form, dated, for
  a failure that ends the run: H2's planner exhausting the heap on a twenty-call chain); a test's own connection
  text re-spelled to the session's backend (`LaneFloor.model`); floor keys by invocation ordinal; the floors live
  (`LaneFloorTest`); the T-d audit's fixes and the track D audit's records. Three findings for track F (the
  `asOfJoin` emulation, `concatenate`'s mismatch refused by the database, H2's nesting). Track T is built but for
  Arrow's judge (A). Next: track L.
- **The queued items, as they stand after Z (2026-10-29, the audit's finding 4):** D18's four hour-on-zone sites,
  D17's element-name print and D7's host-judge trim wait on S121's ruling (pending; the README §9 row); the MongoDB
  lane is NOT built (the seeds need the session seam: a README §9 row, the user's call); item 5 (Arrow in the LEAN
  judge with a gate that the two agree) is met by `//core:arrow_writer_test` over one batch of every kind, not by the
  twin's tests (a README §9 row).

### Track C. The corpus, channel B and the service tests on track B

Switch `PureTestRunner` (bodies and setups), `ChannelB`, `ServiceTestRunner` and the referee (`SqlReplayReferee`,
`ReplayReferee`, `RefereeVerify`) to track B's entry and typed sides. The plan path program's step 10 (the 2,798 bodies
through plan sequences). Gate: the rosters (`spec/src/test/resources/rcorpus`) unchanged, both judges agreeing,
`//spec:judge_lanes`, `//gates:corpus_duckdb`, `//gates:corpus_h2`, channel B per test on every lane.

**Built 2026-10-29** (GATES, the track C entry; the decisions log D39): `PureTestRunner` (the bodies, the setups, the
fixtures), `ServiceTestRunner` and `ChannelB` call `Execution.runProgram(Compiler.queryResolved(ctx, resolved),
runtime, Sessions.given(connection), listener, referee, options)`; nothing else changed. The referee keeps
`ExecutionResult` as the judges' side until D (D36). The gate: GATES, the track C entry and "track C's gate" (the
chain measured after the commit: the corpus rosters, both judges, channel B, channel A, the Postgres targets, the Python
route). The old door's remaining callers are core's tests (track T) and `QueryService`'s materialized `execute`s
(track D). Audited (D41): the code confirmed; two record defects of the commit corrected (this paragraph had claimed
the measurements before the chain ran; §2's `testing/Rows.java` was wrongly called non-existent and is restored).

### Track D. The deletions

After T and C: `Execution.execute*` and `executeResolved`, `exec/ExecutionResult`, `QueryService`'s materialized
`execute` methods, `TestDataGenerationNatives.planTextResult`'s result type, the old helpers in `AbstractDatabaseTest`,
`StatementExecutor`'s JDBC; `StatementExecutor` renamed to what it is (the body driver) if it stays. The census before
and after: `python3 -I docs/result-boundary-homework/probes/oldpath_census.py.txt` must read zero for the step-7
symbols. Guards shrink with dated notes: `JavaEvalLedgerTest`'s exec register and pins, `TenetRatchetTest`'s JDBC
accessor pin, `ArchitectureTest`'s pins. The plan path program's step 11.

**Built 2026-10-29** (GATES, the track D entry; the decisions log D48): the old door's entries deleted
(`Execution.execute*`, `executeResolved`, the query service's materialized `execute`); `exec/ExecutionResult` renamed
`exec/TypedSide`, the judges' collected side; `StatementExecutor` renamed `BodyDriver`; the guards' entries moved
with dated notes; AGENTS.md and core/README.md corrected; `probes/oldpath_census.py.txt` reads zero for every step-7
symbol. Open: the plan path program's step 11 in its 2026-10-17 wording (the body driver itself deleted) is
superseded by track B -- the body driver stays, named for what it is, every statement through its plan -- and its
closure waits on step 9's residue (S81, S82) for the user's rulings; the integration base's JDBC `connection` stays
for its seeds (the MongoDB lane, D47).

### Track L. Mixed lists as typed values (the program's step 6)

A list of tagged values (one tag, one typed slot per kind), as the ratified selection design
(`docs/RESULTS_AND_RENDERERS_2026_10_22.md` §5a) extended to lists: `MixedEncoding` (`rankedElems`' text ranks go),
`Scalars` (mixed `sort`, the Any-kind carrier), `Lowerer` (a mixed collection at a root as parallel typed arrays),
`RowDecode`/`JdbcDecode` (the tag then the slot per element), `CanonicalRenderSql` (the canon per slot),
`ColumnFacts`/`MirJson`/`PlanJson` (a format bump); then delete the `LITERAL` slot, `LiteralText.parse`, the dialects'
literal-text arms and `Fold.conformValueEgress`. Gate: a `TaggedListTest` beside `TaggedSelectionTest`; the PCT suites;
every lane's channels A and B; the `LITERAL` census zero. Traps: arithmetic over a tagged value is not designed; H2 has
no list encoding (its lane runs the relation suite alone); SQLite reads a BIT as the text "false".

- **L1 built 2026-10-29** (GATES, the L1 entry; the decisions log D50): `SqlExpr.TaggedList` -- a mixed list as
  PARALLEL TYPED ARRAYS indexed by POSITION (tags, a comparable, one channel per (kind, slot); the positions that are
  the list), its type `ARRAY<TAGGED<...>>`; the element at a position a `Tagged`; at a root the positions unnested
  into rows of a tagged value (the plan's one `TAGGED<...>` column with the list's multiplicity: the decoders', the
  judges' canon's and the writers' tagged arms unchanged). `MixedEncoding.taggedList` builds it from a literal
  collection of the nine kinds (the eight primitives' and an enumeration's: its path the kind, its name the slot; a
  type's or element's name a String); a Number-LUB literal mix is read as one at a root, a sort, a dedup, an index, a
  size, a print (elsewhere its variant carrier stays for arithmetic); a multi-column relation in value position
  (`$result.values.rows.values`) is one over its CELLS (`ValueCollections.rowMajorTaggedList`: the rows numbered once
  and crossed with the column indexes, the arrays ordered aggregates, the list carrying the cells as its rows so a
  root and an equality read no array: H2 runs it; D50d). The
  operations (`lowering/TaggedLists`): sort (the positions ordered by the comparable), removeDuplicates (plain, with
  `eq`/`equal`/`cmp`, with a comparator or a key: the kept-list reduce over positions), removeDuplicatesBy, contains
  and in (by the needle's kind's channel), at/first/last, size/isEmpty, exists/forAll, reverse, makeString, hashCode,
  format's argument list, equality (two mixed lists; a mixed list against a list of one kind; a tagged element against
  a value; two lists in their own order channel by channel, null-safe, no lambda). A lambda over a mixed list
  ranges over its positions: its parameter IS the element there (`LambdaBinding`), so the body's rules read a tagged
  value; equality reads the value itself, never its comparable (D50a). A list-less dialect reads a literal array at a
  computed position as a CASE (`CarrierStrategies`). Gate: `exec/TaggedListTest` beside `TaggedSelectionTest`.
  Measured: the full chain (GATES, the L1 entry).
- **L2 built 2026-10-29** (GATES, the L2 entry; the decisions log D52): the `LITERAL` slot, `LiteralText.parse`, the
  decoders' and the dialects' literal arms and the lowering's spelling machinery DELETED; the census probe reads
  zero for the slot's own spellings; the plan format is 19. `Fold.conformValueEgress` is step 5's deletion and
  waits on a written-digits fact at scalar roots (D51, the user's question). Track L is done but for that ruling.
- **The L2 audit folded 2026-10-29** (GATES, the L2 audit entry; the decisions log D54; the register S123, S124): the
  flatten numbers a sorted star frame (it threw) and carries an ordered source's keys; crossing numbers (an Integer
  against a scale-zero Decimal, a Float against a Decimal: Pure equates them by text) refused by name where a static
  false stood; `filter` over a mixed list; `$tds.rows.values->size()` is rows times columns (it answered the row
  count); the gate that reached only the variant lane renamed and a typed-columns gate added. Posted to the user: a
  Decimal's rank among the numbers (S123), an unordered relation's cells in all-columns order (S124).

### Track A. The Arrow writer (the program's steps 2 and 5)

Ruled 2026-10-28: "yes keep arrow writer at the boundary in this program (but no arrow dependencies in compiler!)".
Move the warehouse's hand-written Arrow IPC framer (`ArrowStreams`, `FlatBuilder`) into a dependency-free module both
the warehouse and core's writers use, with its pyarrow judge; write `ArrowWriter` from typed batches; consume it in the
Python route (`ARROW_IPC`) and the tab. Guards: no `arrow` repository in the deps of `//core:plan_side`, the
WebAssembly planner or the native image (R9). Gate: the warehouse's Arrow tests unchanged; a round-trip gate that
Arrow and JSON carry the same values for every kind; DataCube's Python tests.

- **Built 2026-10-29** (GATES, the track A entry; the decisions log D55, D56): `//arrow_ipc` (the framer, on java.base
  and `//base`: `FlatBuilder`, `ArrowType`, `ArrowField`, `ArrowIpc`, `ZstdFrame`), the warehouse delegating its
  framing; `result/ArrowWriter` (a column's Arrow type its slot's: D55's table; a tagged column a dense union; the
  plan's facts as field metadata; every misfit refused by name); the server's `?serializationFormat=ARROW_IPC` execute
  (`PureV1Api.executeArrow`: upstream's metadata, headers and zstd frame, raw blocks by our own `ZstdFrame`);
  `PureV1Api.arrowAnswer` and the embedded hosts' entry points (`Boundary.executeArrowAnswer`, the tab's
  `executeArrowAnswerOrError`, the native `lite_execute_arrow_answer`; a binary answer folded as base64). Guards:
  `//tools/deps:arrow_closure_test` (R9), `WarehouseClosureTest`, `ArchitectureTest`, `WriterCarriageGuardTest`. Gate:
  `//core:arrow_writer_test` (pyarrow against the JSON rows of the same batches), `PureV1ApiTest`'s Arrow execute.
  NOT switched: Python's route and the tab still answer as before -- switching changes the Python route's existing
  bytes (DuckDB's Arrow types to the plan's slots'; timestamp[us] to timestamp[ns, UTC]): posted, the user's call
  (D56).
- **Audited 2026-10-29** (an independent agent, eighteen findings; the decisions log D57, folded before the commit):
  the union's type ids stated (apache-arrow JS read nothing without them); the decoders' 63-bit bound (a long holds
  63 bits beside its sign; `[2^62, 2^63-1]` rode as a BigInteger); a non-numeric Pure type over a numeric slot typed
  as the decoders carry it (text); a tenth subsecond digit refused, never truncated; a class-typed column's object as
  the JSON route writes it; the envelope from the plan's stated columns before any statement runs; `legend.multiplicity`
  in Pure's spelling; `abs` in the carriage guard's vocabulary; the gate's seven new cases, the judge reading a union
  member by member. POSTED for the user: a DAY under a TIMESTAMP column (the JSON route spells its day, the Arrow
  route refuses: D57 (4)); the JSON route's silent tenth digit (D57 (10)); the server's Arrow format against S28's
  upstream format (register S125). The tag in the batch (D57 (5)) is track F's.

### Track F. The open findings

Each with its evidence in the homework README §9: S116 (`toVariant()` of an empty value on DuckDB; the branch's S113 until S1.0; a new
id after S1.0); the engine-parity
H2 dialect's `fromJson` (a JSON string, not parsed: a silent wrong answer); a `BIT` store column's type (a ruling);
Snowflake's far-BC `TIMESTAMP_NTZ` (its dialect returns the database's own text; a lane measurement);
`RootNumericTypes`' guessed unbounded Decimal; the tab's Arrow nested values in `HostRows`; an absent to-many property
written as `null`; the PCT JVMs at their 4 GB heap ceiling. Added 2026-10-29 (S1.5, the decisions log D19 and
D24): the writer's refusal of a fold-built object in a value root CSV loses the old door's reason (the object's
identity is not tracked); LEGACY spells a year as its number (`%d`) where legend-engine's `ValueTransformer` pattern
is `yyyy` (zero-padded year-of-era: a year below 1000 and a BC year differ in bytes), unprobed.
Added 2026-10-29 (track T's lanes, T-d and T-d2; the T-d audit's findings 3 and 4): the `asOfJoin` emulation on SQLite and
H2 answers matches against an empty right side (DuckDB's `ASOF JOIN` answers none: a wrong answer, on both floors); a
`concatenate` of relations of different column counts reaches SQL and the database refuses it, not the typer (invariant
4); H2's planner is exponential in nested derived tables (a twenty-call chain of a window-extending function exhausts
the heap and ends the run: the H2 floor's one not-run pin). Added 2026-10-29 (track L): a mixed list across comparison
groups sorts by a rank struct that SQLite and MariaDB order as text (pre-existing; a multi-key ordered list aggregate
is the fix); an Any list holding an instance keeps the variant lane (an instance kind is undesigned; an
enumeration's value is a kind since D50d).

- **Worked 2026-10-29** (GATES, the track F entry; the decisions log D58; every finding read against the pinned
  upstream sources first): BUILT -- S116, `toVariant` of an empty value is JSON's null (the engine's
  `ifnull(to_json(x), 'null'::json)` by value); the engine-parity H2 dialect parses a cast to JSON (`H2.jsonParseCast`,
  measured on 2.1.214 and 2.2.224; the H2 corpus roster moved 63 tests from failing to passing); the tag in the batch
  (`result/TaggedCell`, `Batch.tag`, the Arrow union reads it). CLOSED by upstream's own rule -- the `concatenate`
  width mismatch (legend-pure compiles it too), the `[0..1]` collection element (legend-pure refuses it too),
  `RootNumericTypes`' unbounded Decimal (by design). RECORDED for the user -- `asOfJoin` against an empty right side
  (register S126: the emulation answers the spec, DuckDB's own answer is a defect of its planner -- resolved the
  same day by the driver bump to 1.5.6.0, D60); the multi-key
  ordered list aggregate for a mixed sort's ties and the rank struct's text order (sized: plan format 20, ten
  dialect arms, the MariaDB lane); the lane twin of the mixed-list gate; a typed kind; the rest of the list as before.
  NOT REPRODUCED -- the absent to-many property (the probe `anEmptyToManyPropertySerializesAsAnEmptyList` pins `[]`).

### Track P. The backlog from the plan path program

Not part of this program, recorded so it is not lost (the plan path program's §9 status): PCT parity batch 3 (a
Variant's key order on MySQL and Postgres, a carrier change; a named zone where the database has no zone rules, a
question for the user; SQLite's big integers); the ClickHouse and Postgres pins that legend-engine passes; the lane
pins that legend-engine also fails (MySQL and MariaDB windows, H2 lists, Trino rows, MongoDB clusters); the deep SQL
optimization passes the user asked for after parity; the TeaVM bump to 0.16.0 that closes PARK-40 (delete
`tools/teavm`'s `WasmDataOffsets`); the program's step 0 local measurements M1 to M4.

### Z. The final audit and the landing

An audit against the tenets and the program (an independent agent, the code not the recollection); the documents
updated for every renamed and deleted piece; every lane measured; the PR handed to the user to merge.

**The doors (2026-10-29; the user: "I want to close all the doors here as part of this PR so we know that it can be
done").** After track Z the program's work was complete but for the ways a statement could still reach a session
without the plan runner. The seam: `exec/Statements` is the session seam, `PlanRunner.send` the one place a plan's
statement meets it (the target's session, `check(target)`, the session statements, the count, the ask); a door is
a class that reaches `Statements` or a JDBC connection around it. The map (the decisions log, "The doors"):

| # | Door | How it reached the session | Through the runner |
|---|---|---|---|
| 1 | `Execution.ProgramReport.last` | not a session door: the body driver's last side MATERIALIZED (`TypedSide`) and handed to a caller, which spells its cells (the service test runner through `CellText.engine`; `testing/Program` through `Lean.of(TypedSide)`) | the last statement's answer STREAMED to a sink the caller hands (a planned statement through `PlanRunner.stream`; an answer the prelude or a fold gave, a graph array, a count: written through a `Batcher` as host rows); `ProgramReport` deleted; `TypedSide` leaves the public surface |
| 2 | `testdatagen/TestDataGenerator` | its own JDBC on `session.connection()`: `Statement.execute` (CREATE TEMP TABLE AS, DROP), `executeQuery` + `rs.getObject` (the transcript rows, the seed-string rows, the CSV lines), `getMetaData` (the text columns) | effects (`PlanRunner.effect`), typed rows (`PlanRunner.rows` over a relation the database describes), reported columns (`PlanRunner.columns`) |
| 3 | `exec/SetupRunner` (the body driver's seeds; `Execution.dialectOf`'s session statements; `SystemDatabase.seed`) | `Statements.execute` / `Statements.load` | `PlanRunner.setUp(steps, target, sessions, listener)`: a statement step as an effect, a rows step through the session's bulk loader inside the runner; the steps' form from the plan side's one owners (`CsvSeed.planned`, `Ddl.metamodelSteps`, shared with `PlanMaker`); `SetupRunner` deleted |
| 4 | the probes: `ColumnProbe`, `GridProbe` (LIMIT-0 columns), `WireTypes.reported` (describe), `DynamicPivot.reportedType` (LIMIT-0 columns) | `Statements.columns` / `Statements.describe` | `PlanRunner.columns(Sql, sessions)` and `PlanRunner.describe(Sql, sessions)`: a probe is a plan statement of the body's target, checked as any other; a schema read is not counted as a statement sent (the probes that count themselves do so by name, as before) |
| 5 | the guard | none | `ArchitectureTest.theRunnerIsTheOneCallerOfTheSessionsStatements`: outside `exec.PlanRunner`, the session owners (`PlanSessions`, `Sessions`) and the implementations (`JdbcStatements`, `MongoDbStatements`), no class calls a method of `Statements` and none takes a session's statements (`Sessions.Session.statements`); the transaction bracket (`inTransaction`, `ownsTransaction`) is the session's own; `ExecEnv.statements()` gone |

- **Doors 3, 4 and 5 built 2026-10-29** (the decisions log D61; GATES, the doors 3-5 entry): `PlanRunner.columns`,
  `describe` and `setUp`; `ExecutionPlan.Sql.of`; `Sessions.Session.inTransaction`/`ownsTransaction`;
  `CsvSeed.planned` and `Ddl.metamodelSteps` (the one owners of a setup step's form, shared with `PlanMaker`); the
  probes and the pivot take the plan sessions and the target; the body driver's `planSessions()`/`target()` in place
  of `statements()`; `Execution.dialectOf` on a session; `SystemDatabase.seed` through the runner; `SetupRunner` and
  `PlanSessions.statementsOf` deleted; the guard `ArchitectureTest.theRunnerIsTheOneCallerOfTheSessionsStatements`.
- **Door 2 built 2026-10-29** (the decisions log D62; GATES, the door 2 entry): the generator's temps and drops as
  effects, its read-backs as typed rows of the columns the database describes, the CSV's text columns as reported
  columns, the whole run under the TDG origin; no JDBC in `testdatagen` (the funnel tightened, the census shrunk).
- **Door 1 built 2026-10-29** (the decisions log D63; GATES, the door 1 entry): a program's last statement streams
  to the sink the caller hands for its shape (`Execution.Answer`); `ProgramReport` deleted, `TypedSide` off the
  public surface; the service test runner on `PureV1Writer.objects` and the new `result/TdsObjectsWriter`; the twin's
  `Program` on `Lean.Capturing`. Every door in the map is closed (a body's every statement the plan runner's;
  nothing materialized past a public entry); the independent audit of the doors named three raw sends that remain
  inside the chartered seams (the plan path's seeds, the server's fill, the MariaDB connector) and twelve findings.
- **The audit folded 2026-10-29** (the decisions log D64; GATES, the fold entry): the records corrected with door 1;
  an effect statement that ends the body answered after its script; every temp dropped; the dead arms deleted
  (`PrepTrace`'s timed prepare/execute, `SqlTypeCensus`'s wire arm, `TypedSide`'s accessors, `Lean.ofCollectedSide`),
  both files off the JDBC census; the stale sentences. Two of the service test runner's readings and the routing of
  the plan path's seeds and the server's fill are posted to the user (D64), not built.
- **The three routings built 2026-10-29** (the decisions log D65; GATES, the three routings' entry): the plan
  path's seeds and the server's held-database fill as plan setup steps the runner sends (the target's session
  statements first, as the body path and as upstream's test connections); the MariaDB raise recovery declared by
  its connector as data and read by the seam (`JdbcStatements.held`); `DatabaseConnector.raisedMessage` and
  `Sessions.raisedMessage` deleted. In core's main sources the seam and its DuckDB bulk loader are the only JDBC
  senders. The audit of the routings (twelve findings, one on the code: the held raise read in HEAD's order again)
  is folded in D65. **PR #80 merged 2026-10-30 (`7bc0797d`)**; the branch restarted from main.

**The java.sql revamp (the user, 2026-10-29/30: "all the sql fixes ... the session values and deleting or fixing all
the places that use java.sql, pin the rest by class"):**

- **Step 1 built 2026-10-30** (the decisions log D66; GATES, the step 1 entry): a target's session statements run once
  per connection (the session's memo; a rollback clears it, Postgres's `SET` being transactional), where the runner
  re-sent them before every statement.
- **Step 2a built 2026-10-30** (the decisions log D67; GATES, the step 2a entry): the execution front door on
  sessions (no JDBC); `QueryService` and `OutputFormat` deleted (the HTTP door's runner is `ConnectionResolver.RUNNER`
  over `Execution.run`; the text conveniences were tests' alone); the free tier (six unused connector imports, the
  runner's JDBC type number into the seam). The census register shrank by nine files.
- **Step 2b built 2026-10-30** (the decisions log D68; GATES, the step 2b entry): the test runners, the observer and
  the corpus harness on `Sessions.Session` (an owning session from the opener, non-closing views to the program
  entry); core's `test` package holds no `java.sql`. The census register shrank by three more files.
- **Step 3 built 2026-10-30** (the decisions log D69; GATES, the step 3 entry): the JDBC census is a table of rows,
  each registered class pinned to the acts it may perform (opens, checks, sends, loads, decodes, transacts, closes)
  and measured in its source; the funnel exempts the register's classes and nothing else (one list for the two
  guards); a new rule lets no public class outside `exec` offer a `java.sql` or driver type in a public signature;
  the referee's SPI names no JDBC; F1.3b, the root's class-list pin, deleted as subsumed; `PostgresCells` judged
  at last (it was not on the product jars). The revamp's three steps are built: the register held 22 files then (18
  since step A), each with its acts; the seam's own public API, where a connection changes hands, is the one left open by design.

**The seam and the wall (the user, 2026-10-30, on the revamp's assessment: "We have to do 1, 2, and 3 ... 3 is the
huge payoff of this whole program"; the plan ratified that day: step A then B in two commits, `judge` its own package,
no refusal registry, the roster lanes the oracle for B2, and B1's cost named -- mostly dated guard edits):**

- **Step A built 2026-10-30** (the decisions log D70; GATES, step A's entry): one way in (`Sessions.owned`,
  `Sessions.borrowed`, `Sessions.view`, the openers returning sessions; `PlanSessions.of` and `setUp`), the driver's
  connection the seam's alone (`Sessions.Session` an abstract class no class outside `exec` can extend:
  `onlyTheSeamMakesSessions`), the session's state the owning session's own and the process-wide map keyed by the
  connection deleted; the server's lease a session; the test side keeps one session per connection it opened
  (`TestSessions`). Four more files off the census (18 rows; the server package holds no `java.sql`).
- **Step B1 built 2026-10-30** (the decisions log D71; GATES, step B1's entry): a move, no behavior. The judge layer
  out of `exec` into `com.legend.judge` (`//core:judge`; thirteen classes, each diff a package line and imports), the
  schema probes to the root (`ColumnProbe`, `GridProbe`, `DynamicPivot`, and `WireProbe` split from the wire table
  `exec/WireTypes`), the census's one dialect name gone (`StableScanOrder` read by the corpus lane itself);
  `//core:exec` loses lowering, plan and platform, and the ratchet (`EXEC_PLANNING_REFERENCES`) shrinks from sixteen
  rows to the runtime's five (`Column`, `JdbcStatements`, `Sessions`, `SystemDatabase`, `WireTypes`). Zero roster
  movement in every lane.
- **Step B2 built 2026-10-30, THE WALL** (the decisions log D72; GATES, step B2's entry): `exec` depends on no
  planning library. The judges' column and row to `judge` (the runtime's rows were the plan's columns already), the
  four driver decode facts from the dialect to the connector (`DatabaseConnector.decoding`; the dialect's seam is
  rendering alone), the session source to the execution door (`Execution.Source`), the system database to the driver
  (the root), the wire table a NAME of a type (`WireTypes.pureTypeNameOf`; the root's `WireProbe` rebuilds the typer's
  primitive). `//core:exec` depends on base, json, cache, diagnostics, error, execution_plan, model, result, sql and
  values; the ratchet map is deleted and `ArchitectureTest.execReachesNoPlanning` asserts the empty set. Zero roster
  movement in every lane.
- **Step C built 2026-10-30** (the decisions log D73; GATES, step C's entry): the root dissolved -- the five doors at
  the root (`Compiler`, `TypedQuery`, `HeldModels`; `Execution`, `ExecuteOptions`), the plan machinery
  `com.legend.planner` (six files), the execution side `com.legend.driver` (twenty-five), the members that crossed a
  package line public and marked as not the API's, two rules keeping the design (`theDriverHasOneCaller`,
  `plannerIsExecutionFree`), two new lanes, the registers split in three. A move: zero roster movement. The plan as
  ratified, for the record:
- **Step C after B2 (the user, 2026-10-30: "Step C is fine, let's make sure we do it"):** the root dissolved. The
  root package `com.legend` holds two layers in one Java package today (the plan side's nine files, `//core:planner`;
  the execution side's twenty-eight since D72, `//core:driver`), told apart by Bazel alone. Step C keeps only the public doors
  at the root (`Compiler`, `Execution`, `TypedQuery`, `ExecuteOptions`, `HeldModels`), moves the plan machinery into
  `planner/` and the execution loop, the judges' arms, the seeds and the probes into `driver/` (one public entry on
  the driver side for the front door, pinned by an ArchUnit rule as the verdict seam is), so the package names say
  the layers (`planner`, `driver`, `judge`, `exec`, `result`) and the planner-driver wall is a language wall as well
  as a build wall. A B1-sized rename, about twice the surface; the root tests that rely on package-private access
  move with the driver. After B2, because B2 moves root-adjacent pieces that a rename first would re-do.

- **Next program, PLANNED 2026-10-30, not ratified: the planner-driver seam** (`docs/PLANNER_DRIVER_SEAM_PROGRAM_2026_10_30.md`,
  its evidence `docs/planner-driver-seam-homework/`; the decisions log D74). Step C left the driver talking to the planner
  through a wide informal interface (it takes `specs()` and `resolved()` off the query and re-runs G½/H/I/J itself; four
  probes read the session to finish planning, their answers re-entering the compiler). The plan: lite's own execution
  model applied to the body -- planned ONCE as one `Sequence` the database executes, the verdict the database's (or the
  host judge's, D7), a shape the planner cannot know a step whose rows fill the next statement's regions (the `Keyed`
  pattern, the user 2026-10-20), the driver the runner plus the judges, the probes retired shape by shape, the widenings
  reverted. Upstream was read as spec input (a body with lets needs no result to plan the next statement; planning
  reaches no database), not as an execution model (the user's correction, D75). **RULED 2026-10-30 (D77): a GO**, on a
  fresh branch from `main` after PR #82 lands (the user merges when green); step H (read-only measurements) started
  that day; the raw grid takes the pivot's pattern (D76); the defect F1 closes inside S3c; no JPMS spike.

## 5. How we work (the user's rules; each learned the hard way)

**Before code**
- **Upstream first (the user, 2026-10-29; AGENTS.md's upstream-fidelity tenet).** Every decision is settled by one
  question, asked first: can we do exactly as legend-pure and legend-engine do? Read their sources or ask the engine
  runner (the homework README §7); never recall. Exact where possible; where exactness is too expensive, the closest
  thing, posted to the user before it lands (the expression, upstream's answer, lite's) and a row of
  `docs/SEMANTICS_REGISTER.md` with the ruling. This settles the questions the earlier rules leave open (a spelling
  change, a midnight, a negative zero): upstream's answer is the default, the divergence the exception.
- Measure first. A design argument without a number is an opinion.
- Plan each step with the user and get agreement before writing code. Plain words, short sentences, one settled
  answer, a recommendation rather than a survey.
- Find the root cause. No hacks, no fallbacks, no defaults (AGENTS.md invariant 4). A guard that rejects a change is
  usually right. A pin, ceiling or allowlist entry moves only with a dated justification naming the task.
- Never put a value computation in Java (R3). Formatting at the edge; anything that decides a value goes back to the
  database or to the plan's facts.
- **Never change how an output is spelled without the user's word, case by case** (2026-10-28): a measured
  difference in any writer's output (LEGACY or LEAN) is put to the user alone, with its expression, before and after.
- **Probe legend-engine, do not guess** any spelling of its (the homework README §7 has the recipe; PARK-34's lesson).
  The same for legend-pure: its source is in the build (§2), read it before saying what Pure prints.
- **An answer is not a go.** "okay" to a recommendation agrees to the decision; building it now is a separate question
  when the session was asked for something else. On 2026-10-28 the user asked for the handoff, answered "okay" to the
  negative zero, and the session built it unasked ("why are we running tests? did we change code? i thought we were
  building hand off doc?"). Ask "record it, or build it now?".
- Never commit a probe as code. Probes live in a scratch directory; their source is archived as text under
  `docs/result-boundary-homework/probes/` and their numbers quoted in GATES with the date.

**While coding**
- Develop and push only on the designated branch (`git push -u origin <branch>`). **No session pushes to `main`
  without the user's explicit word in that session** (the 2026-10-26 incident: 15 CI lanes red). A landing of more than
  one change goes through a PR.
- Commit messages: plain, dated where a fact is cited, honest about what was NOT run, no model names; end with the
  attribution lines the session's instructions give.
- `docs/IN_FLIGHT.md`: announce each step before touching its files; land with its GATES entry.
- **Records land with the code** (the audit's first lesson): GATES, IN_FLIGHT, the program's §9 line, a register row
  for every deliberate difference. None of these is a later step.
- **Every switch is preceded by running old and new side by side on every lane**, and each lane proves which route ran
  (a marker per crossing, counted); every deletion is in the same step as its switch (R7). Deleting first and
  measuring later is how phases 3 and 4 lost passes (homework README §1).
- At most two heavy Bazel jobs at once; Bazel runs one command at a time per output base.

**Landing a step:** its own checks, the guards, the local gate; the lanes per test (§7); the GATES entry with the
numbers; the deletion; IN_FLIGHT and §9.

## 6. How to run each check (exact)

| Check | Command | Pass means |
|---|---|---|
| A core package | `bazel test //core:core_tests_<package>` (architecture, builtin, cache, compiler, exec, ide, integration, ladder, lexer, lineage, lowering, model, normalizer, parser, platform, protocol, resolver, server, setup, sql, test, testdatagen, testing, values; `//core:core_tests_root` the rest) | all pass |
| The guards | `bazel test //core:guardrails //core:census //pct:pct_discipline` | all pass (`JavaEvalLedgerTest` is in `census`) |
| PCT channel A, one lane | `bazel test //pct:pct_<lane>_{essential,grammar,relation,standard,unclassified} --local_test_jobs=2` (lanes: duckdb, sqlite, mongodb, h2 (relation only: `//pct:pct_h2`), postgres, mariadb, mysql, clickhouse, trino, snowflake, bigquery, databricks); 4 GB heap each | each suite exact against its ledger (§7 reads the failures) |
| PCT channel B | `bazel test //pct:pct_channel_b_<suite>` (DuckDB) or `//pct:pct_channel_b_<lane>_<suite>`; `//gates:pct_channel_b` | per test identical to the previous commit |
| The corpus | `bazel test //gates:corpus_duckdb //gates:corpus_h2` and `//spec:judge_lanes` (both judges) | the rosters unchanged |
| The served routes and legend-engine's answers | `bazel test //core:core_tests_server` (`PureV1ApiTest`) | all pass |
| DataCube and Python on the same Java | `bazel test //datacube:python_execute_test` (builds the native library and the WebAssembly planner; about 12 minutes cold) | all pass |
| Postgres targets in a cloud container (root) | `cp tools/postgres/as_unprivileged.sh $SCRATCH/; bazel test <target> --run_under="$SCRATCH/as_unprivileged.sh ubuntu" --strategy=TestRunner=standalone` (`docs/CLOUD_SESSIONS.md`) | all pass |
| The local gate | `bazel test --lockfile_mode=error //gates:local` | all pass |
| A cloud container's first build | `tools/cloud-distdir.sh` once per container | Bazel resolves |
| Asking legend-engine | the homework README §7 | the answer recorded under `upstream-api/` |
| The old-door census | `python3 -I docs/result-boundary-homework/probes/oldpath_census.py.txt` | zero where a step says |

## 7. The lanes: running the other databases

**What runs where.** A cloud container runs DuckDB, SQLite, H2 and MongoDB, and the embedded servers (Postgres
unprivileged, MariaDB, MySQL, ClickHouse, Trino: Linux x86_64 only), but its disk allowance is about 3 GB free once
Bazel's caches are warm, so a session usually runs the local four and hands the rest to lane sessions. Snowflake,
BigQuery and Databricks need credentials, which only an environment's setup script provides (never the repository,
never a message).

**This account's warm lane sessions (2026-10-28; `get_session` says whether one is still alive):**

| Lane session | Databases | Id |
|---|---|---|
| step 9 lane | Postgres (unprivileged), SQLite, MongoDB | `session_01UmbETZRDU9ugZ8X2d1hGyb` |
| step 9 lane | MariaDB, MySQL, ClickHouse | `session_018ATdMqvopmrLibQhwN4GQ4` |
| step 9 lane | Trino, H2 | `session_01Hztqdk3qySkTkH2uUDuUdL` |
| step 9 lane | Databricks | `session_01757FUtRcj12NGhvSxL6A1b` |
| PCT lane | BigQuery | `session_019tku7nRKBygjSBz9QQBGfM` |
| PCT lane | Snowflake | `session_01WDoDkeNvgCsaPVzNx6wGDA` |
| the core writer until 2026-10-28 (writes no more code; the next session is the core writer) | DuckDB, SQLite, MongoDB, H2 locally | `session_01Vz1L3X6p56AZdTCQXUXYfE` |

**From another account** none of these sessions, their environments or their credentials exist. Raise your own:
1. An environment per credential set, whose setup script writes the properties file `LEGENDLITE_PCT_CLOUD_PROPERTIES`
   names (`docs/CLOUD_BACKENDS_PCT_2026_10_11.md` §4 the file's keys, §6 a cloud session's setup). The script lives in
   the environment's configuration, not in the repository; nobody pastes a key into a session.
2. One session per lane group (the table above is a good split: the embedded servers fit two or three per container),
   created on this branch (`create_session` with `source_url` the repository and `source_revision` the branch).
3. Each lane bootstraps Bazel (`tools/cloud-distdir.sh`), then measures the base commit once and keeps its logs: a
   lane's value is its history.

**The brief** (every message to a lane, plain text, under 64 KiB; the Snowflake lane refuses encoded blobs):
> MEASURE ONLY: no commits, no pushes, no credential reads, no tracked file edited. `git fetch origin <branch>`;
> `git checkout --detach <commit>`; a clean tree. Run <channel A / channel B>, <suites>, on <databases> (Postgres
> unprivileged), <with any --test_env flag>. Compare each suite with your <previous commit> logs (no need to re-run
> them). I expect <exact / identical per test>. Report: one table per lane; every lost pass, every moved message and
> every channel B difference in full (test name, expected and actual text); the route markers counted
> (`grep -c '\[LegendLite PCT\] engine JSON:'`); leave the tree clean.

**Reading a report.** `docs/result-boundary-homework/probes/compare_lanes.py.txt` is the comparison every lane ran:
channel A failures by their wording (STALE, MOVED, LOST), channel B per test, the markers. A report without marker
counts after a route switch is not yet evidence (Databricks and Snowflake each had to be asked on 2026-10-28).

## 8. Traps already hit (add to this list)

- **The disk allowance** (about 3 GB free in a warm container): clear `$(bazel info output_base)/sandbox/*` when idle;
  the engine runner's jars are about 600 MB; the repository-contents cache holds an 11 GB toolchain, do not delete it.
- **A local `git stash` in the original container** (`stash@{0}`, a Snowflake per-key column fix of 2026-10-21) is
  obsolete: its change landed as `deaceee4`. The auto-mode classifier refused dropping it; it dies with that container.
- **Register ids are taken by whoever lands first**: a branch's new row takes the next free id at its merge with `main`
  (S1.0's collision: `main` and the branch both used S112 to S114 on 2026-10-27/28).
- **`BigDecimal` and a double's text**: `new BigDecimal(Double.toString(d))` keeps the mantissa's scale (`1.0E-4` ->
  `0.00010`), and `BigDecimal` has no negative zero (`BigDecimal.valueOf(-0.0)` is `0.0`): a double's text routed
  through it needs a test of both.
- **The census bans naming a database** in a test that is not its owner (identifiers, `jdbc:` URLs; a database's name
  inside a Pure model string is fine): `DatabaseOwners`, `DialectLeakRatchetTest`.
- **`JavaEvalLedgerTest`'s line pins are exact both ways**: a shrink must re-pin, with a dated reason.
- **Closing a parked item deletes its anchor** in `ParkedWorkLedgerTest` in the same commit.
- **The engine-parity H2 dialect is not H2Modern**: H2 2.1 and 2.2 (the engine's) keep this dialect's walls; `fromJson`
  there answers a JSON string (track F).
- **`toVariant()` of an empty value is SQL's NULL on DuckDB** (register S116, the branch's S113 until S1.0): a test over
  nullable data sees an empty Variant.
- **CI runs only on pushes to `main` and on PRs**, and ignores `**/*.md`: a branch push runs nothing.
- **A lane can be idle and say nothing**: `get_session` gives its task summary; `list_events` its last messages.
- **The old traps of this line, still true:** the clouds' channel A on a system-mode connection (2.4x slower: use the
  suite's held session); a tagged column's plan slot needs its `Cast(..., SqlType.Tagged)` identity; every exhaustive
  switch over `SqlExpr`/`SqlType` needs the new arm (`SqlRewriter`, `FoldToListReduce`, `UnqualifyPivotArgs`,
  `SubselectPrune`, `Windows`, `AnsiSqlRenderer`, `MongoDb`, Snowflake's `raisedAs`, BigQuery's `bqType`,
  `ColumnFacts.slot`, `MirJson`); `PlanJsonTest` pins the format version; Snowflake's Float text caps at 16 significant
  digits (`docs/RESULTS_AND_RENDERERS_2026_10_22.md` §10.5); a probe's VALUES rows are run-time values on Snowflake;
  `//gates:local` as root needs the unprivileged Postgres run; TeaVM's 1 MiB data-segment cliff (PARK-40); `bazel
  info` blocks while another build runs.

## 9. What was never in the repository, and where it went

| What | Where it lived | Where it is now |
|---|---|---|
| the audit of phases 1 to 5, the lost-pass analysis, how the PCT works, the three-stage plan | a session's conversation | the homework README §1 to §3; this page §3 and §4 |
| the maps of the code (corpus path, judges' sides, `StatementExecutor`'s kinds, the decided end state) | a session's scratch log | the homework README §6 |
| the 51 writer differences | a session's scratch file | `docs/result-boundary-homework/wire-differences-2026-10-27.tsv` |
| the census of core's tests | a session's analysis | `docs/result-boundary-homework/test-census-2026-10-28.tsv` and its script |
| every probe's source; the census and comparison scripts | scratch directories of this session and the lanes | `docs/result-boundary-homework/probes/` |
| what legend-engine answered | a probe's output | `core/src/test/resources/upstream-api/` and the homework README §7 |
| what legend-pure prints for a Float (its source read, the negative zero, the `0.00010` fix measured) | this session's reading and scratch programs | the homework README §7; `probes/FloatTextProbe.java.txt`; register S117; S1.4 item 2 |
| how far `main` moved past the branch (105 commits, 12 files on both sides, the register ids) | this session's `git` comparison, 2026-10-28 | S1.0; GATES 2026-10-28 (S1.4, item 2), item 4 |
| the user's rulings | the conversation | §10 below and the program's §8 |
| the lanes and their credentials | other sessions and their environments | §7 (how to raise your own); credentials never copied |
| other branches | `origin/claude/step9-*` (the step 9 lanes' branches, merged on 2026-10-19: history); `origin/claude/deephaven-3-browser` (the Deephaven line, `docs/DEEPHAVEN_START_HERE.md` there); `origin/studio-program` (the Studio line) | not this program's |

## 10. The user's rulings and words, in order

| Date | Words | What it decided |
|---|---|---|
| 2026-10-24 | (D6, the renderers redesign) | two first-class renderers, LEAN and LEGACY |
| 2026-10-26 | the eight rulings, "arrow look good", "sounds great" (the program's §8) | the batch is our own representation; no Arrow library anywhere; the Float rule; the database judge stays (R10); zero dependencies (R9) |
| 2026-10-26 | "use the sessions that connect to bq, snow, dbx in their own parallel sessions to iterate fast only if that helps" | lanes measure in parallel; core has one writer |
| 2026-10-26 | after the push to `main` without a PR: "reset" | AGENTS.md "Pushing to main": no session pushes `main` without the user's word; landings through a PR |
| 2026-10-27 | "Skip the guards - let's actually do the real work of making the plan real and deleting everything else including the string spellings and old execution - keep going in phases, do expert work, audit after each phase" | phases 1 to 5 |
| 2026-10-27 | "we need to pause and do an audit - what did we actually do? did we do it right? did we stick to our core tenets?" | the audit (homework README §1) |
| 2026-10-27 | "keep it, and keep going" (answering the audit's first question: keep phase 5's commit) | the three-stage plan, stage 1 first (the program's §8, ruling 9) |
| 2026-10-28 | "Do the probe" | PARK-34 closed by asking legend-engine |
| 2026-10-28 | "Do it" | the engine crossing measured on every lane, then switched; `PureTdsText` deleted |
| 2026-10-28 | "i would prefer not to delete the overlapping tests but use the exact same logic but with platform output that we think users would actually run/use instead of backwards compat mode - so its almost the platform twin of PCT/core-relational" | track T |
| 2026-10-28 | "write this all up ... make sure there is nothing in this session ephemeral that we lose" | this page and the homework folder |
| 2026-10-28 | "any output spelling changes need to come to me on case by case basis" | no session accepts or registers a spelling change of any output: each goes to the user alone, before and after, and the answer is recorded (S1.4, S1.5, every track) |
| 2026-10-28 | "yes keep arrow writer at the boundary in this program (but no arrow dependencies in compiler!)" | track A stays in this program, at the boundary only; R9's guard |
| 2026-10-28 | "writer guards are for what exactly" | answered (S1.4 item 3 keeps the explanation) |
| 2026-10-28 | "yes add this to the plan" | the two writer guards are built in S1.4, right after the precision fact |
| 2026-10-28 | "what should we do about the negative zero? what is the right move", then "okay" to the answer | LEAN JSON and CSV write `-0.0`; Pure's `toString` stays `0.0` (S1.4 item 2, register S117) |
| 2026-10-28 | "why are we running tests? did we change code? i thought we were building hand off doc?" | an answer is not a go (§5) |
| 2026-10-28 | "keep the fix, fix the plan wording including adding that we need to fix 0.00010 .. then do a full read of the what you wrote - make sure we capture all the homewokr and detail and plans in detail for new sesison without any context - do not make any more code changes here" | `b944a42e` kept; the `0.00010` trailing zero ruled a defect to fix (S1.4 item 2); this session writes no more code; the handoff re-read whole and completed (S1.0, §12) |
| 2026-10-29 | "wherever possible we should stick to upstream legend-pure and legend-engine as possible - every answer should be 'can we do exactly as pure/engine does, or as close to as possible where too expensive to be exact but with an explicit posting to me and a ledger for diverging' every decision should be settled with that tenet" (answering S1.4's report, its two open questions among them) | the upstream-fidelity tenet (AGENTS.md; §5's first rule; the register's header): exact upstream by default, a divergence only where too expensive, posted and ledgered; S1.4's open questions settled by it (§4, S1.4); S1.5's classification of the 51 writer differences is by upstream's answer |
| 2026-10-29 | "The standing rule is if changing to something that diverges from upstream not if change something in lite - but have you checked pct and core-relational? Are you sure it's always 9 digits expected? I feel like lite mostly copied what tests expect / Also no need to stop, just keep going all the way until done with whole program and all old ways to execute are deleted - do a deep audit after every phase and keep a log of decisions you made or wanted to so you can post me on decisions you made after each phase too with audit - don't stop keep going" (answering S1.4's correction report) | three readings, each recorded where it acts: (1) the 2026-10-28 rule ("every spelling change to me case by case") binds a change that DIVERGES from upstream; a move TOWARD upstream needs no asking (AGENTS.md's tenet paragraph; the decisions log D4, D13, D14 rest on it); (2) the nine-digit claim checked against the PCT and core_relational (S1.4 corrected: the homework README §7, D5); (3) the program continues without stopping between phases, an independent audit after each, a decisions log posted after each (`docs/result-boundary-homework/DECISIONS_LOG.md`; D11 reads "keep going" as the word for S1.6's DRAFT PR, never a merge). Added to this table 2026-10-29 by the S1.5 audit's finding 2 (the words were quoted only in the program's §9 until then) |

## 10a. The open questions for the user, in one place (2026-10-29, after Z)

Every question the program posted and the user has not ruled; the recommendation is the session's, the call is
the user's. Where a row names a register id, the row's text holds the expression, upstream's answer and lite's.

| Id | The question | Recommendation |
|---|---|---|
| S119 / D15 | LEAN JSON's NaN or infinite Float under a Float column: refuse (legend-pure), `null` (legend-engine's TDS), or the name as a string (today) | refuse, as legend-pure does (the PCT route already does) |
| S121 / D18, D17, D7 | a DateTime's zone written from the HOUR (lite) where legend-pure writes `+0000` from the MINUTE; an anonymous instance printed as `@_` + id (lite) vs `@_0000003` (the interpreter) / `Anonymous_StripedId` (compiled); the host judge's trim by the slot's text | the minute (legend-pure's rule); the interpreter's instance text; the trim by the precision fact |
| S123 | a Decimal's place in a mixed sort: with the numbers (lite) or after dates, Booleans and Strings (compiled legend-pure, not transitive) | keep lite's (a total order); ledger it |
| S124 | a relation of no order of its own in value position: the rows' arrival order (legend-pure) or lite's deterministic numbering (its own sort keys, else an ordered source's, else every column ascending) | keep lite's; ledger it |
| S125 / D55 | the server's ARROW_IPC format vs upstream's S28: timestamp[ns, UTC] vs [us, UTC]; decimal128 at the slot's scale never rounded vs HALF_UP; typed columns vs JDBC-driven or utf8; per-field `legend.*` metadata | keep lite's (lossless); ledger it |
| S126 | `asOfJoin` against an empty right side: DuckDB's own 0 rows (a defect of its planner against a proven-empty derived table) or the spec's left rows (the emulation on every other database) | RESOLVED 2026-10-29 by the driver (D60): core's DuckDB bumped 1.4.4.0 to 1.5.6.0 on the user's ask; 1.5.5 and later keep the left rows, the test asserts the spec on every lane. Open: whether the warehouse's own pin (1.5.5.1) should be the same version |
| S127 / D50c | numbers of different kinds equated by their TEXTS in compiled legend-pure (`3 == 3D` true, `1 == 1.0` false): lite refuses the crossing pairs by name in a mixed list, and the scalar rule is unmeasured | build the text rule only if a user program needs it (the float's shortest digits in SQL); else keep the refusal and ledger |
| D8 | the tab prints a Float's negative zero as `0` (`signDisplay: 'negative'`) | keep: a display representation (no pinned upstream for a grid's text) |
| D9 | LEAN's nested dates (a struct's, a list's) keep the value's own precision with `+0000`; legend-engine's graph-fetch JSON writes nine digits and no zone | keep LEAN's (the lean writer is lite's own); ledger if the user wants the engine's |
| D10 | LEAN's Float with an exponent: legend-pure's layout (`0.00000025`) or legend-engine's JSON (`2.5E-7`) | legend-pure's (the Float rule's shortest digits) |
| D16 | an instance's text in CSV and `toString()` | the interpreter's (the PCT's reference): `@_` and a counter |
| D30 | what the body driver keeps in Java (call frames, `assertError`, `$plan` navigation, `planToString`, `print`, the generator's fold) | keep as named residue (planning, carriage of an error, effects); the generator's JDBC on its session was door 2 of "the doors" (§4), CLOSED 2026-10-29 (D62): every statement of a run is the plan runner's |
| D36 / D37 | `ProgramReport.last`: the one materialized side a public entry returns (the service test runner's) | CLOSED 2026-10-29 (door 1, D63): the last statement streams to the caller's sink; `ProgramReport` deleted; `TypedSide` off the public surface |
| D51 | `Fold.conformValueEgress` (step 5's delete): add a written-digits fact at scalar roots and delete it, or keep it and strike the step | (a): the fact, then the delete |
| D56 | switch the Python route and the tab to the new Arrow writer (changes the Python route's existing bytes: DuckDB's types to the plan's slots'; timestamp[us] to [ns]) | switch, with the bytes' change posted |
| D57 (4) | a DAY under a TIMESTAMP column: the JSON route spells its day, the Arrow route refuses | refuse in both (a timestamp holds no day) |
| D57 (10) | a tenth subsecond digit: the JSON route truncates silently, the Arrow route refuses | refuse in both (a value is never truncated) |
| D58 (9) | the multi-key ordered list aggregate (a mixed sort's ties; the rank struct's text order on SQLite and MariaDB): plan format 20, ten dialect arms, the MariaDB lane | build it (a day) if the MariaDB lane matters now; else a README row |
| D58 (10) | the lane twin of the mixed-list gate on SQLite and H2 | build it (an hour) |
| D58 (11) | a typed kind for a tagged slot; the H2 planner's exponential nesting; a BIT column's type; Snowflake's far-BC timestamp; `HostRows`' Arrow nested values; the PCT heap | each a README §9 row; none urgent |
| D59 (4b) | the MongoDB lane of core's integration tests (the seeds through the session seam) | build it after the landing |
| D59 (12) | PR #80: merge as is (96 commits, two merges of `main`) or squash | the user's call; the GATES entries are the per-landing record either way |

## 11. Reading order

1. This page.
2. `docs/result-boundary-homework/README.md`, all of it.
3. `docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md`: §0 (the plan in plain words), §1 (the rules R1 to R10), §8 (the
   rulings), §9 (the status lines).
4. `docs/RESULTS_AND_RENDERERS_2026_10_22.md` §0 to §5a and §10.5; `docs/PLAN_AND_RESULT_MODEL_2026_10_18.md` §2 to §6.
5. `docs/PLAN_PATH_PROGRAM_2026_10_17.md` §5 (lets), §6 (both judges on plans), §7 (steps 8 to 11).
6. `docs/TENET_CHARTER.md` Clauses 1 to 4; `docs/SEMANTICS_REGISTER.md` S104 and S115, S116, S117 (the
   Variant digits, `toVariant()` of an empty value, the negative zero; the branch's S112 to S114 until S1.0, `main` has its own S112 to
   S114).
7. `docs/GATES.md`'s entries from 2026-10-26 on (the merge, phase 5, the crossing, PARK-34, the switch, S1.4's negative
   zero).
8. Before the first message to a lane: `docs/CLOUD_BACKENDS_PCT_2026_10_11.md` §4 to §6 and `docs/CLOUD_SESSIONS.md`.
9. The code: `ExecutionPlan.java`'s javadoc, `exec/PlanRunner.java`, `result/CellText.java`, `PctPlanRoute.java`,
   `AbstractDatabaseTest.java`, `BodyDriver.execute` (`StatementExecutor` until track D).

## 12. The message that opens a new session

What the user pastes into a fresh session, from any account (kept here so it is not lost; update the commit and the
task when they move):

> Repository neema2/legend-lite. Continue the result boundary program from branch claude/wizardly-dirac-rw6tim at its
> head (the track Z fold of 2026-10-29; every track built, see the start page's §0 and §3): fetch it; if your session has its own
> designated branch, create that branch from this head. Never push to main, never merge without my explicit word in
> your session; S1.6's draft PR opens under my "keep going" of 2026-10-29 (the decisions log D11), the merge is mine.
>
> Read first, in full: AGENTS.md, then docs/RESULT_BOUNDARY_START_HERE.md (the whole roadmap, the rules, the commands,
> the traps, my rulings in §10), then docs/result-boundary-homework/README.md, then
> docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md §0, §1, §8 and §9.
>
> Your task: what the start page's §0 "next action" says (after Z: the landing, on my rulings of §10a; before it was
> the next item of §4 in order -- S1.6's draft PR, then tracks B, T, C, D, L, A, F, Z), without stopping between phases: after each phase an independent audit of the code against the
> tenets, its findings fixed, and the decisions log (docs/result-boundary-homework/DECISIONS_LOG.md) posted to me
> with what was decided and what is wanted of me. Announce your files in docs/IN_FLIGHT.md before you start.
>
> Rules that bind you: upstream first (AGENTS.md's upstream-fidelity tenet): every decision settled by "can we do
> exactly as legend-pure and legend-engine do?", read from the pinned sources or asked of the engine runner, never
> recalled; a move toward upstream needs no asking; a divergence from upstream is posted to me before it lands (the
> expression, upstream's answer, lite's) and ledgered in docs/SEMANTICS_REGISTER.md. An answer of mine to a question
> is not a go to build something else; ask. A guard pin or allowlist moves only with a dated justification. Never
> read, print or copy credentials; lane sessions measure only. Run the start page's §6 checks before each commit;
> commit and push to the branch; records (GATES, IN_FLIGHT, the program's §9, register rows) land with the code.
