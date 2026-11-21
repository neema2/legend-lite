# Item 3, start here: a handoff for a session with no context

You are picking up **item 3 of legend-lite's compatibility program**: assert verdicts become records, and the judges'
rules become mechanical. This file says how to work here. The design says what to build:

- `compat-assert-records-2026-11-21.md` (beside this file) has the target, what the homework found, the design, the
  slices S0-S9, the guards, the measurements M1-M6, the decisions Q-A to Q-D, and the risks.
- `compat-assert-records-homework/code-map.md` has lite's code: every site, with file:line and its size.
- `compat-assert-records-homework/upstream.md` has legend-pure's and legend-engine's assert behaviour, each claim
  cited from the pinned sources.
- `compat-assert-records-homework/fast-round-targets.txt` is the local test round the previous session ran before
  every commit.

Written 2026-11-21 by the compatibility program's owner session.

## 1. Read these first, in this order

1. `AGENTS.md` at the repository root. It holds the architecture's invariants, the pipeline and the rules on guard
   files. It is binding.
2. `compat-assert-records-2026-11-21.md` (the design), then `code-map.md` §0 and `upstream.md` §§0-3.
3. `docs/records/compat-error-model-2026-11-21.md`: BE, the one error model this builds on. Read §1 (the rule),
   §4 (D) and §7 (what was delivered and what was deferred).
4. `docs/SEMANTICS_REGISTER.md`, rows S389, S392 to S397: the user's rulings item 3 must respect.
5. `docs/records/compat-stage-b-plan-2026-11-20.md`: stage B, where item 3 sits (before B3).
6. `docs/CLOUD_SESSIONS.md` (Bazel in a cloud container) and `docs/GATES.md` (the gate chain).

## 2. Where to build

- **The base.** Build on `main` once PR #131 (BE) has merged. Until then, build on `claude/compat-error-model`
  (BE's head, `9fa42018`), because item 3 changes BE's record types.
- **The branch name** is the repository owner's to give. Ask before you push. The program's own records live on
  `claude/compat-program`.
- **The code map's line numbers** are at `dad8a710`. BE's later commits touched `HostJson`, `Failure`, `PlanJson`,
  `MappingNormalizer`, `ChainDispatch`, `Substitution` and the dialects. Find each site by its symbol, not its line.
- **Do not push to `main`, open a PR, or merge** without the owner's word in your session (AGENTS.md, "Pushing to
  main").

## 3. Words this program uses

| Word | Meaning |
|---|---|
| the owner, the user | the repository owner. Their word, typed in your session, is the only authority for a ruling, a moved pin or a landing |
| the product plan, the test plan | the planner's output for a query (`executionplan/`, `PlanJson`, version 60) and for a test body (`testexecutionplan/`, `TestPlanJson`, version 37). Both are data: no Java, no prose |
| the host judge, the database judge | the two ways a test's asserts are judged. The host judge compares values in Java after the run (`testplan/HostAsserts`, `testexec/HostJudgments`). The database judge decides in SQL (`testplan/DatabaseAsserts`, `DatabaseJudge`, `lowering/VerdictSql`). BOTH runs each alone and compares them (`testexec/IsolatedRun`) |
| the edge, the compatibility path | everything that makes lite answer as legend-pure or legend-engine does (`com.legend.edge`, `PlanStyle` other than PRODUCT). Its test runs are PCT and the relational corpus |
| PCT, channel A, channel B | legend-pure's platform compatibility tests. Channel A runs them through upstream's own runner (`Test_LegendLite_*_PCT`). Channel B runs them through lite's test road (`pct/.../channelb`) |
| the LITE-PCT lane | channel B under lite's own rules (`//pct:pct_channel_b_lite_*`). It is the only PCT lane the database judge runs on |
| the relational corpus | legend-engine's `core_relational` tests (`spec/.../rcorpus`, `MinimalCorpus`). Its rosters are under `spec/src/test/resources/rcorpus/` |
| the ladder | 12 hand-picked verdict statements with current and lean SQL (`core/src/test/resources/ladder/*.{current,lean}.sql`, `//core:core_tests_ladder`, `//core:update_ladder`) |
| D | the user's ruling (S392): a test body lite cannot fully plan is refused whole at planning; an error keeps its turn only if upstream raises it at run |
| Q15 = A | the user's ruling: lite's own assert-failure words on every route, one catalog |
| A4 | the earlier change that refuses the database judge on every compatibility road (`TestPlanner.refuseTheDatabaseJudgeInCompatibility`) |
| S-rows | `docs/SEMANTICS_REGISTER.md` rows: every deliberate difference from upstream, PROPOSED until the owner rules it |
| the pinned sources | legend-pure and legend-engine at the versions the build fetches: `$(bazel info output_base)/external/+http_archive+legend_pure_src` and `…/+http_archive+legend_engine_src`. Upstream's answer is read there, never recalled |

## 4. How to run things

- **One Bazel run at a time.** Two at once run out of memory in a cloud container. Run the PCT lanes alone. Do not
  edit the tree while Bazel runs in it.
- **Postgres in a root container.** `initdb` refuses root. Copy `tools/postgres/as_unprivileged.sh` outside the
  workspace and pass `--strategy=TestRunner=standalone --run_under="<copy> ubuntu"`. A target whose data files
  cannot be reached that way fails with "not in this test's runfiles"; CI runs it as non-root.
- **The local fast round** (about 25 minutes): `bazel test --keep_going $(cat
  docs/records/compat-assert-records-homework/fast-round-targets.txt)`. That is every core target including the
  `_judge_database` and `_judge_both` variants, the engine-client and Python suites, the strict typecheck and the
  WASM routes.
- **The lanes item 3 moves**, run before each commit that touches them:
  - **Judges and plans:** `//core:core_tests_testplan`, `//core:core_tests_testexec`, `//core:core_tests_judges`,
    `//core:core_tests_ladder`, `//core:guardrails`, `//core:guardrails_edge` and `//core:core_edge_tests`.
  - **PCT:** `//pct:pct_channel_b_lite_{essential,grammar,relation,standard,unclassified}` and
    `//pct:pct_duckdb_lite_*`, alone.
  - **The corpus:** `//spec:corpus_duckdb_verdict`, `//spec:corpus_h2_verdict`, the `update_rcorpus_*` and
    `update_edge_corpus_*` diff tests, and `//spec:lean_sql_corpus_{duckdb,h2}`.
  - **Sizes:** `//core:lean_sql_baseline_test` and `//pct:update_ratchets_test`.
  - **`//parser-equivalence:parser_parity`** (5 minutes). Every new test model literal moves its floor, and the last
    session met that failure first in CI.
- **Regenerations.** Each writes a pinned file. A move needs the owner's word (§5) unless it only shrinks.
  - `bazel run //core:update_lean_sql_baseline`
  - `//spec:update_lean_sql_corpus_{duckdb,h2}`
  - `//pct:update_ratchets`
  - `//parser-equivalence:update_ratchets`: the dated note goes in `PeRatchets.java`, not the generated `.tsv`.
  - `//spec:update_rcorpus_{duckdb,h2}`
  - `//spec:update_edge_corpus_{duckdb,h2}`
  - `//core:update_ladder`
  - `//docs:update_dialect_capability`
- **Lanes this container cannot run.** The database servers (MariaDB, MySQL, SQLite, Trino, ClickHouse, MongoDB) and
  the cloud databases (BigQuery, Snowflake, Databricks, whose credentials are in the dialect program's warm sessions)
  run in the owner's warm sessions. Ask the owner to have them run; do not assume they pass.

## 5. Rules of engagement (the owner's standing rules)

1. **A pin, ceiling, ledger, golden, roster or allowlist moves only with the owner's word and a dated note** naming
   the task. A move that only shrinks is pre-approved and still dated. List every move in the PR's pins table.
2. **Upstream's answer is read from the pinned sources or asked of the engine runner, never recalled.** A
   difference from upstream is a PROPOSED `SEMANTICS_REGISTER` row, posted to the owner before it lands.
3. **No fallbacks or defaults (AGENTS.md invariant 4).** Refuse by name where lite cannot do a thing. A defect fails
   loudly.
4. **Measure before building.** Bring M1, M3, M4 and M5 with the decisions to the owner first.
5. **An independent audit after each slice,** by a reviewer that wrote none of it, run in the background, its
   findings fixed or named.
6. **Commits end with the session's trailers.** Never name a model in a commit, PR or code comment.
7. **Never read, print or copy a credential.**

## 6. How to start, step by step

1. Read §1. Build the base (§2). Run the fast round once to get a green baseline before you change anything.
2. **S0, the dead code** (needs no ruling): the design's S0 row lists it. Commit it with the round green.
3. **The measurements:**
   - M1: prototype three ladder statements.
   - M3: count the asserts planned as `Messaged` with an unplanned text, on the PCT and corpus lanes.
   - M4: count the `Literals.fold` firings per caller.
   - M5: grep PCT and the corpus for the `assertError` edge cases.
   Each is described in the design's §7. Write the results down.
4. **Bring the measurements and Q-A to Q-D to the owner** (design §8, each with a recommendation). Stop there until
   they rule.
5. **Then S1 to S9 in order** (design §5), each green and audited. S1 to S3 need only Q15 = A, already ruled. S4
   waits for Q-A, S6 for Q-C, S7 for Q-D and S8 for Q-B.

## 7. What not to touch

- **B3's ground:** the edge's appeal path (`edge/SqlTextVerdicts`, `Appeal`, `EngineTextJudgment`), the referee,
  `Failure.unjudgedReason`, and E1. Item 3 narrows `PlanTypesAreDataTest`'s residue for these and leaves them.
- **BE's named follow-ups** (the error-model record's §7 item 8): S396, S397, the dialect class names in refusals, the
  row class of a cast, #31, #27.
- **The two heavy targets red on main** (`//spec:update_reference_lane_test`, `//spec:manifest_world_census`): a
  separate fix to `TypeClassifier`.

## 8. Traps the last session met

- **The parser floor moves when you add a test model** (rerun `parser_parity`).
- **`PlanTypesAreDataTest` denies by default.** Every new text component of a plan record must be classed as a fact,
  or named as prose with the step that removes it.
- **`ArchitectureTest.staticStateIsConstantOrRegistered`** refuses a mutable static (`EnumSet`); use `Set.of`.
- **Every concrete `AnsiSqlRenderer` subclass needs `product()`,** test dialects included (`CarrierDifferentialTest.Portable`).
- **D refuses a body that holds one expression lite cannot plan.** A change that makes an expression unplannable can
  move many corpus bodies to "refused" with no verdict change. The lean-SQL corpus counts them.
- **A fused verdict statement is a `UNION ALL`,** so its columns need one type across members (design §9).
- **`JavaEvalLedgerTest`'s line pins** (`AssertVerdicts`, `DatabaseJudge`, `HostJudgments` and more) move with any
  edit to those classes. Re-pin with a dated note.
