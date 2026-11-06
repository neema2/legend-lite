# Closing PCT and core, then the corpus: START HERE (2026-11-06)

A session that knows nothing reads this first. It says what the program is, what the user ruled, where everything
stands as of 2026-11-06, what was learned the hard way, and the roadmap in order, starting with PCT and core closed at
100%. It points to the deeper records instead of repeating them; where a fact matters for the next step it is stated
here, with its source.

Read in this order: §0 and §1 (ten minutes), §2 (the rules -- breaking one costs the user's trust), §5 (the state),
§6 (what to do next), then §8-§10 as you need them. `AGENTS.md` is the architectural law and is always loaded.

---

## 0. In one page

**legend-lite** (`core/` is the product) replaces legend-pure and legend-engine: it compiles Pure (a modelling and
query language) to SQL and runs it on the database -- "Java orchestrates, the DATABASE executes" (`AGENTS.md`). It is
measured against upstream with two test suites:

- **PCT** (the Platform Compatibility Tests, about 1,250 Pure test functions in five suites: essential, grammar,
  relation, standard, unclassified), on twelve databases: DuckDB, H2, SQLite, Postgres, MariaDB, MySQL, ClickHouse,
  Trino, Databricks, Snowflake, BigQuery, and MongoDB (channel A only; H2's channel B runs the relation suite only,
  `pct/BUILD.bazel` line ~495).
- **core's own tests** (`//core:core_tests`, about 4,000 JUnit tests), and the same packages under each judge
  (`//core:core_tests_judges`).

Then **rcorpus**, the relational corpus (about 2,600 legend-engine relational tests, `//spec:corpus_duckdb`,
`//spec:corpus_h2`), comes next.

**The program on this branch** -- "the test is the product" -- runs every test body the way the product runs a query:
the whole body is compiled into ONE plan before anything runs (the "program road"); the plan's runner executes it;
the asserts are verdict steps of the plan, judged by a **host judge** (Java compares rows the database computed), a
**database judge** (the database computes the verdict in SQL), or **both** (each assert judged twice and the two must
agree). Nothing is planned while a plan runs (the **runtime wall**); the execution library depends on no planning
library (the **exec wall**). The old way -- the **walk**, which interpreted a body statement by statement, planning as it
went -- is closed for core and PCT and still open for the corpus; deleting it is phase 3.

**The user's order (in force):**
1. PCT and core at 100% under the host and database judges, strict (no fallbacks), the plan/exec split complete and
   walled off at run time, every test "on the plan".
2. rcorpus the same.
3. Delete the walk.
4. An audit for special-casing and test overfitting.
5. Lean SQL ("we are going to have to look at these crazy large SQLs after we close PCT and core" -- the user,
   2026-11-06).

**Where it stands (2026-11-06, head `7c93fbc5` on `claude/wizardly-dirac-rw6tim-seam`, PR #83, draft):** round 22
of the lanes is green on DuckDB, H2, SQLite, Postgres, MongoDB, MariaDB, MySQL, ClickHouse, Trino and Databricks,
under both judges and the database judge alone. **Outstanding for 100%:** BigQuery's round 22 (it must accept a SQL
shape never run on BigQuery, §5.4), Snowflake's round 22, CI green on every OS (the corpus lanes are red on one test,
`strictdate::testProject`, §6.1 step 3), the cloud lanes measuring the session fix (`7c93fbc5`), the records, and the
user's decision on the channel B judge passes (§4). **All six lane sessions are blocked by the account's weekly usage
limit until 2026-11-08 03:00 UTC** (§9.2): BigQuery's and Snowflake's round 22 ran in the background when it hit, so
their results are probably on their disks, unreported.

---

## 1. What you are looking at

### 1.1 Where things are

| What | Where |
|---|---|
| The branch | `claude/wizardly-dirac-rw6tim-seam` (pushes are free; never `main`) |
| The PR | #83, draft, "The test body as one plan: PCT and core on the program road under the host and database judges". Only the user lands it. |
| The architectural law | `AGENTS.md` (always loaded), `core/README.md` |
| The program's plan | `docs/TEST_IS_THE_PRODUCT_PLAN_2026_10_31.md` (§1 rulings, §6 design briefs, §7 steps, §8 start-here, §9 open, §10 status -- a day-by-day log) |
| The previous handoff | `docs/TEST_IS_THE_PRODUCT_HANDOFF_2026_11_01.md` (M6-era; its §6 measurement scripts and §8 traps still hold) |
| Asserts as plan nodes | `docs/ASSERT_NODES_PLAN_2026_11_04.md` (the user's design, the rulings of 2026-11-05, progress) |
| The audit that set the bar | `docs/ASSERT_WALL_LEANSQL_AUDIT_2026_11_04.md` (§1 asserts, §2 the plan/exec wall, §3 lean SQL measured) |
| Every check's result | `docs/GATES.md` -- one entry per change, newest near the end; read the 2026-11-05 entries (from "The deep audit on PR #83" on) |
| Every divergence from upstream | `docs/SEMANTICS_REGISTER.md` (S1-S141). A divergence that is not a row is a defect. |
| Decisions | `docs/result-boundary-homework/DECISIONS_LOG.md` |
| Measurement evidence | `docs/test-is-product-homework/` (M0-M10 measurements, audit briefs, code maps) |
| Bazel in a cloud session | `docs/CLOUD_SESSIONS.md`; check commands `docs/RESULT_BOUNDARY_START_HERE.md` §6, lanes §7 |
| Who is working on what | `docs/IN_FLIGHT.md` |

### 1.2 The code you will touch most

| Package / class | Role |
|---|---|
| `com.legend.Execution`, `com.legend.Compiler`, `TypedQuery` | the public doors (`AGENTS.md` "Entry points") |
| `driver/BodyRouter` | routes a test body: the program road (planned whole) or the walk; picks the judge's planner (`BodyRouter.java:97`) |
| `driver/HostAsserts`, `DatabaseAsserts`, `AgreementAsserts` | the three judges' planners for a body's asserts (§4) |
| `judge/VerdictBatch` | the database judge's batch: verdicts fused into one statement per group, the split rung, `withinBudget` (§5.4) |
| `lowering/VerdictSql` | the SQL of a verdict row, `batch` (the fusion: `UNION ALL` of verdict rows, CTEs hoisted), messages |
| `driver/BodyDriver` | the walk (statement by statement); its door `driver/WalkDoor` |
| `exec/PlanRunner` | runs a plan on a session; `Judging.HOST/DATABASE/BOTH` |
| `base/RunScope` | the runtime wall (JDK-only, TeaVM-compatible) |
| `driver/SqlTextVerdicts` | the engine-text assert (compare lite's SQL text with legend-engine's golden) and its compatibility appeal |
| `compiler/spec/RelationCountAsserts`, `AssertLift` | S137; the lift of an assert from value position |
| `sql/dialect/*` | one renderer per database (`AGENTS.md` invariant 3); `SqlDialect.fusedStatementBudget` |
| `pct/.../channelb/ChannelB` | PCT channel B's runner; `-Dpct.channelb.judge` |
| `spec/.../rcorpus/MinimalCorpus(Test)` | the corpus runner and its rosters and registers |
| `core/src/test/.../testbackend/*` | test backends; `EngineH2Referee` (the corpus's referee), `CloudBackend` |

---

## 2. The rules (the user's, in force; verbatim where quoted)

**Process:**
- "Never push to main, and never open or merge a PR, without my explicit word." Branch pushes are free.
- Never read, print or copy credentials. Lane sessions measure only.
- A guard pin, ceiling or allowlist moves only with a dated justification naming the task (`AGENTS.md`, top).
- Records land with the code (GATES entry, register rows, plan status).
- Every divergence from upstream is a `SEMANTICS_REGISTER.md` row the user ruled; ask first, case by case, with the
  expression and upstream's answer read from the pinned sources (never recalled).
- "never change how any output is spelled without asking me first, one case at a time".
- **Never move the user's work to another branch, park it or reset it without asking** (2026-11-05: a session moved
  the uncommitted H2 work to a side branch on its own judgment -- "why did you do this? who asked you to move to
  another branch?"). Commit in place on the seam branch.
- **No hour-long waits; no rabbit holes.** Run the targeted check, report, move. When the user asks "what is going on",
  answer plainly in a few lines: what is done, what is red, what is next.
- Do not leave `/home/user/legend-lite-m6` deleted (an old worktree the user wants kept).

**Design rulings (all still in force):**
- The database judge has **no fallback**: "it should hard fail if cannot judge something and then we need to fix".
- **Zero exemptions** to the runtime wall ("yes to zero exemptions please"): nothing is planned while a plan runs;
  what compatibility mode needs after a verdict (the engine-text appeal's rows, its fixtures) is planned with the
  assert and run at the appeal (`ec0eed01`).
- "After a verdict, the harness may only explain" (a failure); anything that changes a verdict is in the plan or is
  compatibility-only.
- **Clean mode** (lite's own tests, `JudgingRules.LITE`) judges exactly; **compatibility mode**
  (`COMPILED_PURE_COMPAT`, PCT and the corpus) carries upstream's leniencies as named rules (S15, S131, S132, S127).
- **assertError: "the host catches, the database judges"** (S135). Under BOTH an assertError over an assert is judged
  by both judges (`2bed81ec`).
- Floats: every Float literal a DOUBLE (S20); clean mode exact, compatibility within 2 ULP (S15, S132). Decimal by value
  in clean mode (S139). A relation in assertSize/assertEmpty/assertNotEmpty: refused in clean mode, counted as one
  value in compatibility mode (S137).
- **H2: lite runs the latest, 2.5.252** ("I want latest version of H2", S141); the corpus's replay referee is
  legend-engine's own **2.1.214, loaded in isolation** ("Engine's 2.1.214, isolated").
- The fused-statement size: the user first chose to raise ClickHouse's AST limit; measured, that OOM-killed the server
  (§7.5); the fix is the planning-time budget (`c1bf51fc`).
- **No parallel lanes yet**: "that masks real problems".
- **Deferred by the user, not part of the 100% bar:** IEEE floats beyond S20, order tracking
  (`docs/ORDER_TRACKING_DESIGN_2026_11_05.md`, not ratified), the asserts' shared design (incl. the `'TDSNull'`
  sentinel), body shapes (until rcorpus).

---

## 2b. The tenets: plan/exec and judge/verdict (the law this program is held to)

Each is either the user's ruling or `AGENTS.md`'s law; the source is in brackets. A change that breaks one is wrong
however green the tests are.

**Plan and execution**
1. **Java orchestrates, the DATABASE executes.** No host interpreter; the query compiler executes no values; what Java
   may compute is decided by `docs/TENET_CHARTER.md` C1-C5 and `docs/JAVA_EVICTION_PLAN.md` §1 [AGENTS.md].
2. **Planning never touches a database** (C2a); the planner library has no execution dependency
   (`ArchitectureTest.plannerIsExecutionFree`) [AGENTS.md].
3. **Execution depends on no planning library** (D72): `//core:exec`'s deps are base, json, the records and the
   switchboards (`tools/deps/core-layers.txt`; `ArchitectureTest.execReachesNoPlanning`) [AGENTS.md].
4. **JDBC only at the seam** (D69: `JdbcSurfaceCensusTest.REGISTER`, each class pinned to its acts;
   `noJdbcTypeInAPublicSignatureOutsideTheSeam`); **a session is made by exec alone** (D70, `onlyTheSeamMakesSessions`);
   **every statement a session runs is the plan runner's** (`theRunnerIsTheOneCallerOfTheSessionsStatements`)
   [AGENTS.md].
5. **The runtime wall:** while a plan runs, nothing on that thread types, lowers, renders or composes a statement; every
   statement a run sends was planned before it started (`base/RunScope`, strict, **zero exemptions** -- "yes to zero
   exemptions please"); "closed off at runtime any plan vs exec stuff so they can never cross even through the driver"
   [the user, 2026-11-05].
6. **A test body is the product plan** (R1); the database judge is "exactly the same as product plan ... literally zero
   difference" except what is named test-only (R2); the host judge "could be exception to java rule only not exception
   to plan+exec split" (R3) [plan §1].
7. **The body is compiled whole, then run** (R14): an invalid or unsupported program is a compile error and the whole
   test fails; a valid program whose evaluation raises early is planned as a RAISE at its position [plan §1].
8. **Whole bodies move, never statements** (R16); a router decides by a static rule over the typed body, never by
   catching a failure [plan §1; handoff §8 trap 8].
9. **No plan names a server version** (S141); the dialect is the database type's.
10. **NO FALLBACKS, NO DEFAULTING** (invariant 4): an unknown is a failure, never a guess [AGENTS.md].
11. **Upstream fidelity**: do exactly what legend-pure/engine do; a divergence is a ruled register row, read from the
    pinned sources, never recalled [AGENTS.md].
12. **The reference checkouts are spec and test input only**, never runtime components (the referee replays the
    engine's golden; it does not run lite) [AGENTS.md].

**Judges and verdicts**
13. **Two judges over one plan:** each assert is a verdict step carrying the host's sides AND the database's verdict
    statement ("in the plan of an assert there would be a single sql lowering plan that db judge can run AND in same
    plan have the two sides with the enum of the assert function that the host judge can run") [the user, 2026-11-04;
    `ASSERT_NODES_PLAN`].
14. **The bar:** "anyone can write a NEW test function using any of the assert* constructs and they should just do what
    they say they do on the tin" -- upstream's model: `assert` is the one native, every other `assert*` a Pure function
    with upstream's message texts, which both judges spell the same [the user, 2026-11-04].
15. **The database judge has no fallback:** "it should hard fail if cannot judge something and then we need to fix" --
    a decline, a silent drop or an UNJUDGED verdict is a failure, never a hand-off to the host [the user, 2026-11-05].
16. **The database judge is self-contained** (R5): the verdict computed in SQL, the canon over the product plan's
    columns (R10), the fused statement kept ("we definitely still want the assert fuze", R4) [plan §1].
17. **After a verdict, the harness may only explain** a failure, never change it; the split rung explains, with its
    tripwire (a rejection no member reproduces is a loud error, never a pass) [ASSERT_NODES_PLAN, rulings].
18. **Nothing is planned or sent at judge time:** a caller's message, a member's own statement, a ruled fallback, the
    engine-text appeal's rows and fixtures are all planned ahead with the verdicts (`VerdictBatch.Ahead`,
    `SqlTextVerdicts.RowsAhead`) [the runtime wall].
19. **The judging rules are explicit in the plan** (`JudgingRules.LITE` / `COMPILED_PURE_COMPAT`), chosen up front;
    lite's own tests judge exactly ("I want ours to be super clean, but be able to pass PCT/rcorpus in backwards compat
    mode"); compatibility mode is judging only, one execution semantics [the user, 2026-11-04/05].
20. **assertError: "the host catches, the database judges"** (S135): the runner catches what `f` raised in a
    savepoint and binds it as a side both judges compare [the user, 2026-11-05].
21. **Lite's tests are deterministic** ("our tests in lite should always be deterministic and fail if not"): order
    tracking, report-only first; S131's unordered comparison compatibility-only [the user, 2026-11-05].
22. **BOTH is the agreement gate:** the host's verdict of record, the database's checked, a disagreement a failure
    naming both; a database decline under BOTH is a failure too (15) [`AgreementAsserts`].

---

## 3. The concepts (a glossary in dependency order)

- **Plan** (`ExecutionPlan`): data the planner produces and the runner executes -- statements with their SQL text,
  slots, target (database, session, setup), verdict steps, branches. It carries no server version since `53b52c80`.
- **Program road**: a test body planned whole as one plan (`Execution.runProgram` -> `BodyRouter` -> the test door
  `TestDoor.planned`), then run by `PlanRunner.program`. **The walk** (`BodyDriver`): the old interpreter of a body,
  statement by statement. **The walk door** (`WalkDoor`, `-Dlegend.walk.allowed`): `none` for core
  (`core/BUILD.bazel:673`) and PCT channel B (`pct/BUILD.bazel:463`); `all` for the corpus lanes (`spec/BUILD.bazel`,
  `spec/corpus.bzl`) and since `78554443` the stress suites (`core/BUILD.bazel:1617`: 444 graph-fetch and 17 temporal
  service queries the query program does not plan as a last statement yet). Default with no property: closed.
- **Judges** (`ExecuteOptions.JudgeMode`): **HOST** -- the plan computes each assert's sides; Java compares them
  (`judge/` package: the host lattice, `Equality`, the canon). **DATABASE** -- the plan's verdict statements compute
  holds/message in SQL. **BOTH** -- the agreement gate: both judges per assert, the host's verdict of record, a
  disagreement a failure naming both. §4 explains why each mode plans different SQL.
- **Judging rules** (`ExecutionPlan.JudgingRules`): `LITE` (exact) or `COMPILED_PURE_COMPAT` (upstream's leniencies),
  a required field of every verdict step, chosen up front (`ExecuteOptions.judgingRules()`; PCT and the corpus set
  compatibility, `withEngineCompat`).
- **Verdict / verdict step** (`ExecutionPlan.Verdict`, `Verdicts`): one assert's planned judgment -- its sides (host),
  its verdict statement (database), its message side (read only where it fails). A **segment**: the asserts between
  two non-assert statements; under the database judge a segment's verdicts are **fused** into one statement
  (`VerdictSql.batch`: a `UNION ALL` of verdict rows, each member's CTEs hoisted and deduplicated, frames as CTEs at
  the head). **The split rung**: if the database rejects the fused statement, each member's own planned statement runs
  to attribute the failure; its **tripwire**: a rejection no single member reproduces is "a composition bug", never a
  pass.
- **The runtime wall** (`base/RunScope`): `PlanRunner` marks the thread while a plan runs; the planning seams
  (`AnsiSqlRenderer.write`, the lowerer's entries, `SpecCompiler.compile`/`typeExpression`) refuse inside it by name
  (`RunScope.Refused`). Strict; `-Dlegend.runwall=report` is a diagnostic only. Zero exemptions (`ec0eed01`).
- **The exec wall** (D72): `//core:exec` depends on base, json, the records and the switchboards only
  (`tools/deps/core-layers.txt`); `ArchitectureTest.execReachesNoPlanning`, `noJdbcTypeInAPublicSignatureOutsideTheSeam`,
  `onlyTheSeamMakesSessions`, `theRunnerIsTheOneCallerOfTheSessionsStatements`, `plannerIsExecutionFree`,
  `theDriverHasOneCaller`; the JDBC census (`JdbcSurfaceCensusTest.REGISTER`).
- **Channel A** (`pct/.../Test_LegendLite_*_PCT`): the official legend PCT harness -- upstream's own Pure runner
  executes each test function and judges its asserts; lite is the backend each query is sent to (`LegendLiteAdapter`
  -> `PctPlanRoute` -> `Execution.run`). The yardstick shared with legend-engine (expected-failure lists comparable to
  the reference adapters'). It checks lite **as a query engine**.
- **Channel B** (`pct/.../channelb/ChannelB`): no upstream runner -- lite parses the PCT test functions, replaces the
  adapter call by its argument (`$f->eval(|expr)` is `expr`), and runs the WHOLE BODY, asserts included, as its own
  plan under its own judges. It checks lite **as the whole platform**. Census lines `[chB] census={PASS=n, ERROR=m}`;
  each lane's ERROR rows are ledgered (ruled divergences or upstream-known failures); "does not match its ledger" is
  the failure.
- **100% (the bar)**: on every lane, every channel B suite under BOTH and under the DATABASE judge alone matches its
  ledger (no unledgered failure, no ledgered row that passes), channel A matches its expected-failure list, core's
  matrix is green under HOST, DATABASE and BOTH, the guards and census are green, CI is green on every OS, and no
  line says "the runtime wall". Ledgered rows remain by design (e.g. Postgres essential 13).
- **The corpus's referee**: the engine's golden SQL for a test is replayed on legend-engine's own H2 (2.1.214,
  `EngineH2Referee`, an isolated class loader over `@h2_engine//jar`) beside the lane's session (the "mirror"); the
  engine-text appeal compares rows when lite's SQL text differs from the golden (compatibility only).
- **Rosters and registers** (`spec/src/test/resources/rcorpus/<lane>-*`): the fail roster (the host pass's failing
  tests), the unordered register, and the database pass's registers -- lost (pass on host, fail on database),
  outside-body (a statement sent outside the body's artifact), host-compared (an assert decided outside a verdict
  row; shrink-only, "must reach zero"), differential, untriaged, the unjudged ceiling. **The first register that
  differs ends the check** -- later ones are unseen (§7.2).
- **Lane sessions**: other cloud sessions with the server and cloud databases installed, which measure a commit on
  request and report (§9).

---

## 4. Why BOTH runs different SQL from the database judge alone (the user's question, 2026-11-06)

**First, BOTH is not "channel A plus channel B".** Channel A has no lite judge at all (upstream's runner judges). BOTH,
HOST and DATABASE are three settings of **channel B** (and of core's tests, `-Dlegend.test.judge`).

**The three settings plan three different programs** (`BodyRouter.java:97-102`):

| Setting | Planner | What runs | Statements per test (PCT essential, DuckDB, measured 2026-11-06) |
|---|---|---|---|
| HOST | `HostAsserts.plan` | each assert's sides as statements; Java compares at the verdict step | -- |
| DATABASE | `DatabaseAsserts.plan` | **one fused statement per segment** (all its asserts, frames as CTEs at the head, a frame never run at its let) | **2**: 705 for 345 tests (358 `body` + 343 `session`) |
| BOTH | `AgreementAsserts.plan` | **per assert**: the host's sides (`HostAsserts`) AND the database's verdict statement for that assert alone (`DatabaseAsserts.planEach`); "no batch is held across asserts and every frame runs at its let" (the class comment) | **8**: 2,762 (2,419 `other` + 343 `session`) |

Why BOTH is built that way: the agreement rule compares the two judges **per assert**, at that assert's turn, and the
host judge needs each frame's rows when its let runs; so BOTH reproduces the walk's agreement arm one assert at a time
(it predates the fused segments of M7). Consequences, all measured:

- **BOTH never runs the fused statement the product's database judge runs.** ClickHouse's 1.3M-character fused
  statement (`equal::testEqualNonPrimitive`) failed under the DATABASE judge alone and passed under BOTH (round 21b).
  So today the DATABASE-alone pass is the only check of the fused shape, and BOTH is the only check of agreement --
  neither covers the other, which is why the lanes run both (and why a Snowflake round is ~75 minutes, §7.6).
- **The proposal (not built; the user agreed to take it after PCT and core close):** make BOTH plan the DATABASE
  judge's real program (the fused segments) and, beside it, the host's sides, comparing per verdict at the segment's
  last verdict. Then one channel B pass covers the shipped shape and agreement, and the DATABASE-alone pass can go.
  Design questions: the host needs frame rows that the fused plan never materializes (a frame is a CTE there) -- the
  host's sides must read the frames as their own statements or the fused plan must surface them; and assertError's
  two-step shape (S135) must survive.

---

## 5. State as of 2026-11-06

### 5.1 The commits of 2026-11-05/06 (oldest first; all on the seam branch)

| Commit | What |
|---|---|
| `ec0eed01` | Runtime wall with zero exemptions: the compatibility appeal's rows and fixtures planned with the assert |
| `c185b047` | `RunScope` within TeaVM's class library (no `StackWalker`, no `newKeySet`) -- CI had failed every lane on `//wasm:planner` |
| `2bed81ec` | Both judges decide an assertError's inner assert. **Broken push**: it also carried four deletions from the then-uncommitted H2 work (`H2Modern`, `GridReportCtesInline`, two tests), so the branch did not build here; repaired by `53b52c80` |
| `53b52c80` | **H2 2.5.252** for lite; the referee legend-engine's 2.1.214 isolated (`EngineH2Referee`, `@h2_engine`); no plan names a server version (S141) |
| `5a911f06` | The H2 corpus closed out: every register through the judge differential and the unjudged ceiling (§7.2) |
| `ca23d78f` | ClickHouse's NOT brackets its whole operand (89 PCT failures) |
| `b59aeedd` | SQLite and BigQuery write a list once: the NULL guard binds it (statement doubling, §7.4) |
| `4a492594` | ClickHouse's test AST limit 1M -> 10M (the user's choice) -- **reverted** by `f2f47257` after the lane measured an OOM |
| `c1bf51fc` | A fused verdict statement within the database's budget (`SqlDialect.fusedStatementBudget`, `VerdictBatch.withinBudget`) |
| `28a27931` | GATES: the budget's checks |
| `78554443` | **Another session's** (`session_0195rQEmsRT7otsv9tTmNxj5`) CI fixes on 28a27931: `PostgresArmTest` compile (broken by `53b52c80`), the stress suites' walk door, three generated files |
| `7c93fbc5` | Cloud lanes: one connection view per held session, so channel B sends its session statements once per suite |

### 5.2 Round 22 (PCT, all five suites; channel A; channel B under BOTH and under the DATABASE judge alone)

| Lane | Commit | Result |
|---|---|---|
| DuckDB, H2 | local | green (channel B under both settings, channel A) |
| Postgres, SQLite, MongoDB (A) | `b59aeedd` | 35/35, census identical in both settings |
| Trino, MongoDB (A) | `b59aeedd` | 16/16 |
| Databricks | `b59aeedd` | 15/15, identical to round 21 |
| MariaDB, MySQL | `4a492594` | 20/20 |
| ClickHouse | `c1bf51fc` | 10/10 channel B (grammar 137/137 under the DATABASE judge too, no OOM) |
| **BigQuery** | `b59aeedd` | **pending** -- requested 2026-11-05 22:2x |
| **Snowflake** | `b59aeedd` | **pending** -- ~75 min, started 22:26 |

No lane printed "the runtime wall" or an unledgered failure. Census per lane (BOTH = DATABASE everywhere):
Postgres essential {332,13} grammar {132,5} relation {467,2} standard {205} unclassified {92,1}; SQLite {333,12}
{134,3} {458,11} {205} {92,1}; MariaDB {334,11} {137} {456,13} {205} {93}; MySQL {333,12} {137} {436,33} {205} {93};
ClickHouse {333,12} {137} {464,5} {202,3} {93}; Trino {336,9} {136,1} {469} {205} {93}; Databricks {335,10} {137}
{461,8} {205} {93}.

### 5.3 CI

CI runs on every push to PR #83 (`gates` and `diagnostics` workflows; each push cancels the previous `gates` run). The
last completed `gates` run with a result is on `28a27931`: five lane kinds failed on every OS, four fixed by
`78554443`; the fifth is the corpus lanes on `strictdate::testProject` (§6.1 step 3). `gates` on `7c93fbc5` was
pending when this was written. Check with `mcp__github__actions_list` (`list_workflow_runs`, branch filter) and
`list_workflow_jobs`; a job's log with `get_job_logs` (`tail_lines`).

### 5.4 What changed in the SQL and why (the last day)

- **The list written twice** (SQLite, BigQuery): a NULL-guarded list operation wrote
  `CASE WHEN xs IS NULL THEN NULL ELSE <body over xs> END`, so nested lists (the database judge's canon over a firm's
  employees) doubled per level: `exists::testExistsInSelect` 1,244,880 characters on BigQuery. Now bound once:
  SQLite `(SELECT CASE WHEN __l.__xs IS NULL ... FROM (SELECT <xs> AS __xs) AS __l)` (a correlated one-row derived
  table; SQLite 264,610 -> 22,954 chars), BigQuery `... FROM UNNEST([STRUCT(<xs> AS __xs)]) AS __l` (**never run on
  BigQuery** -- if BigQuery refuses it, that is the one fix left; a NULL literal is not bound because a bare NULL types
  INT64 there). MariaDB has the same doubling pattern (`MariaDb.java` `unlessNull`) but cannot correlate a derived
  table; it passes today.
- **The fused-statement budget**: ClickHouse 100K, SQLite and BigQuery 500K characters, every other database
  unbounded; a group past it is halved at planning until each part fits or is one assert. ClickHouse
  `testEqualNonPrimitive`: 28 statements, largest 135,011 (six single asserts over budget, which cannot be halved).

---

## 6. The roadmap

### 6.1 Phase 1 -- PCT and core at 100% (do these in order)

1. **Collect BigQuery's round 22** (`session_019tku7nRKBygjSBz9QQBGfM`). If BigQuery refuses the bound-list shape
   (`UNNEST([STRUCT(xs AS __xs)])` -- a correlated-subquery or struct error), fix it in `BigQuery.unlessNull`
   (alternatives: `(SELECT ... FROM UNNEST(xs) ...)` with a separate null test on a one-row CTE, or skip binding where
   xs is known non-null) and ask the lane again. Then ask it again at the head for the budget (`c1bf51fc`, 500K) and
   the session fix (`7c93fbc5`).
2. **Collect Snowflake's round 22** (`session_01WDoDkeNvgCsaPVzNx6wGDA`); then one round at the head to measure
   `7c93fbc5` (channel B under the DATABASE judge should drop from ~0.87 s/test to about half: one statement per test
   instead of two). Databricks too (`session_01757FUtRcj12NGhvSxL6A1b`).
3. **`strictdate::testProject`** (`meta::relational::tests::mapping::dates::strictdate::testProject`): red on both
   corpus lanes, so CI's corpus lanes are red. DuckDB's database pass: `DataError: Parser Error: window functions are
   not allowed in window definitions` (a nested window in the verdict SQL); H2's host pass: `DialectCapability: LIST_AT
   reached a dialect without a list encoding`. It regressed before 2026-11-05 (in no roster). Bisect between the last
   green corpus run and `ec0eed01` (`git log -- spec/src/test/resources/rcorpus/duckdb-fail-roster.txt` dates the last
   roster move), with `bazel run //spec:corpus_one -- --jvm_flag=-Dlegend.diagnostics=judge-declines,dump-sql duckdb
   database meta::relational::tests::mapping::dates::strictdate::testProject`. Fix the cause (do not roster it without
   the user's word).
4. **CI green on every OS** at the head. Read every failing job's log; fix forward; never re-run to "kick" CI.
5. **The channel B judge passes (§4)**: post the proposal to the user with the measured numbers; build it if agreed
   (it can wait until after the corpus -- the user's call).
6. **The deep audit's open items** (GATES "The deep audit on PR #83"): `'TDSNull'` sentinel (deferred to the shared
   assert design), compatibility float tokens inside a String on the host judge only (needs a register row -- not yet
   written), S137's relation not evaluated in compatibility mode (corpus phase: run the relation as its own statement;
   `size()` is not enough, DuckDB prunes unused computed columns).
7. **Records**: a GATES entry for round 22 with every lane's census; `docs/TEST_IS_THE_PRODUCT_PLAN_2026_10_31.md`
   §10 status; `docs/IN_FLIGHT.md`; refresh PR #83's description (it predates H2 2.5, the walls, the budget).
8. **An independent final audit** (plan step Z): two fresh agents, read-only, on the head -- both judges real, no
   fallback, every assert a real function, no planning at run time (incl. the driver), the exec wall -- given the code,
   not the conclusions. Fold its findings; then tell the user PCT and core are at 100% and ask about landing.

Open rulings to put to the user (none blocks a test): **S17** (`splitPart`: upstream's doc says a literal separator,
its interpreter a character set; lite's DuckDB splits on the whole string, H2 on characters), **S121** items (2) an
anonymous instance's text (`@_` and lite's id vs compiled's constant) and (4) LEAN's nested dates, **S13** (external
formats, "to be ruled at C3"), **S125** (Arrow typing). Ask once, briefly.

### 6.2 Phase 2 -- rcorpus (the relational corpus)

The goal: every corpus body on the program road under both judges, no walk. Today the corpus lanes open the walk door
(`all`). Known items, measured 2026-11-05 (GATES "One H2 for lite" and "The H2 corpus closed out"):

- `strictdate::testProject` (Phase 1 step 3).
- H2 database pass: **46 asserts the database judge declines** (the unjudged ceiling `h2-judge-unjudged-ceiling.txt`
  raised 5 -> 46 with the reason): 41 `null-canon-cell`, 5 JSON not byte-equal -- all in tests already on the
  database-lost register; on 2.1.214 they failed at planning (no JSON), now the judge sees them. DuckDB's ceiling is 0.
- 16 database-lost tests where the database judge's canon reads `''` for a JSON-navigated value the host reads as
  `'STOCK'` (H2's JSON navigation in the verdict SQL: `H2.variantAwareCast`, `TRIM(BOTH '"' FROM CAST(x AS VARCHAR))`).
- Two verdict statements H2 2.5 refuses on a JSON parameter (`Invalid value "BIGINT"/"INTEGER" for parameter "JSON | ROW"`:
  `dropAndCreateTable`, `testExecuteInDbToTDS`).
- 31 host-compared rows (H2): tests failing on both lanes whose failure is a planning wall (e.g.
  `testConnectionEqualityTypeDiff`: "scalar match: the arm collection has a non-literal prefix ... no runtime arm
  list"; the graph-fetch plan tests: `instanceOf` over an `ExecutionNode` row). Shrink-only, must reach zero.
- `alloy::testAlloyTestDatGenWithQuotedColumnsForViews`: the host stops at a frame (`class query under if() with a
  runtime condition is not resolvable yet`) before the quantified guard the database judge passes; a differential
  row on H2; DuckDB has the same pair, unseen because its database pass stops at strictdate.
- `selfJoin::testSelfJoinPropertyMapping`: newly passes on the H2 host pass, lost on the database pass (the canon reads
  a struct field H2 carries as JSON and keeps its quotes).
- The 19 outside-body rows (`fallback=1`: compatibility mode's 2-ULP comparison run as a statement after the exact
  one, S132) -- plan the comparison as a column of the verdict row instead of a second statement.
- S137 in compatibility mode: evaluate the relation (§6.1 step 6).
- The stress suites walk (444 graph-fetch, 17 temporal service queries as a body's last statement).

### 6.3 Phase 3 -- delete the walk

`BodyDriver`'s statement-by-statement path, `WalkDoor`, the router's walk branch, `BodyRouterParityTest`'s comparison,
the classifier (`testplan/BodyClass`, `BodyKinds` census). Precondition: no lane opens the door (core `none`, PCT
`none`, corpus and stress `all` today). `AGENTS.md` describes the router and the body driver; update it when they go.

### 6.4 Phase 4 -- the special-casing audit

Two independent agents look for code that recognises a test, a shape or a lane instead of a rule: shape recognisers
the assert nodes made redundant (`ASSERT_NODES_PLAN` step 8), per-family judge logic, lane-specific branches, ledger
rows that hide a bug.

### 6.5 Phase 5 -- lean SQL (measured, to start from)

- **The instance canon**: each instance canonicalised at its construction site as a one-element list mapped and read
  back at `[0]` (`ConstructionCanon.bind`: deliberate, a `let` that keeps text linear; on SQLite each costs a
  correlated subquery, a `json_group_array`, a `json_extract` and an index `CASE`). `testEqualNonPrimitive`: ~1,000
  maps, 608K on SQLite, 1.3M on ClickHouse as one statement; still 2.1M characters over 28 statements after the
  budget.
- **BOTH's 8 statements per test** (§4).
- `LIST_INIT`/`LIST_SLICE` on SQLite still write the list in their WHERE (`json_array_length(xs)`); MariaDB's
  `unlessNull` doubles like SQLite's did.
- Statement volume per test on cloud lanes (§7.6). The user: no parallel lanes until the SQL is examined.
- Core's SQL size: `docs/CORE_SQL_SIZE_2026_11_04.md` (85% is the system metamodel's seed re-sent per test).

---

## 7. Homework and learnings (with the numbers)

### 7.1 H2 versions (measured 2026-11-05, before choosing)

- legend-engine pins `h2.version` 2.1.214 in its root POM, PATCHED: it replaces `org.h2.value.TypeInfo` and
  `org.h2.engine.Mode` and registers Java functions in the database (`legend_h2_extension_json_navigate`,
  `_json_parse`, `_reverse_string`, `_split_part`, ...). That is how the engine passes the variant tests on 2.1.214.
- Lite on 2.4.240: 13 core floor tests pass (JSON, zips, structs), 87 fail differently, 11 write plans refused by the
  old version check; the H2 corpus lost six (decimal division at a higher scale; `acos`/`asin` raising outside
  [-1, 1] where 2.1.214 answers NaN).
- Chosen: lite on **2.5.252** (one dialect `H2`; `H2Modern` and `GridReportCtesInline` deleted -- 2.5 fixed the
  lost-CTE-parameter defect; ACOS/ASIN guarded AND clamped, because H2 folds a constant inside an untaken CASE branch;
  `JSON_TYPE` emulated from `IS JSON` predicates; `LIST_LENGTH` is `CARDINALITY`). With lite's 2.5 as the corpus
  referee, DuckDB's corpus changed (acos/asin, four calendar witnesses) -> the referee is the engine's 2.1.214 in an
  isolated class loader (`EngineH2Referee`: `org.h2.` read child-first from the jar handed by path
  `-Dlegend.referee.h2[.rlocation]`; `EngineH2VersionPinTest` reads the engine POM).
- No plan names a server version any more (`ExecutionPlan.Servers`, `SqlDialect.forServer/serverVersions`, the JDBC
  and MongoDB version checks, `Sessions.version()`, PCT's `serverVersion()` all deleted).

### 7.2 The corpus registers: how not to be fooled

The H2 corpus's database-pass registers had drifted **before** the upgrade, unseen: CI on `c185b047` (still 2.1.214)
stopped at the outside-body register (NEW 19, STALE 5) and never reached host-compared; the DuckDB database pass
stops at the lost register (strictdate) and has never reached its differential check. **The first register that
differs ends the check.** When re-baselining, run until every register passes, and compare against CI's run on a
commit before your change (`get_job_logs` on the corpus job) before attributing a move to your change. Every row
added carries a dated reason; the 2026-11-05 rows say why (not the upgrade, with evidence).

### 7.3 The runtime wall (how it was made strict)

Report mode first (`-Dlegend.runwall=report`) over core's matrix and PCT: four mechanisms planned while a plan ran,
all in the judge layer (a caller's message row, the split rung's per-member statements, the ruled fallbacks, the
engine-text appeal). Each is now planned with the fused statements (`VerdictBatch.plan`, `Ahead`) or with the
assert (`SqlTextVerdicts.RowsAhead`, `AssertListener.fixtureAhead` -> `Execution.planProgram`). Providing the
appeal's fixtures before the program broke `projection::testGroupByWithWindowSubset` (its own setup drops and
recreates tables; with the fixture's aside on the search path a drop resolved into the aside); so fixtures are
planned ahead and run at the appeal. The refusal has a type (`RunScope.Refused`) so a catch that turns a failure into
a decline rethrows it. TeaVM (the WebAssembly planner, which every CI lane builds) has no `StackWalker` or
`ConcurrentHashMap.newKeySet`: base must stay within TeaVM's class library.

### 7.4 Doubling: a NULL guard that writes its operand twice

`CASE WHEN x IS NULL THEN NULL ELSE f(x) END` writes x twice; nested, it is exponential. Bind once (a one-row derived
table on SQLite, a one-row UNNEST of a struct on BigQuery; DuckDB and ClickHouse use native `list_transform` /
`arrayMap`, which write once). Look for the same pattern in scalar guards (`SQLite.java` around the
`IS NULL THEN NULL ELSE coalesce(` sites) when lean SQL starts.

### 7.5 Raising a database limit is not a fix

ClickHouse at `max_ast_elements` 10M accepted the 1.3M-character statement and its analyzer took 10-12 GB building the
query tree (`QueryTreeBuilder::buildExpression` materialising string constants) until the kernel killed the server;
the client said only "Query request failed (attempt: 4)". At 1M the refusal (`TOO_BIG_AST`) is immediate and clean.
Ask the lane for `dmesg` and a manual `curl --data-binary @stmt.sql` against the server with the harness's URL
settings to get the server's own error. The limit is back at 1M (`EmbeddedClickHouse.jdbcUrl`).

### 7.6 Why Snowflake takes ~75 minutes

It is the matrix: 15 runs (channel A, B under BOTH, B under DATABASE, five suites each), serial. Snowflake costs
~0.3-0.5 s per statement however small (cloud compile + round trip). Channel B under the DATABASE judge sends 2
statements per test (the body, and `SET TimeZone` re-sent per test because `CloudBackend.open()` answered a new
proxy per call so the platform made a new session per test -- fixed in `7c93fbc5`); under BOTH 8 per test. Round 21
on Snowflake: channel A ~12 min, B BOTH ~41 min, B DATABASE ~21 min; channel B 30-45% slower than rounds 18-19. The
user: fix the per-test session statement (done), no parallelism yet, the SQL after PCT and core.

### 7.7 Statement counts and sizes: how to measure

- Per test: `-Dlegend.diagnostics=sql-sizes` prints `[sql-size] <chars> <test> <origin>` per statement (origins: body,
  session, fallback, other, let, seed, system, referee_ours, referee_golden, mirror_seed, ...).
- The SQL itself: `-Dlegend.diagnostics=dump-sql` prints `[sql:<origin>] <text>` (multi-line; split the test.log on
  `\n(?=\[sql:)`).
- One PCT test: `-Dlegend.diagnostics=chb-only=<substring>` (the suite's discovery guard then fails the target --
  expected; read the test's own line).
- PCT channel B judge: `--jvmopt=-Dpct.channelb.judge=HOST|DATABASE|BOTH` (default BOTH). `-Dlegend.judge.mode` is the
  CORPUS's flag and does nothing in PCT (a lost hour, 2026-11-05).
- The switches (`Diagnostics.java`): dump-sql, pct-cases, pct-shapes, corpus-trace, detach-trace, progress, timing,
  corpus-containing, chb-only, primitives, plan-sizes, forbid-walk, sql-sizes, judge-declines.

### 7.8 Process lessons (each cost the user time)

- **Commit only the files you mean.** `2bed81ec` was committed with `git add -A` while other work was uncommitted;
  the push carried half of it and broke the branch on every OS. `git status` and `git diff --stat --cached` before
  every commit.
- **The local gate does not build everything.** `//core:postgres_arm_test` stopped compiling and only CI saw it.
  After an API change, `bazel build` the test targets that use it (grep the callers).
- **Another session pushes to this branch** (`session_0195rQEmsRT7otsv9tTmNxj5` fixed CI on 2026-11-05). Fetch
  before pushing; rebase onto it; read what it changed.
- **Never act on your own recommendation as if the user had chosen it**, and never move or park work without asking.
- **Answer "what is going on" in a few plain lines** -- the user's frustration on 2026-11-05 came from long reports
  that did not say whether anything had broken.
- The worker process can restart mid-turn: run long checks with `run_in_background` and write logs to the
  scratchpad; re-read state (`git status`, the log tail) after a restart.
- A Stop hook asks to commit and push uncommitted work. Commit in place on the seam branch with an honest message
  (what is verified, what is still running); never to a side branch.
- Disk is a fixed allowance: delete big build outputs you no longer need (`bazel clean` is expensive; prefer removing
  `bazel-bin` of other packages or old sandboxes); never `bazel query //...` (it fetches every platform).

---

## 8. How to run every check

```
# core: the matrix (every package under HOST, DATABASE, BOTH) and the guards
bazel test //core:core_tests //core:core_tests_judges //core:guardrails //core:census --keep_going --test_output=errors
# the SQL dialect tests alone
bazel test //core:core_tests_sql --test_filter=com.legend.sql.dialect.ClickHouseDialectTest --test_output=errors
# PCT on the in-process lanes (DuckDB is //pct:pct_channel_b and //pct:pct_<suite>; H2 and SQLite have their own)
bazel test //pct:pct_channel_b //pct:pct_channel_b_h2 //pct:pct_channel_b_sqlite --keep_going --test_output=errors
bazel test //pct:pct_channel_b_sqlite --jvmopt=-Dpct.channelb.judge=DATABASE --keep_going --test_output=errors
# the corpus (both passes, every register; ~3-10 min each locally)
bazel test //gates:corpus_duckdb --test_output=errors
bazel test //gates:corpus_h2 --test_output=errors
# one corpus test, one pass, with diagnostics
bazel run //spec:corpus_one -- --jvm_flag=-Dlegend.diagnostics=judge-declines h2 database <test fqn>
# the local gate before asking the user to land anything
bazel test --lockfile_mode=error //gates:local
```

The corpus verdict target `//spec:corpus_<lane>_verdict` reports both passes; the passes' logs are
`bazel-bin/spec/judge_{host,database}_{duckdb,h2}/{host,database}.log` and their per-assert ledgers
`judge-{host,database}.tsv`. Postgres targets refuse root: `--run_under="tools/postgres/as_unprivileged.sh ubuntu"`
(`docs/TEST_IS_THE_PRODUCT_PLAN_2026_10_31.md` §8.3). Foreground Bash calls stop at 10 minutes: run longer checks in
the background and poll the log.

---

## 9. The warm lane sessions: PCT on every database in parallel

### 9.1 What they are

Six Claude Code cloud sessions of this account, created 2026-10-19/21 by the MongoDB-backend session
(`session_01Vz1L3X6p56AZdTCQXUXYfE`) with `create_session`, all in environment **`env_0175UqCzjEvLwcgBVyfEzE5S`**
(its setup script writes the cloud credentials file that `LEGENDLITE_PCT_CLOUD_PROPERTIES` names --
`docs/CLOUD_BACKENDS_PCT_2026_10_11.md` §4 and §6; nobody pastes a key into a session). Each has the repository, a
warm Bazel, and its databases (embedded servers in the container, or cloud credentials), so a round costs only the
tests' time. They **measure only**: no edits, commits, pushes, PR comments or credential reads. (`RESULT_BOUNDARY_START_HERE.md`
§7 calls these sessions "GONE" -- that table is stale; they answered every round through 2026-11-05.)

| Session | Lanes | Round 22 | Logs (in its container) |
|---|---|---|---|
| `session_01UmbETZRDU9ugZ8X2d1hGyb` | Postgres (unprivileged), SQLite, MongoDB channel A | 35/35 at `b59aeedd` | its scratchpad `r22/`; disk 462 MB free after the round |
| `session_01Hztqdk3qySkTkH2uUDuUdL` | Trino, MongoDB channel A | 16/16 at `b59aeedd` | `/tmp/claude-0/-home-user-legend-lite/92cb3ba2-c034-51e2-8e89-63497dbca8eb/scratchpad/r22/` |
| `session_018ATdMqvopmrLibQhwN4GQ4` | MariaDB, MySQL (as root), ClickHouse | 20/20 at `4a492594`; ClickHouse 10/10 at `c1bf51fc` | `.../d23be100-fdf0-507f-ab4f-c92adac6a65c/scratchpad/` (`r22d/stmt.sql` the 1.3M statement) |
| `session_01757FUtRcj12NGhvSxL6A1b` | Databricks | 15/15 at `b59aeedd` (~31 min: A 474 s, B BOTH 846 s, B DB 517 s) | `.../342a829e-abb4-5604-9090-3b1c242eee23/scratchpad/r22/` |
| `session_019tku7nRKBygjSBz9QQBGfM` | BigQuery | **ran, unreported** (§9.2); round 21 ~95 min | `.../278be9cb-0b5f-59d0-893f-028688338a67/scratchpad/r21b/` (`A-*`, `BOTH-*`, `DATABASE-*`, `size-*` logs) |
| `session_01WDoDkeNvgCsaPVzNx6wGDA` | Snowflake | **ran, unreported** (§9.2); ~75 min a round | `.../b7b8abab-4c01-593e-9a2a-1accbcfe230a/scratchpad/r22/` (`summary.txt`, `run.sh`) |

### 9.2 Their state on 2026-11-06: blocked until 2026-11-08 03:00 UTC

Every lane session's last turn reports the account's **weekly usage limit** ("You've hit your weekly limit · resets
Nov 8, 3am (UTC)"; `get_session` -> `external_metadata.rate_limit_info`, `post_turn_summary.status_detail`). BigQuery
and Snowflake hit it while their round 22 ran in the background, so they never sent the result. **When the limit
resets, ask each to report from the logs it already has** (the paths above) before re-running anything. A new session
from the same account is under the same limit; this session is too.

### 9.3 Running a round in parallel

1. Push the candidate commit to the seam branch.
2. Send every lane its message **at once** (one `mcp__claude-code-remote__send_message` each); they run concurrently,
   each in its own container. Ask only the lanes the change can touch when that is clear; the full set for a
   checkpoint.
3. Each acknowledges, runs in the background, and replies with `send_message`; the replies arrive here as
   notifications (`ReadNotifications`). Do not poll.
4. Record every lane's census in GATES (the census lines of §5.2 are the format).

The message that works (round 22's):
> Round N, measure only (no edits, commits or pushes). Candidate: `<sha>` on claude/wizardly-dirac-rw6tim-seam (fetch
> that branch and check the commit out detached). Changed since round N-1 (`<sha>`): `<one line per commit>`. Same runs
> as round N-1 for your lanes: channel A all five suites; channel B all five under BOTH (default) and under the DATABASE
> judge alone (-Dpct.channelb.judge=DATABASE). Report per run PASS/FAIL and the census line, verbatim any "does not
> match its ledger" block, and whether any line contains "the runtime wall".

For a diagnosis add: `-Dlegend.diagnostics=chb-only=<test>,dump-sql,sql-sizes` on one suite, `dmesg | grep -i
kill` for a server that died, and the server's own error by sending the dumped statement by hand (ClickHouse:
`curl --data-binary @stmt.sql 'http://127.0.0.1:<port>/?user=default&max_query_size=16777216&...'`).

How a lane runs the targets (from their own command lines): `export PATH=/root/.local/bin:$PATH;
tools/cloud-distdir.sh //pct:pct_<lane> //pct:pct_channel_b_<lane>`, then `bazel test //pct:pct_<lane>
--keep_going --test_timeout=3600 --cache_test_results=no` (channel A's five suites), the same for
`//pct:pct_channel_b_<lane>` (channel B, BOTH), and the DATABASE judge with
`--test_env=JAVA_TOOL_OPTIONS="$JAVA_TOOL_OPTIONS -Dpct.channelb.judge=DATABASE"` (the cloud lanes keep their proxy's
trust store in `JAVA_TOOL_OPTIONS`, so they append rather than use `--jvmopt`; the effect is the same). They copy each
suite's `bazel-testlogs/pct/<target>/test.log` into `scratchpad/rNN/` per round. Lane names in targets: `postgres`,
`sqlite`, `mongodb` (A only), `mariadb`, `mysql`, `clickhouse`, `trino`, `databricks`, `snowflake`, `bigquery`, `h2`
(channel B relation only).

### 9.4 Recreating a lane

If a lane session is gone, archived or poisoned: `mcp__claude-code-remote__create_session` with `environment_id`
`env_0175UqCzjEvLwcgBVyfEzE5S`, `source_url` `https://github.com/neema2/legend-lite`, `source_revision`
`claude/wizardly-dirac-rw6tim-seam`, a title naming its lanes, and a prompt: the measure-only rules, "bootstrap Bazel
per docs/CLOUD_SESSIONS.md (tools/cloud-distdir.sh), measure <the head> once for <lanes>, keep each round's logs in
your scratchpad", then the round message. The embedded servers fit two or three per container (the split above);
Postgres must run unprivileged (`tools/postgres/as_unprivileged.sh`). From another account none of this exists: follow
`RESULT_BOUNDARY_START_HERE.md` §7's three steps (an environment per credential set, one session per lane group, a
baseline round first).

---

## 10. Named, not fixed (carry these)

- `strictdate::testProject` (both corpus lanes; CI red).
- BigQuery's bound-list shape unverified.
- The cloud session fix (`7c93fbc5`) unmeasured.
- A register row for compatibility mode's float tokens inside a String within 2 ULP on the host judge only.
- `'TDSNull'` sentinel equal to a null cell (the shared assert design).
- S137's relation not evaluated in compatibility mode.
- MariaDB's `unlessNull` doubling; SQLite's `LIST_INIT`/`LIST_SLICE` re-reading the list.
- The instance canon's verbosity (lean SQL).
- BOTH's per-assert program vs DATABASE's fused program (§4).
- `RunScope`'s `[run-wall]` report mode is diagnostic only; nothing may turn the wall off.
- The pending rulings S13, S17, S121 (2)(4), S125.

---

## 11. The first hour of a new session

1. `git fetch origin claude/wizardly-dirac-rw6tim-seam && git status && git log --oneline -15` -- check for commits from
   other sessions; rebase if needed.
2. Read this file, then the GATES entries from "The deep audit on PR #83" to the end.
3. `ReadNotifications`, then the lanes' state (`get_session` on each, §9.1): before 2026-11-08 03:00 UTC they are blocked
   (§9.2); after it, ask BigQuery and Snowflake to report round 22 from their existing logs.
4. Check CI on the head (`mcp__github__actions_list`, `list_workflow_runs` filtered to the branch; failed jobs'
   logs).
5. Bootstrap Bazel per `docs/CLOUD_SESSIONS.md`, then the core matrix in the background (§8) to confirm the tree.
6. Start §6.1 at the first open step; tell the user in three lines what you found and what you are doing.

---

## 12. The research, indexed (read the one you need)

| Document | What it holds |
|---|---|
| `docs/TEST_IS_THE_PRODUCT_PLAN_2026_10_31.md` | the program: rulings R1-R24, the ledger of differences, the two judges, the movement register, guards, design briefs B1-B14, steps M0-M10/Z, open items, the day-by-day status |
| `docs/TEST_IS_THE_PRODUCT_HANDOFF_2026_11_01.md` | M6-era handoff: measurement scripts, baselines, traps (§8), what lived only in that session |
| `docs/ASSERT_NODES_PLAN_2026_11_04.md` | asserts as plan nodes: the user's design, upstream's assert model, the node, the order, the rulings of 2026-11-05, progress |
| `docs/ASSERT_WALL_LEANSQL_AUDIT_2026_11_04.md` | the audit that set the bar: the assert family, the plan/exec wall at run time, lean SQL measured |
| `docs/ORDER_TRACKING_DESIGN_2026_11_05.md` | order tracking (not ratified; its §9 asks three questions) |
| `docs/CORE_SQL_SIZE_2026_11_04.md` | core's SQL size: 85% the system metamodel's seed re-sent per test; the H2 OOM |
| `docs/test-is-product-homework/` | A code inventory, B plan audit, C M0-M5 measurements (C_M0, C_M2, C_M3*, C_M4A, C_M5A), D M3 brief, E MR inventory, M6-M10 briefs and audits, M8C design question (the canon), MIXED_SYSTEM_USER_BODIES_DESIGN, T8 generator design, REVALIDATION_2026_11_01 |
| `docs/planner-driver-seam-homework/` | the seam program's code measurements, step H's seven measurements, upstream's plans/executors/lets read |
| `docs/result-boundary-homework/` | the result boundary program's research; `DECISIONS_LOG.md` (every decision, D1-D108+); the 51 writer differences by upstream's answer; track T's expectations |
| `docs/PLANNER_DRIVER_SEAM_PROGRAM_2026_10_30.md`, `docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md`, `docs/PLAN_AND_RESULT_MODEL_2026_10_18.md` | the predecessor programs this one builds on (the plan record, the result model, the seam) |
| `docs/JUDGING_TWO_MODES_2026_09_17.md`, `docs/SQLTEXT_ROW_VERDICT_CHARTER.md`, `docs/OBJECT_IDENTITY_2026_10_20.md` | the two judges' original design; the engine-text verdict; object identity (`__id`) |
| `docs/TENET_CHARTER.md`, `docs/WORLD_MAP.md`, `docs/TENETS.md` | the execution tenet's clauses; Java vs Pure vs input; eager knowledge, lazy work |
| `docs/CLOUD_BACKENDS_PCT_2026_10_11.md`, `docs/CLOUD_SESSIONS.md` | the cloud lanes' credentials and setup; Bazel in a cloud session |
| `docs/GATES.md` (2026-11-05 entries) | every check of the last week, with numbers; the corpus registers' reasons |
| `docs/SEMANTICS_REGISTER.md` S120-S141 | the week's rulings as register rows (S127 numbers, S131-S132 leniencies, S134 indexOf, S135 assertError, S137 relations, S138 guards, S139 Decimal, S140 precision guards, S141 H2) |

---

## 13. What lived only in the old session's container (now preserved)

- **Three local worktrees with uncommitted work**, copied 2026-11-06 to backup branches without touching them (a commit
  through a temporary index); unreviewed, most likely superseded by the seam branch -- consult only if something seems
  lost:
  - `claude/backup-m6-worktree-2026-11-06` (`5086d20b`, base `c9847ebe`): M6-era edits to `BodyClass`, `BodyRouter`,
    `BodyRouterParityTest`, `PlanRunner`, `TestKinds`, the plan (2026-11-01);
  - `claude/backup-branch-worktree-2026-11-06` (`5a21d9da`, base `9b5ea3c3`): edits to `BodyRouter`, `DatabaseJudge`,
    `HostJudge`, `AssertVerdicts`, `VerdictSql`, GATES and the assert plan (2026-11-05, ~01:40);
  - `claude/backup-pct-fixes-wip-2026-11-06` (`9b4fda4f`, base `c9a59fe5`) and `claude/backup-pct-fixes-wip-commit-2026-11-06`
    (`c9a59fe5`, "wip: step 0's code (base for the fixes; not pushed)"): 48 files, among them `PctBackend`,
    `PctPlanRoute`, a new `EngineExecuteCardinalityTest` and a channel B `ledger.tsv` (2026-11-03).
- `claude/wizardly-dirac-rw6tim-h2` (`9cc79f34`): the H2 upgrade parked by mistake on 2026-11-05 and restored into
  `53b52c80`; redundant -- the user may delete it.
- The old session's scratchpad (the H2 corpus logs, the SQLite dumps, one-off scripts) and its transcript die with the
  container; everything that mattered is in GATES, the register and this file.

---

## 14. The open work list (the old session's task list, carried)

In progress or pending, in the order they serve §6 (D = deferred by the user):

1. Collect BigQuery's and Snowflake's round 22 after the limit; measure `c1bf51fc` (BigQuery's budget) and `7c93fbc5`
   (the cloud session fix) on the cloud lanes.
2. `strictdate::testProject`; CI green on every OS.
3. Records (GATES round 22, plan §10, IN_FLIGHT, PR #83's description); the register row for compatibility mode's
   float tokens inside a String on the host judge only.
4. The final independent audit (plan step Z), then hand the landing to the user.
5. BOTH plans the database judge's fused program (§4), then drop the DATABASE-alone pass.
6. Asserts as plan nodes, step 8: delete the shape recognisers the nodes made redundant; "fix every assert" for PCT
   and core from the bar (`AssertBarTest`'s shrink-only gap list).
7. The leniencies strict where lite's own tests run: sort strict, unordered strict (with order tracking, D), the
   `'TDSNull'` sentinel (D, the shared assert design), forAll.
8. Assert messages from upstream for every family; message arguments; the asserts PCT and core do not exercise.
9. "Planning sends nothing" closed mechanically (the count query, the seed as a program step).
10. The corpus: every body on the program road under both judges (§6.2); the corpus reconciliation (witness texts, the
    H2 registers, strictdate).
11. Delete the walk, the router's walk branch and the classifier (§6.3); then the type census becomes a planning check.
12. The special-casing audit (§6.4).
13. Lean SQL (§6.5), including the system metamodel's seed re-sent per test.
14. D: IEEE floats beyond S20 and the JSON serializer fix (after the multiset restore); order tracking's report; S137's
    relation evaluation in compatibility mode (corpus phase).

