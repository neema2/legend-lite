# A test body is the product plan -- the plan (v2, 2026-10-31)

Status: **v2, written after the independent audit (`docs/test-is-product-homework/B_PLAN_AUDIT.md`). The user ruled
§1's decisions; §9 lists what is still open. No step of §7 starts until the user agrees v2.** It replaces the
remainder of `docs/PLANNER_DRIVER_SEAM_PROGRAM_2026_10_30.md` (S4b to S5); that program's S1 to S3 and S4a stand, and
its §7 rules stand unchanged (§8.2 restates them). Evidence: `docs/test-is-product-homework/` -- A, the code inventory
(roads R1-R20, differences D1-D49, every claim with file and line); B, the audit (findings W, C, G, O, K); `probes/`,
the parked measurement probe as text.

## 0. In plain words

A test body is a Pure program: lets, statements, writes, effects and asserts. The product already plans a Pure
program (`PlanMaker.program`: a `Sequence` of steps, lets spliced into their readers, a staged let a create-temp-table
step, a write an update step), and `PlanRunner` runs it. The test road shares almost none of that: it never calls
`TypedQuery.lowerer` or `PlanMaker.plan`/`program`, it plans over twenty roads of its own with 49 differences from the
product (inventory §1-§2), and the product road cannot plan a test body today (a read-only body with a non-let
statement before its last is refused, `UserCallInliner.java:204-207`; `program` is taken only for a body that writes;
`PlanMaker` never emits an effect or a verdict step).

After this plan: **the product planner plans a test body into one `ExecutionPlan`, `PlanRunner.program` runs it, and
the judges and the test listener are the sink.** Every statement, let, write, effect and assert side is planned by the
product's code; what is test-only is the short named list of §2.C. The database judge is 100% a SQL plan; the host
judge is the product's sides, then Java compares the rows. Realistic size: **9 to 12 weeks of sessions**, in eleven
steps with checkpoints (§7).

## 1. The user's rulings (2026-10-31, verbatim where quoted)

| # | Ruling |
|---|---|
| R1 | A test body is the product plan (the user agreed to "merge into product path"). |
| R2 | The database judge: "exactly the same as product plan ... literally zero difference", except what is named test-only here. |
| R3 | "The host judge could be exception to java rule only not exception to plan+exec split." |
| R4 | The fused verdict statement is kept ("we definitely still want the assert fuze"). |
| R5 | The database judge is self-contained: "Yes exactly this all in database, agree that the replay is separate thing." |
| R6 | T13: the H2 referee replay stays the verdict source of the engine-text asserts where our text differs from the golden (agreed recommendation). |
| R7 | "Agree with all recommendations": decisions 1 (the eager run at a let kept, a plan step), 2 (a verdict group closes before a database-changing step; refined in §9 N6), 5 (one `check` value), 6 (a literal root a constant, a product feature), 8 (the corpus row-order emulation a run option), 9 (the two "AssertVerdicts.side" texts corrected), 11 (step by step, outcomes frozen but for the approved movement register, §4), 12 (statement changes approved by class), 13 (the parity guard byte-identical, §5), 14 (consecutive effects one script, a product feature). |
| R8 | Decision 4: "will keep as exception": the order key the planner adds to a grid under assertion that the test did not sort stays, a named test-only difference (it decides which rows a capped unsorted read returns, audit W7). |
| R9 | Decision 7: "fine to keep ours but yes needs to be same code path": the graph's separate system database stays (the user's rulings of 2026-09-02 and 2026-10-11, `SystemDatabase.java:20-56`), and a product query and a test reach it by one code path (§6 B7). |
| R10 | Decision 10: the canon computed over the product plan's columns, fully ("why would we not fix the cannon fully"); object ids: one counter per PROGRAM, for the product and tests alike (§6 B9), agreed after the explanation of its meaning. |
| R11 | "No need to push to CI until we have a good checkpoint": commits stay local between agreed checkpoints (with a backup, §9 N5). |
| R13 | §9's N1, N2, N4, N5, N6 as recommended ("Agree to all except the assertError"): the product's query door refuses the test-only steps (B6); the test-only planning in its own library (B4); the eager run at a let on both passes (MV7); backups between checkpoints on the work branch `claude/wizardly-dirac-rw6tim-seam-wip` (no CI runs on it); a verdict group closes before every non-verdict step (2.D). N3: option A, `assertError`'s comparison in Java ("Okay that's fine"). |
| R14 | The body is COMPILED WHOLE, then run ("Sounds good, I like it"). Two kinds of planning error, kept apart: an INVALID or UNSUPPORTED program (a type or model error, a construct lite refuses by name) is a compile error and the whole test fails, nothing recovers ("like a compiler error in another language that should just fail the whole function"); a VALID program whose evaluation raises (an error Pure raises at run time that lite detects early because its input is constant, e.g. `timeBucket`'s unit check) is planned as a RAISE at its position (`PureSql.raise`, or a raising step), so an untaken branch never raises and `assertError` catches it like any run-time error. ONE plan per body: what must run before a later statement can be written is a Keyed step inside it (the dynamic pivot's pattern, S110, S1a), never a plan in parts. |
| R15 | 2026-10-31, the way of working: "Agree to go back to back but you need to add an audit at the end of each step before going to next step, that way we have confidence to not stop for me to look at brief or have to recheck your work manually". Each step's brief is written into this plan and the step proceeds without waiting; every step ends with an independent audit (a fresh reviewer given the code, the brief and the rulings, not the author's conclusions), its findings folded, before the next step starts. The stops that remain: an output spelling change (R12, case by case), a movement outside an approved class (§4), a brief that must depart from these rulings or a design choice no ruling covers, a red CI or lane result not attributable to infrastructure. |
| R12 | Unchanged from the predecessor program (§8.2): never push to main, never open or merge a PR without the user's word; no output spelling change without the user's word, case by case; an answer to a question is not a go to build something else; a guard pin or allowlist moves only with a dated justification; never read, print or copy credentials; records land with the code. |

## 2. The ledger

### 2.A Differences that go: the test road takes the product's way

| D | Today | After | Moves (§4) |
|---|---|---|---|
| D1, D20 (host), D2 | host identity asks, frame CTEs and value statements on the identity layout (`__canon`) and per-node ids | the product's object layout; ids by one counter per program (B9) | MV5 (the counter's scope, M1: both roads one per program); the layout, the `i` spelling and the per-node ids converge with 2.A at M5 (MV6) |
| D3 | the value lane strips synthetic fields in SQL (`withDeclaredEgress`) | the writer strips them, as the product does | text |
| D5 | the exists-join form always on (no stated reason) | the product's rule: on unless a temporal class is read | MV6 (semantics of milestoned reads, measured) |
| D7, D28 (values) | a graph value is one JSON-array cell, split in Java | the product's streaming root, one object per row | MV6 |
| D8 | three render entries | `renderStatement` | MV6 (H2 type holes) |
| D18 | no cross-store check on an effect let | checked on every resolve | MV6 (a new refusal) |
| D21, dead code | stale "wire reconciliation" comments; `BodyDriver.evalValue` (no caller) | deleted | none |
| D27 | a value side planned as a RELATION, its timestamps trimmed in Java | the product's VALUE shape, once M0 shows them equivalent | MV6 |
| D36 | the judges' own target (no `oneDefinition`, `forServer` always) | `PlanMaker.target` | MV6 |
| D40, D41 | intermediate values decoded into Java; a write's count returned in Java | a `Check` step; the product's count statement | text, count |
| D46 | an effect or write run while a database side is being planned | a step of the plan, in order | order |

### 2.B Language features: they move INTO the product planner (the rows marked "test library" are planned by B4's library on the product's statement entry and refused on the product door, B6: the product planner, which ships in the WebAssembly and native planners, gains only what a product query can use)

| D/R | Feature |
|---|---|
| gate | any body, read-only or writing, planned by the program road |
| D4, D6, D9, D10, D13, D14, D16 | a from()/execute context's time zone, feature flags, post-processors and driver-PK/import-data-flow appends; the inliner's consumed lets; the resolver's let bindings and plan rows; `planToString`/`toSQLString` as plan-time strings |
| D15 | `execute(...)` inside a program: its chain spliced into its readers (design B10) |
| D17, R18 | DDL text generators and orchestration handles answered with no statement; `print`'s argument never evaluated |
| D26 | a literal root a constant (R7 decision 6; the runner accepts a constant-only program, B2) |
| D29, D30 | raw-grid reads; the to-one reader's check (if the product lacks it: measured at M0) |
| D35 | a from()'s inline test data and named runtime as `Setup` steps, established once per session (B2) -- in the test library (B4), refused on the product door (B6) |
| R11, D34 | effects as `Effect` steps; consecutive effects one script (R7 decision 14); the transaction rule (B2) -- planned by the TEST library (B4) on the product's statement entry, refused on the product door (B6) |
| D37 | the system tables: one code path to the graph's system database (R9, B7) |
| ids | one object-id counter per program (R10, B9) |
| D31 | a let read more than once materialized once (a CTE) where both judge modes and the product read it alike (B10; MV9) |

### 2.C Test-only, kept, named (the whole list)

| # | Construct | Where today |
|---|---|---|
| T1 | the verdict step: one assert as data, its base sides the product plans of its expressions, every judge-time fact the host judge needs (B3) | R7, R8 |
| T2 | the fused verdict group: consecutive verdicts as one statement (index per row), lets read by several members as shared CTEs, a keyed sub-group for raw-grid reads, the split rung on rejection | R9, D31, D45 |
| T3 | the database judge's lowering of each assert: one statement answering `holds`, the finished `message`, `unjudged`, a census `lenient` flag; the canon over the product plan's columns (R10); JSON key order and the scan order inside the canon wrap; a constant verdict where planning decides; `none(reason)` where the database has no form (§3.1) | D1-D2, D7, D19-D24, D22-D23 |
| T4 | the host judge: Java compares the sides' rows by `check` (§3.2); and the order key on a grid under assertion that the test did not sort (R8) | R7, D24 |
| T5 | the sink: the test listener (verdicts, unjudged reasons, the last answer), the BOTH mode's agreement, the trace stamp reserved for a fused statement | D44, D49 |
| T6 | the corpus lanes' engine row-order emulation (`StableScanOrder`), a run option the lanes pass, not a JVM global (R7 decision 8) | D25 |
| T8 | the test-data generator: a Keyed step (its queries the keys step, its rows filling the value regions of the statements after it; R14); if M0 finds a statement whose compiled SHAPE depends on the generator's rows, that case is posted before any other form | D38 |
| T9 | a computed effect string, and the map over `executeInDb` (R13): a Keyed step -- the string's query the keys step, the effect statement its region (one effect per row for the map; R14) | D39, R13 |
| T10 | `assertError`: an `ExpectFailure` step around its body's sub-plan; the error the body raises at run time (an early-detected one is a planned raise, R14) compared in Java with the expected message and position (N3) | R17 |
| T11 | the engine-text asserts: the two SQL texts compared as a constant of the plan; where they differ, the rows leg one statement on the lane's database where the golden runs there | R10 |
| T12 | staged raw grids (a raw grid read after an effect, held in a temporary table) | R6, D12 |
| T13 | the H2 referee replay: the verdict source of an engine-text assert whose texts differ (about 1,258 replays per DuckDB corpus pass), outside the body's plan (R6) | R10 |

### 2.D Order and 2.E the database judge self-contained

**2.D Order.** Today a database-mode segment runs its value statements, staged lets, store writes AND re-seeds first,
then its fused statement (D33, audit O1): an assert before a write or a re-seed is judged on the later state. After: a
verdict group closes before every non-verdict step (§9 N6), so an assert sees the state its position sees, in both
modes and as the engine does. MV1.

**2.E The database judge, self-contained (R5).** Today, after the row returns, Java applies the polarity, turns an
unjudged reason into a failure and builds the message (`DatabaseJudge.verdictOf`, `DatabaseJudge.java:227-258`), and
three arms judge in Java even on the database pass (D43: the grid pair by `TdsCompare.grids`, class-instance `assertEq`
by `HostJudge.eq`, the engine-text asserts' sides). After: polarity and message in the statement; the grid pair by
`VerdictSql.gridPair`; instances by `__id`; constant and `none` verdicts as plan entries; the engine-text asserts by
T11 and T13. MV3.

### 2.F What the database judge cannot do yet (measured 2026-10-31)

| Run | Asserts | Unjudged by the database |
|---|---|---|
| corpus, DuckDB | 5,847 | 0 |
| corpus, H2 | about 5,600 | 5, all `assertJsonStringsEqual` |
| channel B, DuckDB | 2,683 | 1, an `assertTdsEquivalent` |

(Today's "judged by the database" includes 2.E's Java arms; M0 counts them apart.) The declining shapes and their
fate: class instances of different static kinds and object-graph sides (a canon form: work); JSON equal as trees but
not as canonical text (SQL where the database has JSON functions; `none` on H2 where they fall short); a rendered grid
text not byte-equal (the line multiset in SQL: work); a `Number`/`Any` side of ambiguous kind (decided at planning or
by the wire's kind); a host-value side (a step of the plan); `assertIs`/`assertInstanceOf` of untracked values (a
design question when met); **two sides on two databases** (the system database and a user's, R9 keeps them apart:
`none`, named); a dialect's own limit (reported in its words).

## 3. The two judges

### 3.1 The database judge (T3): 100% a SQL plan

One statement per assert, answering one row: `holds` (BOOLEAN, never NULL, the polarity inside), `message` (spelled
exactly as today's messages, the bounded excerpt included; NULL when it holds), `unjudged` (NULL, or the reason with
its evidence), `lenient` (a census flag only). The sides' product plans are its subqueries; the canon (over the product
columns, R10), the comparison and the message are written around them. The sink reports the row as it is.

| Assert | Database form |
|---|---|
| `assertEquals`, `assertNotEquals` | the canonical texts in order (a collection its ordered canon list); `NOT` for not-equals |
| two relations (the grid pair) | `VerdictSql.gridPair` |
| a relation against cells | `gridRows` / `gridCells` |
| `assertSameElements` | the canon multisets |
| `assertSize`, `assertEmpty`, `assertNotEmpty` | counts |
| `assertContains` | membership of the canon |
| `assertEq` | canons; instances by `__id` |
| `assertEqWithinTolerance` | `abs(e - a) <= delta` |
| `assertTdsEquivalent` | `gridTolerance` |
| `assertJsonStringsEqual` | canonical JSON text (keys sorted, compact) |
| `assert`, `assertFalse` | the condition; `NOT` |
| a quantified assert | the predicate a column over the rows; holds when no row fails |
| `assertIs`, `assertInstanceOf` | identity for enums and tracked objects; the catalog's native |
| a rendered grid text | the line multiset with the cell tolerance |
| the engine-text asserts | T11 (a constant, or the rows leg); T13 where the golden runs only on H2 |

### 3.2 The host judge (T4): the product's sides, then Java

The runner runs each base side (the product plan of its expression; a literal a constant) at the verdict's place;
Java compares the rows by `check`, with the facts the verdict record carries (B3), reading no model:

| `check` | Java comparison (today's owners) |
|---|---|
| EQUALS, NOT_EQUALS | the engine's equality (`judge/Equality`), ordered unless the side's order is arrival order |
| GRID_PAIR | `TdsCompare.grids` |
| SAME_ELEMENTS, SIZE, EMPTY, NOT_EMPTY, CONTAINS, EQ | multisets, counts, membership, identity over `__id` |
| TOLERANCE, TDS_EQUIVALENT | `PureAsserts.assertEqWithinTolerance`, `TdsCompare.tdsEquivalent` |
| JSON, RENDERED_TEXT | parsed trees; `TdsCompare.renderedText` |
| CONDITION, IS, INSTANCE_OF, ROWS_ALL | the boolean (and the forAll-contains fold), identity, the class test, each row's element assert |

A host side differs from the product plan only by T4's order key on an unsorted grid under assertion (R8).

## 4. The movement register (replaces "zero movement", audit O0)

Every test's outcome and message stay identical **except the classes below, each measured at M0 (count, one example
before and after), ruled by the user, landed in its own commit, and listed in a committed register (test -> old ->
new)**. A movement not in an approved class stops the step and is posted. SQL text and statement-order classes are
posted the same way (R7 decision 12).

| MV | Class | Moves | Step |
|---|---|---|---|
| MV1 | 2.D: a verdict group closes before every non-verdict step | DATABASE-pass verdicts of asserts before a write or a re-seed; fused-statement counts | M10 |
| MV2 | decision 9: the two "AssertVerdicts.side" texts | failure messages, census keys | M8 |
| MV3 | 2.E: the database judge self-contained, arm by arm | grid-pair verdicts and messages; instance `assertEq` (a loud refusal becomes a verdict or `unjudged`); messages built in SQL per dialect | M8 |
| MV4 | R10: the canon over the product columns | verdicts and messages carrying a canon; untracked evaluations | M8 |
| MV5 | R10/B9: one id counter per program | id literals in multi-statement product write programs (SQL text); tests: none expected, checked. **Measured at M1 (2026-10-31): 0 test statements moved** (every core test package's 17,373 sent statements byte-identical before and after; the corpora's and channel B's censuses identical); product write programs now spell one counter across their statements (`ProgramObjectSitesTest`) | M1 |
| MV6 | 2.A's convergences | D5 milestoned reads, D7 streamed graph answers, D8 H2 type holes, D18 a new refusal, D27 value-shape facts, D36 target refusals | M5 |
| MV7 | decision 1 in one plan for both modes | the eager run at a let on the DATABASE pass too (+about 1,950 statements per corpus pass), §9 N4 | M5 |
| MV8 | R14: compile errors fail the whole test; early-detected run-time errors become planned raises | a test failing today at an earlier assert that also holds an invalid or unsupported later statement reports the compile error instead (still failing); such a statement in a branch that never runs now fails the test; each of today's lowering-time guard errors classified compile-time or run-time by Pure's meaning (an ambiguous one posted) | M10 |
| MV9 | D31/O9: a let read twice one form in both modes | host-mode text; outcome risk measured (the H2 pass's history of frames not found) | M3 |
| MV10 | B2: the transaction rule | effect bodies' transaction scope; the savepoint before a fused statement in a transaction (F11) | M3 |
| MV11 | R9/B7: one code path to the system database | product metamodel queries move to the system database; statement order on the system session | M4 |
| MV12 | R7 decision 6: constants in the product plan | the plan JSON format (a constant answer); product outputs unchanged | M2 |

## 5. Guards and oracles

- **Oracles.** The corpus censuses and rosters (`corpus_duckdb`, `corpus_h2`, host and database passes, uncached),
  channel B, the five core lanes, the gate, the Postgres targets unprivileged, the heavy lanes by hand where a step
  touches effects or the wire, and at each checkpoint the five lane sessions on every server and cloud backend (§8).
  Frozen except the approved register (§4).
- **The parity guard (R7 decision 13; audit K1).** Definition: for statement *i* of a body, the test road's steps are
  byte-identical to the program road's steps for statement *i* over the same statements before it, under the same run
  options; for a host side, the product plan of the side expression as statement *i*'s value, within the program;
  allowlist: T4's order key, T3's wrappers (a database side's subqueries are themselves under the guard after M8).
  During M2-M7 a census lane flag counts the differences by class, shrinking; after M10 the test road IS the program
  road, and the permanent form is structural: an ArchUnit rule that only the planner's statement entry builds a body's
  statement plans, plus a unit test per kind.
- **`theDriverCompilesNothing` (end of M10).** No class of `com.legend.driver` depends on `compiler.spec..`,
  `resolver..`, `lowering..`, `sql.dialect..`. Reachable only with B3's verdict record (today 15 of 19 driver files
  name them, audit K2). The Bazel layer file can drop `lowering`, `resolver`, `sql_dialect`, `normalizer`, `parser`
  from the driver's line; `compiler` stays (it also holds `compiler.element`, which `runProgram` needs).
- **Guards that move**, each with a dated justification in its own step:

| Guard | Why it moves | Step |
|---|---|---|
| `ArchitectureTest.plannerIsExecutionFree` | the assert module may not touch `com.legend.judge`: `KindClass`, the census and `CanonicalDivergence` calls move out of planning | M6 |
| `hostVerdictIsReachableOnlyFromTheVerdictSeam` (its exemption list) | the judges' entries change | M6-M8 |
| `theDriverHasOneCaller`, `theBodyDriverAndTheJudgesHoldNoJdbc` | the driver shrinks to the sink | M10 |
| `onlyTheSeamMakesSessions`, `execReachesNoPlanning`, `theRunnerIsModelFree` | the runner's new step kinds; the system database opened by the runner | M3, M4 |
| `JavaEvalLedgerTest` pins and registers | every moved and new file | each step |
| `HarnessDisciplineTest` (driver and planner sections) | sites move | each step |
| the spec registers (engine-order, outside-body), `native-claims.tsv`, `parser-equivalence/ratchets.tsv` | statements and claims move | M5-M10 |
| `PlanJson.VERSION` (22) and the TypeScript runner's pin (`engine-client/src/plan-runner.ts`) | new step kinds and record fields | M2, M3, M6 |
| `tools/deps/core-layers.txt` | the driver's line cut | M10 |

## 6. Design briefs (each written in full before its step's code; since R15 the step proceeds and its audit checks it)

- **B1, the statement entry (M5).** `PlanMaker` gains a public per-statement entry (the program so far, one
  statement -> its steps), so the old walk can ask the product planner statement by statement while asserts still take
  the old road; after M10 it is the program road's own loop. The entry inlines a statement under its position's mark
  (B9) and a statement's verdict sides in one inliner run, so a call written in two sides or two statements is two
  objects (S111's test-road limit removed); the test road's ids take the product's spelling there (MV6).
- **B2, the runner (M3, M9).** One plan per body (R14). New: `ExpectFailure(sub-plan)` for `assertError` (the
  error its body raises at run time handed to the comparison, N3); a raising step where an early-detected run-time
  error stands as a statement of its own (R14; inside an expression it is `PureSql.raise`); `Setup` per from() (D35; established
  once per session with the seeds' memo); constant-only programs accepted (`PlanRunner.java:231-233` refuses them
  today); the transaction rule: a program that changes the database runs in one transaction, effect scripts inside it
  with the dialect's bracket, and a fused verdict statement inside a transaction is preceded by a savepoint where the
  dialect aborts a transaction on error (F11), so the split rung can run. NOT new: what must run before a later
  statement can be written (T8, T9, the map over `executeInDb`) is the existing `Keyed` step inside the `Sequence`
  (S110, S1a); an invalid or unsupported program is a compile error (R14), so there is no "refused" step.
- **B3, the verdict record (M6).** Label, `check`, base sides, the database form, and every fact a judge reads at
  judge time today (the sides' static types, the order view, the keyed-class restriction, the TDSNull direction, a
  quantified assert's literal message, tolerance arguments), so no judge reads the model.
- **B4, where test-only planning lives (M6).** The assert module, the fused composition and the database forms in a
  library the product server and the WebAssembly and native planners do not depend on (`//core:plan_side` ships into
  `wasm/BUILD.bazel:29-45`), held by an ArchUnit rule.
- **B5, census and origins (M6).** Each plan step carries its statement origin; the runner counts by step kind; the
  planner never touches `com.legend.judge` or `exec`.
- **B6, the product's query door (M2-M3, §9 N1).** Effects, inline seeds, the eager run and verdict steps are allowed
  through `Execution.runProgram` (the test door) and refused by name through `Execution.run` (the product door and the
  HTTP query service).
- **B7, the system database, one code path (M4, R9).** The planner names the system tables by one rule; a step's
  `Target` names "the graph's system database"; the runner opens and seeds it once per graph, for a product query and
  a test alike; `Metamodel.read`'s per-plan carriage and the driver's `SystemRoutes`/`openRoute` go; a body mixing the
  system store with a user store stays refused by name.
- **B8, the canon over the product columns (M8, R10).** `__canon` computed in the wrapper from the product layout's
  declared properties, `__id` and `__type` (`ClassLayouts.objectLayoutOf` is the identity layout minus `__canon`);
  arm by arm, each a commit and an MV4 count.
- **B9, one id counter per program (M1, R10).** One `ObjectSites` per program instead of one per lowering
  (`TypedQuery.lowerer`, `ObjectSites.java:14-24`). An object's identity is its evaluation (its construction site and
  the positions of the maps it is built in, `compiler/spec/typed/Evaluation.java`; Pure: a new object per evaluation, a
  let names one); the counter only names sites, and one program is the scope in which objects meet. M1 checks every
  test's ids against today's. As built (M1, its audit folded): `PlanMaker.program` lowers every statement with the
  program's one minter, and inlines each statement under a mark of its position
  (`UserCallInliner.inlineBody(body, statement)`), because a site's instantiation numbers are counted per inliner run:
  without the mark one function's construction called in two statements had one site. The lets are reduced before the
  mark in every run, so a let's construction keeps one site. The test road's minter is the same class (the planner's
  `InstanceIds` deleted), one per body, its ids still spelled `i`; its statements and verdict sides are inlined in runs
  of their own, the limit recorded in S111, which B1 removes at M5.
- **B10, `execute(...)` frames in the program (M3).** The chain spliced into its readers by the compiler's rules
  (`ResultEnvelopeSplice`); the frame planned once and read as a CTE where it is read more than once (MV9); the eager
  run at the let a `Check` step (decision 1); the activity's SQL and its reserved trace stamp as plan data. Carried
  from M2's audit (2026-10-31): a frame's TABLE RENAMES (its connection's `tableReplace` post-processor) become plan
  data for both roads -- today the environment's renames are an `execute(...)` frame's and a `from()` frame's are
  applied on neither road (only a `toSQLString` call applies its own runtime's, `EnginePlans`); the census's one
  unattributed front-half difference (a graph fetch read through a frame, the STREAM factor) is re-measured here.
- **B11, the product's statement road (M2; written 2026-10-31 under R15).** The product planner gains the test road's
  language features, so a product query and a test statement are planned by the same functions; the test road is not
  touched (its own walk stays until M5). One road, `planner/ProductStatements`, used by `PlanMaker.plan` (a query) and
  by `PlanMaker.program` (each statement):
  1. inlined with the inliner's consumed lets KEPT (D9, D13; `TypedQuery.resolve` drops them today), under the
     statement's mark in a program (B9);
  2. natives staged (`NativeDispatch.stage` with `EnginePlans.nativeRoutines`, D14: `planToString`/`toSQLString` become
     the plan-time strings they print);
  3. resolved with the let bindings and the plan rows (`BodyPlanner.resolver`, D13), then the cross-store guard;
  4. prepared (`BodyPlanner.prepare`'s framing, D16): the OUTERMOST from()'s context is the frame -- its connection's
     time zone (D4), its features (D6, with the tree's and the caller's), its connection post-processors (D10) -- and
     its driver-PK and import-data-flow appends and the declared root kept; an answered kind (D17/R18: a DDL text, a
     handle) a constant answer, never a statement (as built: a DDL text is the plan's constant; a handle has no value
     a plan can answer, so a query answering one is refused by name and a program's handle before its last statement
     plans no step -- the test road's null is a stand-in its judges never compare);
  5. lowered with the PRODUCT's configuration (object layout, the program's minter, the exists-join form unless
     temporal, the streaming graph root on the answer, the slots) plus the frame's time zone and features and the
     seedable let prefix (D9); post-processed (`SqlPostProcessors.applyRecorded`, D10); `EnumValueTables`,
     `Metamodel.read`, the keyed pivot as today;
  6. a String or Boolean literal answer folded to a constant (D26, the test road's own rule, `LiteralFold`: the only
     types whose database spelling cannot differ from the literal's), a constant answer a new plan node (MV12:
     `PlanJson` 23, the TypeScript runner's pin); a program of constants only accepted by the runner (B2's
     constant-only programs, `PlanRunner.java:231-233`);
  7. a raw grid's or a dynamic pivot's rows keyed where the test road keys them (`rowsOrGridKeyed`, D29); a `toOne()`
     reader's cardinality held where the product answers (D30), by the same rule the test road's sink applies. As
     built: D29 in `PlanMaker.typed`, for a query and a program's last statement. D30 NOT built: the product answers a
     `toOne()` over a relation of other than one row with its rows today, and holding the rule turns that answer into
     an error -- an output change, put to the user first (§9).
  Not in M2: `execute(...)` read as a value (D15, the frames of B10, M3); effects (M3); the system database (M4).
  Movement: product SQL only, for queries that use these features; measured by every core test package's sent
  statements before and after (the M1 method) and by the M0 parity census re-run (its refusals for D9, D13, D14, D16
  and the generator carriers shrink, its TZ and POST differences go). The test road's statements: none expected.
  Sub-steps, each its own commit with the chain: M2a (1-5), M2b (6), M2c (7); the audit at M2's end. As built: the
  three were built and measured in order (M2a's movement zero) and land as one commit with one chain, since none was
  usable alone.

## 7. The steps (◆ = a checkpoint: the branch pushed, CI on every OS, the lane sessions, the movement register to the user)

| Step | What | Size |
|---|---|---|
| **M0** ◆ | Measure, no product change. Today's lowering-time guard errors classified compile-time or run-time (R14). The parity census BUILT (the parked probe in `docs/test-is-product-homework/probes/` re-lowers already-resolved bodies only, audit W4: the census plans each statement both ways through a statement entry, front half included); every MV class counted with an example; whether any statement's compiled shape depends on the test-data generator's rows (T8); the referee's MATCH/DIVERGED/DECLINED counts; capped host sides; seeds and errors inside groups; planning failures after a body's first failing statement; constant-only bodies; effectful maps and computed strings; frame CTE reads; D27, D30; the census baseline | 3-5 days |
| **M1** ◆ | One id counter per program (B9); every test's ids checked | 2-3 days |
| **M2** | Product features, the safe set: D4, D6, D9, D10, D13, D14, D16, D17/R18, D26 + constant-only programs, D29, D30 | 1-1.5 weeks |
| **M3** ◆ | Product features that need designs: effects + scripts + the transaction rule + F11 (B2), `Setup` seeds (B2), frames and the eager run (B10), the query door's refusals (B6) | 1.5-2 weeks |
| **M4** ◆ | The system database, one code path (B7) | 3-5 days |
| **M5** ◆ | Test statements planned by the product, statement by statement (B1); 2.A's convergences (MV6, MV7) | 1-1.5 weeks |
| **M6** ◆ | The verdict record (B3), the assert module in its own library (B4, B5), the host judge on product sides | 1.5-2 weeks |
| **M7** | The fused group composed at planning (from `VerdictBatch`'s flush) | 1 week |
| **M8** ◆ | The database judge self-contained, arm by arm (MV2, MV3), then the canon over the product columns (B8, MV4) | 2 weeks |
| **M9** ◆ | The kinds that run before a later statement is written, as Keyed steps (T8, T9, the map over `executeInDb`); `assertError`'s `ExpectFailure` (T10); inline `execute(...)` | 1 week |
| **M10** ◆ | One plan per body: `BodyCompiler` and `Segments` deleted; 2.D's rule (MV1); a planning failure a compile error (R14, MV8); `theDriverCompilesNothing`; the parity guard's permanent form; the layer file | 1 week |
| **Z** | An independent audit, the records, CI on every OS; the landing is the user's | 2-3 days |

## 8. Start here (for a session picking this up)

### 8.1 Where things are

Branch `claude/wizardly-dirac-rw6tim-seam`, PR #83 (draft; the user lands it). Records: `docs/GATES.md` (one entry per
step), `docs/result-boundary-homework/DECISIONS_LOG.md` (the next decision after D99), `docs/IN_FLIGHT.md`, this
plan's §10 status, and a `docs/SEMANTICS_REGISTER.md` row for every divergence from upstream (2.D moves toward the
engine; R8, R10 and T13 are reviewed for rows). Commit messages end with the session's attribution lines.

### 8.2 The rules (verbatim from the user, in force)

"Never push to main, never open a PR or merge without my explicit word in your session"; "never change how any output
is spelled without asking me first, one case at a time, with the expression and the before and after ... An answer of
mine to a question is not a go to build something else; ask. A guard pin or allowlist moves only with a dated
justification. Never read, print or copy credentials; lane sessions measure only. Run the start page's §6 checks
before each commit; commit and push to the branch; records ... land with the code." Upstream sources are spec input
only. And R11: commits stay local between agreed checkpoints.

### 8.3 How to run every check

Commands: `docs/RESULT_BOUNDARY_START_HERE.md` §6 (exact) and §7 (the lanes). Bazel in a cloud session:
`docs/CLOUD_SESSIONS.md` (`tools/cloud-distdir.sh`, the disk allowance; never `bazel query //...`, it fetches every
platform and fills the disk; clean `$(bazel info output_base)/execroot/_main/_tmp/*` and `sandbox/*` between runs).
The chain used by the S2-S4a steps, in order:

```
B=/root/.local/bin/bazel
$B test --keep_going --disk_cache= --nocache_test_results --local_test_jobs=2 //spec:corpus_duckdb   # then //spec:corpus_h2
grep -a "statement-origins\|\] pass=" bazel-bin/spec/judge_{host,database}_{duckdb,h2}/*.log           # the census
$B test --keep_going --disk_cache= --nocache_test_results --local_test_jobs=2 //pct:pct_channel_b_{essential,grammar,relation,standard,unclassified}
$B test --lockfile_mode=error --keep_going --disk_cache= --local_test_jobs=2 //gates:local               # 9 Postgres targets refuse root:
$B test --keep_going --disk_cache= --local_test_jobs=1 --strategy=TestRunner=standalone \
   --run_under="tools/postgres/as_unprivileged.sh ubuntu" <the nine Postgres targets of //gates:local>
```

Baseline census (fe643205 and 8a842acb, unchanged since S3): DuckDB host body=3972 let=1974 other=6219 system=121
tdg=890 pass 2471/fail 108; DuckDB database body=6553 pass 2473/108; H2 host let=1950 other=5793 system=120 pass
2289/295; H2 database body=6430 fallback=18 pass 2233/353; `probe=0` everywhere; DuckDB judge agreement 5,847/5,847.

### 8.4 The lane sessions

Five sessions measure every server and cloud backend, MEASURE ONLY (no edits, commits, pushes, PR comments or
credentials): sibling 1 (Trino A/B, Postgres B), sibling 1b (SQLite, MariaDB, MySQL, ClickHouse), Snowflake,
Databricks, BigQuery. They compare per test and per statement (with `-Dlegend.diagnostics=dump-sql`) against their
previous round. Their brief and session ids: `docs/RESULT_BOUNDARY_START_HERE.md` §7 and GATES' lane entries (round 6,
fe643205, all five identical). Asked at each checkpoint (◆), not at every commit.

## 9. Still open (for the user)

- **N3, `assertError` (T10) -- RULED: A.** The shape: an `ExpectFailure` step runs the body's sub-plan and the runner captures
  what came back -- the error's message and position (raised by the database, or by the compiler while planning the
  body, O8), or "no error" -- and the outcome is reported through the same verdict path as every assert (today a
  special arm runs a nested body). Where the comparison happens, two options:
  - **A (recommended):** Java compares the captured message and position with the expected ones, in both judge modes:
    the one other named Java comparison beside the host judge's. Honest because the "actual" side is an error the
    client caught, never a value a query computed: there is no SQL of ours under test in that comparison.
  - B (the user's question, "why can't you send the error message to the database exactly like assertEqual on
    strings"): the captured message bound as a parameter into a string-equality verdict in the fused statement. It
    works and costs no extra round trip where the assert shares a group, but it tests nothing new, adds a run-time
    value bound into a later statement (the flow the program removes elsewhere), and string equality differs across
    databases (MySQL's default collation is case-insensitive; some ignore trailing spaces), so it needs a binary
    collation per dialect to be right.
  Today's failure texts ("Execution error message mismatch. The actual message was ...", "No error was thrown", the
  line and column forms) are kept either way.
- **R14's one ambiguous case -- RULED 2026-10-31: a planned raise.** The host judge's "golden JSON does not parse"
  (`sql.Json`, 2 failing tests per corpus, host pass only) was posted by M0. The golden is a literal, so the failure is
  knowable at compile time, but the engine parses it when the assert runs. The user: "Planned raise is fine". It
  becomes a planned raise at the assert's position, as the engine raises it there. Every other planning failure M0 saw
  is classified in `docs/test-is-product-homework/C_M0_MEASUREMENTS.md` §4.
- **D30, a `toOne()` over a relation in the product -- OPEN (posted at M2, 2026-10-31).** The expression:
  `#TDS\nA\n1\n2\n#->filter(r|$r.A > 0)->toOne()`, run as a product query. Today the product answers its two rows
  (A = 1, A = 2): the lowering makes `toOne()` over a relation the relation itself and nothing checks the rows. The
  test road raises "toOne() over a relation returned 2 row(s) -- the exactly-one contract (engine reader semantics)"
  (`JudgePlans`' `Cardinality` sink, `BodyDriver.enforceToOneReader`), and `docs/audit-22b-wire.md` F1 recorded the
  engine throwing on it (probe W1). Holding the rule in the product changes an answer into an error, so it waits for
  the user. If ruled in: the plan carries the answer's to-one fact and the runner holds it with the test road's
  words (the scalar and the required-root bounds the same way); upstream's answer is read from the pinned engine
  sources before it lands. If ruled out: a `docs/SEMANTICS_REGISTER.md` row for the product's answer. No corpus or
  channel B case reaches it (M0).

## 10. Status

- 2026-10-31: v1 drafted after the code inventory (A); the user's rulings R1-R12; the independent audit (B) found six
  blockers and many majors in v1 (incoherent rulings, a hidden oracle conflict, the step order, missing designs,
  resumability; 4 weeks under-sized to 9-13); v2 written from it. The user agreed v2's §9 but N3 (R13) and R10's ids;
  N3 ruled A. The user's review questions led to R14 (a planning failure a compile error; one plan per body, Keyed
  steps for what must run first) and two corrections (the test-only kinds planned by the test library; M0 builds the
  parity census); then R14 refined: compile errors fail the test, early-detected run-time errors become planned
  raises, the body compiled whole. Next: M0, on the user's word.
- 2026-10-31: **M0 measured** (the user's go: "Yes great let's do all the measurements"):
  `docs/test-is-product-homework/C_M0_MEASUREMENTS.md`, the probe saved as text in `probes/`.
  - Where both roads plan, the SQL is the same: 30,448 identical and 3,250 numbering only of 34,463. The 765
    differences are the plan's five lowering factors, with no front-half difference.
  - The product road's refusals are its missing features, chiefly D15 (92%).
  - MV1 moves no test.
  - MV8 is at most one H2 test.
  - The four lowering-time guards are run-time errors, so planned raises. One case is posted (§9).
  - T8, R13, D18, F11, D30 and MV2 have no corpus or channel B case.

  Checkpoint: the branch pushed, CI, the lane sessions asked. Next: M1, on the user's word.
- 2026-10-31: the user's go for the whole program, back to back, each step ending with an independent audit (R15).
  The lanes' round 7 on S4a's code: all five identical.
- 2026-10-31: **M1 built** (D100). One object-id minter per program: the product's `PlanMaker.program` lowers every
  statement with one `ObjectSites` and marks each statement's own evaluations; the test road's copy (`InstanceIds`)
  deleted. MV5 measured: 0 test statements moved. The audit folded (the statement mark; S111's test-road limit,
  removed at M5). Checkpoint: the branch pushed, CI, the lanes asked. Next: M2.
- 2026-10-31: **M2 built** (D101). The product's statement road (`planner/ProductStatements`): a query's and a
  program's statements planned by the test road's front half; constants as plan nodes (PlanJson 23); D29's keyed raw
  grid in the product. Measured: core's statements move by design only (45 literal answers send nothing); the census
  re-run (`docs/test-is-product-homework/C_M2_CENSUS.md`) finds no front-half difference outside `execute(...)` frames
  and one defect inside one (a `toSQLString`'s table renames), fixed. The audit's blocker (a keyed answer's shape) and
  seven should-fix folded. D30 posted to the user (§9). The lanes' round 8 on M1: all five identical. Next: M3.
