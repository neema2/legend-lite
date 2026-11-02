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
| R16 | 2026-10-31, the re-plan of M5-M10 ("Let's do it", on the recommendation of that day): **whole bodies move, class by class**, never a statement of a body. The per-statement bridge (the old B1: the old walk asking the product planner one statement at a time) is DROPPED. A body runs either entirely as one plan on the program road or entirely on the old walk; a router at the test door decides by a named rule over the body's typed shape (never by catching a failure), the classes moved are pinned in one closed list that only grows, and M10 moves the last class and deletes the walk and the router. Classes open as their features land: assert-free programs (M5), bodies whose asserts all have verdict steps (M6-M7), bodies needing the self-contained database judge's arms (M8), bodies with run-first kinds (M9). Every class moves in its own commit, measured whole (every statement, the census, the lanes). B1, S111's limit and the per-statement pairing go. |
| R17 | 2026-10-31, M3's choices (the same go): (C1) the transaction rule is TODAY'S -- only a store write opens the program's transaction; an effect script commits itself, written with its dialect's bracket where the session owns its transaction (upstream's `executeInDb` runs with autocommit, legend-pure `ExecuteInDb.java:169-173`); B2's sentence corrected; MV10 = 0. (C4) the trace stamp is bound when the plan runs (a slot, as a write's count), so a plan stays deterministic and the stamp is per execution, as upstream mints it. (C7) B4's test-planning library is built NOW (M3), so effects, seeds and the eager run never ship into the WebAssembly or native planners. (C9) the `from()` frame's table renames apply on both roads at M3, the count and an example posted before they land (upstream applies a connection's post-processors to any query on it, `postprocessor.pure:66-80`). The smaller choices as recommended: the product door refuses a let-bound `execute(...)` by name (C12; an inline lazy read stays the product's); refusal messages are errors, not output spellings (C8); the test door's target carries no setup until a body class moves (C6); a savepoint is its dialect's text sent as a statement (C5); staged raw grids are refused on the program road and get a step at M9 (T12); `loadCsvToDbTable` stays on the walk until its body's class moves (C10). |
| R12 | Unchanged from the predecessor program (§8.2): never push to main, never open or merge a PR without the user's word; no output spelling change without the user's word, case by case; an answer to a question is not a go to build something else; a guard pin or allowlist moves only with a dated justification; never read, print or copy credentials; records land with the code. |
| R19 | 2026-11-01, the order after M3 (asked; the user chose the recommendation): **M5 before M4** -- the test door's code carries real test traffic sooner, the window in which two roads coexist shrinks. M4's measurement (M4a) is done and waits. On M4's server rule the user asked "Why are we changing it if it works?": no answer is broken; the narrower M4 (the plan can name the system database, the test door plans it, the product's forms unchanged) is recommended and comes back to the user as a one-line choice at M4. |
| R20 | 2026-11-01, before everything else (the user, verbatim): "The real thing that needs to come before everything is killing mutable state and fixing everything expertly and also split by concern". A step of its own, **MR** (brief B14), before M5: the planning code that survives the program (the planner, the test-planning library, the runner's program loop) without mutable state held across calls or reset by convention, split by concern, the expert-level defects of that code fixed -- zero movement (every statement byte-identical, the census identical), audited as every step. The walk M10 deletes is not refactored. |
| R21 | 2026-11-01, M4's scope and the answer facts (asked after M5; the user's words): M4 -- "Why would test diverge from product? Why change test if product works correctly - why not get test to match product if product correct": the product's system-store road (the store's tables carried in the query's own plan, `PlanMaker.metamodel`) is kept; the TEST road moves onto it, the driver's separate system database (`driver/SystemDatabase`, `planner/SystemRoutes`, `BodyDriver.openRoute`) goes; B13 rewritten. The print answer -- "Does that match engine too? If matches engine too do it": legend-engine has no `print` (refused, upstream-api/m5-answers.json), so the product's answer stays. A test body's literal answer -- "What matches upstream engine/pure? We should always try to match upstream as much as we can/makes sense": Pure types `true` `Boolean[1]` (the engine carries no multiplicity), so a one-value answer is `[1..1]` on both roads. A value through a class's chain -- "We should match upstream engine/pure both as much as we can/makes sense": the engine answers the value's rows (`String`, its SQL column's label, no to-one check); the product's column becomes the value's (`value:String[0..*]`, Pure's multiplicity), the engine's label `NAME` not taken (lite's value answers are `value`; its engine-compatible API refuses a value answer, unprobed until now). |
| R22 | 2026-11-01, the cadence (asked after M5: M6-M10 still ahead; the user's words): "Yes keep plan, yes keep audits, yes go faster": from M4 each step keeps its independent audit; the lane sessions are asked at an M-step's boundary only (not at sub-steps); one measurement chain per step (not per sub-step); a step moves a bigger class of bodies, its movement classes posted to the user in one batch. |
| R23 | 2026-11-01, M4 deferred and no more stops (the user's words, asked whether the test road should take the product's system-store road at the cost of per-session seeding): "What does the product version do exactly? Maybe that needs to move to test way? Can we please skip m4 for now and finish everything else so we don't have to keep revisiting this and stop asking questions - get everything else done now keep going don't stop". M4 is DEFERRED (both directions recorded: the test road onto the product's form, or the product onto the separate system database); M6-M10 and Z run back to back without questions; a step that would change an output keeps today's behaviour and records the residue instead of stopping, its movement reported in the step's batch (R22). |

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
| MV6 | 2.A's convergences | D5 milestoned reads, D7 streamed graph answers, D8 H2 type holes, D18 a new refusal, D27 value-shape facts, D36 target refusals | with each body class (R16), from M5 |
| MV7 | decision 1 in one plan for both modes | the eager run at a let on the DATABASE pass too (+about 1,950 statements per corpus pass), §9 N4 | with the body classes that read frames (R16) |
| MV8 | R14: compile errors fail the whole test; early-detected run-time errors become planned raises | a test failing today at an earlier assert that also holds an invalid or unsupported later statement reports the compile error instead (still failing); such a statement in a branch that never runs now fails the test; each of today's lowering-time guard errors classified compile-time or run-time by Pure's meaning (an ambiguous one posted) | M10 |
| MV9 | D31/O9: a let read twice one form in both modes | host-mode text; outcome risk measured (the H2 pass's history of frames not found) | counted at M3; lands with the body classes that read frames (R16) |
| MV10 | B2: the transaction rule | today's rule kept (R17): effect bodies' transaction scope unchanged (0); the savepoint before a fused statement in a transaction (F11), counted at M3 (M0: 0 such groups) | M3 |
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

- **B1, the body router (M5; rewritten 2026-10-31 by R16, the per-statement entry dropped).** At the test door
  (`Execution.runProgram`) a router decides, by a named rule over the body's typed shape, whether the body is planned
  WHOLE by the program road (B12's test-door entry) and run as one plan, or walked by today's driver. The classes moved
  are one closed list in one place, pinned by a guard that lets it only grow; nothing is decided by catching a
  failure. A moved body's statements and verdict sides are inlined the program road's way (B9's marks), so S111's
  test-road limit and the per-statement pairing go with the old walk; a moved body's ids take the product's spelling
  (MV6). M10 deletes the router with the walk.
  **M5's detail (written 2026-11-01 from M5a, `docs/test-is-product-homework/C_M5A_MEASUREMENTS.md`):**
  1. **The class, by a named rule** (`testplan/BodyClass`): ASSERT_FREE -- no statement holds an assert or an
     `assertError` (the query door's scan) and none holds a shape a later step plans, each named and detected on the
     typed body, never by trying the test door: an effectful map and a computed effect argument (T9, M9), an
     `executeInDb` result read (T8), a raw grid read as its cells or its text, a staged let used as a statement, an
     `execute(...)` read where it stands or `values->at(k>0)` (M9), a system-store read (M4). The classes moved are one
     closed enum, pinned by a guard that lets it only grow; a body of a moved class the test door refuses FAILS (R14),
     and a guard holds every ASSERT_FREE body core's tests run planned whole.
  2. **The run.** `Execution.runProgram` routes an ASSERT_FREE body to `TestDoor.plan` and `PlanRunner.program` on the
     body's own session (its trace, its options' features), its answer streamed to the caller's `Answer` sink by its
     shape, no verdict event (an assert-free body has none, as on the walk).
  3. **The raw-SQL ledger** (`ExecuteOptions.recorder`, which the referee mirrors to replay the golden SQL of later
     bodies): every effect and seed statement the moved body executes is recorded after it ran, in order, with the
     text the walk records (`EffectPlanner.Statement.recorded`, `CsvSeed.Planned.text`), a failed one never: the
     texts ride beside the plan (the test door's own index of its steps, never the product plan's), and the runner's
     program sink is told when a step has run.
  4. **Movement (posted, measured):** MV6 (a moved body's ids the product's spelling), MV7 (a frame let's eager run on
     every pass), MV9 (a frame read more than once by its CTE); every moved body's outcome and every core answer
     identical; the census per pass; the lanes.
  Sub-steps: M5a measured; M5b the class rule and its guard; M5c the ledger beside the plan and the sink's step
  event; M5d the router and the move, measured; M5e the audit, the records, the checkpoint.
  5. **The classes, as built (2026-11-01, M5b-M5e; item 1 superseded).** M5a showed the assert-free bodies are four
     populations, not one: the corpus's effect setup programs (616 per pass), core's metamodel reads (76, the system
     store: M4's), core's raw-grid, staged and `execute(...)` bodies (each a kind a later step plans: T8, T9, M9),
     and plain query programs. A rule admitting "assert-free minus the named shapes" would re-derive over the typed
     body refusals the lowering makes deep inside it (a raw grid read as cells, `toString` over a grid, an `execute`
     read where it stands): a second copy of the lowering's knowledge. So each class is a POSITIVE rule that admits
     only the shapes it names. `EFFECT_PROGRAM` (M5d, narrowed by its audit): database effects with literal
     arguments and lets of them read only as a connection; orchestration handles (`BodyPlanner.handleType`, one rule
     with the planner); String and Boolean literals; `print` holding no test kind and no store read; at least one
     effect; no statement names test data and the runtime declares none. 600 bodies per corpus pass move (M5d's first
     rule moved 616; the 16 the narrowing returned to the walk are shapes it does not admit, not yet itemized). The answer a moved body hands
     its caller is the walk's (a `[0..1]` column, no row for no value: `ProductStatements.asTheWalkAnswers`, the
     test door only, a named residue until §9's question is ruled); a failing script is named as the walk names it
     (`testplan/EffectSegment`, one spelling for both roads). `BodyRouterParityTest` runs each admitted shape both
     ways on every in-process database Pure's `DatabaseType` names and compares the outcome and message, the
     raw-SQL ledger, the answer, both censuses and the database left; each excluded shape is pinned there as the
     walk's. `QUERY_PROGRAM` (M5f, narrowed by its audit): a body whose one statement that runs is its last, a query the
     product's query door plans -- before it only lets of literals or handles, String or Boolean literals, prints,
     handles; no test kind; no `execute(...)`, staged rows, raw grid, store write, test-data generation, system-store
     read, object built, graph or temporal read; an answer that is a relation or a value reading no class's extent,
     not an object; a to-one reader only as the answer's root. The router holds the walk's answer contract (at most
     one row, at least one, exactly one for a to-one reader: the D30 residue) and asks for the caller's sink before
     anything is sent, as the walk does. The excluded shapes are each pinned in `BodyRouterParityTest` and move with the
     step that rules or plans them (MV6's facts and ids, D5, D30, M4, M9); every assert-free body is in a class by
     M9.
- **B2, the runner (M3, M9).** One plan per body (R14). New: `ExpectFailure(sub-plan)` for `assertError` (the
  error its body raises at run time handed to the comparison, N3); a raising step where an early-detected run-time
  error stands as a statement of its own (R14; inside an expression it is `PureSql.raise`); `Setup` per from() (D35; established
  once per session with the seeds' memo); constant-only programs accepted (`PlanRunner.java:231-233` refuses them
  today); the transaction rule (corrected 2026-10-31 by R17: the first wording could not hold -- DuckDB's and Postgres's
  scripts open their own `BEGIN ... COMMIT`, and upstream runs `executeInDb` with autocommit): only a store write opens
  the program's transaction; an effect script commits itself, written with its dialect's bracket where the session owns
  its transaction; and a fused verdict statement inside a transaction is preceded by a savepoint where the dialect
  aborts a transaction on error (F11), so the split rung can run. NOT new: what must run before a later
  statement can be written (T8, T9, the map over `executeInDb`) is the existing `Keyed` step inside the `Sequence`
  (S110, S1a); an invalid or unsupported program is a compile error (R14), so there is no "refused" step.
- **B3, the verdict record (M6) -- STRUCK 2026-11-01 (the user, at M6's revalidation: "shouldn't asserts be regular
  lite functions that lower like any other function in the database judge? Why would they need extra data/metadata in
  the plan?").** As written: label, `check`, base sides, the database form, and every fact a judge reads at judge time
  today (the sides' static types, the order view, the keyed-class restriction, the TDSNull direction, a quantified
  assert's literal message, tolerance arguments), so no judge reads the model. Why it goes: in the DATABASE judge an
  assert already IS a regular function lowered like any other (`VerdictSql`: one statement whose row is `holds`,
  `message`, `unjudged`); the plan carries that SQL and nothing about the assert (after M8's MV2/MV3 not even the
  polarity). The facts B3 named are read by the HOST judge's Java comparison alone, and the host judge is the TEST
  HARNESS (the cross-check: channel B's agreement, the corpus host pass, PCT) -- harness Java may read the typed tree
  when it compares, as the referee does. So: no assert metadata in the plan; M6's closures stand as the harness's
  form; `theDriverCompilesNothing` (M10d) is stated as "the driver sends nothing and plans nothing" (it may read typed
  facts to compare rows). A later ruling, after M8 is measured: the database judge the verdict of record, the host
  judge the cross-check (today channel B has it the other way); PCT stays host-judged until the database judge spells
  every lane's dialect.
  **B3 as settled (the user, 2026-11-01, "I love it"), three designs, each in plain words:**
  1. *The database judge* -- an assert IS a function lowered to SQL like any other: one statement whose row says
     `holds`, `message`, `unjudged`; the plan carries that statement and nothing about the assert (M8 moves the
     polarity and the message into it too). Java reads the row.
  2. *The host judge* -- the verdict step carries the two side statements, ONE ENUM naming the comparison (a closed
     list, one Java function each: EQUALS, NOT_EQUALS, SAME_ELEMENTS, SIZE, EMPTY, NOT_EMPTY, CONTAINS, EQ,
     TOLERANCE, TDS_EQUIVALENT, JSON, RENDERED_TEXT, CONDITION, IS, INSTANCE_OF) and two or three small flags the
     enum cannot express (order matters; which side is the golden; a quantified assert's literal message). The runner
     runs the two statements; the sink switches on the enum and calls that function with the rows and the flags; the
     types the comparison needs are the sides' result columns, already in the plan. The two places today's Java
     consults the model at judge time are precomputed at planning: `assertInstanceOf`'s subtype check becomes a SQL
     side answering a boolean (as the database judge lowers it); the identity-key restriction becomes a flag decided
     when the side's SQL is written. After it the sink holds no compiler object and the plan is complete data. M8's
     first item (it touches the arms M8 completes); until then M6's closures stand.
  3. *The engine-text asserts* (M10b) -- `assertSameSQL(golden, $q->toSQLString(...))` is a comparison of two
     strings known at planning (`toSQLString` a plan-time text, the golden a literal), so it plans and runs EXACTLY
     like `assertEquals('a', 'b')`: a constant verdict, no special logic in the plan or the planner, on any database.
     Only when that verdict FAILS does the harness run its extra step at the assert's turn, outside the plan: our rows
     (our query for `q` run through the plan runner as a one-statement plan on the lane's database), the golden
     replayed on the H2 referee seeded from the body's recorded setup, the two row sets compared in Java; agree ->
     pass (the text mismatch counted, as today), differ -> fail showing both. The three variants (`assertSameSQL`, the
     test-data-generator form, the dual-golden form) the same. Nuances: a golden can run only where its dialect runs
     (today every golden is engine H2 text: the referee is H2 by construction; a golden in a lane's own spelling would
     match our text and never reach the appeal); the harness fetches our rows itself, so the test's plan is exactly
     what the Pure says.
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

- **B12, the program road's effects, seeds and frames, and the query door (M3; written 2026-10-31 under R15, its
  choices R17's; drafted from a read of the code at d9dc669a, `docs/test-is-product-homework/D_M3_BRIEF_DRAFT.md`).** The program road
  (`PlanMaker.program`, `ProductStatements`) gains, behind a door, what a test body's non-verdict statements need. The
  test road is not switched (bodies move from M5, R16); verdict steps reach the program road at M6; T8-T10 and T12 at
  M9; the system database at M4 (refused by name on the test door until then).
  1. **The doors (B6, N1).** `PlanMaker.plan` stays the QUERY door for every product caller; a new TEST-door entry plans
     any body through `program`, called only from `com.legend.driver` (an ArchUnit rule; no caller until M5). The query
     door scans the typed body once and refuses by name, at the first statement holding one: an effect, a `from()`'s
     inline seeds or the seeds of a named runtime OTHER than the plan's own (the plan's own runtime's seeds are its
     target's setup already: M3a found 47 working server queries naming it), a let-bound `execute(...)` (its eager
     run, C12), a verdict (an assert; `fail` is a product raise, not a verdict: M3a found 16 Cypher write guards
     calling it), an `assertError` (the owners reused: `StatementEffects.containsEffect`/`callsVerdict`,
     `ExecutionContext.reader()`, `LetPlanner.kind`). An inline lazy `execute(...)` read stays the product's (D15).
     M3a measured the narrowed rule refusing none of core's 3,568 product-door queries.
  2. **One program state.** `PlanMaker.program` holds one planning state for the program -- the plan rows and lets a
     frame needs across statements, the frames, the minter (B9), the door, the last seed key emitted, the steps waiting
     to be placed before a statement -- where `ProductStatements.prepare` makes a fresh environment per statement today.
  3. **The test-planning library (B4 brought forward, C7).** A library the product server and the WebAssembly and
     native planners do not depend on (`tools/deps/core-layers.txt`, an ArchUnit rule): the test door's entry, the
     effect and seed planning, the eager run's step. The planner keeps what a product query can use (frames' lazy
     reads, constants).
  4. **Effects as `Effect` steps (R11, D34, decision 14; test door).** `Prepared.Effect` split by its catalog row: a
     database effect's statements written by `EffectPlanner` (each String argument a literal, folded; a computed one
     refused by name, T9 at M9; M0 counted 0 of 124,975); `setUpDataSQLs` a constant of its texts; `print`/`println`
     no step (a null constant when last, both doors, R18); `connectionByElement` a handle; an effectful map refused
     (T9, M9). Consecutive effect statements one script (today's segment rule and text); `ExecutionPlan.Effect` gains
     the script's statements, its bracketed form and its abort statement, the runner sending the bracket only where
     the session owns its transaction, the abort first on a failure (today's `sendScript` rules). The effect writer is
     handed the program's dialect once the runtime is decided (M2's road names none). An effect as the program's last
     statement answers as the test road's host rows do (C11).
  5. **The transaction rule (R17 C1, MV10 = 0).** `PlanRunner.program` opens one transaction only when a step is an
     `Update`; an `Effect` inside an open transaction sends its bare text; after each `Effect` the session is marked
     written (as `BodyCompiler` does), so a later seed step re-seeds.
  6. **F11's rung (built, emitted at M6/M7).** `SqlDialect` states whether its database aborts a transaction on error
     and writes its savepoint, rollback-to and release texts (or none); `ExecutionPlan.Verdicts` gains the optional
     savepoint; `PlanRunner.verdicts` sets it before the fused statement inside a transaction, rolls back to it before
     the wrappers when the database rejects the fused statement, releases it otherwise (C5: texts sent as statements,
     the JDBC census unmoved); `VerdictBatch` fills it. M3a: no group runs in a body transaction, but about 133 per
     corpus pass are flushed inside the corpus harness's per-test transaction; H2 has savepoints and never aborts,
     Postgres has both; DuckDB aborts a transaction on a run-time error and has NO savepoint, so on DuckDB a fused
     statement rejected inside a transaction cannot fall back to the wrappers -- today's behaviour, kept (no movement;
     M3a counted 0 such rejections on DuckDB) and recorded as a named limit.
  7. **Seed steps (D35).** A new node `ExecutionPlan.Setup(key, steps)`, emitted before a statement whose seed sources
     (the `from()`s under it, their named runtimes, the program's runtime: `establishContexts`' sources) name test
     data, when the key differs from the last emitted or an `Effect` came since; its steps `CsvSeed.planned`'s; the
     runner skips a key the session holds clean (`SessionState`) and otherwise runs it under SEED and records the key.
     The planner memoizes the generated steps by the sources' value, never the model's identity (C2). The test door's
     target carries no setup (C6).
  8. **Frames (D15, B10).** `planner/ProgramFrames` implements `ResultEnvelopeSplice.Frames` (the pasted chain, an
     inline execute's chain and activity rows, the activity SQL); `ProductStatements.prepare` inlines with the splice
     hook; the let loop dispatches on `LetPlanner.kind` (FRAME into the frames, never the lets; on the test door the
     eager run a `Check` step at the let, decision 1; ALIAS; STAGED as today; STAGED_GRID refused, T12 at M9; VALUE
     into the lets); a value-position `execute(...)` a `Check` before the last statement and the result step over its
     run chain; the renames widening (`BodyDriver.frameReplaceEnv`) moves to the planner.
  9. **The frame's CTE (D31, MV9 counted).** Read as a CTE where a statement reads the frame more than once (B10's
     words, per statement; M3a: about 2,008 database-pass statement-frames read once would move from a CTE to the
     pasted chain, at most 118 host-pass ones the other way -- MV9's text movement, landing with the body classes), lowered once at its let with the product's configuration and the program's minter, its
     definition attached by `FrameCtes.attach`; the movement against today's two modes counted, landing with the
     classes that read frames (R16).
  10. **The activity's SQL and stamp (D47, D49; R17 C4).** The activity SQL is plan data already; the comment the
      activity rows record is a slot the runner binds when the plan runs (as a write's count), its value the trace's
      stamp for the statement it precedes -- the plan deterministic, the stamp per execution, as upstream's.
  11. **The `from()` frame's table renames (R17 C9).** `BodyPlanner.prepare` reads the outermost `from()`'s context's
      `tableReplace` and merges it with an execute frame's, loud on a conflict, on both roads -- its own sub-step.
      M3a: 11 statements per host pass and 7 per database pass carry such renames, and in every one the environment
      already holds the same renames from its execute frame, so 0 statements move.
  Sub-steps, each its own commit with its measurement; the audit at M3's end:
  M3a measure (C_M3_MEASUREMENTS.md: transaction scopes, seed sends inside an effect run, frames read once and twice,
  activity comments read, inline eager executes, frames on the system store, staged grids, from()s carrying renames and
  their statements, the door's refusals in core's tests, DuckDB's and H2's abort-on-error and savepoints);
  M3b the plan records and the runner (`Effect`'s script, `Setup`, `Verdicts`' savepoint, the stamp slot; PlanJson 24;
  the TypeScript runner refusing the new nodes by name; the transaction rule, the seed memo, F11's rung) -- no movement;
  M3c the doors, the test-planning library, the program state -- refusal texts only;
  M3d effects on the test door -- every corpus setup program planned whole, its scripts against the texts the walk
  sends (byte-identical expected);
  M3e seed steps -- per statement against the walk's seed texts, keys and skips;
  M3f frames, the eager run, the stamp -- the D15 refusals become plans, census-paired;
  M3g the frame's CTE -- counted;
  M3h the `from()` renames -- test-road SQL moves: counted, posted, landed;
  M3i the census re-run, the audit, the records, the checkpoint.
  **Departures, recorded at M3i (the audit's §2):** item 4's `setUpDataSQLs` as a constant of its texts is NOT built
  (the test door refuses it by name, M5); item 8's value-position `execute(...)` is refused by name until M9; item 3's
  library rides into the product server through the driver (the driver's dependency, not the planner's: ArchUnit pins
  the planner free of it); item 4's and item 7's steps do not carry the referee's recorded texts
  (`EffectPlanner.Statement.recorded`, `CsvSeed.Planned.text`) -- the walk records them, the replay ledger's (T13) with
  M5; item 6's savepoint is filled by `VerdictBatch` on the walk from M3b, so the Postgres verdict groups send it now
  (not "at M6/M7"); item 5's effect inside a keyed step's snapshot runs bare in it (the snapshot holds the program in
  a transaction a store write did not open), a named limit.

- **B14, mutable state out and split by concern (MR; R20, written 2026-11-01 from an independent read-only inventory
  of the code at 96a1dbde, `docs/test-is-product-homework/E_MR_INVENTORY.md`).** The code that survives the program
  -- the planner's program road (`PlanMaker`, `ProgramState`, `ProgramFrames`, `ProductStatements`, `FramePlanner`,
  `QueryDoor`), the test-planning library (`TestDoor`, `TestKinds`, `Seeds`, `EffectPlanner`) and the runner's program
  loop (`PlanRunner.program`) -- holds no state that one call leaves for another to read, and no field reset by
  convention; where mutation is the honest form (a builder with ONE owner: the object-id minter the lowering calls
  deep inside, the inliner's let map while it inlines, the runner's live values the database produces), it is private
  to its owner, never aliased, frozen when handed out. Zero movement: every core package's statements byte-identical
  (17,372), the corpora's and channel B's census identical. The walk M10 deletes is not refactored.
  1. **A statement's facts are a value.** `ProductStatements.prepare` returns what the statement read --
     `StatementReads(ownStamp, definitions, inline runs)` -- in its `Statement`; nothing reads "the last prepare's"
     fields. The hook's recorder is local to one inlining (the inliner calls it re-entrantly), frozen when it ends.
  2. **The program's facts have one owner.** `ProgramFrames` keeps only the program's own (its frames, consumed lets,
     the stamp count, the program's rows); `ProgramState` is a builder confined to `PlanMaker.program`, its lists
     private (no aliased locals, no live views), the test-planning library handed a narrow view and an `add`.
     `TestKinds`' script and established seeds are its own private state, the class no longer a stateful lambda.
  3. **Split by concern.** `PlanMaker.program` into the target decision (shared with `plan`), a let's plan, a
     statement's plan, the stamp post-pass (pure), the staged drops; `ProductStatements.prepare` into inlining and
     resolving; `PlanRunner.program` into a pure shape of the plan, the run of its steps, its transaction bracket,
     its answer's stream (the pre-scan's deliberate differences from `sequence()` kept and named).
  4. **The expert defects of that code:** imports for the inline fully-qualified names; dead code (the read counts,
     `ProgramState.door`, constant guards); orphan and stale comments; duplicated logic made one (the target decision,
     the declared parameters, the rename merge); the nullable-heavy signatures where a value says it better.
  5. **The four latent defects the inventory names** (the eager run and the CTE planned without the frame's renames,
     CTE bodies attached after the renames, a stamp an eager run's own inline read mints, a seed key's `establish`
     clearing another's dirt) are verified one by one AFTER the zero-movement sub-steps; any that is real is a
     behaviour change, posted before it lands.
  Sub-steps, each its own commit, each checked byte-identical before it: MRa imports, dead code, comments; MRb the
  runner's program loop; MRc the program state and `PlanMaker` split; MRd a statement's facts as a value and
  `ProductStatements.prepare` split; MRe the latent defects verified; MRf the chain, the audit, the records, the
  checkpoint.
- **B13, the system database on one code path (M4; written 2026-11-01 under R15, its ruling R9; drafted from a read of
  the code at bb9de6ce).** Today a product query that reads the system metamodel store (`Class.all()`, the metamodel
  tables) carries the tables it reads in its own plan (`PlanMaker.metamodel`: setup steps on the plan's own target
  in process, the session's temporary tables on a server, the rows inline on a database with no temporary table),
  while a test body is routed by the driver to the graph's separate system database (`driver/SystemDatabase`,
  `planner/SystemRoutes`, `BodyDriver.openRoute`). R9: one code path, the graph's separate database kept.
  1. **The plan names the system database.** `ExecutionPlan.Database.System(type)`: the graph's system database on
     the engine the runtime executes on. A statement that reads the system store has that target, its setup the
     graph's whole system store (every table, its DDL and rows: `Ddl.metamodelSteps` in process,
     `Ddl.metamodelTemporary` on a server, the one owner `SystemDatabase.seed` builds today), its table names the
     session's (`SystemRoutes.renames`). A statement reading the system store and a user store stays refused by name.
     `PlanJson` writes the new database (25) and the TypeScript runner refuses it by name.
  2. **The runner opens it once per graph.** `PlanSessions` opens a `System` target from the target's content (the
     graph's rows are its key, as a platform database is kept today): in process a private database seeded once and
     kept while it is used; on a server the host's system connection for the engine (`ExecuteOptions
     .systemConnections`, handed to the source; none configured is refused by name, never a user's session). The
     caller's own-session source (`PlanSessions.of`, the driver's) routes a `System` target the same way.
  3. **Both roads plan it.** The product (`PlanMaker.plan`, the program road) and the test road (the side and value
     plans: `SidePlanner`, `JudgePlans`) name the target; `Metamodel`'s per-plan carriage, `SystemRoutes`' route and
     `BodyDriver.openRoute` and `driver/SystemDatabase` go.
  4. **A database with no temporary table** (no system database is possible on it): the rows stay inline in the
     statement, as today, its one named exception -- measured first, kept or refused by the count.
  Movement (MV11): a product metamodel query's statements move from the user's database to the system database (its
  setup no longer on the user's session); the test road's system statements move from the driver's route to the
  runner's (the same database, the same SQL). Measured by every core package's statements, the corpora's and channel
  B's statement-origins census, and the PCT lanes at the checkpoint.
  Sub-steps: M4a measure (which product tests read the system store, on which engines, with and without temporary
  tables); M4b the plan's target and the runner; M4c the product moved; M4d the test road moved, the driver's route
  deleted; M4e audit, records, checkpoint.

## 7. The steps (◆ = a checkpoint: the branch pushed, CI on every OS, the lane sessions, the movement register to the user)

| Step | What | Size |
|---|---|---|
| **M0** ◆ | Measure, no product change. Today's lowering-time guard errors classified compile-time or run-time (R14). The parity census BUILT (the parked probe in `docs/test-is-product-homework/probes/` re-lowers already-resolved bodies only, audit W4: the census plans each statement both ways through a statement entry, front half included); every MV class counted with an example; whether any statement's compiled shape depends on the test-data generator's rows (T8); the referee's MATCH/DIVERGED/DECLINED counts; capped host sides; seeds and errors inside groups; planning failures after a body's first failing statement; constant-only bodies; effectful maps and computed strings; frame CTE reads; D27, D30; the census baseline | 3-5 days |
| **M1** ◆ | One id counter per program (B9); every test's ids checked | 2-3 days |
| **M2** | Product features, the safe set: D4, D6, D9, D10, D13, D14, D16, D17/R18, D26 + constant-only programs, D29, D30 | 1-1.5 weeks |
| **M3** ◆ | Product features that need designs: effects + scripts + the transaction rule + F11 (B2), `Setup` seeds (B2), frames and the eager run (B10), the query door's refusals (B6) | 1.5-2 weeks |
| **MR** ◆ | Mutable state out, split by concern, the expert defects fixed (R20, B14): zero movement | 3-5 days |
| **M4** ◆ | The system database, one code path (B7); after M5 (R19) | 3-5 days |
| **M5** ◆ | The body router at the test door (B1, R16); the first class moves whole: assert-free programs (the corpus setup programs, assert-free bodies), each planned whole by the program road and run as one plan; the 2.A convergences that class needs (MV6, MV7 where its frames are eager) | 1-1.5 weeks |
| **M6** ◆ | The verdict record (B3), the assert module in the test-planning library (B4, begun at M3; B5), the host judge on product sides; verdict steps planned on the program road | 1.5-2 weeks |
| **M7** ◆ | (The row before M6's revalidation; superseded by the row below.) The fused group composed at planning (from `VerdictBatch`'s flush); the second class moves: bodies whose asserts all have verdict steps (2.D's rule, MV1, for the bodies moved) | 1 week |
| **M9** ◆ | **BUILT 2026-11-01 (D109; §9 "M9a" to "M9d", §10): the frame programs under the host judge, 608 -> 1,080 bodies moved per DuckDB host pass, outcomes identical; its residual kinds (the engine-text asserts, the system-store reads, the test-data generator, `assertError` -- no corpus body --, staged grids, computed effects, effects through helpers, graph frames, seeded frames) named with their counts for M10a-d. Before M7 and M8 since M6's revalidation (the user, 2026-11-01: "the order is fine";** `docs/test-is-product-homework/REVALIDATION_2026_11_01.md`**).** Frames read by asserts (the corpus's bulk: `let r = execute(...)`, its reads spliced into the asserts' sides, the eager run and the CTE as M3f/M3g planned them); the kinds that run before a later statement is written, as Keyed steps (T8, T9, the map over `executeInDb`); `assertError`'s `ExpectFailure` (T10); staged raw grids (T12); inline `execute(...)`; the bodies needing them move, under the host judge first (zero output change expected) | 1-1.5 weeks |
| **M7** ◆ | **BUILT 2026-11-01 (D110; §9 "M7", §10): the database judge's assert and frame programs run whole -- the fused statement composed at planning by the batch's own planning half, with the frames' CTEs at its head; 604 -> 1,026 bodies moved per DuckDB database pass (604 -> 961 on H2), outcomes identical; channel B (BOTH) walks every assert body until M8.** The fused group composed at planning (from `VerdictBatch`'s flush), with the frames' CTEs at its head; the database judge's and channel B's assert programs move (2.D's rule, MV1, for the bodies moved) | 1 week |
| **M8** ◆ | **M8a BUILT 2026-11-01 (D111; §9 "M8a", §10): the host judgment as data, zero movement. M8b BUILT 2026-11-01 (D112; §9 "M8b", §10): the database verdict row self-contained -- holds and message in the statement, the plan's verdict without its polarity (PlanJson 25), zero movement. M8c OPEN 2026-11-01 -- a design question posted to the user (`docs/test-is-product-homework/M8C_DESIGN_QUESTION.md`): a canon over the product columns has no finite static form for a RECURSIVE polymorphic key slot; M10a proceeds meanwhile (R23).** Then: the database judge self-contained, arm by arm (MV2, MV3: polarity and message in the statement, so the plan carries the assert's SQL and nothing about the assert), then the canon over the product columns (B8, MV4); the bodies those arms gate move. B3 is struck (2026-11-01, the user: an assert is a regular function the database judge lowers; the plan needs no assert metadata; the host judge is the harness, §6 B3) | 2 weeks |
| **M10** ◆ | **M10a BUILT 2026-11-01 (D113; §9 "M10a", §10): the system database on the test road (the narrow M4): a step's target names the graph's system database (`Database.System`, PlanJson 26), the class rule `systemRouted`, the router opens that database once per graph and plans a routed body's program and sides on it; 1,165 / 1,111 bodies moved per DuckDB pass (host / database; from 1,080 / 1,026), 1,165 / 1,045 on H2, outcomes identical, every register test green; the server engines, the frame programs over the system store, the bodies mixing a user read with a system read and the CSV census carrier's stay (§9).** **M10b BUILT 2026-11-02 (D114; §9 "M10b", §10): the engine-text asserts on every lane -- the two texts a verdict step's constant sides, a byte-equal text the verdict (the 2026-09-21 ruling kept), the rows leg the harness's appeal at the step on a failed text; the three root arms and the producer shapes planned by the arm the walk runs; DuckDB moved 2,690 / 2,122 per pass (host / database; from 1,165 / 1,111), H2 2,690 / 2,055; outcomes identical, the referee census identical, every register test green; the generator carrier a kind of its own (T8 stays residue).** **M10c IN PROGRESS 2026-11-02 (§9 "M10c"): item 1a built -- the database planner's static scan reads a root engine-text assert's arguments alone (the engine's `assertEqualsH2Compatible` helper's `executeInDb` read, which the program road never runs); DuckDB database pass 2,122 -> 2,398, H2 2,055 -> 2,331, outcomes identical; item 2 built -- the grid pair and the rendered text under the database judge (the arm's `gridPair`: the SQL grid pair over two declared grids, a multiset unless the actual side is sorted; the gate admits declared grids); DuckDB database pass 2,398 -> 2,662, H2 2,331 -> 2,595, outcomes identical; item 3 built -- the lineage lines (the scan a system read, the print planned as the lines the walk rewrites it to); DuckDB host pass 2,690 -> 2,744, database 2,662 -> 2,716, H2 2,744 / 2,649, outcomes identical.** Split by M6's revalidation: **M10a** the narrow M4 on the test road alone (a step's target names the graph's system database, seeded once per graph; the test library plans it, the runner opens it; no product form changes -- the user, 2026-11-01: "fine for now but needs a product follow up", §9); **M10b** the engine-text asserts: the test in the plan on every lane (the body's statements, the query's rows a side, our text against the golden a constant), the appeal a Java harness outside the plan (the H2 referee replay and the row compare, R6/T13), no lane special-cased (the user, 2026-11-01); **M10c** the remaining host shapes `planHost` refuses (the quantified forms, the cell pool, the lineage lines, `assertIs` over non-elements, the grid pair and the class-instance `assertEq` under the database judge); **M10d** the last class moves; the walk (`BodyCompiler`, `Segments`, the driver's plan-run interleave), the router and `BodyClass` deleted; a planning failure a compile error (R14, MV8); the driver sends nothing and plans nothing (it may read the typed tree to compare rows: the harness); the parity guard's permanent form; the layer file | 2 weeks |
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

- **A test body's answer facts, and the product's no-value answer -- RULED 2026-11-01 (R21), upstream asked
  (`core/src/test/resources/upstream-api/m5-answers.json`): item 1 kept (the engine has no `print`; register S128);
  item 2 done (a one-value answer `[1..1]`, Pure's, on both roads; a no-value one no row); item 3 done (the product's
  value through a class's chain `value:String[0..*]`). As posted:
  1. *The product's answer for a query ending on `print`.* The expression `|print('x', 1)`, run as a product query
     (`Execution.run`, the JSON rows writer). Today: `[{"value":null}]` (one row, a null cell: the no-value
     constant of M2's R18). The test road answers no row (the walk's `TypedSide.Scalar(null)`, Pure's `[]`, which
     `print`'s `Nothing[0]` returns); after, if ruled: `[]`. R18 said its value is "as the test road's"; M2's
     constant did not match it.
  2. *A test body's literal answer.* The expression `{|...; true}` (a test body ending on a literal) answered to a
     caller's sink (`Execution.runProgram`). Today, on both roads since M5: a `[0..1]` Boolean column, one row (the
     walk's facts, kept for moved bodies by `ProductStatements.asTheWalkAnswers`). The product answers `|true` with a
     `[1..1]` column (`true` is `Boolean[1]`). After, if ruled (MV6, D27's convergence): moved bodies take the
     product's `[1..1]`. Visible only in an answer's column facts (Arrow's `legend.multiplicity`); no test, corpus or
     channel B outcome reads them (measured at M5).
  3. *A product value through a class's chain (found by M5f's audit).* The expression `|my::P.all()->map(p|$p.name)`
     (three rows), run as a product query. Today the product answers the column `u_map__name:String[1..1]` with three
     rows -- the map binder's cell, not the value's -- and the service runner's writer (`TdsObjectsWriter`) refuses it
     ("a to-one value answered more than one row"); the test road answers `value:String[0..*]`. After, if ruled:
     `PlanMaker.typed` names the value column `value` with the root's multiplicity (`ResultShape.valueInfo`), as the
     test road does. Until then such bodies stay on the walk.
- **A moved body that fails to plan fails before any statement runs (the M5 audit's S6) -- POSTED, inherent in R16.**
  R16: "a body of a moved class the test door refuses fails (R14)". The walk ran a failing body's statements up to the
  failing one (its seeds, value statements and closed scripts); a moved body runs none, so the database and the ledger
  a later body on the same session finds can differ. The outcome and message are the same. Measured: 0 such bodies in
  core, both corpora and channel B. MV8 (M10) generalizes it to every class.
- **M6's residue -- RECORDED, not ruled on (R23).** B4: the assert planning (`AssertVerdicts.planHost`,
  `HostJudge.planX`, `HostSides.plan`, `driver/HostAsserts`) lives in the driver, not the test-planning library (M10d).
  B5: the census is counted by the seam at the verdict step, not by the runner per step kind. N1:
  `assertTdsEquivalent`'s tolerances are read in the grids' verdict group (the walk read them after the grids, in a
  group of their own): identical with literal tolerances; a tolerance on another session is now refused ("two
  sessions"). N3: `SqlTextVerdicts.lookThrough` decides "no producer" by catching an inliner failure (R16's letter;
  the walk's own gate, older than M6; goes with it at M10d). N4: a moved body opens one session for the program where
  the walk opened one per assert (identical under the harnesses' autocommit sessions; a keyed side's snapshot
  brackets the program). N7: a guard hole older than M6 -- `CodeShapeGuardrailTest`'s `SIG` pattern skips a method
  whose return type is annotated, so `METHOD_LIMIT` never scans `AssertVerdicts.adjudicate` (about 300 lines) nor
  `planHost`. A let whose handle registration raises with no assert after it fails the moved body at planning (the M5
  audit's S6). The shapes `planHost` does not plan (engine text, quantified, cell pool, lineage, a computed instance
  type) keep their bodies walked; the DATABASE and BOTH judges' bodies move at M7. B3's residue (the judgment a closure
  reading the model at judge time) is no longer residue: B3 is struck (§6).
- **The system database: a product follow-up (the user, 2026-11-01, on M4 narrow: "fine for now but needs a product
  follow up").** The test road keeps its separate system database, seeded once per graph (M10a). Whether the PRODUCT
  moves from embedding the metamodel tables in each query's plan (`PlanMaker.metamodel`) to the seeded database is a
  product decision by measurement on the real projects -- plan size and latency of embedded-metamodel plans against
  a server-side system connection and Trino's lack of temporary tables; upstream has no SQL form of the metamodel
  (`Mapping.all()` is in-memory Pure). Outside this program.
- **M9a's residue -- RECORDED (the independent audit of the frame programs, 2026-11-01; no blockers, its S1-S4
  folded).** (1) The frame's eager run: the walk materializes its rows, shapes them and holds the to-one reader's
  contract (`let r = execute(|<relation>->toOne(), ...)` raises there when the rows are not one); the program road's
  check drops the rows. (2) The eager run is lowered by the product (`ProductStatements.lower`, MV6) where the walk
  lowers it its own way (`BodyPlanner.lowerAndPrepare`): measured byte-identical for the relation-rooted and the
  class-rooted frames the parity test holds; the corpora measure the rest at M9's chain. (3) The activity comment: the
  walk registers the run's trace comment, the pre-planned frame none; a read of a frame's `activities` is excluded by
  name (through a user call too: `StatementEffects.reaches` compiles the callee); the check's stamp is always unread
  and stripped. (4) A frame's OWN runtime argument's declared test data is seeded by neither road (the seeds are a
  `from()`'s), pinned equal. (5) A frame whose query reads an earlier frame: the walk resolves the run chain with no
  splice and fails, the program road splices it -- refused by name in the rule until a later sub-step. (6) A frame
  whose chain cannot be built fails the moved body at planning (the M5 audit's S6): the pre-planned raise never fires
  for frames, since the test door plans the same chain uncaught. (7) The eager run's origin: the walk `enter(LET)`, the
  program road `enterIfUnmarked(LET)` -- equal under every harness (unmarked or STATEMENT), different under a strong
  mark. (8) `TestDoor.Planned.frameRuns` is keyed by the step's place, relying on `withoutUnreadStamps` mapping one to
  one and `finished` only appending (a check that the place holds a `Check`). (9) `BodyClass` learns a frame's
  relation-rootedness by building its chain (`FramePlanner.chain`), the planner's own fact -- replaced at M9b's audit
  (F5) by `FramePlanner.relationRooted`, the query lambda's typed root read with no chain built and no raise (a
  shape the build refuses is left to the walk by name; the chain is built twice per frame, by the host asserts for the
  walk's own frame and by the test door for the plan).
- **M9b's residue -- RECORDED (the independent audit of the widened frame class, 2026-11-01; two should-fix folded:
  F1 one reach memo per predicate -- a memo shared by two predicates blinded the second to every callee the first had
  walked, so a frame's query through a helper reaching a graph, an object or an `execute` was admitted; F2 a frame's
  query reading a frame through a VALUE let tainted by one, now refused by name as the by-name read is (N1); F4 the
  duplicate imports; F5 above; F3's parity cases added).** (1) `readsActivities` refuses an inline
  `execute(...).activities` too, though both roads register identical null-comment rows. (2)
  `inlineExecutesAsTheirQueries` rewrites the statement's own tree only: a helper that wraps `execute(...)` (the
  corpus's `executeInternal` idiom) is refused by `plannedAnotherWay` and stays on the walk until a sub-step inlines
  callees before the scan. (3) `plansNoStep` admits `print(<anything>)`: a `print` of a frame read or an inline
  execute is inert on both roads where Pure would run it (S128's class, M6). (4) The host asserts' frames carry a
  null activity comment; the eager run's origin under a strong mark (M9a's (3), (7)). (5) The effect program's
  `onlyConnectionReads` guard is not needed by the frame class (effect lets refused outright); a sub-step admitting
  effect lets must carry it. (6) The kind census (`testplan/BodyKinds`, `Census.STAYED_KINDS`, the corpus's
  `[corpus2] body-router stayed` histogram) is a measurement carrier in the test-planning library, deciding nothing;
  it goes with the router at M10.
- **M9c -- activities reads, the side-kind rule, the kind census (2026-11-01; its independent audit folded: F1 the
  census list registered, F2 a store write refused in a side and a body it reaches walked, F3 a pending let with a
  frame or an effect after it walked, F4 the query peel non-raising for the if without a taken branch and the
  concatenate over a non-literal collection (`ExecuteChainAssembly.queryLambda`; `prepare` spells each refusal), F5
  the census's frame-query flag, F6 the splice hook on the body's environment as the walk's, F7-F8 parity cases).**
  (1) A frame's activities (`$r.activities`: the run's SQL and its trace comment) read by a moved body: the SQL a
  planning-time render on both roads; the comment the eager run's trace stamp -- the walk's frame holds it as a
  LITERAL in the rows it registers after the run, the pre-planned frame as a PARAMETER (`#stamp.let<i>`, minted by
  `HostAsserts`, adopted by the test door for the frame's check step through `TestDoor.Asserts.frameStamp`, bound by
  the runner when that step runs: R17 C4), so a side reading activities differs in text at that one cell by design
  (`BodyRouterParityTest.assertParity` compares statements with the two spellings normalized -- every `?` of a side,
  so another bound parameter's text difference would be masked, the count still compared;
  `theActivityCommentIsTheRunsStamp` pins both roads' comment as the run's and the moved side's as a parameter never
  spelled). `PlanEnv.stamps`: the third run-scoped accumulator, bound by `BodyPlanner.lowererFor` as typed String
  plan parameters. (2) The side-kind rule (`BodyClass.sideKind`): an assert statement or a value let of the ASSERT and
  FRAME classes is refused for a system-store read (M10a: the system session), a store write (run where its side is
  planned: the walk inside the body's transaction, the program road before the program) and the eager inline execute
  alone; every other kind (an object, a graph, a temporal read, a class reference, an execute inside a helper) is
  admitted, since the sides are the walk's plans on both roads and a value let sends nothing on either; the frame's
  QUERY keeps the another-way test (its eager run is the product's lowering), a temporal class read excepted since
  M9d. (3) An element reference is a system-store read by the resolver's own criterion (`systemElementReference`: a
  bare tracked class the system mapping binds); a class reference (`instanceOf`'s type argument, the corpus's `sql()`
  helper) is a value -- `element-ref` retired as a kind. (4) The kind census: `testplan/BodyKinds` +
  `Census.STAYED_KINDS` + the corpus's `[corpus2] body-router stayed` histogram over passing tests and
  `corpus2-body-stayed.tsv`; it reads each statement where the rule reads it; measurement only, goes with the router
  at M10. Residue: (a) a helper-wrapped execute bound by a let (`let r = execHelper()`) is a VALUE let both roads
  splice lazily where upstream runs it at the let -- the walk's standing divergence, now the program road's too
  (M9b's residue (2) obsolete; a register row is the user's call); (b) an inline `execute(...).activities` comment is
  `""` on both roads under the host judge where upstream records the run's (M9b's residue (1) obsolete as a
  difference between the roads); (c) rule-time raises older than M9 whose timing differs from the walk's
  statement-turn raise (`EffectPlanner.storeOf` for a let-bound database argument, `BodyPlanner.handleType` for a
  connection-typed value carrying an effect, the callee compile in `StatementEffects.reaches` catching the typer's
  exception alone); (d) a frame's query reading a handle let's rows: the program's eager run is planned on an
  environment whose plan rows hold no handle rows, the walk's does -- no corpus case; (e) the census tags
  `frame-reads-frame` by name only; (f) `ChainNormalizer` holds a second element-identity criterion (a `Class<X>`
  reference against a class-typed row read), reachable only through a system-class row read `sideKind` refuses.
- **M9d -- handles, from() contexts, the session guard, temporal frames (2026-11-01).** (1) The rule reads a frame's
  and a HANDLE's query (`executionPlan(...)`, the lineage scans) and a `from()`'s source, never their mapping and
  runtime references (`framesAndHandlesAsTheirQueries`): the corpus's `let plan = executionPlan($q, Mapping, ...)`
  and `let tds = execute(...).values->toOne()` idioms, refused as system reads of their mapping argument, move. (2) A
  side the walk's planner routes to another session than the body's (a lineage scan's rows join the metamodel tables
  through `column.owner`; measured: five `scanColumns` tests failed moved, "schema metamodel does not exist") keeps
  the body on the walk, decided by the planned route (`HostSides.Planned.on()`) before any run (`HostAsserts.plan`);
  a moved body's program opens the body's session alone until M10a. (3) A frame over a temporal class is admitted
  (`frameQueryKind`): its eager run byte-identical on the walk and the program road for the business-temporal frame
  the parity test pins on every in-process database (the walk's exists-join form is the product's since D5); the
  corpora measure the rest at M9's chain.
- **M7 -- the database judge's assert and frame programs move (2026-11-01; its independent audit folded: F1 a STATIC
  pass over every let and assert before any batch opens, so a body left for the walk has counted nothing (the arms
  count the assert entered and its family as they plan); F2 the planned batch's one-shot latch allowlisted; F3 a frame
  environment that raises before an arm and F4 a let that raises leave the body to the walk, which raises there
  before anything is flushed; F5 planning stops at a WALL -- the segment closes at the wall's assert, every later assert
  absorbed unplanned, the wall's exit counted where it is planned; N6 the walk's fragment map handed to the batch so a
  split rung's reason names the frame's let).**
  (1) `VerdictBatch.flush` split into `plan()` (the groups per session composed as the walk composes them: the fused
  statement over the batch's frames, the dialect's split rung at planning member by member, the keyed members a
  group of their own) and `Planned.judge()` (the root loop: a pending step's row, or `executeOne` at its place where
  absent -- the split rung's wrapper, lazily, under FALLBACK; a resolved step's failure; the listener; the first
  failure raising); the walk's flush is plan, run, judge, unchanged in behaviour; the flush census counted when the
  groups start to run, so a batch planned and left for the walk counts nothing. (2) `AssertVerdicts.planDatabase`: the
  walk's `batched` protocol under a COLLECTING batch with no flush -- a wall a step raised at the root's turn, told to
  no listener (`VerdictBatch.wall`) -- gated statically by `databasePlansWithoutReading`: the host checks'
  classification (`planHost` null: the shapes neither judge plans statically), then the three shapes whose database
  arm reads a side on the host at the assert (the grid pair of two relation-typed sides, D3; the rendered-text pair;
  `assertEq` over a class-kind side, D43) -- the walk's. (3) `driver/DatabaseAsserts` (the twin of `HostAsserts`): the
  body walked under the collecting batch; a frame let built as the walk builds it under a batch (never run, its CTE
  defined when a reader splices it, its activity comment the SEGMENT's fused statement's stamp `#stamp.fused<k>`, a
  parameter that fused step binds: the walk's reserved comment); a segment closes where the walk flushes (an effect
  statement, the body's end) and its groups are placed at the segment's last assert (the asserts before it absorbed:
  `TestDoor.Asserts.at` answers a list, empty for an absorbed assert); refused before planning: a raw grid read, a
  lineage scan; refused after: a group on another session than the body's, a keyed member, a segment the dialect
  spells no member of, a frame whose activity stamp a later segment reads. (4) `TestKinds.frame` places no eager run
  under the database judge (`Asserts.runsFrames()` false), adopting the stamp. (5) `BodyRouter`: the planner by judge
  mode; the sink hands each row to its segment and judges at the segment's last verdict; the fused statement under
  BODY. (6) `BodyClass.of(body, runtime, oneJudge)`: the assert and frame classes for one judge, host or database;
  channel B (BOTH) walks every assert body until M8. Measured on the DuckDB corpus: the database pass `body-router
  moved` 604 -> 1,026, outcomes identical (2473 / 108); the host pass unchanged (1,080; 2471 / 108). The kind census
  of what still walks on the database pass: 1,131 engine-text asserts over activities, 392 system-store reads, 181
  temporal frames with engine-text asserts, 102 other shapes the database arms do not plan statically, 29
  database-unplanned frame bodies, system reads mixed with effects or frames, about 20 effect bodies; on H2 the
  database pass 604 -> 961, `fallback=18` and every count the baseline's. Residue: (a) the census the arms count at
  planning (the assert entered, its family) is counted on the program road before the program runs and on the walk at
  the assert's turn -- the same count per test; the static pass refuses before any count every shape the corpus
  shows, and the checks after planning (a group on another session, a keyed member the pre-scan missed -- none: the
  grid report is minted for raw grids alone --, a segment the dialect spells no member of, an unbound frame stamp the
  static pass missed through a let) would count it twice on a lane whose dialect refuses a canon; (b) a fused group's
  verdicts carry no wrapper, so the split rung's wrapper runs through `executeOne` from the sink's judge, a statement
  outside the body's plan as on the walk (M8 may carry the wrapper and let the runner's loop stop at the sink's
  first failure); (c) the frame's CTE is defined on the batch at splice, so a frame read in two segments is defined in
  both fused statements, as on the walk; (d) the rendered-text pair is gated to the walk though the database arm
  plans it without reading (conservative; admit at M8); (e) an inline `execute(...)` inside a side registers its
  activity comment as the walk's reserved literal, where the program road's fused statement is sent under a comment
  minted on a trace-less session -- unobservable from Pure, a difference in the statement's comment alone; (f) the
  router runs a moved body's program on `PlanSessions.of(session)` with no trace, so the body environment's
  `ExecutionTrace` sees no moved body's stamps; (g) both roads run a fused statement after an effect bare (no
  program transaction), the Postgres savepoint path unchanged and not parity-pinned (the parity backends are the
  in-process ones).
- **M8a -- the host judgment as data (2026-11-01; its independent audit folded: S1 the parity test pins the keyed
  bodies' outcomes ABSOLUTELY beside the parity, since both roads run the same judgment; S3 a key slot whose type is an
  unsubstituted type variable counts as Any's; N1 `HostJudgment` pinned in the Java-evaluation ledger; N5 a stale
  javadoc; the parity cases its E named; S2, N2-N4 recorded below).** Each host arm's `planX` builds one
  `driver/HostJudgment` record -- the comparison's name and the few facts it needs beyond the rows, every one read from
  the typed tree and the model when the arm is PLANNED: the side facts (type, class, kind class, Any and Nil stamps, map
  carrier, type name), the key trees (`Keys`: each side's tree resolved exactly where the walk resolved it -- the
  expected side's where the classes agree or it is the narrower, the actual side's where it is the narrower -- the two
  subtype facts, and the keyed classes a value may carry as its own in a slot that declares no keyed class), the byte
  gates (shared keys; the keyless-constructor scan under the same static conditions the byte verdict asked it), the
  flat facts (the bare-row and wrapped-relation stamps), the order views (`GridPair.sortedOrder`, `Json.incidentalRoot`,
  `Rendered.sortedOrder`), `Size.envelopeValuesRead`, `InstanceOf.acceptedTypes` (the type and its subtree: the
  knowledge layer's downward walk of the same explicit generalizations `isSubtype` walks upward), `TdsEquivalent
  .hasTimeDelta`, `Decided` (a statically identified `assertIs`: held, or its failure's message), `Raise` (a planned
  raise at the assert's turn). `HostJudge.judge` is the one dispatch, a switch over the sealed records with no default;
  each `xOf` takes the record and the rows and nothing else; `HostSides.Check(asks, judgment)` holds data, the closure
  interface deleted; `AssertVerdicts.instanceKeys` and `structuredSide` left for the records. **The key slots' own
  classes, found by the corpus and not by the quick suites (two runs):** `compiler/element/EqualityKeys.Key` gained
  `declaredClass` (the class a key property is declared as, which `collect` already read; Any for an unsubstituted type
  variable), and `Keys.of` collects, for a slot that declares no keyed class, the keyed classes among the declared class
  and its subtree (`ctx.subtree`), the whole `resolutionUniverse()` for Any, transitively through the classes found --
  resolving ONLY a class that DECLARES an equality key (`EqualityKeys.declaresKeys`, read off the parsed definitions
  with the class compiler's one stereotype test, `isKeyStereotype`), so no class the body never touched is compiled.
  The first cut read `ctx.subtree(Any)`: the knowledge layer's index holds explicit generalizations alone, so every
  class with no `extends` was missing and 19 host-pass tests failed (`toPostgresModel`, `debugPrint`: nested `Node[*]`
  values kept `__id`, `__type`, `__canon`); the second cut resolved every class of the declared subtree and compiled a
  class the tolerant corpus model holds poisoned (`ResultType` unknown), 10 tests raising at planning. Measured on the
  DuckDB corpus after the third: both passes M7's to the count (host 2471 / 108, `other=6219`, `moved=1080`; database
  2473 / 108, `moved=1026`). Residue, recorded: (a) `HostAsserts.Planned` still holds the asserts' `Check.asks` (the
  typed arguments and the splice hook the sides were PLANNED from, dead once planned) and `JudgePlans.PlannedSide.root`
  (a typed-tree read at judging in `toOneReader`): the sink's remaining compiler objects, dropped with the walk's run
  forms at M10 (the audit's D, N3); (b) `SideFacts` carries the side's `ExprType` because `Equality.Typed.all` and
  `PureAsserts.assertEquals` take `Type`s, and `sideCanonOf` constructs a lowering `CanonRider` at judging ("dead by
  construction"): data-not-objects residue (N2); (c) `Keys.of` walks the whole `resolutionUniverse()` per assert whose
  key slot is declared Any (a fresh set per call; no cost measured on the corpus: 171s a pass either way) -- a
  per-context memo of the keyed classes is the right scope when one is needed (S2); (d) `InstanceOf` with a null type
  name declines AFTER reading its side, as the walk's arm did (parity); (e) **a register question for the user (N4,
  pre-existing, not M8a's):** no layer makes Any an implicit generalization, so `isSubtype(x::Firm, Any)` is false by
  both relations and `assertInstanceOf(^x::Firm(name='a'), Any)` fails on every road where Pure holds -- a
  `docs/SEMANTICS_REGISTER.md` row to rule on, or the typer's and the judge's Any made one; (f) **a second register
  question (pre-existing, found writing the audit's parity cases):** `assertInstanceOf(^x::Lit(value=1), x::Lit)` and
  the same against `x::Node` FAIL on both roads today ("expected ... to be an instance of Lit, actual: LinkedHashMap":
  a CONSTRUCTED instance's wire is judged by the pure rule over a map, not by its classifier, where a store-read
  instance carries `__type`, batch 53) where Pure holds them; the parity test keeps the three bodies parity-only until
  the user rules. (g) The parity test's absolute pins hold on the platform backend alone: H2 refuses the keyed
  constructions themselves (`LIST_GET` with no list encoding), the same refusal on both roads.
- **M8b -- the database verdict row self-contained (2026-11-01; its independent audit found no blocker and asked no
  code change: its should-fix 2 folded -- the failing negated and polarity-folded families as parity witnesses under
  the database judge; its nits 3-6 folded -- two javadocs, the dead outputs list, a once-only guard; its nits 7-8
  recorded).** The user's
  ruling (§6 "B3 as settled", design 1): an assert under the database judge is one lowered statement answering holds /
  message / unjudged, and the plan carries nothing about the assert. `VerdictSql.selfContained(row, name, wantEqual)`
  gives every verdict row two more columns, composed IN the statement: `__holds`, the verdict under the assert's
  polarity (`__verdict`, or `NOT __verdict` for the negated families), and `__message`, the failure text the judge
  raises with -- the family's name, `expected:` and `actual:` each on its line over the two canons as text (a null
  canon the word `null`), or the negated families' one sentence -- spelled byte for byte as `DatabaseJudge.verdictOf`
  spelled it in Java from the row. The batch wraps every row once at `defer` (the fused members, the keyed members and
  the split rung's lazy wrapper alike: `VerdictBatch.SelfContained` beside `Fusion`, both the lowering's, handed by the
  body driver); `verdictOf(name, row)` reads unjudged, then holds and message, and knows no polarity;
  `ExecutionPlan.Verdict` drops `wantEqual` (MV2: the plan's verdict carries the listener's name and the census arm,
  nothing else about the assert; PlanJson 24 -> 25); the host judgment records carry their own polarity since M8a.
  Zero movement by design; measured on the DuckDB corpus: both passes M8a's to the count (host 2471 / 108,
  `other=6219`, `moved=1080`; database 2473 / 108, `moved=1026`), every register test green -- so the failure messages
  the registers witness are byte-identical; H2: the database pass 2233 / 353, `fallback=18`, `moved=961`, the host pass 2289 / 295, `moved=1080`, the registers green -- every count M7's. Residue: (a) the unjudged DETAIL (the two excerpts) is
  still Java-formatted (`AssertVerdicts.excerpt`): diagnostics for the register, not a verdict; (b) the leniency flag
  is still read in Java for the census (`sqlUlpPolicy`); (c) `name` and `arm` stay in the plan's verdict (the listener
  and the census); (d) the message is computed where the verdict holds too (unread); (e) every family's two canon
  columns were text already (the tolerance's numbers and the counts cast by the database before M8b), so the message
  pastes the text Java pasted -- the audit read every builder and every dialect's `CONCAT`, `COALESCE`, `NOT` and
  string literal (BigQuery escapes the newline, every other dialect writes it raw; each engine reads it back as one);
  (f) the verdict statements' text changes on every lane (two more columns): the core ids dumps move for the database
  judge's statements at the chain; (g) `ProgramRunnerTest`'s fixture rows still carry five verdict columns (the
  runner's own plumbing, judged by nothing); `VerdictSql.renderedText` has no caller in main (pre-existing: the
  rendered road is host-judged).
- **M8c -- the canon over the product columns: OPEN, a design question posted (2026-11-01;
  `docs/test-is-product-homework/M8C_DESIGN_QUESTION.md`).** The identity lane's `__canon` is filled at each
  CONSTRUCTION site bottom-up, which is what makes a RECURSIVE polymorphic key slot (`DynaFunction.parameters :
  RelationalOperationElement[*]`, the `debugPrint`/`toPostgresModel` trees) finite; a canon computed in the verdict
  wrap over the product's columns (`__id`, `__type`, the declared properties) can branch on `__type` over a slot's
  declared subtree for every non-recursive shape but has no finite static form for a recursive one (SQL has no recursive
  scalar expression). Options for the user: (a) keep the construction canon for recursive classes alone; (b) a depth
  bound from the statement's own construction tree; (c) a dialect-specific recursive form; or withdraw MV4. Until the
  ruling the identity lane is unchanged; M10a proceeds (R23). 2026-11-02: the user asked whether the depth bound (b) is a
  device to pass a test and whether `__id` should be the one equality for every object; the session's answer, posted: (b)
  is withdrawn as a device (sound for literal trees alone, the construction canon's own domain, no coverage gained); the
  construction-site canon is the GENERAL mechanism -- Pure's key equality is structural recursion over the value, done
  once where the value is built -- so the proposal is to carry the object's one `equal()` value on every lane as `__id`
  and `__type` are carried (`InstanceEquality`'s value: a keyed class's key canon, a keyless class's `__id`), MV4
  restated as one equality value per object and one layout on both lanes; `__id` alone cannot be the equality of a
  keyed class without diverging from legend-pure's `equality.Key` rule (`docs/OBJECT_IDENTITY_2026_10_20.md` §1).
  Awaiting the ruling.
- **M10a -- the system database on the test road (the narrow M4; 2026-11-01; its independent audit: COMMIT AFTER FOLDS,
  folded; `docs/test-is-product-homework/M10A_BRIEF.md`).** The user (2026-11-01, on M4 narrow: "fine for now but needs a
  product follow up"): a step's target names the graph's system database, seeded once per graph; the test library plans
  it, the runner opens it; the product unchanged. (1) `ExecutionPlan.Database.System(type)`: the graph's system database
  on an engine, named by type alone (PlanJson 25 -> 26, tag `system`); the runner knows no database: `Statements.check`
  compares the type, the sessions source opens it. (2) `BodyClass.systemRouted(body, runtime)`: some statement reads the
  system metamodel store (a system class's extent, the store's tables, a system function, a tracked element's row) and
  no statement reads a user store (`userStoreRead`: a mapped class's extent, a user store's table, a `from()`, an
  `execute` or a handle over a runtime, the test-data generator) -- each through the user functions it calls
  (`readsStore`: `StatementEffects.reaches`' walk, one memo per callee), a lambda literal's body left aside (a lambda is
  a value: the native that runs one against a store is itself the read) and the arguments of a planning-time native
  (`NativeFn.Handle`, `NativeFn.JavaRoutine`: a mapping handed to `executionPlan(...)` is consumed when the plan is
  made, never read as a row) -- on a runtime whose engine runs in process (`SystemRoutes.inProcess`: the system tables
  keep their names there). The frame, effect and store-write exclusions are the class's (`assertProgram`), not the
  predicate's; such an assert program's `SYSTEM_READ` sides are admitted (`sideKind(..., routed)`). (3) The router
  (`BodyRouter`) opens the graph's system database for a routed body before planning its asserts
  (`BodyDriver.systemRoute`, the walk's own `SystemDatabase.routeFor`: one session object per graph, seeded once; the
  route's renames checked against the plan's as `openRoute` checks them) and plans them on that session
  (`env.withSession`), so every side's statement names the System target (`JudgePlans.target`: an environment on the
  graph's system database, `SystemDatabase.engineOf`); the test door plans the program on the System target
  (`TestDoor.planned(..., systemRouted)`, `PlanMaker.testProgram(..., systemRouted)`); the sessions source opens the
  system session for that target (`PlanSessions.of(session, trace, system)`, a non-closing view; the shared and the
  own-session sources refuse a System target by name). **Measured:** DuckDB host 1,165 bodies moved per pass (from
  1,080), the database pass 1,111 (from 1,026); H2 host 1,165 (from 1,080), database 1,045 (from 961); outcomes
  identical (DuckDB 2471 / 108 and 2473 / 108, H2 2289 / 295 and 2233 / 353 with `fallback=18`), every register test green;
  the census: the printed histogram counts passing bodies: `assert,system-read` 390 (the host pass before M10a) -> 211 on the host pass and 213 on the database pass, each pass's 85 moved; 94 on each pass now counted `host-unplanned-assert,system-read` / `database-unplanned-assert,system-read` (engine-text asserts over system reads: the class admits them, the host checks do not plan them -- M10b's); 3 `assert,csv-census,system-read` (the census carrier); the 211 / 213 still `assert,system-read` read a user store beside the system read (the graph-fetch suites' shape: a query executed over the user store, its mapping and runtime let-bound elements) or hold a shape the assert class refuses regardless -- the per-body listing `corpus2-body-stayed.tsv` of each pass. **The class rule's cuts, caught by the corpus and the parity
  suite, not the quick suites (as M8a's):** the first cut counted a user class's extent INSIDE A LAMBDA LITERAL
  (`relationalMapperSqlQuery({|Product.all()}, mapping, ...)`: a lambda handed to a SQL-text generator, run nowhere) as
  a user read, so 40 of the 390 moved; the second (lambda-opaque) made the HANDLE bodies system-routed (`let p =
  executionPlan(...)`: a mapping and a runtime, tracked elements, as a native's arguments) and the test-data generator's
  bodies too (three DuckDB host regressions, `scalar lowering not yet implemented for TypedCsvCensus`; an H2 judge
  disagreement on `testGenerateNecessaryData`), so a planning-time native's arguments are left aside and the generator is
  a user read; the third moved the three CSV CENSUS tests (`let mapping = <element>; let d =
  getRelationalCSVDataFromQuery($query, $mapping)`: the carrier `TypedCsvCensus` holds its mapping as a name, the
  let-bound element is the body's one visible read, the system store's) and they walled at lowering (`scalar lowering
  not yet implemented for TypedCsvCensus`: the walk folds the carrier to instance literals in Java from the model,
  `TestDataGenerationNatives.foldCensus`, and the program road has no fold), so the carrier is a kind of its own
  (`Kind.CSV_CENSUS`, the census tag `csv-census`) that keeps a body on the walk whatever else it reads; the parity
  test walks such a body and pins its class. Until then those three bodies rode the walk on the strength of their
  system reads alone -- the kind that named them was not the kind that kept them. **The audit** (COMMIT AFTER FOLDS): its one blocker was M8b's omission below (the TypeScript plan-version
  pin), folded here; its should-fixes folded: a unit test of the sessions source (a System target opens the opener's
  session; the shared source refuses it by name: `ProgramRunnerTest`), the store-free assert beside a system read as a
  parity body (it runs on the system session on the program road, one target, so the walk sends one more statement on
  the body's session and one more fused statement under the database judge: the parity test allows that pair's delta
  alone), the `system` tag's round trip (`PlanJsonTest`); its nits folded: the router checks the route's renames as the
  walk does, the planner's javadoc merged onto the one `testProgram`, the plan vocabulary names the ROLE (the graph's
  system database) not the driver class, the predicate's javadoc says whose the frame and effect exclusions are; its
  nits recorded: (a) the predicate is computed twice per routed body (the class rule and the router); (b) a latent
  two-target where a body session's and the system session's server versions differ (no witness: both the one
  in-process jar); (c) a derived route object made for every graph; (d) a `system-read-server` tag for the server lanes'
  residue. **Residue:** (e) server engines (Postgres, the rest): the walk renames the system tables to the session's
  temporary names; the program road does not apply the renames, so the rule refuses such a runtime by name
  (`inProcess`); (f) frame programs over the system store (`let r = execute(..., systemRuntime)`) and bodies mixing a
  user store's read with a system read (two targets): the walk's -- the `assert,system-read` bodies still counted stayed
  are these, or shapes the assert class refuses regardless; (g) the engine-text asserts over system reads (`host-unplanned-assert,system-read`,
  `database-unplanned-assert,system-read`) wait for M10b; (h) on the walk a system-routed side's plan now names the
  System target in its metadata (behaviour unchanged: the walk's source ignores the target); (i) the graph's system
  database is seeded by whichever road first touches it (the parity test seeds it before its pairs; on a corpus lane the
  seeding is counted once, as before); (j) the product's own metamodel carriage (`PlanMaker.metamodel`) unchanged: the
  follow-up the user named. **M8b's omissions, found by CI on 8bae5b1a (read 2026-11-01 22:12 UTC; the runs on
  52092449 and 189c0475 were cancelled by the pushes after them):** 24 jobs green, 15 macOS cancelled (the pattern), 12
  FAILED, all M8b's: the `checks` lane -- the twelve SQL ladder goldens (`core/src/test/resources/ladder/r*.current.sql`:
  the verdict statement's text with its two new columns) and `parser-equivalence`'s `ratchets.tsv` (`own_corpus.matched`
  3038 -> 3041) not regenerated (`bazel run //:update_generated` regenerates both; `//:generated` green); the `ui`,
  `datacube`, `warehouse` and `parser_equivalence` lanes on Linux, Windows and arm -- engine-client's TypeScript plan
  runner pins the plan format (`PLAN_VERSION = 24`, `engine-client/src/plan-runner.ts`) and refused every plan the WASM
  planner wrote at 25 ("not a legend-lite execution plan of version 24": `//engine-client:plan_runner_test`,
  `//datacube:table_output_test`, `verify_flow_test`, `headless_test`, `//python:engine_test`); the pin moved to 26 with
  this step. The rule re-learned: PlanJson's version moves with the TypeScript pin and `//:generated` in the same commit
  (M8b did not run `//gates:local`; `//gates:local` runs at M10's chain).
- **M10b -- the engine-text asserts: the test in the plan on every lane, the appeal a harness (2026-11-02; the brief
  `docs/test-is-product-homework/M10B_BRIEF.md` written before the code from the code map `M10B_CODE_MAP.md`; its
  independent audit: DO NOT COMMIT as it stood -- its blocker and should-fixes folded below, then COMMIT; `docs/test-is-product-homework/M10B_AUDIT.md`).** The user (2026-11-01, the revalidation's §2.4): no lane bifurcated; the test fully in
  the plan; where it does not match, the rest in Java. The code's rule, read before the design and KEPT: a text byte-equal
  to the golden IS the verdict (the user's ruling of 2026-09-21, option 1) and the rows leg is the appeal on a failed
  text alone -- the charter's §0 sentence ("rows, never text") describes the design before that ruling. (1) ONE verdict
  site: the arms' three policy sites (the rows tail `rowsLegAndVerdict`, the generator's fetch text, the text-decided
  declines) fold into `SqlTextVerdicts.decide(env, name, golden, ours, textEqual, decline, appeal)`: under a COLLECTOR
  the comparison and its appeal are handed over; under an active verdict BATCH the text equality is a row of the body's
  fused statement with the appeal at the flush (the walk's own path since 2026-09-21); else decided now. The two decline
  sites report their reason through it (the walk's order kept: the decline, then the verdict). (2) The planning entry
  `SqlTextVerdicts.plan(bare, ...)`: the arm the walk runs for the assert (the root arm its FQN names --
  `assertSameSQL`, the generator's `assertSqlEquals`, `assertEqualsH2Compatible` -- else the equality arm's producer
  shapes, `mayArm`) run ONCE under a collector (`ExecEnv.withEngineText`); while it runs, `JudgePlans.evalSide` plans
  a side and refuses one that is not a constant of the plan (`NotAConstant`: the body stays on the walk, by name --
  nothing is sent at planning); the one comparison collected (`Collected`: the texts, their equality, the decline, the
  appeal, the environment) becomes a host check whose two asks are the texts as constants
  (`VerdictQueries.text`) and whose judgment is `HostJudgment.EngineText(collected, routed)` -- the one judgment that
  holds a closure and the harness's objects (R3, T13, R6: the rows leg is harness Java outside the plan, run at the
  step on a failed text alone). (3) `AssertVerdicts.planHost` plans the three root FQNs through it under no family
  (the walk counts none before a root arm; a raise inside the arm is the assert's raise at its turn, the one catch) and
  the equality arm's producer shapes where `mayArm` holds (the generic path where the arm declines, as the walk's);
  `HostJudge.judge(EngineText)` → `SqlTextVerdicts.judge`: the two text sides checked against the planned texts, then
  `decide` (the equality arm's census route counted by `HostAssert.judge` once the judgment answered, never on a raise
  -- as `adjudicate` counts it). (4) The database road is today's batch: the static gate `databasePlansWithoutReading`
  admits an engine-text shape (its texts constants, its appeal the row's), the planning pass `planDatabase` →
  `adjudicate` → the arm → `decide` → `batch.defer(textEquals(golden, ours), appeal)` as the walk defers it, the segment's
  fused statement carries the row and `VerdictBatch.Planned.judge` runs the appeal where the row failed. (5) The class
  rule: a Java routine (`toSQLString`, `planToString`: a planning-time text) reads as its FIRST argument, the query or
  plan under test, its mapping, runtime and dialect consumed when the text is made (`framesAndHandlesAsTheirQueries`,
  as the handles since M9d); an element BOUND by a let is a system read where a later statement reads the variable as a
  value and none where planning-time natives alone consume it (`systemRouted`: M10a's rule had routed such a body to
  the system database -- harmless while nothing was sent, wrong once the appeal reads the user store); the generator
  carrier (`TypedTestDataGen`, the `generate` and `seedString` flavours, folded by the walk through the database) a kind
  of its own, `Kind.TDG_GENERATOR`, keeping a body on the walk (T8 stays residue for M10c/M10d). (6) The database
  judge's static gate in three (`AssertVerdicts.databaseGate`: PLANS, RAISES, UNPLANNED): an assert whose arm RAISES
  at planning and is the first root of its segment is the walk's, decided statically -- the planning pass would wall a
  batch with no row to carry the wall (`DatabaseAsserts.close`), give the body back after counting, and the walk would
  count it again (an M7-era gap, found by the foreign-dialect witness). **Measured:** MEASURED_M10B. **The witnesses**
  (`BodyRouterParityTest.engineTextPrograms`, both judges, every in-process backend): the text equal (no replay), the text
  differing with the rows agreeing (MATCH), the rows differing (DIVERGED: the arm's message byte-identical), a frame's
  executed SQL under `assertEquals`, `assertEqualsH2Compatible` and the Result overload of `assertSameSQL`, and the
  foreign dialect (the routine raises at planning: under the host judge the raise is planned at the assert's turn and
  the body moves; under the database judge the static gate leaves it to the walk -- the one road difference, named).
  **Found by the corpus, not the witnesses (the first chain):** the one verdict site collected AGAIN at the step --
  the collected environment still carries the collector object -- and answered ok() without judging, so every
  engine-text assert passed vacuously on both roads under the host judge (the walk reaches the same site through
  `planHost`): the host pass showed 2 referee replays where M10a's showed 1,188 and seven expected-failing tests
  "passed" (the accepted roster's differential caught it). The site collects only while the collector is PLANNING
  (`Collector.planning`), and the witnesses pin their outcomes absolutely since (an equal text holds with no rows leg,
  the matching body replayed, the diverged body fails with the arm's message), so two roads agreeing can no longer hide
  a vacuous pass. The lesson for the step's audit: parity alone is a necessary check, not a sufficient one.
  **The audit's findings, folded** (its blocker B1 was this defect, found by the corpus first): B2 -- the let rule
  un-routed the let-bound-mapping bodies but the class rules still read the let's element reference as a system read, so
  they neither routed nor moved: the same consumed-binding test (`BodyClass.bindsASystemElement`, `bindingConsumed`)
  now exempts such a let from the assert and frame programs' side-kind reach, and the bodies move on the user runtime;
  S1 -- a system-routed body's engine-text assert would appeal on the system session (the user store is not there):
  `HostAsserts.plan`/`DatabaseAsserts.plan` take the routing and leave such a body to the walk -- the corpus's
  `testViewChainsWithBusinessDate` (a `toSQL` handle over a helper's runtime whose body references a store element,
  so the body is routed) failed exactly so on the second chain and passes on the walk; S3 -- the dual-golden rewrite
  reports under the call the body spells; S5 -- the database gate's RAISES proxy tracks "a row planned before it in the
  segment", not "an assert before it" (a root RESOLVED at planning carries no row either: an M7-era gap, recorded);
  S6 -- the witnesses pin their outcomes absolutely, the listener records the appeal's declines and referee outcomes, a
  frame with a differing text (MATCH) and a dialect with a renderer but no referee (DB2: a DECLINE before the texts are
  compared, the text the contract) are witnessed; S7 -- the records, the charter's §0 correction, the arms' class
  javadoc. S2 (the collector's `planning` flag, mutable) considered and kept: it is set false in `plan`'s `finally`
  (reset by construction, not convention) and the appeal closures the arms build capture the planning environment,
  which the step must then read as not planning. S4 was the intermediate tree's: with `NOT_A_CONSTANT` the walk's own
  arm runs where a text side is not a constant. Nits folded: the javadocs; recorded: `BodyKinds` tags `tdg` (never, for
  a carrier) beside `tdg-generator`; the texts folded three times per assert on the database road (the static gate,
  the planning gate, the arm).
  The referee of the witnesses is a test double in core's test tree (the golden replayed on the body's own connection,
  rows compared as sorted text multisets), where the brief had placed the witnesses in `spec` beside the harness's
  `ReplayReferee`: the parity machinery (the walk forced, the census, the ledger) is the core test's and
  package-private, and a 60-line double is not a second comparison policy -- recorded as the deviation it is. The
  engine test library's three functions are defined in the witnesses' model under their own FQNs (the arms detect by
  FQN; core's catalogue has no library). **Residue:** (a) the referee replay, the comparison policy, the plan replayer
  and the TDG transcript stay the harness's (T13, R6); (b) the foreign-dialect and `unresolved`-dialect texts the
  contract, counted; a declined golden a counted decline; (c) a text side that is not a constant of the plan keeps its
  body on the walk (`NotAConstant`; no corpus body measured); (d) the generator carrier's bodies on the walk (T8);
  (e) the two `TestDatGenForNestedViews` bodies, moved since M9e, pass with their generator result reaching an effect
  native's argument -- unchanged by the new kind, as expected; (f) `SqlTextVerdicts` shrinks by nothing: its detection is
  now a planning half and its rows leg the appeal; (g) the engine-text asserts inside shapes M10c owns (a quantified
  form, the cell pool) wait for M10c; (h) a decline under the database road is reported at planning (`planDatabase`'s
  adjudicate, as M7's census), where the walk reports it at the assert's turn -- the totals the lanes pin are the same,
  the order within a segment differs; (i) the charter's §0 sentence corrected to the 2026-09-21 ruling here, not yet in
  `docs/SQLTEXT_ROW_VERDICT_CHARTER.md`.
- **M10c -- the remaining shapes the two planners refuse, item by item (2026-11-02; the brief
  `docs/test-is-product-homework/M10C_BRIEF.md`: the inventory measured on the DuckDB corpus after M10b and attributed
  to the refusal site by a probe, each cause with a corpus witness; the host pass walks 153 passing bodies the class
  admits and `planHost` refuses, the database pass 683).** **Item 1a BUILT (2026-11-02):** `DatabaseAsserts.plannable`'s
  static scan reads what the program road RUNS of a statement (`scanned`): for a root engine-text assert (M10b's three
  root arms) the call's arguments alone, since on the program road the arm plans the two texts from them and the
  callee's body is never planned (`planHost`: the arm under a collector, else the dual-golden rewritten to
  `assertEquals(upgraded, actual)`, else the walk's; on the walk the other two roots' declines inline the callee as
  before -- the audit's correction of "either road") --
  legend-engine's `assertEqualsH2Compatible` reads the H2 version through `executeInDb` in its helper `getH2Versions`
  (h2Extension.pure:44-49), a raw grid the scan had read as the body's by following the user call. DuckDB database pass
  moved 2,122 -> 2,398 and H2 2,055 -> 2,331, outcomes identical on every pass, every register green, no register
  changed; the witness: the frame model's helper carries the engine's own body, so the DATABASE-judge witness pins the
  move (two spellings: the frame's SQL through the envelope, and the corpus's String actual). Its independent audit:
  COMMIT WITH FIXES, folded (the "either road" wording; `scanned` matches the statement itself, no let unwrapped; the
  second witness spelling). The brief's first attribution of these 321 bodies (the handle's rows as a raw relation) was wrong -- the probe
  read the helper's body -- and is corrected there; its item 1b (the handle's text through a navigation: host 43,
  database 39) names the tenet's form (the text side a step whose row binds the verdict step's parameter, as the frame
  stamp) and refuses the fold in Java (a shadow evaluator). **Item 2 BUILT (2026-11-02):** the grid pair and the
  rendered text under the database judge. What the code said, read first: under the DATABASE judge the walk's
  `adjudicate` handed a pair of two relation-typed sides to the HOST grid owner before any arm (D3), which is why M7's
  gate refused it; the rendered text's database arm planned without reading but its late-bound branch is a keyed
  statement the program road does not place. The arm gains `gridPair` (the one dispatch: the router classifies, the
  judge's arm judges) -- the host arm the grid owner as before, the database arm `VerdictSql.gridPair` over the two
  planned grids, a row multiset unless the actual side's order view is SORTED (the grid owner's own rule), the agreement
  arm both -- and `databaseGate` admits a pair of two `staticGrid`s (named columns, none dynamic or late-bound) and a
  rendered pair whose rendered grid is one; a late-bound grid stays the walk's, by name. DuckDB database pass moved
  2,398 -> 2,662 and H2 2,331 -> 2,595, outcomes identical on every pass, the rosters byte-identical; the witness
  `gridPairsAndRenderedTextsUnderTheDatabaseJudge` pins the outcomes absolutely on both roads. Its independent audit
  (COMMIT WITH FIXES, folded): the ledger's exact pins re-dated (`//core:census` is the local gate's, not the driver
  suite's); the first build had routed every two-relation pair on the walk to the judge's arm (a late-bound pair the
  host judged on its cells would be declined under the DATABASE judge: an outcome change), and the SQL grid pair does
  not see three facts the host grid owner decides from the grids first (the column names, the width, the expected
  grid's declared kinds typing both) -- folded as ONE predicate for the gate and the route (`sqlGridPair`: two
  declared grids of one width, named and typed alike position by position), a pair it refuses keeping the host grid
  owner on every road; three mismatched pairs witnessed with the owner's verdicts; the residue (the Float leniency's
  positional join under the multiset view, two predicates for the rendered shape, the agreement lane's extra
  statement) in the brief. **Item 3 BUILT (2026-11-02):** the lineage lines. `LineageTreeLines.asLines` rewrites a
  lineage tree print against its golden into a collection equality (the golden's lines; ours the prelude's
  `relationTreeLines` over the scan's handle, whose rows ride the query and join the metamodel tables); the walk
  continued with the rewrite where `planHost` refused the shape -- now `planHost` plans the rewritten assert under the
  call's own name. The scan is a read of the system store (`BodyClass.anotherWay`: `Kind.SYSTEM_READ`), so a body whose
  only store read is the scan is system-routed and its program runs on the graph's system database, the one session the
  walk routes the side to; the kind decides the routing alone (the class rules see a handle as its query), so a scan
  beside a user read is admitted by the class and refused by the planners as before (the host's session check, M9d;
  `DatabaseAsserts.plannable`, admitting the scan in a system-routed body now); the five `scanColumns::test` bodies
  (an equality over the scan's rows, no user read) are system-routed now and move with the prints (49 + 5). DuckDB host
  pass moved 2,690 -> 2,744 and database 2,662 -> 2,716, H2 2,744 / 2,649, outcomes identical on every pass, the
  rosters byte-identical; the witness `lineagePrograms` pins equal and differing goldens, an equality over the scan's
  rows and the `scanColumns` chain on both roads under both judges, every boundary the same but the walk's one
  body-session setup statement (the M10a difference). Its independent audit (COMMIT WITH FIXES, folded): the ledger
  pin; the mechanism sentence and the five bodies' attribution corrected; the two witness shapes added; a stale
  comment rewritten. **Next:** item 1b, a user assert helper at the root (20), the quantified forms (18),
  the tails.
- **CI's Linux runners and the LLVM toolchain -- a standing hazard, not this program's (2026-11-01).** On
  c9847ebe (M6) three Linux lanes (`stress`, `datacube`, `pct_postgres`) died while Bazel extracted the LLVM toolchain
  on the runner ("Free space left: 84 MB", then "No space left on device"), two before any test ran; the same death
  took `stress` on 3bd68a81 (M5f). The lanes build no native code; the toolchain (about 11 GB extracted) is fetched for
  their analysis, against the runner's 14 GB with the Bazel cache restored. Noted on PR #83; a follow-up for the build
  program (trim the distribution, or keep the toolchain out of the lanes that need no C++).

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
- 2026-10-31: M2 closed (its chain green but for a TypeScript guardrail its own code broke, fixed; two more product
  tests; the audit's finding 3 recorded as partly folded). **The plan re-shaped (R16, D102)**: whole bodies move class
  by class, the per-statement bridge dropped, M5-M10 rewritten. **M3 ruled (R17)** and its brief written (B12). Next:
  M3a, the measurements.
- 2026-10-31: **M3a measured** (`docs/test-is-product-homework/C_M3_MEASUREMENTS.md`); B12 narrowed by it (the door's
  seeds rule for a runtime other than the plan's own; `fail` not a verdict; DuckDB's F11 limit kept; the renames move 0).
- 2026-10-31: **M3b built**: the plan records and the runner -- `Effect`'s script (its statements, bracketed form and
  abort), the `Setup` node (a key and its steps; the runner skips a key the session holds clean, else seeds under SEED
  and records it), `Verdicts`' optional savepoint (Postgres writes one; DuckDB none, its limit documented), PlanJson
  24 and the TypeScript pin; `PlanRunner.program` brackets only for an `Update` (an `Effect` commits itself, its
  bracket sent only where the session owns its transaction; after it the session is marked written). Measured: every
  core package's statements against M2 -- the only new statements are the new runner tests' own. Next: M3c.
- 2026-10-31: **M3c built**: the doors. The query door (`planner/QueryDoor`, called first in `PlanMaker.plan`) scans
  the typed body once with the compiler's own reach scans (`StatementEffects.reaches`, `callsVerdict` now one of its
  uses) and refuses by name at the first statement holding an effect, a `from()`'s inline test data, another runtime's
  test data, a let-bound `execute(...)`, an `assertError` or an assert (`fail` excepted, by its declarations). The
  test door (`testplan/TestDoor`, the test-planning library, its own target `//core:testplan`, B4 brought forward):
  a body planned whole through `PlanMaker.testProgram`, its target carrying no setup (C6), a test's kinds handed to
  the library through the planner's extension point (`planner/ProgramKinds`) and refused by name until the sub-step
  that plans them. One program state (`planner/ProgramState`). ArchUnit: the library is the driver's alone; the
  program road's test entry is the test door's. Measured: every core package's statements against M3b -- identical
  (17,335), every package green, so the door refuses none of core's product queries. Next: M3d.
- 2026-10-31: **M3d built**: a test's effects on the test door (`docs/test-is-product-homework/C_M3D_EFFECTS.md`).
  `EffectPlanner` moved into the test-planning library (R17 C7); `TestKinds` plans an effect statement's statements
  (each String argument a literal folded, a computed one refused by name: T9) into the open script, which any other
  statement closes, as an `Effect` step (its statements, bracket, abort); an effect let keeps the walk's guard; an
  effect as the last statement answers as the walk's arm (C11); `loadCsvToDbTable` (C10) and an effectful map (T9)
  refused by name. Both doors: `print`/`println` no step, a no-value constant when last (R18); `connectionByElement`
  a handle; the test door answers a program ending on a handle no value. The census: on both corpora, both passes,
  597 effect bodies per pass planned whole and every script byte-identical to the walk's (3,603 scripts, 109,162
  statements); 157 refused by name (116 asserts, M6; 19 handle-ended, folded after; 17 let-bound executes, M3f; 4
  effectful maps; 1 `loadCsvToDbTable`). Core's statements unchanged. Next: M3e.
- 2026-10-31: **M3e built**: a test's seeds on the test door (`docs/test-is-product-homework/C_M3E_SEEDS.md`).
  `testplan/Seeds` is the one owner of a statement's seeds for the walk and the test door (moved from the body
  driver): their sources (the session state's key), their steps (`CsvSeed.planned`) and their name in a plan (a hash
  of the steps' texts). `TestKinds` places a `Setup` step before each statement whose sources name test data, unless
  placed since the program's last write (an effect); the door's scan reports every kind, so a seeded statement holding
  an assert is still refused. The census: core's 9 seed-naming bodies the test door plans match the walk's
  establishments exactly, every sent seed a planned step; the corpus has none in the class planned at M3e (its 12
  seed-naming bodies hold asserts, M6); M3d's scripts re-run with the handle fold: 616 effect bodies per pass, all
  identical. Core's statements unchanged but the test door's own tests (now run through the program runner). Next:
  M3f.
- 2026-11-01: **M3f built**: frames, the eager run and the trace stamp (`docs/test-is-product-homework/C_M3F_FRAMES.md`).
  `planner/ProgramFrames` holds a program's frames, plan rows and stamp names; every product statement is inlined with
  the splice hook over them and runs under the renames of the frames it reads (`FramePlanner.renames`, moved from the
  driver). An inline lazy `execute(...)` read is the product's on both doors (D15); a frame let on the test door is
  run at the let as a stamped `Check` (decision 1, MV7). The let kinds are `LetPlanner`'s (STAGED_GRID refused, T12).
  The activity comment is a plan parameter (R17 C4): a typed cell from the activity rows to the TDS literal's `?`,
  `ExecutionPlan.Sql.stamp` minted by the session (`Statements.nextStamp`) and bound by the runner; the user, asked
  whether its cost (15 files, +217/-37) is worth it, kept it (2026-11-01). Core's statements unchanged but the test
  door's own tests. Next: M3g.
- 2026-11-01: **M3g built**: the frame's CTE (B10's rule, per statement). A frame let's CTE is lowered once at the
  let with the product's configuration (`FramePlanner.cte`, its chain's lowering now a parameter: the walk's identity
  lane, the product's road); a statement that reads a frame more than once is planned again with that frame read by
  reference to its CTE, defined ahead of the statement (`FrameCtes.attach`, under the frame's alias prefix); one that
  reads it once pastes its chain. MV9's movement (M3a's counts) lands with the body classes that read frames.
  Measured: core's statements unchanged but the test door's own new test.
- 2026-11-01: **M3h built**: a `from()`'s own table renames (R17 C9): `BodyPlanner.prepare` merges the outermost
  `from()`'s runtime's renames with the environment's (an execute frame's), a conflict refused by name, on both roads.
  Measured: core's statements unchanged (M3a: 0 corpus statements move, the environment already holding the same
  renames); the corpora re-measured with M3i's chain. Next: M3i.
- 2026-11-01: **M3i: the census re-run, the audit folded, the records** (D103;
  `docs/test-is-product-homework/C_M3I_CENSUS.md`). The census: the product's refusals on the corpus fall from 3,632
  to 136 (DuckDB database pass) and 4,381 to 120 (host), the frames planned; channel B no front-half difference;
  effects identical. The independent audit found eight bugs, all fixed (a rollback leaving the seed memo clean; the
  transaction decided once, wrong under a keyed snapshot; a seed step with no target; CTE reads counted by hook calls;
  an effect marked written after it ran; the frames' consumed lets lost; four stamp edge cases; seeds inside an
  effect run, refused), with tests; writing them found two more (seeds placed before the open script closed; the let's
  own CTE planning re-stamping its activity rows), fixed. **Corrections:** M3b's "no movement" is wrong for Postgres --
  its verdict groups send the savepoint on the walk (B12 item 6, departures above); M3b's "brackets only for an
  Update" held only outside a keyed snapshot (fixed: asked at each step); M3d's and M3e's censuses compared texts and
  never ran the plans. Every core package's statements against M3h: 17,362 -> 17,372, the ten new the new tests' own.
- 2026-11-01: M3's chain green on 01141e17 (corpora and channel B identical to M2's census; the local gate after two
  stale generated files were regenerated); the checkpoint pushed (90ea4e5c, with M2's commits), CI and the lane
  sessions' round 9 asked. **M4a measured** (`docs/test-is-product-homework/C_M4A_MEASUREMENTS.md`): no core test
  executes a product query over the system store (one test plans one per dialect); the test road opens 75 system
  databases in core holding 6 distinct contents, one per corpus pass, so a store kept by content is bounded; Trino
  alone has no temporary table. Posted to the user: a server engine with no system connection configured turns a
  product store read from an answer into a refusal under B13 item 2; and whether M5 comes before M4.
- 2026-11-01: M3's checkpoint CI green on every OS (52 of 52, `gates green` included); the lane sessions' round 9 on
  90ea4e5c: every outcome identical on every lane, channel B and the test road byte-identical; channel A's one
  movement M2's literal answers (MV12: 207 per lane, 148 on the four-engine lane, with their session statements),
  measured on the PCT lanes for the first time -- the fold is the compile-time unroll's (ratified 2026-09-03), M2
  only stopped sending its literal. The user's rulings R19 (M5 before M4) and R20 (MR first); B14 written from an
  independent inventory (`docs/test-is-product-homework/E_MR_INVENTORY.md`). **MRa built**: imports for the inline
  fully-qualified names in the seventeen files of the program road; the dead `ProgramState.door`; the stacked and
  orphan javadoc (BodyPlanner, PlanRunner, ResultEnvelopeSplice) and the stale SessionState comment fixed. Measured:
  every core package's statements byte-identical (17,377 -- M3's own count, its B7d test's five included, which the
  M3i dump predated: the record's 17,372 was one test short).
- 2026-11-01: **MR built** (R20, B14; D104). MRa imports, dead code, comments; MRb the runner's program loop split
  (a pure shape, the steps, the bracket, the answer stream; the answered flag gone); MRc the program state a builder
  with one owner (copies out, no aliased lists) and `PlanMaker.program` split (decide, planLet, stagedLet,
  planStatement, finished); MRd a statement's frame reads a frozen value (`ProgramFrames.Reads`, from a per-inlining
  `Recorder`), `prepare` split into inlining and resolving, `ProgramKinds` one instance per program; MRe the inventory's
  four latent defects: (1) not a defect, pinned by a test; (2) sound; (3) a stamp no step binds refused by name where
  a statement reads it; (4) REAL and fixed -- a write forgot one flag for all seed keys, establishing one key cleared
  it for every other; a write now forgets every key (zero movement measured: core, both corpora's four passes, channel
  B). The independent audit (no blockers; six should-fix) folded: S1 -- MRd had let an inline run's activity rows
  outlive their statement, two paths core does not cover reading stale rows (one on the query door's write programs):
  an inline run's rows are now the statement's own layer (`ProgramFrames.StatementRows`), gone with it, the old
  semantics without a reset by convention; S2 the stamp check exact (only where read, over the finished plan); S3 a
  frame let's eager run and CTE planned under its runtime's renames as the walk runs them (its own from() case tested);
  S4 the seed memo tested; S5 the remaining item-4 duplicates partly made one (the declared parameters, the statement
  environment), the constant guards, the stacked javadoc, the split names; the nits. Named, not done: the rename
  merge's two copies (BodyPlanner, FramePlanner: their conflict messages differ), the two effect memos (two scans),
  `ProgramFrames.planRows()` handed to the test library for a frame let's program-level rows, `ProgramState.frames()`
  the test library's whole frames (the program's second planner), `TestKinds.effect` and `PlanMaker.plan` still long.
- 2026-11-01: MR's checkpoint pushed (9a0fe445), CI and the lane sessions' round 10 asked. **M5a measured**
  (`docs/test-is-product-homework/C_M5A_MEASUREMENTS.md`): per corpus pass 623 assert-free bodies planned whole by
  the test door (616 effect setup programs), 5 refused (3 effectful maps, 2 bodies that fail to compile on the walk
  too); channel B none (every body asserts); core 162 assert-free bodies planned whole, 8 refused, each a kind a later
  step plans. M5's detail written under B1 (the class by a named rule, the raw-SQL ledger the referee mirrors).
  Measured: every core package's statements byte-identical through MRa-MRf (17,377); the chain (on the fold): both corpora uncached, each pass's statement-origins census and pass/fail counts identical to M2's and M3's; channel B 5 of 5; `//gates:local` 448 pass and 1 skipped (the 9 Postgres targets refusing root, 9 of 9 unprivileged).
- 2026-11-01: **M5b-M5e: the body router; the first class, the effect setup programs, runs whole** (D105). M5b the
  class rule (`testplan/BodyClass`, a closed list that only grows, `BodyClassTest`); M5c the raw-SQL ledger's texts
  beside the plan (`TestDoor.Planned`) and the runner's program sink told when an effect ran or failed and when a seed
  step ran; M5d the router (`driver/BodyRouter`, `Execution.runProgram`'s one call): a body of a moved class planned
  whole by the test door and run as one plan, every other walked. A first cut marked the whole program BODY and moved
  3,573 census labels per DuckDB database pass from seed to body; fixed to the walk's (an effect's script BODY around
  its send alone). The independent audit (R15) found three blockers and six should-fix, none in a shape the corpora or
  core run, all folded: B1 a seeded runtime (the two roads key the seed memo and order seeds against an open script
  differently) -- the class admits no body that names test data; B2 a no-value answer (one null row where the walk
  answers none) and a literal's column facts -- the test door answers as the walk (`asTheWalkAnswers`, §9's question);
  B3 a failing script's message -- named as the walk names it (`testplan/EffectSegment`, one spelling); S1 a seed
  failing midway records the statements before it; S2 store writes and staged rows, S3 computed values, S4 a print of
  an effect -- refused by the class; S5 the one-caller rule (`theDriverHasOneCaller`: the walk the router's alone, the
  router the door's) and the stale docs; S6 posted (§9); the nits (one answer-shape rule, one list for the open
  script, seeds' texts apart from effects', a size check, the effects census counted on the program road). Found on
  the way: the mutable-field guard read no annotated type (47 fields were outside it, 46 of them older than M5) --
  widened, the found fields registered with what each is (dated); `H2.failingStatement` looks for
  `"SQL statement: "` where H2 2.1.214 writes `"SQL statement:\n"`, so on H2 neither road records a partly applied
  script's prefix (pre-existing, both roads alike; named, not fixed here: the fix moves the H2 corpus's referee
  mirror). `BodyRouterParityTest` (new) holds the two roads equal on each admitted shape. Measured: the corpora's
  four passes' censuses and pass/fail counts identical to MR's, 600 bodies moved per pass; every core package's
  statements identical but labels (the program road's own exec and testplan tests: an effect's script is labelled body,
  as the walk labels it) and the parity test's own; channel B 5 of 5; `//gates:local` green but the 9 Postgres
  targets refusing root (9 of 9 unprivileged).
- 2026-11-01: M5's first-class checkpoint (98e3e388): CI green on every OS; the lane sessions' round 11 zero movement on
  every lane (`bodies.moved` absent from every PCT log: every PCT body asserts). **M5f: the second class, the plain
  query programs** (D106). `BodyClass.QUERY_PROGRAM` (B1 item 5); the router holds the walk's answer contract (at most
  one row, at least one, exactly one for a to-one reader -- the D30 residue, `JudgePlans.holding`) and asks for the
  caller's sink before anything is sent, as the walk does; a program's answering statement is labelled as the walk
  labels a body's (STATEMENT, where no mark is in force). Its first cut admitted shapes the product answers otherwise;
  the measurement (both corpora, core, channel B) saw none of them, the independent audit (R15) found four blockers
  and four should-fix by probing both roads: a value through a class's chain (the product's value column is the
  cell's, `u_map__name:String[1]`, where the walk answers `value:String[0..*]` -- a product defect too: the service
  runner's writer refuses its 3 rows), an object answered (a graph on the product's road), a value statement before the
  last (a check the walk holds and labels, planned unmarked and unchecked), a to-one reader through a let or a user
  function; the caller's sink asked for late (a caller that refuses the shape: SKIPPED became FAIL), a temporal read's
  exists-join form (D5), the parity test comparing no statement text (it now compares every statement sent, origin and
  text, through the census's new observer `StatementOrigin.observing`), missing pins. All folded by narrowing the class
  and holding the walk's timing; each excluded shape pinned; `ProductStatements.toOneReader` the one to-one rule. Named,
  for the user's §9 and MV6: the product's value column through a class's chain (`PlanMaker.typed` without
  `ResultShape.valueInfo`). Measured: the corpora's four passes' censuses and pass/fail counts identical to MR's, 604
  bodies moved per pass (the 600 setup programs and 4 query programs); every core package's statements identical but
  labels (the program road's own tests) and the parity test's own (17,471); core 28 of 28; channel B 5 of 5;
  `//gates:local` green but the 9 Postgres targets refusing root (9 of 9 unprivileged) after the parser-equivalence
  ratchet regenerated (3,036 -> 3,038: the parity test's model joined the own corpus, both elements matching).
- 2026-11-01: M5f's checkpoint (3bd68a81): CI 49 of 51 (`stress` died on its runner's disk before any test, noted on
  the PR; `gates green` its roll-up); the lane sessions' round 12 zero movement on every lane. The user's rulings R21
  (M4: the test road takes the product's system-store road; the answer facts as upstream answers them, asked of the
  engine runner) and R22 (the faster cadence). **Built (D107):** a one-value answer `[1..1]` on both roads
  (`JudgePlans.host`, `ProductStatements.asATestAnswers`: only a no-value answer is answered as no row); the product's
  value through a class's chain answered in the value's column (`PlanMaker.typed` through `ResultShape.valueInfo`).
  Measured: every core package's statements identical to M5f's (17,471), core 28 of 28; `pct_duckdb` 6 of 6; both
  corpora's censuses and pass/fail counts identical, 604 moved per pass; channel B 5 of 5; `//gates:local` 448 pass
  and 1 skipped (the 9 Postgres targets refusing root, 9 of 9 unprivileged). Next: M4 as R21 rules it (B13 rewritten).
- 2026-11-01: **M6 built and its audit folded** (D108; the handoff `docs/TEST_IS_THE_PRODUCT_HANDOFF_2026_11_01.md`
  closed by the session that took it over; its WIP commit `89e604de` brought in by cherry-pick). The host judge's
  arms split into a plan half and a judgment (`HostSides.Check`); the third class `ASSERT_PROGRAM` (the host judge
  alone) planned whole: each statement-root assert classified and its sides planned before the program
  (`driver/HostAsserts`, through the test library's hook `TestDoor.Asserts`), placed by `TestKinds` as a verdict step
  at its turn, judged at that step by the router's sink with the walk's census and listener protocol; a planning
  failure a planned raise at the assert's turn (R14) -- inside the arm counted under its family, before the arm
  uncounted, as the walk raises each (the audit's S1, N2); a host side's read failure of any type counted (S2); the
  four declining judgments decided statically (S3); the parity test's cases (S4). The walk runs both halves back to
  back: no behaviour change. Measured against the base regenerated on f08cc4d9's source (17,471 core statements, 28 of
  28): core 28 of 28, every package byte-identical in text and labels but `driver` (+200 lines, all additions: the
  parity test's own new runs, 168 verdict sides under `other` and 32 session statements); `pct_duckdb` 6 of 6; both
  corpora uncached, every pass's statement-origins census and pass/fail counts identical to the baseline,
  `body-router moved` 604 -> 608 on both HOST passes (four assert programs per pass run whole), 604 on the database
  passes; channel B 5 of 5 (it judges BOTH: nothing moves); `//gates:local` 447 pass, 1 skipped, the 9 Postgres
  targets refusing root (9 of 9 unprivileged) and `//core:census` red on the Java-evaluation ledger (re-pinned, dated:
  the plan halves; `HostAsserts` registered) then green. The R21 checkpoint 93c930cd recorded: CI 52 of 52. **The plan
  revalidated at the user's invitation** (`REVALIDATION_2026_11_01.md`; the user's rulings of 2026-11-01): R16, the
  audits and the register hold; the order after M6 is M9, M7, M8, M10a-d ("the order is fine"); M4 narrow on the test
  road inside M10 with a product follow-up (§9); **B3 struck** (an assert is a function the database judge lowers;
  the plan needs no assert metadata; the host judge is the harness: §6 B3); the engine-text asserts: the test in the
  plan on every lane, the appeal a Java harness, no lane special-cased (M10b). Next: M9.
- 2026-11-01: **M9 built** (D109): the frame programs under the host judge, in four sub-steps each with its independent
  audit folded (§9 "M9a" to "M9d"): M9a frames read by asserts (`BodyClass.FRAME_PROGRAM`, the eager run a check step,
  the asserts' sides over the walk's own frames); M9b aliases, inline executes, effects between, handle lets, the rule's
  memos one per predicate, frame taint; M9c activities reads (the trace stamp a plan parameter: R17 C4), the side-kind
  rule, the element-reference rule, the kind census (`BodyKinds`, `Census.STAYED_KINDS`, `[corpus2] body-router
  stayed`); M9d handles and `from()` contexts read as their queries, the session guard, temporal frames. Measured on the
  DuckDB corpus after each sub-step: outcomes identical throughout (host pass 2471 / 108, database pass 2473 / 108),
  `body-router moved` on the host pass 608 -> 853 (M9b) -> 892 (M9c) -> 1,080 (M9d); the database pass 604
  (unchanged: the class is the host judge's). The kind census of the bodies still walked (host pass, passing tests):
  1,131 engine-text asserts over activities (M10b), 390 system-store reads (M10a), 181 temporal frames whose asserts
  are engine text (M10b), 98 other host shapes the checks do not plan, about 190 system reads mixed with effects or
  frames, about 20 effect bodies (through helpers or computed), 10 seeded frames, 8 graph frames, one each of
  loadCsvToDbTable, a staged grid, the eager inline execute; no body holds `assertError` or the test-data generator.
  The remaining M9 kinds (T8, T9, T10, T12, the effect helpers) move inside M10's classes: each is a handful of bodies
  where M10a and M10b are thousands, so the order holds and nothing is built without a body to measure it on. Next:
  M7.
- 2026-11-01: **M7 built** (D110): the database judge's assert and frame programs run whole. `VerdictBatch.flush` split
  into its planning half (`plan()`: the groups per session, the fused statement over the frames' CTEs, the split rung
  at planning) and its judging half (`Planned.judge()`: the root loop, the split rung's wrapper lazily at its place);
  `driver/DatabaseAsserts` plans the body's asserts under a COLLECTING batch before the program, a static pass first
  (the arms count nothing for a body left to the walk), each segment's groups placed at its last assert, the frames
  never run (their CTEs inside the fused statement, their activity comment the fused statement's own stamp); the
  router's sink judges each segment at its last verdict. The audit's F1-F5 and N6 folded; its N7-N8 recorded. Measured
  on the corpora: the database pass `body-router moved` 604 -> 1,026 on DuckDB and 604 -> 961 on H2, outcomes and
  every census count identical (`fallback=18` on H2); the host passes unchanged. The chain: GATES "The
  test-is-the-product plan's M7". Next: M8.
- 2026-11-01: **M8a built** (D111): the host judgment as data -- one `HostJudgment` record per host comparison, its
  facts read at planning (the side facts, the key trees and the classes a polymorphic key slot may hold, the byte gates,
  the flat facts, the order views, the accepted types, a decided verdict, a planned raise), `HostJudge.judge` the one
  dispatch; the sink holds no closure and no model. Zero movement by design; the DuckDB corpus M7's to the count after
  three cuts of the key slots' own classes (the first two caught by the corpus, not the quick suites: §9 "M8a"); the
  audit folded (S1, S3, N1, N5, its parity cases), S2 and N2-N4 recorded, N4 a register question for the user. The
  checks: the core suite 28 of 28, the driver, judge, compiler, lowering, testplan, architecture, guardrail and census
  suites, `//spec:corpus_duckdb` 6 of 6; the chain runs at M8's end. Next: M8b (the database wrapper self-contained,
  MV2/MV3).
- 2026-11-01: **M8b built** (D112): the database verdict row self-contained -- `__holds` and `__message` composed in
  the statement (`VerdictSql.selfContained`), the judge reading them and knowing no polarity, the plan's verdict
  without `wantEqual` (PlanJson 25). Zero movement; the DuckDB corpus M8a's to the count with every register test
  green (the messages byte-identical); H2 run too (database 2233 / 353 with `fallback=18`, host 2289 / 295, the registers green). Next: M8c (the canon over the product columns, MV4).
- 2026-11-01: **M8c OPEN** -- a design question posted to the user (`docs/test-is-product-homework/M8C_DESIGN_QUESTION.md`,
  §9 "M8c"): the canon over the product columns has no finite static form for a recursive polymorphic key slot, where
  the construction-site canon is finite by construction. The identity lane stays as it is; M10a (the system database on
  the test road) proceeds (R23). Next: M10a.
- 2026-11-01: **M10a built** (D113): the system database on the test road -- `Database.System` in the plan (PlanJson
  26), the class rule `systemRouted`, the router's route and sessions source, the test door's target; DuckDB host
  `moved=1080 -> 1,165`, database `moved=1026 -> 1,111`; H2 host `1080 -> 1,165`, database `961 -> 1,045`; outcomes
  identical, every register test green. The commit carries M8b's omissions (the ladder goldens, the ratchets, the
  TypeScript plan-version pin 24 -> 26) that CI found on 8bae5b1a. Next: M10b (the engine-text asserts).
- 2026-11-02: **M10b built** (D114): the engine-text asserts on every lane -- the two texts a verdict step's
  constant sides, the text equality the verdict, the rows leg the harness's appeal at the step; the three root arms and
  the producer shapes planned by the arm the walk runs (`SqlTextVerdicts.plan`, one verdict site `decide`); the class
  rule reads a routine as its query and a let-bound element as a read only where read; the generator carrier a kind;
  the database gate in three. DuckDB host `moved=1165 -> 2,690`, database `1111 -> 2,122`; H2 host `1165 -> 2,690`,
  database `1045 -> 2,055`; outcomes identical, the referee census identical, every register test green. Next: M10c.
- 2026-11-02: **M10c item 1a built**: the database planner's static scan reads what the program road runs -- a root
  engine-text assert's arguments alone (the engine's `assertEqualsH2Compatible` helper reads the H2 version through
  `executeInDb`, a body the program road never runs); DuckDB database pass `moved=2122 -> 2,398`, H2 `2055 -> 2,331`, outcomes
  identical, every register green. The brief's attribution of the 321 corrected (the probe read the helper's body).
  Next: M10c item 2 (the grid pair and the rendered text under the database judge).
- 2026-11-02: **M10c item 2 built**: the grid pair and the rendered text under the database judge -- the arm's
  `gridPair` (host: the grid owner; database: `VerdictSql.gridPair` over two declared grids, a multiset unless the
  actual side is sorted), the gate admitting declared grids; DuckDB database pass `moved=2398 -> 2,662`, H2
  `2331 -> 2,595`, outcomes identical, every register green. Next: M10c item 3 (the lineage lines).
- 2026-11-02: **M10c item 3 built**: the lineage lines -- the scan a read of the system store (the body system-routed),
  the lineage print planned as the lines the walk rewrites it to; DuckDB host pass `moved=2690 -> 2,744`, database
  `2662 -> 2,716`, H2 `2,744` / `2,649`, outcomes identical, every register green. Next: M10c item 4 (a user assert
  helper at the root).
