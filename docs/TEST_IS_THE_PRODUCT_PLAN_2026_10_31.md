# A test body is the product plan -- the plan (DRAFT for the user's review, 2026-10-31)

Status: **DRAFT, not ratified.** No step of §6 starts until the user agrees to §1-§5 and answers §7. It replaces the
remainder of `docs/PLANNER_DRIVER_SEAM_PROGRAM_2026_10_30.md` (S4b to S5). That program's S1 to S3 and S4a stand. Its
way of working stops here: it relocated each test road into `planner/` byte for byte, and relocating twenty roads
keeps twenty roads.

Evidence: `docs/test-is-product-homework/A_CODE_INVENTORY.md` (the code inventory, read-only, 2026-10-31; every row
below cites its D- or R-number, each with file and line there). The user's rulings this plan follows (2026-10-31): a
test body is the product plan; the database judge has literally zero difference from the product plan except what is
named here as test-only; the host judge differs only in that Java compares the rows after the SQL runs, and it is an
exception to the Java rule ONLY, never to the plan-then-run split; the fused verdict statement is KEPT; the database
judge is SELF-CONTAINED, 100% a SQL plan, its every verdict decided in the database, and the engine-text referee's
replay on H2 is a separate instrument, not part of it (the user: "Yes exactly this all in database, agree that the
replay is separate thing"); no output spelling changes without the user's word, case by case.

## 0. In plain words

A test body is a Pure program: lets, statements, writes, effects and asserts. The product already plans a Pure
program: `PlanMaker.program` turns a multi-statement body into one `ExecutionPlan` (a `Sequence`: lets spliced into
their readers, a staged let a create-temp-table step, a store write an update step, an intermediate statement a check,
the last statement typed), and `PlanRunner` runs it.

The test road shares almost none of that. It never calls `TypedQuery.lower`/`lowerer` or `PlanMaker.plan`/`program`;
it borrows five `PlanMaker` helpers and does every other step its own way, over twenty roads (R1 to R20), with 49
differences from the product's planning (D1 to D49). And the product road cannot plan a test body today: a read-only
body with any statement before its last that is not a let is refused by the inliner, `program` is chosen only for a
body that writes, and `PlanMaker` never emits an effect step or a verdict step.

After this plan: **the product planner plans the whole test body into one `ExecutionPlan`, `PlanRunner.program` runs
it, and the judges are the sink.** Every value statement, let, write, effect and assert side is planned exactly as
the product plans the same expression; a parity guard checks it byte for byte. What is test-only is a short list of
named constructs (§1.C), each a plan node, a lowering of an assert, or a sink, never a second way of planning the
same expression.

## 1. The ledger

### 1.A Differences that go: the test road converges on the product's planning

The product's way is right, or neither way has a stated reason; the test road takes the product's. Each changes
statement TEXT somewhere and is posted, class by class, before it lands (§5).

| D | What differs today | After |
|---|---|---|
| D1, D20 (host), D2 (values) | host identity asks, frame CTEs and value statements use the identity layout (`__canon`) or a body-wide id minter | the product's object layout and a fresh minter per statement; the host compares identity over `__id` in Java |
| D3 | the value lane strips synthetic struct fields in SQL (`withDeclaredEgress`) | the writer strips them, as the product does |
| D5 | the exists-join form always on (no stated reason) | the product's rule: on unless a temporal class is read |
| D7, D28 (values) | a graph value is one aggregated JSON-array cell, split in Java | the product's streaming root, one object per row |
| D8 | three render entries (`render`, `Sql.of`, `renderStatement`) | `renderStatement` alone |
| D18 | no cross-store check on an effect let | checked on every resolve |
| D21 | "wire reconciliation" comments with no code | deleted |
| D27 | a value side planned as a RELATION, then trimmed in Java | the product's VALUE shape (its MINIMAL precision); the trim deleted if equivalent (measured first) |
| D36 | the judges' target built separately (no `oneDefinition`, `forServer` always) | `PlanMaker.target` |
| D40 | intermediate value statements decoded into Java | a `Check` step |
| D41 | a write's count returned as a Java value | the product's count statement |
| D46 | an effect or write run while a database side is being planned | a step of the plan, run in order |
| dead | `BodyDriver.evalValue` (no caller) | deleted |

### 1.B Product features the test road has and the product lacks: they move INTO the product planner

Each is a fact of the Pure language, not of testing; a product program using the construct gets it too. Moving one
changes nothing for a product query that does not use it.

| D | The feature |
|---|---|
| (gate) | a multi-statement read-only body (today refused by the inliner unless it writes): `PlanMaker.program` plans any body |
| D4 | a from()/execute context's connection time zone |
| D6 | the context's feature flags and the tree's |
| D9 | the inliner's consumed lets prefixed to the lowering (a surviving `$let` read resolves) |
| D10 | the connection's post-processors (table renames, extractCtes, nonExecutable) |
| D13 | the resolver's let bindings, plan rows and handle registrar (`executionPlan(...)`, `$plan`) |
| D14 | `planToString`/`toSQLString` as plan-time strings (native staging) |
| D15 | the Result-envelope splice: `execute(...)` inside a program, its readers spliced |
| D16 | the driver-PK and import-data-flow appends; the declared collection root |
| D17 | DDL text generators and orchestration handles answered with no statement |
| D29 | raw-grid and late-bound-grid reads |
| D30 | the to-one reader's exactly-one check (if the product lacks it: verified first) |
| D35 | a from()'s inline test data and its named runtime as setup |
| D37 | metamodel reads inside a program (`Metamodel` in `program`, today `Metamodel.NONE`) |
| R11 | effects (`executeInDb`, the DDL natives) as `Effect` steps |
| D26 | a literal root answered as a constant (no round trip): proposed as a product feature, §7 Q6 |

### 1.C Test-only, kept, named (the whole list)

| # | Construct | What it is | D/R |
|---|---|---|---|
| T1 | the verdict step | `ExecutionPlan.Verdict`: one assert as data; its base sides are product plans (or constants the test wrote) | R7, R8 |
| T2 | the fused verdict group | `ExecutionPlan.Verdicts.fused`: a run of asserts as one statement, one row per assert by index; its frames as shared CTEs; the keyed sub-group for raw-grid reads; the split rung (at planning: member by member; at run: each member's own wrapper) and appeals | R9, D31, D45 |
| T3 | the database judge's lowering of an assert, SELF-CONTAINED | the wrapper around the sides: the canon columns (D22), the identity lane and a minter shared by the sides of one verdict (D1, D2, D20), JSON key order (D23), scan order inside the canon wrap (D24), the one-document graph form for JSON asserts (D7), a helper call that inlines to an assert root (D19) -- the assert's semantics in SQL, applied OVER the sides' product plans. It answers the WHOLE verdict: whether the assert holds (its polarity, `assertNotEquals`'s and `assertFalse`'s NOT, inside the SQL) and the finished failure message; Java decides nothing (1.E) | D1-D2, D7, D19-D24 |
| T4 | the host judge | Java compares the sides' typed rows by the step's check (equals, same elements, size, tolerance, grid, JSON, contains, ...); the assert-boundary order (D24 host) as §7 Q4 rules | R7, D24 |
| T5 | the sink | the test listener (verdicts, unjudged reasons, the last answer); the BOTH mode's agreement (D44); the trace stamp reserved for a fused statement (D49) | D44, D49 |
| T6 | the corpus lanes' engine row order | `StableScanOrder` for the corpus goldens (980 tests changed on the host pass, 666 on the database pass); today a JVM global read by the DuckDB dialect, after a run option the lane passes (§7 Q8) | D25 |
| T7 | effects as one script per run of consecutive effects | the session's script bracket | D34 |
| T8 | the test-data generator's two-plan body | the generator's plan runs first, its rows become the next plan's declared inputs | D38 |
| T9 | a computed effect string | a step whose one cell is the next step's statement text (the keyed pattern) | D39 |
| T10 | `assertError` | a step whose sub-plan is run expecting a failure, the failure handed to the matcher | R17 |
| T11 | the engine-text asserts and activity SQL | the engine-spelled text planned as a constant; the text comparison a constant verdict of the plan; where the texts differ, the rows leg one statement comparing both queries' rows on the lane's database | R10, D47 |
| T13 | the engine-text REFEREE replay | the golden SQL replayed on an H2 referee database (a second database: it judges legend-engine's own SQL text, not our results): a separate test instrument, outside both judges and outside the body's plan (the user, 2026-10-31) | R10 |
| T12 | staged raw grids | a raw grid read after an effect held in a temporary table | R6, D12 |

### 1.D Order: the database judge's deferral (a hazard found by the inventory)

Today a database-mode segment runs its value statements, its staged lets AND its store writes first, then its fused
verdict statement (D33): a write does not close a segment, so an assert written before a write in one segment is
judged on the state after the write. The engine judges it before. The plan: a verdict group closes before any step
that changes the database (a write, a staged let, an effect), so every assert sees the state its position in the
program sees. Measured first (how many corpus and channel B bodies have that shape; §6 P0), posted, §7 Q2.

### 1.E The database judge, self-contained (RULED 2026-10-31)

Today the database judge is not wholly the database's. In Java it applies the polarity (`held == wantEqual`,
`DatabaseJudge.verdictOf`, DatabaseJudge.java:227-258), turns an unjudged reason into a failure and assembles the
failure message from two cells; and three arms judge in Java even on the database pass, at the assert, outside the
fused statement (D43): the grid-pair `assertEquals` (`TdsCompare.grids`, AssertVerdicts.java:319-345, though
`VerdictSql.gridPair` exists), class-instance `assertEq` (`HostJudge.eq`, DatabaseJudge.java:643-652), and the
engine-text asserts (sides evaluated at the assert, the appeal's rows leg in Java). The user's ruling: all of it in the
database.

| Today, in Java | After |
|---|---|
| the polarity | the verdict statement answers whether the assert HOLDS (a `NOT` for the negated asserts) |
| the failure message from two cells | the statement answers the finished message text; Java passes it on as any query result is passed on |
| the grid pair (`TdsCompare.grids`) | `VerdictSql.gridPair`; its verdicts measured against today's and every moved one posted |
| class-instance `assertEq` (`HostJudge.eq`) | identity over the `__id` the identity lane writes; a short design first |
| a verdict decided statically at planning (the cross-kind gate, a static column mismatch) | a verdict step with a constant answer: a fact of the plan, nothing run |
| an unjudged shape (refused at planning) | a named plan entry, "no database form" with its reason; the database pass fails it, as today |
| the engine-text asserts | T11: the texts' comparison a constant, the rows leg one statement; T13 the referee replay, separate |

The host judge stays the independent referee (its 5,847-of-5,847 agreement is what says the canon SQL is right), but
nothing depends on it: deleting it would be a small change, never a project.

## 2. The target

```
Execution.runProgram(body, runtime, options, listener)
  -> the body's TypedQuery                      (the product's: Compiler.query)
  -> PlanMaker.program(query, runtime, options) (the product's; 1.B added; asserts planned by the planner's
                                                 assert module into T1/T2/T3)
       = ExecutionPlan(Sequence[ statements, staged lets, writes, Effect steps, Verdicts{fused, Verdict{base, wrapper}},
                                 ..., the last statement's answer ])
  -> PlanRunner.program(plan, sessions, judging, sink)   (the runtime's, unchanged)
  -> sink = the judges and the listener                  (the driver: T4, T5)
```

The driver is `runProgram`, the sink and the two judges. `BodyCompiler`, `Segments`, `BodyDriver`'s planning,
`JudgePlans`' planning half, the routing of `AssertVerdicts`, the planning of `DatabaseJudge` and of
`SqlTextVerdicts`, and `VerdictBatch`'s composition at the flush move to the planner or go. By the inventory's
estimate (its §6) that is about 3,100 lines of planning in `driver/` today, beside about 2,000 of running and 4,150 of
judging; the judging stays.

## 3. The verdict step, as the planner will make it

The record exists (S1b). The planner fills it for every assert, before anything runs, from the assert's family:

```
Verdict(label,                          -- which assert, at which statement (the listener reports it)
        check,                          -- the host judge's comparison: EQUALS, NOT_EQUALS, SAME_ELEMENTS, SIZE,
                                        --   TOLERANCE, TDS_EQUIVALENT, JSON, CONTAINS, EMPTY, ... (today arm +
                                        --   wantEqual; folded into one value, §7 Q5)
        base:    [side plan | constant] -- each side the product plan of its expression (T1)
        database: statement | constant | none)
                                        -- the database judge's lowering of the assert over the base (T3): one
                                        --   statement answering (holds, message, unjudged); a constant verdict where
                                        --   planning decides it; none, with its reason, where the database has no
                                        --   form (the database pass fails it, as today)
Verdicts(fused, [Verdict...])           -- a run of verdicts as one statement (T2), closed before any step that
                                        --   changes the database (1.D)
```

## 4. What each judge mode runs

| Mode | The runner sends | The sink does |
|---|---|---|
| HOST | each verdict's base sides | the host judge compares the rows by `check` |
| DATABASE | each group's fused statement (on rejection, each member's own statement) | reports each verdict's row as it is: holds, or fails with the message the statement wrote, or unjudged with its reason; it decides nothing (1.E) |
| BOTH | both | both judges, then their agreement (today 5,847 of 5,847 corpus asserts agree) |

## 5. Oracles and guards

- **Results: zero movement.** Every corpus roster on both judges, channel B, the five core lanes, the heavy lanes by
  hand, and the lane sessions on every server and cloud backend: every test's outcome and message identical. A moved
  outcome stops the step and is posted.
- **Statements: every change posted, one class at a time, before it lands.** Converging 1.A changes SQL text; 1.D
  changes order. Each class is shown with one example before and after and its count; it lands on the user's word.
  The statement-origin census and the lane sessions' statement-by-statement comparison measure it.
- **The parity guard (new, permanent):** every value statement and every host side the corpus and channel B plan is
  byte-identical to `PlanMaker`'s plan of the same expression with the same lets. It starts as a census (the count of
  each difference class, shrinking step by step) and ends as a test that fails on any difference.
- **`theDriverCompilesNothing`:** no class of `com.legend.driver` depends on `compiler.spec..`, `resolver..`,
  `lowering..` or `sql.dialect..`; the layer file's driver line cut to match.
- **One road:** the twenty roads' planning deleted from the driver; the ledger's pins shrink to zero for them.

## 6. The steps

Each step: its brief to the user first where §7 leaves a choice; the full local chain (the corpora uncached with
their censuses, channel B, the gate, the Postgres targets unprivileged); the lane sessions; the records; one commit.

| Step | What | Size |
|---|---|---|
| **P0** | Measure, no product change: (a) the parity census -- every value statement and host side planned both ways, each difference attributed to its cause (the probe is written and parked); (b) the order hazard of 1.D, counted; (c) D27's VALUE-shape equivalence; (d) D30, whether the product enforces `toOne` at its reader | 1 day |
| **P1** | The product planner gains 1.B, feature by feature, each with its own product tests; `PlanMaker.program` plans any body | 4-5 days |
| **P2** | The test body's non-assert statements (values, lets, staged lets, writes, effects) planned by `PlanMaker.program`; the parity guard on value statements reaches zero; 1.A's value-statement rows land | 3 days |
| **P3** | Host-judge verdicts: the planner's assert module classifies every assert (from `AssertVerdicts`), the host sides are product plans, `Verdict` steps planned before the body runs; the host arms take rows only; the parity guard on host sides reaches zero | 4 days |
| **P4** | Database-judge verdicts: the wrapper composed by the planner over the product side plans (T3), the fused group composed at planning (from `VerdictBatch`'s flush); 1.D's closing rule; the database judge self-contained (1.E: polarity and message in SQL, the three Java arms given SQL forms, constant and no-form verdicts as plan entries); T13 the referee replay moved out of the judge | 5 days |
| **P5** | One plan per body: `runProgram` = `PlanMaker.program` + `PlanRunner.program` + the sink; `BodyCompiler`, `Segments` and the driver's planning deleted; `theDriverCompilesNothing`; the parity guard permanent | 2 days |
| **P6** | The kinds that feed planning: T8 the generator's two-plan body, T9 a computed effect string, an `execute(...)` met while inlining another statement (D15), T10 `assertError`, T11 the engine-text asserts | 3 days |
| **Z** | An independent audit, the records, CI on every OS; the landing is the user's | 1 day |

About four weeks of sessions. P1 and P2 are the largest and the safest (product features with product tests); P3 and
P4 carry the order and text changes the user rules on.

## 7. Questions for the user

1. **The eager query run at a let (host pass, D32):** keep it as a `Check` step (the engine's order; a broken query
   fails at its let; 1,974 statements on DuckDB, 1,950 on H2), or drop it (a broken query then fails at its first
   reader). Recommended: keep.
2. **The verdict group's closing rule (1.D):** a group closes before any step that changes the database, so an assert
   sees the state its position sees. Recommended: yes; the moved verdicts counted and posted at P0.
3. ~~Host verdicts inside the database judge~~ **RULED 2026-10-31:** the database judge is self-contained, all in the
   database; the referee replay is a separate thing (1.E, T13).
4. **The assert-boundary order on host sides (D24):** keep the `ORDER BY` the planner adds to a grid under
   assertion (a named test-only rule, today's), or move order-insensitivity into the Java compare. Recommended: keep,
   named.
5. **The verdict record:** fold `arm` + `wantEqual` into one `check` value (as §3), the database's polarity inside its
   wrapper. Recommended: yes (no statement changes; the plan's JSON format bumps).
6. **Literal roots as constants (D26):** a product feature (any plan answers a literal root with no round trip), or
   test-only. Recommended: product.
7. **The metamodel store:** the plan names its tables as the product's `Metamodel.read` does; the system database's
   session, seeded once per graph, stays a runner optimization. Recommended: yes.
8. **The engine row-order emulation (T6):** from a JVM global to a run option the corpus lanes pass. Recommended: yes.
9. **Two unjudged reason texts** read "AssertVerdicts.side" where "side" was meant (a rename that reached the strings;
   DatabaseJudge.java:116-117, :298). They appear in failure messages and census keys: correct them, or leave them.
   Recommended: correct (a message spelling change, so it is asked).
