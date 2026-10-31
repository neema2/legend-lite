# Audit of `docs/TEST_IS_THE_PRODUCT_PLAN_2026_10_31.md` (independent, read-only, 2026-10-31)

Auditor: an independent session; nothing edited, built or run. Plan read at HEAD `b3735bd6` (382 lines). Evidence:
the plan, `docs/test-is-product-homework/A_CODE_INVENTORY.md` (373 lines), and the code under
`core/src/main/java/com/legend/` read with grep/sed. "Plan Lnn" = line nn of the plan file; "Inv" = the inventory.
Severity: BLOCKER (execution cannot start or will go wrong as written), MAJOR (a step will stall or an outcome will
move unannounced), MINOR (wording, stale reference, small gap).

(Sections are written in order; a section missing at the end means the audit was interrupted there.)

## 1. Internal consistency

**C1 -- MAJOR. The status line and §7's header contradict the rulings.** Plan L3: "Status: **DRAFT, not ratified.**
No step of §6 starts until the user agrees to §1-§5 and answers §7." and L334: "**Revised after the user's
questions, awaiting the user's yes:**" (decisions 10 and 7). Per the brief the user has agreed all §7
recommendations and both revised ones. A fresh session reading the doc will believe 7 and 10 are open and will stop.
Fix: a dated "RULED" block that lists all fourteen decisions in their FINAL wording, with the user's quoted words;
move "The questions as they were asked" to an appendix marked "history -- superseded by the rulings above".

**C2 -- MAJOR. Decision 10 (ruled) is contradicted by 1.A, 1.C T3 and the old Q10 text.**
- 1.A row "D1, D20 (host), D2 (values)" (L45): "the product's object layout and a **fresh minter per statement**; the
  host compares identity over `__id` in Java". Decision 10 (L340-343) says the fix is "ids derived from the
  construction site, deterministic in every plan (a product change)". A fresh per-statement minter is exactly what
  breaks cross-side identity (see A1 below: it is wrong, not just stale).
- 1.C T3 (L89) still keeps "the identity lane and a minter shared by the sides of one verdict (D1, D2, D20)" inside
  the test-only wrapper. Under decision 10 the identity lane and the shared minter GO (the side is the product plan;
  ids are site-derived), so T3 must drop them.
- Q10's text (L377-382) still recommends the pre-ruling compromise ("over the product plan where P0 shows it
  expresses the same canon; the rest named").
Fix: rewrite 1.A's D1/D2/D20 row as "the product's object layout; ids derived from the construction site (decision
10) in every plan, product included"; delete the identity lane and shared minter from T3; mark Q10 superseded.

**C3 -- MAJOR. Decision 7 (ruled) is contradicted by 1.F, Q7 and the rest of the body.**
- 1.F (L156): "**the two sides read different databases** ... **impossible as one statement**: a named `none`".
  Decision 7 (L347-348): "1.F's one impossible case goes".
- Q7 (L371-372) recommends the opposite of the ruling: "the system database's session, seeded once per graph, stays a
  runner optimization."
- Nothing in §2, §4 or §6 implements decision 7: no step deletes `SystemDatabase`, `openRoute`, `SystemRoutes`'
  renames, `ExecuteOptions.withSystemConnections`; no step moves the system seeds into the test session.
Fix: 1.F's row -> "after decision 7: one statement on the test session"; mark Q7 superseded; add decision 7 as an
explicit sub-step (see §5 of this audit) with its own measurement and lane checkpoint.

**C4 -- MAJOR. Decision 14 (ruled: "consecutive effects one script, as a product feature") contradicts 1.C T7.**
T7 (L93) lists "effects as one script per run of consecutive effects" in "Test-only, kept, named (the whole list)".
After decision 14 it belongs in 1.B. Likewise decision 6 vs 1.B's D26 row (L81: "**proposed** as a product feature,
§7 Q6") -- ruled, not proposed. Fix: move T7 to 1.B; reword D26's row; renumber nothing (cite T7 as "moved to 1.B").

**C5 -- MAJOR. Decision 1 (ruled: "keep the eager run at a let as a plan step") is placed nowhere in the body.** D32 is
in none of 1.A-1.E, no step implements it, and the plan never says whether the DATABASE pass now runs it too. Under
"the database judge has zero difference from the product plan" there is one plan for both modes, so the database
pass would gain ~1,974 (DuckDB) / ~1,950 (H2) statements (the inventory's D32; H report `statements.LET`), and the
product's `PlanMaker.program` would run `execute(...)` at its let for product queries as well. Fix: a 1.B row for D32
stating (a) it is a product rule, (b) it runs in every judge mode, (c) the statement-count change is posted at P2.

**C6 -- MAJOR. "Byte for byte" is asserted where the plan's own T3 keeps inner rewrites.** §0 (L31-33): "Every value
statement, let, write, effect and assert side is planned exactly as the product plans the same expression; a parity
guard checks it byte for byte." §3 (L188): "each side the product plan of its expression, byte for byte". But T3 keeps
D23 (`JsonKeyOrder.sort`, an IR pass that rewrites the side's OWN plan: `planner/SidePlanner.java:67-69`,
`sql/JsonKeyOrder.java:9-17` "Applied to a verdict side's plan only"), and the TDS canon wrap rewrites the inner
select twice (`ScanOrder.stabilize` and `Fold.conformValueEgress`, `lowering/CanonicalRenderSql.java:405-415`; the
latter is a named residue in AGENTS.md). T2 keeps "its frames as shared CTEs" while 1.B D15 splices frames into their
readers the product's way (pasted). T6 makes corpus-lane statements differ from the product's by a dialect pass.
Fix: state the parity claim as "byte-identical except the named verdict-side rewrites V1..Vn (D23 key sort, the TDS
wrap's scan order and egress conform, the frame CTE reference, T6's pass)", each a row with its count, and make the
guard's allowlist exactly those.

**C7 -- MINOR. §3's record is presented as existing but is a different record.** L180: "The record exists (S1b). The
planner fills it". The existing record is `Verdict(String name, String arm, boolean wantEqual, List<Side> base, Node
wrapper)` (`executionplan/ExecutionPlan.java:148-149`); §3 proposes `(label, check, base, database: statement |
constant | none(reason))`. Fix: say "the record changes (format 22 -> 23)" and list the TypeScript runner's pin
(`engine-client/src/plan-runner.ts:2,84`) as a consequence.

**C8 -- MINOR. §3.1's row contract is self-contradictory.** "`holds` BOOLEAN, never NULL" beside "`unjudged` NULL, or
the reason the statement could not decide" (L205-207): what is `holds` on an unjudged row? Fix: `holds` NULL iff
`unjudged` is not NULL, and the sink's rule for that row stated (fail with the unjudged message, as
`DatabaseJudge.unjudged`, `driver/DatabaseJudge.java:311-315`).

**C9 -- MINOR. T2 still lists "appeals" (L88).** After T11/T13 the text comparison is a planning-time constant and the
rows leg is planned only where the texts differ; there is no run-on-failure appeal left. Fix: delete "and appeals" or
say what still appeals.

**C10 -- MINOR. T6's wording is garbled and its consequence for the parity guard is missing.** L92: "today a JVM
global read by the DuckDB dialect, after a run option the lane passes (§7 Q8)". Decision 8 is ruled. A run option
that changes SQL text is a PLANNING option (it must reach `PlanMaker` through `ExecuteOptions`), so the guard must
compare corpus statements with the product planned under the same option. Fix: "to become a planning option the
corpus lanes pass (decision 8); the parity guard plans the product side with the same option".

**C11 -- MINOR. Two numbering vocabularies and orphan rulings.** The ruled list uses "decision 1..14"; the body cites
"§7 Q2/Q4/Q6/Q8" (L81, L90, L92, L107, L278). Rulings 11-14 have no question text in "The questions as they were
asked" (it stops at 10), so what the user agreed to for 11-14 is recorded only as a phrase. T13 is listed before T12.
Fix: one vocabulary ("decision n"), the text of 11-14 written out, T12/T13 in order.

**C12 -- MINOR. §3.1 "a let read by several members is one shared CTE (today's frame CTEs)" (L215) is not today's
behavior.** Today only `execute(...)` frames become `frame_<let>` CTEs (D31); ordinary lets are beta-inlined
(`UserCallInliner.java:196-205`, "a let used twice EVALUATES twice in SQL -- ... CTE sharing is the future fix").
Fix: say "a frame read by several members" or, better, make let materialization a product feature (see §5).

**C13 -- MINOR. §3.2's ROWS_ALL describes one check, but the host's quantified unroll yields one verdict per
element** (`driver/AssertVerdicts.java:700-717`), reported as ONE listener event per statement root
(`judge/VerdictBatch.java:383-428`, `Root`). §3's record has no root grouping. Fix: add `root` (the statement's
listener label) to the record and state "one listener event per root, its verdicts judged in order".

## 2. Completeness: every D and R placed?

Placement found (plan section): D1 1.A+T3; D2 1.A+T3+dec.10; D3 1.A; D4 1.B; D5 1.A; D6 1.B; D7 1.A+T3; D8 1.A; D9 1.B;
D10 1.B; **D11 none**; D12 T12; D13 1.B; D14 1.B; D15 1.B+P6; D16 1.B; D17 1.B; D18 1.A; D19 T3; D20 1.A+T3; D21 1.A;
D22 T3; D23 T3; D24 T3+T4+dec.4; D25 T6; D26 1.B; D27 1.A; D28 1.A (with D7); D29 1.B; D30 1.B+P0(d); D31 T2;
**D32 dec.1 only**; D33 1.D; D34 T7 (contradicted by dec.14); D35 1.B (half); D36 1.A; D37 1.B+dec.7; D38 T8; D39 T9;
D40 1.A; D41 1.A; **D42 none**; D43 1.E; D44 T5; D45 T2 (half); D46 1.A; D47 T11; **D48 none**; D49 T5.
R1 P2; R2 1.B D15 (half); R3 D15/D31; **R4 none explicit**; R5 product; R6 T12; R7 T4; R8 T3; R9 T2; R10 T11+T13;
R11 1.B+T7; R12 product+D41; **R13 none**; R14 dec.7; R15 T8; R16 D35 (half); R17 T10; **R18 half**; R19 D14; R20 D7.

**G1 -- MAJOR. R13, the effectful map, is omitted.** `$sqls->map(s|executeInDb(...))`: the source is evaluated, its
elements pulled into Java and each bound as a string literal into the lambda's statement, run per element
(Inv R13; `BodyDriver.effectfulMap`, BodyDriver.java:1108-1145; `SeedSqlForms.mappedExecutionForm`). It is a
run-then-plan kind exactly like T9 (a value computed by the database becomes statement text), and a plan made before
anything runs cannot hold it. Fix: a T-row (or a 1.B row if it is a language fact): "a step whose rows become the
texts of the next steps" -- the Keyed pattern over N rows, i.e. T9 generalized -- placed in P6, measured in P0.

**G2 -- MAJOR. D35/R16 is half placed: the per-statement, state-dependent seeding has no design.** 1.B says only "a
from()'s inline test data and its named runtime as setup". Today seeds run before EVERY statement, keyed by
`SeedSources`, skipped when the session holds the key "and has run no WRITING statement since", re-run after a write
("the per-statement tolerance the harnesses rely on", `driver/BodyDriver.java:723-790`), each recorded for the
referee ledger. A plan's `Target.setup` is a property of the target, and `PlanRunner.program` refuses steps whose
targets differ (`exec/PlanRunner.java:218-224`; `Target` includes `setup`, `ExecutionPlan.java:684`). So two
statements whose from()s carry different inline data cannot be one program, and a re-seed after a write cannot be
expressed at all. Fix: a `Setup` step kind (seed steps at a statement's position, run by the runner against the
session's memo), the memo rule stated, and the referee ledger's recording named (T5 or T13).

**G3 -- MAJOR. D42 (transactions) is omitted, and it is not "equivalent".** Today a body runs in one transaction
only when it stages rows or writes (`BodyDriver.java:71-79`); an effect script is bracketed by `dialect.script` only
when the session owns the transaction. `PlanRunner.program` brackets the WHOLE program in one transaction as soon as
any step is an `Update` or an `Effect` (`PlanRunner.java:160-173, 237-281`). So every effect body (up to 281 corpus bodies end
in an effect, H report L110) moves into one transaction: DDL semantics differ per engine (H2/MySQL/MariaDB commit
implicitly, DuckDB/Postgres do not), a failing assert rolls the effects back where today they persist into the
session the next test reuses, and on Postgres a rejected fused statement aborts the transaction so the split rung's
wrappers fail -- the predecessor plan's F11 (`docs/PLANNER_DRIVER_SEAM_PROGRAM_2026_10_30.md` L157-164, L285-291:
"a savepoint, measured first"), which this plan silently drops. Fix: a 1.A/1.D row for D42 with the transaction rule,
F11's savepoint carried over verbatim, and a P0 count of bodies whose transaction scope changes.

**G4 -- MINOR. D11 (deferred-pivot refusal by name) and D48 (EnumValueTables) are not placed.** D48 is harmless today
(Inv D48) but becomes live the moment test statements get bound parameters (D8's `renderStatement`). Fix: two 1.A rows.

**G5 -- MINOR. R4 (value-position `execute(...)`, run even under a batch) and R18's `print`/`println` are not placed
explicitly.** `print`'s argument is "NEVER evaluated (it may be an unlowerable diagnostic)" (Inv §5 row, BodyDriver.java
:681-683) -- under 1.B the product would lower it unless the rule moves too. Fix: name both in 1.B/D17.

**G6 -- MAJOR. D19 is misplaced.** T3 (database judge's lowering) carries D19 "a helper call that inlines to an assert
root", but D19 is a STATEMENT-LEVEL classification (a value statement becomes a verdict, NUM) applied in both judge
modes (`BodyDriver.java:320-336`, "adjudicating them here as plain equality regressed ~200 text-golden flips"). It
belongs to the planner's assert module (classification), not to the database wrapper. Fix: move to §3's
classification paragraph, both modes.

**G7 -- MAJOR. The user's rulings are not all in the body text.** Decisions 1, 7, 8, 9, 10, 14 change behavior but
none has a step in §6 or a row in §1 that implements it (C3, C4, C5 above); decision 10's "about a week more" is not in
§6's sizes or the "about four weeks" total; decision 9's message change is not in §5's movement list. Fix: one
"rulings -> where implemented" table (decision, section, step, posted movement class).

**G8 -- MAJOR. The census counters and the statement-origin tags are not placed.** The oracle of every step is "the
corpora uncached with their censuses" and "the statement-origin census" (§5, §6). Today census counters are bumped
at PLANNING inside the judges (`CanonicalDivergence.sqlJudgedInDatabase/sqlUnjudged/sqlDeclined`, e.g.
`DatabaseJudge.java:69,76,312`) and origins are entered by the driver around each call (`StatementOrigin.enter(SEED|
SYSTEM|BODY|FALLBACK)`, e.g. `BodyDriver.java:782`, `SystemDatabase.java:223`, `VerdictBatch.java:375`). Once the planner
plans and `PlanRunner.program` runs everything, (a) `plannerIsExecutionFree` forbids the planner from touching
`com.legend.judge` (`ArchitectureTest.java:575-585`), so the counters must move to the sink, and (b) the runner must
tag each step's origin, or the census moves for accounting reasons alone. Fix: a T5 sub-row "the census: counted by
the sink from facts the plan carries; each step carries its origin"; P0 snapshots the census for comparison.

## 3. Claims vs code (spot checks)

Confirmed as stated:
- `PlanMaker.program` is taken only when `query.writes()` (`planner/PlanMaker.java:57-62`); it plans with
  `Metamodel.NONE` (:115-117); lets accumulate and are re-inlined per statement (`with(lets, stmt)`, :177-182); it emits
  only `Sequence/Update/Check/TdsResult` (+`Keyed` in `plan`); `new ExecutionPlan.Effect`/`Verdicts` appear only in
  `PlanRunner.java:473`, `BodyDriver.java:972`, `TestDataGenerator.java:509`, `HostSides.java:78`, `VerdictBatch.java:
  353,458,530` and `PlanJson` (grep).
- The inliner refusal: "only let statements may precede the query expression" (`compiler/spec/UserCallInliner.java:
  208-210`).
- `objectLayoutOf` = `layoutOf(ctx,t,true)` minus `__canon` (`compiler/element/ClassLayouts.java:134-145`, filter at
  :140-141).
- The fused group is composed in `VerdictBatch.flush` at the flush, AFTER the segment's values ran (`judge/
  VerdictBatch.java:285-382`; `BodyCompiler.Segments.closeVerdicts`), one fused statement per SESSION (:295-302), and its
  members carry no base and no wrapper: `new ExecutionPlan.Verdict(p.name(), armOf(p.name()), p.wantEqual(),
  List.of(), null)` (:349-352). Single wrappers are planned lazily in the root loop "so nothing is sent past the first
  failure" (:304-306, :386-391).
- `DatabaseJudge.verdictOf` decides polarity (`held == wantEqual`) and assembles both messages in Java, with
  `AssertVerdicts.excerpt` bounding the unjudged evidence and census side effects (`driver/DatabaseJudge.java:227-258`).
- The three Java arms inside the database judge: the grid pair, in ANY judge mode, intercepted before the arm
  (`driver/AssertVerdicts.java:318-345`, `TdsCompare.grids`); class-instance `assertEq` -> `HostJudge.eq`
  (`DatabaseJudge.java:643-652`); the engine-text asserts with their appeal (`SqlTextVerdicts.java:1362-1373`,
  `VerdictBatch.java:396-405`).
- `SystemDatabase`: one per graph per engine, a private in-process database or the host's SYSTEM connection, seeded
  once under `StatementOrigin.SYSTEM`, read-only after, temporary tables + renames on servers, closed by a Cleaner
  (`driver/SystemDatabase.java:20-56, 115-135, 175-225`).
- `PlanRunner.program`: one `ProgramSink` (`verdict`, `fused`, `sides` + the `ResultSink` answer); the answer is HELD in
  memory and streamed after all steps (`exec/PlanRunner.java:235, 283-290`); refuses >1 result step (:227-230) and
  steps naming different targets (:218-224).

Wrong or overstated:

**W1 -- BLOCKER. §2: "`PlanRunner.program(plan, sessions, judging, sink)` (the runtime's, unchanged)".** It cannot run
a test body unchanged: (a) it refuses a program of constant steps alone, "names no target: nothing to run"
(`PlanRunner.java:231-233`) -- every body whose asserts all fold to constants (more of them after decision 6) is
refused; (b) it refuses steps with different targets, and `Target` carries `setup` (`ExecutionPlan.java:684`), so
per-from() seeds (G2) cannot be expressed; (c) it has no step kind for `assertError` (T10), a seed step, a text-feeding
step (T9/R13: `Keyed.KeysOf` is only `ROWS, COLUMNS`, `ExecutionPlan.java:388`), the generator's two plans (T8); (d) its
transaction rule (any `Update`/`Effect` -> one transaction, :160-173, 237-281) changes effect bodies (G3) and carries
F11. Fix: §2 lists the runner's additions as plan-format changes (each a node kind, the TypeScript runner's refusal or
support, a version bump) and sizes them.

**W2 -- MAJOR. Decision 10: "so the wrapper computes `__canon` over the product plan's columns".** Overstated.
`__canon` is stamped at the CONSTRUCTION site from sibling fields and the class's key tree, deliberately, so "a
recursive polymorphic shape's text stays linear in its size" (`lowering/ConstructionCanon.java:21-28`;
`CanonicalRenderSql.constructionCanon`, :859-905). Over product columns: struct-carried keyed instances have a
precedent (X5, `instanceCanon` over the struct's fields, `CanonicalRenderSql.java:225-235`); but a JSON-carried
(Any/variant) object's canon is read from its own `__canon` and "an object without one -- a producer outside the
construction sites -- is its identity" (:950-958), so after decision 10 such keyed objects compare by IDENTITY instead
of by keys; a polymorphic slot needs the runtime class's key tree (a CASE over `__type`); and where the product
writes `CAST(NULL AS VARCHAR)` for an untracked or incomplete evaluation's id (`lowering/Lowerer.java:305-307`), the
identity lane writes a per-node id today -- `PureAsserts.assertEq` then throws "eq is INSTANCE identity, which is not
observable" on an id-less instance (`judge/PureAsserts.java:164-181`). "nothing named at the end" (L344) is not
supported. Fix: decision 10 delivered as measured slices with a named residue list allowed (each with its count),
P0 counting the four classes above.

**W3 -- BLOCKER. 1.A: host identity "over `__id`" with "a fresh minter per statement".** The product's minter
(`lowering/ObjectSites.java:14-24`) mints `"o" + ordinal` on first sight, one instance per lowerer
(`TypedQuery.java:171`). Two sides planned separately each start at `o1`, so `let a = ^A(); let b = ^A();
assertEq($a, $b)` gives `o1` vs `o1` -- equal -- where the engine says distinct; and `PureAsserts.assertEq` is
correct only because "ids are unique per site" (`PureAsserts.java:165-168`, the body-wide `InstanceIds`,
`planner/InstanceIds.java:14-16`). Fix: ids derived from the construction site (decision 10's id half) MUST land
before or with P3 (host sides as product plans), not inside P4.

**W4 -- MAJOR. P0(a): "the parity census -- every value statement and host side planned both ways ... (the probe is
written and parked)".** The parked probe (`.../scratchpad/homework/probe/ParityProbe.java`, not in the repository)
re-lowers the test road's ALREADY inlined and resolved body under ten Lowerer factors (LAYOUT, LANE, EGRESS, TZ,
EXISTS, SEED, POST, ENUM, FEATURES, STREAM) and renders with `dialect.render`. It does not plan the product way end to
end: not `TypedQuery.resolve` (D13's resolver bindings, D9's queryLets, D15's splice), not the assert-boundary order
(D24), the shape (D27), the target (D36), `renderStatement` (D8) or the canon wrap. `ParityC` measures something
else (each assert's predicate as a plain value, host pass only). Fix: P0(a) restated as what the probe measures, plus
the missing measurements, and the probe committed under `docs/test-is-product-homework/probes/` as `.java.txt`.

**W5 -- BLOCKER. T13 "a separate test instrument, outside both judges" is not neutral: today the referee's replay
DECIDES ~1,250 verdicts per corpus pass.** When the texts differ, the rows leg compares OUR rows with the golden
REPLAYED ON THE REFEREE (`referee.verify`/`verifyPlan`, `driver/SqlTextVerdicts.java:1448-1459`) and the outcome is
the verdict: `MATCH -> ok()` ("rows are the verdict (§0); text is a census number"), `DIVERGED/FAULT -> fail`,
`DECLINED -> text decides` (:1461-1480), in both judge modes (HOST :1376-1383; DATABASE as the appeal :1362-1373). The
H report's census shows `REFEREE_GOLDEN=1258` and `REFEREE_OURS=1188` on the DuckDB passes
(`docs/planner-driver-seam-homework/H_REPORT_2026_10_30.md` L293). T11's replacement, "the rows leg one statement
comparing both queries' rows on the lane's database", runs legend-engine's H2-spelled golden on DuckDB/Postgres/...,
which will not always parse or agree. Fix: P0 counts the MATCH/DIVERGED/DECLINED outcomes today; the plan states what
decides each after; the movement is posted to the user BEFORE T13 is built (the ruling was taken without this number).

**W6 -- MINOR. §3.1: the message "spelled exactly as today's (`<assert>\nexpected: <text>\nactual:   <text>`, ...)".**
There are several templates today: the equality row (`DatabaseJudge.java:254-257`), the unjudged one with bounded
excerpts (:237-241, :313-314), and the grid pair's `name + ":\n" + summarize(e) + "\n does not match:\n" + summarize(a)`
(`AssertVerdicts.java:339-341`, `HostJudge.summarize` = column names + row count, `HostJudge.java:637-640`). Fix: a
per-arm message table, each with its SQL spelling and its dialect-specific newline/concat/NULL handling.

**W7 -- MAJOR. Decision 4's stated consequence ("a FAILURE message may list rows in another order") is understated.**
`ScanOrder.stabilize` places its key "at the same level" as a root LIMIT/OFFSET and collapses "a bare cap WRAPPER over
an orderable inner" into it (`sql/ScanOrder.java:27-30, 47-61`): on a capped unsorted read it decides WHICH rows come
back, which no order-insensitive Java comparison can restore; the user design of 2026-08-29 calls it "a FEATURE of the
assert surface, always on (positional reads over an unordered relation are otherwise semantically undefined)"
(:13-17; `JudgePlans.java:577-583`). T6's pass masks it on the DuckDB corpus lane only (D25), not on channel B's lanes.
Fix: re-post decision 4 with this consequence and a P0 count (host tabular sides whose plan stabilize changes, split
by "has LIMIT/OFFSET/at(N)"), and cite the 2026-08-29 ruling it overturns.

**W8 -- MINOR. Unsourced numbers.** 5,847/5,847 agreement, 0/5/1 unjudged (1.F), 980/666 (T6), 1,974/1,950 (Q1): no
GATES entry, census line or log is cited, so a fresh session cannot re-derive them (unverified here). Fix: cite each.

**W9 -- MINOR. §2's size estimate omits the planner side of the test road.** About 2,750 lines in `planner/`
(BodyPlanner 438, SidePlanner 128, FramePlanner 227, LetPlanner 154, SystemRoutes 81, EffectPlanner 182, EnginePlans
918, PlanAllocations 489, PlanEnv 80, Prepared 52; Inv §6b) plus `VerdictSql` 1,709 and `CanonicalRenderSql` 1,089 are
reshaped or deleted, and the plan never says which survive. Fix: a disposition column per class.

## 4. Outcome movement hidden by the oracle

§5 (L290-292): "**Results: zero movement.** ... every test's outcome and message identical. A moved outcome stops the
step and is posted." Decision 11 repeats it ("every outcome frozen"). At least eleven ruled or planned changes WILL or
MAY move outcomes or messages. As written, the oracle stops the program at the first of them, or (worse) a session
"fixes" each movement to get back to zero. **O0 -- BLOCKER (the presentation): §5 needs a movement ledger.** Each
class below gets an id (M1..), a P0 count with one example before/after, the user's ruling, and a committed
expected-movement register (test -> old outcome/message -> new), so the oracle is "frozen except the approved
register", and each class lands in its own commit (bisectable), never inside a refactor commit.

**O1 -- MAJOR. Decision 2 (a group closes before any database-changing step).** A DATABASE-pass assert written before a
write in its segment is judged on the post-write state today (Inv D33); after, on the pre-write state: a verdict CAN
flip (fail->pass or pass->fail), towards the host pass and the engine. The plan counts it (P0(b)) but §5 forbids the
result. Two hazards the plan and the inventory both miss:
- **Seeds.** `BodyDriver.establishContexts` runs a statement's seeds IMMEDIATELY during the walk, re-seeding when
  the from()'s inline data differs or a write happened (`BodyCompiler.java:118`, `BodyDriver.java:723-790`), while the
  segment's verdicts are deferred to its close: an assert before a re-seeding statement is judged on the re-seeded
  tables today. The closing rule must list seed steps, and P0(b) must count them.
- **Errors inside a group.** A value statement or a frame's eager run (decision 1) between two asserts does not close
  the group; if it raises, today's DATABASE pass reports its error and never the earlier failing assert, while the
  HOST pass reports the assert. One plan for both modes must say where the group SITS (its first or last member) and
  whether a `Check` closes it; either choice moves failure messages in one mode. Fix: groups close before every
  non-verdict step (the simplest rule that keeps both modes' order), its cost in fused-statement count measured.

**O2 -- MAJOR. Decision 4 (order-insensitivity into Java).** Beyond message row order (W7): (a) rows chosen under a
cap on an unsorted read change on the host pass; (b) BOTH mode's agreement compares a host channel without the
stabilize against a database channel that keeps it (T3), so the 5,847/5,847 agreement may drop; (c) on channel B's
non-DuckDB lanes nothing re-adds the order. Present: a count of host tabular sides whose SQL `ScanOrder.stabilize`
changes, split by cap/no cap, and the outcome diff of a host pass run without it, on DuckDB with several threads.

**O3 -- MINOR (ruled). Decision 9 changes failure-message and census-key text** ("AssertVerdicts.side" -> "side",
`DatabaseJudge.java:116-117, 298`). Present: in the movement register; the census keys' consumers updated in the same
commit.

**O4 -- MAJOR. Decision 10 moves more than SQL text.** (a) Ids reach OUTPUT: an instance cell prints `"@_" + id`
(`result/CellText.java:73-75`, "interpreted Pure's anonymous instance name"), so site-derived ids change product
output spelling, which the program's rules say needs the user's word case by case, not "posted" as SQL text;
(b) failure messages that carry a keyless instance's canon (`{"_type":...,"_id":"o3"}`, `constructionCanon`,
`CanonicalRenderSql.java:879-885`) change; (c) verdicts move where W2's classes apply (JSON-carried keyed objects
compared by identity; id-less untracked evaluations now raising). Present: three counts at P0 and an output-spelling
case list for the user.

**O5 -- MAJOR. Decision 7 (system tables in the test's session).** Movement and risk the plan does not mention:
statement order (the system seeds before the body on the test session instead of on another session: posted, but also
counted in `SEED`/`SYSTEM` origins); name clashes in process (the tables are created under the store's declared
schema names, `Ddl.metamodelSteps(def, schema.name(), ...)`, `SystemDatabase.java:182-188`, in the USER's database,
where a test's `dropAndCreateSchemaInDb` or a user table of the same name can collide); transactions (a temp table
created inside a body transaction that rolls back is gone on Postgres while the memo says "established"; G3);
privileges (the user's unprivileged connection must create TEMPORARY tables); servers without temporary tables get
the product's inline VALUES for ~20 tables in every metamodel statement (D37) -- statement size on the cloud lanes;
the memo's "no write since" rule re-seeds after every writing test; and the public `ExecuteOptions
.withSystemConnections` goes. It overturns two recorded user rulings (2026-09-02 "separate from every user
connection", 2026-10-11 "the host's system connection ... the platform holds no server credential",
`SystemDatabase.java:20-56`), which the plan should quote. Present: 97 corpus + 1 channel B system-route bodies
(H report H5) run before/after on every lane, with the statement diff by class.

**O6 -- MAJOR. The database judge's self-contained forms move verdicts and messages.** The grid pair: `TdsCompare.grids`
(column order, the order policy, its own cell equality) replaced by `VerdictSql.gridPair` (row canons, the 2-ULP float
leniency, TDSNull sentinel) -- the plan itself says "its verdicts measured against today's and every moved one posted"
(L123), i.e. it EXPECTS movement under a "zero movement" oracle; the message format changes too (W6). Class-instance
`assertEq` by `__id` in SQL: today an id-less instance is a LOUD NotImplemented (`PureAsserts.java:178-181`); in SQL it
becomes a verdict or `unjudged`. Polarity and message in SQL: newline, concatenation and NULL spelling per dialect,
and today's bounded excerpt (`AssertVerdicts.excerpt`) re-implemented in SQL. Present: a per-arm count on the DuckDB
and H2 database passes and channel B, before the arm moves.

**O7 -- MAJOR. 1.A's convergences are not all text-only.** D5 (the exists-join form off for temporal reads) changes
the SQL of milestoned reads in test bodies -- semantics, not spelling, until measured. D36 (`PlanMaker.target`)
introduces `oneDefinition`'s refusal of differing connections (`PlanMaker.java:617-628`) where the judges accept them,
and `forServer` only when versioned (the H2 2.4 finding, `JudgePlans.java:47-55`). D18 adds a cross-store refusal on
effect lets: new failures. D8's `renderStatement` writes H2 type holes where `render` wrote literals. D7 changes the
streamed answer's shape (one object per row instead of one array cell). Present: each in the movement ledger with a
count, not as "statement text, posted".

**O8 -- BLOCKER. Planning the whole body first (P5) moves failures.** Today a body is planned statement by statement,
interleaved with running; a failing assert ends the body before later statements are planned. After P5 every
statement is planned before anything runs, so a planning refusal (NotImplemented, a wall, a ModelException) in a
later statement surfaces INSTEAD of the earlier assert's failure. Worse, `assertError` catches `ModelException` --
a lowering-time "deferred-body guard" raised while ITS body is planned (`driver/AssertErrorNative.java:68-77`,
timeBucket's unit check; 23 channel B statements use assertError, H report L126): planned up front, that error escapes
the matcher and the test flips from pass to error. Fix: a planning failure becomes a plan step that raises at its
position ("refused(reason)"), and `assertError`'s sub-plan carries a planning failure as its expected error; both
measured at P0 (count of bodies with a planning failure after their first failing statement).

**O9 -- MAJOR. Frames: one form for both modes.** Today DATABASE-mode value statements and sides read a frame as a
`frame_<let>` CTE reference (`JudgePlans.withFrames`, :313-316; `BodyDriver.renderFor`, Inv D31) while HOST-mode ones
paste the chain (Inv D31). One plan for both modes means one form for value statements: one mode's text changes (and
the corpus H2 database pass's history of "660 row legs reading a frame that was not there", `JudgePlans.java:308-312`,
says the CTE form has outcome risk). Present: a class in the ledger, and preferably the CTE made a PRODUCT feature (a
let read twice materialized once -- the inliner's own "CTE sharing is the future fix", `UserCallInliner.java:200-204`),
so both modes and the product share it.

**O10 -- MAJOR. Transactions (G3)** move effect bodies' failure state and cross-test session state. Present: count of
bodies whose transaction scope changes; F11's savepoint.

## 5. Tractability of the steps

### 5.1 Step by step

**P0 (1 day) -- MAJOR: not executable as written, and too small.** (a) The probe measures lowering configuration only
(W4) and is not in the repository. (b)-(d) are right but incomplete. Missing measurements the rulings need: the
referee's MATCH/DIVERGED/DECLINED counts (W5); capped host sides under stabilize (W7/O2); W2's four canon classes and
the id classes (O4); seeds and errors inside a group (O1); bodies whose transaction scope changes (G3); planning
failures after a body's first failing statement and assertError's planning-time catches (O8); constant-only bodies
(W1); effectful maps and computed effect strings (G1, T9); DATABASE-mode frame CTE reads (O9); the census baseline
(G8). Realistic: 3-5 days (four corpus passes uncached, five channel B suites, writing the counts down).

**P1 (4-5 days) -- MAJOR: the largest unknown, under-designed and under-sized.** Sixteen features, each with
"its own product tests", several of which need a design and a user decision first:
- **Product exposure (decision for the user).** Effects (R11: `executeInDb`, the DDL natives), a from()'s inline
  `sqlSetups`/CSV seeds (D35) and the eager run at a let (decision 1) become things a PRODUCT query can do through
  `Execution.run`, i.e. the HTTP query service (`com.legend.server`). L61-62 "Moving one changes nothing for a product
  query that does not use it" is true and beside the point: a query that DOES use one would run user DDL/DML, or seed
  tables, on a production connection. Needs: refuse on the server door, or gate by a test connection, posted.
- **D15 (the splice)** is not a lift: `BodyDriver.spliceHook` is execution-bound by its own javadoc ("frame lookup,
  inline-execute frame builds (JDBC), and the aggregationAware rewrittenQuery print", Inv D15); frames, plan-execute
  peeling, activity SQL with a trace stamp reserved before the run (D47, D49), and the two read forms (O9) need a
  design written before code.
- **D35 + seeds** need a `Setup` step (G2); **D37 + decision 7** need its own sub-step (O5); **D26 + decision 6** need
  the runner to accept constant-only programs (W1); **R11 + decision 14** need the script bracket and the
  transaction rule (G3); **D17/R18** need the "print's argument never evaluated" rule in the product.
- Each node kind or record change bumps `PlanJson.VERSION` (22, `executionplan/PlanJson.java:70`) and the TypeScript
  runner's pin (`engine-client/src/plan-runner.ts:2,84`); `planner/` is compiled into the WebAssembly and native
  planners through `//core:plan_side` (`wasm/BUILD.bazel:29-45`), so every planning class added there ships in them
  (TeaVM limits; product size) -- the plan does not mention either.
Realistic: 3-4 weeks, split into sub-steps with their own commits and a checkpoint.

**P2 (3 days) -- MAJOR: the coexistence mechanism is missing.** "The test body's non-assert statements ... planned by
`PlanMaker.program`" while asserts still take the old road (P3/P4) means the old segment walk asks the product
planner statement by statement: `PlanMaker.program` is private and plans a whole body; a per-statement product entry
(lets so far + one statement -> its steps) is needed and should be named. Also: decision 1 lands here (C5), D7
changes the streamed answer (O7), the DATABASE-mode frame form changes (O9). Realistic: 1-1.5 weeks.

**P3 (4 days) -- BLOCKER as ordered; MAJOR as designed.** Ordering: host identity over `__id` across separately
planned sides needs decision 10's site-derived ids first (W3). Design: §3's record carries `check` and the sides, but
the host arms read much more at judge time today: the sides' static types (`typedSide(e, args.get(0))`,
`PureAsserts.assertEquals(e, a, type, type)`, `HostJudge.java:1228-1252`), the order view (`OrderView.of(args.get(1),
letPrefix)`), the keyed-class restriction read from the MODEL (`restrictToKeys(eVals, ik, env.ctx())`, :1224-1227),
the TDSNull sentinel direction, the literal-vs-computed message of a quantified assert, tolerance arguments. Each must
become plan data or the host judge keeps reaching the compiler (and `theDriverCompilesNothing` fails, §6). And BOTH
mode mid-transition (host on the new road, database on the old) is not addressed. Realistic: 1.5-2 weeks.

**P4 (5 days) -- MAJOR: three programs in one step.** (i) the fused group composed at planning (from `VerdictBatch`),
(ii) the self-contained database judge (polarity, per-arm message templates in SQL across 11 dialects, grid pair,
instance `assertEq`, constant and `none` verdicts, T11/T13 with W5's movement), (iii) decision 10's canon over the
product columns ("about a week more", L343, not in the 5 days). Plus 1.D's closing rule, which only has a meaning once
the body is one plan (P5) -- built in P4 it is built into `BodyCompiler.Segments`, which P5 deletes. Realistic:
2.5-3 weeks; split (see 5.3).

**P5 (2 days) -- BLOCKER as ordered.** "`BodyCompiler`, `Segments` and the driver's planning deleted" cannot happen
before P6: the generator's fold (T8), computed effect strings (T9), effectful maps (G1), inline `execute(...)` during
inlining (D15) and `assertError` (T10) still need the walk. And plan-first moves failures (O8). Fix: P6 before P5, or P5
keeps a named residue that P6 removes. Realistic: 1 week with O8's design.

**P6 (3 days) -- MAJOR.** T8 changes the generator's mechanism (folded INSTANCE LITERALS today,
`TestDataGenerationNatives.java:42-86`, vs "declared inputs" in T8: bound parameters change SQL text and types) and
breaks "one plan per body" (two plans; their transaction is unstated). T9 and R13 need a text-feeding step kind (W1).
T10 needs a savepoint on Postgres (an error inside the body's transaction aborts it, F11) and O8's planning-failure
capture; its Java matcher is an explicit exception to "Java decides nothing" (a database error is caught by the
client; the matcher compares message, line and column, `AssertErrorNative.java:85-128`) that the user has not ruled.
Realistic: 1.5 weeks.

**Z (1 day)** -- 2-3 days with CI on every OS and the lane sessions.

**Total.** The plan says "About four weeks of sessions" (L322). My estimate is 9-13 weeks. P1 and P2 are not "the
safest" (L322-323): P1 changes the product's surface.

### 5.2 Hidden risks not in the plan
- **Corpus lane time.** Decision 1 adds ~1,950-1,974 statements to each DATABASE pass; decision 2 and O1's fix split
  fused groups; pasted frames (O9) re-run chains; decision 7 seeds on the test sessions. The plan has no time budget
  per lane; `docs/GATES.md` has the 2026-09-02 budget entry that motivated `SystemDatabase` (`SystemDatabase.java:28-29`).
- **Dialects.** Messages, polarity and the grid pair in SQL must spell on all eleven dialects and both engine-text
  printers; the plan's lane checks come only at checkpoints. MongoDB's text is an aggregation pipeline
  (`docs/MONGODB_PLAN_2026_10_16.md`); whether the database judge runs there at all is unstated (unverified).
- **Plan readers.** The TypeScript runner refuses any version but 22; no Python plan reader was found (grep of
  `*.py`; unverified beyond that).
- **Windows/macOS.** Newlines in SQL-built messages vs Java's `"\n"`; CI on other OSes only at checkpoints.
- **Local-only commits.** "Commits stay local" (L306-307) on a cloud container whose disk is nearly full: a container
  loss loses every commit since the last checkpoint. Fix: push to a non-PR work branch (CI need not run), or keep a
  `git bundle` outside the container, at the end of every step.

### 5.3 A better breakdown (each line one or more local commits; ◆ = a checkpoint: push the branch, CI on every OS,
the lane sessions on every server and cloud backend, the movement register posted for the user's word)
1. **M0 Measure** (P0 widened, the probe committed as text, the census baseline, the movement ledger opened). ◆
2. **M1 Ids by construction site** (decision 10's id half; product output spelling posted case by case, O4). ◆
3. **M2 Product features, safe set**: D4, D6, D9, D10, D13, D14, D16, D17/R18, D29, D30, D26+constant-only programs.
4. **M3 Product features, decisions**: effects + scripts + transactions + F11 (G3), seeds as `Setup` steps (G2),
   the eager let run (decision 1), the splice and frames with one read form (D15, O9), the server door's refusals. ◆
5. **M4 System tables in the test session** (decision 7, O5), alone. ◆
6. **M5 Statements by the product**, per statement through a named product entry (P2). ◆
7. **M6 The verdict record and the host judge** (P3; the record carries every judge-time fact). ◆
8. **M7 The fused group at planning**, members with their own wrappers, the split rung (P4 i).
9. **M8 The database judge self-contained**, arm by arm, each arm its own commit and movement class (P4 ii), then the
   canon over product columns in slices (P4 iii). ◆
10. **M9 The run-then-plan kinds**: T9/R13 text-feeding steps, T8, T10, inline execute (P6). ◆
11. **M10 One plan per body** (P5 with O8's refusal steps; 1.D's rule now real; the guards). ◆
12. **Z** audit, records, landing on the user's word.

## 6. Guards and oracles

**K1 -- MAJOR. The parity guard is not yet well defined.** §5 (L296-298): "every value statement and every host side
the corpus and channel B plan is byte-identical to `PlanMaker`'s plan of the same expression with the same lets".
- "The same expression" is ambiguous once the test road has spliced frames (D15), folded generator literals (T8),
  synthesized per-element lets (the quantified unroll), inlined a helper to an assert root (D19), or re-bound a staged
  let (`StagedRows.boundTo`). A workable definition: "the last step of `PlanMaker.program` over the body's statements
  before this one plus the side expression as the last statement, under the same options (T6)".
- Until site-derived ids land (M1), every object-constructing statement differs in its `o<n>` literals (the body-wide
  `InstanceIds` vs a fresh `ObjectSites`), so the census cannot "reach zero" in P2/P3 as written.
- Byte identity needs the named exceptions of C6 as an allowlist.
- After P5 the test road IS `PlanMaker`, so "byte-identical to PlanMaker" is a tautology. The permanent form should be
  structural: an ArchUnit rule that only the planner's one statement entry constructs a body's statement plans
  (`Side.Rows`, `Check`, the verdict's subqueries), plus unit tests per kind, plus the census lane flag for the
  transition only.
Fix: write the definition, the exception list and the permanent form into §5.

**K2 -- MAJOR. `theDriverCompilesNothing` is achievable only with P3's missing design.** Today 15 of the 19 files in
`driver/` name `compiler.spec`, `resolver`, `lowering` or `sql.dialect` (grep count of qualified uses: AssertVerdicts
133, SqlTextVerdicts 120, BodyDriver 89, DatabaseJudge 58, HostJudge 30, BodyCompiler 17, SeedSqlForms 12, CsvLoad 8,
JudgePlans 8, AssertErrorNative 7, AgreementArm 3, VerdictArm 3, LiteralFold 3, HostSides 2, SystemDatabase 1). The
judges that STAY (HostJudge, AgreementArm, VerdictArm's interface, AssertErrorNative's matcher, SqlTextVerdicts'
judging half) take `TypedSpec` arguments and read `OrderView` (compiler.spec) and `CanonRider` (lowering) today; they
pass the rule only if the verdict record carries every judge-time fact (P3 gap) and the arm interface is redesigned.
Also `BodyDriver.ENGINE_TEXT` is an `EngineTextDialect` (sql.dialect). And "the layer file's driver line cut to match"
(L300): the Bazel `:compiler` library holds both `compiler.spec` and `compiler.element` (`core/BUILD.bazel:156-160`), so
the driver cannot drop `compiler` while `runProgram` needs `ModelContext`; only `lowering`, `resolver`, `sql_dialect`,
`normalizer`, `parser` can go from `tools/deps/core-layers.txt:111`. Fix: state the end state per file and per layer.

**K3 -- MAJOR. The guards that must move are not listed** (AGENTS.md: "A ceiling, pin, or allowlist entry moves ONLY
with a dated justification comment naming the task/incident"). At least: `ArchitectureTest.theDriverHasOneCaller`
(:558), `plannerIsExecutionFree` (:575; the assert module may not touch `com.legend.judge`, so `KindClass`, the census
and `CanonicalDivergence` calls move), `hostVerdictIsReachableOnlyFromTheVerdictSeam` (:1319, its exemption list names
AssertVerdicts, HostJudge, DatabaseJudge, AssertErrorNative, VerdictBatch), `theBodyDriverAndTheJudgesHoldNoJdbc`
(:1201), `onlyTheSeamMakesSessions` (:826; decision 7 deletes `SystemDatabase`'s opens), `execReachesNoPlanning`
(:1016) and `theRunnerIsModelFree` (:948) for the runner's new step kinds; `JavaEvalLedgerTest`'s per-file pins
(BodyDriver 1012, DatabaseJudge 596, AssertVerdicts 1226, ...; `JavaEvalLedgerTest.java:580-583, 830`) and its file
registers ("every file registers at birth", :1459-1486, and the planner list :1404-1421); `HarnessDisciplineTest`'s
driver and planner sections (:202-277); the `spec` registers the predecessor steps moved (the engine-order and
outside-body registers, `native-claims.tsv`, `parser-equivalence/ratchets.tsv`; IN_FLIGHT, S3b/S3c entries);
`PlanJson.VERSION` and the TypeScript pin. Fix: a "guards that move" table per step, each with its dated reason.

**K4 -- MINOR. The oracles are named but not defined.** "the five core lanes", "the heavy lanes by hand", "the
statement-origin census", "the lane sessions' statement-by-statement comparison" (§5, §6) have no command or file.
Fix: point to `docs/RESULT_BOUNDARY_START_HERE.md` §6 (commands) and §7 (lanes, the brief, `compare_lanes.py.txt`), and
name the census flag and the baseline (H report L293 is the latest snapshot).

## 7. Resumability: could a fresh session execute this plan from the documents alone? No.

Missing (each MAJOR unless noted):
1. **How to run every check**: the plan says "the full local chain (the corpora uncached with their censuses, channel
   B, the gate, the Postgres targets unprivileged)" (L305-306) and nothing else. Needs the commands (RESULT_BOUNDARY_
   START_HERE §6), `tools/cloud-distdir.sh`, the unprivileged Postgres wrapper (`docs/CLOUD_SESSIONS.md`), the disk
   budget, and how "uncached" is forced.
2. **The census**: how it is turned on, where it prints, the baseline to compare with.
3. **The lane sessions' role and access**: they measure only; their ids go stale (RESULT_BOUNDARY_START_HERE §7 already
   lists dead ones); the brief's template; how to raise new ones from another account.
4. **The rules**: L5 "Its way of working stops here" can be read as voiding the predecessor's §7 rules (never push or
   open a PR without the user's word; output spelling only case by case with before/after; "an answer of mine to a
   question is not a go"; guard pins with dated justification; no credentials; records land with the code; plan each
   step with the user before code). They must be restated verbatim here.
5. **The probe**: P0 relies on "the probe is written and parked"; it is in
   `/tmp/claude-0/-home-user-legend-lite/aee5dfb7-5bf4-5321-839f-0e3f75cdcca9/scratchpad/homework/probe/`
   (`ParityProbe.java`, `ParityC.java`, `bodyplanner.diff`), outside the repository; a new container loses it. Commit
   it as text under `docs/test-is-product-homework/probes/` with how to apply and run it (the predecessor's homework
   kept its probes that way, `docs/planner-driver-seam-homework/probes`).
6. **The branch and base**: none named; commits "stay local" (L306) with no backup rule (§5.2).
7. **Records**: where each step records (GATES entry name, IN_FLIGHT, `docs/SEMANTICS_REGISTER.md` rows for every
   divergence from upstream -- decision 2 moves toward upstream, decisions 4, 7, 10 and T13 may move away -- and the
   decisions log numbering after D99).
8. **A status section** the steps update (the predecessor's §8 pattern), and the plan listed in AGENTS.md's standing
   documents table (only IN_FLIGHT names it).
9. (MINOR) The text of rulings 11-14 and the user's exact words for 7 and 10.

## 8. Verdict

**Expert in its diagnosis; not coherent; not tractable as written.** The ledger (1.A-1.E) is a sound, evidence-based
reading of the inventory, the target shape (§2) is right in principle, and most code claims check out (§3's confirmed
list). But the rulings of 2026-10-31 were appended to §7 without being carried into §1-§6 (C1-C5, G7); the
"byte for byte" and "zero movement" promises are contradicted by the plan's own kept constructs and rulings (C6, O0);
two claims are wrong in ways that break steps (W1: the runner is not "unchanged"; W3: a fresh minter breaks host
identity); one ruling was taken on an unstated consequence (W5: the referee replay decides ~1,250 verdicts per corpus
pass today); the step order has two dependency inversions (P3 before ids, P5 before P6); several kinds have no design
(seeds G2, effectful maps G1, transactions G3, plan-first failures O8, the verdict record's judge-time facts); and the
size is about a third of a realistic estimate.

**The top five changes before execution starts:**
1. **Carry every ruling into the body and replace "zero movement" with a movement ledger.** Rewrite 1.A (D1/D2/D20
   with site-derived ids), 1.C (T3 without the identity lane, T7 to 1.B), 1.F (decision 7), a D32 row (decision 1);
   add the "ruling -> section -> step" table; §5's oracle becomes "frozen except a committed, user-approved movement
   register", one class per commit (C1-C5, G7, O0).
2. **Fix the order.** Site-derived ids (decision 10's id half) before host sides become product plans; the
   run-then-plan kinds (T8, T9, R13, T10, inline execute) before the walk is deleted; 1.D's closing rule (including
   seeds and checks) with the one-plan step; decision 7 as its own step and checkpoint (W3, P5, O1, O5).
3. **Widen P0 and put it in the repository, then re-post three rulings with numbers.** Commit the probe as text;
   add the measurements for the referee's verdicts (W5), capped sides under stabilize (W7), canon/id classes (W2/O4),
   planning failures and assertError's planning-time catches (O8), transaction scope (G3), frame read forms (O9),
   seeds/errors inside a group (O1), constant-only bodies (W1), the census baseline (G8). Re-post decision 4, decision
   10's output-spelling effect (`@_<id>`, `result/CellText.java:73-75`) and T13 with those counts before building them.
4. **Write the missing designs before code.** The runner's additions (a setup step, a text-feeding step, an
   expect-error step, a refused-at-position step, constant-only programs, the transaction and savepoint rule F11);
   the verdict record carrying every judge-time fact plus the root grouping; the per-statement product entry for the
   strangler; the product-surface decision for effects, inline seeds and eager runs on the server's query door; where
   the assert module lives in the build (outside `//core:plan_side`, which is compiled to WebAssembly and native);
   the parity guard's definition, exception list and permanent structural form; the guards-that-move table (K1-K3).
5. **Re-size, re-checkpoint and make it resumable.** 9-13 weeks, the M0-M10 breakdown of §5.3 with its checkpoints;
   a resumability block: commands (RESULT_BOUNDARY_START_HERE §6-§7), the census, the lane sessions and their brief,
   the user's rules verbatim, the branch, a backup rule for local commits, where records go, and a status section.
