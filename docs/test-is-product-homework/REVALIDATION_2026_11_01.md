# The plan revalidated at M6, with fresh eyes (2026-11-01)

The user, handing M6 over: "you are allowed to revalidate the whole plan ... feel free to challenge and think
fresh/unbiased ... but then we want to execute the whole program like we did for the product". This is that
revalidation, written by the session that folded M6, from the code as it stands (the handoff's §4.1, the M6 audit)
and the plan (`docs/TEST_IS_THE_PRODUCT_PLAN_2026_10_31.md`). Each item says what holds, what I would change, and
what this session does about it under R23 (no stops: a change the rulings cover is made and recorded; one they do not
is posted in the M6 batch and the recommendation taken until the user says otherwise).

## 0. The goal, restated in the product's words

The product has one plan door (`PlanMaker.plan` behind `TypedQuery.plan`) and one execute door (`Execution.run` ->
`PlanRunner`). The test has two roads: the walk (`BodyCompiler`/`BodyDriver`, planning as it runs) and the program
road (`TestDoor` -> `PlanMaker.testProgram` -> `PlanRunner.program`, the judges the sink). "The test is the product"
means: one plan door for a body (`TestDoor`, the test-planning library over the product planner), one execute door
(`PlanRunner.program`), the walk deleted. Everything below is judged against that sentence.

## 1. What holds

1. **R16, whole bodies class by class.** The alternative -- switch every body at once and delete the walk -- throws
   away the only oracle that says the program road is right: the walk, measured body by body on the corpora, PCT,
   channel B and the five lane backends. The classes are scaffolding (`BodyRouter`, `BodyClass`, the parity test)
   and go at M10; the price is the positive rule per class. Keep.
2. **The audits (R15, R22).** M5's audit found four blockers and M6's four should-fix items that the chain alone
   would not have caught (the chain measures the shapes the corpora hold; the audits probe the shapes they do not).
   Keep, one per M step.
3. **The oracles and the movement register (§4).** Keep; M6's residue is recorded, not fixed (R23).
4. **R14, the planned raise.** M6's `HostAssert.raising` is the pattern and the parity test holds it (a planning
   failure raised at the assert's turn, after the earlier asserts reported). Keep.

## 2. What I would change, and what is done about each

### 2.1 M4 (narrow) is a prerequisite of M10, not an optional step

M10 "deletes the walk" once "the last class moves". A body that reads the system store (core's 76 metamodel-reading
bodies, the corpus's `system=121` statements per DuckDB pass) has no class while M4 is deferred (R23): the test door
cannot plan it (the test road opens a separate system database per execution, `BodyDriver.openRoute`,
`driver/SystemDatabase`; the product carries the store's tables in the query's own plan, `PlanMaker.metamodel`). So
M10 cannot delete the walk without SOME resolution of M4, and R23 forbids re-asking.

What this session does: M10 gains a first sub-step, **M10a, the narrow M4 on the test road only** (R19's
recommendation, which the user did not rule against: "the plan can name the system database, the test door plans it,
the product's forms unchanged"): a step whose `Target` names the graph's system database, planned by the test library,
opened by the runner as the walk opens it today, every statement identical. It changes no output and no product form,
so it is R23's "keep today's behaviour and record" case, not M4's open question (both directions stay recorded in
the plan's R23 row for the user: the test road onto the product's form, or the product onto the separate database).

### 2.2 M9 before M8

The corpus's main shape is `let r = execute(...); ... assertEquals(..., $r.values...)` (M0: 92% of the product
road's refusals were D15, frames; M3f planned the frame let on the test door already). M9 admits it (frames read by
asserts, T8, T9, T10, T12) and moves most of both corpora and channel B. M8 (the database judge self-contained, MV2 to
MV4) moves few bodies (2.F: the database judge is unjudged on 0 DuckDB corpus asserts today, its three Java arms
counted as "host decided") and changes OUTPUTS (messages, census keys) the user ruled to accept. Doing M9 first puts
the bulk of the traffic on the program road sooner, with no output change, and leaves M8's movement to land on
bodies already measured on the program road -- each MV then attributable to M8 alone.

What this session does: the order becomes **M7, M9, M8, M10** (M8 after M9), recorded in the plan's §7 and §10. If
the user prefers the written order, nothing is lost: M8 and M9 are independent.

### 2.3 B3's verdict record -- STRUCK by the user (2026-11-01)

As first written here: B3 asks that the verdict step carry "every fact a judge reads at judge time ... so no judge
reads the model"; M6 built the judgment as a closure over the typed arguments (reading the model and the typed tree
at judge time), and this note proposed reifying the facts into a record at M8.

The user's question cut under it: "shouldn't asserts be regular lite functions that lower like any other function in
the database judge? Why would they need extra data/metadata in the plan?" They should, and they do: in the database
judge an assert IS a lowered function (`VerdictSql`: one statement whose row is `holds`, `message`, `unjudged`); the
plan carries that SQL and nothing about the assert (after M8's MV2/MV3 not even the polarity). The facts B3 named are
read only by the HOST judge's Java comparison -- and the host judge is the TEST HARNESS (the cross-check: channel B's
agreement, the corpus host pass, PCT), not a product-grade judge. Harness Java may read the typed tree when it
compares, as the referee does. So: B3 struck; no assert metadata in the plan; M6's closures stand as the harness's
form; the M10 rule is "the driver sends nothing and plans nothing". A later ruling, after M8 is measured: the database
judge the verdict of record, the host judge the cross-check (channel B has it the other way today); PCT stays
host-judged until the database judge spells every lane's dialect.

### 2.4 The engine-text asserts: the test in the plan, the appeal a harness (the user, 2026-11-01)

`assertSameSQL`, `assertSqlEqualsTDG`, `assertEqualsDualGolden` and the `toSQLString`-producer equalities
(`SqlTextVerdicts`, 1,720 lines; about 1,258 referee replays per DuckDB corpus pass) are refused by `planHost` and
appear in no M step by name (T11 and T13 are ruled -- R6, R10 -- but unscheduled). Without them a class holding
every corpus body does not exist, and M10's "last class" is a euphemism for a step the size of M7.

This note first proposed a SQL verdict on H2 lanes and Java elsewhere. The user: "I don't think we should bifurcate
the h2 lane ... the test itself should be fully run in platform/plan ... then if test doesn't match everything else
can be done in java" -- the right shape, and simpler. **M10b:** on EVERY lane the same plan -- the body's statements,
the query under test (`toSQLString` a plan-time text, the query's rows a side), our text against the golden text a
constant of the plan; where it does not match, the appeal is harness Java outside the plan: the golden replayed on the
H2 referee, the rows compared, the appeal decided (R6/T13 as ruled). One behaviour detail pinned from the code before
it lands: today the arm judges on rows in both cases and counts the text; whether a matching text short-circuits the
replay stays as today unless the user says otherwise.

**M10's "last class" is split**: M10a (the narrow M4, 2.1), M10b (above), M10c (the remaining host shapes `planHost`
refuses: the quantified forms, the cell pool, the lineage lines, `assertIs` over non-elements -- each a form or a
named refusal), M10d (the deletion: the walk, the router, `BodyClass`, the parity test's permanent form, the driver
rule, the layer file). Each a commit, measured. The plan's §7 row for M10 is rewritten and its size doubled (2 weeks).

### 2.7 The user's rulings on this note (2026-11-01)

- The order M9, M7, M8, M10a-d: "the order is fine".
- M4 narrow: "fine for now but needs a product follow up" -- recorded in the plan's §9 (the product's embedded
  metamodel tables against a seeded system database, decided by measurement on the real projects).
- B3: struck (2.3).
- The engine-text asserts: no lane special-cased; the test in the plan, the appeal a harness (2.4).

### 2.5 PCT moves bodies for the first time at M6

Every PCT body asserts (M5's rounds: `bodies.moved` absent from every PCT log). PCT judges on the host, so M6 is the
first step whose movement the PCT lanes see (ASSERT_PROGRAM bodies: `assertEquals(expected, <expr>->from(...))`
shapes). Round 13's brief must say so and ask for the count per suite; the chain's PCT DuckDB run reports the count
from `Census.Key.BODIES_MOVED` (the PCT logs print no census: added to the report if needed).

### 2.6 The things I looked for and did not find wrong

- The class rules are positive and static (R16). The one letter-of-R16 breach the audit found (`SqlTextVerdicts
  .lookThrough` decides "no producer" by catching an inliner failure) is the walk's own gate, older than M6, and goes
  with the walk at M10d; named as residue, not changed (zero movement).
- The plan's §4 register names every movement so far and its step; nothing moved outside it.
- The audit cadence costs about a fifth of each step and has paid for itself at every step.

## 3. The step list as this session runs it

| Step | What | Size |
|---|---|---|
| M6 ◆ | done with this note (the handoff's §4.3 closed: the audit folded, the chain, the records) | -- |
| M7 ◆ | the fused group composed at planning; the database judge's and channel B's assert programs move | 1 week |
| M9 ◆ | frames read by asserts, T8, T9, T10 (A), T12, inline `execute`; the frame bodies move (the corpus's bulk) | 1-1.5 weeks |
| M8 ◆ | B3's facts a record (2.3); the database judge self-contained arm by arm (MV2, MV3); the canon (MV4) | 2 weeks |
| M10a | the narrow M4 on the test road (2.1); the system-store bodies move | 3 days |
| M10b | the engine-text asserts (T11; T13 outside the plan) | 3-5 days |
| M10c | the remaining host shapes | 2-3 days |
| M10d ◆ | the walk, the router, `BodyClass` deleted; R14/MV8; `theDriverCompilesNothing`; the parity guard's permanent form; the layer file | 3-5 days |
| Z | the independent audit, the records, CI on every OS; the landing the user's | 2-3 days |
