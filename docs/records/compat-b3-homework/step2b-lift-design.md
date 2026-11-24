# B3 step 2b: the lift -- design (2026-11-24, for review before building)

## What it is for

Three compatibility needs share one shape -- an assert whose check only test tooling may make -- and today each is
answered by compatibility code inside the run:
1. **A relation's own printed text** (B2's Q8, ruled 2026-11-24 to need no plan fact): `assertEquals(golden,
   $rel->toString())`, `toCSV($result.values)->replace('\n', ',')` (the corpus's 41 calendar cases): upstream's text is
   legend-pure's printer's, which only Java at the edge writes (B2's S1 printer, from typed rows), then literal
   `replace`s in Java (the user: "do the replace in java too").
2. **An unordered printed table** (step 4): compared line by line in any order -- the test tooling's rule, never the
   product judge's.
3. **A SQL-text assert** (step 6): the assert's query re-run through lite's public door, its rows against the
   referee's golden rows.

## The shape

    harness:  prepare(test)  ->  door.admit(typed body)  ->  run(admitted body, after-body checks)  ->  verdicts
                                   |                              |
                                   +-- lifted checks -------------+  (run on the body's session after the body)

- **prepare** (the test runner's public first half, product): resolve, type, state the facts -- what
  `PureTestRunner.run` does before planning today. Returns the typed body and its facts.
- **door.admit** (edge): today's rewrites (S137, S144) plus the LIFT: each liftable assert is taken out of the body
  (replaced by a no-op statement at its place, so positions and lets are kept) and returned as a lifted check: the
  side expression to re-run (typed), the golden (a literal), the post-print steps (literal replaces), the compare rule
  (exact text; lines in any order), and the assert's name and position.
- **run** (the test runner's public second half, product): plan and run the admitted body exactly as today, then -- on
  the same session, before it is released or rolled back -- hand that session to the caller's after-body step. The
  product passes none.
- **The after-body step** (the harness's, test tooling): for each lifted check, a query of the body's lets the side
  reads plus the side's relation, typed, run through lite's public door (`TestExecution.runProgram` with an
  `Output.Rows` sink: typed rows); the rows printed by B2's printer; the replaces applied; compared by the check's rule;
  the verdict told to the run's listener under the assert's name.

## Rules

- **What lifts:** an assert (assertEquals, assertEq, assertSameElements, ...) one of whose sides is a relation's own
  `toString`/`toCSV` at its root, followed only by `replace` calls with literal arguments, the other side a String
  literal (or literals joined by `+`); later, step 4's and step 6's shapes. Nothing else.
- **What re-runs:** the lets the side reads, transitively, and the side. A let that writes (an effect) is never re-run:
  such a side is refused by name and counted (the corpus reports it), never approximated.
- **Verdict order:** the body's verdicts as today, then the lifted checks' in body order. A body that fails before its
  end runs no lifted check (as upstream stops at the first failure).
- **One audience choice** (the review of step 2's M1): the harness takes ONE audience object -- legend-pure's (PCT) or
  legend-engine's (the corpus) -- giving the plan style, the run hook and the door together; `TestObserver.admitted`
  and the separate style/hook choices go. A product run has no audience object (no door, no lifted check).
- **Guards:** a test that a compatibility-style plan was admitted (the audience object is the only way to get the
  style); the product runner's after-body step is the caller's and nothing in the product names the edge.

## Proof

- Built empty first (no shape lifts): every lane unchanged.
- Then the printed-relation shape with B2's printer: the corpus's calendar cases on H2 (`testAnnualizedValue`,
  `testPmaValue` pass), PCT's printed-table sides byte-identical (B2's measure), S132 then deleted.

## Open (for the review)

- Re-running the lets: a let bound to `execute(...)` re-runs the query (no effect); is any corpus side bound to a let
  that is not a pure query? Counted before building.
- The no-op left in place: `true` as a statement, or the assert removed and the positions renumbered?
- Where the after-body step's typed query comes from: `TypedQuery.withBody([lets..., side])` over the admitted query's
  model (the same compiler, no second typing), or a fresh `Compiler.queryResolved`.

## The design review (2026-11-24, read-only) -- build with changes

- **Structural (to the user):** substitute the printed text as a String literal into the assert BEFORE planning (the
  read run once after the setups, printed and replaced in Java), so lite's judge compares at the assert's own place --
  instead of a harness comparison after the body (a second comparator with copied words, against tenet 5; 82 corpus
  asserts followed by another assert would reorder their failures; a body whose only assert is lifted would score
  SKIPPED). The cost: a row-derived literal in a compatibility test plan (the fold rule: "never row data", which is the
  compiler's; here test tooling composes two runs).
- **The gate is the body's effects** (`BodyFacts.effects()` false), not the side's lets: a write elsewhere changes what
  a read sees. No liftable side reads a write today; one reads a raw grid (`executeInDbToTDS`), refused by name.
- **The golden may be a one-element list** (`['...']`: the 41 calendar cases, 5 groupBy cases); never several.
- **Lift every side whose root is a print** (a literal, a #TDS literal's print, another print, a let bound to a
  literal on the other side): ~197 corpus sides, ~368 PCT relation sides; then the edge's SQL table printer
  (`RelationTextLowering`) goes with S132 -- one printer of legend-pure's table text (G1).
- **The corpus's toCSV is a test helper** (`meta::relational::tests::csv::toCSV`, a native in lite) with its own cell
  rule, which B2's printer spells.
- **The audience object:** its type in the product (`com.legend.test`), its instances at the edge; `PureTestRunner` takes
  it; `TestObserver` goes back to watching only; a shrink-only register of the test-body routes that plan in upstream's
  style without one (six today). No public prepare/run halves are needed under substitution.
- **Step 4's wording:** an unordered printed table becomes an explicit `assertSameElements` over the two texts' lines
  (split in Java at the edge), judged by lite -- an explicit order-free assert, not a built-in shortcut.
