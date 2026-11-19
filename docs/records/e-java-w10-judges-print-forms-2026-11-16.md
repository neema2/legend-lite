# E-Java: W10, the judges' print forms (approved 2026-11-16)

**The direction (the user, relayed by the orchestrator, 2026-11-16):** "Didn't we agree to do all the compatibility in
JAVA? And for toString inside an expression use engine cast(as varchar)." The reading, settled with the user the same
day (docs/records/total-wall-plan-2026-11-16.md §2): on the PRODUCT road a Float's text stays S311's six decimals, one
simple format call per database; on the EDGE road a Float's `toString` inside an expression is legend-engine's own cast
for that database, and legend-pure's exact print of a value is Java at the edge, from the typed value. (Corrected
2026-11-16: this paragraph first said the product's in-query Float becomes legend-engine's cast; it does not.) W10
applies the same principle to the judges: no legend-pure print form is computed in SQL.

## What the judges' print forms do today (measured on D's stage)

| Job | Where | Count |
|---|---|---|
| Spell a failed verdict's MESSAGE | `VerdictSql`'s `Repr.of` projections (expected/actual `%r`); `SideShapes`' "Cannot cast a collection of size" text; `TDS_NULL_CELL`'s printed cell; `LiteralSpelling`'s `+0000` where it spells a message | ~4 sites in `VerdictSql`, 3 + 1 in `SideShapes` |
| DECIDE a verdict by comparing canonical text | `VerdictSql.canonOf`, `CanonicalRenderSql`'s row and TDS canons, `FLOAT_CANON` in every dialect | multiset, grid and mixed-list equality |
| Compare a pure-printed GOLDEN with our rows | the rendered-text family (`toString`/`toCSV` against a golden String) | one family |

## The approved plan (the orchestrator, 2026-11-16)

1. **Messages to Java at the edge.** The verdict row keeps `holds`, decided in SQL, and carries the two sides as
   TYPED value columns instead of their printed text. On a failure the edge formats the message with the host judge's
   own formatter (`PureAsserts.repr`): the same text under both judges by construction (G2).
2. **Canons become typed comparisons in SQL.** A Float compared as a number under the ruled tolerance, a Decimal
   exactly, a date as a date; where one column mixes kinds, lite's own tag-plus-typed-value key (as `TaggedLists`),
   never pure's printed text. `FLOAT_CANON`/`FLOAT_PRINT`'s judge uses go; their deletion is coordinated with D's float
   change (D owns the product's six-decimal form and the edge's in-query engine cast).
3. **Rendered-text goldens parsed by the edge.** The edge parses the golden (a plan constant) into typed rows in Java
   at planning; both judges compare typed rows.

## Conditions, checked at each step

1. No verdict flips: channel B on DuckDB, H2 and SQLite under HOST, DATABASE and BOTH, and both corpus verdicts. The
   orchestrator sends a lane round at the end of steps 2 and 3.
2. Message parity by test: a planted set of failing asserts (Float, Decimal, date, mixed list, TDS) gives
   byte-identical messages under HOST and DATABASE.
3. None slower: the judge's statements and characters pinned, shrink-only; channel B's time checked.
4. The typed Float comparison uses exactly the host judge's ruled tolerance: the same constant, in one place.
5. Deferred goldens (read from a store at the step, not constants): counted first; if any exist, the options (refuse
   such a body by name, or the edge parses at run as an appeal) go to the decision log with the counts, for the
   user's end batch, without stopping.

## Condition 5 measured: deferred goldens (2026-11-16)

A deferred golden is a rendered-text golden that is not a constant of the plan (read from a store at the step); the
database judge leaves it UNJUDGED as "rendered-text: the golden is not a string constant"
(`AssertVerdicts.renderedValueVerdict`). Counted with `-Dlegend.diagnostics=judge-declines`:

| Lane | Deferred goldens | How |
|---|---|---|
| PCT channel B, DuckDB, all five suites | 0 | the diagnostic over the five suites |
| The relational corpus, DuckDB | 0 | its database judge leaves nothing UNJUDGED (ceiling 0) |
| The relational corpus, H2 | 0 | each of its 45 UNJUDGED tests run alone (`//spec:corpus_one h2 database <test>`): 40 `assertEquals` null-canon-cell, 5 `assertJsonStringsEqual` JSON not byte-equal |

**Decision.** None exist, so step 3 parses every rendered-text golden at planning, and nothing goes to the user's
batch. A golden that is not a constant stays refused by name, as today (UNJUDGED under the database judge).

Side finding: the H2 corpus's UNJUDGED count is 45 against its ceiling of 46 (headroom, to re-pin with the next
change to that lane).

## Step 1's design, as approved (2026-11-16): on-failure side reads

**The ruling (the orchestrator's, with five conditions).** The database judge's verdict row decides `holds`; it no
longer spells a message. Each database verdict carries, as plan data, the HOST judge's planned check for the same
assert -- its sides (planned statements) and its judgment. When `holds` is false, the runner sends that check's side
statements and the host judgment writes the failure: the same text HOST mode gives, by construction.

1. **A planned branch (charter A3(b)).** The database computes the Boolean; the runner sends the plan's named failure
   sides and never others. Javadoc and a witness test say so: a failure side is sent only after a false `holds`, and
   only the plan's.
2. **The test plan's format is bumped** (numbered at landing, in merge order), with its version-history entry; an older
   plan is refused by name.
3. **Under BOTH, no extra statement:** the host's check is already planned and read; it is reused. Pinned.
4. **Message parity:** a planted set of failing asserts (Float, Decimal, date, mixed list, TDS) gives byte-identical
   messages under HOST and DATABASE; PCT's expected-failure messages unchanged on channel B DuckDB.
5. **SideShapes' "Cannot cast a collection of size"** becomes a kind plus operands on D's Phase 2 raise marker, agreed
   with D first; last in 4b if the marker is not ready.

**What goes:** the verdict row's MESSAGE column and the `Repr` projections that spell `%r` in SQL
(`VerdictSql` ~608, `messageAsInstances`, ~2611); the canons stay until step 2 (they decide verdicts).

**Where:** `testexecutionplan.TestPlan.Verdict` gains the failure check (the host check's base and judgment);
`DatabaseAsserts` plans it beside each database verdict (the host's planner, `HostAsserts`, already plans the same
assert under BOTH); `testexec` (`DatabaseVerdicts.verdictOf`, the root loop) on a false `holds` runs it through the
host judge (`HostJudgments`) for the message; `TestPlanJson` carries the field.

## State

| Step | State |
|---|---|
| 1. Messages | approved 2026-11-16 as on-failure side reads (the orchestrator's five conditions); building |
| 2. Typed comparisons | after 1 |
| 3. Parsed goldens | after 2 |
