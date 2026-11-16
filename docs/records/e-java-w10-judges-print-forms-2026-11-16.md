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

## State

| Step | State |
|---|---|
| 1. Messages | next, beside W9's seam |
| 2. Typed comparisons | after 1 |
| 3. Parsed goldens | after 2 |
