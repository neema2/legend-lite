# Slice 1: the "same values" instruction carries the two sides' values (2026-11-27)

*The half page the plan of record asks for before building. Read: `sql/dialect/SameValuesForm`, the base dialect's and
DuckDB's arms for the instruction, `lowering/WholeComparisons`, `Equalities.equal`, `planner/RootComparison`.*

## What is there today

- The instruction exists: `SAME_IN_ORDER(list e, list a, rows of e, rows of a)`. DuckDB writes its first two
  arguments (`e IS NOT DISTINCT FROM a`, `sameLists`); every other dialect writes the last two in the portable rows
  form (`SameValuesForm`).
- But the lowering builds all four from each side's PLAN: the side lowered as a statement of its own, its list turned
  into rows, numbered, then listed again. On DuckDB that is the list unpacked and repacked (about 600 characters for
  two short lists), and it is a sub-query, which a DuckDB list lambda refuses.
- A statement of its own exists only for a side that reads nothing of the row, so any other list comparison takes a
  second path: the database's own `=` on two list values, with no rule of ours (wrong where a list is JSON text).

## What changes

Scope: `==` where both sides are lists (or one value against a list) of exact numbers or of Strings, on lite's own
door (`NumberFamilies.TWO_FAMILIES`). The doors' rule is untouched.

1. **The lowering builds the instruction from the two side VALUES**, in `Equalities.equal`, wherever the comparison
   stands: `SAME_IN_ORDER(value e, value a)`, plus the two sides' rows where the lowering has them:
   - two lists written in the query whose element types differ (a scale, an Integer against a Decimal): their
     elements as rows of typed values, written directly;
   - a side that reads nothing of the row: its rows from the planner (`WholeComparisons.sideRows`, new), as today.
2. **The planner no longer plans a statement of its own for these kinds** (`RootComparison.plan` stands aside), so a
   query's answer, an `if`, a lambda and a filter all get the same instruction.
3. **Dialects.** DuckDB writes the two values (unchanged arm): one short expression in every place. The base arm
   writes the rows form when the rows are there, and, when they are not, the database's own comparison of the two
   values, which is exactly what those databases get today in those places. So no database but DuckDB changes its
   answers, except that two written lists of different element types are now compared as typed rows.
4. **A test writes the same comparisons both ways on DuckDB** (its list writing, and the base rows writing through a
   test-only dialect) and requires one answer.
5. **The pin:** a test holds the measure's counts (`tools/equality-measure/positions-baseline.tsv`).

## What the measure should show (from the baseline `positions/baseline-slice-0-5dae42452.md`)

- **DuckDB:** the list comparisons of these kinds that FAIL in the four list-lambda places ("subqueries in lambda
  expressions are not supported") become right everywhere: computed list same, other order, two computed lists,
  shorter, list int and decimal, list decimal two scales, list strings computed, one value and list of one, list
  decimal differs, and the computed empties. "Fails somewhere" falls from 29 by about 10 to 12.
- **H2:** "written list decimal two scales" stops contradicting (the plain `==` true, as the assert). "Contradicts"
  6 to 5.
- **No other row moves.** SQLite is not a goal of this slice.
- **SQL size:** DuckDB's statement for `[1, 2]->map(x | $x + 1) == [2, 3]` from about 600 characters to under 100.
  No size pin rises; if one would, the slice stops for the user's word.

## Not in this slice

The other kinds (slice 3); maps and tables (slices 3 and 4); `contains` and the rest (slice 5); binding a side once
(slice 2); H2's JSON-text comparison of two computed lists that read the row (a named gap until H2 has list values).
