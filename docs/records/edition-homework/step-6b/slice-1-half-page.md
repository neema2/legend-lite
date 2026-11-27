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

## What was built and measured (2026-11-27, late evening; scratch branch `claude/step-6-numbers`)

Commits `3bd6ef0cd` (the first cut) and the one after it (the rows form binds each side once).

- `lowering/Equalities.asSameValues`: the plain `=` of two list values of exact numbers or Strings becomes
  `SAME_IN_ORDER(value, value[, rows, rows])`; called from `Lowerer.written` for the product's own `==` (a door's rule
  keeps its own writing).
- `lowering/WholeComparisons.sideRows` and its one implementation in `planner/RootComparison`: a side's typed rows,
  where the side reads nothing of the row.
- `planner/RootComparison.plan` stands aside for these kinds (after the order rule has had its say).
- `sql/dialect/AnsiSqlRenderer.sameValues`: the two-value form is the database's own comparison, as before the
  slice; the rows form binds each side's rows once (`SameValuesForm.boundOnce`).

**The measure, against the baseline** (5,646 runs; every answer that moved is one the half page named):

| | right everywhere | fails somewhere | contradicts |
|---|---|---|---|
| DuckDB | 62 to 73 | 29 to 18 | 9 |
| H2 | 78 to 79 | 7 | 6 to 5 |
| SQLite | 71 | 4 | 12 |

- DuckDB: the eleven list comparisons of these kinds that failed in the four list-lambda places now answer, each
  the expected answer.
- H2 and SQLite: two written Decimal lists at two scales are equal as a plain `==`, as the assert already said.
- Nothing else moved.

**SQL** for `[1, 2]->map(x | $x + 1) == [2, 3]` as a query's answer: DuckDB 580 characters to 80
(`SELECT (list_transform([1, 2], x -> x + 1) IS NOT DISTINCT FROM [2, 3]) AS value`); SQLite 2,371 to 2,199; H2
refuses the computed list at the product's door before and after (its missing list operations).

**Still to do in this slice:** the result of CI's core and checks lanes run here (started 23:20 UTC); the test that
writes the comparisons both ways on DuckDB; the test that holds the measure's counts; an independent review of the
diff; the size pins read; then the corpus and PCT lanes, one push to the pull request, the lanes once, and the
program branch moves.

## The independent review of the diff (`review-3-of-slice-1.md`) and what became of it

| Finding | What it was | Done |
|---|---|---|
| B1 | BigQuery wrote the two-value form as "not equal" (its null-safe arm read the instruction as a null-safe comparison) | fixed: the instruction goes to the base arm first |
| B2 | A value drawn afresh in a `let` the side reads was drawn again for the side's rows | fixed: the same transitive check the planner's statement had |
| S1 | `!` of the two-value form was a bare `NOT`, where the plain equality's negation was null-safe | fixed in `NullSemantics.negate` |
| S2 | The two-value form ignored its parent's precedence (`a = b = TRUE`) | fixed: written in parentheses |
| S3 | MongoDB refused the two-value form it used to answer as an equality | fixed |
| S4 | The guard on how a list is carried was gone | fixed: plain typed lists only (no JSON, no Decimal or date as its text) |
| S5 | DuckDB compares the list values as they are, where the rows form drops an empty element | a row of the measure asks it ("list with an empty element") |
| N6 | Two written String lists would have been compared as rows, by a database's collation | fixed: rows ride only for a computed side, or two written lists the SQL types differently |
| N1 | The instruction is typed "never NULL", and its two-value form can be | open: read with the round's result |
| N3 | An unreachable refusal with the wrong words | fixed: a malformed call is a defect |
| N4 | The planner's rows are lowered on DuckDB too, where nothing writes them | open: measured cost first |
