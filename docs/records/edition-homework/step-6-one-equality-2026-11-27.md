# Step 6, the one equality: how it is built (2026-11-27)

*The edition program's step 6 after slice 6.2. Written by the orchestrator's session, where all of step 6's code is
written from here on (the user, 2026-11-27: "all coding work is supposed to be done here"; "focus and get 6 done";
SQLite's date-time text "super low priority").*

## What was measured first

The equality measurement (`tools/equality-measure/run.sh`, about 5 minutes) on slice 6.2's merged head: 1,704 test
bodies, each as written and with its assert rewritten to the plain comparison inside a condition, under both judges
on DuckDB, H2 and SQLite. The two forms differ on 183 bodies (DuckDB 97, H2 137, SQLite 125). The causes:

| Cause | DuckDB | H2 | SQLite | What it is |
|---|---|---|---|---|
| a compile refusal by strict typing | 30 | 29 | 30 | `assertEquals(1D, $i)` answers "not equal"; `1D == $i` written is a compile error. Not a gap: an assert's two sides are `Any`, compared when the query runs; a written `==` of two known types is refused. The rewrite is not the same program |
| H2 has no list form inside an expression | 0 | 65 + 9 | 0 | a list compared as a value inside one expression needs list functions H2 lacks |
| SQLite's list and map forms | 0 | 0 | 21 | the same on SQLite |
| a scalar against a list, list lambdas (DuckDB) | 16 | 0 | 0 | the value form compares a value with an array |
| empties | 10 | 1 | 12 | a computed empty list is not NULL |
| dates and written precision | 8 | 7 | 30 | a day against a date-time known only at run time; SQLite's date-time text |
| grids, instances, number types, the rest | about 20 | about 15 | about 25 | column names, keys compared as text, a Decimal's scale inside a list |

**Almost every difference has one cause:** the judge compares two whole sides, each computed once as rows of the
statement, and the product's `==` inside an expression compares two values, where a list is an array or JSON. The
judge's way works on every database (H2 included) because rows need no list functions.

## The design

1. **The product's equality has two forms and one owner.**
   - *Inside an expression* (`lowering/Equalities`): two values. What `==` and `eq` lowered to before, in one class.
   - *Over two whole sides* (`lowering/SideEqualities`): each side a plan of its own, computed once as rows; equal
     means the same type and the same value, a type known only at run time read from the side's rows. This is the
     code the database judge compared with (`VerdictSql`'s typed members and `JudgmentFacts.typedPair`), **moved as it
     stood**: values, lists and maps, tagged values, instances by class then keys or identity, membership, the subset
     fold, a quantified equality, two tables.
2. **The database judge keeps no comparison of its own.** It plans each side, asks the product's equality to compare
   them, and builds the failure's record from the values the comparison read. Its size pins: `VerdictSql` 2,263 to
   1,120, `JudgmentFacts` 355 to 237.
3. **A query whose answer is a comparison of two whole sides is planned the same way** (`|$a == $b` over lists,
   instances, tables, values typed at run time): the product planner plans each side once and the same code compares
   them. So an assert is the plain comparison it means, planned like any query, and what a test judges is what a user
   gets. Two values of one known scalar type keep the plain `a = b`.
4. **Not done here:** comparing two lists inside an expression on every database (a list in a filter's condition on
   H2, MySQL, BigQuery). That needs each database's own way of turning a list into rows; it is the lean-SQL follow-up's
   work (`docs/records/database-recheck-2026-11-26/START_HERE.md`), which the user deferred to after Stage B. Until
   then the value form is as it was on those databases.

## Why this and not the plan's wording

The plan's slice 6.4 said "two lists compared as rows on every database (H2 included)" inside the product's `==`,
then 6.5 deleted the judge's layer. Reading the code showed the judge's layer already IS that, for whole sides, on
every database; writing it a second time inside expressions would have meant eleven dialects' list-to-rows forms
(the deferred database work) and then deleting code that already answers. Moving it makes it the product's, with no
answer moved.

## What judges it

No test's answer may move: the code that compares is the same code. The guards, core's test suites, PCT on DuckDB
and H2 and channel B, and both corpora run on it; then the equality measurement again.

## What is left of step 6 after it

- The product planner's root comparison (item 3).
- The value form's gaps the measurement shows, on the in-process databases: a value typed at run time by an upcast
  (`[1, 2]->cast(@Number)->contains(1.0)` is true, should be false), a literal `[]` against a computed empty list, a
  scalar against a computed list, an hour-precision date-time literal, instances by typed keys and run-time class.
- The typer's part (the plan's 6.3): a function typed into each call held to the strict rule; ordering by value then
  type (decision D3); a literal against a `Number` inside a function (D5); pairs, lists and maps by the types inside.
- The leftovers (6.6): the tolerance assert as its formula, nanoseconds, the mixed-days refusal, pins.
- Carried, not built here: SQLite's one date-time text (its work is on `claude/sqlite-datetime-form`, `626b330`,
  unverified; low priority by the user's word), so on SQLite two date-times written at different precisions still
  compare as texts inside an expression.
