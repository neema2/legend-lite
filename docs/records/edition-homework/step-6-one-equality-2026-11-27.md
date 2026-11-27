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
3. **A query whose answer is a comparison of two whole sides is planned the same way** where a side is a computed
   list or is typed only when the query runs (`planner/RootComparison`): the product planner plans each side once and
   the same code compares them. Two sides an expression compares exactly on every database keep the plain comparison:
   values of known scalar types, instances, anything written in the query itself. *Measured on the first run
   (2026-11-27): planning EVERY root comparison over sides made trivial ones (`[] == 1`, `%2014 == %2014`) about twenty
   times longer (the size baseline's `equal` family on DuckDB 1,073 to 20,933 characters), so it is narrowed to the
   sides an expression cannot compare.*
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

## The same meaning inside an expression (the user's question, 2026-11-27)

The user, on being told the judge's comparison had moved: "Did we merged the meaning of equality? So like if a filter
or join condition or any other lambda uses an == that is the same meaning as assert?" Not yet: inside a filter or a
lambda `==` compares two values in one SQL expression, and differed from a comparison of two whole sides in the rows
below. Each is being brought to the one meaning; held by `exec/EqualityInExpressionsTest` and the tests named.

| Case | Inside an expression before | Now | State |
|---|---|---|---|
| a computed list of no element against `[]` | false on DuckDB and SQLite (a NULL test), an error on H2 | true: the other side's emptiness (`Scalars.emptiness`) | built 2026-11-27 |
| one value against a computed list | a database error (a value against an array) | the value as the list of it | built 2026-11-27 |
| two instances of one declared class | a text of the keys (`1.0D` differs from `1.00D`; the declared class, not the actual one) | typed leaves: the actual class, then the keys by value or the identity (`SideEqualities.sameInstance`; `InstanceIdentityTest.aKeyIsComparedByItsValueInACondition`) | built 2026-11-27; a to-many key and lists of instances keep the text |
| one side empty (`$x.opt == 1`) | SQL's NULL, so `!($x.opt == 1)` drops the row | open: a total `==` changes answers and lengthens SQL | **put to the user 2026-11-27** |
| a computed list on a database with no list comparison in an expression (H2; JSON-carried lists on SQLite, MySQL, MariaDB) | refused, or compared as text | needs each database's list-to-rows form | the database follow-up (deferred by the user to after Stage B) |
| two tables | column names and row order ignored | not built | open |
| a `Number` or `Any` column against another | the database's comparison | not built (the type is not known when the query is planned) | recorded |

A join written in a mapping's own store language is the database's `=` by definition and is not part of this.
