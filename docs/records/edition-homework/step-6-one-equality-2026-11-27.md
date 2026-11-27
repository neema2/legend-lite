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

## The whole of what is left, checked against every source (2026-11-27, after the user's "That's all that's remaining for all of step 6? Everything?")

The orchestrator's first two lists for the user left items out. This one is made from four sources read line by
line: the plan record's carried-forward table (section 5b), the user's rulings in it, the research's gaps G1 to G8
(`r3/research-step6-equality.md` section 2.3), and the last equality measurement's groups (176 bodies whose assert
form and in-expression form differ; the judges disagree on NO verdict, only on what the database judge cannot
judge: 10, 8 and 8 bodies on DuckDB, H2 and SQLite).

| Item | Source | State |
|---|---|---|
| An assert of two known types is a compile error on lite's own door | the user, 2026-11-26, "yes compile error"; the measurement's 30 "compile refusal" bodies | **coded 2026-11-27, not run** (`EqualityClasses`, `Kind.ASSERT`); lite's own tests that assert across types on purpose are rewritten as the round finds them |
| A comparison in a function between positions a call fixes: a compile error at the call | the user, 2026-11-27, "the split with compile error when we know"; 5b's first row | **coded, not run** |
| An upcast value compared by the type it holds | the measurement ("number types", `1->cast(@Any) == 'a'->cast(@Any)` a conversion error) | **coded, not run** |
| Pairs, lists and maps by the types inside them | G4; the plan's 6.3 | **coded, not run** |
| Numbers in two families | the user, 2026-11-27 | coded; its confirming round green; the stack's full run green but for regenerated files. The other two modes keep their answers by a rule of their own (`NamedRule.NUMBERS_OF_THREE_TYPES`, `lowering/NumberFamilies`, the host judge's `HostForms.exactNumbersByValue`): **coded, not run** (the user's catch: "I thought engine has a different rule from pure?") |
| legend-pure's door answers a function's comparison as legend-pure does (`same(1, 1.0)` false; no literal read as the argument's type) | the user, 2026-11-27, "yes we should measure and hopefully fix" | **coded, not run** (`edge/PureComparisons` reads bodies); PCT at legend-pure's door is its measure |
| Arithmetic with a written literal reads the literal as the other operand's type | the user, 2026-11-27, "good let's do it" | to probe first: the typer already types a Decimal times a Float literal a Decimal; whether the database computes it exactly, and its text |
| `max` and `min` of an Integer and a Float column at legend-engine's door in the database's promoted type | the user, 2026-11-27, "great to match engine but only if not too complex and costly" | not built; the two-argument form is one CASE, the database's own |
| A `Number` or `Any` value compared by the type its column holds: an exact number's column against a Float's refused where the query is planned, a written literal still reading as the column's type | G2; the user, 2026-11-27, "great do it" | **coded, not run** (`lowering/HeldNumbers`, the product's rule alone; dates not read there) |
| Two tables with `==`: column names and row order | G5; the measurement's "grids" (4, 3, 4) | not built |
| Lists of instances, and an instance with a to-many key, by typed leaves (text today) | the measurement's "instances and keys" | not built |
| A keyless instance against itself (`$xs->at(0) == $xs->at(0)`) | G4 | not checked |
| Maps compared whatever order their entries were written in, inside an expression | the measurement's "other" (`newMap` order) | not built |
| An hour-precision date-time literal inside an expression (a database error today) | the measurement (4, 4, 0) | not built |
| A day against a date-time known only at run time, inside an expression | the measurement's "dates and written precision" (7, 7 on DuckDB and H2) | to re-measure, then build |
| The mixed-days refusal | the measurement (3, 2, 3); the plan's 6.6 | not built |
| An element's own message inside `forAll`, and `assert(cond, msg)` inside `forAll` | the measurement (4, 1, 4); G6 | not built |
| `is` as a product native (`assertIs`, `assertIsNot`) | G6 | not checked |
| DuckDB's nanoseconds in the product's `==` (today only in the judge's record) | G7; the plan's 6.6 | not built |
| The tolerance assert as its formula (`abs(e - a) <= d`) | the plan's 6.6 | not built |
| An abstract `Date`'s text (`adjust(...)->toString()` at legend-pure's door) | 5b; the plan's 6.6 | not built |
| The JSON lane: `Any` cells compared by JSON kind; a Decimal or a date there refused | G2 | the judge's sides read JSON kinds; inside an expression not checked |
| A Variant read through `Any`: the host judge's `assertEq` holds it against the Integer of its text where the database judge fails it, and the two judges' records of `assertEquals('"abc"'->fromJson()->cast(@Any), 'abc')` write the Variant differently (a both-judges run stops on each) | found 2026-11-27 by `AssertBarTest`'s new lines, which hold them as they are | not built: the host's is the wrong one (S401) |
| One side empty inside an expression (`$x.opt == 1` is SQL's NULL) | the measurement's "empties" (3, 1, 4) | the user's decision, after its SQL cost is measured |
| What the database judge cannot judge (6, 2, 6 bodies) | the measurement | to read one by one |
| The equality measurement run again, every body that still differs explained | this record | at the end |
| Lists inside an expression on a database with no list type (H2 59 + 9 bodies, SQLite 22) | G3; the plan's 6.4 | the user, 2026-11-27: looked at again, with SQLite's date-time text, when everything else is done |
| A Float's text through a `Number` or `Any` inside a store query at legend-engine's door | 5b | rides with step 8 (paused): the Float row it needs is step 8's |
| A table's cells counted by an assert (D6); two date-times at one instant tying in a sort (D14) | the open decisions | the user's, unanswered |
| Slice 6.2's database lanes and outside audit | the fast setup | not run; the user's go |

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

## The typer's part: comparisons whose types a caller gives (built 2026-11-27, branch `claude/step-6-3`)

A function written for `Number`, the abstract `Date`, `Any` or a type variable compares values whose types are known
only where the function is used. Where the function is typed, such a comparison is kept as a record around its call
(`TypedRunTimeComparison`); once every function a query reaches stands in the query, each argument in its parameter's
place, the door's comparison setting answers it from the types its sides have there (`CrossTypeRule.atTheCall`, asked
at the end of the expansion pass). Nothing after that pass sees the record, and no SQL layer changes.

| | lite's own doors | legend-pure's and legend-engine's doors |
|---|---|---|
| `same(a: Number, b: Number) { $a == $b }`, `same(1, 1.0)` | false: two types (the equality rule) | the comparison as written, the database's (unchanged) |
| the same over an Integer column and a Float column | false for every row | the database's comparison |
| `$a < $b`, `less(1, 1.0)` | true: by value, then by type, Integer below Float below Decimal (decision D3), folded into the operator (`<` becomes `<=`), no SQL added | by value |
| `amongWholes(a: Number) { $a->in([0, 1]) }`, `amongWholes(1.0)` | true: a literal the function's author wrote reads as the value's type (decision D5) | the database's |
| `isOneAndAHalf(a: Number) { $a == 1.5 }`, `isOneAndAHalf(1)` | false: 1.5 is no Integer | the database's |
| a literal handed on through another function (`sameAsOne(a) { same($a, 1) }`) | a value there, not a written literal: `sameAsOne(1.0)` false | |
| a side still typed only at run time after expansion (a `Number` column, a computed `Number`) | the comparison as written: the database compares the values it holds | |

A comparison between two generic positions of a body typed into each call (a function over "any relation") takes the
same answer, from that call's column types. To be measured on the timeseries library and both corpora before it
stays: the library compares two lines' period columns and refuses a day against a date-time itself.

Not built: legend-pure's own answer at its door (false for `same(1, 1.0)`, strict for a written literal), which needs
its setting to read bodies; the door's bodies are the database's comparison as before.
