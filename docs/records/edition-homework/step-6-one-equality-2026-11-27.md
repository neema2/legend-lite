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
| Arithmetic with a written literal reads the literal as the other operand's type | the user, 2026-11-27, "good let's do it" | **coded, not run** (`compiler/spec/NumberLiterals.inExactArithmetic`). Probed first (2026-11-27, DuckDB): over two written values the database already computed a Decimal (`2.5D * 0.5` is 1.25, `0.1D + 0.2` is 0.3), but over a `DECIMAL(10,2)` column it computed a double under a type that says Decimal (`$p.amount * 0.1` answered 0.010000000000000002, `+ 0.2` answered 0.30000000000000004; `* 3` was exact). Now a Float literal in a sum, a difference or a product whose other operands are all exact numbers, one a Decimal, reads as the Decimal its digits write: legend-pure's own answer (its `CompiledSupport.toBigDecimal` reads a Float by `BigDecimal.valueOf`, read the same day). Not reached, each as it was: a Float VALUE in the arithmetic (a Float column times a Decimal column is the database's double under a type that says Decimal -- **to bring to the user**), `divide` (a Float whatever its operands), SQLite (refuses a Decimal's arithmetic by name, as before) |
| `max` and `min` of an Integer and a Float column at legend-engine's door in the database's promoted type | the user, 2026-11-27, "great to match engine but only if not too complex and costly" | **built 2026-11-27, not yet run**: at legend-engine's API door and its own tests' door, inside a store query, `max`/`min` of an Integer and a Float (two arguments) is the database's GREATEST/LEAST over the Integer converted and the Float, so a Float whichever wins, as legend-engine's CASE there answers (`NamedRule.EXTREME_OF_TWO_IN_THE_DATABASES_TYPE`, named by `EngineTable`; `UpstreamTiesTest.anIntegerAndAFloatColumnsExtremeIsAFloatAtLegendEnginesApi`). Not guessed: an Integer against a Decimal (not run upstream), and legend-engine's PCT door (it stamps nothing; what it answers there was not run) |
| A `Number` or `Any` value compared by the type its column holds: an exact number's column against a Float's refused where the query is planned, a written literal still reading as the column's type | G2; the user, 2026-11-27, "great do it" | coded, its round green (`lowering/HeldKinds`, the product's rule alone). Dates behind an abstract `Date` (the user, 2026-11-27, "You can do the error way if it's only for lite mode"): a day against a date-time is refused where the question is whether they are the same (`==`, `eq`, `in`, `contains`). Refused in an ordering too, as numbers are (the user, 2026-11-27: "We don't really want date vs datetime to work for greater than/less than vs not for equal do we?"). For one afternoon it was refused for "the same" alone, because the strict rule stopped four of the time-series documents' queries (`FILTER date >= '2024-01-01'` and `date > latest - 2y` over a sampled line): **the wrong fix, taken back the same day**. Their cause was two wrong date facts, not the rule: a truncation to the week, month, quarter or year was recorded a TIMESTAMP where every dialect delivers a DATE and Pure says a day (`sql/SqlTyping`); and a day typed only `Date` adjusted by years became a date-time (`lowering/DateShifts`). Both fixed; **not run** |
| Two tables with `==`: column names and row order | G5; the measurement's "grids" (4, 3, 4) | **coded, not run** where the comparison is the query's answer (`planner/RootComparison.tables`, `lowering/SideEqualities.tables` over the assert's own `grids`): the same columns by name, then the same rows in order, each cell by its column's type; columns that differ in name or number answer false from the types, no read; an Integer column against a Decimal column by value (lite's two families; by kind under a door's three types). Every door alike (upstream's answer for two tables under `==` was not read: no upstream body in the measurement compares two tables so). Inside an expression (`if($a == $b, ...)`) it is as it was: recorded with the lists there |
| Lists of instances, and an instance with a to-many key, by typed leaves (text today) | the measurement's "instances and keys" | not built |
| A keyless instance against itself (`$xs->at(0) == $xs->at(0)`) | G4 | **checked 2026-11-27, wrong, not fixed**: with `let xs = [1, 2]->map(x|^p::K(name=$x->toString()))`, `$xs->at(0) == $xs->at(0)` and `$xs == $xs` answer false on DuckDB and SQLite (Pure: true; an assert of the two passes, its sides planned with the objects' identities), and are refused by name on H2 (no list). Two instances built apart (`^p::K(name='a') == ^p::K(name='a')`) answer false, as Pure does. The product's sides are planned without the objects' identities (docs/OBJECT_IDENTITY_2026_10_20.md): **carried, with the lists of instances below** |
| Maps compared whatever order their entries were written in, inside an expression | the measurement's "other" (`newMap` order) | measured 2026-11-27: `newMap([a, b]) == newMap([b, a])` answered false on DuckDB and SQLite (H2 holds no map). **Coded, not run** where the comparison is the query's answer (a map is a whole side of `planner/RootComparison`, its entries compared as the assert compares them). Inside an expression (`if($m1 == $m2, ...)`) it is the database's own map comparison still: carried with the lists there |
| An hour-precision date-time literal inside an expression (a database error today) | the measurement (4, 4, 0) | **coded, not run**: it was `date(2015, 4, 16, 14)`, a date built to the hour, which keeps how it prints (a text); against a TIMESTAMP the expression's `==` now reads the text as the assert's judge does (`SideEqualities.instantOfText`, register S399) |
| A day against a date-time known only at run time, inside an expression | the measurement's "dates and written precision" (7, 7 on DuckDB and H2) | to re-measure, then build |
| The mixed-days refusal | the measurement (3, 2, 3); the plan's 6.6 | **checked 2026-11-27**: over a WRITTEN list the product answers right on DuckDB, H2 and SQLite (`[%2020-01-01T00:00, %2020-01-02]->contains(%2020-01-02T00:00)` false, `->contains(%2020-01-02)` true, `==` likewise: each element by the kind it was written as), where the assert refuses by name; a comparison of two whole sides refuses as the assert does (`SideEqualities.pair` asks `refuseMixedDays`). The assert's refusal of a written list is the stricter of the two and no wrong answer: left. A list read through a `map` in an expression: to probe |
| An element's own message inside `forAll`, and `assert(cond, msg)` inside `forAll` | the measurement (4, 1, 4); G6 | not built |
| `is` as a product native (`assertIs`, `assertIsNot`) | G6 | **checked 2026-11-27**: `is` is refused by name in a query on every database ("declared by the spec and not implemented by the platform"); no wrong answer. The asserts are the judges'. Left |
| DuckDB's nanoseconds in the product's `==` (today only in the judge's record) | G7; the plan's 6.6 | **checked 2026-11-27**: two written date-times one nanosecond apart are not equal on DuckDB, H2 and SQLite, as a query's answer and inside an `if`. Nothing to build for written values |
| The tolerance assert as its formula (`abs(e - a) <= d`) | the plan's 6.6 | not built |
| An abstract `Date`'s text (`adjust(...)->toString()` at legend-pure's door) | 5b; the plan's 6.6 | not built |
| The JSON lane: `Any` cells compared by JSON kind; a Decimal or a date there refused | G2 | the judge's sides read JSON kinds; inside an expression not checked |
| A Variant read through `Any`: the host judge's `assertEq` held it against the Integer of its text where the database judge fails it | found 2026-11-27 by `AssertBarTest`'s new lines | **fixed, not run** (`HostJudgments.eqOf` reads a Variant side as `assertEquals` does) |
| The two judges' records of a Variant read through `Any` differ (a both-judges run stops on the difference); both judges FAIL each assert, as they should | `AssertBarTest`'s line; probed 2026-11-27 over ten bodies (`TempProbe4Test`) | **cause found, four wording defects, not fixed**. (1) A JSON string: the host reads a side declared `Any` decoded, so `"abc"` arrives as the text inside it and is written `abc`, and the JSON string `"1"` is written `1 (Variant)` as if it were the number; the database judge writes `"abc" (Variant)` and `"1"`, rightly. (2) A JSON object or array against a value of another type: the DATABASE judge's record loses the other side (`expected: [{"a":1}], actual: []` where the actual is `'x'`) and brackets the Variant; the host's is right for an object (`{"a":1}` against `"x"`). (3) A JSON array: the host spreads it as its elements (`[1, 2]`), the database judge holds it as one value (`[[1,2]]`). (4) `assertSameElements`: the database judge writes no kinds where the two texts are the same (`["abc"]` against `["abc"]`). A JSON number, a Boolean and a Float are worded the same by both. The fixes are in the host's read of such a side (its values back to JSON text) and in the database judge's record for a Variant against another type; neither is in the product's equality |
| One side empty inside an expression (`$x.opt == 1` is SQL's NULL) | the measurement's "empties" (3, 1, 4) | the user's decision, after its SQL cost is measured. Measured so far (2026-11-27, DuckDB and SQLite): inside a condition the answer is already Pure's (`if([]->first() == 1, ...)` takes the false branch, `if(!(...))` the true one); as a VALUE (a projected column) it is the database's NULL where Pure says false. So the cost is only in a comparison whose value is kept and one of whose sides may be empty |
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
| `$a < $b`, `less(1, 1.0)` | true: by value, then by family, an exact number (Integer, Decimal) below a Float (decision D3, as amended by the two families' ruling of 2026-11-27: an Integer against a Decimal is the comparison itself, by value), folded into the operator (`<` becomes `<=`), no SQL added | by value |
| `amongWholes(a: Number) { $a->in([0, 1]) }`, `amongWholes(1.0)` | true: a literal the function's author wrote reads as the value's type (decision D5) | the database's |
| `isOneAndAHalf(a: Number) { $a == 1.5 }`, `isOneAndAHalf(1)` | false: 1.5 is no Integer | the database's |
| a literal handed on through another function (`sameAsOne(a) { same($a, 1) }`) | a value there, not a written literal: `sameAsOne(1.0)` false | |
| a side still typed only at run time after expansion (a `Number` column, a computed `Number`) | the comparison as written: the database compares the values it holds | |

A comparison between two generic positions of a body typed into each call (a function over "any relation") takes the
same answer, from that call's column types. To be measured on the timeseries library and both corpora before it
stays: the library compares two lines' period columns and refuses a day against a date-time itself.

Built since (2026-11-27, the user's "yes we should measure and hopefully fix"): legend-pure's own answer at its door
(false for `same(1, 1.0)`, strict for a written literal): its setting reads bodies (`edge/PureComparisons`). An upcast
written in the query (`1->cast(@Number) == 1.0`) is answered there as legend-pure answers it too (false; answers.md
C10b). Pairs, lists and maps whose types differ only inside them stay the database's comparison at that door:
legend-pure's answer for them was not read.

## The outside audits of 2026-11-27 (two reviewers, read-only, of `claude/compat-b3-nufdlk..d3e81136a`)

One reviewer read the type-checker, the two compatibility doors, the host judge, the guards and the records; the
other the lowering, the planner, the SQL and the database judge. Nothing was run by either. What each found, and what
became of it (fixed in `ad1e546a6` and `2d28215ea` unless said otherwise):

| # | Severity | Finding | What became of it |
|---|---|---|---|
| 1 | HIGH | The host judge's sort broke a tie by family whatever the forms, so at a compatibility door `assertSameElements([3, 3.0], [3.0, 3])` passed where legend-pure's stable sort fails it | fixed: lite's rule alone (`testexec/Equality.sorted`; `PureAssertsTest.sameElementsAtADoorKeepATieAsWritten`). The ledger's stated reason for the pin is true now |
| 2 | HIGH | The root comparison changed a door's answer for sides typed `Number` or `Any` (`1->cast(@Number) == 1D->cast(@Number)`: the database's true before, false after), and refused at a door what the plain path answered | fixed: `planner/RootComparison` is the product's own rule alone; a comparison a door admitted is the plain comparison it was |
| 3 | MEDIUM | A let holding `random()` or `generateGuid()` was planned into both sides, drawn twice | fixed: left to the plain path |
| 4 | MEDIUM | One value typed `Number` compared as a query's answer and inside an `if` answered differently (`$row.n == 1.0` over an INTEGER column: false at the root, true inside) | fixed: one value is never a whole side; only a list, a map or a table is |
| 5 | MEDIUM | Two tables compared row by row in the order a database returned them where no sort defines one | fixed: in order only where the query defines both tables' order, else as a multiset of rows |
| 6 | MEDIUM | Two tables' Decimal columns, one a store's text and one typed, had no common carrier | fixed (`SideEqualities.grids`) |
| 7 | MEDIUM | An Integer cast beside a Decimal of a scale past 19 can overflow, a silent clamp on MySQL and MariaDB (from their documentation, not run) | fixed for a pair of sides and for two tables: compared as the canonical text. **Left** for a mixed list's compare column (finding 8) |
| 8 | MEDIUM | An Integer beside Decimals of two scales in one mixed list is compared in the first Decimal's column | **left, recorded**: needs a mixed list holding an Integer and Decimals more than 38 digits apart |
| 9 | MEDIUM | legend-pure's door changed its answer for two pairs whose types differ inside them; legend-pure's answer for them was not read | fixed: that door keeps the database's comparison for pairs, lists and maps |
| 10 | MEDIUM | A comparison kept for its call, reaching the lowering unanswered, was lowered as its bare call | fixed: one whose types the call fixes is refused there |
| 11 | MEDIUM | `TypeInferenceIntegrationTest.testMinNumbers` accepted a refusal on any database | fixed: on H2 and SQLite alone |
| 12 | MEDIUM | A lambda the query writes and hands to a function generic over a relation's columns is typed again as that function's body: its comparison loses its position (the planning-time refusal skips it), an upcast in it is the database's comparison, and a refusal names the generic function | **fixed 2026-11-27, after the user's decisions** (`Typer.callerLambdas`: a lambda its caller wrote into the call is typed as its author's code inside the body; `NumberFamiliesTest.aLambdaHandedToAGenericFunctionIsStillTheQuerys`) |
| 13 | MEDIUM | Where a function's author declared a side open and the caller's two types differ, the answer is the constant false, so a side that would raise (`[1, 2]->at(5)->cast(@Number) == 1.0`) no longer raises | **left, for the user**: the answer needs a form that still evaluates both sides |
| 14 | MEDIUM | A function generic over a relation whose other parameters are declared `Number`, called with `1` and `1.0`: refused at the call where the rule says false (the parameters' declarations are gone once the body is typed into the call) | **carried** (looked at again 2026-11-27): the body is typed with the caller's `1` and `1.0` in it, so nothing there says the parameters were declared `Number`. Giving the arguments their declared type would type every such function's arithmetic as `Number` (the time-series library's among them), a change far wider than this case; marking the two sides alone needs their identity carried through the fold and the typing, for a call that is rare. Stays a refusal that names the fix, never a wrong answer |
| 15 | MEDIUM | The arithmetic rule (S429) changes legend-engine's door on the databases where legend-engine computes a double, and legend-engine's answer was read from its literal processors, not run | **for the user's word** before it lands: it is Pure's answer, which the fidelity tenet takes where legend-engine departs from Pure, posted first |
| 16 | MEDIUM | Size pins raised by regenerated files with no dated reason per row | the rows that go up are brought to the user with their reasons before the regenerated files join the branch |
| 17 | LOW | `date(2015, 4)` against a date-time reaches the instant cast (an error, as before step 6) | left, recorded |
| 18 | LOW | The canonical Decimal text on SQLite (a REAL's exponent) and for an H2 parameter (a DECFLOAT's) | left, recorded: older than step 6 |
| 19 | LOW | `[3, 3D]->max()` keeps the Integer written out and the Decimal computed | left, recorded: which of two equal exact numbers an extreme keeps is not ruled |
| 20 | LOW | The planning-time refusal and the type-checker's rule read different `contains` overloads | fixed |
| 21 | LOW | The library's `within` compared its two dates by their own types, as `meet` did | fixed |
| 22 | LOW | An assert's Integer literal is rewritten, so its failure records the literal as the type it was read as | recorded where the rule is written (`NumberLiterals`) |
| 23 | LOW | Records and javadocs that said what the code no longer does | fixed |

## The third review (2026-11-27, one reviewer at high effort, read-only, of `d3e81136a..b43dadcc9`)

Everything changed since the two audits above, given the diff, the rules and the commits' claims as claims to test,
never the author's conclusions. Nothing run. What it found and what became of each:

| # | Severity | Finding | What became of it |
|---|---|---|---|
| H1 | HIGH | `==` answers differently by where it stands: the strict table rule, the any-order map comparison and the list-as-statements comparison existed only for a query's last statement; inside an `if`, a filter or a test body's `assert(...)` the old path ran (the test road never asked the planner) | **being fixed** (the user, 2026-11-27: "why cant we do the work we promised"): the lowering asks the planner for every `==` and `sameElements` inside an expression, on the product's road and the test body's (`RootComparison.inside`, `d535ced03`); `RootComparisonTest.aComparisonAnswersTheSameInsideAnExpression` |
| M1 | MEDIUM | A LIST side whose order the query does not define (`p::Party.all()->map(p\|$p.name) == ['A', 'B']`) is compared in the order the database returned it; an assert of the same side is refused | fixed below (the refusal a table's side gets) |
| M2 | MEDIUM | `HeldKinds` skipped any author's upcast, so a no-op upcast of a property already declared `Number` (`$a.n->cast(@Number) == $a.f`) escaped the planning-time refusal the same query gets without the cast; the same for dates | fixed below (the skip only where the cast's source has a known type) |
| M3 | MEDIUM | The planner infers who is asking from the number rule a door named (tenet 6) | disclosed; question 4 to the user |
| M4 | MEDIUM | The two pins for `sameElements` moved on words of the user's that named other spellings | disclosed; questions 2 and 3 to the user |
| M5 | MEDIUM | The always-false fix turned two answers into refusals: a never-equal comparison inside a collection's value lane ("not supported yet"), and at the root over whole sides (a list cast to text, which H2, SQLite and BigQuery refuse) | fixed below (the collection lane lowers it as an expression does; over many-valued sides the constant stands, recorded) |
| M6 | MEDIUM | `sameElements` checked two known types only for primitives and enumerations: two lists of unrelated classes were no compile error; and the check runs at planning, not at typing | fixed below for classes (the type-checker's own question); at planning still, recorded |
| M7 | MEDIUM | The `max`/`min` commit was never run and its test reads SQL text alone; the regenerated size files predate ten SQL-changing commits | run since (`b59`, `b62`); the full round regenerates |
| L1 | LOW | Two tables whose column names differ are answered false without evaluating either side | recorded |
| L2 | LOW | `harmless()` took any cast as harmless, a downcast that can fail included; membership and index stay constants | the downcast fixed below; membership and index are piece 3 of the work under way |
| L3 | LOW | "One value read twice is one evaluation" holds at one lambda depth only: a let read outside and inside a lambda is still two evaluations (`$ks->filter(k \| $k == $ks->at(0))`) | recorded, a gap (a suspicion, not run) |
| L4 | LOW | The caller's mark on a lambda is lost where a desugaring rebuilds the lambda without carrying it (the fold's map unroll and static `if`, the legacy column forms, project's and groupBy's rebuilds) | recorded: there the lambda is typed as before the fix |
| L5 | LOW | The two date facts (a truncation to week, month, quarter or year is a day; a day behind `Date` moved by days stays a day) apply at every door and are no register row | **to post to the user** with the corpus run's moved answers; register row to write |
| L6 | LOW | GREATEST and LEAST answer NULL for a NULL argument on MySQL, MariaDB and Snowflake where legend-engine's CASE answers the other argument | fixed below (the CASE legend-engine writes, the Integer converted) |
| L7 | LOW | A comparison typed as code the rule never reads (an island's translation) keeps its position, so the planning-time refusal could take it for the query's own | fixed below (only the query's own comparisons keep a position) |
| L8 | LOW | The fresh-draw check reads the body, not the lets the environment seeds | recorded |
| L9 | LOW | Weak or missing tests: the always-false test accepts any error; no root-against-expression test; none for unordered lists under `==`, `sameElements` over classes, the PCT door's `max`/`min` | the root-against-expression and PCT-door tests exist since; the others added below |
| L10 | LOW | `RootComparison.whole`'s comment out of date; a parser ratchet moved with no dated line | fixed below |

Checked and found sound by the same review: the empty-side back-out is complete; the host judge's tie-break is lite's
door's alone; the `max`/`min` rule does not touch the PCT door; every dialect's truncation to those units delivers a
DATE and Pure's `firstDayOf*` return a day (read in the pinned sources); the Decimal carrying is symmetric; the
guard entries carry dated words; no upstream function is named `sameElements`.

## SQL size, measured (2026-11-27; the user: "def want to see/understand the growth")

The size table regenerated at the stack (`core/src/test/resources/lean-sql/baseline.tsv`), against the program
branch `9adb84bf`: 13,963,445 characters before, 13,473,390 after (490,055 fewer). 39 rows down (521,518 characters),
49 rows up (31,463).

**Why it had grown, and the fix.** With the one equality first built, the `equal` rows went up by half on several
databases (MySQL 51,539 to 77,220). Read in the statements themselves: a query's comparison of two sides carried the
assert judge's failure record beside it, each side's rows a second time, which a query never reads. The comparison
now gives a form without them (`SideEqualities.Compared.answerOnly`), and those rows are below where they started
(MySQL 39,052, Snowflake 45,789 to 16,719, Trino 17,979 to 6,806).

**The 49 rows that go up, each needing the user's word:**
- 45 have ONE MORE STATEMENT than before: a query that was refused and now plans. `equal`, `greaterThan`,
  `lessThan` and `min` of an Integer and a Decimal on each of 11 databases (the two families' ruling: two exact
  numbers compare by value; `2->min(1.23D)`, `9999999999999992D->compare(9999999999999991) > 0`), and H2's mixed
  sort from slice 6.2. No query of these rows is longer than it was.
- 4 have the same statements and longer SQL: PCT's `let` on H2 (126,502 to 136,762), core's `equal` on SQLite
  (24,280 to 27,271), core's `let` on DuckDB (136,469 to 137,877) and on SQLite (29,965 to 31,150).

**Every query whose SQL is longer than at slice 6.2** (each of the 30,488 measured query-and-database pairs compared:
111 shorter by 188,626 characters in all, 21 longer by 19,092, 43 that were refused now planned, none newly refused).
The 21 are 15 queries:
- an instance compared with an instance, or a list of instances with another (`$side1 == $side1`,
  `[^s::Side(...), ...] == [...]`, `[$a] == [$a]`): 14 of the 15, on H2 and SQLite (H2 1,796 to 4,780 for the
  simplest). They are compared by typed leaves now -- the class each instance actually is, then each key by its
  value -- where they were compared as one text of the keys, which kept a Decimal key's written scale and read the
  declared class;
- one mixed list deduplicated, sorted and compared with another, on SQLite (18,253 to 20,600): its sides compared as
  rows, where SQLite has no list to compare.

**The database judge's SQL on PCT (lite's rules; `pct/.../channelb/ratchets.tsv`), measured test by test** (1,249
tests at slice 6.2 and at the stack, 2026-11-27): 3,466,097 characters to 3,475,870. Seven tests are longer and one
shorter; no other test's judge SQL changed.
- Longer, each a list that mixes number types sorted or asked for its extreme: `testLeast_Number` (7,895 to
  11,502), `testGreatest_Number` (7,950 to 11,556), `testMode_Number` (5,398 to 7,978), `testMax_NumbersArray`
  (6,433 to 8,664), `testMin_NumbersArray` (7,971 to 8,968), `testMixedSortNoComparator` (2,753 to 4,445). A tie of
  value is placed by family and two equal exact numbers keep their written order (the two families' ruling), which
  takes the stable, windowed sort where a sort by type did.
- `testCompareDecimalAndLongTypes` (0 to 1,265): refused before, an Integer against a Decimal; it runs now.
- Shorter: `testEqualNonPrimitive` (106,026 to 99,821).
So the suite pins that go up (Essential 901,459 to 903,151, Standard 780,331 to about 793,000) are these seven
tests. For the user's word with the size table's rows.

## The database lanes (2026-11-27, measure only)

| Lane | Base | Head | Result |
|---|---|---|---|
| Postgres, Trino (PCT channels A and B, 20 suite targets) | `9adb84bf` | `43ab8f20a` | every target passes at both; no test's outcome differs between the two (channel A's test lists and channel B's per-body outcomes identical; Trino's query ids in four error messages aside); no ledger or ratchet line out of date |
| MariaDB, MySQL, ClickHouse (PCT channels A and B, 6 targets of 5 suites each) | `9adb84bf` | `1d6dd1a1c` | channel A on all three, and channel B on MySQL and ClickHouse: every suite passes at both, no test's outcome differs. **MariaDB's channel B fails at BOTH commits and differs from run to run** (base 25 unledgered failures; head 26, and 20 when run again; 16 tests differ between two runs of the same commit): every one a list-order assert, the same elements in a rotated order (`testTakeManyOnNonEmptyList`: expected `[1, 2]`, actual `[2, 1]`), the expected side rotated too in several. Not step 6's: a list's elements come back from MariaDB in no stable order on that road, at the base as at the head. **Recorded for the database follow-up; not looked into** |
| SQLite, MongoDB (PCT channel A on both, channel B on SQLite; 3 targets of 5 suites each) | `9adb84bf` | `d44222cab` | every suite passes at both; no test's outcome differs; no ledger or ratchet line out of date. (MongoDB's grouped results come back in another order from run to run, which no test reads; the plans are identical) |
| Snowflake (PCT channels A and B) | `9adb84bf` | `d44222cab` | channel A 1,249 tests pass at both, none differs; channel B's five suites pass at both with the same per-body counts and outcome lines; no ledger or ratchet line out of date |
| BigQuery (PCT channels A and B) | `9adb84bf` | `d44222cab` | no test's outcome differs. Channel A: every suite passes at both. Channel B: grammar, relation and standard pass at both; essential (16 tests) and unclassified (3 tests) fail at BOTH, the same tests with the same text ("Column N in UNION ALL has incompatible types: INT64, STRING"): older than step 6, the queued task "Fix BigQuery channel B: untyped NULL in the batch union" |
| Databricks (PCT channels A and B) | `9adb84bf` | `efe7bcfc7` | no test's outcome differs: channel A 1,249 tests pass at both; channel B's five suites pass at both, the same 21 errors at both (date-times of 7 to 9 fraction digits refused, out-of-range dates, an unresolved column under SELECT DISTINCT in 8 relation tests: all older than step 6) |

## The user's decisions of 2026-11-27 after the audits, and what is left (the working list)

**Decided, in the user's words:**
- The size rows that go up: "1 okay on size". The regenerated files are on the branch (`efe7bcfc7`).
- Exact-decimal arithmetic at legend-engine's door: "2 okay A" (Pure's answer at every door, the difference recorded:
  S429, differences item 24).
- A comparison that is always false but whose side would raise: "4 agreed" -- built for `==`
  (`TypedRunTimeComparison.answeredNeverEqual`, `Equalities.neverEqualButEvaluated`); `in`, `contains` and
  `indexOf` with a side that may raise keep their constant (recorded).
- One side empty: "Yes do the empty side fix", "okay fix for lite if small" -- **built, found not small, backed
  out the same day** (below). The doors: each as its own upstream answers (legend-pure false, legend-engine the
  database's NULL); legend-pure's door not yet checked.
- Two tables under `==`: strict, an unsorted side refused, and a separate function for the rows in any order
  ("Yeah or ~= if want to be cute, or setCompare()"; named `sameElements`, after `assertSameElements`: `~=` would be
  a new operator in a grammar checked against upstream's, and "set" would say duplicates do not count). The strict
  half is built (`2d4edec3f`). **`sameElements` is built, not yet run**: `meta::legend::lite::sameElements(Any[*],
  Any[*]): Boolean[1]`, user-facing by its bare name (`Pure.Lite.SAME_ELEMENTS`, `LITE_SURFACE`); planned as a
  query's last statement by `planner.RootComparison` over `SideEqualities` with the multiset member (the code
  `assertSameElements` is judged by); two sides of two known types a compile error; inside an expression refused by
  name (`Scalars.sameElementsInsideAnExpression`). Register S430, differences item 25. The catalog's count of
  functions upstream does not declare goes 76 to 77 on the user's word above (`CatalogUpstreamDiffTest`); the
  generated lists follow at the next regeneration.
- The lanes: "Have to run the lanes always when making major changes" (in AGENTS.md).
- The date rule: strict for "the same" and for an ordering alike ("We don't really want date vs datetime to work for
  greater than/less than vs not for equal").

**A Float COLUMN combined with a Decimal:** kept as it is ("Okay keep the float thing then as is"): the database's
double under a type that says Decimal, recorded in register S429. Not typed a Float in lite's own mode (the
recommendation put to them), not converted through text.

**The empty-side fix, backed out (2026-11-27).** The user's condition was "fix for lite if small". It was built as
a guard written by the comparison itself (`x IS NOT NULL AND x = v` wherever exactly one side may be empty), and the
run of `261ee728d` showed it is not small: the same comparison is also a filter's, a join's and an `exists`'s
condition, a `CASE WHEN`'s test, an enumeration mapping's decode and a union's route-key join, and the guard appeared
in all of them (five targets failed: `JoinTortureTest`, `LowerRelationDuckDbTest`, an enumeration mapping's SQL,
`UnionTargetLeanJoinTest.oneKeyPerArm`, a planner test). Stripping it again from a filter and a join (`d39d70540`)
covers two of those places, not the rest. So the guard and the stripping are both removed; the answer is as it was
before step 6 (right inside a filter, an `if` and under `!`; the database's NULL where the comparison's value is
shown as a column). **What the fix needs:** the guard added only where a comparison is kept as a VALUE (a projected
or extended column, a grouping key), by the code that writes that column, never by the comparison -- a slice of its
own, with the SQL size of every query that shows a comparison measured first. Carried; the measurement's "empties"
bodies stay in the list of bodies that differ.

**The always-false fix: one correction to what was said.** The example put to the user
(`[1, 2]->at(5)->cast(@Number) == 1.0`) was wrong: a written `1.0` against a whole number reads as `1`, so that
query compared two whole numbers and already raised. The case that answered a quiet false is a literal that cannot
adapt (`== 1.5`) or two computed sides of different types; the test now uses those
(`RunTimeComparisonTest.aComparisonOfTwoTypesStillRaisesWhatItsSideRaises`).

**Run 2026-11-27 at `5e9ed346c`** (16 targets: core's exec, compiler, lowering, planner, integration and test-plan
suites, the time-series documents and island, the guards and the census, PCT on DuckDB essential and grammar, PCT
channel B's three lite suites): all 16 pass. That run covers the back-out above, the always-false fix, one value
read twice is one evaluation (`UserCallInliner.finish`, `ff19de458`) and strict `==` on tables (`2d4edec3f`).

**Run 2026-11-27 at `38b367872`** (18 test targets: the 16 above with core's root suite and the time-series
library): all 18 pass. That run covers `sameElements` and a caller's lambda in a generic function's body.

**Built after that run, not yet run:** `max`/`min` of an Integer and a Float at legend-engine's door.

**Put to the user 2026-11-27, on their question "any more deferrals or open decisions or worries ... did we actually
get to real one equality ... did we stick to our tenets?" -- awaiting their word:**
1. *The answer can depend on where the comparison stands.* As a query's answer two maps written in different orders
   are equal and two tables compare column names and row order; inside an `if` or a filter the old path still runs
   (reordered maps false on DuckDB and SQLite; tables as before step 6). Proposed: refuse maps and tables inside an
   expression by name on lite's own route until they are built, after measuring which of our own tests rely on it
   (the probe `TempProbeTest6`, same comparison at the root and inside an `if`, is written and to run).
   **Measured 2026-11-27 (`TempProbeTest6`, the same comparison as `|a == b` and as `|if(a == b, |'same', |'other')`,
   lite's own route):**

   | comparison | as the answer | inside an `if` |
   |---|---|---|
   | two maps, entries in another order | DuckDB true; H2 and SQLite refused by name | DuckDB **other**; SQLite **other**; H2 a database error (`MAP_FROM_ENTRIES` not found) |
   | two tables, the same rows | true on all three | DuckDB, SQLite same; H2 a database error (`LIST` not found) |
   | two tables, rows in another order | false on all three | DuckDB, SQLite **same** |
   | two tables, other column names | false on all three | DuckDB, SQLite **same** |
   | a computed list against a written one | right on all three | DuckDB, SQLite right; H2 refused by name |
   | two Decimal lists, other written scales | true on all three | DuckDB same; SQLite **other**; H2 refused by name |
   | lists typed only at run time, a mixed list, `list(...)`, `pair(...)` | right | right where it is planned |

   So inside an expression two different tables answer "the same" on DuckDB and SQLite, reordered maps answer "not
   the same", and SQLite compares two Decimal lists by their text. Each was so before step 6; step 6 made the
   answer right only where the comparison is the query's last statement.
2. *The name `sameElements`* (the user offered `~=` or `setCompare()`): a public name, to confirm.
3. *The two pins moved for it* (lite's surface 9 to 10; functions upstream does not declare 76 to 77): to confirm.
4. *The planner tells lite's own code from a door's by the number rule the door named*
   (`RootComparison`: `numbersOf != TWO_FAMILIES`; `HeldKinds` the product's rule alone by the same signal): the
   core inferring who asks, a strain on tenet 6. Proposed: recorded debt for the edition's guard phase, where the
   type-checker's rewrite replaces the question.
5. *The empty-side fix* (with `in`/`contains`/`indexOf` and a side that would raise): its own slice right after
   landing, before step 8?

**What the landing waits for, all on the final commit:** the full round; the relational corpus (never run on this
stack); the acceptance measure, every remaining difference explained; every database lane and the heavy run; one
independent review of everything changed since the two audits; CI on every operating system; any SQL size row that
goes up brought to the user first.

**Left to do, in order:**
1. `sameElements(a, b)`: built and run (above). **Upstream's answer for `==` on two tables, RUN 2026-11-27** (a
   worker session; interpreted and compiled legend-pure 5.99.0, legend-engine 4.145.0): legend-pure compares two
   tables by IDENTITY -- two written tables with the same rows are not equal, a table equals only itself, and
   `assertEquals` of two identical written tables fails; legend-engine never plans the comparison (refused by name
   store-less and inside a store query). In register S430 and differences item 25. **To put to the user:** lite's
   by-value rule was ruled before this was run; it departs from Pure's identity, which is an artifact of a class
   with no equality key.
   The same session ran `max`/`min` inside legend-engine's H2 store query: two COLUMNS (3, 3.0) answer 3.0 whichever
   wins, as built; two WRITTEN values answer the winner in its own type (`max(3, 3.0)` is 3, `max(3.0, 3)` is 3.0),
   so the rule now leaves two written values to the rule that keeps a tie's own element. **The follow-up, RUN the
   same day** (each statement's column type read over JDBC on legend-engine's H2 2.2.224): a column against a
   written literal of the other type, and a computed Integer against a Float column, come back DOUBLE whichever wins,
   as two columns do -- as built. An Integer column against a Decimal column comes back a Decimal (NUMERIC(12,2),
   each value in its own scale) and a Decimal column against a Float column H2's DECFLOAT: lite keeps the winner's
   own type there; recorded in S428, not built (one database's promotion, not guessed for the others).
2. The two type-checker leftovers of the audit: the first is fixed (a query's lambda handed to a function generic
   over a relation's columns is the query's code: `Typer.callerLambdas`), to run; the second is carried with its
   reason (the audits' table, row 14).
3. `max`/`min` of two arguments at legend-engine's door in the database's type: built for an Integer and a Float
   (the pair that was run upstream), to run.
4. The test checker's small items: the two judges' wording for a Variant read through `Any`; the tolerance assert as
   its formula; the asserts the database judge cannot judge, read one by one.
5. legend-pure's door for an empty side: **measured 2026-11-27** (`[1, 2]->filter(x | $x > 5)->first() == 1` as a
   query's answer): no value (NULL) at all four doors -- lite's, legend-pure's, legend-engine's API and its PCT run --
   where Pure says false; under `!` true and inside an `if` the false branch at all four, as Pure. So legend-pure's
   door has the same gap as lite's own, and the fix that is carried (the guard written by the code that shows a
   comparison as a value) covers both.
6. Run again: the full round, `tools/equality-measure/run.sh` (the step's acceptance measure), every lane and the
   heavy run; then one follow-up review of everything changed since the two audits.
7. Then: the regenerated files once more, the push to the pull request's branch for CI, the program branch. Nothing
   to `main` without the user's word.

**The lanes so far** (each at the stack before the fixes of this section): Postgres, Trino, MariaDB, MySQL,
ClickHouse, SQLite, MongoDB, Snowflake and BigQuery show no test whose outcome differs from the base. Databricks and
the heavy run (the local gate, the relational corpus) have not reported.
