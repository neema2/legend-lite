# Third independent review of slice 3's product change (2026-11-28, about 03:15 UTC)

*An Opus reviewer, read-only, given the diff `claude/step-6b-slice-2..25c9d78c9 -- core/src/main` and ONE question:
for which Pure expressions does the code answer differently from the landed code, and is each new answer the
rule's. It read the code and ran nothing; the author then RAN its first finding (below). Shortened.*

## The one regression (two faces of one cause)

**The value form and the emptiness tests read a list's raw SQL value, which holds a NULL slot where an element was
empty; the planner's statement they replace compared rows with those dropped.** A `map` whose function may answer
nothing writes no compaction (`ListEncodings.map`); only `size`, `at` and `indexOf` compact (`CollectionLanes`).
- `[1, 2]->map(x | if($x == 1, |true, |[])) == [true]` on DuckDB: true before, false now (the same on a rows
  database is right: the rows that ride drop the NULLs).
- `[1, 2]->map(x | if($x > 5, |1, |[])) == []` on every database: true before, false now -- for every plain kind,
  exact numbers and Strings included, because "the written empty list against a plain kind" is new in
  `comparedAsValues`.
- **"The same flaw already exists in the base for exact numbers and Strings on DuckDB since slice 1."**

**Run by the author, 2026-11-28 03:10 UTC, and confirmed:** `[1, 2]->map(x | if($x == 1, |1, |[])) == [1]`
answers FALSE on DuckDB on the scratch code AND on the landed slice 1 code (`SELECT (list_transform([1, 2], x ->
CASE WHEN x = 1 THEN 1 ELSE NULL END) IS NOT DISTINCT FROM [1])`); SQLite true. The second example false on DuckDB
and SQLite on the scratch code.

**Fixed** (`CollectionLanes.compacted`, `holdsNoEmptySlot`): the two values of the instruction, and every length
that answers "is this list empty", read the list with its empty slots dropped, unless the list can hold none by
how it is written (a written list of single values; a `map` whose function answers exactly one value; a `filter`
or an upcast of such a list; one value that is always there) -- so the short SQL of slice 1 stays as it is for
those. Three rows of the measure hold the cases.

## Unsure, likely rare regressions: the planner stands aside by Pure type alone

For DateTime the lowering reads a text list against a TIMESTAMP list, and two text lists both declared DateTime;
for any other carrier (a text list against a DATE or a zoned date-time list; text elements that are plain VARCHAR;
JSON) it leaves the plain `=` of two differently typed lists, where the planner's statement read each row as its
instant. **Fixed:** the planner stands aside for days and date-times only where each side's own statement holds
them as the value form reads them (a day as DATE; a date-time as TIMESTAMP or its written text); held any other
way, the planner's statement answers as before (`RootComparison.heldAsTheValueFormReads`).

## A minor door change

`UserCallInliner.atTheCall` folded register S169's pairs after the call's rule with no test for quoted code, so at
legend-pure's door a kept comparison inside a quoted lambda became `false` where it had been left as written.
**Fixed otherwise:** no fold is made after the call. The door's setting says whether a kept comparison is answered
by its types (`CrossTypeRule.answersAKeptComparisonByItsTypes`: lite's own alone), and only then does the inliner
leave that one fold unmade; every other door folds where it rewrites, as it always did.

## Refusals that move (no answer lost)

DuckDB refuses a sub-query inside a list's function when the plan is made, where the database refused it when the
query ran. Two catchers of that refusal behave differently for it: a fused batch of verdicts is split, so the
failure is blamed on the right assert; and a plan's text rendered and never run (`executionPlan(...)`, an activity)
records no SQL for such a query where it recorded the SQL that would have failed. Not changed.

## Notes

1. The arm for ONE list of texts against held date-times does not test the declared type (a `Date`-declared list
   of years would be cast to an instant: a run error, as the base's). **Tried and withdrawn:** requiring the side to
   be declared DateTime left a `Date`-declared list of date-time texts to the database's `=`, which answered FALSE
   inside a list's function (the measure's row "Date lists built and written"); a run error for a year is the
   base's behaviour, a false is a wrong answer. Open as it was.
2. `RootComparison.kind` still reads a `Number` side's SQL carrier: on ClickHouse, BigQuery and Snowflake
   `[1]->cast(@Number) == [1.0]->cast(@Number)` is true as the query's answer (a Float literal is a Decimal in the
   SQL model there) and false inside an expression. As before the diff. Open.
3. On DuckDB two list values with no rows riding compare with `IS NOT DISTINCT FROM`: two NULL lists true
   (right), a NULL list against `[]` false (Pure: true; NULL before). Open; the compaction above does not change it.
4. The planner reads the rows' SQL type and the lowering the value's: a Decimal held as its text in one and as
   itself in the other would disagree. Not shown reachable.

## Came out clean

Review 5's three blockers (no promoted SQL type read as a kind; two DateTime text lists by instant in values and
rows, both orders; the abstract Date never takes the value form). Floats, Booleans and DATE/TIMESTAMP lists on the
rows databases answer as before (the same rows). Every new arm is the product's rule's alone; both doors route `==`
through their own rule and the planner is not asked at a door. The resolver walks give no wrong answer: a refusal
by name becomes the rule's false. Apart from the door note, the inliner changes only lite's type-variable case.

## Not determined by reading

Whether a `map` with a function that may answer nothing really leaves NULL slots (run by the author: it does);
how each rows database writes a list function over two written DateTime text lists with no rows riding; where
text-, JSON- or zoned date lists come from; MongoDB's plan on the base; ClickHouse's Decimal against Float64.
