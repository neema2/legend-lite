# Independent review of slice 5 (2026-11-28, about 04:30 UTC)

*An Opus reviewer, read-only, given `git diff 4b77dadaf cf1197356 -- core/src/main` and then
`cf1197356..0e97fbc3b` of the scratch branch `claude/step-6b-slice-5`, the equality rule, and seven things to check
by name. It read the code and ran nothing. Shortened; the author's disposition under each, each example then RUN by
the no-Bazel loop.*

## Findings

**1. (high) `hasSameElements` inside an expression refused on every database that compares rows, for two written
lists that are no exact numbers** (`if(['a','b']->hasSameElements(['b','a']), |'y', |'n')`): the value form is tried
before the planner's statement, and two written lists rode without their rows, which is the only writing such a
database has. Also: the value form did not ask how a list of days or date-times is held, as the planner does for
`==`.
- *Fixed.* In any order the rows always ride (`Equalities.sameValues`); a list of days or date-times held any other
  way than the value form reads it goes to the planner's statement (`heldAsItsDateKind`). Run: answered on H2,
  DuckDB and SQLite.

**2. (high) The list of what a held type is read through named function ids the type-checker never emits**: `take`
and `limit`, `drop`, `slice`, `sortBy` and the deduplications reach the lowering as their own typed nodes
(`TypedLimit`, `TypedDrop`, `TypedSlice`, `TypedSortBy`, `TypedDistinct`), so
`[1, 2]->cast(@Number)->take(1)->contains(1.0)` was still the database's membership.
- *Fixed* (`EqualityClasses.valuesOf` reads the typed nodes; the dead ids are gone). Run: false through `take`,
  `drop` and `sortBy`. (The author had these arms, lost them undoing a debug line, and rewrote them from memory
  with the ids alone.)

**3. (medium) A list's `indexOf` took the new reading at the compatibility doors too** (no door row named a rule
for it): at legend-pure's door a date-time built to the hour was found among written date-times by its instant,
where that door's rule reads the written precision as part of the value.
- *Fixed.* `Scalars.indexOfRule(numbers)`: the product's rule alone asks `Equalities.member`; the doors' table
  names their own for a list's `indexOf` (`edge/UpstreamTies`, beside `contains` and `in`), which keeps the
  database's own position as before the slice.

**4. (medium) "No element is the needle" evaluated the list by casting it to a text**, which Trino and BigQuery
refuse for a list.
- *Fixed.* The list is evaluated by its emptiness (`Scalars.noElements`), the needle as a single value
  (`Equalities.neverAmongButEvaluated`).

**5. (medium) A year or a day read as "no instant" is a NULL in its list, and a database that compares two lists
calls two NULLs in one place the same**: `[%2020]->cast(@Date) == $x.optDateTime`, the date-time empty, answered
true.
- *Fixed* for the list arm: the held side's own empty slots are dropped there (`Equalities.heldInstants`), so a NULL
  on the texts' side meets no NULL. Run inside a list's function: false.
- *Left, as before the slice, recorded:* ONE year written under the abstract `Date` against ONE held date-time
  fails when the query runs (the single-value arm reads the text as an instant). Reading it as "no instant" there
  would answer true where both sides may hold nothing and the held one does: a wrong answer in place of a failure.

**6. (medium to low) The compaction of a mapped list where it is built runs at the doors too, and a consumer that
compacts wrapped it a second time** (two filters).
- The doors: left so, on purpose. `[1, 2]->map(x | if($x == 1, |1, |[]))->map(y | 7)` answering `[7, 7]` is wrong
  at every door (Pure's `map` drops what its function does not answer); the compaction is the list's own repair,
  no rule of lite's. The round's corpus lanes say whether any pinned SQL moves.
- *Fixed:* a consumer leaves a list that is already compacted (`CollectionLanes.compactIfValueLane`, the root's
  unnest). The two text-form matchers the reviewer suspected (`sql/dialect/CellTexts`, `edge/PurePrintForms`) match
  the join's OWN per-element transform, whose source the user's `map` is; a compacted source does not hide it.

**7. (low) Two optional enumerations at legend-pure's door are now compared null-safe**: Pure's answer; that door's
SQL moves for such a pair. legend-engine's door inside a store query is untouched (its table answers the empty
side itself). A mapped enumeration's two raw values that decode to nothing (a NULL and an unmapped value) compare
false where Pure says both are empty: an edge left.

## The second commit (the held type read deeper, in the type-checker)

The reviewer traced each caller: on lite's route the same answers, given earlier (by the type-checker) or where the
lowering gave the database's (inside a function of `Any`); at legend-pure's door
`[2, 1]->cast(@Number)->sort()->contains(1.0)` and `[1, 2]->cast(@Number)->first()->in([1D])` are now false, which
is legend-pure's own answer (three number types there) where the database's was true. A door's answer moves toward
its own upstream: for the user's list. It found no value read as a type it does not hold.

## Found nothing in

Membership against `==` for Integer and Decimal, a String and an enumeration, a mixed list; a needle that raises
still raises; `in` with an empty optional needle still false; the String `contains`; layering (typed nodes only, no
type inferred, no fallback).

## Not checked by the reviewer

No run of any kind (the author ran each example after); MongoDB; the pinned corpus SQL at legend-engine's door.
