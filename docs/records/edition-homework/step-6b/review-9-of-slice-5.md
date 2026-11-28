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

## The audit of these fixes (a second Opus reviewer, read-only, `0e97fbc3b..d988dd043`; 2026-11-28, about 04:45 UTC)

Found nothing in: `indexOf` at the doors (the door's path is the body the rule had before the slice; nothing a
door's call passed through is skipped; the String `indexOf` untouched); each arm of the held-type reading; two
written lists on a rows database (their rows have the columns the form reads); "no element is the needle" (a list's
elements are ordinary arguments in every dialect read, so a raising element still raises); the held side's
compaction beside texts read as "no instant"; a compacted list left alone by a consumer.

Its findings, and what was done:
1. **(medium) The deeper reading of a held type moves legend-pure's door for four more functions** (`take`, `drop`,
   `slice`, `sortBy`): `[1, 2]->cast(@Number)->take(1)->contains(1.0)` was the database's membership (true) and is
   now legend-pure's own answer (false). *Kept, and on the user's list:* at legend-pure's door, a value upcast to
   `Number`, `Any` or `Date` and then filtered, sorted, deduplicated, reversed, or cut (`take`, `drop`, `slice`,
   `tail`, `init`), or one value read from such a list (`toOne`, `first`, `last`, `at`, `head`), is compared by the
   type it holds, as legend-pure compares it. legend-engine's door does not move (its rule keeps the call).
2. **(low to medium) A mixed list was wrapped before its emptiness was tested**, hiding it from the rule that
   counts its positions. *Fixed* (`Lowerer.withoutEmptySlots` leaves a mixed list).
3. **(low) Two written String lists under `hasSameElements` are compared as rows on MariaDB and MySQL, whose
   collation pads spaces**: `hasSameElements(['a'], ['a '])` true there. As before the slice (the planner's
   statement compared rows too); the pad-space behaviour is the database-recheck record's. *Left, noted.*
4. **(low) A list of date-times held zoned goes to the planner's statement** (the value form reads DATE and
   TIMESTAMP alone, as the planner's own test for `==` does). *Left: the same line as `==`'s.*
5. A comment that said a list's deduplication is never a call (it is), and an indentation. *Corrected.*

## The audit of the last arms (a third Opus reviewer, read-only, `4d73d7746..6f33e9206`; about 04:55 UTC)

No correctness defect in the two arms (two single date-time texts by their instants; a built date-time among
built ones). It traced: no arm shadowed (a year or year-month is typed the abstract `Date` and answered before);
nothing at a door; an empty side stays empty through the reading; the whole-sides code reads the same pair by
instants too, so an assert and `==` now agree.

1. **(medium) Recorded, not changed:** a date-time text with a digit past the microsecond
   (`%2020-01-01T10:00:00.1234567->adjust(1, DAYS)`) is refused by name where it is read as an instant (the
   database's TIMESTAMP does not hold it). Two such texts, the same to the letter, compared equal as texts before
   the arm; now the comparison raises that refusal, as a text beside a held date-time and two lists of texts already
   did, and as the assert of the two does.
2. (low, a suspicion) `date(2020)->cast(@DateTime)` compared with a date-time now fails at the instant's cast where
   the texts were compared (false); Pure fails that cast.
3. A stale sentence in `asInstants`' javadoc. *Corrected.*
