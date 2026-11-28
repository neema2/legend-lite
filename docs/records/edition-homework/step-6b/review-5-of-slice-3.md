# Second independent review of slice 3's product change (2026-11-28, about 02:40 UTC)

*An Opus reviewer, read-only, given the diff `claude/step-6b-slice-2..HEAD -- core/src/main` of the scratch branch
at `fa6de11c2` (after the author's fixes to the first review, `review-4-of-slice-3a.md`), the rules, and the first
review to verify. It read the code and ran nothing. Its findings, shortened; the author's disposition is in the
status log and the commit that follows this record.*

## Blockers

**1. `heldApart` reads a promoted SQL type as the value's kind** (`Equalities.kindHeld`,
`MixedEncoding.kindOfCarrier`). `kindOfValue` returns null for a CASE, arithmetic or a COALESCE because there "the
SQL type is a PROMOTION, never the value's own"; the fallback then reads exactly that type.
- `if($x.flag, |1, |2.5) == $x.intCol`, flag true, intCol 1: the `if` is typed Number, its CASE is typed DOUBLE, so
  "Float" against "Integer": false on every database. The rule says true; the base wrote `CASE … = intCol`, true.
- ClickHouse, BigQuery, Snowflake: `$x.n * 1.5 == $x.f` (a Float literal is a Decimal in the SQL model there):
  "Decimal" against "Float", false; by value before.
- `if($x.flag, |t::E.A, |'x') == t::E.A`: an enumeration's value lowers to a text, so "String" against the
  enumeration: false; true before.
Fix: the carrier only where the SQL type is the value's own; never a Case, Coalesce, arithmetic or a parameter.

**2. (a regression) Two computed lists of date-times, both carried as texts.**
`[date(2015,4,16,14), date(2015,4,16,15)]->filter(x|true) == [date(2015,4,16,14,0),
date(2015,4,16,15,0)]->filter(x|true)`: true by the rule (S399) and on the base (the planner's statement read each
row as its instant). Now the planner stands aside, `asInstants` skips two text lists, and the database compares
`'…T14'` with `'…T14:00'`: false everywhere. The reason given ("a list may hold a year") does not hold for sides
typed DateTime: such a list holds no partial date.
Fix: both lists through `instantsOfTexts` where both sides' kind is DateTime; or DateTime out of
`comparedAsValues` until that is built.

**3. (a regression) Sides declared the abstract Date: the planner stands aside where the lowering cannot answer.**
(i) The rows ride raw: `instants(rows, <declared type>)` returns the rows unchanged for `Date`.
`[date(2015,4,16,14), …]->cast(@Date) == [%2015-04-16T14:00:00, …]->cast(@Date)`: right on DuckDB, text rows
against TIMESTAMP rows on H2, Postgres and SQLite; the base gave the tagged form, right. (ii) Both sides texts: the
planner returns null, the lowering does not take the pair (`plainList`), and the plain `=` answers false. The
planner's test is "the side has typed rows", not "the lowering will take it".
Fix: the kind under the upcast to `instants`, the planner's test as strict as the lowering's; or date kinds out of
`heldInOneKind` for now.

## Should fix

**4. `n.pos() == null` also skips comparisons in a user's function.** `EqualityClasses.notTheQuerys` clears the
position of every body comparison. `function t::same(a: Any[1], b: Any[1]): Boolean[1] { $a == $b }` called with a
VARCHAR column and a BIGINT column, both declared Any: the call's rule returns the plain call, `heldApart` is
skipped, DuckDB's `'1' = 1` is true; the same comparison written in the query answers false. Not a regression; the
diff makes the two disagree. Fix: mark "lite wrote it" apart from "the function's own".

**5. The kept-for-its-call skip fits `==` of two values only.** For `in` and `contains` register S169's fold runs
per element inside their own folds, so `t::inT(t::E.A, ['A', 'B'])` is false on lite's door where it should be that
call's compile error. As before the diff.

## Notes

6. `heldApart` answers false for a mismatch only a column gives where the rule's refusal reads numbers and dates
   alone: `$x.d == %2020-01-01` with `d: Date[1]` on a VARCHAR column would be no rows where the database coerced
   the text before. An unrecorded change of an answer (not confirmed how the resolver types such a property).
7. `UserCallInliner.atTheCall` now folds inside quoted code (it has no `quotedFrames` test; `rewrite` never folds
   there).
8. Other resolver walks do not know `TypedRunTimeComparison`: `StackBuilder.projectable` is false by default, and
   `SyntheticHeads` (`liftFilteredHeads`, its mapper) has `default -> n`, so the record's sides are not visited. The
   substitution refused the record by name before; now it passes and those walks skip its inside (read, not run).
9. StrictTime still reaches the database's `=` (`literal()` lacks a time literal, `kindOf` names no StrictTime). As
   before the diff.
10. An empty list of no stated type reads as holding a value in `holdsNothing`.
11. Two javadocs orphaned by insertions (`MixedEncoding.kindOfValue`'s, `RootComparison.valueType`'s); DuckDB's
    comment says 1.4.4, the jar is 1.5.6.

## The first review's findings, as it found them now

Fixed: blocker 1's first example (text against held, DateTime-declared), SQLite's one form, blocker 2 (no side
dropped; doors untouched), should-fix 4 and 5, note 6 (typed lists). Partly: should-fix 3 (the carrier fallback is
finding 1 above), note 7 (TIME, finding 9), note 8 (over-broad, finding 4). Not fixed: blocker 1 for Date-declared
pairs (finding 3) and two text lists (finding 2, a regression); note 9 (as before the diff, no regression); note
10's version text.

## Found nothing in

The fold after the call's rule for each door (legend-engine's makes no record; legend-pure's still answers S169
false; lite's gives the compile error); `Substitution`'s mapping of the record's children; the planner lowering its
sides (already so on the base) and the place of its early return (after the order rule and the fresh-draw refusal;
`hasSameElements` excluded); the value traces for lists typed Number; nothing can fire at a door; each side written
once in the three "never equal" forms; `readsAs`'s direction; DuckDB's reading of what it writes.

## Not checked by the reviewer

No run of any kind; H2, Postgres and SQLite outcomes read from the code; how the resolver types a Date or Any
property on a VARCHAR column; MongoDB.
