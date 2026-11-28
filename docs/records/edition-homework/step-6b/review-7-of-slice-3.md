# Fourth independent review of slice 3: the last fix pass (2026-11-28, about 03:45 UTC)

*An Opus reviewer, read-only, given `git diff 36d7f07c1 675d1b8dc -- core/src/main`, the rule and review 6. It read
the code and ran nothing. Shortened; the dispositions are in the commit that follows this record.*

## Blocker (by reading): ONE date-time carried as its text against a computed LIST of such texts

`date(2015,4,16,14) == [date(2015,4,16,14,0), date(2015,4,16,15,0)]->take(1)`: both sides DateTime, both carried as
texts, so the planner stands aside (`heldAsTheValueFormReads`); `asInstants` reads a single TIMESTAMP beside a list
of texts, not a single TEXT, so nothing is read as an instant and the two text lists are compared: false. The rule
(S399) and the landed code (the planner's statement, rows read as instants): true. Fix: a single text beside a
list of texts read as the list of its instant, both sides declared DateTime.

## Should fix: the written-`[]` ladder changes answers at both doors, unruled

`Equalities.equal`'s ladder runs for every door's rule. (1) The computed list is now compacted there too: `[1,
2]->map(x | if($x > 5, |1, |[])) == []` false before at legend-pure's and legend-engine's doors, true now (Pure's
answer; legend-engine's neither read nor asked; no register row). (2) A side that is not many but whose SQL value
is an array went from "is it NULL" to its length (the old test keyed on the side's multiplicity, the new one on
the SQL value). Fix: the new reading for the product's rule alone; the doors keep the old line.

## Should fix (an incomplete fix, not a regression): an empty slot survives a later map or filter

`[1, 2]->map(x | if($x == 1, |1, |[]))->map(y | if($y > 0, |'a', |'b')) == ['a']`: the second `map` is applied to
the empty slot and writes `'b'`; compaction at the comparison has nothing left to drop: false, Pure true. The landed
code: false too. The cause is the producer: a `map` whose function may answer nothing writes no compaction, and
the next list function reads the slot as an element. A list defect of its own, not the comparison's.

## Notes

- `holdsNoEmptySlot` is sound for the shapes it names; it is wrong only for a value stamped exactly-one that the
  SQL writes as NULL (a `[1]` property over a nullable column, the store resolver's composed map, a JSON null under
  a Variant cast), every one already outside Pure's meaning and, for exact numbers and Strings, the landed
  behaviour.
- SQL size: the default arm compacts `sort`, `distinct`, `take`, `drop`, `slice`, `reverse`, `concatenate` even over
  clean sources (a `list_filter` each); and `CompactList` is not "always evaluated" for the let-binding rule, so a
  repeat inside it that can raise is no longer bound once. Either could raise a size pin; arms for those shapes
  that recurse into the source would keep the SQL as it was.
- A kept `in` or `contains` with a type variable given an enumeration still folds to false before lite's refusal
  (as landed; slice 5).

## Clean

Compaction and the dialects (a NULL slot in a typed list of scalars always means empty; the gate before compaction
is still "a typed list of scalars"; positions kept; values and rows agree; every dialect that reaches the
two-value form writes the compaction; H2 never wrote list values). The planner's early returns for every kind
(nothing the landed code compared as values is now planned). The inliner: at every door but lite's it is the
landed code byte for byte; lite's call rule cannot hand back an enumeration-against-text pair as a plain call.
Invariants 2 and 3a.

## Not determined by reading

Whether a written hour- or minute-precision date is carried as its text or as a TIMESTAMP; whether a value's
carrier and its rows' carrier can differ for date lists; legend-engine's answer for the moved ladder cases; MariaDB's
and SQLite's writing of the new list filter inside a row's function.
