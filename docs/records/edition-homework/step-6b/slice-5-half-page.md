# Slice 5: the other comparing functions ask the same decisions (2026-11-28, written ahead of the slice)

*The half page the plan of record asks for before building. Read so far: `lowering/Scalars.containsRule` (the
membership rule, about 130 lines of arms), the measure's third matrix (2026-11-28, the no-Bazel loop over slice 3's
code). NOT read yet, to read before building: the `in` rule and the `indexOf` rule of `Scalars`, the folds of `in`
and `contains` in `compiler/spec/LiteralUnroll` (`inFold`, the contains fold), `TaggedLists.contains*`, the
deduplications (`distinct`, `removeDuplicates`), `hasSameElements`, and the test checker's `databaseContains`.*

## The rule

AGENTS.md, "The equality rule": "`==`, `!=`, `in`, `contains`, `indexOf`, the deduplications, joins and ordering all
hold to it." So `[R, R]->contains(L)`, `L->in([R])` and `[R, R]->indexOf(L) >= 0` must each answer as `L == R`
does, for every pair of single values, wherever they stand.

## Measured (the measure's third matrix: 44 comparisons of two single values, 9 places each)

`in`, `in` of a list of two, `contains`, `!contains`, `contains` over a computed list, `indexOf`, `in` inside a
list's function, `assertContains` under each checker.

| | as `==` everywhere | refused by name everywhere | only some places | fails somewhere | wrong somewhere | contradicts |
|---|---|---|---|---|---|---|
| DuckDB | 35 | 0 | 6 | 1 | 0 | 2 |
| H2 | 32 | 1 | 8 | 1 | 1 | 1 |
| SQLite | 30 | 2 | 9 | 0 | 2 | 1 |

On DuckDB, what is not as `==`:

| Comparison | What happens | Kind |
|---|---|---|
| two values declared `Number`, an Integer and a Float | `contains` and `indexOf` answer TRUE; `in` answers false, rightly | wrong answer |
| a day and a date-time under the abstract `Date` | every function answers TRUE | wrong answer |
| a date built to the hour against a written date-time | every function fails when the query runs (a text met a TIMESTAMP) | fails |
| two values declared `Any` (same, or an Integer and a String, or a day and a date-time) | `contains` over a computed list and `indexOf` are refused by name; `in` of one is refused for the dates | partly |
| `assertContains` | under the database checker unjudged for six rows; under the Java checker refused for values declared `Number` or `Any` | partly (slice 6) |
| the mixed-list row | refused by name | part 2 |

## What the code does today (read: `containsRule`)

One rule, `Scalars.containsRule(numbers)`, in arms: the string function (a substring's test) where the callee is
the string overload; a Variant; a comparator given; the TDSNull sentinel; a mixed (tagged) list by the needle's
kind; then the database's own list membership. It asks NONE of the decisions `Equalities.equal` asks: no instant
reading of a date carried as its text, no "two kinds named by the text can never be equal". `in` is another rule.
That is the second code the plan of record names (section 2.2: "the membership ladder").

## Direction (to confirm against the unread code before building)

- Membership is "some element is the same value as the needle": the rule asks `Equalities` for the element's side
  and the needle's side as it asks for `==`'s two sides -- the same reading of kinds (`MixedEncoding.kindThroughUpcasts`,
  `NumberFamilies`), the same instants (`asInstants`), the same "never equal, both evaluated".
- `in(v, list)` is `contains(list, v)`: one rule, the arguments turned.
- `indexOf` is the same comparison, answering a place.
- `assertContains(c, v)` is `c->contains(v)` planned by the product (slice 6 takes the assert's own code out).
- Not here: the deduplications over mixed lists (part 2); ordering, grouping and joins are MEASURED in this slice
  and their contradictions go to the user.

## One finding that is not a defect

`['a ']->contains('a')` is true: a written list of one text IS that text (a one-element collection is its
element), and `contains` of a text and a text is the STRING function, by the functions' signatures (legend-pure's
own resolution too). The matrix asks lists of two.

## Built (2026-11-28, the scratch branch `claude/step-6b-slice-5`, stacked on slice 3; the no-Bazel loop)

Read before building, as this page asked: the `in` and `indexOf` rules of `Scalars`, the folds in the type-checker,
`TaggedLists.contains*`, and upstream's `in` in the pinned sources
(`legend-engine/.../core_functions_standard/collection/in.pure`: `in(value: Any[0..1], collection)` is
`$value->isNotEmpty() && $collection->exists(x | $value == $x)`, so nothing is in any list; a side that may hold no
value is therefore not asked of the functions matrix, whose answers for an empty needle are their own).

| What | Where | The measured case it fixes |
|---|---|---|
| Membership asks `Equalities` for the element's side and the needle's side, as `==` asks for its two: the kinds the text names, the instants of dates, "never equal, both evaluated". `in(v, list)` is `contains(list, v)`; `indexOf` answers the place | `Equalities.member`, `Scalars.containsRule`, `inRule`, the list `indexOf` rule | two `Number` values of two families answered TRUE through `contains` and `indexOf`; a day and a date-time under `Date` TRUE through every function; a date built to the hour against a written date-time failed |
| A list's kind is named through what keeps or reorders its values (`sort`, `sortBy`, `reverse`, `take`, `drop`, `slice`, `tail`, `init`, the deduplications) | `MixedEncoding.kindThroughUpcasts`, `KEEPS_ITS_VALUES` | `[2, 1]->cast(@Number)->sort()` against Floats: the database's own `=`, TRUE inside a list's function |
| Two optional enumerations that both hold nothing are equal, as any two empty sides are | `Equalities.emptySidesAsPure` (the enumeration exception removed; it copied legend-engine's SQL, whose door never reaches this rule inside a store query) | no value in a kept value, false in an `if` |
| (taken out again, 2026-11-28: a `map` over a list built with empty slots reads it without them) | was `ListEncodings.lowerMap` | `[1, 2]->map(x | if($x == 1, |1, |[]))->map(y | 7)` answers `[7, 7]`; the fix adds a list filter to one of core's pinned statements, and a size pin rises only with the user's word (`for-the-user-2026-11-28.md`, C2). The measure keeps the row as a known wrong answer |
| A year or a day in a list declared the abstract `Date`, beside held date-times, reads as no instant (it equals none) | `SideEqualities.instantOfTextOrNone`, `Equalities.asInstants` | the query failed ("invalid timestamp field format: 2020") where the answer is false |
| `hasSameElements` of two list values of one compared kind is the "same values" instruction in ANY order over the two values, by the same decisions `==` reads; a database that compares rows refuses by name the case where a side reads the row or element | `Equalities.asSameElements`, `Lowerer.sameElementsOfTwoValues`, `AnsiSqlRenderer.sameValues` | every `hasSameElements` inside an expression was a statement of its own, which a list's function cannot read on DuckDB |

## Not in this slice, and why

- `assertContains`, `assertSameElements` under each checker (unjudged or refused for some rows): the asserts' own
  code, slice 6.
- Values declared `Any` in a computed list (`contains` over a computed list, `indexOf`): a list of `Any` is a mixed
  list, part 2.
- Found, the type-checker's, outside equality (task 29): a `map` whose function may answer nothing keeps its
  source's count (`[1, 2]->map(x | if($x == 1, |1, |[]))` is typed `[2]`), so the Java checker fails an
  `assertEquals` over such a list. Four rows fail only there.
- To rule (added to `measure-gaps-2026-11-28.md`): a literal under an upcast (`1.0->cast(@Number)`) arrives as the
  literal, so against a computed value it is read by the literal rule; a literal under an upcast to `Any` is
  refused at the call of a function over one type variable where a computed value under the same upcast is
  compared when the query runs.

## The two size rounds (2026-11-28, about 05:10 to 05:25 UTC)

The slice's first Bazel round failed the SQL size pin and the query-capture pin: compacting every mapped list where
it is built added one list filter to PCT's relation-sort family on every database and to two of core's families.
Narrowed to "a map over a list that visibly builds empty slots reads it without them", the PCT families and the
capture pin came back, and one of core's statements still grew by two filters: the fix doing its job on that
statement. Taken out; the choice is the user's. Also from the round: the guard against JVM-salted set copies (two
lookup sets, now ordered); a list's emptiness where it stands drops its slots only where the text shows them
(`CollectionLanes.buildsEmptySlots`, the positive test), never on suspicion.
