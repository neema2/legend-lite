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
