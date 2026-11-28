# Slice 3: the other plain kinds, and the value form first (2026-11-28)

*The half page the plan of record asks for before building. Read: `lowering/Equalities` (`equal`, `asSameValues`,
`emptySidesAsPure`), `lowering/SideEqualities.comparedAsValues`, `planner/RootComparison` (`insideAnExpression`,
`plan`), `Lowerer.scalarInner`'s two arms for `==`, and the measure's report on the scratch branch (the no-Bazel
loop, 2026-11-28 01:45 UTC).*

Mixed values (a list that holds two types) are part 2 of B, by the user's word. This slice is every kind a database
holds in ONE type.

## Where the measure stands (106 comparisons; DuckDB / H2)

| | right everywhere | refused by name everywhere | only some places | fails somewhere | wrong somewhere | contradicts |
|---|---|---|---|---|---|---|
| after slice 1 (landed code) | 75 / 80 | 0 / 10 | 2 / 3 | 18 / 7 | 2 / 1 | 9 / 5 |
| with part 3a below (scratch, not reviewed) | 86 / 85 | 0 / 10 | 2 / 3 | 16 / 7 | 1 / 1 | 1 / 0 |

## Part 3a: written and measured, to land first (its own review and lanes)

1. **The value form for every plain kind** (`SideEqualities.comparedAsValues`): lists of Booleans, Floats, days and
   date-times on both sides, and the written empty list against any of them, are one instruction over the two list
   values wherever the comparison stands, as slice 1 did for exact numbers and Strings.
2. **Two values typed only when the query runs, held in two types** (`Equalities.heldApart`): a side declared
   `Number`, `Any` or the abstract `Date` is compared by the type it holds, read from the SQL type it is carried in.
   An exact number against a Float, a number against a text, a day against a date-time: false, both sides still
   evaluated; the same only where both are empty. Until now the database's own `=` answered (`let a =
   1->cast(@Any); let b = '1'->cast(@Any); $a == $b` was true).
3. **Dates written to the year, month or hour, as lists** (a year passed through a filter; a date built to the hour
   against a held date-time): the same reading as the single value's.
4. **The measure:** the "reads the element" place wraps BOTH sides (with one side left as written, a number literal
   there read as the other side's type, and `1 == 1.0` was true by rule in that place alone); two answers the rule
   itself makes different are no contradiction; the rule for a function over one type variable.

## Part 3b: to build (each a fast loop of its own, in this order)

| # | What is wrong today (DuckDB unless said) | Cause, read in the code | Fix |
|---|---|---|---|
| 1 | Two empty computed lists compared inside a list's function answer no value (`[]->filter(..) == []->filter(..)`); a date built to the hour through a filter on both sides answers false | the value form has no arm for two lists that both may be empty, and none for two lists of date texts | `asSameValues`: the instruction answers for empty lists itself (it is never "no value"); both sides' date texts read as instants |
| 2 | Lists typed `Number` or `Any` that hold one type each (`[1, 2]->cast(@Number) == [1D, 2D]->cast(@Number)`), lists of pairs, maps: fail inside a list's function ("subqueries in lambda expressions are not supported") | `Lowerer.scalarInner` asks the planner for a statement of its own FIRST (`wholeSides`), and reads it as a sub-query; the value form comes second | the order turned around: the value form wherever both sides have a value of one carrier; the planner's statement only for a side that has none (a table). Maps: the instruction in any order over the two maps' entries |
| 3 | Tables compared inside a list's function: a run-time error | a table has no value, so only a sub-query can answer, which DuckDB refuses inside a list's function | refused by name where it is planned (a DuckDB capability), or the comparison bound once outside the function where it reads nothing of the element |
| 4 | `t::sameT(t::E.A, 'A')` (a function over one type variable) answers false where a compile error is ruled | the check at the call (`LiteCrossTypeRule.atTheCall`) does not see an enumeration against a String as two types | the call's check reads the same "never equal" rule as a written comparison |
| 5 | Two columns that can never be equal, inside a function whose parameters are `Any`, in a filter: refused ("TypedRunTimeComparison is not substitutable yet") | the store resolver cannot carry the kept comparison through its substitution | the resolver carries it (false at run time is the rule there) |
| 6 | `pair(1, 1D) == pair(1, 1)`: the assert under the database checker is unjudged | the test checker's own gate | with slice 6 (asserts planned as the comparison they mean), unless it falls out of 2 |

## Not in this slice

Mixed lists (part 2). Tables as sides (slice 4). `contains`, `in`, `indexOf` (slice 5). The asserts' own code (slice
6). H2: it has no list functions at all (ten comparisons refused by name everywhere, four places left out): its
capability, not a rule's gap.

## Found on the way, not equality's (for the user)

- **A `map` over an empty value can answer a value** (all three in-process databases): `$r.i->map(x | 7)` is 7
  where `i` holds nothing, and `$r.s->map(x | $x + $r.s2->toOne())` is `s2`'s text; Pure's `map` over nothing is
  nothing. The store resolver composes the function over the property read with no guard for an empty source
  (`Pipelines.composeScalarReadMap`). It is why `$r.i->map(x | $x + 1) == 2`, kept as a column, answers no value
  for such a row. A resolver fix of its own; SQL size pins may move, so it waits for the user's word.
