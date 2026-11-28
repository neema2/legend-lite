# What the equality measure does not yet ask (2026-11-28, about 03:45 UTC)

*An Opus researcher, read-only, asked for every arm of the comparison code and every clause of the equality rule
that no row of the measure reaches, each with a concrete row. It read the rule, the measure and the lowering; it
ran nothing. Three reviews of slice 3 each found a wrong answer in a case the matrix did not ask; this list is to
stop finding them by review. Rows are to be added and RUN before any is believed; "(unverified)" marks Pure text
it could not confirm parses.*

## The three biggest blind spots

1. The store model has no enumeration column and no property declared `Number` or `Date`: several arms fire only
   there, and two look wrong by reading.
2. No row uses the literal rule against a COMPUTED value: every literal row compares two literals (a compile error),
   so `NumberLiterals.read` and `HeldKinds.readsAs` are never exercised.
3. No row has a run-time-typed side whose kind the code cannot name (`sort`, `if`, `max` under `Number`):
   `heldApart` then leaves the database's own `=`.

## Model additions the rows need

Storeless model: `Enum t::F { A, B }`; `Class t::KK { <<equality.Key>> id: Integer[1]; name: String[0..1]; }`;
`Class t::KKSub extends t::KK { }`.

Store model: `Enum t::E { A, B }`; `function t::sameT<T>(a: T[*], b: T[*]): Boolean[1] { $a == $b }`; on `t::R`:
`e: t::E[0..1]; e2: t::E[0..1]; n: Number[0..1]; nf: Number[0..1]; dt: Date[0..1]; dtt: Date[0..1];`; table columns
`E VARCHAR(1), E2 VARCHAR(1)`; mapping `e: EnumerationMapping EM: R.E, e2: EnumerationMapping EM: R.E2, n: R.D, nf:
R.F, dt: R.DAY, dtt: R.AT` and `t::E: EnumerationMapping EM { A: 'A', B: 'B' }`; CSV columns `E,E2` with rows
`A,A` / `A,B` / `,A` / `A,` / `,` (the s/s2 pattern). Whether a `Number` property maps to a DECIMAL column and a
`Date` property to DATE is unverified.

## Proposed rows, most valuable first

V = value matrix, single values (above the `singleValued` mark, so the functions matrix asks them too); VL = value
matrix, lists and the rest; S = store matrix. `**` = likely wrong today, by reading.

| # | Name | M | L | R | Expected | Arm no row reaches |
|---|---|---|---|---|---|---|
| 1 ** | two enum columns | S | `$r.e` | `$r.e2` | `1,5` | `emptySidesAsPure`: an enumeration keeps the plain `=`, so two empties are NULL |
| 2 ** | enum and string columns through Any | S | `$r.e->cast(@Any)` | `$r.s->cast(@Any)` | `3,5` | `heldApart`, an enum kind against a String kind |
| 3 ** | Number column and fraction literal | S | `$r.n` | `1.5` | `2@` | `HeldKinds.readsAs` reads only a whole Float as exact; `NumberLiterals` reads 1.5 as a Decimal: likely refused |
| 4 ** | Number list through sort, and Floats | VL | `[2, 1]->cast(@Number)->sort()` | `[1.0, 2.0]->cast(@Number)` | `F` | `kindThroughUpcasts` names no kind through `sort` |
| 5 ** | Number value from a sorted list, and a Float | V | `[2, 1]->cast(@Number)->sort()->at(0)` | `1.0->cast(@Number)` | `F` | the same, one value |
| 6 | Number list whose computation raises | VL | `[1, 2]->map(x \| [1]->at($x))->cast(@Number)` | `[1.0, 2.0]->cast(@Number)` | `E` | never equal with a side that raises |
| 7 | raising single value, two families | V | `[1, 2]->at(5)->cast(@Number)` | `1.0->cast(@Number)` | `E` | `neverEqualButEvaluated` |
| 8 | two optional enums both empty | V | `[t::E.A]->filter(e \| $e == t::E.B)->first()` | `[t::E.B]->filter(e \| $e == t::E.A)->first()` | `T` | as row 1, values |
| 9 ** | optional one and a computed list of one | VL | `[1, 2]->filter(x \| $x > 1)->first()` | `[1, 2]->filter(x \| $x > 1)` | `T` | a `[0..1]` value against an array reaches the null arms as a scalar |
| 10 ** | optional none and a computed empty list | VL | `[1, 2]->filter(x \| $x > 5)->first()` | `[1, 2]->filter(x \| $x > 5)` | `T` | the same, and `holdsNothing` |
| 11 | two optionals of two kinds via Any, both empty | V | `[1]->filter(x \| $x > 5)->first()->cast(@Any)` | `['a']->filter(s \| $s == 'b')->first()->cast(@Any)` | `T;generic=R` | `heldApart`, both may be empty |
| 12 | two optionals of two families, both empty | V | `[1]->filter(x \| $x > 5)->first()` | `[1.5]->filter(x \| $x > 5.0)->first()` | `N@T` | the call's rule with a side that may be empty |
| 13 ** | year and string through Any | V | `%2020->cast(@Any)` | `'2020'->cast(@Any)` | `F;generic=R` | the partial-date arm lets `Any` fall through: the text carrier may leak |
| 14 ** | Date lists: years and date-times | VL | `[%2020, %2021]->cast(@Date)` | `[%2020-01-01T00:00:00, %2021-01-01T00:00:00]->cast(@Date)` | `F` | `asInstants`, one list of texts: a year is cast to an instant and raises |
| 15 | keyed instances, a non-key value differs | V | `^t::KK(id=1, name='a')` | `^t::KK(id=1, name='b')` | `T` | INSTANCES with keys (no keyed class in the model) |
| 16 | keyed instances, keys differ | V | `^t::KK(id=1)` | `^t::KK(id=2)` | `F` | the same |
| 17 | keyed subclass and its class | V | `^t::KKSub(id=1)` | `^t::KK(id=1)` | `F` | the run-time class is part of the value |
| 18 | unrelated classes | V | `^t::K(id=1)` | `^t::KK(id=1)` | `N` | `EqualityClasses.differing`, classes |
| 19 | two enumerations | V | `t::E.A` | `t::F.A` | `N` | two enumeration types |
| 20 | two enumerations through Any | V | `t::E.A->cast(@Any)` | `t::F.A->cast(@Any)` | `F;generic=R` | `heldApart`, two enum kinds, both stored 'A' |
| 21 | computed Float and whole literal | V | `1.5 + 0.5` | `2` | `T@F;generic=R` | `NumberLiterals.read`: no row has a literal against a non-literal |
| 22 | computed Decimal and Float literal | V | `0.1D + 0.2D` | `0.3` | `T@F;generic=R` | a Float literal read as a Decimal by its digits |
| 23 | computed Integer and fraction literal | V | `1 + 1` | `2.5` | `N` | a literal not exact in the other type |
| 24 | computed Float and Decimal literal | V | `1.5 + 0.5` | `2D` | `N` | a Decimal literal states its type |
| 25 | Integer literal past 2^53 against a Float | V | `0.5 + 0.5` | `9007199254740993` | `N` | exactness in a Float |
| 26 | signed literal | V | `1 - 2` | `-1.0` | `T@F;generic=R` | the minus arm |
| 27 | Decimal arithmetic with a Float literal | V | `1.0D * 0.1` | `0.1D` | `T` | `NumberLiterals.inExactArithmetic` |
| 28 | table read with distinct (order undefined) | VL | `<ab>->distinct()` | `<ab>` | `N` | `refuseUnordered`, tables |
| 29 | the same, sorted | VL | `<ba>->distinct()->sort(~id->ascending())` | `<ab>` | `T` | a computed table in order |
| 30 | tables, the same columns in another order | VL | `#TDS\n s, id\n x, 1\n y, 2\n#` | `<ab>` | `F` | `columnsDiffer`, order not name |
| 31 | table against a list | VL | `#TDS\n id\n 1\n#` | `[1]` | `N` | a side that is no table |
| 32 | two empty tables | VL | `#TDS\n id, s\n 1, x\n#->filter(r \| $r.id > 5)` | `#TDS\n id, s\n 2, y\n#->filter(r \| $r.id > 6)` | `T` | no rows |
| 33 ** | removeDuplicates keeps its order | VL | `[2, 1, 2]->removeDuplicates()` | `[2, 1]` | `T` | the order rule may refuse it wrongly |
| 34 | Float lists computed | VL | `[1.5, 2.5]->filter(x \| $x > 0.0)` | `[1.5, 2.5]` | `T` | no Float list row |
| 35 | enum lists computed | VL | `[t::E.A, t::E.B]->filter(e \| true)` | `[t::E.A, t::E.B]` | `T` | enum list pair |
| 36 | String lists, a trailing space | VL | `['a ', 'b']->filter(s \| true)` | `['a', 'b']` | `F` | rows ride: a database's collation may pad |
| 37 | written lists, Integer and Decimal | VL | `[1, 2]` | `[1D, 2.00D]` | `T` | two written exact lists ride their rows |
| 38 | map values, Integer and Decimal | VL | `newMap([pair('a', 1)])` | `newMap([pair('a', 1D)])` | `T` | the map refusal for two scales |
| 39 | pairs, Integer and Float | V | `pair(1, 'a')` | `pair(1.0, 'a')` | `N` | types inside a pair |
| 40 | lists inside lists, Integer and Decimal | V | `list([1, 2])` | `list([1D, 2D])` | `T` | exact family inside a list |
| 41 | built to the hour, sides swapped | V | `%2015-04-16T14:00:00` | `date(2015, 4, 16, 14)` | `T` | the other argument order |
| 42 | built to the hour against a held list | VL | `date(2015, 4, 16, 14)` | `[%2015-04-16T14:00:00]->filter(d \| true)` | `T` | one text against a list of held |
| 43 | a year through a filter | VL | `[%2020]->filter(d \| true)` | `%2020` | `T` | the written-year list arm |
| 44 | year and day | V | `%2020` | `%2020-01-01` | `F` | a partial date against a full one |
| 45 | date-time with a time zone | V | `%2020-01-01T10:00:00+0100` | `%2020-01-01T09:00:00` | `T` | no zoned literal anywhere |
| 46 | date-time fraction at two precisions | V | `%2020-01-01T10:00:00.5` | `%2020-01-01T10:00:00.500` | `T` | two digit counts |
| 47 | a comparison with an empty side, as an operand | V | `[1, 2]->filter(x \| $x > 5)->first() == 1` | `false` | `T` | the empty-side answer under an outer `==` |
| 48 | true and an empty optional Boolean | V | `true` | `[true]->filter(b \| !$b)->first()` | `F` | one empty side, Boolean |
| 49 | empty string and the empty list | V | `''` | `[]` | `F` | the written-`[]` ladder over a text |
| 50 | 38-digit Decimals differ | V | `99999999999999999999999999999999999999D` | `99999999999999999999999999999999999998D` | `F` | a Decimal's full precision |
| 51 | a toOne over many | V | `[1, 2]->toOne()` | `1` | `E` | E through the plain path |
| 52 | an empty list forced to one, and the empty list | V | `[]->toOne()` | `[]` | `E` | the ladder with a raising side |
| 53 | Float divided by zero | V | `1.0 / 0.0` | `1.0` | `E` (check legend-pure) | a database answers inf or NULL |
| 54 | a column against itself | S | `$r.i` | `$r.i` | `1,2,3,4,5` | two plain optional columns |
| 55 | conditional with an empty branch, and a column | S | `if($r.id > 2, \|[], \|$r.i)` | `$r.i2` | `1,4,5` | both may be empty, not plain columns |
| 56 | Number (Decimal) and Integer columns | S | `$r.n` | `$r.i` | `1,5` | `HeldKinds`, exact family from columns |
| 57 | Number Decimal and Float columns | S | `$r.n` | `$r.nf` | `N@5` | `HeldKinds.refuseAcrossKinds`, numbers |
| 58 | Number Float column and whole literal | S | `$r.nf` | `1` | `1,4@` | `HeldKinds.readsAs` |
| 59 | Date columns, day and date-time | S | `$r.dt` | `$r.dtt` | `N@3,5` | `HeldKinds`, dates over columns |
| 60 | Date day column and date-time literal | S | `$r.dt` | `%2020-01-01T00:00:00` | `N` | a column against a written date |

(`<ab>` = `#TDS\n id, s\n 1, x\n 2, y\n#`; `<ba>` the same rows in the other order.)

## Two small matrices `==` rows cannot carry

**hasSameElements** (places: statement, not, if, lets, kept-in-map, and, assert): `[1, 2, 3]->filter(x | true)` /
`[3, 1, 2]` T; `[1, 1, 2]` / `[1, 2, 2]` F; `[1, 2]->filter(x | true)` / `[2.0D, 1D]` T; `[1, 2]->cast(@Number)` /
`[2.0, 1.0]->cast(@Number)` F; `<ab>` / `<ba>` T; `<ab>->distinct()` / `<ba>` T (an unordered side is admitted
here); `[1, 2]` / `[1.0, 2.0]` N; a table against a list: refused.

**Ordering** (`<`, `<=`; places: statement, not, if, a store's filter; model: `t::lessN(a: Number[1], b:
Number[1])` and `t::lessEqN`): `t::lessN(1, 1.0)` T, `t::lessN(1.0, 1)` F, `t::lessEqN(1.0, 1)` F,
`t::lessEqN(1, 1.0)` T (a tie of value by family, an exact number before a Float); `t::lessN(1, 1D)` F,
`t::lessEqN(1, 1D)` T; `1 < 1.0` N; `2 < 2.5D` T; a day `<` a date-time N; store `$r.i < $r.f` N; store `$r.dt <
$r.dtt` N.

## An existing row whose expected answer looks wrong

"Number lists int and float" is expected `F`; its single-valued twin is `F;generic=R`. Under `t::sameT` the call
holds an Integer list against a Float list, which the rule makes that call's compile error. Either the expectation
is `F;generic=R`, or the call's refusal misses lists. To run and decide.

## Places a comparison can stand that no position includes

Value places: `{|!((L) != (R))}`; `{|((L) == (R)) == true}`; the comparison as an `if`'s VALUE (`{|if([1]->filter(y
| $y > 0)->isNotEmpty(), |(L) == (R), |false)}`); a let read twice (`{|let c = (L) == (R); if($c, |$c, |false);}`);
as text (`{|((L) == (R))->toString() == 'true'}`); `find` (`{|[1]->find(x | (L) == (R))->isNotEmpty()}`); `fold`
(unverified); `match` over `Any` (unverified; a run-time place); a derived property with `Any` parameters
(unverified; a run-time place); `assertNotEquals(L, R)`, `assert(!((L) == (R)))`, `assertFalse((L) == (R))`.

Store places: `filter` with `||`; `!(... || ...)`; `!(... && ...)`; `!(L != R)`; under `forAll` (not a positive
condition) and under `exists` (positive); inside a function over a type variable (needs `sameT` in the store model
and the report to treat it as the generic place); a kept value as an operand (`== true`); as an `if`'s value; a
kept value read back by a later filter (unverified); a window's column; an aggregate's input (unverified); a
join's condition (unverified); `in` and `!in` (their expected answers differ from `==` where the needle is empty).

Needing harness or model work: a query parameter (`{a: Integer[0..1] | $a == R}`); a constraint; a model-to-model
mapping's filter; a function's or derived property's body where the rule is a compile error of the body.

## To rule, not to guess

A literal against a value typed only at run time (`1->cast(@Number) == 1.0`, `1.5D->cast(@Number) == 1.5`): the
rule does not say whether the literal adapts; the code adapts a whole number and answers false for 1.5 against a
held Decimal, while the column case (row 3) refuses. And a literal read through a `let` (rows 21 to 26 in the
"lets" place): the literal is a variable there, which the literal rule does not adapt.
