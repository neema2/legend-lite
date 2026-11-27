# The position matrix: one comparison asked in every place

## Values: literals and computed values

72 comparisons, 4104 runs.

| database | one answer, the expected one | one answer, NOT the expected one | refused everywhere | ANSWERED ONLY IN SOME PLACES | CONTRADICTS |
|---|---|---|---|---|---|
| DuckDB | 36 | 0 | 0 | 27 | 9 |
| H2 | 51 | 0 | 12 | 4 | 5 |
| SQLite | 47 | 0 | 1 | 12 | 12 |

Places a database cannot answer even for the simplest comparison (left out of the counts):
- DuckDB: none
- H2: kept-in-map (refused DialectCapability), list-exists (refused DialectCapability), list-forAll (refused DialectCapability), reads-element (refused DialectCapability)
- SQLite: none

Not plain from the equality rule, for the user (counted as expected whatever they answer):
- **nested lists**: `list([1, 2])` == `list([1, 2])`
- **pairs int and decimal**: `pair(1, 1D)` == `pair(1, 1)`

### Comparisons that are not one expected answer everywhere

- **decimal two scales** (expected `T`): `1.0D` == `1.00D`
  - **SQLite: ANSWERED ONLY IN SOME PLACES**
    - `T`: 18 places (statement, not, ...)
    - `refused DialectCapability`: reads-element
- **int and float** (expected `N`): `1` == `1.0`
  - **DuckDB: CONTRADICTS**
    - `refused ModelException`: 17 places (statement, not, ...)
    - `T`: reads-element
    - `F`: function-any
  - **SQLite: CONTRADICTS**
    - `refused ModelException`: 17 places (statement, not, ...)
    - `T`: reads-element
    - `F`: function-any
- **date-time two written precisions** (expected `T`): `%2020-01-01T10:00:00.000` == `%2020-01-01T10:00:00`
  - **SQLite: CONTRADICTS**
    - `F`: 17 places (statement, not, ...)
    - `T`: assertEquals (host), assertEquals (database)
- **year same** (expected `T`): `%2020` == `%2020`
  - **DuckDB: CONTRADICTS**
    - `T`: 18 places (statement, not, ...)
    - `F`: reads-element
  - **SQLite: CONTRADICTS**
    - `T`: 18 places (statement, not, ...)
    - `F`: reads-element
- **year-month same** (expected `T`): `%2020-01` == `%2020-01`
  - **DuckDB: CONTRADICTS**
    - `T`: 18 places (statement, not, ...)
    - `F`: reads-element
  - **SQLite: CONTRADICTS**
    - `T`: 18 places (statement, not, ...)
    - `F`: reads-element
- **built to the hour and literal** (expected `T`): `date(2015, 4, 16, 14)` == `%2015-04-16T14:00:00`
  - **DuckDB: CONTRADICTS**
    - `T`: 18 places (statement, not, ...)
    - `F`: reads-element
  - **SQLite: CONTRADICTS**
    - `T`: 18 places (statement, not, ...)
    - `F`: reads-element
- **Number int and float** (expected `F`): `1->cast(@Number)` == `1.0->cast(@Number)`
  - **DuckDB: CONTRADICTS**
    - `F`: 16 places (statement, not, ...)
    - `T`: lets, reads-element
    - `refused ModelException`: function-generic
  - **H2: CONTRADICTS**
    - `F`: 13 places (statement, not, ...)
    - `T`: lets
    - `refused ModelException`: function-generic
  - **SQLite: CONTRADICTS**
    - `F`: 17 places (statement, not, ...)
    - `T`: lets
    - `refused ModelException`: function-generic
- **Any int and string** (expected `F`): `1->cast(@Any)` == `'1'->cast(@Any)`
  - **DuckDB: CONTRADICTS**
    - `F`: 16 places (statement, not, ...)
    - `T`: lets, reads-element
    - `refused ModelException`: function-generic
  - **H2: CONTRADICTS**
    - `F`: 13 places (statement, not, ...)
    - `T`: lets
    - `refused ModelException`: function-generic
  - **SQLite: ANSWERED ONLY IN SOME PLACES**
    - `F`: 18 places (statement, not, ...)
    - `refused ModelException`: function-generic
- **Any day and date-time** (expected `F`): `%2020-01-01->cast(@Any)` == `%2020-01-01T00:00:00->cast(@Any)`
  - **DuckDB: CONTRADICTS**
    - `F`: 16 places (statement, not, ...)
    - `T`: lets, reads-element
    - `refused ModelException`: function-generic
  - **H2: CONTRADICTS**
    - `F`: 13 places (statement, not, ...)
    - `T`: lets
    - `refused ModelException`: function-generic
  - **SQLite: ANSWERED ONLY IN SOME PLACES**
    - `F`: 18 places (statement, not, ...)
    - `refused ModelException`: function-generic
- **Date day and date-time** (expected `F`): `%2020-01-01->cast(@Date)` == `%2020-01-01T00:00:00->cast(@Date)`
  - **DuckDB: CONTRADICTS**
    - `F`: 16 places (statement, not, ...)
    - `T`: lets, reads-element
    - `refused ModelException`: function-generic
  - **H2: CONTRADICTS**
    - `F`: 13 places (statement, not, ...)
    - `T`: lets
    - `refused ModelException`: function-generic
  - **SQLite: ANSWERED ONLY IN SOME PLACES**
    - `F`: 18 places (statement, not, ...)
    - `refused ModelException`: function-generic
- **max of a mixed list** (expected `T`): `[1, 2.0]->max()` == `2.0`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `T`: 18 places (statement, not, ...)
    - `refused DialectCapability`: reads-element
  - **SQLite: ANSWERED ONLY IN SOME PLACES**
    - `T`: 18 places (statement, not, ...)
    - `refused DialectCapability`: reads-element
- **empty and value** (expected `F`): `[]` == `1`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `F`: 18 places (statement, not, ...)
    - `other answer`: reads-element
  - **SQLite: ANSWERED ONLY IN SOME PLACES**
    - `F`: 18 places (statement, not, ...)
    - `other answer`: reads-element
- **computed empty and written empty** (expected `T`): `[1, 2]->filter(x | $x > 5)` == `[]`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `T`: 15 places (statement, not, ...)
    - `error DataError`: kept-in-map, list-filter, list-exists, list-forAll
- **two computed empties** (expected `T`): `[1, 2]->filter(x | $x > 5)` == `[3]->filter(x | $x > 5)`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `T`: 15 places (statement, not, ...)
    - `error DataError`: kept-in-map, list-filter, list-exists, list-forAll
- **computed empty and value** (expected `F`): `[1, 2]->filter(x | $x > 5)` == `1`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `F`: 15 places (statement, not, ...)
    - `error DataError`: kept-in-map, list-filter, list-exists, list-forAll
- **optional one and value** (expected `T`): `[1, 2]->filter(x | $x > 1)->first()` == `2`
  - **SQLite: CONTRADICTS**
    - `T`: 18 places (statement, not, ...)
    - `F`: list-filter
- **computed list same** (expected `T`): `[1, 2]->map(x | $x + 1)` == `[2, 3]`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `T`: 15 places (statement, not, ...)
    - `error DataError`: kept-in-map, list-filter, list-exists, list-forAll
- **computed list other order** (expected `F`): `[1, 2]->map(x | $x + 1)` == `[3, 2]`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `F`: 15 places (statement, not, ...)
    - `error DataError`: kept-in-map, list-filter, list-exists, list-forAll
- **two computed lists** (expected `T`): `[1, 2]->map(x | $x + 1)` == `[2, 3]->filter(x | $x > 0)`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `T`: 15 places (statement, not, ...)
    - `error DataError`: kept-in-map, list-filter, list-exists, list-forAll
- **computed list shorter** (expected `F`): `[1, 2]->filter(x | $x > 1)` == `[1, 2]`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `F`: 15 places (statement, not, ...)
    - `error DataError`: kept-in-map, list-filter, list-exists, list-forAll
- **list int and decimal** (expected `T`): `[1, 2]->filter(x | $x > 0)` == `[1.0D, 2.00D]`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `T`: 15 places (statement, not, ...)
    - `error DataError`: kept-in-map, list-filter, list-exists, list-forAll
  - **SQLite: ANSWERED ONLY IN SOME PLACES**
    - `T`: 18 places (statement, not, ...)
    - `refused DialectCapability`: reads-element
- **list decimal two scales** (expected `T`): `[1.0D, 10.125D]->filter(x | $x > 0D)` == `[1.000D, 10.125D]`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `T`: 15 places (statement, not, ...)
    - `error DataError`: kept-in-map, list-filter, list-exists, list-forAll
  - **SQLite: CONTRADICTS**
    - `T`: 18 places (statement, not, ...)
    - `F`: reads-element
- **written list decimal two scales** (expected `T`): `[1.0D, 10.125D]` == `[1.000D, 10.125D]`
  - **H2: CONTRADICTS**
    - `F`: 13 places (statement, not, ...)
    - `T`: assertEquals (host), assertEquals (database)
  - **SQLite: CONTRADICTS**
    - `F`: 17 places (statement, not, ...)
    - `T`: assertEquals (host), assertEquals (database)
- **list strings computed** (expected `T`): `['a', 'b']->map(s | $s + 'x')` == `['ax', 'bx']`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `T`: 15 places (statement, not, ...)
    - `error DataError`: kept-in-map, list-filter, list-exists, list-forAll
- **list date-times two precisions** (expected `T`): `[%2020-01-01T10:00:00.000]->filter(d | true)` == `[%2020-01-01T10:00:00]`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `T`: 15 places (statement, not, ...)
    - `error DataError`: kept-in-map, list-filter, list-exists, list-forAll
  - **SQLite: ANSWERED ONLY IN SOME PLACES**
    - `refused DialectCapability`: 18 places (statement, not, ...)
    - `F`: reads-element
- **list days computed** (expected `T`): `[%2020-01-01, %2020-01-02]->filter(d | true)` == `[%2020-01-01, %2020-01-02]`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `T`: 15 places (statement, not, ...)
    - `error DataError`: kept-in-map, list-filter, list-exists, list-forAll
- **one value and list of one** (expected `T`): `1` == `[1, 2]->filter(x | $x < 2)`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `T`: 15 places (statement, not, ...)
    - `error DataError`: kept-in-map, list-filter, list-exists, list-forAll
- **mixed list computed** (expected `T`): `[1, 'a']->filter(x | true)` == `[1, 'a']`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `T`: 15 places (statement, not, ...)
    - `error DataError`: kept-in-map, list-filter, list-exists, list-forAll
- **Number lists int and float** (expected `F`): `[1, 2]->cast(@Number)` == `[1.0, 2.0]->cast(@Number)`
  - **DuckDB: CONTRADICTS**
    - `F`: 18 places (statement, not, ...)
    - `T`: reads-element
- **Number lists int and decimal** (expected `T`): `[1, 2]->cast(@Number)` == `[1D, 2D]->cast(@Number)`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `T`: 15 places (statement, not, ...)
    - `error DataError`: kept-in-map, list-filter, list-exists, list-forAll
- **nested lists** (expected `?`): `list([1, 2])` == `list([1, 2])`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `T`: 18 places (statement, not, ...)
    - `error DataError`: reads-element
  - **H2: ANSWERED ONLY IN SOME PLACES**
    - `T`: 13 places (statement, not, ...)
    - `error DataError`: assertEquals (host)
    - `refused DialectCapability`: assertEquals (database)
  - **SQLite: CONTRADICTS**
    - `T`: 17 places (statement, not, ...)
    - `F`: reads-element
    - `refused DialectCapability`: assertEquals (database)
- **maps same order** (expected `T`): `newMap([pair('a', 1), pair('b', 2)])` == `newMap([pair('a', 1), pair('b', 2)])`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `T`: 14 places (statement, not, ...)
    - `error DataError`: kept-in-map, list-filter, list-exists, list-forAll
    - `refused NotImplementedException`: reads-element
  - **SQLite: ANSWERED ONLY IN SOME PLACES**
    - `refused DialectCapability`: 17 places (statement, not, ...)
    - `refused NotImplementedException`: reads-element
    - `T`: assertEquals (host)
- **maps other order** (expected `T`): `newMap([pair('a', 1), pair('b', 2)])` == `newMap([pair('b', 2), pair('a', 1)])`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `T`: 14 places (statement, not, ...)
    - `error DataError`: kept-in-map, list-filter, list-exists, list-forAll
    - `refused NotImplementedException`: reads-element
  - **SQLite: ANSWERED ONLY IN SOME PLACES**
    - `refused DialectCapability`: 17 places (statement, not, ...)
    - `refused NotImplementedException`: reads-element
    - `T`: assertEquals (host)
- **maps a value differs** (expected `F`): `newMap([pair('a', 1), pair('b', 2)])` == `newMap([pair('a', 1), pair('b', 3)])`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `F`: 14 places (statement, not, ...)
    - `error DataError`: kept-in-map, list-filter, list-exists, list-forAll
    - `refused NotImplementedException`: reads-element
  - **SQLite: ANSWERED ONLY IN SOME PLACES**
    - `refused DialectCapability`: 17 places (statement, not, ...)
    - `refused NotImplementedException`: reads-element
    - `F`: assertEquals (host)
- **pairs int and decimal** (expected `?`): `pair(1, 1D)` == `pair(1, 1)`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `T`: 18 places (statement, not, ...)
    - `unjudged a pair with no typed form (Pair<Integer, Decimal(38,0)> against Pair<Integer, Integer>) | assertEquals`: assertEquals (database)
  - **H2: ANSWERED ONLY IN SOME PLACES**
    - `T`: 14 places (statement, not, ...)
    - `unjudged a pair with no typed form (Pair<Integer, Decimal(38,0)> against Pair<Integer, Integer>) | assertEquals`: assertEquals (database)
  - **SQLite: ANSWERED ONLY IN SOME PLACES**
    - `T`: 18 places (statement, not, ...)
    - `unjudged a pair with no typed form (Pair<Integer, Decimal(38,0)> against Pair<Integer, Integer>) | assertEquals`: assertEquals (database)
- **list of pairs computed** (expected `T`): `[pair(1, 'a'), pair(2, 'b')]->filter(p | true)` == `[pair(1, 'a'), pair(2, 'b')]`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `T`: 15 places (statement, not, ...)
    - `error DataError`: kept-in-map, list-filter, list-exists, list-forAll
- **instances written alike** (expected `F`): `^t::K(id=1, name='a')` == `^t::K(id=1, name='a')`
  - **H2: ANSWERED ONLY IN SOME PLACES**
    - `F`: 11 places (statement, not, ...)
    - `refused DialectCapability`: assertEquals (host), assertEquals (database), assert (host), assert (database)
- **tables same** (expected `T`): `#TDS  id, s  1, x  2, y #` == `#TDS  id, s  1, x  2, y #`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `T`: 14 places (statement, not, ...)
    - `error DataError`: kept-in-map, list-filter, list-exists, list-forAll, reads-element
- **tables rows other order** (expected `F`): `#TDS  id, s  1, x  2, y #` == `#TDS  id, s  2, y  1, x #`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `F`: 14 places (statement, not, ...)
    - `error DataError`: kept-in-map, list-filter, list-exists, list-forAll, reads-element
  - **SQLite: CONTRADICTS**
    - `F`: 18 places (statement, not, ...)
    - `T`: reads-element
- **tables a cell differs** (expected `F`): `#TDS  id, s  1, x  2, y #` == `#TDS  id, s  1, x  2, z #`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `F`: 14 places (statement, not, ...)
    - `error DataError`: kept-in-map, list-filter, list-exists, list-forAll, reads-element
- **tables a column's name differs** (expected `F`): `#TDS  id  1 #` == `#TDS  ix  1 #`
  - **DuckDB: ANSWERED ONLY IN SOME PLACES**
    - `F`: 13 places (statement, not, ...)
    - `error DataError`: kept-in-map, list-filter, list-exists, list-forAll, reads-element
    - `refused TypeInferenceException`: function-generic
  - **H2: ANSWERED ONLY IN SOME PLACES**
    - `F`: 14 places (statement, not, ...)
    - `refused TypeInferenceException`: function-generic
  - **SQLite: CONTRADICTS**
    - `F`: 17 places (statement, not, ...)
    - `T`: reads-element
    - `refused TypeInferenceException`: function-generic

## A store's rows: a comparison that reads the row it stands in

13 comparisons, 312 runs.

| database | one answer, the expected one | one answer, NOT the expected one | refused everywhere | ANSWERED ONLY IN SOME PLACES | CONTRADICTS |
|---|---|---|---|---|---|
| DuckDB | 13 | 0 | 0 | 0 | 0 |
| H2 | 13 | 0 | 0 | 0 | 0 |
| SQLite | 13 | 0 | 0 | 0 | 0 |

Places a database cannot answer even for the simplest comparison (left out of the counts):
- DuckDB: none
- H2: none
- SQLite: none

### Comparisons that are not one expected answer everywhere

none

