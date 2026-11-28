# Upstream's answers, request 3 (2026-11-27): for the step-6 closing batch

Every row RUN by the upstream worker session (measure only; nothing edited). Interpreted legend-pure 5.99.0, compiled
legend-pure 5.99.0, legend-engine 4.145.0's own Java (store-less: a platform node, its generated Java). Store queries
on legend-engine's H2 2.2.224: table `P (ID, NICK VARCHAR, N INTEGER, ALIAS VARCHAR, TS TIMESTAMP)`, rows
`(1, 'a', 1, 'a', 2015-04-16 14:00:00)` and `(2, NULL, NULL, NULL, NULL)`. All three runtimes agree on every
store-less row below, so one column is shown.

## M. A tie of two equal exact numbers (batch row B9)

| Written | Answer (value, type) |
|---|---|
| `max(3, 3D)`, `min(3, 3D)` | 3 Integer |
| `max(3D, 3)`, `min(3D, 3)` | 3 Decimal |
| `[3, 3D]->max()`, `[3, 3D]->min()`, `[3, 3D]->greatest()` | 3 Decimal |
| `[3D, 3]->max()`, `[3D, 3]->min()`, `[3D, 3]->greatest()` | 3 Integer |
| `[3, 3D]->least()` | 3 Integer |
| `[3D, 3]->least()` | 3 Decimal |
| `max(3, 3.00D)` | 3 Integer |
| `max(3.00D, 3)` | 3.00 Decimal |

The pattern is register S428's, for an Integer and a Decimal too: a two-argument `max`/`min` keeps its FIRST argument
on a tie; a list's `max`/`min` (a fold) and `greatest` keep the LAST element; `least` (the first of a stable sort)
keeps the FIRST. A Decimal that wins keeps its written scale.

## E. An empty computed list (batch row B19)

| Written | Answer |
|---|---|
| `[1,2]->filter(x\|$x > 5) == []`, `[] == [1,2]->filter(x\|$x > 5)` | true |
| `[1,2]->filter(x\|$x > 1) == []` | false |
| `[1,2]->filter(x\|$x > 5) == [1,2]->filter(x\|$x > 7)` | true |

## V. One value against a list (batch row B19)

| Written | Answer |
|---|---|
| `1 == [1,2]->filter(x\|$x < 2)`, `[1,2]->filter(x\|$x < 2) == 1`, `1 == [1]` | true |
| `1 == [1,2]->filter(x\|$x > 0)`, `2 == [1,2]->filter(x\|$x < 2)` | false |

A one-element list equals its element.

## D. A date built to the hour or the minute against a date-time (batch row B19)

| Written | Answer |
|---|---|
| `date(2015,4,16,14) == %2015-04-16T14:00:00` | **false** |
| `date(2015,4,16,14) == %2015-04-16T14` | true |
| `date(2015,4,16,14,30) == %2015-04-16T14:30:00.000` | **false** |
| `date(2015,4,16,14) == %2015-04-16T14:00:00.000+0000` | **false** |

In all three runtimes the precision is part of the value: an hour-precision date equals only an hour-precision date.
(Lite's own rule is register S399, a date-time by its instant; what lite's two compatibility doors answer here since
step 6's shared equality code is to be checked against this table.)

Inside a store query legend-engine refuses `date(...)` ("No SQL translation exists for the PURE function
'date_Integer_1__Integer_1__Integer_1__Integer_1__DateTime_1_'"). With literals: `$r.TS == %2015-04-16T14:00:00` is
`"p_0".TS = TIMESTAMP'2015-04-16 14:00:00'` (row 1 true, row 2 no value). `$r.TS == %2015-04-16T14` is written
`"p_0".TS = TIMESTAMP'2015-04-16'`: legend-engine drops the hour and compares against midnight (row 1 false). Not
traced in source.

## O. Both sides may be empty, inside legend-engine's store query (batch row B16)

Row 1 is NICK 'a', ALIAS 'a'; row 2 both NULL.

| Written | SQL | As a column: row 1 / row 2 | As a filter |
|---|---|---|---|
| `$r.NICK == $r.ALIAS` | `"p_0".NICK is not distinct from "p_0".ALIAS` | true / true | rows 1, 2 |
| `$r.NICK == $r.ALIAS->toUpper()` (and the two variants asked) | refused: "Can't find a match for function 'toUpper(Varchar(10)[0..1])'" | | |
| `$r.NICK == $r.ALIAS->toOne()->toUpper()` | `"p_0".NICK = upper("p_0".ALIAS)` | false / no value | no row |
| `!($r.NICK == $r.ALIAS->toOne()->toUpper())` | `"p_0".NICK is distinct from upper("p_0".ALIAS)` | true / false | row 1 |
| `$r.NICK->toOne()->toUpper() == $r.ALIAS->toOne()->toUpper()` | `upper("p_0".NICK) = upper("p_0".ALIAS)` | true / no value | row 1 |

Two plain nullable COLUMNS under `==` get the null-safe form (two empties are equal, and the row passes a filter).
Once a side is computed, `==` is a plain `=` (no value in a column, the row dropped in a filter), and its negation
stays null-safe (`is distinct from`: on row 2, false).

Store-less, all three runtimes: `[1]->filter(x|$x > 5)->first() == [2]->filter(x|$x > 5)->first()` is true (two
empties are equal); `[1]->filter(x|$x > 5)->first() == [2]->first()` is false.
