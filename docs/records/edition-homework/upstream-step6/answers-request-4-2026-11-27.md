# Upstream's answers, request 4 (2026-11-27): a day moved by a whole unit, and the first-day functions

RUN by the upstream worker session (measure only). Interpreted legend-pure 5.99.0, compiled legend-pure 5.99.0,
legend-engine 4.145.0's own Java (a platform node; a `PureDate`'s precision read from its printed text).
`U` is `meta::pure::functions::date::DurationUnit`.

## A. A day moved by a whole unit

| Written | Interpreted | Compiled | legend-engine's Java |
|---|---|---|---|
| `%2020-01-31->adjust(1, U.DAYS)` | 2020-02-01 StrictDate | 2020-02-01 StrictDate | 2020-02-01 (day) |
| `...adjust(1, U.WEEKS)` | 2020-02-07 StrictDate | the same | 2020-02-07 (day) |
| `...adjust(1, U.MONTHS)` | 2020-02-29 StrictDate | the same | 2020-02-29 (day) |
| `...adjust(1, U.YEARS)` | 2021-01-31 StrictDate | the same | 2021-01-31 (day) |
| `%2020-01-31->cast(@Date)->adjust(1, U.DAYS)` | 2020-02-01 StrictDate | the same | 2020-02-01 (day) |
| `%2020-01-31->adjust(1, U.HOURS)` | raises (`UnsupportedOperationException`, `StrictDate.addHours`) | raises | raises ("Cannot add hours to a date that does not have an hour") |
| `%2020-01-31T10:00:00->adjust(1, U.DAYS)` (control) | 2020-02-01T10:00:00+0000 DateTime | the same | 2020-02-01T10:00:00 (second) |

A day moved by days, weeks, months or years stays a day on all three. A day moved by hours is not made a date-time:
all three raise.

## F. The first-day functions

They are legend-engine's own Pure, not legend-pure's (READ:
`legend-engine-core-pure/.../core/pure/corefunctions/dateExtension.pure`): `firstDayOfMonth(date:Date[1]):Date[1]`
(:467) and `firstDayOfYear` (:477) build `date(year, month, 1)` / `date(year, 1, 1)`; `firstDayOfQuarter(date:Date[1]):
StrictDate[1]` (:192); `firstDayOfWeek(date:Date[1]):Date[1]` (:202) is the most recent Monday. The two legend-pure
runtimes of this harness do not load that repository and refuse the call at compile ("can't find a match for the
function").

| Written | legend-engine's Java (RUN) |
|---|---|
| `%2020-01-31->firstDayOfMonth()`, `->firstDayOfYear()`, `->firstDayOfQuarter()` | 2020-01-01 (day) |
| `%2020-01-31->firstDayOfWeek()` | 2020-01-27 (day; the Monday) |
| `%2020-01-31T10:00:00->firstDayOfMonth()` | 2020-01-01 (day: the time is dropped) |

## E. Does a moved day equal a written day?

`%2020-01-31->adjust(1, U.DAYS) == %2020-02-01`: true on all three. `%2020-01-31->firstDayOfMonth() == %2020-01-01`:
true on legend-engine's Java.
