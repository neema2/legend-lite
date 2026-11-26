# legend-engine's own SQL on the other databases, read and run by the lanes (2026-11-26)

*The user, 2026-11-26: "Run all the round/substring stuff on all the warm lanes on parallel so we get answers for
everything". Each lane read legend-engine 4.145.0's templates for its database and ran them on the real database, and
set lite's row beside each. A row of legend-engine's door's table answers on a database only where this file shows
lite's form gives legend-engine's answers there. Each section is the lane's report, set out.*

## Snowflake

Paths: SF = `snowflakeExtension.pure`; DEF = `extensionDefaults.pure`; P2S = `pureToSQLQuery.pure`. Snowflake's
templates are DEF's with SF's own on top (SF:41-42). It does not go through the Postgres-model translation (only H2
does: `relationalMappingExecution.pure:440-443`).

| Function | legend-engine writes | Answers on the battery | lite's row | Verdict |
|---|---|---|---|---|
| `substring` (2, 3 arguments) | the arguments as given, `substring(args)` (P2S:4660-4665, SF:379) | `'abcdef'`: start 0 or 1 gives `abcdef`, with end 0, 3, 9: `''`, `abc`, `abcdef`; start 2: `bcdef`, `''`, `bcd`, `bcdef` (Snowflake reads a start of 0 as 1) | `substr(args)` | the same answers on all 12 |
| the corrected `substring`, and `substr` | `substring(s, start+1, end-start)` (P2S:4654-4657, 4675-4692) | start 0: `abcdef`, `''`, `abc`, `abcdef`; start 1: `bcdef`, `''`, `bc`, `bcdef`; start 2: `cdef`, `''`, `c`, `cdef` | `substr(s, start+1, end-start)` | the same answers |
| the String `indexOf` | `CHARINDEX(p1, p0)` (P2S:10644, SF:331) | `'b'` in `'abc'` is 2; `'z'` is 0 | `CHARINDEX(b, a)` | the same text |
| `round` (1 argument) | `round(x, 0)` (SF:375, DEF:714-719) | a half away from zero on DOUBLE and NUMBER: 0.5 is 1, 2.5 is 3, -2.5 is -3, 16.5 is 17 | `CAST(ROUND(x) AS BIGINT)` | the same values (legend-engine's result is a FLOAT, lite's an integer) |
| `round` (2 arguments) | `round(x, n)` | `round(0.125, 2)` is 0.13 | `ROUND(x, n)` | the same answer |

Without a row today, legend-engine's template and answer alone:
- `toDecimal`: `to_decimal(3.8, 38, 1)` for a literal (3.8); `to_decimal(x, 38, 19)` for anything else (a double 3.8
  shows `3.7999999999999998224`); `to_decimal(8)` for a whole literal (SF:786-798). **Not the bare DECIMAL of DuckDB
  and H2.**
- one-argument `parseDecimal`: `cast(%s as decimal)` (DEF:721-727, SF:348): `'123.450021'` is `123`, the fraction lost.
- `parseInteger`: `cast(%s as integer)` (SF:350); `parseFloat`: `cast(%s as float)` (SF:349); `'abc'` raises
  "Numeric value 'abc' is not recognized"; a leading blank is read.
- `isNumeric`: **no template at all** for Snowflake.
- `dateDiff`: `datediff(unit, d1, d2)` (P2S:10609, SF:306, 535), boundaries crossed; weeks start on Monday
  (Saturday to Sunday 0, Sunday to Saturday 1); 10:59:59 to 11:00:01 is 1 hour, 1 minute, 2 seconds.
- `splitPart`: the index plus one, then `split_part` (P2S:3197-3206, SF:383): `('a,b,,c', ',', 2)` is `''`;
  `('abc', '', 0)` is `'abc'`; `('abc', ',', 1)` is `''`.
- `rem`: `mod(%s,%s)` (DEF:267); `mod`: `mod(mod(a,b)+b, b)` (DEF:254); a zero divisor raises "Division by zero".
- `toString`: `cast(%s as varchar)` for every type (SF:388): 1.3421e-8 is `'1.3421e-08'`, 1.0 is `'1'`, a timestamp
  with microseconds is `'2015-07-04 10:00:00.123'` (the session's format drops them), true is `'true'`.
- `divide`: `((1.0 * %s) / %s)` (DEF:218): 1/0 raises; `sqrt(-1.0)` raises.

**So on Snowflake:** the `substring`, `substr`, `indexOf` and `round` rows give legend-engine's answers.

## Postgres (16.15)

Paths: PGX = `postgresExtension.pure`; DEF and P2S as above. No dialect translation (H2 only).

| Function | legend-engine writes | Answers on the battery | lite's row | Verdict |
|---|---|---|---|---|
| `substring` (2, 3 arguments) | `substring(s, a[, b])`, as given (PGX:248) | `'abcdef'` (start, length): (0,0) `''`, (0,3) `ab`, (0,9) `abcdef`, (1,3) `abc`, (2,3) `bcd`, (2,9) `bcdef`; two arguments: 0 and 1 `abcdef`, 2 `bcdef`, 9 `''`, -2 `abcdef`; a negative length raises | `substr(s, a[, b])` | the same answers |
| the corrected `substring`, and `substr` | `substring(s, start+1, end-start)` | (0,3) `abc`, (0,9) `abcdef`, (1,3) `bc`, (1,9) `bcdef`, (2,3) `c`, (2,9) `cdef`; an end below the start raises | `substr(s, start+1, end-start)` | the same answers |
| the String `indexOf` | `strpos(%s, %s)` (PGX:220) | `'b'` 2, `'z'` 0, `''` 1 | `strpos(s, t)` | the same text |
| `round` | `round((%s)::numeric, %s)` (PGX:243), `0` added for one argument | a half away from zero on a double and on a decimal (0.5 is 1, 2.5 is 3, 16.5 is 17); `round(0.125, 2)` is 0.13 for both | bare `round(x)`, `round(x, n)` | **differs on a Float:** Postgres's `round(float8)` sends a half to even (0, 2, 2, 16), and `round(double precision, integer)` does not exist (an error). The same on a Decimal. To match, the rule needs legend-engine's `::numeric` |

Without a row, legend-engine's template and answer:
- `toDecimal`: `cast(%s as decimal)` (PGX:252), an unconstrained numeric: 3.8 stays `3.8`.
- one-argument `parseDecimal`: `cast(%s as decimal)`: `'123.450021'` stays `123.450021`.
- `parseInteger`: `cast(%s as integer)` (PGX:238), 32 bits; `parseFloat`: `cast(%s as float)` (PGX:237), a double.
  `' 12'` is 12; `'abc'` raises "invalid input syntax".
- `isNumeric`: **no template** (H2's `h2Extension2_1_214.pure:232` only).
- `dateDiff`: `generateDateDiffExpressionForPostgres` (PGX:292-328), fields subtracted for years and months, days
  divided by 7 for weeks, the elapsed seconds truncated for hours, minutes, seconds. **Broken on some types:** weeks
  on timestamps raises (`trunc(interval)` does not exist), days on timestamps returns an interval, the sub-day units
  on dates raise. 2015-12-31 to 2016-01-01 is 1 year, 1 month, 1 day.
- `splitPart`: `split_part` with the index plus one: `('a,b,,c', ',', 2)` is `''`; `('abc', '', 0)` is `'abc'`.
- `rem` `mod(a,b)`; `mod` `mod(mod(a,b)+b,b)`; a zero divisor raises. `divide` `((1.0 * a) / b)` raises on zero.
  `sqrt(-1.0)` raises.
- `toString`: `cast(%s as varchar)`: 1.3421e-8 is `'1.3421e-08'`, 1.0 is `'1'`, a timestamp keeps its microseconds.

**So on Postgres:** the `substring`, `substr` and `indexOf` rows give legend-engine's answers; `round` needs its own
rule (a cast to numeric first).

## Trino (483)

Path: `trinoExtension.pure` (TX) over the defaults; not Presto's. A function with no template raises
"[unsupported-api] ... is not supported yet".

| Function | legend-engine writes | Answers on the battery | lite's row | Verdict |
|---|---|---|---|---|
| `substring` (2, 3 arguments) | `substring(s, a[, b])` (TX:119) | a start of 0 is `''` (Postgres clips, Trino does not): (0,3) `''`, (1,3) `abc`, (2,9) `bcdef`; -2 is `ef` (from the end); a negative length is `''` | `substr(s, a[, b])` | the same answers on every case |
| the corrected `substring`, and `substr` | `substring(s, start+1, end-start)` | (0,3) `abc`, (1,3) `bc`, (2,9) `cdef` | `substr(s, start+1, end-start)` | the same answers |
| the String `indexOf` | `strpos(s, t)` (TX:88) | 2, 0, 1 | `strpos(s, t)` | the same text |
| `round` | `round(x, 0)`, `round(x, n)` (TX:115) | a half away from zero on a double and a decimal | `CAST(round(x) AS BIGINT)`, `round(x, n)` | the same values (the one-argument result is legend-engine's double or decimal, lite's integer) |

Without a row: `toDecimal`, one-argument `parseDecimal` and `isNumeric` have **no template** on Trino.
`parseInteger` `cast(%s as integer)` (32 bits), `parseFloat` `cast(%s as double)`. `dateDiff` is
`date_diff(unit, a, b)`, elapsed whole units (2015-12-31 to 2016-01-01 is 0 years, 0 months, 1 day; Saturday to
Sunday 0 weeks; 10:59:59 to 11:00:01 is 0 hours). `splitPart`: `('abc', '', 0)` is `'a'`, `('abc', ',', 1)` is NULL.
`rem(5, 0)` raises on integers and is NaN on doubles. `toString`: 1.3421e-8 is `'1.3421E-8'`, 1.0 is `'1.0E0'`. A
double divided by zero is Infinity; `sqrt(-1.0)` is NaN.

**So on Trino:** the `substring`, `substr`, `indexOf` and `round` rows give legend-engine's answers.

## ClickHouse (26.3)

Path: `clickHouseExtension.pure` (CH) over the defaults; no dialect translation. A Float or Decimal literal is
written as plain text, so a Pure Decimal literal reaches ClickHouse as a Float64; a DateTime literal is
`'...'::DateTime64`, precision 3 (its microseconds are lost in the literal).

| Function | legend-engine writes | Answers on the battery | lite's row | Verdict |
|---|---|---|---|---|
| `substring` (2, 3 arguments) | `substring(args)` (CH:138): a start of 0 is `''`, the third argument a length | (0,3) `''`, (1,3) `abc`, (2,9) `bcdef` | `substringUTF8(args)` | the same on plain text; **differs on multi-byte text** (`substring` counts bytes, `substringUTF8` characters) |
| the corrected `substring`, and `substr` | `substring(s, start+1, end-start)`, literals folded; a negative length cuts from the right | (0,3) `abc`, (1,0) `bcde`, (2,0) `cd` | `substringUTF8(s, start + 1, end - start)` | the same on plain text; bytes against characters as above |
| the String `indexOf` | `position(%s, %s)` (CH:108) | 2, 0 | `toInt64(positionUTF8(s, t))` | the same on plain text; bytes against characters |
| `round` | `round(x, 0)`, `round(x, n)` (CH:131) | a double sends a half to EVEN (0.5 is 0, 2.5 is 2); a Decimal away from zero (2.5 is 3). A Decimal LITERAL is a Float64 here, so legend-engine's `round(2.5D)` is 2 on a literal and 3 on a column | `round(x)` cast to Int64, `round(x, n)` | the same on a column; **a Decimal literal differs** (lite writes it as a Decimal) |

Without a row: `toDecimal` `toDecimal128(%s, 10)` (CH:144); one-argument `parseDecimal` `cast(%s as Decimal)`
(`'123.450021'` is `123`, the scale lost); `parseInteger` `cast(%s as Int32)`, `parseFloat` `cast(%s as Float32)`
(a leading blank raises); `isNumeric` **no template**; `dateDiff` `datediff(UNIT, a, b)`, boundaries crossed, weeks
from Monday; `splitPart` `splitByString(sep, s)[i+1]` (an empty separator splits into characters); `divide` by zero
is `inf`, `sqrt(-1.0)` is `nan`; `toString` `cast(%s as String)`.

**So on ClickHouse:** no row answers yet: each differs from legend-engine's on multi-byte text or on a Decimal
literal, and would need its own rule (the byte functions; a Decimal literal as text).

## MariaDB and MySQL

**Nothing to match.** legend-engine 4.145.0 has no MySQL or MariaDB database: its `DatabaseType` enum
(`legend-engine-xt-relationalStore-protocol/.../connection/DatabaseType.java:17`) and its extension directory hold
neither; the family's only member is MemSQL (SingleStore), another database. A request at legend-engine's door on
either stays refused by name.

## Databricks

Path: `databricksExtension.pure` (DBX) over the defaults; no dialect translation. legend-engine opens its session
with `ansi_mode=true` (`DatabricksManager.java:61`), and lite sends `SET ANSI_MODE = true` and UTC; every answer
below is from a session with lite's two settings. With ANSI off (the warehouse's default) the seven raising cases
answer NULL instead. A Float or Decimal literal is written as Pure's text, so `0.5` is a DECIMAL literal there.

| Function | legend-engine writes | Answers on the battery | lite's row | Verdict |
|---|---|---|---|---|
| `substring` (2, 3 arguments) | `substring(%s, ...)` as given (DBX:161) | a start of 0 reads as 1; the third argument a length: (0,3) `abc`, (1,3) `abc`, (2,9) `bcdef` | `substr(s, a[, b])` | the same answers (Spark's aliases) |
| the corrected `substring`, and `substr` | `substring(s, start+1, end-start)`, literals folded | (0,3) `abc`, (1,3) `bc`, (2,9) `cdef`; a negative length is `''` | `substr(s, a + 1, b - a)` | the same answers |
| the String `indexOf` | `locate(t, s)` (DBX:127) | 2, 0 | `instr(s, t)` | the same answers |
| `round` | `round(x, 0)`, `round(x, n)` (DBX:155) | a half away from zero on a double and a decimal | `CAST(ROUND(x) AS BIGINT)`, `ROUND(x, n)` | the same values (the one-argument type differs) |

Without a row: `toDecimal` and one-argument `parseDecimal` are `cast(%s as decimal)` (DBX:165, 147), which is
`decimal(10,0)` there: 3.8 is `4`, `'123.450021'` is `123`. `parseInteger` `cast(%s as bigint)` (DBX:149),
`parseFloat` `cast(%s as double)` (DBX:148); `'abc'` raises `[CAST_INVALID_INPUT]`. `isNumeric` is
`(lower(%s) = upper(%s))` (DBX:128). `dateDiff` is `dateDiffDatabricks` (DBX:529-558): **weeks are refused when the
query is planned** ("The DurationUnit 'WEEKS' is not supported yet for date_diff"); days `datediff(b, a)`; hours and
minutes the elapsed seconds divided and ROUNDED (10:59:59 to 11:00:01 is 0 hours, 0 minutes, 2 seconds). `splitPart`
`split_part` with the index plus one (`('abc', '', 0)` is `'abc'`). `rem` and `mod` the same text as lite's; a zero
divisor raises. `divide` `((1.0 * a) / b)` is a DECIMAL with six places over integers (`1/3` is `0.333333`), where
lite's is a double. `sqrt(-1.0)` is NaN. `toString` `cast(%s as string)`: 1.0 is `'1.0'`.

**So on Databricks:** the `substring`, `substr`, `indexOf` and `round` rows give legend-engine's answers; `toDecimal`
would by the bare DECIMAL rule (scale 0, as H2's), and `isNumeric` by the no-cased-letter rule.

## BigQuery

Path: `bigQueryExtension.pure` (BQX) over the defaults; no dialect translation. An override in BQX replaces every
arity of the default.

| Function | legend-engine writes | Answers on the battery | lite's row | Verdict |
|---|---|---|---|---|
| `substring` (2, 3 arguments) | `substring(s, start[, end])` as given (BQX:99) | a start of 0 reads as 1: (0,3) `abc`, (1,3) `abc`, (2,9) `bcdef` | `SUBSTR(s, start[, end])` | the same answers on all 12 |
| the corrected `substring`, and `substr` | `substring(s, start+1, end-start)` | (0,3) `abc`, (1,3) `bc`; an end below the start raises "Third argument in SUBSTR() cannot be negative" | `SUBSTR(s, (start + 1), (end - start))` | the same answers, the two errors included |
| the String `indexOf` | `strpos(%s, %s)` (BQX:81) | 2, 0 | `STRPOS(s, t)` | the same text |
| `round` (1 argument) | `round(%s, 0)` (BQX:94) | a half away from zero on FLOAT64 and NUMERIC | `CAST(ROUND(x) AS INT64)` | the same values (the type differs) |
| `round` (2 arguments) | **cannot be rendered:** its one template takes one argument, and legend-pure's `format` raises "Unused format args" | none: legend-engine fails when it plans | `ROUND(x, 2)` | lite answers where legend-engine refuses |

Without a row: `toDecimal`, `isNumeric` and `dateDiff` have **no template** on BigQuery (an unsupported-api error).
One-argument `parseDecimal` `cast(%s as decimal)` keeps the value (`123.450021`, NUMERIC). `parseInteger`
`cast(%s as int64)`, `parseFloat` `cast(%s as float64)`. `splitPart`'s default `split_part` **does not exist** on
BigQuery: legend-engine's SQL is refused by the database. `rem` and `mod` are `mod`, which has no FLOAT64 signature
(7.5 mod 5 raises). `toString` `cast(%s as string)`: 1.0 is `'1'`. A division by zero and `sqrt(-1)` raise.

**So on BigQuery:** the `substring`, `substr`, `indexOf` rows and the one-argument `round` give legend-engine's
answers; the two-argument `round` has no legend-engine answer to give.

## What the table can take from this (per row and database)

| Row | H2 | DuckDB | Snowflake | Postgres | Trino | ClickHouse | Databricks | BigQuery |
|---|---|---|---|---|---|---|---|---|
| `substring` as given | yes | yes | same answers | same answers | same answers | differs on multi-byte text | same answers | same answers |
| corrected `substring`, `substr` | yes | yes | same | same | same | differs on multi-byte text | same | same |
| String `indexOf` | yes | yes | same | same | same | differs on multi-byte text | same | same |
| `round` | yes | yes | same values | differs on a Float | same values | differs on a Decimal literal | same values | one argument the same; two has no legend-engine answer |
| `toDecimal` | bare DECIMAL | bare DECIMAL | `to_decimal(x, 38, 19)` | unconstrained `decimal` | none | `toDecimal128(x, 10)` | bare DECIMAL (scale 0) | none |
| `isNumeric` | a pattern | `lower = upper` | none | none | none | none | `lower = upper` | none |
| plain parses | `integer`, `float` | `BIGINT`, `float` (single) | `integer` (38 digits), `float` | `integer`, `float` | `integer`, `double` | `Int32`, `Float32` | `bigint`, `double` | `int64`, `float64` |
| `dateDiff` | its own `datediff` | its own `datediff` | its own `datediff` | field arithmetic, broken on some types | `date_diff` | `datediff` | its own form; weeks refused | none |
| `splitPart` | its own Java | `split_part` | `split_part` | `split_part` | `split_part` | `splitByString` | `split_part` | `split_part`, which the database lacks |

MariaDB, MySQL, SQLite and MongoDB have no legend-engine database to match.
