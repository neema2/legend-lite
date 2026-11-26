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
