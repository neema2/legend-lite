# Request 5: a date-time column against a date written to the hour or the minute, inside a query legend-engine sends to a database (RUN 2026-11-27)

*Asked by the orchestrator for the final review's finding S2 (the doors' date rule inside a store query was never
measured). Every row below was RUN by the upstream runner's session on legend-engine 4.145.0 (EngineProbe), on H2
2.2.224 and on DuckDB 1.3.0; the source lines were READ in the pinned release. Measure only.*

**Set-up.** Table `D (ID INTEGER PRIMARY KEY, TS TIMESTAMP)` with rows `(1, 2015-04-16 14:00:00)` and
`(2, 2015-04-16 14:30:00)`; read as a relation (`#>{p::DB.D}#->filter(r | $r.TS == X)`) and through a class mapped to
it, one property declared `DateTime` and one declared `Date`, both on `TS`. The three forms sent the same SQL and got
the same rows in every case; swapping the sides only swaps the operands.

| X | SQL legend-engine sends (H2; DuckDB the same but for a space after `TIMESTAMP`) | rows |
|---|---|---|
| `date(2015, 4, 16, 14)` | none: REFUSED when planned, "No SQL translation exists for the PURE function 'date_Integer_1__Integer_1__Integer_1__Integer_1__DateTime_1_'." | none |
| `date(2015, 4, 16, 14, 0)` | none: refused, the same words with the five-integer signature | none |
| `%2015-04-16T14` | `... where "d_0".TS = TIMESTAMP'2015-04-16'` | `[]` |
| `%2015-04-16T14:00` | `... where "d_0".TS = TIMESTAMP'2015-04-16'` | `[]` |
| `%2015-04-16T14:00:00` | `... where "d_0".TS = TIMESTAMP'2015-04-16 14:00:00'` | `[1]` |

**What it means.** Inside a store query legend-engine writes an hour-precision and a minute-precision literal as the
DAY alone (`TIMESTAMP'2015-04-16'`): the hour and the minute are dropped, so the 14:00 row is not matched (a row at
midnight would be). Only a seconds-precision literal keeps its time. A `date(...)` built to the hour or the minute
has no SQL translation there: refused.

**Why (READ, legend-engine 4.145.0, `core_relational/relational/sqlQueryToString`).** H2's literal processor
(`dbSpecific/h2/h2Extension2_1_214.pure:154-155`) formats a `DateTime` and a `Date` with
`convertDateToSqlString` (`extensionDefaults.pure:144-154`), whose format string is
`'%t{[' + $timeZone + ']yyyy-MM-dd?[ HH:mm:ss?[.S*]]}'` (`:151`): the optional section ` HH:mm:ss` is one unit, so a
date that carries an hour or a minute and no second loses the whole section. DuckDB's own literal processor was not
read; its output is the same.

**For contrast, legend-engine's own Java, outside any store query (RUN the same day; as request 3's group D):**

| expression | answer |
|---|---|
| `%2015-04-16T14:00:00 == date(2015, 4, 16, 14)` | false |
| `%2015-04-16T14:00:00 == date(2015, 4, 16, 14, 0)` | false |
| `%2015-04-16T14:00:00 == %2015-04-16T14` | false |
| `%2015-04-16T14:00:00 == %2015-04-16T14:00` | false |
| `%2015-04-16T14:00:00 == %2015-04-16T14:00:00` | true |
| `date(2015, 4, 16, 14) == %2015-04-16T14` (request 3) | true |

So Pure's meaning, which its own Java answers: a date's written precision is part of its value.

## What lite does, and what was decided (the orchestrator, 2026-11-27)

- **A date BUILT to the hour or the minute against a date-time column, at a compatibility door:** lite answers
  false (no rows), which is Pure's answer; legend-engine refuses to plan it there. Lite answers where legend-engine
  refuses: kept, recorded (register S432).
- **A date-time LITERAL written to the hour or the minute against a date-time, at a compatibility door:** lite
  compares the two by their instant, as at its own door (register S399), so the 14:00 row IS matched: neither Pure's
  answer (false) nor legend-engine's rows inside a store query (none). Older than step 6; no recorded test holds the
  case. NOT changed in step 6's landing: recorded with these answers (register S432, the differences page) and put
  to the user in the landing summary; its fix goes with the doors' date rule in step 6B.
