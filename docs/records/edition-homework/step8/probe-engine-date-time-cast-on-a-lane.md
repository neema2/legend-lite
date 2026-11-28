# Step 8: a lane probe for a DateTime's text at legend-engine's door (measure only)

*Written 2026-11-26 by the step 8 session for the orchestrator, who sends it to the database lanes (the outside
audit of slices 3 and 4, its S2). Nothing here is committed by a lane: the two edits are temporary, in the lane's own
Bazel tree.*

**VERIFIED HERE on 2026-11-28** (the step 8 session, on `claude/edition-step8` after the program branch's merge,
`37e84d47`; the steps below as written, on `//pct:pct_duckdb_essential`, `//pct:pct_h2_essential` and
`//pct:pct_sqlite_essential`): the probe class is selected and prints its four `PROBE CASE` lines on each. The target
itself fails, as step 3 says it will. What it answered:

| Database | The SQL lite sent | literal, a fraction | literal, no fraction | computed | the column's three rows |
|---|---|---|---|---|---|
| DuckDB | `SELECT CAST(TIMESTAMP '2014-01-01T00:00:00' AS VARCHAR) AS value`; `CAST(TIMESTAMP '2014-01-01T10:20:30.250' + to_hours(1) AS VARCHAR)`; `CAST(_tds0."at" AS VARCHAR)` | `2014-01-01 00:00:00` | `2014-01-01 00:00:00` | `2014-01-01 11:20:30.25` | `2014-01-01 10:20:30.25`, `2014-01-01 10:20:30.123456`, `2014-01-02 00:00:00` |
| H2 | `SELECT CAST(TIMESTAMP '2014-01-01T00:00:00.000' AS VARCHAR) AS "value"`; `CAST(dateadd(HOUR, 1, TIMESTAMP '2014-01-01T10:20:30.250') AS VARCHAR)`; `CAST("_tds0"."at" AS VARCHAR)` | `2014-01-01 00:00:00` | `2014-01-01 00:00:00` | `2014-01-01 11:20:30.25` | the same three as DuckDB |
| SQLite (legend-engine has no SQLite: no row, the call's own rule) | a literal is the constant `'2014-01-01T00:00:00+0000'`; a computed value and a column are lite's own `strftime` text | `2014-01-01T00:00:00.000+0000` | `2014-01-01T00:00:00+0000` | `2014-01-01T11:20:30.250000000Z` | `2014-01-01T10:20:30.250000000Z`, `2014-01-01T10:20:30.123456000Z`, `2014-01-02T00:00:00.000000000Z` |

DuckDB's and H2's answers are the ones "What to read in the answer" below expects.

## The six databases, run by the lanes on 2026-11-28 (at `7831492c`, each by the steps below; measure only)

Every lane selected the probe class (four `PROBE CASE` lines), nothing raised, and each tree was restored. The SQL
and the answers are each lane's report, verbatim in substance (the full statements are in the lanes' logs).

| Database | What lite sent for `toString` | literal, a fraction | literal, no fraction | computed | the column's three rows |
|---|---|---|---|---|---|
| Postgres 16.15 | `CAST(TIMESTAMP '2014-01-01T00:00:00.000' AS VARCHAR)`; the column `CAST("_tds0"."at" AS VARCHAR)` | `2014-01-01 00:00:00` | `2014-01-01 00:00:00` | `2014-01-01 11:20:30.25` | `2014-01-01 10:20:30.25`, `2014-01-01 10:20:30.123456`, `2014-01-02 00:00:00` |
| Databricks | `CAST(TIMESTAMP '2014-01-01T00:00:00.000' AS STRING)` | `2014-01-01 00:00:00` | `2014-01-01 00:00:00` | `2014-01-01 11:20:30.25` | the same three as Postgres |
| Trino 483 | `CAST(TIMESTAMP '2014-01-01 00:00:00.000' AS VARCHAR)` | `2014-01-01 00:00:00.000` | `2014-01-01 00:00:00` | `2014-01-01 11:20:30.250` | `2014-01-01 10:20:30.250000`, `2014-01-01 10:20:30.123456`, `2014-01-02 00:00:00.000000` |
| ClickHouse 26.3 | `CAST(toDateTime64('2014-01-01 00:00:00.000', 6, 'UTC') AS Nullable(String))` | `2014-01-01 00:00:00.000000` | `2014-01-01 00:00:00.000000` | `2014-01-01 11:20:30.250000` | `2014-01-01 10:20:30.250000`, `2014-01-01 10:20:30.123456`, `2014-01-02 00:00:00.000000` |
| Snowflake | **not a cast:** `REGEXP_REPLACE(TO_CHAR(TIMESTAMP '...', 'YYYY-MM-DD HH24:MI:SS.FF6'), '\\.?0+$', '')` | `2014-01-01 00:00:00` | `2014-01-01 00:00:00` | `2014-01-01 11:20:30.25` | the same three as Postgres |
| BigQuery | **not a cast:** `FORMAT_DATETIME('%Y-%m-%d %H:%M:%E*S', DATETIME '...')` | `2014-01-01 00:00:00` | `2014-01-01 00:00:00` | `2014-01-01 11:20:30.25` | the same three as Postgres |

Snowflake's session (read by its lane over a plain session, no `ALTER SESSION`): `TIMESTAMP_NTZ_OUTPUT_FORMAT` is
`YYYY-MM-DD HH24:MI:SS.FF3`, `TIMESTAMP_OUTPUT_FORMAT` `YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM`, `TIMESTAMP_TYPE_MAPPING`
`TIMESTAMP_NTZ`, all at their defaults.

**What legend-engine itself writes for the operand (READ 2026-11-28 in the pinned 4.145.0 sources; the text of a
literal is `convertDateToSqlString`, `yyyy-MM-dd HH:mm:ss` and the literal's own sub-second digits, `[.S*]`:
`extensionDefaults.pure`:144-154):**

| Database | legend-engine's DateTime literal | Where |
|---|---|---|
| Postgres | `Timestamp'<text>'` | `postgresExtension.pure`:176 |
| Trino | `Timestamp'<text>'` | `trinoExtension.pure`:49 |
| Databricks | `to_timestamp('<text>')` | `databricksExtension.pure`:87 |
| ClickHouse | `'<text>'::DateTime64` | `clickHouseExtension.pure`:68 |
| Snowflake | `'<text>'::timestamp` | `snowflakeExtension.pure`:165 |
| BigQuery | `PARSE_DATETIME('%Y-%m-%d %H:%M:%E6S', '<text>')` | `bigQueryExtension.pure`:46 |

**The reading, database by database:**

- **Postgres, Databricks: as ruled.** Lite sends the database's cast of a timestamp, legend-engine's template with
  an operand of the same type, and the texts are DuckDB's and H2's.
- **Trino: the database's cast, and its text follows the timestamp's own precision.** A literal written with three
  fraction digits prints three (`.000`), one with none prints none, and a column of mixed rows prints six for every
  row. Lite's literal is spelled as legend-engine's is (`TIMESTAMP '<text with the literal's digits>'`), so a
  literal's text should be legend-engine's; a computed value's and a column's follow lite's type for them, which was
  not compared with legend-engine's.
- **ClickHouse: the database's cast, of another operand type.** Lite's timestamp is `DateTime64(6, 'UTC')`, so every
  text has six fraction digits. legend-engine's literal is `'<text>'::DateTime64` with no precision written. What
  that casts to is being run on the lane's ClickHouse (below).
- **Snowflake, BigQuery: lite does not send the cast.** Their dialects write a cast of a timestamp to text as a
  format call with lite's own fixed layout (the fraction trimmed), so the text does not follow the database's own
  cast. On Snowflake legend-engine's `cast(... as varchar)` would follow the session's format, three fraction digits
  (`2014-01-01 00:00:00.000`), by reading; on BigQuery its cast of a `DATETIME` is being run (below).

So of the six, two are as the row says, one is the database's cast with a precision question, and three do not give
legend-engine's text or are not yet known to. This goes to the user as a question (narrow the row to the databases
measured equal, or change what those dialects write for this rule); nothing is changed here.

**legend-engine's own SQL RUN on the three databases** (the lanes' follow-up the same day: legend-engine's literal
and its `cast(... as <text type>)`, written by hand from the templates above and sent as plain statements; nothing
raised):

| The value | ClickHouse 26.3: `cast('<text>'::DateTime64 as String)` | lite there | Snowflake: `cast('<text>'::timestamp as varchar)` | lite there | BigQuery: `cast(PARSE_DATETIME('%Y-%m-%d %H:%M:%E6S', '<text>') as string)` | lite there |
|---|---|---|---|---|---|---|
| `2014-01-01 00:00:00.000` | `2014-01-01 00:00:00.000` | `2014-01-01 00:00:00.000000` | `2014-01-01 00:00:00.000` | `2014-01-01 00:00:00` | `2014-01-01 00:00:00` | `2014-01-01 00:00:00` |
| `2014-01-01 00:00:00` | `2014-01-01 00:00:00.000` | `2014-01-01 00:00:00.000000` | `2014-01-01 00:00:00.000` | `2014-01-01 00:00:00` | `2014-01-01 00:00:00` | `2014-01-01 00:00:00` |
| `2014-01-01 10:20:30.25` | `2014-01-01 10:20:30.250` | `2014-01-01 10:20:30.250000` | `2014-01-01 10:20:30.250` | `2014-01-01 10:20:30.25` | `2014-01-01 10:20:30.250` | `2014-01-01 10:20:30.25` |
| `2014-01-01 10:20:30.123456` | `2014-01-01 10:20:30.123` | `2014-01-01 10:20:30.123456` | `2014-01-01 10:20:30.123` | `2014-01-01 10:20:30.123456` | `2014-01-01 10:20:30.123456` | `2014-01-01 10:20:30.123456` |

- **ClickHouse:** legend-engine's `'<text>'::DateTime64` is `DateTime64(3)` (`toTypeName`, run), so its text has
  exactly three fraction digits and the VALUE is cut at the millisecond (`.1239` answers `.123`, run: cut, not
  rounded); the session's time zone does not change the text. Lite's timestamp there has six digits. The two differ
  on every case, and legend-engine's loses the digits past the millisecond.
- **Snowflake:** legend-engine's cast follows the session's `TIMESTAMP_NTZ_OUTPUT_FORMAT` (`...FF3` at its
  default): exactly three fraction digits, the finer ones dropped from the text (whether cut or rounded was not
  run). Lite writes the fraction trimmed and whole. The two differ on every case, and legend-engine's text depends
  on a session setting lite does not set.
- **BigQuery:** its own `cast(DATETIME as string)` writes no fraction for a whole second, else three or six digits
  padded with zeros. Lite's format call trims instead, so the two differ only where the fraction is not exactly
  three or six digits long (`.25` against `.250`). The plain cast over the same `DATETIME` would be legend-engine's
  text exactly, with no digit lost.

**Are the digits lost, or only not shown? (the user's question, 2026-11-28: "Don't snow and click have nanosecond
timestamp though".)** Both databases hold nine fraction digits, and lite sends a literal's nine whole on both
(register S181; `Snowflake.literalFractionDigits`, `ClickHouse.timestampLit`). RUN on Snowflake by its lane, plain
statements over `'2014-01-01 10:20:30.123456789'::timestamp`:

| Statement | Answer |
|---|---|
| `SYSTEM$TYPEOF(...)` | `TIMESTAMP_NTZ(9)[SB16]` |
| `to_char(..., 'YYYY-MM-DD HH24:MI:SS.FF9')` | `2014-01-01 10:20:30.123456789` |
| legend-engine's `cast(... as varchar)` | `2014-01-01 10:20:30.123` |
| the same cast over `.1239` | `2014-01-01 10:20:30.123` (cut, not rounded) |
| the session's `TIMESTAMP_NTZ_OUTPUT_FORMAT` | `YYYY-MM-DD HH24:MI:SS.FF3` |
| lite's text rule today (`TO_CHAR(..., '...FF6')`, zeros trimmed) | `2014-01-01 10:20:30.123456` |
| the same rule with `FF9` | `2014-01-01 10:20:30.123456789` |
| both rules over `.25` and over a whole second | `2014-01-01 10:20:30.25`, `2014-01-01 00:00:00` (the same from each) |

So on Snowflake the value is whole and legend-engine's text only shows three of its digits, by a session setting.

RUN on ClickHouse by its lane (`clickhouse local` 26.3.60.2, time zone `Etc/UTC`, plain statements):

| Statement | Answer |
|---|---|
| legend-engine's literal, `cast('...10:20:30.123456789'::DateTime64 as String)`, and its type | `2014-01-01 10:20:30.123`, `DateTime64(3)` |
| `toUnixTimestamp64Nano` of that literal | `1388571630123000000` (the value itself holds milliseconds only) |
| lite's literal of a digit past the microsecond, `cast(toDateTime64('...123456789', 9, 'UTC') as String)` | `2014-01-01 10:20:30.123456789` |
| `toUnixTimestamp64Nano` of lite's literal | `1388571630123456789` |
| lite's literal otherwise, `cast(toDateTime64('...10:20:30.25', 6, 'UTC') as String)` | `2014-01-01 10:20:30.250000` |
| a `DateTime64(9, 'UTC')` column cast to String | `2014-01-01 10:20:30.123456789` |
| a `DateTime64(3, 'UTC')` and a `DateTime64(6, 'UTC')` column holding `.25`, cast to String | `2014-01-01 10:20:30.250`, `2014-01-01 10:20:30.250000` |

So on ClickHouse the cast prints exactly the digits of the operand's type. legend-engine's LITERAL is a
`DateTime64(3)` and loses the digits in the value itself, cut and not rounded; a column keeps and prints its own
precision, and there the same cast is what legend-engine sends and what lite sends. The two differ on a literal
(three digits against lite's six or nine), not on a column.

**A defect of lite's found on the way (not step 8's; nothing changed).** Lite's own text of a date-time on Snowflake
(`Snowflake.java`, the cast of a timestamp to text: `'YYYY-MM-DD HH24:MI:SS.FF6'`) drops digits seven to nine of a
value lite itself sent whole: `.123456789` prints `.123456`. With `FF9` the same rule prints all nine and answers
the short values as before (run, above). It is in the product's SQL on `main`, older than step 8.

**Summary of D10 for the user.** As ruled on Postgres and Databricks; Trino's literal as legend-engine's by
reading; BigQuery differs in padding and could be made exact by sending the plain cast; Snowflake and ClickHouse
differ on every case, and there matching legend-engine would mean a text that drops digits past the millisecond
(and on Snowflake follows a session setting). Nothing is changed; the choice is the user's.

## The question

Inside a store query at legend-engine's door a DateTime's `toString` is `NamedRule.DATE_TIME_TEXT_BY_CAST`, the
database's own cast of the timestamp to text, on every database of `PureText.ENGINE_DATABASES` (the user, 2026-11-26:
"match engine for engine"). It was run on DuckDB and H2. On Postgres, Trino, Snowflake, ClickHouse, Databricks and
BigQuery its SQL is held as text (`edge/EngineDateTimeTextRowTest`) and was never run. The probe shows, on one lane's
database, the SQL lite sends and the text the database answers, for a literal with and without a fraction, a
computed DateTime and a DateTime column.

## What legend-engine writes (read in the pinned 4.145.0 sources, not run)

One template for every type's `toString`, per extension (`dynaFnToSql('toString', $allStates, ...)`):

| Database | legend-engine's template | Where |
|---|---|---|
| Postgres | `cast(%s as varchar)` | `postgresExtension.pure:254` |
| Trino | `cast(%s as varchar)` | `trinoExtension.pure:123` |
| Snowflake | `cast(%s as varchar)` | `snowflakeExtension.pure:388` |
| ClickHouse | `cast(%s as String)` | `clickHouseExtension.pure:146` |
| Databricks | `cast(%s as string)` | `databricksExtension.pure:168` |
| BigQuery | `cast(%s as string)` | `bigQueryExtension.pure:102` |
| H2, DuckDB (run) | `cast(%s as varchar)` | `h2Extension2_1_214.pure:266`, `duckdbExtension.pure:284` |

Its operand is the timestamp: a literal goes through the extension's literal processor (a timestamp literal, never
the quoted text), a column is the column.

## The probe

1. Copy the probe class into the lane's tree (it is in this record's folder as
   `probe-engine-date-time-cast-on-a-lane.java.txt`):
   `cp docs/records/edition-homework/step8/probe-engine-date-time-cast-on-a-lane.java.txt pct/src/test/java/org/finos/legend/lite/pct/extension/EngineDateTimeCastProbeTest.java`
   (the name must end in `Test`: the runner selects no other class)
2. Print the SQL: in `pct/src/test/java/org/finos/legend/lite/pct/extension/PctPlanRoute.java`, after the line
   `com.legend.result.Collector.Collected c = collector.collected();` add
   `if (System.getProperty("probe") != null) { System.err.println("PROBE SQL " + statements(plan)); }`
3. Run it on the lane's essential suite target, at legend-engine's audience:
   `bazel test //pct:pct_<name>_essential --test_arg=--select-class=org.finos.legend.lite.pct.extension.EngineDateTimeCastProbeTest --jvmopt=-Dpct.audience=engine --jvmopt=-Dprobe=1 --test_output=all --cache_test_results=no`
   The lane's essential suite runs with it (a JUnit 3 suite cannot be filtered out) and the target FAILS: the suite's
   own expectations are legend-pure's, and the run is at legend-engine's audience. Neither matters here. `--jvmopt`
   discards Bazel's analysis cache once.
4. Report the probe's lines of the test's log: each `PROBE CASE` line, the `PROBE SQL` line that follows it and its
   `PROBE ANSWER` or `PROBE RAISED` line (`grep -A2 'PROBE CASE' test.log`; the suite's own cases print `PROBE SQL`
   lines too, which are not the probe's), then
   `git checkout -- pct/src/test/java/org/finos/legend/lite/pct/extension/PctPlanRoute.java` and delete the probe
   class.

## What to read in the answer

- **The SQL** is the lane's dialect's cast of a timestamp to its text type. It is legend-engine's where the cast's
  target is the template's above and its operand a timestamp (a literal written as a timestamp, not as a text).
- **The text** is that database's layout of a timestamp. On DuckDB and H2: `2014-01-01 00:00:00` for a literal with
  or without a fraction, `2014-01-01 11:20:30.25` for the computed value, and for the column's three rows
  `2014-01-01 10:20:30.25`, `2014-01-01 10:20:30.123456`, `2014-01-02 00:00:00`.
- **What would narrow the row's set:** a cast whose operand is not the type legend-engine casts (a zoned timestamp
  where legend-engine's is not, so the text carries `+00`), or a text that is not the database's cast of that
  operand.
- **Snowflake:** a timestamp's cast to text follows the session's `TIMESTAMP_NTZ_OUTPUT_FORMAT` (or
  `TIMESTAMP_OUTPUT_FORMAT`). Lite's session sets neither (no `ALTER SESSION` anywhere in lite), so the text is the
  account's default, as legend-engine's own cast's would be on the same session. Report the session's value with the
  answer (`SHOW PARAMETERS LIKE 'TIMESTAMP%OUTPUT_FORMAT' IN SESSION`).
