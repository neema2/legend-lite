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

DuckDB's and H2's answers are the ones "What to read in the answer" below expects. The six databases it was never
run on (Postgres, Trino, Snowflake, ClickHouse, Databricks, BigQuery) are measured by the database lanes; their
answers are added below when they report.

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
