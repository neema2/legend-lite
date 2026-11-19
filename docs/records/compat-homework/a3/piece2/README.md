# A3, piece 2: substring and indexOf answer Pure's meaning in the core (2026-11-19)

**The code:** local branch `claude/compat-a3-switch`, on A2's pushed branch (`claude/compat-a2-printers` at
`6551c203`). It is **not pushed**: this session pushes only to `claude/compat-translator-spike` without the user's word
here. On top of A2 it has piece 1 (`633a0561`) and:
- `2f01b45b`:
  - `substring`'s plain rule is Pure's corrected one, 0-based and end-exclusive, with or without
    `CORRECT_SQL_SUBSTRING_INDEXING` (S130);
  - `indexOf(s, t)` is `STRPOS - 1`, and `indexOf(s, t, from)` follows Java's `String.indexOf` (S134);
  - **the out-of-range raise** (S383): `RaiseKind.SUBSTRING_OUT_OF_RANGE`, its product words in `ErrorKind`, and
    legend-pure's in `PureMessages`;
  - `FeatureRules`: javadoc only.
- `c2cb56bf`: lite's PCT adapters set no feature flag, on channel A (`PctPlanRoute`) and channel B (`ChannelB`). This
  reverses the 2026-09-12 deviation.
- `c1979009`: `SubstringRangeTest`, new. It checks Pure's answers and the raise on every in-process database.
- Piece 1's guard moves, carried over (`c32e6744`, `bc356444`, by the user's word).

## Measured

`fast-loop-targets.txt`, `lanes-targets.txt`.

- **The fast loop, plus the lanes the change touches:** red only where a guard records the old meaning (below).
  - Green: census, `core_tests_sql`, `core_tests_planner`, normalizer, IQ and SQL parity, the corpus verdicts, the
    PCT suites other than Essential.
  - The `substring` tests pass without the adapter's option.
- **`SubstringRangeTest`:** green on every in-process database.
- **PCT Essential, by lane** (`pct-stale-rows.txt`): every lane measured fails on exactly the 3 `indexOf` known
  failures, which now pass, and on nothing else.
  - Channel A, 8 classes: DuckDB, SQLite, MariaDB, MySQL, ClickHouse, Trino, MongoDB, and Postgres as `ubuntu`.
  - Channel B, 7 lanes: the same databases less MongoDB, which has no channel B lane.
  - **Not runnable here:** BigQuery, Snowflake and Databricks, on both channels (cloud).
  - A stale known failure turns both channels red: channel A's runner ("expected an error containing"), and channel
    B's ledger ("PASSES, ledgered as failing").
- **The corpus**, against A2's rosters (`h2-fail-roster.txt`, `duckdb-fail-roster.txt`):
  - each fail roster gains the same 7 tests on both databases: the 4 answer tests and the 3 hidden text tests;
  - each unordered register loses 4: the three `testFilterUsing...Function` tests and
    `testToSQLStringSubstringIsUncorrectedByDefault`;
  - the skipped rosters and engine-order registers do not move.
- **The edge net** (`edge-net-diffs.txt`): the only moves are `indexOf`'s and the 7 corpus tests'.

## The guard moves it needs, for the user's word (none made)

| move | direction | lines |
|---|---|---|
| PCT known-failure rows for `indexOf::testSimple`, `testFromIndex`, `indexof::testIndexOfOneElement` | **down** | 63: 45 measured (8 channel A classes and 7 channel B lanes, 3 each); 18 on the cloud lanes, **predicted from the core change, measured by the batch's CI run**, since a stale row turns those lanes red |
| corpus unordered registers, both databases | **down** | −4 each |
| corpus fail rosters, both databases | **up** | +7 each |
| the 9 old-meaning pins: `TypeInferenceIntegrationTest` (`testSubstringStart`, `testStringIndexOf`, `testStringIndexOfSimple`, `testStringIndexOfFromIndex`, `testIndexOfOneElement`, `testSortWithKeyFunction`), `ExtendCheckerTest$StringExtend.testIndexOf`, `ValueSortComparatorTest.keyFunctionSort`, `SubstrIndexingTest.substrAlwaysCorrected` | re-pinned to Pure's answers (neither) | 9 tests |
| the edge net (`bazel run //pct:update_edge_contract`, the corpus edge net's update) | re-recorded (neither) | 5 files: `duckdb_essential_answers.tsv` and `sqlite_essential_answers.tsv` (−3 +7 each: the `indexOf` tests now run past their first assert); `channel_b_duckdb_essential_outcomes.tsv` (3 lines, ERROR to PASS); `corpus_{duckdb,h2}_host_judge-host.tsv` (−14 +4 each: the 7 corpus tests) |

`duckdb_essential_outcomes.tsv` and `sqlite_essential_outcomes.tsv` are red only while the known-failure rows stay.
They match the golden once the rows leave.

## S383's text (read and measured: `s383/`)

**legend-pure's `substring` is Java's `String.substring`:**
- interpreted: `SubString.java:56,61`;
- compiled: `CompiledSupport.java:1546-1556`.

It wraps the JDK's exception without changing its text:
- interpreted (`FunctionExecutionInterpreted.java:883-893`):
  `Execution error at (<source>), "<JDK text>"`;
- compiled (`FunctionExecutionCompiled.java:247-264`): `Error executing <fn>. <JDK text>`.

**The JDK's text**, measured by `Sub.java.txt` (`jdk25.txt`, `jdk21.txt`): `Range [-1, 6) out of bounds for length 6`.
- It is the same on lite's and the engine runner's JDK, 25.0.4 (`.bazelrc:6`, remotejdk_25), and on the container's
  21.0.13.
- **Lite pins the JDK's part**, as it pins legend-pure's message part for every other raise kind. The envelopes are
  the presenter's, for B1's error records.

**Tooling finding, for the compat owner after stage A:** the engine runner cannot run a storeless function test.
legend-engine compiles it to Java, and the `@maven_runner` pool lacks the plan-dependencies jar
(`org.finos.legend.engine.plan.dependencies.store.shared`). Every probe function errored with that, the in-range
control included (`s383/report.tsv`, `s383/runner-excerpt.txt`).
