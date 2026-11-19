# H14: a table's `toString()` computed by the database in the product, with lite's own value texts (2026-11-19)

**Status: DONE (about 40 minutes of a half-day box).** Scratch, never proposed for merge.

- **Branch:** `claude/compat-table-text-db-spike`, from `origin/main` `b75d6c73` with A5's `claude/compat-judges-parity`
  (`f13e73c2`, its channel-B LITE switch `-Dpct.channelb.rules=LITE`) merged.
- **Session:** `session_0195rQEnceykGKzTfRUo7XWp`, for the program's owner (`session_0195rQEoSpv1FxC3WndEk5ZH`).
- **The question (the user, ~16:40 UTC, verbatim):** "either we need to refuse or we need to print in database like
  every other function / Can we test with doing in db with lean sql of lite how many we can pass in the database? I
  guess it will break where 6 decimal float print or timestamp does not match what java does?"
- **Governing:** `docs/records/compat-rules-of-record-2026-11-19.md` on `claude/compat-program` (rule 1: "Database
  calculates everything ... including format and tostring"; it names this gap: "lite's own route refuses a relation's
  `toString()` by name today").
- **Nothing here moves a pin, a ledger, a roster, a golden or an allowlist.** The LITE lanes go red against their own
  ledgers exactly where tests now pass (listed below); no row was edited.

## 0. The answers on one page

**If the database computes a table's `toString()` in legend-pure's `#TDS` layout, each cell spelled by lite's OWN
product text, 363 of PCT's 423 relation-text tests pass byte for byte on DuckDB.** The rest break on exactly the two
cases the user guessed, and on no other:

| Database (PCT channel B, LITE rules) | Relation-text tests run | Pass byte for byte | Fail: a whole Float (`5.0` against lite's `5`) | Fail: an abstract `Date` with a time (`2026-01-07T00:00:00.000+0000` against the database's own cast) | Fail: other |
|---|---|---|---|---|---|
| **DuckDB** (relation 365, standard 58) | 423 | **363** (331 + 32) | 27 | 33 | **0** |
| SQLite (both suites) | 423 | 352 (320 + 32) | 27 | 33 | 11: SQLite capability refusals in the tests' own queries (10 `regexp_extract_all`, 1 `CURRENT_USER`), not the print |
| Postgres 16 (both suites) | 423 | 361 (329 + 32) | 27 | 33 | 2: Variant keys in `jsonb`'s order (fail on `main` too, H11 §4.5) |
| H2 (the relation suite; the only H2 channel-B lane) | 365 | 297 | 3 | 30 | 35: H2 capability refusals (list forms 30, `LATERAL` 4, `fold` 1), none the print's |

- **The Float guess was half right.** No PCT table holds a Float with more than six decimals, so `FLOAT_TO_TEXT`'s six
  decimals never bite (the one long number, `0.9988445981121533` in `testSimpleWindowCorr`, is a `Number` column, which
  prints all its digits and passes). **What breaks is the other half of lite's Float text: a whole Float loses its
  `.0`** (`5.0` is `5`, `1.0` is `1`, `101.0` is `101`): 27 tests on DuckDB, SQLite and Postgres, 3 on H2.
- **The timestamp guess was right, and worse than a spelling.** All 33 are `time:Date[1]` columns (the ABSTRACT `Date`)
  holding a time. The product has no text of its own for such a value: its scalar `toString()` falls to a plain VARCHAR
  cast, so the text is EACH DATABASE'S: `2026-01-07 00:00:00` on DuckDB, H2 and Postgres, `2026-01-07 00:00:00.000000`
  on SQLite (444 of its cells). PCT expects `2026-01-07T00:00:00.000+0000`. **Lite's own product answers differently on
  two databases here**, which is a product parity gap (G2) whatever the compatibility route does. A `DateTime`-typed
  cell would take `DATETIME_TO_TEXT` (`...T00:00:00.000000Z`), which does not match PCT either; PCT's relation tables
  never declare one.
- **Nothing else moved.** On DuckDB every test that changed is one of the 423, and every one of them went from ERROR
  (the product's refusal) to PASS or to a text mismatch; essential, grammar and unclassified are identical test by
  test. Under both of lite's judges (the LITE lane's default, `BOTH`) and under the host judge alone the outcomes are
  the same (relation 431 passes, standard 177, both ways).
- **No hook, no switch, no mode in the core.** One new class in `lowering/` built from product parts only, and the
  `Lowerer`'s `toString` arm calling it instead of the product's refusal (§1).
- **The SQL (DuckDB, the 365 relation tests, the host judge, session statements left out):**

  | | Statements | Characters |
  |---|---|---|
  | `main`'s edge builder (measured on this branch with the change reverted; equal to H11 §4.6) | 376 | 696,964 |
  | **lite's own text in the database (this spike)** | 372 | **659,940** (5% less) |
  | the rows alone, the text presented in Java (H11 §4.6, the lean baseline) | 376 | 187,864 |

  The standard suite's 58: 194,885 characters with the edge builder, 97,594 with lite's text (half). The 4 fewer
  statements are the `asOfJoin` tests, which send one statement less under lite's rules than under the compatibility
  rules (the rule sets, not the print).
- **Two side effects measured:**
  - **H11's P1 is hidden again.** `testExtendJoinStringOnNull` passes on SQLite and Postgres: building the text
    in SQL wraps the relation as a subselect, as the edge's builder did, so the window is computed before the list
    operation reads it. The product defect itself is unchanged (a product query of that shape is still wrong).
  - **H2's 4 `LIST_GET` tests fail again** (`testSimpleProject` and three more, H11 §4.2): the text built in SQL needs a
    list form H2 has not; the rows do not.
- **Database limits:** none reached on the four databases run. The longest PCT table is 5,471 characters
  (`flatten.pure`); 2 of PCT's 442 expected tables exceed MySQL's 1,024-character `GROUP_CONCAT` default, and lite's
  MariaDB and MySQL dialects already raise `group_concat_max_len` to 1 GiB per session (`MariaDb.java:278`,
  `MySql.java:171`; read, not run: no MySQL or MariaDB server in this container).

## 1. What was built

- **`core/src/main/java/com/legend/lowering/ProductRelationText.java`** (149 lines, 118 of code). `toString(rel)` and
  `toString(rel, false)` as one scalar subquery:
  - the layout: `#TDS\n   ` + the names joined by `,` (each trimmed, quoted unless `^[a-zA-Z0-9_]+$` or already
    quoted) + `\n` + the rows, each `   ` + its cells joined by `,`, joined by `\n` in the relation's order + `\n#`; no
    rows leaves the rows segment empty. The names line is written at planning from the plan's typed columns;
  - a cell: an empty one `null` (`'null'` under a Variant); a Variant's text quoted, a String holding `{` or `[` quoted
    (legend-pure's `s()` layout rules, `\` and `'` escaped); **any other its value's PRODUCT text, the exact nodes a
    scalar `toString()` lowers to** (`Scalars`'s toString rule): a DateTime the style's `dateTimes` (the product's
    `DATETIME_TO_TEXT`), a Float `floatRepr` (`FLOAT_TO_TEXT`, or the digits of a Float the store holds as an integer),
    every other kind `pureToString` (a Boolean `BOOL_TO_TEXT`, a Decimal or Number `DECIMAL_TO_TEXT`, the rest the
    product's cast);
  - the order: the product's own `Render.liftOrder` (the relation's sort keys, a hidden column where a key is no
    output), as the `STRING_AGG`'s `ORDER BY`;
  - refused by name: `toString(rel, true)`, a raw grid (`executeInDb`), a pivot whose columns nothing names, a mixed
    (tagged) column, a DISTINCT or star select ordered by a key that is no column. **None was met in PCT.**
- **`Lowerer`'s `relation::toString` arm** calls it, in place of the plan style's facet (whose product answer was the
  refusal `TextRefusals.relation("toString")`). The change is unconditional: under the compatibility style too (whose
  edge builder is therefore no longer reached for `toString`). The compatibility lanes were not run with it; under
  B2's design they would print in Java at the edge anyway. `toCSV` and a TDS's `csv` are untouched.
- **Not used:** `FLOAT_SHORTEST_TEXT`, the edge's `RelationTextLowering`, any `PlanStyle` facet, any hook.
- **Two scratch edits to the PCT HARNESS (test code, not the core):** `-Dpct.channelb.unclipped=true` prints a failure's
  whole message (the display clips a long middle, which hid lite's text); and under `-Dpct.channelb.rules=LITE`,
  `PctBackend.connect` opens the PRODUCT's database. Without the second, H2 under LITE refuses 431 sessions: the lane
  opened the edge's `MODE=LEGACY` H2 while lite's plan declares lite's settings (a harness limit of the LITE lane,
  which A5 built for DuckDB alone).

## 2. The runs

| Run | Command (all `--nocache_test_results`; SQL sizes `--jvmopt=-Dlegend.diagnostics=sql-sizes`) | Result |
|---|---|---|
| Baseline, the branch before the change | `bazel test //pct:pct_channel_b_lite_{relation,standard,essential,grammar,unclassified}` | all five green against their ledgers: relation 100 pass, standard 145, essential 313, grammar 136, unclassified 93; every relation-text test an ERROR (the product's refusal) |
| The change, both judges (the lane's default) | the same | relation **431**, standard **177**; the other three identical test by test |
| The change, the host judge, unclipped | `--jvmopt=-Dpct.channelb.judge=HOST --jvmopt=-Dpct.channelb.unclipped=true` on relation and standard | the same outcomes; the failures' whole texts |
| `main`'s edge builder, for the SQL | the change reverted (`git stash`), `//pct:pct_channel_b_host_{relation,standard}` | 469 and 205 pass, as `main` |
| H2, SQLite, Postgres | `//pct:pct_channel_b_{h2_relation,sqlite_relation,sqlite_standard,postgres_relation,postgres_standard}` with `-Dpct.channelb.rules=LITE -Dpct.channelb.judge=HOST -Dpct.channelb.unclipped=true`; Postgres as `ubuntu` (`docs/CLOUD_SESSIONS.md`) | H2 relation 395 pass; SQLite 420 and 177; Postgres 429 and 169 |

**The LITE lanes' ledger moves the real change would owe** (not made): `duckdb-lite` relation loses 331 failure rows
and standard 32 (the tests that now pass); the other 60 change their words from the refusal to a text mismatch.

## 3. Every failure, by cause (DuckDB, the host judge's whole texts; `tools/classify.py`, scratch)

**A whole Float without its `.0`: 27 tests** (relation 4, standard 23), all of them a Float cell whose value is whole.
`FLOAT_TO_TEXT` trims trailing zeros and the point; legend-pure prints a Float with at least one decimal. The pairs
most met: `5.0`/`5` 33 cells, `10.0`/`10` 25, `1.0`/`1` 24, `3.0`/`3` 23.
- `cumulativeDistribution::testOLAPWithPartitionAndOrderCummulativeDistribution`: PCT `   10,0,J,1.0`, lite
  `   10,0,J,1`.
- `average::testAverage_Floats_Relation_Window` (standard): PCT `   10.0,0,J,10.0`, lite `   10,0,J,10`.

**An abstract `Date` holding a time: 33 tests** (relation 30: `asOfJoin` 4 and `over::testRangeInterval_*` 26;
standard 3: `testExtendMaxDate`, `testExtendMinDate`, `testDayOfWeek_Relation`), 484 cells, every one of them PCT's
`yyyy-MM-ddTHH:mm:ss.SSS+0000` against the database's cast.
- `asOfJoin::testAsOfJoinWithKeyMatch`: PCT `   1,2000-10-25T06:30:00.000+0000,5000,null,null,null`, lite
  `   1,2000-10-25 06:30:00,5000,null,null,null`.
- `max::testExtendMaxDate` (standard; the column declared `time:Date[1]` in the test's own TDS): PCT
  `   1,2026-01-07T00:00:00.000+0000,820000,2026-01-09T01:00:00.000+0000`, lite
  `   1,2026-01-07 00:00:00,820000,2026-01-09 01:00:00`.
- SQLite, the same test family: lite `   0,2024-01-29 00:32:34.000000,10,20` (PCT `   0,2024-01-29T00:32:34.000+0000,10,20`).

**Decimal, StrictDate, null cells, Boolean, Integer, String, enumerations, Variant, ordering: 0 failures on DuckDB.**
Every such cell PCT holds matched lite's text, and every row order matched (the product's order lift).

**Per database, beyond the two causes:**
- **Postgres, 2:** `extend::testVariantColumn_keyExtraction`, `filter::testVariantColumn_filterOnKeyExtractionValue`:
  PCT `'{"boolean":true,"integer":1,"string":"hello"}'`, Postgres's `jsonb` text `'{"string":"hello","boolean":true,"integer":1}'`.
  They fail on `main` too, with the same cell.
- **SQLite, 11:** 10 `groupBy::testSimpleGroupBy_*`, `joinStrings::testJoinStrings_GroupBy_Unordered` and
  `extend::testOLAPAggStringWithPartitionAndUnboundedWindow` refused by SQLite's dialect ("a regexp_extract_all pattern
  that is not a literal"), 1 `TestJoin_CurrentUserId` ("SQLite has no users"). The print writes no regexp and no user
  function: the refusals are in the tests' own queries under lite's rules (the relation is lowered before its text is,
  and the refusal names a function the print never emits). Whether the compatibility rules lower those queries
  differently on SQLite was not checked here.
- **H2, 35:** `STRING_AGG` 12, `LIST_GET` 4, `LIST_FILTER` 4, `UNNEST` 3, `LIST_SORT` 2, and one each of
  `LIST_REVERSE`, membership, `LIST_POSITION`, `LIST_SLICE`, `LIST_TRANSFORM`, `fold` (H2's list encodings), and
  `LATERAL` 4 (H2 has no `LATERAL`). The same families fail on `main`'s H2 lane, except the 4 `LIST_GET` tests,
  which the Java printer fixed (H11 §4.2) and the text built in SQL breaks again.

## 4. What it would take to pass the other 60, by the rules of record

- **The whole Float:** lite's product text of a Float is `FLOAT_TO_TEXT` (six decimals, trailing zeros and point
  trimmed). Matching PCT means either changing lite's own Float text in the product (a product decision, one text for
  every audience), or leaving the product as it is and letting compatibility re-spell the typed Float in Java (rule 3,
  B2's design). The value is the same; only the spelling differs, so tenet 3 allows the re-spelling.
- **The abstract `Date` with a time:** a product gap before it is a compatibility one. The product has no text for an
  abstract `Date` that holds a time, so each database spells it its own way (DuckDB and SQLite differ). Giving it one
  (for example `DATETIME_TO_TEXT` when the value has a time, the day otherwise) would make lite answer the same on
  every database; PCT's `.SSS+0000` would still be compatibility's re-spelling.
- **Neither needs a refusal.** With those two, the database computes every PCT table's text in the product.

## 5. Method and data

- Census of the 423: `H11-data/pct-judge.tsv` (H11 §13.1: every judge-side `toString` of a relation at an assert
  side's root).
- Outcomes: each lane's `test.log` (`[chB-<suite>] PASS|FAIL|ERROR <test> :: <message>`); a message's two texts are
  JSON-quoted (`expected "...", got "..."`), parsed and compared line by line and cell by cell.
- SQL: `[sql-size] <characters> <test> <origin>` lines (`StatementOrigin`), summed over the 423 tests' statements,
  `session` left out.
- The probes: `H14-probes/classify.py.txt` (outcomes and causes), `detail.py.txt` (cell pairs), `sizes.py.txt` (SQL).
- Every log is in the session's scratch folder; none is committed.

## 6. V2: a whole Float keeps one fraction digit (`2.0`), the user's question of ~16:45

"Is there any cheap/lean sql way to get whole floats to be what people expect 2.0? If not that's fine." **Yes: one
more `REPLACE` around the product's existing six-decimal text, the value read once.**

**The spelling.** A six-decimal text ends in `.000000` exactly when the value is whole (six digits always follow the
point; `NaN` and the infinities hold no point). So the whole value's fraction is marked before the trailing zeros are
trimmed, and the mark turned back into a `0` after:

| | The shared helper (`AnsiSqlRenderer`) | DuckDB's cell, `x` a column |
|---|---|---|
| V1 (today) | `rtrim(rtrim(REPLACE(t, '-0.000000', '0.000000'), '0'), '.')` | 81 characters |
| **V2** | `REPLACE(rtrim(REPLACE(REPLACE(t, '-0.000000', '0.000000'), '.000000', '.#'), '0'), '#', '0')` | 114 (**+33**) |
| V1, standard trim | `TRIM(TRAILING '.' FROM TRIM(TRAILING '0' FROM REPLACE(t, '-0.000000', '0.000000')))` | Trino 105, MariaDB 127 |
| **V2, standard trim** | `REPLACE(TRIM(TRAILING '0' FROM REPLACE(REPLACE(t, '-0.000000', '0.000000'), '.000000', '.#')), '#', '0')` | Trino 126, MariaDB 148 (**+21**) |

No concatenation (MySQL's `||` is OR), no second read of the value, a marker (`#`) no number's text holds. The two
helpers are what every dialect writes but Snowflake's and MongoDB's own forms (Snowflake's CASE uses the helper for
its ordinary values; MongoDB's aggregation form was not changed). V2 is its own commit on this branch (§7).

**Checked on every database it can run here** (`core:h14_float_probe`, scratch, each value's `toString()` through the
product's plan; Postgres as `ubuntu`, MySQL, MariaDB, ClickHouse and Trino the build's embedded servers):

| Value | V1 (today) | **V2, on DuckDB, H2, SQLite, Postgres, MySQL, MariaDB, ClickHouse and Trino alike** |
|---|---|---|
| `2.0`, `-2.0`, `0.0`, `100.0`, `101.0`, `123456789.0` | `2`, `-2`, `0`, `100`, `101`, `123456789` | `2.0`, `-2.0`, `0.0`, `100.0`, `101.0`, `123456789.0` |
| `-0.0` | `0` | `0.0` (the sign still dropped: S311's ruled line) |
| `1e20` | `100000000000000000000` | `100000000000000000000.0` |
| `1e-7`, `-1e-7` (below six decimals) | `0` | `0.0` |
| `1.5`, `-1.25`, `0.30000000000000004`, `1/3`, `0.000001`, `1234567.89` | `1.5`, `-1.25`, `0.3`, `0.333333`, `0.000001`, `1234567.89` | the same |
| `NaN`, `Infinity`, `-Infinity` | by name | by name on DuckDB, H2, Postgres, ClickHouse, Trino; SQLite refuses NaN by name ("cannot hold"), MySQL and MariaDB all three, as today |

**PCT's 423 under V2** (channel B, LITE rules; DuckDB both judges, the others the host judge):

| Database | V1 | **V2** | Float failures left | Left |
|---|---|---|---|---|
| **DuckDB** | 363 | **390** (relation 335, standard 55) | **0** | the 33 abstract-`Date` tests, exactly V1's |
| SQLite | 352 | 379 | 0 | 33 Date, 11 SQLite refusals |
| Postgres 16 | 361 | 388 | 0 | 33 Date, 2 `jsonb` key order |
| H2 (relation) | 297 | 300 | 0 | 30 Date, 35 H2 refusals |

DuckDB's whole PCT under V2: relation 435, standard 200, essential **315** (+2: `testFloatToStringWithExcessTrailingZeros`
`17.0` and `testFloatToStringWithPositiveExponent` `134210000.0` now pass), grammar 136, unclassified 93.

**What V2 moves among lite's own tests and pins** (V2 against V1 at this branch's head: `//core:core_tests`, both
core integration lanes, `//pct:pct_duckdb_lite`, the five `pct_channel_b_lite_*`; NOTHING moved here, counted only).
V2 changes the product's scalar `toString()` of a whole Float from `2` to `2.0`: a change to the user's ruling of
2026-11-10 (S311, "let's go back to 6 ... and use decimal where need").
- **5 core test cases pin V1's text and fail under V2:**
  - `integration.FloatTextTest.aFloatsTextIsSixDecimalsTrimmed`: `[1.5, 2, -1.25, 1234567.89, 100000000000000000000]`
    becomes `[..., 2.0, ..., 100000000000000000000.0]`;
  - `integration.TypeInferenceIntegrationTest.testFloatToStringWithNegativeExponent`: `0` becomes `0.0`;
  - `AuditRound3Test.floatReprLargeBand`: `1000000000000000000` becomes `1000000000000000000.0`;
  - `sql.dialect.BigQueryDialectTest.aFloatsTextIsSixDecimalsTrimmed` and
    `sql.dialect.SnowflakeDialectTest.aFloatsPrintFormsAreSpelledHere_theValueReadOnce`: the pinned SQL text.
- **3 rows of channel A's LITE ledger** (`pct_duckdb_lite_essential`): `testFloatToStringWithExcessTrailingZeros`
  and `testFloatToStringWithPositiveExponent` PASS (ledgered as failing with `17` and `134210000`);
  `testFloatToStringWithNegativeExponent` still fails, its words `0.0` where the ledger has `0`.
- **Channel B's LITE ledger (`duckdb-lite`):** the same two essential tests pass, and the relation and standard rows
  of §2 move by 4 and 23 more.
- **No other core suite moved** (33 of 35 targets unchanged, with V1's own 4 cases, §2's, unchanged). Not run under V2:
  the compatibility lanes (the corpus, PCT's COMPAT lanes, the edge net): their Float text is the edge's, not the
  product's `FLOAT_TO_TEXT` (read, not run).

## 7. Commits

- `46354194`: V1, the table text in the database (§0 to §5).
- The next commit: V2 in `AnsiSqlRenderer`'s two helpers, the probe `core/src/test/java/com/legend/exec/H14FloatTextProbeTest.java`
  and its manual target `//core:h14_float_probe` (excluded from `core_tests_exec`).
- **V1's own moves among lite's tests** (V1 against `main`, found by the same runs): 4 core cases, all the spike's
  `toString` change, not the Float text: `RenderCsvIntegrationTest.theProductRefusesARelationsTextByName` (the
  refusal it pins is gone), `DynamicPivotTextRefusalTest` (its refusal's words), `TaggedListTest.anUntypedGridsCellsAreReadOnTheVariantLane`
  (a raw grid's text, refused by the spike), and `FloatTextTest.anEdgeRelationsTextPrintsAFloatCellAsLegendPure` (the
  edge style's table text now goes through the product's lowering, whose `FLOAT_TO_TEXT` the edge style spells as
  legend-engine's cast: `1e-05`, `1e+20`).

## 8. The abstract `Date` holding a time, given ONE product text (D1, D2), each on top of V2

The 33 tests of §3 hold `time:Date[1]` columns, which the database keeps as timestamps; the product had no text of its
own for such a value and spelled it with each database's VARCHAR cast. Each variant gives it one text, in
`Scalars.pureToString` (so a scalar `toString()` gets it too, not only a table cell): an ABSTRACT `Date` whose SQL slot
is `TIMESTAMP` or `TIMESTAMPTZ` (a value with a time: legend-pure's `hasHour` is a precision fact, H11 §13.4); a `DATE`
slot keeps its day.

- **D1, lite's own DateTime text:** the style's `dateTimes` (the product's `DATETIME_TO_TEXT`: ISO 8601 in UTC, six
  fraction digits, `Z`). Commit `9bcbd165`.
- **D2, legend-pure's TDS text:** `STRFTIME(x, yyyy-MM-ddTHH:mm:ss)` + `.` + the first three of the nine subsecond
  digits + `+0000`, composed in SQL (legend-pure's TDS re-reads a date at exactly three subsecond digits, §2 of H11).
  The next commit.

**PCT's 423 (channel B, LITE rules, the host judge; DuckDB's five suites, then the other three):**

| Variant | DuckDB | SQLite | Postgres 16 | H2 (relation) | The 33 Date tests |
|---|---|---|---|---|---|
| V1 (§0) | 363 | 352 | 361 | 297 of 365 | fail: the database's cast |
| V2 (§6) | 390 | 379 | 388 | 300 | fail: the database's cast |
| **V2 + D1** | 390 | 379 | 388 | 300 | **fail on all four, on spelling alone**: PCT `2000-10-25T06:30:00.000+0000`, lite `2000-10-25T06:30:00.000000Z` |
| **V2 + D2** | **423 of 423** | 412 | 421 | 330 | **pass on all four** |

- **One text on every database, both variants:** under D1 every failing Date test's whole text is identical to
  DuckDB's on H2 (30 of 30), SQLite (33 of 33) and Postgres (33 of 33); under D2 they all equal PCT's, byte for byte.
  The database's own spellings (SQLite's `.000000`, the others' space) are gone either way.
- **What is left under V2 + D2 is not the print:** SQLite's 11 refusals in the tests' own queries, Postgres's 2
  `jsonb` key orders (fail on `main` too), H2's 35 capability refusals (§3).
- **Nothing else moved on DuckDB** under D1 or D2: essential 315, grammar 136, unclassified 93 under both; relation
  435 and standard 200 under D1, 465 and 203 under D2.
- **Not run for D1 or D2** (the owner's loop change, 17:34): lite's own suites. They run once, on the variant the
  user picks, for the count of lite's tests and pins that would move. D1 is lite's text and only spelling separates it
  from PCT (compatibility's re-spelling, rule 3); D2 makes the product print legend-pure's form, which the rules of
  record leave to compatibility (rule 3, tenet 4), so D2 is a measurement of the ceiling, not a recommendation.

**The SQL of one abstract-`Date` cell held as a timestamp** (`x` a column; each dialect's own render;
`core/src/test/java/com/legend/sql/dialect/H14DateCellSizeTest.java`, scratch):

| Database | Today (the database's cast) | D1 (`DATETIME_TO_TEXT`) | D2 (legend-pure's, composed) |
|---|---|---|---|
| DuckDB | 18 | 36 (`strftime(x, '%Y-%m-%dT%H:%M:%S.%fZ')`) | 89 |
| SQLite | 15 | 166 | 206 |
| Postgres | 18 | 53 | 108 |
| H2 | 18 | 76 | 134 |

## 9. The compatibility routes under the product's table text

**The spike routes the `Lowerer`'s `toString` arm to the product's lowering under EVERY style**, so the edge style's
table text no longer comes from the edge's builder. Measured at this branch's head (V2 + D2), the compatibility rules,
the host judge: **PCT's `pct_channel_b_host_relation` 469 of 469 and `_standard` 205 of 205 pass, all 423 table
texts equal**, as on `main`. Lite's own edge test shows where it would differ:
`integration.FloatTextTest.anEdgeRelationsTextPrintsAFloatCellAsLegendPure` (`0.00001` and `1e20` print `1e-05` and
`1e+20` under the edge style through the product's lowering; legend-pure's `0.00001` and `100000000000000000000.0`).
The corpus under the compatibility rules was not run with the change.

**What the real slice needs so the compatibility routes stay untouched:** the compatibility edge keeps its own
spelling of a table until B2's Java formatter replaces it. Two ways, neither run here:
- **Keep the `Lowerer`'s call of the plan style's `RelationTexts` facet**, with the product's answer the product's
  lowering instead of the refusal: `RelationTexts` becomes a TWO-FORM kind (the product form passed by the caller, as
  `DateTimeTexts` and `ListTexts` are, `PlanEnv.styled`), which is one row in `testplan.PlanStyleFacets.TWO_FORMS`,
  the set `HookRulingTest` pins exactly: a guard row, for the user's word. No new hook: the facet exists (B17).
- **Or land it with or after B2:** the compatibility edge prints a table in Java from lite's typed rows (H11: PCT 431
  of 431, the corpus byte-identical on DuckDB and +3 on H2), so it never reads the product's in-database text.

**Ruled (the user, ~18:05, "Agree with all four table printing recommendations", relayed by the program's owner at
18:15):** the second way, landing with B2. The ~18:12 ruling ("Yes record it", the rules of record on
`claude/compat-program`) settles the edge's half: a `toString` that is the program's last step is legend-pure's text
printed in Java from typed rows; one used further in the query is lite's own text, computed by the database; no SQL
is kept for legend-pure's spelling. The slice's shape: `H14-slice-shape-note.md`.

## 10. Lite's own suites at the ruled variant, V2 + D1 (one run, 18:17 to 18:26 UTC)

**The branch put at V2 + D1:** `c604cd72` (D2's hunk in `Scalars.pureToString` reverted to D1's
`styled.dateTimes().dateTimeText(x, null)`; the code is `9bcbd165`'s). One run: `//core:core_tests`, both core
integration lanes, `//pct:pct_duckdb_lite` (channel A, lite's rules), the five `//pct:pct_channel_b_lite_*` (channel B,
`duckdb-lite`), `--keep_going --nocache_test_results`. Every moved row and case, with its old and new text:
`H14-data/v2d1-core-moves.tsv` and `H14-data/v2d1-channel-b-ledger-moves.tsv` (`H14-probes/junit_fails.py.txt`, `H14-probes/ledger_moves.py.txt`).

**33 targets pass, 7 fail. What fails is exactly V2's set: D1 moves no core test and no channel A row.**

| Where | Moves against the base (`f13e73c2`, green: V1's run showed no other failing case) | Why |
|---|---|---|
| Core, 9 cases in 3 targets | `FloatTextTest.aFloatsTextIsSixDecimalsTrimmed`, `TypeInferenceIntegrationTest.testFloatToStringWithNegativeExponent`, `AuditRound3Test.floatReprLargeBand`, `BigQueryDialectTest.aFloatsTextIsSixDecimalsTrimmed`, `SnowflakeDialectTest.aFloatsPrintFormsAreSpelledHere_theValueReadOnce` | V2: a whole Float's `2` is `2.0`, a value that rounds to zero `0` is `0.0`, the two dialects' pinned SQL gains the `'.000000'` mark |
| | `RenderCsvIntegrationTest.theProductRefusesARelationsTextByName`, `TaggedListTest.anUntypedGridsCellsAreReadOnTheVariantLane` | the table text: the product's refusal is gone; a raw grid's text keeps a refusal, in the new lowering's words |
| | `FloatTextTest.anEdgeRelationsTextPrintsAFloatCellAsLegendPure`, `DynamicPivotTextRefusalTest.aDynamicPivotsTextIsRefusedByNameAtLowering` | the spike's arm is unconditional, so the edge style's text goes through the product's lowering (`1e-05` for `0.00001`; a refusal's words). Under the ruled landing B2's Java takes both (a last step) |
| Channel A, `pct_duckdb_lite_essential`, 3 rows | `testFloatToStringWithExcessTrailingZeros` (`17`) and `testFloatToStringWithPositiveExponent` (`134210000`) pass and leave; `testFloatToStringWithNegativeExponent` reads `0.0` where its row has `0` | V2 |
| Channel B, `duckdb-lite`, **426 rows of `ledger.tsv`** | **392 leave** (Relation 335, Standard 55, Essential 2); **34 change their text** (the 33 abstract-`Date` tables, Relation 30 and Standard 3, from the refusal to `...T06:30:00.000000Z` against PCT's `.000+0000`; Essential 1, `0` to `0.0`) | the table text and V2; every one of the 423 refusal rows moves, and no other row |

**PCT channel B under lite's rules (DuckDB, of 1,249): 1,179** (relation 435 of 469, standard 200 of 205, essential
315 of 345, grammar 136 of 137, unclassified 93 of 93). The base: **787** (100, 145, 313, 136, 93). The rows left on
`duckdb-lite` after the moves: Relation 34 (the 30 Date tables and 4 others), Standard 5, Essential 30, Grammar 1.

**The SQL at V2 + D1** (DuckDB, the relation-text tests, the host judge's statements as §0 counts them: origin
`OTHER`; `-Dlegend.diagnostics=sql-sizes`, one extra run of the two suites, `H14-probes/sizes.py.txt`): relation 365 tests **660,748** characters (V1 659,806; `main`'s edge builder 696,964;
the rows alone, H11's Java print, 187,864), standard 58 tests **99,037** (V1 97,594; the edge builder 194,885). The
body's fused verdict statements (origin `BODY`, the database judge's, under the lane's `BOTH`): 1,428,486 and 216,487
(V1 1,427,400 and 215,044).
