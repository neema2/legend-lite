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
