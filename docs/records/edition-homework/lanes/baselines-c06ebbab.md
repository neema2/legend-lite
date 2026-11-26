# Lane baselines at `c06ebbab` (2026-11-25)

*Each lane's first round on the merged tree (step 1, B2's branch merged, the audit's fixes), kept as its history: later
rounds compare with these logs test by test. Each lane keeps its logs in its own scratchpad under `r1/c06ebbab/`.*

## SQLite and MongoDB (`session_0195rQEn8iDNDtGvFr9Db1jW`)

| Run | Essential | Grammar | Relation | Standard | Unclassified |
|---|---|---|---|---|---|
| SQLite channel A | 345/345 | 137/137 | 469/469 | 205/205 | 93/93 |
| SQLite channel B | PASS=336 ERROR=9 | PASS=134 ERROR=3 | PASS=458 ERROR=11 | PASS=205 | PASS=92 ERROR=1 |
| MongoDB channel A | 345/345 | 137/137 | 469/469 | 205/205 | 93/93 |

Every suite passed (channel B's errors are the ones its ledger expects). No "does not match its ledger" block.
Timings: setup 7m24s, build 1m11s, `pct_sqlite` 2m03s, `pct_channel_b_sqlite` 28s, `pct_mongodb` 2m17s. The docs'
commands worked as written.

## Snowflake (`session_0195rQEixiryogCvRMhCTxvs`)

| Run | Essential | Grammar | Relation | Standard | Unclassified |
|---|---|---|---|---|---|
| Channel A | 345/345 | 137/137 | 469/469 | 205/205 | 93/93 |
| Channel B | PASS=338 ERROR=7 | PASS=133 ERROR=4 | PASS=469 | PASS=205 | PASS=93 |

All 10 suites pass; every channel B error is inside its ledger. Timings: setup 432 s, credentials under a second,
build 61 s, channel A 264 s, channel B 128 s: about 6.5 minutes of warehouse time for the round, not the 75 the older
records estimate.

### A finding for the user: the exact Float text on Snowflake is not what its javadoc says

B2's `SqlFn.FLOAT_FULL_TEXT` on Snowflake is `CAST(CAST(x AS DECFLOAT) AS VARCHAR)` (amendment 21; "unmeasured as this
node until Snowflake's lane runs"). The lane sent nine doubles through that exact expression (no test of the embedded
lanes runs on a cloud lane: the pin is `ProductRelationTextTest.aCsvsFloatCellIsTheDatabasesExactTextOfTheDouble`):

| Double | Snowflake's text | Reads back as the same double |
|---|---|---|
| 0.1 | `1.0000000000000000555111512312578270212e-1` | yes |
| 1.0E-7 | `9.9999999999999995474811182588625868561e-8` | yes |
| 123456789.12345679 | `123456789.12345679104328155517578125` | yes |
| 1.7976931348623157E308 | `1.797693134862315708145274237317043568e308` | yes |
| 4.9E-324 | `4.9406564584124654417656879286822137237e-324` | yes |
| 0.30000000000000004 | `3.0000000000000004440892098500626161695e-1` | yes |
| 100.0 | `100` | yes, with no `.0` |
| NaN | `NaN` | yes |
| Infinity | `inf` | only if `inf` is read as infinity |

Every value keeps its exact double, so nothing is lost. But the text is up to 38 digits of the double's exact binary
expansion, not 17 significant digits in exponent form as `Snowflake.floatFullText`'s javadoc says, and a `toCSV` cell
on Snowflake prints `0.1` as `1.0000000000000000555111512312578270212e-1`. Not ruled: whether that is acceptable as
"the database's own full-precision text", or the Snowflake arm should be another of its own expressions. It belongs
to the presentation work (the plan's step 8), with register S311's Snowflake note.

**What the lane found wrong or missing in the docs:** `docs/CLOUD_BACKENDS_PCT_2026_10_11.md` section 4 names the
key-password property two ways (`private_key_file_pwd` is the one that works) and does not list the secret's
`public_key` and `username`; the round's cost estimates are far above what was measured; on login the Snowflake
driver logs SEVERE 403s probing cloud metadata endpoints, which is noise.

## BigQuery (`session_0195rQEo31vtHdx1NMEUJvJr`)

| Run | Essential | Grammar | Relation | Standard | Unclassified |
|---|---|---|---|---|---|
| Channel A | 345/345 | 137/137 | 469/469 | 205/205 | 93/93 |
| Channel B | **FAILED:** PASS=317 ERROR=28, 16 not ledgered | PASS=136 ERROR=1 | PASS=457 ERROR=12 | PASS=198 ERROR=7 | **FAILED:** PASS=88 ERROR=5, 3 not ledgered |

**Channel B fails on 19 tests its ledger does not expect, with one cause.** Each is "FAILS, not ledgered", each passes
on channel A, and each message is BigQuery's `Column N in UNION ALL has incompatible types: INT64, STRING` (or the
reverse) at line 7 of the statement:
- Essential (16): `date::tests::testDateFromYear`, `testDateFromMonth`, `testDateFromHour`, `testDateFromMinute`,
  `testDateFromSubSecond`, `testDatePartYearMonthOnly`, `testDatePartYearOnly`, `testAdjustByYears`,
  `testAdjustByMonths`, `testAdjustByMicroseconds`, `testAdjustByMicrosecondsBigNumber`, `testAdjustByMilliseconds`,
  `testAdjustByMillisecondsBigNumber`, `string::tests::parseDate::testParseDate`, `testParseDateWithZ`,
  `testParseDateWithTimezone`;
- Unclassified (3): `flow::test::coalesce::coalesce_AllEmpty`, `coalesce2_AllEmpty`, `coalesce3_AllEmpty`.

**It is older than this branch: the same 16 and 3 tests fail on `main`'s head (`c04c30ec`), with the same messages**
(the lane's round 1b). The cause, from two dumped statements: channel B's batch `UNION ALL` writes a slot's NULL untyped
on one branch (a bare `NULL`, or `COALESCE(NULL, NULL)`), which BigQuery types INT64, against a STRING on the other;
where the slot is written `CAST(NULL AS STRING)` it unifies. It is the test harness's batch statement, not a product
query. Not fixed here: suggested to the user as a separate task (2026-11-25).

**The exact Float text on BigQuery** (`SqlFn.FLOAT_FULL_TEXT` is `CAST(x AS STRING)`), sent by hand since no test of
the embedded lanes runs here:

| Double | BigQuery's text | Reads back as the same double |
|---|---|---|
| 0.1 | `0.1` | yes |
| 1.0E-7 | `1e-07` | yes |
| 123456789.12345679 | `123456789.12345679` | yes |
| 1.7976931348623157E308 | `1.7976931348623157e+308` | yes |
| 4.9E-324 | `4.94065645841247e-324` | yes (15 digits, not the shortest form) |
| 0.30000000000000004 | `0.30000000000000004` | yes |
| 100.0 | `100` | yes, with no `.0` |
| NaN | `nan` | as a value; Java's parser refuses the spelling |
| Infinity | `inf` | the same |

Every finite value reads back exactly.

Timings: setup about 9 minutes, credentials under a minute, channel A (build and five suites) 378 s, channel B 151 s:
far under the 95 minutes a round was said to take.

**What the lane found wrong or missing in the docs:** `docs/CLOUD_BACKENDS_PCT_2026_10_11.md` never names BigQuery's
default dataset (`integration_dataset1` is the one that works); to use `bq` as the service account,
`CLOUDSDK_AUTH_ACCESS_TOKEN` must be unset; its section 6 is stale against `tools/cloud-distdir.sh`.

## MariaDB, MySQL and ClickHouse (`session_0195rQEjDbpHcLjorPDXJjG6`)

**Channel A:** all 15 suites pass (five each).

**Channel B:**

| Lane | Essential | Grammar | Relation | Standard | Unclassified |
|---|---|---|---|---|---|
| MariaDB | **FAIL** | **FAIL** | fails its ledger: 2 tests now pass | pass | pass |
| MySQL | pass | pass | fails its ledger: 21 tests now pass | pass | pass |
| ClickHouse | pass | pass | fails its ledger: 5 tests now pass | pass | pass |

**The three Relation suites are red for improvements only:** every line of their ledger blocks is "PASSES, ledgered as
failing (remove it from the ledger)". ClickHouse 5 (`testVariantColumn_filterOnKeyExtractionValue`,
`testVariantColumn_keyExtraction`, `testFlatten_LateralJoin_Nested`, `testFlatten_LateralJoin_Nested_Extend`,
`testVariantColumn_filterOnIsNotEmptyOfModelConversion`); MariaDB 2 (the two `testFlatten_LateralJoin_Nested*`);
MySQL 21 (16 `over::testRows_*` window-frame tests, four `extend::testOLAP*` tests, `lateral::testLateralJoin_Chained`).
The ledgers are stale; they shrink.

**MariaDB's Essential and Grammar fail for real, and differently on each run.** None is ledgered. Every failure is a
list returned in a rotated or shuffled order:
- `values::testValues`: expected `[1, 2]`, actual `[2, 1]`;
- `sort::testSimpleSort`: expected `["Doe", "Smith", "Branche"]`, actual `["Branche", "Doe", "Smith"]`;
- `range::testRange`: expected `[0, 1, 2, 3, 4]`, actual `[2, 3, 4, 0, 1]`.

Run 1: Essential 30 unledgered failures (PASS=306, ERROR=39), Grammar 4. Run 2 at the same commit: Essential 13
(PASS=323, ERROR=22), Grammar 4, a different set. So a list's order is not stable on MariaDB's channel B: it looks
like a list literal or an array materialised in an order the database does not keep. Channel A passes all of them.

This is the merged tree's baseline, so it is not from step 2. Asked of the lane (round 1b): the same suites on
`main`'s head, twice; MySQL a second time; and one dumped statement.

Timings: setup 1,551 s (it packed both upstream archives from git clones), build 75 s; each lane's channel A about
140 s and channel B about 42 s.

**What the lane suggests for the docs:** a channel B suite whose ledger block holds only "PASSES, ledgered as failing"
lines still fails its target, so a lane reads red for improvements; and a lane should run a suspect suite twice before
reporting.

### Round 1b (the same lane, 2026-11-25): the shuffle is on `main`, and its cause

**MariaDB's shuffled lists are older than this branch.** On `main` (`c04c30ec`) channel B's Essential and Grammar
fail the same way, and the set of failing tests moves between two runs there too. MySQL is stable: two runs at this
branch's commit both match (apart from the stale Relation ledger).

**The cause, from one dumped statement.** Channel B's batch statement gives a list's elements their position with
`ROW_NUMBER() OVER ()`, a window with no `ORDER BY`, over a `UNION ALL` derived table. MariaDB 11.4.13 sorts a
window's input with a filesort that does not keep the input's order, so the numbering is whatever order the sort
left. Run by hand in the lane's own server:

```sql
SELECT ROW_NUMBER() OVER () rn, value FROM (SELECT 1 AS value UNION ALL SELECT 2) b
-- answers (1, 2), (2, 1); with four rows the values come back rotated: 2, 3, 4, 1
```

SQL promises no order here, so MariaDB is within its rights; the statement is wrong to depend on it. **The fix's
direction:** each arm of the union carries an explicit ordinal and the window reads `OVER (ORDER BY ord)`. It is
channel B's harness statement, not the product's SQL, and not this program's change; it is a finding for `main`.

**The Relation ledgers, on `main`:** MySQL's and ClickHouse's Relation suites match their ledgers there, so the 21
(MySQL) and 5 (ClickHouse) "PASSES, ledgered as failing" lines are improvements that are new with this branch (the
merged B2 and B3 work), and their ledger rows leave with it. MariaDB's Relation on `main` fails a different pair
(`sort::testSimpleSortShared`, `testSimpleSort_MultipleExpressions`, an expected side rotated, the same shuffle), and
its two `testFlatten_LateralJoin_Nested*` improvements are new with this branch as well.

**What follows:** (1) the three stale Relation ledger blocks shrink in the step that lands this branch's work, with a
dated note; (2) the unordered window is posted as a defect on `main`, to fix outside this program.

## Round 2 (2026-11-26): steps 2 and 3 at `29f58b21`, against the baselines above

The claim measured: legend-engine's door changed (the table and the stamp in the old rewrite's place) and PCT's and
the corpus's doors still hand the product's own edition, so no PCT answer moves on any database.

| Lane | Verdict | How it compared |
|---|---|---|
| SQLite, MongoDB | nothing differs | channel B line by line (verdict and message of every test); channel A by counts (all pass in both) |
| BigQuery | nothing differs | channel A every testcase verdict; channel B all 1,249 lines with their messages; the whole logs equal but for the heap line. The 19 known failures (the untyped NULL, on `main`) are the same tests with the same messages |
| Snowflake | nothing differs | every per-test line of all ten suites, timings stripped |
| Databricks | nothing differs | every junit outcome and message and every channel B line against its round 1 candidate (`2cd0b2f9`); whole logs equal after normalising ids and timings |
| MariaDB, MySQL, ClickHouse | nothing differs on MySQL and ClickHouse; MariaDB moves only by its known list shuffle | test by test. ClickHouse channel A: four echoed results list the same rows in another order (unordered relations; the tests pass either way). MariaDB channel B: Essential 21 unledgered failures (30 at the baseline), Grammar the same 4, Relation one of the two sort tests seen on `main`, a different one on a rerun: all rotated lists, the window with no order |
| Postgres, Trino | (running when this was written) | it is measuring `c06ebbab` first, its stored logs being older |

The three stale Relation ledger blocks (MySQL 21, ClickHouse 5, MariaDB 2 "PASSES, ledgered as failing") are unchanged
and still to shrink.

The audit's fixes came after `29f58b21`; they change legend-engine's door alone, and are checked here by the edge
tests, the guards and a second full round.
