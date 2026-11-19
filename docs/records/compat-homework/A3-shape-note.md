# A3: the shape note (substring and indexOf to Pure's meaning, loud at pure/v1)

The translator session, 2026-11-19, on the compat owner's ask. A shape note only: **no build, no PR, no pin moved.**
Read from the code at `main` (`b75d6c73a`) and from this branch's H5 and H12 measurements (`H5-translator-spike.md`,
`H12-combined-dry-run.md`, their evidence under `h5/` and `h12/`). The user's ruling D1 (2026-11-19, ~13:25 UTC,
relayed): **"Pure's way, LOUD, no translator."**

**Revision 2 (2026-11-19, the compat owner's calls).**
- **Taken:** option (a) for PR1; FeatureRules javadoc-only, with `LEGACY_SQL_NULL_UNSAFE_EQUALS` and `withFeatures`
  untouched; the raise on legend-pure's three conditions, one register row (both on the user's list).
- **Added:**
  - the fixed order, A1, A2, A3, A4, with A3's corpus lines measured for it (section 2);
  - the reach into qualified properties and the runtime's mapping (section 3a);
  - the load-time report (section 3b);
  - H2's literal-0 exemption, measured (section 3);
  - the test-data routes (section 3);
  - the cross-kind gate (section 5).
- **Read against:** the final plan's §4a, `docs/records/compat-final-plan-2026-11-19.md` on `claude/compat-program`
  (`78a9a71e`).

## Counts first (revision 2)

| | count |
|---|---|
| PRs | **2**: PR1 is the frontends and the dynaFunction split; PR2 is the core switch, the detector and the load-time report |
| days | **about 6 to 6.5**: PR1 1.5; PR2 4.5 to 5. About half a day over the final plan's 5 to 6 |
| PR2's days, by part | the switch and guard moves 0.75; the raise 0.5 to 1; the PCT adapter's option off 0.25; the detector on 5 routes with the mapping reach 1.5; the report 1.5 |
| gate | the cross-kind `!=` fix lands before PR2 (not sized here) |
| lite tests that move, by name | **16 of the 17 named**: 9 old-meaning pins (PR2) and 7 text goldens (PR1). The IQ showcase's SQL is predicted not to move under option (a) |
| PCT known-failure rows that leave | **63**: S134's 3 tests on 11 channel A classes (33) and 10 channel B lanes (30) |
| edge net golden lines | **9**: 3 answer lines in each of `duckdb_essential_answers.tsv` and `sqlite_essential_answers.tsv`, and 3 outcome lines in `channel_b_duckdb_essential_outcomes.tsv` |
| ENGINE lane rows | **12**, only if PR #124 lands first: DuckDB essential 7, H2 essential 5, each BOTH_FAIL to LITE_FIXES or LITE_ANSWERS |
| corpus lines, A3 after A2 | **20, measured** (section 2). Each fail roster gains the same 7 tests on both databases: 4 answers and the 3 text tests the old `indexOf` hid. Each `*-unordered-register` loses 3 |
| corpus with the cross-kind fix landed | **the same 7 on DuckDB.** Its datetime test already fails under A2, before its `indexOf` assert |
| `NativeCatalogGovernanceTest` ceiling | **1**: `INTERNAL_DESUGAR` from 38 to 40 |
| register rows | **3**: S130 and S134 rewritten; one new row, the out-of-range `substring`, naming all three conditions |
| detector routes | **5**: generatePlan; execute, Arrow included; the three test-data routes that run a query. The census route is excluded: it runs nothing |
| load-time report | **once per model, off the answer's path.** About 4 s on the corpus world (about 8 s when compile triggers it). There it lists 48 calls and 83 unverified bodies (H5's V6 by proxy) |
| H2's literal 0 | **measured equal to Pure's** on H2 2.1.214 and 2.5.252, in both REGULAR and LEGACY mode, whenever Pure answers. Exempt on H2, 2- and 3-argument forms |
| DuckDB's literal 0 | the 2-argument form is equal too (measured); the 3-argument form is not |
| new core hooks | **0**: the detector and the report use only public API, inside the edge |
| pure/v1 net lines | **0**: the net records status and JSON only, and none of its 28 requests calls either function |

## 1. The slice plan

### The choice inside PR1: how the frontends say SQL's meaning

The frontends write SQL's meaning today: a 1-based start with a length, and a 1-based `indexOf` that answers 0 on a
miss. The frontends are `#Cypher`, `#SQL` (Postgres) and `#IQ`, with 47 call heads; the `###Relational` dynaFunctions
add 3 sites (`indexOf`, `substring`, `position`). H5's spike moved them onto Pure's `substring` and `indexOf` with a
shift, option (b) below. Option (a) is lite-internal natives that keep SQL's meaning verbatim.

| | (a) lite-internal natives, verbatim (**taken**) | (b) the spike's shift over Pure's functions |
|---|---|---|
| what a site writes | `meta::legend::lite::sqlSubstring(s, start1[, length])` and `meta::legend::lite::sqlPosition(s, t)`. Each lowers through one Scalars rule, to `SqlFn.SUBSTRING` and `SqlFn.STRPOS`, the same calls main's rules emit today | `substring(s, start-1[, start-1+length])` and `indexOf(s, t)+1`, literals folded (`SqlToPure.sqlSubstring` and `sqlIndexOf`, +33 lines) |
| lands alone, before the switch | **yes**: the answers and the SQL are identical before and after the switch | **no**: before the switch the core still reads `substring` as SQL's, so the shift is wrong. PR1 and PR2 become one PR |
| SQL | **byte-identical** (predicted; the PR's parity lanes confirm it) | moves, and is less lean: the spike's IQ showcase moved `CHARINDEX('l', vc)` to `CHARINDEX('l', vc) - 1 + 1` (`h5/parity/runB-...iq_parity_test.txt`) |
| a start of 0 or below, once the raise lands | answers as SQL does, as the source language means | becomes Pure's `substring(s, -1)` and **raises** where Cypher, Postgres or IQ answer |
| the pure/v1 detector | never flags an island's code (it is a different function) | flags every `#Cypher`, `#SQL` or `#IQ` island's generated `substring`: false positives it cannot tell apart |
| guard cost | the `INTERNAL_DESUGAR` ceiling, 38 to 40 (2 bare names) | none |

The name `sqlPosition` is chosen over `sqlIndexOf` because the native's meaning is SQL `POSITION`'s: 1-based, 0 on a
miss. "indexOf" names Pure's meaning.

### PR1: the frontends and the dynaFunction split (about 1.5 days)

| file at main | change | lines (spike, under (b)) |
|---|---|---|
| `builtin/Pure.java` (`Lite` constants, `INTERNAL_DESUGAR`) and the native declarations | 2 natives, signatures verified as every lite-internal native is (`everyLiteInternalNativeIsNamedAndVerified`) | (a) only, about 20 |
| `lowering/Scalars.java` | 2 rules: `sqlSubstring` is `SqlFn.SUBSTRING` (args verbatim), `sqlPosition` is `SqlFn.STRPOS` | (a) only, about 10 |
| `protocol/cypher/CypherToPure.java` | 18 call lines retargeted: 466, 491, 7988, 11674, 11677, 13123, 14455, 14469, 14471, 14472, 14480, 14774, 14793, 17781, 17817, 17833, 17881, 18964 | +18/−18 |
| `protocol/sql/PostgresFunctions.java` | 10 lines: 212, 458, 477, 692, 787-789, 908, 911, 915 | +10/−10 |
| `protocol/sql/iq/IqFunctions.java`, `IqAggregates.java` | 13 lines (667, 910, 1512, 1515, 1541, 1580, 1993, 1998, 2008, 2151, 2194, 3241, 4593) and 1 (1489) | +14/−14 |
| `protocol/sql/SqlToPure.java` | (b)'s two helpers; under (a), two one-line builders of the native calls | +33 (b), about +10 (a) |
| `normalizer/RelOpTranslator.java` | the dynaFunctions' `indexOf` (`:451-457`), `substring` (`:469-473`) and `position` (`:497-505`) | +49/−5 (b); about +15/−5 (a), with no shift arithmetic |
| `NativeCatalogGovernanceTest.java:177` | the ceiling, 38 to 40, with its dated note (section 2) | (a) only |
| the 8 goldens (section 2) | re-blessed; each is a reviewed diff | |

**Gate for PR1:** `//core:cypher_parity_test`, `//core:iq_parity_test`, `//core:sql_parity_test` and
`//core:core_tests` stay green with no answer moving (H5's Run B, under (b): Cypher 4,267 pass/0 fail, IQ 5,984/19
fail (the 19 also fail at main; their ledger test passes), Postgres 1,651/0). Under (a), the same lanes are predicted
unchanged at main's counts.

### PR2: the core switch, the detector, the report and the guard moves (about 4.5 to 5 days)

**Gate first:** the product's cross-kind `!=` fix (`63421e0a` on `claude/hosts-followups`) lands before PR2.

| file at main | change | lines |
|---|---|---|
| `pct/.../extension/PctPlanRoute.java:54`, `pct/.../channelb/ChannelB.java:393` | lite's PCT adapter stops setting `CORRECT_SQL_SUBSTRING_INDEXING` (both channels). It was set on every query since 2026-09-12, so PCT never saw the old `substring`; upstream's adapter sets none. PCT is predicted unchanged without it, since `substring` is Pure's by default. The edge's `substring` answers do not move | about −4, comments rewritten |
| `lowering/Scalars.java:1489-1497` | `AT_STRING_SUBSTRING`'s plain rule becomes `FeatureRules.CORRECTED_SUBSTRING`, as `substr`'s already is (`:1504-1506`) | spike +20/−26 for both functions |
| `lowering/Scalars.java:1507-1563` | String `indexOf`: `STRPOS(args) - 1`. 3-argument: from clamped to 0, searched from `from+1`, −1 on a miss (Pure's, as Java's `String.indexOf(String, int)`). The list `indexOf` is unchanged | (same) |
| the out-of-range raise | `substring` with a start below 0, an end past the length, or an end before the start raises in the database (a `CASE` raising lite's text, the plain set's pattern), as legend-pure raises (section 2's new row) | about +40 and a test, 0.5 to 1 day |
| `edge/PureMeaningCheck.java` (new), `edge/PureV1Routes.java`, `edge/TestDataRoutes.java`, `edge/EdgeServer.java` | the detector on 5 routes, with the mapping reach (sections 3, 3a) | about 220, tests about 250 |
| `edge/PureMeaningReport.java` (new), `edge/EdgeServer.java` | the load-time report and its endpoint (section 3b) | about 330, tests about 300 |
| `lowering/FeatureRules.java` | **javadoc only** (section 4) | about 5 |
| the 9 pins, 63 PCT rows, 9 edge lines, 31 corpus lines, S130, S134, the new row (and 12 ENGINE-lane rows if #124 is in) | section 2 | |

**Gate for PR2:** `//gates:local`, then one full CI run on the branch, because every PCT lane moves: the other 9
databases were measured nowhere but by CI.

**Order:** PR1 before PR2. Without PR1, Run A measured about 1,240 broken parity cases: Cypher 321 fail and 406 error,
IQ 212 and 18, Postgres 27.

## 2. Every lite test and guarded line that moves, with the note each would carry

Every note below waits for the user's word. The notes are drafts for the user: none is written in a file here.

### The 9 old-meaning pins (PR2)

Each asserts legend-engine's relational answer for a Pure call. Each flips to Pure's answer, under one note:
*"2026-11-?? A3 (the user's D1, 2026-11-19: "Pure's way, LOUD, no translator"): Pure's 0-based `substring` and
`indexOf` (−1 on a miss) in the product; legend-engine's relational answer was the old pin."*

| test | file | what it pinned, and what it becomes |
|---|---|---|
| `TypeInferenceIntegrationTest.testSubstringStart` | `core/src/test/java/com/legend/integration/` (in `//core:core_edge_tests`) | the 1-based start, now Pure's 0-based |
| `TypeInferenceIntegrationTest.testStringIndexOf` | (same) | 1-based, now 0-based |
| `TypeInferenceIntegrationTest.testStringIndexOfSimple` | (same) | (same) |
| `TypeInferenceIntegrationTest.testStringIndexOfFromIndex` | (same) | the 3-argument form's 1-based from and result, now Java's |
| `TypeInferenceIntegrationTest.testIndexOfOneElement` | (same) | (same as `testStringIndexOf`) |
| `TypeInferenceIntegrationTest.testSortWithKeyFunction` | (same) | a key built with `substring`: the order follows Pure's slice |
| `ExtendCheckerTest$StringExtend.testIndexOf` | `core/src/test/java/com/legend/integration/ExtendCheckerTest.java:714` | 1-based, now 0-based |
| `ValueSortComparatorTest.keyFunctionSort` | `core/src/test/java/com/legend/lowering/ValueSortComparatorTest.java:51` | a substring key, now Pure's |
| `SubstrIndexingTest.substrAlwaysCorrected` | `core/src/test/java/com/legend/SubstrIndexingTest.java:67` | "substring passes its indexes straight to SQL by default", now "substring is corrected with or without the flag; substr always" |

### The 8 text goldens (PR1)

Each compares the text a frontend writes. Under (a) a site's call head changes from `substring(x, 2)` to
`meta::legend::lite::sqlSubstring(x, 2)`; under (b) its index shifts (`substring(x, 1)`). One note:
*"2026-11-?? A3 PR1 (D1, 2026-11-19): the frontends say SQL's substring and position by name
(`meta::legend::lite::sqlSubstring`, `sqlPosition`), so Pure's `substring` and `indexOf` can take Pure's meaning; no
answer moves (the parity lanes)."*

| test | file |
|---|---|
| `CypherToPureTest.aStoredZonedValueIsItsInstantWithItsOffsetSlotAsItsZone` | `core/src/test/java/com/legend/protocol/cypher/CypherToPureTest.java` |
| `CypherToPureTest.keysOfANodeAreTheNamesOfItsPropertiesThatHoldAValue` | (same) |
| `CypherToPureTest.aMarkedDateTimeIsANanosecondTimeOfDayOrDateTime` | (same) |
| `CypherToPureTest.aPatternComprehensionIsANavigationFromTheBoundNode` | (same), `:1184` |
| `PostgresToPureTest.theLongTailOfTextAndNumbersIsPostgresDefinitionStepByStep` | `core/src/test/java/com/legend/protocol/sql/PostgresToPureTest.java:547` |
| `IqFunctionsTest.substringReadsAnsiVirtualPositions` | `core/src/test/java/com/legend/protocol/sql/iq/IqFunctionsTest.java:192` |
| `CypherParityTest.theShowcasePlansItsPinnedSql` | `core/src/test/resources/cypher-parity/showcase/` (`hard/pure.txt` moves under both options; the SQL files only under (b)), and `docs/CYPHER_SHOWCASE.md` as its update message says |
| `IqParityTest.theShowcasePlansItsPinnedSql` | `core/src/test/resources/iq-parity/showcase/{duckdb,snowflake}.sql`: **moves under (b) only** (the showcase pins SQL alone) |

### S134's PCT rows: 3 tests, 63 rows leave (PR2)

The 3 tests are `meta::pure::functions::string::tests::indexOf::testSimple` (expected 4, actual 5),
`...::indexOf::testFromIndex` (expected 1, actual 2) and `meta::pure::functions::collection::tests::indexof::
testIndexOfOneElement` (expected 0, actual 1). Under the switch each answers PCT's expected value (H12, DuckDB, both
channels).

- **Channel A:** the `one(...)` rows in 11 classes, `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_
  {,BigQuery_,ClickHouse_,Databricks_,MariaDB_,MongoDB_,MySQL_,Postgres_,SQLite_,Snowflake_,Trino_}
  EssentialFunctions_PCT.java`. In DuckDB's class (`Test_LegendLite_EssentialFunctions_PCT.java:86-105`) the comment
  block above them goes too.
- **Channel B:** `pct/src/test/resources/org/finos/legend/lite/pct/channelb/ledger.tsv`, 3 rows on each of 10 lanes:
  duckdb, postgres, sqlite, trino, mariadb, mysql, clickhouse, snowflake, bigquery, databricks. Each lane's comment
  line goes with its rows ("Pure's indexOf is 0-based, the lowering answers 1-based").
- **Why they must leave in the same commit:** the ledger's own rule (its header) is that "a test that starts passing
  leaves". A row whose test passes is a mismatch.
- **Note for each:** *"2026-11-?? A3 (D1): left; Pure's 0-based indexOf is the product's (S134 rewritten); measured
  passing on DuckDB in H12; the other lanes measured by the PR's CI."*
- **Measured only on DuckDB.** The other 10 channel A classes and 9 channel B lanes are predicted, from the identical
  ledgered texts.

### The edge net (PR2)

Re-recorded by `bazel run //pct:update_edge_contract`, citing D1. These lines are read from the pins and predicted;
H12 did not run `//pct:edge_*`.

- `pct/src/test/resources/edge/duckdb_essential_answers.tsv` and `sqlite_essential_answers.tsv`, 3 lines each:
  - `testFromIndex` from `Long:2` to `Long:1`;
  - `testIndexOfOneElement` from `Long:1` to `Long:0`;
  - `testSimple` case 007 from `Long:5` to `Long:4`.
- `channel_b_duckdb_essential_outcomes.tsv`: the 3 rows from `ERROR` to `PASS`.
- `duckdb_essential_outcomes.tsv` and `sqlite_essential_outcomes.tsv` stay `SUCCESSFUL`, provided the class's
  `one(...)` rows leave in the same commit.
- `substring::testStart` and `testStartEnd` do not move: the edge's PCT already runs under
  `CORRECT_SQL_SUBSTRING_INDEXING` (S130, `PctPlanRoute.java:49`).

### The ENGINE lane (PR2, only if PR #124 is on main first)

`pct/engine_lane/duckdb_essential.tsv` has 7 rows: `indexOf` `testSimple`, `testFromIndex` and `testIndexOfOneElement`;
`substring` `testStart` and `testStartEnd`; `sort` `testSimpleSortWithKey` and `testSimpleSortWithFunctionVariables`.
`pct/engine_lane/h2_essential.tsv` has 5: the same less the two sorts, which lite refuses on H2 before and after.

Each row goes from `BOTH_FAIL` to `LITE_FIXES`, or `LITE_ANSWERS` for `testFromIndex`, which legend-engine refuses.
H12 measured them under (1) alone. Note: *"2026-11-?? A3 (D1): lite answers PCT's expected value where legend-engine's
manifest marks a DEFECT or a refusal (E-R2)."*

### The corpus (PR2), in the fixed order: A3 lands after A2

**Measured for that order** (`a3/corpus-a3-after-a2.txt`, the scripts beside it):
- **Y, A2 alone:** main's `Scalars.java` and `RelOpTranslator.java`, with lite's own SQL text in place of
  legend-engine's (`-Dh5.off=sqltext`, B2's probe). It reproduces B2's counts: H2 423 failing, DuckDB 260.
- **X, A2 and A3:** this branch's core, `-Dh5.off=sqltext`. H2 430, DuckDB 267.
- X is H12's attribution exactly: `main` plus (2), (1)+(2), (2)+(3/4), (1) and the 3 interactions.

**`spec/src/test/resources/rcorpus/h2-fail-roster.txt` and `duckdb-fail-roster.txt`, +7 each: the same 7 on both.**
- **4 answers:** legend-engine's SQL meaning in a query lambda, which D1 declines to translate:
  - `meta::relational::tests::functions::sqlstring::testToSQLStringSubstringIsUncorrectedByDefault`;
  - `meta::relational::tests::query::function::testFilterUsingSubstringFunction`;
  - `...::testFilterUsingParseIntegerFunction`;
  - `...::testFilterUsingParseDecimalFunction`.
- **3 text tests the old `indexOf` hid:** `meta::pure::executionPlan::tests::datetime::
  testPlanForDateTimeConstantParameter{NoTimeZone,GMTTimeZone,ESTTimeZone}` (H12's interaction). Their
  `indexOf(...) != -1` assert is vacuous under the old meaning and false under Pure's, against lite's own plan text.

**`h2-unordered-register.txt` and `duckdb-unordered-register.txt`, −3 each:**
- the three `testFilterUsing...Function` tests leave: under A2 they pass by answer through the appeal, under A3 they
  fail;
- nothing joins.

**Unchanged:** the skipped rosters and the engine-order registers.

**Not A3's in this order:**
- **The 4 legend-engine SQL-text tests** that H5's V2 counted against A3 alone (`testModelConnectionAgg`,
  `testModelConnectionMultipleAgg`, `testSubstringIndexingCorrectedByFeatureFlag`, `testToSQLStringWithPosition`)
  already fail under A2.
- **H5's appeal lines** (`propertyfunc` 4, `sqlFunction::indexOf::testProject`, `testToSQLStringSubstringIs...`) are
  already in A2's registers.
- **DuckDB's `testPlanForDateTimeVariableESTTimeZone`** fails under A2 already, before its `indexOf` assert: it is B2's
  class a3, "plan parameter 'dt' has no type and is not typed by its value"
  (`census-b2-sqltext-probe-2026-11-19.tsv:162`). It moves in A2, not A3.

Note: *"2026-11-?? A3 (D1): Pure's substring and indexOf in the core, no translator; measured after A2 (`a3/corpus-a3-after-a2.txt`): 7 tests, 4 answers and the 3 text tests the old indexOf hid."*

### `NativeCatalogGovernanceTest` (PR1, option (a) only)

`core/src/test/java/com/legend/builtin/NativeCatalogGovernanceTest.java:177`, `INTERNAL_DESUGAR.size() <= 38` becomes
`<= 40`. The draft note in the file's own style:

```
// 38 -> 40 (A3 PR1, 2026-11-??, the user's D1 of 2026-11-19 "Pure's way, LOUD, no translator", and the user's word
// on this ceiling): sqlSubstring and sqlPosition, which the #Cypher, #SQL and #IQ frontends and the ###Relational
// dynaFunctions write for SQL's 1-based substring and position, so that Pure's substring and indexOf take Pure's
// meaning (register S130, S134)
```

### The register (`docs/SEMANTICS_REGISTER.md`, PR2)

- **S134 rewritten.** It becomes an outcome row:
  - **Upstream:** legend-pure's `indexOf` is 0-based with −1 on a miss. legend-engine's relational lowering is 1-based
    with 0 on a miss, `locate`, with no flag (`pureToSQLQuery.pure:10644`, read in H5).
  - **Lite:** legend-pure's on every road. pure/v1 marks a request whose store-routed code calls the 2-argument
    String `indexOf` (the detector), and refuses it under `LEGEND_LITE_PURE_MEANING=refuse`.
  - **Ruled:** the user's D1, 2026-11-19, which replaces the 2026-11-05 "it is correct that we fail right now".
  - **Evidence:** H12, the 63 PCT rows left.
- **S130 rewritten.** `CORRECT_SQL_SUBSTRING_INDEXING` becomes a no-op: `substring` is Pure's with or without it.
  The corpus runs Pure's meaning too. The `substring` calls in store-routed code at pure/v1 are marked or refused
  likewise, and quiet under the flag, where legend-engine answers Pure's too. Ruled by D1.
- **New row: the out-of-range `substring`.**
  - **Upstream:** legend-pure raises Java's `StringIndexOutOfBoundsException` for a start below 0, an end past the
    length, or an end before the start: `String.substring(start[, end])`, interpreted at `SubString.java:56,61` and
    compiled at `CompiledSupport.java:1546-1556`, read at the pin. legend-engine passes the indexes to SQL unchecked
    (with the flag, `start+1` and `end-start`).
  - **Lite:** raises in the database, as legend-pure does. The message's words are read from the JDK's text at the
    slice, not recalled here.
  - **Ruled by the compat owner (revision 2), on the user's list:** all three conditions, in one row.
- **The PCT adapter's option (S130 and the 2026-11-07 ruling):**
  - reversing the 2026-09-12 written deviation, the adapter setting `CORRECT_SQL_SUBSTRING_INDEXING`;
  - the user's 2026-11-07 "our tests should use the option", as far as it covers this flag. The same comment in
    `PctPlanRoute` names `DETERMINISTIC_AS_OF`, which stays.
  - Both on the user's list.

## 3. The detector

### Where it runs

At the edge alone, in `PureV1Routes`:
- `generatePlan` (`:262-292`);
- `execute` (`:302-323`), whose `ARROW_IPC` form is `executeArrow` (`:334-357`), the same route.

`lambdaReturnType`, `lambdaRelationType`, `compile` and the grammar routes compute no value, so nothing differs
there.

**Added (revision 2): the test-data routes that run a query.** `generateSeedData`,
`generateTestData_WithSeed` and `generateTestData_WithDefaultSeed` each type the request's query at
`TestDataRoutes.doored` (`:393-397`, `Compiler.query(ctx, r.query())`), and the check reads that `TypedQuery`. Their
mapping comes from the request's own `mapping` field (`r.mapping()`), which the mapping reach takes (section 3a). The
census route (`DONOTUSE_generateTestData`, `:348`) is left out: "a model fact: nothing runs".

So the detector sits on **5 routes**.

### What it reads, all public

| it reads | through |
|---|---|
| the query's typed statements | `TypedQuery.body()` (Phase G output, before G½) |
| each call's callee | `TypedNativeCall.callee().id()` and `qualifiedName()` |
| each call's position | `TypedNativeCall.pos()`, the call name's `SourceInfo` when the request carries `sourceInformation` |
| a user function's body | `TypedUserCall.callee()` and `query.specs().compile(callee).body()`, memoized by `FunctionId`, cycle-safe. This is `StatementEffects.reaches`' descent, but it records positions, and it never scores an un-typeable callee as clean |
| the corrected option | `TypedFrom.context().features()` (the execute call's context argument, already folded by `ExecutionContext`) and `ExecutionContext.treeFeatures(query.body())` (`withFeatureFlags` in the body). These are the two carriers the lowering reads |

**No core change is needed.** ArchUnit's `PLANNER_ENTRIES_ONLY` covers `com.legend.(planner|testplan|lowering)` only
(`ArchitectureTest.java:1638-1640`), and the edge already imports `compiler.spec.typed`.

### What it flags

A call of `meta::pure::functions::string::substring` (the 2- and 3-argument String overloads, `Pure.AT_STRING_SUBSTRING`)
or of the 2-argument String `indexOf`, when the call is **store-routed**, meaning that legend-engine would send it to
the database (H5 F1, meaning by location). It does not flag:
- the list `indexOf`, the same meaning in both;
- `substr`, which legend-engine always corrects;
- the 3-argument `indexOf`, which legend-engine refuses (the `testFromIndex` manifest row);
- `meta::legend::lite::sqlSubstring` and `sqlPosition`, the frontends' natives;
- a dynaFunction. A dynaFunction lives in a mapping, which Phase G's tree does not reach (the store resolver does, in
  phase H), and under (a) it writes the verbatim natives anyway.

**The store-routed rule:** a call inside a statement whose chain reads a store (a `getAll`, a `#>{db...}#` accessor,
a table reference), including the lambdas that chain's functions take and the user functions it calls. A statement
that reads no store is host-side and not flagged: for example, `let x = 'abc'->substring(1)` before the query.

**Host-side must be confirmed before the slice is built.** The rule rests on legend-engine planning a store-free `let`
as a platform node (`PureExpressionPlatformExecutionNode`, `core_java_platform_binding/.../platform.pure`), which is
legend-pure's meaning. That is not yet read. At the slice it is asked of the engine runner: the plan of
`|let x = 'abc'->substring(1); ...`. The host-side test follows the answer.

### Quiet under the corrected option

With `CORRECT_SQL_SUBSTRING_INDEXING` set on either carrier, `substring` is quiet: legend-engine's processSubstring
answers Pure's meaning then (FeatureRules' javadoc). The 2-argument `indexOf` stays flagged, because legend-engine has
no flag for it (`indexOf` is always `locate`, `pureToSQLQuery.pure:10644`).

This differs from the brief's "quiet under the corrected option", and is posted for the user's word. The final plan's
§4a now says the same: "`indexOf` always warns".

### H2's literal 0: measured, and exempt (revision 2)

**The question:** does legend-engine's `substring(s, 0, n)` or `substring(s, 0)` answer what legend-pure does? This is
the commonest prefix idiom. legend-engine writes both verbatim, on H2 (`h2Extension2_1_214.pure:262`,
`substring%s` over the arguments) and on DuckDB (`duckdbExtension.pure:277`).

**Measured** (`a3/h2-literal-zero.tsv`, `a3/duckdb-literal-zero.tsv`, probes beside them):

| expression | legend-pure | H2 2.1.214 and 2.5.252, REGULAR and LEGACY | DuckDB 1.5.6 |
|---|---|---|---|
| `substring('abcdef', 0, 3)` | `'abc'` | `'abc'` | **`'ab'`** |
| `substring('abcdef', 0, 0)`, `(…, 0, 1)`, `(…, 0, 6)` | `''`, `'a'`, `'abcdef'` | the same | **`''`, `''`, `'abcde'`** |
| `substring('abcdef', 0, 9)` (end past the length) | **raises** | `'abcdef'` | `'abcdef'` |
| `substring('abcdef', 0)` | `'abcdef'` | `'abcdef'` | `'abcdef'` |
| `substring('', 0, 2)`, `substring('', 0)` | raises, `''` | `''`, `''` | `''`, `''` |

**H2 reads a start of 0 as position 1** (all four combinations measured), so legend-engine's answer is legend-pure's
whenever legend-pure answers. Where it does not (an end past the length), H2 clamps and legend-pure raises. After A3
lite raises too, so that difference is loud on its own, not a silent answer.

**The exemption:** on H2, a `substring` whose start is the literal `0` is not flagged, for both the 2- and 3-argument
forms. The database type comes from the request's runtime connection, the one `connectionOf` reads
(`PureV1Routes.java:519`).

**DuckDB (for the user's word):**
- **The 3-argument form differs** (`'ab'` for `'abc'`), so it stays flagged, as §4a says.
- **The 2-argument `substring(s, 0)` is equal** (the whole string). A second exemption is possible: a literal 0 in the
  2-argument form on DuckDB. It goes beyond §4a's "not on DuckDB" and is posted, not assumed.

### The header, as §4a words it (revision 2)

The header carries **a code and a count, never the query's text, and is bounded by construction**:
`x-legend-lite-pure-meaning: substring/3=2, indexOf/2=1, unverified=1`. There are at most four codes:
`substring/2`, `substring/3`, `indexOf/2` and `unverified`. Positions go to the log line only.

### What it does

The check is data: `PureMeaningCheck.of(TypedQuery)` returns the flagged sites and the unverified ones. It never
changes the query: the route plans and runs the `TypedQuery` it already has, and adds a header or throws.

- **`warn`, the default:**
  - The answer carries the header **`x-legend-lite-pure-meaning`**, with a code and a count per kind (the section
    above), for example `substring/3=2, indexOf/2=1, unverified=1`. `unverified` counts what could not be followed.
  - The answer also carries `Access-Control-Expose-Headers: x-legend-lite-pure-meaning`, so a browser client can read
    it. The server writes no expose list (`LegendHttpServer.java:452-455`), and the edge's answer carries it, so no
    product change is needed.
  - Arrow's answer carries it beside `x-legend-response-format: FormatNotSet`.
  - The server writes **one log line** on `System.err`, in the server's `WARNING:` style:

    ```
    WARNING: pure/v1 execute: substring/3 at 1:34 (the query), indexOf/2 at 12:9 (in my::helper; positions when the
    request carried sourceInformation, else the element) answer legend-pure's
    meaning (0-based; -1 on a miss), not legend-engine's relational SQL's (1-based; 0 on a miss): register S130, S134;
    LEGEND_LITE_PURE_MEANING=refuse refuses such a query
    ```

    It holds no query text, model text or data.
- **`refuse`:** the route throws `NotImplementedException` with the same sentence. The route's envelope answers it as
  every named refusal: 500, errorType `COMPILATION`, legend-engine's error body (`EngineEnvelope.answer`). `unverified`
  is refused too: a loud mode is loud about not knowing.
- **The setting:** **`LEGEND_LITE_PURE_MEANING`** = `warn` | `refuse`; unset means `warn`. Any other value stops the
  server's start (`ERROR: LEGEND_LITE_PURE_MEANING=... is neither warn nor refuse`), so there is no fallback.
  - It is read once, at the edge's composition root, `EdgeServer.UPSTREAM`, as `LegendHttpServer` reads its
    `LEGEND_LITE_*` settings (`:681-697`).
  - It is passed to `pureV1(runner, meaning)` and on to `PureV1Routes.route(..., meaning)`.
  - The product server is untouched.

### H5's 40 unfollowable calls, at this door

In the corpus, 40 routed calls (`execute`, `executionPlan`, `toSQLString` in test bodies) had a first argument that
the syntactic search could not follow (`$t.query`, a function call). At pure/v1 that problem is gone, because the
request's lambda is the routed query itself.

What remains unfollowable:
- a function value whose body is not in the tree: a lambda read from an instance's property, a function a function
  returns, an `eval` of an unbound value;
- a user function whose body does not type;
- a query over an M2M (`Pure`) class mapping chained on a store (`ExecutionContext.chainMappings()` non-empty), where
  legend-engine may push the M2M transform into SQL. To be read at the slice.

Each is reported **`unverified`**, by name in the log line, and never counted clean. In practice the planner must see
any body it lowers (G½), so an `unverified` site the planner then plans is a detector bug, and the test net plants one
to keep the two in step.

### Tests (`core/src/test/java/com/legend/edge/PureMeaningCheckTest.java`, in `//core:core_edge_tests`)

1. **A planted request is warned.** `execute` and `generatePlan` with `Person.all()->project(~[n: p|$p.name->
   substring(1, 3), i: p|$p.name->indexOf('o')])`: 200, the header names both calls, one log line. The body equals
   the same request's body through the check-free `generatePlan(String)` overload, so the query is not changed.
2. **The corrected option is quiet for `substring`.** The same request under `->from(..., ^ExecutionOptionContext
   (... CORRECT_SQL_SUBSTRING_INDEXING))` and again under `withFeatureFlags`: no `substring` item, and `indexOf/2`
   still listed.
3. **A dynaFunction is not flagged.** A `###Relational` mapping maps `name: substring(PERSON.NAME, 1, 3)`, and the
   query projects `$p.name`: no header.
4. **A host-side call is not flagged.** `|let x = 'abc'->substring(1); Person.all()->filter(p|$p.name == $x)->
   project(...)`: no header. This is subject to the engine runner's answer (section 3).
5. **`refuse`.** The planted request answers 500 `COMPILATION`, with a message naming the calls and the setting.
6. **`unverified`.** A query over an M2M chain on a store: the header carries `unverified`, and `refuse` refuses it.
7. **Arrow.** The header sits beside `x-legend-response-format`.
8. **A frontend island is not flagged** (option (a)). A `#SQL{...}#` source whose SQL calls `substring`: no header.
9. **The test-data routes.** `generateSeedData` with the planted query is warned; the census route never is.
10. **H2's literal 0.** `substring($p.name, 0, 3)` on an H2 runtime: not flagged. The same on DuckDB: flagged.
    `substring($p.name, 1, 3)` on H2: flagged.
11. **The mapping reach** (section 3a). An M2M transform and a constraint each calling `substring`, read through the
    query: flagged with their kinds; a `###Relational` property mapping's `substring`: not.
12. **The header's value is bounded.** A query with 50 flagged calls carries the four codes and their counts, under a
    fixed length.
13. **Clients ignore the header** (§4a).
    - DataCube's engine mode (lite's `datacube/` client) answers the same with the header present.
    - Studio's fetch layer reads the body and the content type only. That is read at the slice; no Studio runs here.

The pure/v1 net (`PureV1EdgeGoldenTest`) does not move: it records `status` and JSON only (`:91`), and none of its 28
requests calls either function.

## 3a. The reach (the final plan's §4a): what the query door's walk covers, and what it adds

| reached from the request's query | covered by the first design? | how |
|---|---|---|
| the query's own statements | yes | `TypedQuery.body()` |
| user functions the query calls, transitively | yes | `TypedUserCall.callee()`, `query.specs().compile(callee).body()` |
| qualified (derived) properties the query reads | **yes, already** | the typer types a qualified-property call as a call of its lifted body, `<class>$prop$<name>` (`Typer.java:555-592`: `Calls.exact(d.bodyFunctionFqn(), ...)`), so the walk follows it as a user call |
| the runtime's mapping: class-mapping transforms (`~src`, M2M) | **added** | phase E lifts every mapping transform into a function, `<mapping>$class$<classFqn>` (`SynthHat.CLASS`; `ModelNormalizer` javadoc, `SynthHat.java:22-41`). The query's typed tree does not reach them (the store resolver does, in phase H). The walk adds, for each mapping the query names (each `TypedFrom.context().mapping()` and its chain mappings; the request's own `mapping` field on the test-data routes) and each class the query's typed tree names (a node's `ExprType` class), that mapping's lifted function for the class, and for associations navigated `<mapping>$assoc$<assocFqn>` (`SynthHat.ASSOC`), through `ctx.functionFqns()` and `findFunction`: public |
| constraints on the classes the query reads | **added** | `<class>$constraint$<name>` (`SynthHat.CONSTRAINT`), the same way |
| `###Relational` property mappings and filters | not flagged, by construction | they are relational class mappings' lifted bodies too, but after PR1 they call `sqlSubstring` and `sqlPosition`, never Pure's functions |

**What stays to read at the slice.** Whether legend-engine runs a chained M2M transform or a constraint in Java
(legend-pure's meaning, where lite's answer after A3 is the same) or in SQL. §4a says flag them, and the design does,
each item tagged `mapping` or `constraint` in the log line; if the read shows legend-engine runs one in Java, that flag
is a false alarm and the kind turns quiet. The constraints that reach SQL in legend-engine are named by that read.

## 3b. The load-time report

**Its question differs from the detector's.** The detector asks, per request, where lite's answer differs from
legend-engine's: only store-routed calls. The report asks, per model, where an answer changes at A3 at all. Lite runs
every call in the database, and lite's own meaning was SQL's until A3, so every String `substring` (2 or 3
arguments) and 2-argument String `indexOf` lite lowers changes its answer: in a function, a qualified property, a
mapping transform, a constraint, a service query or a saved query. The report lists them all, each with its kind; the
store-routed ones are marked.

### Where it runs

At the edge only; nothing in the product server.

1. **On legend-engine's `compilation/compile`** (`PureV1Routes.compile`, `:188`): Studio compiles a project there when
   it loads one and at each edit. The route already types every body (`Compiler.compileErrors`, eager G).
2. **On the first execute, generate-plan or test-data request** for a model the edge has not reported, for clients that
   never call compile (legend-query, DataCube in engine mode).
3. **On demand**, for any model: `POST /api/compat/v1/pure-meaning` with the model; lite's own clients (Python, the
   page, lite's API) reach it this way, since the product server holds no hook for it (G5; adding one would be a new
   hook in lite's core, which this note does not design).

It runs **once per distinct model**, keyed by the model's hash as `HeldModels` keys a model, in a bounded set (1,024
models, least recently used out). It runs on one background thread after the request's answer is written: it never
delays an answer and never changes one.

### What it scans

- **The model's bodies, all through public calls:** the loop `Compiler.compileAllBodies` runs (`Compiler.java:1001-1035`:
  `ctx.functionFqns()`, `findFunction`, `new SpecCompiler(ctx).compile(tf)`), repeated at the edge, since
  `compileAllBodies` returns only the walls and keeps no typed body. Phase E has lifted every body site into a
  function, so the loop covers functions, qualified properties (`$prop$`), mapping transforms (`$class$`, `$assoc$`),
  constraints and their messages, service queries, defaults, pre- and post-conditions (`SynthHat`).
- **A pre-filter by name first:** only a body whose parsed text names `substring` or `indexOf` is typed. A call site
  lives in exactly one body, so this is exact for a census.
- **The saved queries:** the store is one (`SavedQueries`, mounted at both `/api/pure/v1/query` and
  `/api/lite/v1/queries`), read through `search` and `get`. A saved query holds its lambda's text, its mapping and
  runtime, and its project coordinates; **not its model**. So:
  - **typed**, when the report runs for a model that defines the saved query's mapping: the lambda is typed against
    that model (`Compiler.query`) and walked as the detector walks a request, with its id and version;
  - **untyped**, at the server's start, for every saved query: its lambda parsed (no model needed) and every call
    named `substring` (2 or 3 arguments) or `indexOf` (2 arguments) listed as a candidate. Untyped, a list `indexOf`
    cannot be told apart, and user functions are not followed.
  - A store lite cannot see stays out of reach: legend-engine's own query store in a deployment lite does not serve,
    and queries a client keeps itself.

### What it emits

- **A log block**, once per model, in the server's `WARNING:` style:
  ```
  WARNING: pure-meaning report, model 3f9c2a1e (11168 elements): 48 calls answer legend-pure's meaning since A3
    (substring/3 28, substring/2 4, indexOf/2 16), 83 bodies unverified; register S130, S134
    function  my::helper          substring/3 at 12:9   store-routed
    property  my::Person$prop$x   indexOf/2   at 40:17
    mapping   my::M$class$my::P   substring/2 at 7:31
    ... (+N more: GET /api/compat/v1/pure-meaning)
  ```
  No query text, no model text, no data; at most 200 site lines.
- **An endpoint:** `GET /api/compat/v1/pure-meaning` answers the reports held (per model: its hash, when, counts and
  every site: kind, element, call, line and column, store-routed or not, unverified with its reason) and the saved
  queries' sites (typed, or untyped candidates). `POST` the same path with a model answers that model's report. The
  path is the edge's, mounted by `EdgeServer.UPSTREAM` beside `currentUser`: neither legend-engine's namespace nor
  lite's product API.
- **No file.** A file would be a second place to keep; the endpoint serves the same JSON.

### Its cost on the corpus world (11,168 elements)

Measured by proxy: H5's V6 ran this exact scan on the corpus world (the typed retarget: build, pre-filter by name,
type every body that names a candidate, collect the String calls' positions).

| part | time | source |
|---|---|---|
| the model's build (A to F): reused from the request on execute, generate plan and the test-data routes (the request's `ModelContext` handed to the background thread); built once more when the trigger is `compilation/compile`, whose `compileErrors` keeps no context | 0, or 3.4-4.1 s | H5 §2 (d) |
| the name pre-filter, a whole-model walk | at most 1.1-1.3 s (V5's upper bound, every record rebuilt) | H5 §2 (d) |
| typing the 65 candidate bodies | 2.6-2.8 s | H5 V6 |
| the walk over the typed bodies | milliseconds | the probe: 4 bodies in 4 ms |
| **the report** | **about 4 s once per model (about 8 s when compile triggers it), off the answer's path** | |

On the corpus world it would list **48 calls** (16 `indexOf/2`, 4 `substring/2`, 28 `substring/3`) and **83 bodies
unverified** (they do not type in lite; mostly legend-engine's own implementation files the corpus loads), and pass
over 174 list `indexOf`, 55 `substring` of another overload and 6 three-argument `indexOf` (V6's counts,
`h5/corpus/V6-typed-duckdb.txt:2`). A real project is a small fraction of the corpus world. A saved query costs one
parse and one typing, milliseconds each (not measured).

**A tenet to post:** the report is eager G (it types bodies before any query asks), where `docs/TENETS.md` says work is
lazy. It is the same work `compilation/compile` already does eagerly, done once per model off the answer's path, as a
diagnostic. Posted for the user's word, not assumed.

### Its tests (`core/src/test/java/com/legend/edge/PureMeaningReportTest.java`, in `//core:core_edge_tests`)

1. A model with a function, a qualified property, an M2M transform and a constraint, each calling the String
   `substring` or `indexOf`, plus a list `indexOf` and a `###Relational` dynaFunction: the report lists the 4, each
   with its kind, element and position, and neither of the other 2.
2. Once per model: the same model compiled twice writes one log block; a changed model writes a second.
3. Never on the answer's path: `compilation/compile`'s answer is byte-identical with the report on, and arrives while a
   latch holds the report.
4. Saved queries: one whose mapping the model defines is typed and listed with its id and version; one whose model is
   not held is an untyped candidate; a list `indexOf` in a typed one is not listed.
5. An untypeable body is listed `unverified`, by element.
6. The log block stops at 200 site lines with `+N more`; the endpoint answers every site.
7. `POST` with a model answers its report.

### Its size

About 1.5 days: the scan about 150 lines, the saved-query scan about 80, the endpoint about 60, once-per-model and
the background thread about 40; tests about 300.

## 4. FeatureRules and `Lowerer.withFeatures` after A3

A3 adds nothing to the flag machinery. B5 deletes it.

### `CORRECT_SQL_SUBSTRING_INDEXING`

- **The plain rule.** Scalars' plain rule for `AT_STRING_SUBSTRING` becomes `FeatureRules.CORRECTED_SUBSTRING`, the
  same object `substr` already uses.
- **FeatureRules' code is left byte for byte as it is.** `UNDER`'s one registration (`:75-79`) stays. Under the flag,
  it now selects a rule identical to the plain one, which is a no-op.
- **Why the registration stays:**
  - Removing it would make `requireConsumed` (`:101-109`) refuse the flag. That would refuse lite's PCT, which sets it
    (S130, `PctPlanRoute`), and legend-engine's own corpus tests that set it.
  - Moving the flag into `LOWERER_CONSUMED`, beside `PUSH_DOWN_ENUM_TRANSFORM`, would be an addition to the machinery.
- **Only the javadoc changes** (`:18-37`, `:51-58`): "a documented no-op since A3: substring is Pure's with or
  without it; the registration stands until B5 deletes the machinery".

### `LEGACY_SQL_NULL_UNSAFE_EQUALS`

**Untouched by A3.** It is a different strand: H5 task 0's `nullunsafe`, which costs −1 on each corpus lane in text
only, and departs from Pure under CP-12. Its fate is its own ruling, or B5's.

### `Lowerer.withFeatures` (`:230-236`)

**Unchanged:** `requireConsumed`, the features set, `legacyNullUnsafeEquals`. `Scalars.lower` keeps its `features`
parameter and its `FeatureRules.select` call (`:2744-2747`). Both are B5's to delete.

### The detector reads no FeatureRules

It reads the flag off the typed tree (section 3), from the same two carriers, through public accessors. FeatureRules
gains no caller; it is package-private in `lowering`, which the edge does not reach.

## 5. Risks

1. **Databases beyond H2 and DuckDB.**
   - **Measured:** corpus H2 and DuckDB; PCT on DuckDB in both channels; the ENGINE lane on DuckDB and H2.
   - **Not measured:** 10 channel A classes and 9 channel B lanes. They are predicted to move the same 3 rows each,
     from identical ledgered texts. CI on PR2 is the measurement, for the lanes CI runs.
   - **Each dialect's arms** (`sql/dialect/*`) render `SUBSTRING` and `STRPOS` unchanged: H2 `LOCATE`, Snowflake
     `CHARINDEX`, ClickHouse `positionUTF8`, SQLite and Databricks `instr`, MariaDB `INSTR`. A3 moves only the
     arithmetic around them.
   - **MongoDB** writes `STRPOS` as `$indexOfCP + 1` (`MongoDb.java:1106`). After A3 it becomes `($indexOfCP + 1) - 1`:
     right, but not lean. Folding it is the lean-SQL program's job, not A3's, since A3 adds no rendering.
   - **Out-of-range indexes** are where dialects differ (SQLite's `substr(s, 0, n)` answers n−1 characters). That is
     exactly what the new raise covers. Each dialect's raise is tested on the PR's lanes.
2. **Lite's own saved queries.**
   - A query saved under the old meaning answers differently after A3. Through pure/v1 it is warned. Through lite's
     own routes (`/api/lite/v1/queries`, DataCube) it is not, because the product promises Pure's meaning; there it
     changes silently.
   - In the repository, a search of main's query, saved-query and DataCube sources finds the two names only as Java's
     and JavaScript's own string methods.
   - **Answered by the load-time report** (section 3b), for the store lite serves: it scans every saved query, typed
     where the model is at hand and untyped otherwise, and lists each with its id and version. No separate upgrade
     census is needed.
   - **A store lite cannot see stays out of reach**: legend-engine's own query store in a deployment lite does not
     serve, and queries a client keeps itself.
3. **The `testDataGeneration` routes:** covered (revision 2). The detector sits on the three that run a query; the
   census route runs nothing.
4. **The host-side rule and the M2M chain** rest on legend-engine's router, which has not been read yet. Sections 3 and
   3a name the reads. Until they are done, mapping transforms and constraints are flagged with their kinds, and an M2M
   chain on a store is `unverified`: never silent.
5. **Order (fixed: A1, A2, A3, A4).**
   - PR1 before PR2, or about 1,240 parity cases break.
   - A3 lands after A2, so H12's 3 interaction tests (`testPlanForDateTimeConstantParameter{NoTimeZone,GMTTimeZone,
     ESTTimeZone}`) move in A3. They are inside its 7, measured (section 2).
6. **The cross-kind `!=` fix gates A3.** It is `63421e0a` on `claude/hosts-followups` (legend-pure: a String and an
   Integer are never equal, `'x' != -1` is true).
   - **A3's DuckDB roster with it landed: the same 7 as H2's.** In this order, DuckDB's
     `testPlanForDateTimeVariableESTTimeZone` already fails under A2, on the untyped plan parameter (B2's class a3),
     before its `indexOf(...) != -1` assert can reach the cross-kind comparison. So neither A3 nor the fix moves it.
   - The earlier "+9, or +8 if fixed first" counted A3 alone against `main`, where that test passed vacuously.
   - The fix's own moves are measured in its own landing (task in hand).
7. **The report is eager work** (section 3b): the user's word on TENETS' lazy-work tenet, as a once-per-model
   diagnostic.
