# A3: the shape note (substring and indexOf to Pure's meaning, loud at pure/v1)

The translator session, 2026-11-19, on the compat owner's ask. A shape note only: **no build, no PR, no pin moved.**
Read from the code at `main` (`b75d6c73a`) and from this branch's H5 and H12 measurements (`H5-translator-spike.md`,
`H12-combined-dry-run.md`, their evidence under `h5/` and `h12/`). The user's ruling D1 (2026-11-19, ~13:25 UTC,
relayed): **"Pure's way, LOUD, no translator."**

## Counts first

| | count |
|---|---|
| PRs | **2**: PR1 is the frontends and the dynaFunction split; PR2 is the core switch and the detector |
| days | **about 5**: PR1 1.5; PR2 3, plus 0.5 to 1 for the out-of-range raise |
| lite tests that move, by name | **17**: 9 old-meaning pins (PR2) and 8 text goldens (PR1). Under option (a), 7 of the 8 move: the IQ showcase's SQL is predicted not to move |
| PCT known-failure rows that leave | **63**: S134's 3 tests on 11 channel A classes (33) and 10 channel B lanes (30) |
| edge net golden lines | **9**: 3 answer lines in each of `duckdb_essential_answers.tsv` and `sqlite_essential_answers.tsv`, and 3 outcome lines in `channel_b_duckdb_essential_outcomes.tsv` |
| ENGINE lane rows | **12**, only if PR #124 lands first: DuckDB essential 7, H2 essential 5, each BOTH_FAIL to LITE_FIXES or LITE_ANSWERS |
| corpus roster and register lines | **31**: `h2-fail-roster` +8, `duckdb-fail-roster` +9; each lane's `*-unordered-register` +6/−1 (7 lines) |
| `NativeCatalogGovernanceTest` ceiling | **1**: `INTERNAL_DESUGAR` from 38 to 40, option (a) only |
| register rows | **3**: S130 and S134 rewritten, one new row (the out-of-range raise) |
| new core hooks | **0**: the detector uses only public API, inside the edge |
| pure/v1 net lines | **0**: the net records status and JSON only, and none of its 28 requests calls either function |

## 1. The slice plan

### The choice inside PR1: how the frontends say SQL's meaning

The frontends write SQL's meaning today: a 1-based start with a length, and a 1-based `indexOf` that answers 0 on a
miss. The frontends are `#Cypher`, `#SQL` (Postgres) and `#IQ`, with 47 call heads; the `###Relational` dynaFunctions
add 3 sites (`indexOf`, `substring`, `position`). H5's spike moved them onto Pure's `substring` and `indexOf` with a
shift, option (b) below. Option (a) is lite-internal natives that keep SQL's meaning verbatim.

| | (a) lite-internal natives, verbatim (recommended) | (b) the spike's shift over Pure's functions |
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

### PR2: the core switch, the detector and the guard moves (about 3 days, plus 0.5 to 1)

| file at main | change | lines |
|---|---|---|
| `lowering/Scalars.java:1489-1497` | `AT_STRING_SUBSTRING`'s plain rule becomes `FeatureRules.CORRECTED_SUBSTRING`, as `substr`'s already is (`:1504-1506`) | spike +20/−26 for both functions |
| `lowering/Scalars.java:1507-1563` | String `indexOf`: `STRPOS(args) - 1`. 3-argument: from clamped to 0, searched from `from+1`, −1 on a miss (Pure's, as Java's `String.indexOf(String, int)`). The list `indexOf` is unchanged | (same) |
| the out-of-range raise | `substring` with a start below 0, an end past the length, or an end before the start raises in the database (a `CASE` raising lite's text, the plain set's pattern), as legend-pure raises (section 2's new row) | about +40 and a test, 0.5 to 1 day |
| `edge/PureMeaningCheck.java` (new), `edge/PureV1Routes.java`, `edge/EdgeServer.java` | the detector (section 3) | about 150, tests about 200 |
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

### The corpus (PR2)

Measured by H5's V2 (`h5/corpus/V2-off-*.txt`): the core switch plus the splits, no translator.

**`spec/src/test/resources/rcorpus/h2-fail-roster.txt`, +8:**
- `executionPlan::tests::testModelConnectionAgg`, `testModelConnectionMultipleAgg`,
  `testSubstringIndexingCorrectedByFeatureFlag`;
- `functions::sqlstring::testToSQLStringSubstringIsUncorrectedByDefault`, `testToSQLStringWithPosition`;
- `query::function::testFilterUsingSubstringFunction`, `testFilterUsingParseIntegerFunction`,
  `testFilterUsingParseDecimalFunction`.

Four of these (the two model-connection aggregations, `...CorrectedByFeatureFlag` and `...WithPosition`) judge
legend-engine's SQL text and are in B2's CP-4 list. The other four are legend-engine's SQL meaning in a query lambda,
which D1 declines to translate.

**`duckdb-fail-roster.txt`, +9:** the same 8, plus `executionPlan::tests::datetime::
testPlanForDateTimeVariableESTTimeZone`. That test was a vacuous pass at main. It is now a real assert that reaches a
cross-kind `!=` (String vs Integer); legend-pure answers `true` there, and DuckDB raises. That is a product defect (H5
F4, S127's family), not A3's. If that fix lands first, DuckDB's count is +8.

**`h2-unordered-register.txt` and `duckdb-unordered-register.txt`, +6/−1 each:**
- `testToSQLStringSubstringIsUncorrectedByDefault`, the four `propertyfunc` tests
  (`testSimplePropertyFuncGetAll`, `...Project`, `...ProjectWithJoin`, `...QueryWithJoin`) and
  `sqlFunction::indexOf::testProject` now pass by answer through the appeal;
- `testFilterUsingParseDecimalFunction` leaves.

Note: *"2026-11-?? A3 (D1): Pure's meaning in the core, no translator (H5's V2, measured; H12's (1))."*

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
  - **The question for the user:** the plan names the negative start only. legend-pure raises on all three, so the row
    names all three, unless the user rules that only the start is guarded. In that case the other two are rows of
    their own: lite answers the database's clamp.

## 3. The detector

### Where it runs

At the edge alone, in `PureV1Routes`:
- `generatePlan` (`:262-292`);
- `execute` (`:302-323`), whose `ARROW_IPC` form is `executeArrow` (`:334-357`), the same route.

`lambdaReturnType`, `lambdaRelationType`, `compile` and the grammar routes compute no answer, so nothing differs
there.

The `testDataGeneration` routes (`TestDataRoutes`) run a query too. They are left out by the brief and listed as a
risk (section 5).

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

This differs from the brief's "quiet under the corrected option", and is posted for the user's word.

### What it does

The check is data: `PureMeaningCheck.of(TypedQuery)` returns the flagged sites and the unverified ones. It never
changes the query: the route plans and runs the `TypedQuery` it already has, and adds a header or throws.

- **`warn`, the default:**
  - The answer carries the header **`x-legend-lite-pure-meaning`**, for example
    `substring/3@1:34, indexOf/2@12:9; unverified`. Each item is `<name>/<arity>`, then `@<line>:<column>` when the
    request carried `sourceInformation`. At most 16 items are listed, then `+N`. `unverified` is appended when
    anything could not be followed.
  - The answer also carries `Access-Control-Expose-Headers: x-legend-lite-pure-meaning`, so a browser client can read
    it. The server writes no expose list (`LegendHttpServer.java:452-455`), and the edge's answer carries it, so no
    product change is needed.
  - Arrow's answer carries it beside `x-legend-response-format: FormatNotSet`.
  - The server writes **one log line** on `System.err`, in the server's `WARNING:` style:

    ```
    WARNING: pure/v1 execute: substring/3 at 1:34 (the query), indexOf/2 at 12:9 (in my::helper) answer legend-pure's
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

The pure/v1 net (`PureV1EdgeGoldenTest`) does not move: it records `status` and JSON only (`:91`), and none of its 28
requests calls either function.

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
     and JavaScript's own string methods. A deployed store is unmeasured here.
   - **Proposed, for the user's word:** a one-time census of a deployed store at upgrade. It would apply
     `PureMeaningCheck` to each saved query through an edge tool, and list the hits.
3. **The `testDataGeneration` routes** run a query on the runner without the detector. They should get it too, or be
   ruled out of scope.
4. **The host-side rule and the M2M chain** rest on legend-engine's router, which has not been read yet. Section 3
   names the reads; until they are done, the M2M case is `unverified`, never silent.
5. **Order.**
   - PR1 before PR2, or about 1,240 parity cases break. Under (b), PR1 and PR2 must be one PR.
   - If B2's CP-4 (lite's own SQL text) lands after A3, 3 more corpus tests per lane fail:
     `testPlanForDateTimeConstantParameter{NoTimeZone,GMTTimeZone,ESTTimeZone}`. This is H12's interaction: A3 makes
     their `indexOf(...) != -1` asserts real.
6. **The cross-kind `!=` defect** surfaces on DuckDB (`testPlanForDateTimeVariableESTTimeZone`). It is a product fix of
   its own (S127's family). Before A3 or after, it changes only DuckDB's count, from +9 to +8.
