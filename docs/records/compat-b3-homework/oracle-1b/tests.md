# B3 step 1b, phase 1: the 19 tests S15 and S132 move, and how to ask legend-engine about each

Prepared 2026-11-24 for phase 2 (run.sh). Everything below is READ from the pinned sources, never recalled:

- legend-engine 4.145.0: `$E` = `/root/.cache/bazel/_bazel_root/a6663fbd60fad5804a563aa83422814f/external/+http_archive+legend_engine_src`
  (`$CR` = `$E/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational`)
- legend-pure 5.99.0: `$P` = `.../external/+http_archive+legend_pure_src`; `CompiledSupport.java` =
  `$P/legend-pure-runtime/legend-pure-runtime-java-engine-compiled/src/main/java/org/finos/legend/pure/runtime/java/compiled/generation/processors/support/CompiledSupport.java`
- lite: `/home/user/s4` (branch `claude/compat-b3-nufdlk`, head `bf380031`).

## 1. The 19 tests as found

| # | test (all `meta::relational::tests::...`) | test fn / assert line | db | rule | golden (Campus, Lateral) or list | lite's actual | expected class |
|---|---|---|---|---|---|---|---|
| 1 | `functions::pureToSqlQuery::calendarAggregations::testAnnualizedValue` | 817 / 827 | DuckDB | S132 | 9.812850678733032, 9.267692307692307 | 9.812850678733032, 9.26769230769231 | (a) |
| 2 | `...calendarAggregations::testCmeValue` | 830 / 840 | DuckDB | S132 | 0.5131221719457013, 0.5644343891402714 | 0.5131221719457014, 0.5644343891402716 | (a) |
| 3 | `...calendarAggregations::testP12MtdValue` | 1006 / 1016 | DuckDB | S132 | 6.84, 7.24 | 6.840000000000002, 7.239999999999998 | (a) |
| 4 | `...calendarAggregations::testP12WtdValue` | 941 / 951 | DuckDB | S132 | 4.6, 4.7 | 4.6, 4.700000000000001 | (a) |
| 5 | `...calendarAggregations::testP4WaValue` | 954 / 964 | DuckDB | S132 | 0.8575, 0.98 | 0.8574999999999999, 0.98 | (a) |
| 6 | `...calendarAggregations::testP4WtdValue` | 967 / 977 | DuckDB | S132 | 3.43, 3.92 | 3.4299999999999997, 3.92 | (a) |
| 7 | `...calendarAggregations::testP52WtdValue` | 980 / 990 | DuckDB | S132 | 6.84, 6.98 | 6.840000000000002, 6.979999999999999 | (a) |
| 8 | `...calendarAggregations::testPmaValue` | 1020 / 1030 | DuckDB | S132 | 0.259, 0.222 | 0.259, 0.22200000000000003 | (a) |
| 9 | `...calendarAggregations::testPrioryearValue` | 1084 / 1094 | DuckDB | S132 | 2.84, 3.06 | 2.84, 3.0600000000000005 | (a) |
| 10 | `...calendarAggregations::testPymtdValue` | 1161 / 1171 | DuckDB | S132 | 0.69, 0.46 | 0.69, 0.45999999999999996 | (a) |
| 11 | `...calendarAggregations::testPyqtdValue` | 1174 / 1184 | DuckDB | S132 | 1.4, 1.36 | 1.4, 1.3599999999999999 | (a) |
| 12 | `...calendarAggregations::testPytdValue` | 1187 / 1197 | DuckDB | S132 | 1.59, 1.54 | 1.5899999999999999, 1.54 | (a) |
| 13 | `...calendarAggregations::testQtdValue` | 1234 / 1244 | DuckDB | S132 | 4.23, 4.32 | 4.2299999999999995, 4.32 | (a) |
| 14 | `...calendarAggregations::testYtdValue` | 1281 / 1291 | DuckDB | S132 | 5.59, 5.72 | 5.590000000000001, 5.719999999999999 | (a) |
| 15 | `mapping::sqlFunction::log::testProject` | 568 / 571 | DuckDB | S15 | [0.09531017980432493, 0.5877866649021191] | [0.09531017980432492, unrecorded] | (a) |
| 16 | `mapping::sqlFunction::log::testFilter` | 575 / 578 | DuckDB | S15 | [0.09531017980432493] | [0.09531017980432492] | (a) |
| 17 | `...calendarAggregations::testAnnualizedValue` | 817 / 827 | H2 | S132 | 9.812850678733032, 9.267692307692307 | 9.81285067873303158, 9.26769230769230804 | (b) |
| 18 | `...calendarAggregations::testCmeValue` | 830 / 840 | H2 | S132 | 0.5131221719457013, 0.5644343891402714 | 0.513122171945701362, 0.564434389140271501 | Campus (b); Lateral (c), already ruled |
| 19 | `...calendarAggregations::testPmaValue` | 1020 / 1030 | H2 | S132 | 0.259, 0.222 | 0.258999999999999991, 0.221999999999999996 | (b) |

Files: rows 1-14 and 17-19 are `$CR/calendarAggregation/tests/testCalendarFunctions.pure`; rows 15-16 are
`$CR/tests/mapping/sqlFunction/testSqlFunctionsInMapping.pure`. The goldens are also in `goldens.tsv`, extracted from
those lines by `tools/make_models.py`.

**How the 14 were found.** The progress record names only the three H2 tests and the two log tests. The 14 DuckDB
names come from two places:
- the DuckDB rosters: `spec/src/test/resources/rcorpus/duckdb-accepted-roster.txt:32-38` holds the seven
  `h2-decfloat-divide` calendar tests, which already fail and are accepted, so they cannot move;
  `duckdb-fail-roster.txt` holds no calendar test;
- the measured DuckDB cell differences in `docs/V7_FLATCELLS_ATTEMPT_CENSUS.md:11-30` (2026-08-28): 21 calendar
  cell differences, which are those 7 plus exactly these 14.

The count matches the record's 14 and H6's 14. The 2026-11-24 run listed no names, so phase 2 should confirm them:
classify.py prints each test.

**Where lite's actuals come from** (`lite_actuals.tsv` gives the source of each row):
- the census's Campus cells and the Lateral prefixes it truncates;
- the record's `9.81285067873303158`;
- `work/calc.py` and `work/h2three.py`, which recompute the sums from the tests' own seed.
  - DuckDB: the doubles summed in insertion order. This reproduces every census cell.
  - H2: IEEE division per row, then H2's exact decimal SUM of the doubles. This reproduces the record's H2 value
    digit for digit.

To READ lite's actual directly you have to switch the two leniencies off, which Runs 1-2 did with an uncommitted
change (`LiteHostForms.STRICT`). Then run `bazel run //spec:corpus_one -- duckdb host <fqn>` (or `h2`). The failure
message prints the expected and actual text. For the log tests, ask B in run.sh gives the same answer without lite:
- ask B runs DuckDB 1.5.6.0 (lite's driver, `tools/deps/jars_table.bzl:27`) on the same data;
- both lite and legend-engine send DuckDB's own `ln` (lite `core/.../sql/dialect/DuckDb.java:654`; legend-engine
  `$CR/sqlQueryToString/extensionDefaults.pure:245`, with no DuckDB override in `duckdbExtension.pure`).

## 2. The calendarAggregations family (rows 1-14, 17-19)

**Model** (`testCalendarFunctions.pure:1-100`):
- `EmployeeDatabase` (:3-12): `EmployeeTable(id INT PK, hireDate DATE, hireType VARCHAR(10), fteFactor DOUBLE)`,
  `EmployeeDetailsTable`, `FirmTable`, two joins, and an `include` of the compiled core's `CalendarDatabase`
  (`$CR/calendarAggregation/calendarStore.pure:17-74`: schema `LegendCalendarSchema` with tables `NY_Calendar` and
  `London_Calendar`, 24 columns each).
- `EmployeeMapping` (:18-33): `Employee[emp] : Relational`, with an embedded `employeeDetails`.
- Classes `Employee` and `employeeDetails` (:85-100).
- The runtime is `meta::external::store::relational::tests::testRuntime()`, a test connection to `db`, read from
  `$CR/tests/relationalSetUp.pure:1208`.

**Data** (`setUp`, :102-242):
- 59 `EmployeeTable` rows (`fteFactor` 0.13 to 0.71, Campus and Lateral alternating), 2 `FirmTable` rows and 2
  `EmployeeDetailsTable` rows;
- schema `LegendCalendarSchema` with `NY_Calendar` (60 rows) and an empty `London_Calendar`.

**Query** (each test, the same shape):

```
Employee.all()->groupBy([p|$p.hireType], [agg(p | FN($p.hireDate, 'NY', %2022-11-16, $p.fteFactor), y | $y->sum())], ['hireType', COL])
```

It runs with `EmployeeMapping` and `testRuntime()`. `FN` and `COL` per test: annualized, cme, p12mtd, p12wtd, p4wa,
p4wtd, p52wtd, pma, priorYear, pymtd, pyqtd, pytd, qtd, ytd. The calendar functions are declared in
`$E/legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/corefunctions/dateExtension.pure:17ff`
and return `Number[0..1]`. They are planned by `$CR/calendarAggregation/calendarAggregationPureToSQL.pure`.

**Asserts.** Each test has two asserts:
1. `assertSameSQL(<H2 SQL text>, $result)`, a SQL-text assert. It is outside S15 and S132, and lite's harness
   handles it by its own SQL-text rules (B3 step 3).
2. `assertEquals(['hireType,COL,Campus,<x>,Lateral,<y>,'], toCSV($result.values)->replace('\n', ','))`, a text
   compare of the rendered TDS. This is the assert S132 tolerates.

**The SQL:**
- legend-engine's: `sum(case when <window on ny_calendar_0/ny_calendar_1> then <value> else null end)`. It uses two
  LEFT OUTER JOINs to `LegendCalendarSchema.NY_Calendar`, one on `hireDate` and one on `DATE'2022-11-16'`, and
  groups by `hireType`. In the value, `divide` is `((1.0 * %s) / %s)` (`extensionDefaults.pure:218`), applied for
  annualized, cme, pma and p4wa.
- lite's: the same windows, but it divides `CAST(x AS DOUBLE) / CAST(y AS DOUBLE)`
  (`docs/DATABASE_MODE_HOMEWORK_2026_09_18.md` §4v, line 1485; the user's ruling "keep it ieee", 2026-09-19).

## 3. The sqlFunction log family (rows 15-16)

**Model:**
- `myDB.dataTable` (`testSqlFunctionsInMapping.pure:735-755`; `float1 Float`, an 8-byte double on H2 and in lite's
  DuckDB DDL, `DuckDb.java:490`);
- class `SqlFunctionDemo` (:646-721; `logResult: Float[1]` at :701);
- `testMapping` (:761-839; `scope([myDB] dataTable)( ... logResult: log(float1) ...)` at :765-816);
- runtime `testDataTypeMappingRuntime()` = `testRuntime(myDB)` (:728-731).

**Data** (`setUp`, :41-50): two rows, `float1` 1.1 and 1.8.

**Queries:**
- testProject (:570): `SqlFunctionDemo.all()->project([s | $s.logResult], ['log'])`. It asserts
  `assertEquals([0.09531017980432493, 0.5877866649021191], $result.values->at(0).rows.values)` (:571), then the SQL
  text `select ln("root".float1) as "log" from dataTable as "root"` (:572).
- testFilter (:577): `SqlFunctionDemo.all()->filter(s | $s.logResult < 0.1)->project([s | $s.logResult], ['log'])`.
  It asserts `assertEquals([0.09531017980432493], ...)` (:578), then the SQL text through `assertEqualsH2Compatible`
  (:579-583).

**What the true value is.** Computed exactly: ln of the double nearest 1.1 is 0.095310179804324940787..., and the
nearest double to it is `0.09531017980432493` (5.9e-18 below; the next double down, ...492, is 2.0e-17 below). So
the golden is the correctly rounded value. ln(1.8)'s nearest double is `0.5877866649021191`, also the golden.

## 4. How upstream compares, and how it prints (read from the sources)

**Where the tests run.** legend-engine runs both families in COMPILED mode, through its server:
- `$E/legend-engine-config/legend-engine-server/legend-engine-server-http-server/src/test/java/org/finos/legend/engine/server/test/pureClient/stores/Test_Relational_UsingPureClientTestSuite.java`:
  :34 uses `CompiledExecutionSupport`, :62 runs `mapping::sqlFunction`, and :88 runs `calendarAggregations`.

**How the value travels from the database to the assert:**
1. `meta::pure::router::execute` (`.../core/pure/router/router_entry.pure:25-48`) goes through
   `mayExecuteLegendTest` to `legendExecute` (`.../core/pure/protocol/vX_X_X/invocations/execution.pure:170-212`).
   That sends the query to the server's execute route (`alloyExecuteLambda`, :250-262) or its plan routes (:182-198).
2. The server writes each cell with `value.toString()` (NaN or infinity becomes `null`):
   - `$E/legend-engine-core/legend-engine-core-base/legend-engine-core-executionPlan-execution/legend-engine-executionPlan-execution/src/main/java/org/finos/legend/engine/plan/execution/result/serialization/ExecutionResultObjectMapperFactory.java:70-86`,
   - called from `RelationalResultToJsonDefaultSerializer.processRow` (:125-138).

   So a DuckDB DOUBLE is written as Java's shortest digits, and an H2 DECFLOAT as all its digits.
3. The Pure side parses that JSON:
   - `legendBuildResultFromJSON` (`execution.pure:214-241`, `parseJSON` at :216) hands a `tdsBuilder` result to
     relational's handler (`$CR/protocols/pure/vX_X_X/extension/extension_relational.pure:307-311`);
   - the handler calls `fromJSON(RelationalTDSResult)`, which uses json-simple's `JSONParser`
     (`.../legend-engine-pure-runtime-java-extension-shared-functions-json/.../JsonDeserializer.java:97`; compiled
     `JsonParserHelper.java:88`, `jsonToDouble` :307-318). json-simple reads a non-integral number as a `Double`.
4. `getTabularDataSetFromProtocol` (`$CR/protocols/pure/vX_X_X/invocations/execution_relational_execute.pure:82-113`)
   applies each column's transformer. For a `Float` or `Number` column the transformer is
   `{a | $a->cast(@Number)*1.0}` (:343-344), which yields a Float (a double). A Decimal column also goes through
   `*1.0` (:345-346).

So upstream holds every one of these cells as a DOUBLE. For a DuckDB DOUBLE that is exactly the database's value; an
H2 DECFLOAT is rounded to the nearest double.

**How the assert compares.** `assertEquals(expected, actual)` (`$P/legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure/essential/tests/assertEquals.pure:17-27`)
is `assert(equal($expected, $actual), ...)`. Compiled `equal` (`CompiledSupport.java:1028-1105`):
- a one-element list against a value compares the element (:1079);
- a Number against a Number goes to `eq(Number, Number)` (:1088), which is
  `left.equals(right) || left.toString().equals(right.toString())` (:994-1011, -0.0 normalized to 0.0). For two
  Doubles that is EXACT;
- a String against a String is `left.equals(right)` (:1104), also EXACT.

Upstream has no tolerance anywhere on this path.

**How upstream prints (calendar asserts).**
- `toCSV` (`$CR/helperFunctions/helperFunctions.pure:208-212`) writes each cell with `toCSVString` (:229-236): a
  TDSNull becomes `''`, and anything else becomes `$a->toString()` (:234).
- A Float's compiled `toString` goes through `CompiledSupport.pureToString(Object)` (:1189), then
  `pureToString(Number)` (:1169), then `primitiveToString(double)` (:1271-1274): `0.0` for zero, otherwise
  `DECIMAL_FORMAT` (:130-137), which is `new DecimalFormat("0.0")` with 340 fraction digits. That gives the shortest
  round-trip digits, written positionally.
- A double sum such as `6.840000000000002` therefore prints with all its digits, and fails against `6.84`.

The log asserts compare Floats, not text, so no printing is involved.

## 5. The expected class of each test, and why

**Rows 1-14 (DuckDB, S132): (a), agrees with lite.**
- **Why the goldens differ.** The goldens are H2's decimal arithmetic. H2 2.x SUMs a DOUBLE column in DECFLOAT,
  exact base-10 over the doubles, and legend-engine's `1.0 *` makes H2 divide in DECFLOAT; the result is read back
  as a double. `work/calc.py` reproduces every golden that way. `docs/DATABASE_MODE_HOMEWORK_2026_09_18.md` §4v
  measured it on H2 2.1.214, 2.2.224 and 2.4.240.
- **legend-engine on DuckDB.** DuckDB sums binary doubles. legend-engine's DuckDB SQL computes in doubles too: in
  DuckDB `DECIMAL * DOUBLE` is DOUBLE and `/` is DOUBLE. So it gets lite's doubles, provided DuckDB sums the rows in
  the same order (insertion order reproduces lite's).
- **The verdict.** Its test would print them (§4) and compare the text exactly, so it would fail exactly as lite
  does.
- **The caveats** phase 2 settles: a different join or sum order in legend-engine's SQL, and DuckDB 1.3.0.0 against
  lite's 1.5.6.0 (asks A and B).
- **annualized and cme.** Their Lateral cells (and cme's Campus cell) also differ from the golden by 1-2 ULP because
  of the division: IEEE on DuckDB, decimal on H2. That is part of the same (a).

**Rows 15-16 (DuckDB, S15): (a), agrees with lite.**
- Both lite and legend-engine send DuckDB's `ln` (see §1).
- Upstream compares the Floats exactly (§4: `eq(Number, Number)`), and the JSON round trip of a Double is exact. So
  if DuckDB answers ...492, legend-engine fails as lite does.
- Phase 2 reads DuckDB's answer on both versions.

**Rows 17 and 19 (H2, S132): (b), a printing difference.**
- **lite's value.** Lite's H2 cell is the DECFLOAT sum of IEEE-divided doubles, for example `9.81285067873303158`.
  Lite prints it with every digit, as a Decimal.
- **legend-engine's value.** legend-engine's cell is the DECFLOAT of decimal division, about `9.8128506787330316742...`
  (`work/h2three.py`), and upstream's test reads it as a double (§4).
- **Same doubles.** Read as doubles, both are `9.812850678733032` and `9.267692307692307` (annualized), and `0.259`
  and `0.222` (pma). These are the goldens.
- **The difference** is in how the DECFLOAT result is read into Pure (lite: a Decimal with every digit; legend-engine:
  the JSON number read as a Float) and printed, not in the double. B2b's typed printer is the natural fix.

**Row 18 (H2 cme, S132): Campus (b); Lateral (c), a real value difference.**
- Campus: `0.513122171945701362` reads as `0.5131221719457013`, which is the golden: (b).
- Lateral: lite's `0.564434389140271501` reads as `0.5644343891402716`; legend-engine's decimal-division value reads
  as `0.5644343891402714`, the golden. That is 2 ULP apart.
- **The cause** is lite's IEEE division against legend-engine's H2 decimal division (`((1.0 * %s) / %s)`,
  `extensionDefaults.pure:218`). The user already ruled it: "keep it ieee", 2026-09-19, §4v.
- **What to post.** Post it as a register row (a meaning difference: Pure's `/` on Floats is IEEE, legend-engine's
  H2 SQL divides decimals), not as printing.

**Is the H2 difference H2's version (2.5 against 2.1.214)? No, on this evidence:**
1. Lite's recorded H2 value is reproduced exactly by IEEE division and then an exact decimal SUM, using nothing
   specific to H2 2.5. §4v measured the same DECFLOAT SUM on 2.1.214, 2.2.224 and 2.4.240.
2. The goldens are reproduced by decimal arithmetic throughout (from about 17 significant digits up:
   `work/decfloat.py`'s sweep), read as doubles.

The difference is therefore printing plus, for cme's Lateral cell, the division. H2's version does matter
elsewhere: register S141 says H2 2.5 divides decimals at a higher scale than legend-engine's patched 2.1.214
(`0.383333333333333334` against the golden `0.383333333333`). That affects only legend-engine's own `1.0 *` SQL, the
seven accepted `h2-decfloat-divide` tests, not these three, because lite's SQL divides in doubles. Ask H shows
legend-engine's raw H2 digits to confirm the decimal side.

## 6. What phase 2 asks (run.sh), and the adaptations made to ask it

Each request is a pure/v1-style body: legend-engine's grammar parses the lambda, and the model is sent as text
(`GoldenRunMain.requestBody`). It is planned by `generatePlan` and the plan JSON is run by `PlanExecutor`
(`tools/engine-runner/src/main/java/planoracle/EngineRoutes.java`). classify.py then applies upstream's own reading:
JSON number to double, then printed as §4 describes. There are three asks:
- **A:** DuckDB 1.3.0.0, legend-engine's own (`$E/pom.xml:165`).
- **B:** lite's DuckDB 1.5.6.0 swapped into the same class path.
- **H:** legend-engine's H2 2.1.214 (`$E/pom.xml:160`), with LocalH2.

The DuckDB files are seeded by `planoracle.SeedDuckDbMain` from the same statements the LocalH2 runs, as
`//tools/engine-runner:trades_duckdb` does.

**Adaptations**, each one leaving the SQL of the asked property unchanged (`tools/make_models.py` writes all of them
from the sources):
- Every element is re-packaged under `oracle1b::calendar` or `oracle1b::sqlfn`. The engine's compiled core already
  holds the `meta::` originals, and a second element at the same path is a risk.
- `EmployeeDatabase` loses its `include` of `CalendarDatabase`. The engine's compiler looks a store up among the
  request's own elements only (`PureModel.java:1245-1248`, `getStore_safe` reads `packageableElementsIndex`). The
  calendar joins name the compiled core's `CalendarDatabase` directly (`calendarAggregationPureToSQL.pure:24-37`), so
  the SQL is the same.
- No measured test uses `EmployeeMappingUnion` or `Employee.fteFactorDyna()`, so both are dropped.
- `SqlFunctionDemo` and `testMapping` are cut to `logResult: log(float1)`.
- The runtime is a Runtime element keyed on the test's own store, not upstream's `testRuntime()`, whose test
  connection names `db`.
- `->from(mapping, runtime)` closes each lambda (`Handlers.java:2430-2431`, the TDS overload).
- **The DDL is written, not recalled from a helper.** It uses the store's column types, spelled so that H2 and DuckDB
  both read them (DOUBLE for Float and DOUBLE).
- **The inserts are the tests' own setUp statements, verbatim**, with the `$columnNames` concatenation expanded.

**Risks, all visible in the logs.**
- A request legend-engine refuses is recorded with its status and message, and classify.py prints it.
- If DuckDB 1.3 refuses the sqlfn seed's integer `10` going into a VARCHAR column, quote it in
  `models/sqlfn-h2.pure`. The log values do not depend on it.
- Ask B may fail if legend-engine's DuckDB module needs a 1.3-only driver API. run.sh tolerates that, and A and H
  stand.

## 7. Discrepancies with the brief and the record

1. **The two ln(1.1) values are swapped.** The progress record (`docs/records/compat-b3-progress.md:29-30`) and the
   brief say DuckDB's `ln(1.1)` is `0.09531017980432493` and the expected `0.09531017980432492`. The source's expected
   is `0.09531017980432493` (`testSqlFunctionsInMapping.pure:571`, :578), the correctly rounded value. So lite's
   DuckDB answer must be `0.09531017980432492`, one ULP below.
2. **The 14 DuckDB names are reconstructed** (§1). The count matches.
3. **cme on H2 is not purely a printing difference**: its Lateral cell is a 2-ULP value difference from the
   division (§5).
4. **The calendar tests' first assert is assertSameSQL**, H2's SQL text. On DuckDB, legend-engine's own run would
   meet it before the CSV assert. Phase 2 judges the CSV assert alone, since that is what S132 tolerated.
