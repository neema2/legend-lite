# legend-engine's known wrong answers, classified (r2, 2026-11-25, night)

*The user's question: "we need to look at all those other known wrong answers to understand are they like
substring/indexof or are they lazy/bugs". Read-only research over legend-engine's and legend-pure's pinned sources, the
r2 inventory and PCT sort, the r2 probe outputs, and lite at `/home/user/s2` (`da231244`). One extra light probe was run:
H2 2.1.214 in memory (§7), with nothing written to disk. No DuckDB was run.*

**Path roots.**
- `ENG/` = `/root/.cache/bazel/_bazel_root/a6663fbd60fad5804a563aa83422814f/external/+http_archive+legend_engine_src/`; `PURE/` = the sibling `+http_archive+legend_pure_src/`.
- `CORE/` = `ENG/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/`.
- `DX` = `ENG/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-dbExtension/legend-engine-xt-relationalStore-duckdb/legend-engine-xt-relationalStore-duckdb-pure/src/main/resources/core_relational_duckdb/relational/sqlQueryToString/duckdbExtension.pure`.
- `ED` = `CORE/sqlQueryToString/extensionDefaults.pure`; `P2S` = `CORE/pureToSQLQuery/pureToSQLQuery.pure`; `PGM` = `CORE/sqlDialectTranslation/toPostgresModel.pure`; `H2X` = `CORE/sqlQueryToString/dbSpecific/h2/h2Extension2_1_214.pure`; `RME` = `CORE/relationalMappingExecution.pure`.
- `H2D` = `ENG/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-dbExtension/legend-engine-xt-relationalStore-h2/legend-engine-xt-relationalStore-h2-sqlDialectTranslation-pure/src/main/resources/core_external_store_relational_sql_dialect_translation_h2/h2SqlDialect.pure` (the H2 route's renderer).
- `SDD` = `ENG/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-sqlDialectTranslation-pure/src/main/resources/core_external_store_relational_sql_dialect_translation/defaults/sqlDialectDefaults.pure`.
- `PCTR` = `ENG/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-PCT/legend-engine-pure-functions-relationalStore-PCT-pure/src/main/resources/core_external_test_connection/pct_relational.pure` (legend-engine's relational PCT adapter).
- `VT` / `RC` = `ENG/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-execution/legend-engine-xt-relationalStore-executionPlan/src/main/java/org/finos/legend/engine/plan/execution/stores/relational/serialization/ValueTransformer.java` / `.../result/ResultColumn.java`.
- `RXP` = `ENG/docs/engineering/reference/regex-portability.md` (legend-engine's own regex defect register).
- `DFP` = `PURE/legend-pure-core/legend-pure-m4/src/main/java/org/finos/legend/pure/m4/coreinstance/primitive/date/DateFormatPattern.java`.
- `PT/` = `PURE/legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure/` (legend-pure's PCT tests); `ET/` = `ENG/legend-engine-core/legend-engine-core-pure/` (legend-engine's own PCT tests: standard, unclassified, relation).
- `MD/` and `MH/` = the `relational-duckdb/` and `relational-h2/` PCT manifests (`*Functions_manifest.json`); `Ess:94` means `MD/EssentialFunctions_manifest.json:94` (or `MH/` in the H2 table).
- `PROBE/` = `/home/user/s1/docs/records/edition-homework/r2/probe/` (the inventory's probe: legend-engine's SQL run on DuckDB 1.3.0/1.5.6 and H2).
- `L/` = `/home/user/s2/core/src/main/java/com/legend/`.
- `INV` = `/home/user/s1/docs/records/edition-homework/r2/sql-inventory.md` §4; `SORT` = `.../r2/pct-sort.md`.

**The rule applied** (the brief's): DELEGATION = legend-engine deliberately maps the Pure function to the database's own
function, so every user query on that database gets the database's meaning (match under `engine`); PRESENTATION = the
value is right, legend-engine's result reader writes or types it differently (match at the edge, no SQL); MACHINERY BUG =
an accident in legend-engine's own machinery (do not reproduce); UNCLEAR = name what settles it. "Inferred" or
"unverified" marks every mechanism not read in source or run.

---

## 0. The answer

**Most of legend-engine's wrong answers are not bugs of the substring kind, and most are not bugs at all.**

| | DuckDB (47) | H2 (54) |
|---|---:|---:|
| DELEGATION (like substring/indexOf) | **15** (9 families) | **19** (11 families) |
| PRESENTATION or TYPE | **23** (3 families) | **20** (3 families) |
| MACHINERY BUG (do not reproduce) | **9** (5 families) | **15** (7 families) |
| UNCLEAR | 0 (2 mechanisms to confirm) | 0 (1 mechanism to confirm) |

- **The presentation rows cost nothing for a real user.** Every DateTime-precision failure (11 DuckDB, 9 H2) comes from
  legend-engine's JSON writer, which prints every TIMESTAMP at nine fraction digits (`VT:25`, `:35-37`), read back by the PCT
  harness (`PCTR:176-177`, `:237-249`). Lite's pure/v1 writer already prints legend-engine's nine digits
  (`L/edge/Presenter.java:155-160`). The number-type and Decimal-scale failures are the harness turning an untyped JSON
  number into a Float (`PCTR:178`, `:181`). In pure/v1 JSON a user sees at most a different spelling, and that spelling
  comes from rows the inventory already prices (S-7, S-10, S-11).
- **The delegations cost about 7 CHEAP substitutes, 6 of them new, plus 2 MEDIUM items already in the inventory.** The
  new substitutes are `round`, `dateDiff` in weeks, `splitPart` with an empty token, `toDecimal`'s bare `decimal`, and,
  on H2 only, `rem` and Boolean `toString`. The seventh, a DateTime's `toString` as the database's cast, is S-16,
  already planned. H2's `ascii` is likely already matched (lite writes plain `ascii` there). The two MEDIUM items are S-11 (enumerations compared across types) and S-3/S-4's placement. Lite's
  `engine` rewrite already has the mechanism: a typed substitute native per function (`L/edge/EngineSubstitutes.java:17-37`).
- **The machinery bugs are rare in real queries, with one exception.** On DuckDB, `startsWith`, `endsWith` and
  `contains` escape `_` and `%` with a backslash and write no `ESCAPE` clause. If DuckDB has no default escape character
  (inferred), then a search text that contains `_` or `%` never matches. That is the most user-visible bug in this list.
  It must not be reproduced, and one DuckDB query confirms it.
- **Two judgment calls for the user** (§5):
  - H2's `mod` of a negative number. I read it as a regression of the H2 route, not a delegation: legend-engine's own
    default writes floor-mod (`ED:254`).
  - `toDecimal` on H2 is a delegation, but it rounds `3.8` to `4`. It changes values, so ask before matching it.

---

## 1. DuckDB: 47 tests

Counted from `MD/*_manifest.json` with the inventory's filter (`scratchpad/r2/wv.py`): Essential 22, Grammar 5,
Relation 1, Standard 14, Unclassified 5. Family letters are INV's; family c is split in two (c1, c2).

| Fam | Tests | Class | Evidence | Visibility | Cost to match | Recommendation |
|---|---|---|---|---|---|---|
| **f** indexOf, 1-based | 2: `indexof::testIndexOfOneElement` (Ess:94), `indexOf::testSimple` (Ess:370) | DELEGATION (ruled) | `DX:237` `instr(%s, %s)` with legend-engine's TODO ("keeping this as H2 also returns 1-based currently, many user tests need to be fixed"); probe `instr('abc','b')`=2, miss=0 (`PROBE/duck-1.5.6.0.out.txt`) | HIGH | CHEAP: the form exists (`sqlPosition`, `EngineSubstitutes.java:54-59`); MEDIUM for exact placement in function bodies (INV §5.2) | match in SQL (ruled) |
| **g** substring, uncorrected | 2: `substring::testStart` (Ess:406), `testStartEnd` (Ess:403) | DELEGATION (ruled) | `P2S:4659-4665`: "Correcting substring unconditionally would change the results of existing queries, hence the flag"; flag read at `P2S:369`; `DX:277` passthrough; probe `substring('abcdef',0,3)`='ab' | HIGH | CHEAP (exists: `substringAsGiven`); MEDIUM placement | match in SQL (ruled) |
| **h** round half away from zero | 2: `testNegativeFloatRoundHalfEvenUp` (Ess:310), `testPositiveFloatRoundHalfEvenDown` (Ess:313) | DELEGATION | `DX:865-871` `cast(round(%s, 0) as BIGINT)`, 2-arg `round(%s, %s)`; probe 16.5→17, -16.5→-17 on 1.3.0 and 1.5.6; Pure half-even (`PT/essential/math/round/round.pure:100-123`) | HIGH (round is everywhere; .5 is common in money) | CHEAP: one substitute; `SqlFn.ROUND_HALF_UP` exists (`L/sql/SqlFn.java:149`) | match in SQL |
| **i** dateDiff in weeks | 1: `testDateDiffWeeks` (Ess:181) | DELEGATION | `DX:213` `datediff('%s',…)`, unit map `DX:478-483` ('week'); probe Sat→Sun = 0 (Pure 1, `PT/essential/date/operation/dateDiff.pure:85`) | MEDIUM (dateDiff common; WEEKS less so) | CHEAP, per database (H2 answers differently, §2) | match in SQL |
| **d** DateTime `toString` | 3: `testDateTimeToString` (Ess:415), `testDateTimeWithTimezoneToString` (Ess:418), `testSimpleDateToString` (Ess:439) | DELEGATION | `DX:284` `cast(%s as varchar)`; probe `2014-01-01 00:00:00`; the brief's own example | MEDIUM (dates in computed text) | CHEAP (S-16's F, already in INV's recommendation) | match in SQL |
| **n** splitPart, empty token | 1: `testSplitPartEmptyToken` (Unc:25) | DELEGATION | `DX:276` `split_part(%s, %s, %s)` passthrough; legend-engine converts the index on purpose (`P2S:3199-3200`, "sql is 1 index based, pure is 0 index based"), so the remaining difference is DuckDB's empty delimiter; probe `split_part('Hello World','',1)`='H'; Pure `split('')->at(0)` (`ET/legend-engine-pure-code-functions-unclassified/.../string/split/splitPart.pure:17-23`) | LOW (empty token is rare) | CHEAP: drop lite's guard under `engine` (`L/lowering/Scalars.java:2216-2225`, `PURE_SPLIT_PART`) | match in SQL |
| **c1** `toDecimal` scale | 1: `testIntToDecimal` (Ess:319) | DELEGATION | `DX:282` `cast(%s as decimal)` (also `H2X:267`, `PGM:406`); DuckDB's bare DECIMAL is (18,3) (probe `typeof(cast('123.450021' as decimal))`=`DECIMAL(18,3)`, value `123.450`). The non-zero scale makes the JSON `8.000`, which the harness reads as Float 8.0, giving `8.0D` (`PCTR:178`) | MEDIUM (toDecimal is common in finance; more than 3 places are rounded to 3: `123.450021`→`123.450`, probed) | CHEAP: write the bare cast under `engine` | match in SQL (it changes values past 3 places: post it) |
| **p** enumeration vs another enumeration's value | 2: `testEqEnum`, `testEqualEnum` (Gra:13, Gra:33 per SORT) | DELEGATION by existing ruling (an unchecked comparison) | Pure `assertFalse(eq(TestEnum1.FIRST, TestEnum2.FIRST))` (`PT/grammar/functions/boolean/equality/eq.pure:74`); legend-engine answers true, carrying enums as names (inferred, SQL not seen, SORT row 129). Ruling 7 and the 2026-11-24 ruling (`L/edge/EngineComparisons.java:13-19`) already say the database decides cross-type comparisons under `engine` | LOW (comparing two enumerations' values is almost always a user error) | MEDIUM (S-11: `LiteralUnroll.neverEqual` folds it at G½, INV T-2) | match in SQL (already ruled; S-11) |
| **o** Variant array: a null joined | 1: `testVariantArrayColumn_joinStrings` (Rel:4) | DELEGATION (inferred) | expected an empty cell, actual `null` (manifest); Pure joins `[]` to `''`, the database's join over a JSON null gives SQL NULL (inferred, SQL not read). Body `ET/.../core_functions_relation/relation/tests/composition.pure:2189-2211` | LOW | 0: **lite already the same** (M4 bucket 2) | nothing to do |
| **a** DateTime precision | 11: `testAdjustByMicrosecondsBigNumber` (Ess:163), `testDateFromSecond` (Ess:196), `testDateFromSubSecond` (Ess:199); `greatest`/`least` `_DateTime`, `_Single`; `max`/`min` `_DateTime`, `_DateTimeArray` (8 Standard) | PRESENTATION | legend-engine's JSON writes every TIMESTAMP as `yyyy-MM-dd'T'HH:mm:ss.nnnnnnnnnZ` (`VT:25`, `:35-37`); the harness parses it back at nine digits for a DateTime (`PCTR:176`) or trims it for a Date (`PCTR:177`, `:237-249`: hence `.499` for adjust's `Date`). Pure's `assertEquals` compares precision | HIGH in principle (every DateTime cell), but **pure/v1 JSON is already identical**: lite writes legend-engine's nine digits (`L/edge/Presenter.java:155-160`) | 0 for users (built). Moving the PCT verdict would need the judge to compare legend-engine's way, which reverses S399: not worth it | match at the edge (done) |
| **b** a number's type | 10: `testRemWithDecimals` (Ess:307); Grammar `testDecimalMinus`/`Plus`/`Times`; Standard `greatest`/`least` `_Number`, `max`/`min` `_Numbers`, `_NumbersArray` (6) | PRESENTATION / TYPE | for a `Number`-typed result the harness keeps the JSON number as parsed, so a Decimal comes back a Float (`PCTR:181`; `max([1.0d])` gives `1.0` where Pure gives `1.0D`, `ET/.../core_functions_standard/math/aggregator/max.pure:98`). Where the list mixes kinds, DuckDB promotes it and the printed number changes (`greatest([1.23, 2])` → `2.0`, Pure `2`, `ET/.../collection/greatest.pure:77`) | LOW: invisible in JSON for one kind; visible only for mixed-kind lists (`2` vs `2.0`) | 0 on its own. The visible part is S-11 (database promotion, ruling 7; MEDIUM) and S-10 (DECIMAL literal, ruled K), both already in INV. Re-typing in Java would change a type (tenet 3) | match at the edge: nothing new (through S-11 only) |
| **c2** a Decimal's scale lost in reading | 2: `testParseDecimalWithPrecisionScale` (Ess:394), `testParseZero` (Ess:400) | PRESENTATION | the database's value has the right scale: `ED:721-727` writes `cast(%s as DECIMAL(p,s))` for 3 arguments (`parseDecimal('123.123',10,5)` = `123.12300`). The harness reads the JSON number as a Float and calls `toDecimal`, losing the scale (`PCTR:178`). `parseDecimal('-000.000')` → bare `decimal` (18,3) = `0.000`, read back as `0.0D` | LOW (the visible part is the one-argument precision, S-7) | 0 on its own (S-7 per database is CHEAP, already in INV) | match at the edge: nothing new |
| **e** an instance's `toString` (the LIKE escape) | 1: `testPersonToString` (Ess:436) | MACHINERY BUG (mechanism inferred) | the text comes from compiled Pure *outside* the query (`PT/essential/string/toString/toString.pure:164`). Only `startsWith(...'Anonymous_')`/`'@_'` runs on the database. The default escapes `_` to `\_` (`ED:800-806`, `:896-905`) and writes no ESCAPE clause (`ED:778-781`, `:288`); DX does not override `startsWith`. The same test **passes on H2**, whose default escape is `\` (probed §7: `'a_b' like 'a\_%'` = true). DuckDB is inferred to have no default escape character (not probed) | **MEDIUM if confirmed**: every `startsWith`/`endsWith`/`contains` (`ED:206`, `:220`, `:288`) whose search text holds `_` or `%` would never match on DuckDB | CHEAP to reproduce, and wrong to do so | do not reproduce; confirm with one DuckDB query (§6) |
| **j** `hour`/`minute` of an hour- or minute-precision literal | 2: `testHour` (Ess:235), `testMinute` (Ess:241) | MACHINERY BUG | the literal processor's format `yyyy-MM-dd?[ HH:mm:ss?[.S*]]` (`DX:145-155`; same in `ED:144-154` and `SDD:987-997`). An optional section writes only if the date can render *every* field in it (`DFP:976-995`), so `%2015-04-15T17` and `%2015-04-15T17:09` lose their whole time and reach SQL as midnight. The comment beside the format scopes it to "day, second, and sub-second precision" (`DX:149-150`): hour and minute precision were not intended. H2 shows the same (`hour(TIMESTAMP '2015-04-15')`=0, §7) | LOW-MEDIUM: rare literal precision, but in a filter it **silently moves the bound to midnight** (wrong rows), not only `hour()` | MEDIUM (print hour/minute literals at day precision) | do not reproduce |
| **k** `pow` through a lambda | 1: `testNumberPow` (Ess:301) | MACHINERY BUG (mechanism inferred) | `{a:Number[1], b:Number[1] \| pow($a,$b)}->eval(3.0, 2)` (`PT/essential/math/power/pow.pure:64-65`) answers 27.0 = 3^3: both parameters evidently bound to the first argument (unverified; SQL not seen). Same on H2 | LOW (a let-bound lambda applied with `eval` inside a query) | EXPENSIVE (reproduce a binding bug) | do not reproduce |
| **l** `adjust` by many minutes into BC | 1: `testAdjustByMinutesBigNumber` (Ess:166) | MACHINERY BUG (result reader) | the SQL is right: the forward assert, year 25489, passes (`PT/essential/date/operation/adjust.pure:167`). The backward one (`:168`, `%-21457-01-08T20:48`) comes back `%21459-07-29`. The read goes through `java.sql.Timestamp` (`RC:59`, `:88-89`), whose calendar is Julian before 1582, and is printed with `yyyy` = year-of-era (`VT:25`), which drops the BC sign. The arithmetic matches exactly: at year −21457 the Julian−Gregorian gap is ⌊Y/100⌋−⌊Y/400⌋−2 = −215+54−2 = −163 days; Jan 8 − 163 days = Jul 29 of proleptic year −21458, which is year-of-era 21459 BCE. INV's "interval overflow in its SQL form" is not the cause | VERY LOW (BC dates) | EXPENSIVE (Java re-computing a value: against tenet 3) | do not reproduce |
| **m** `regexpLike` with MULTILINE | 4: `testRegexpLike_Multiline` (Unc:16), `_Multiline_NonNewlineSensitive` (Unc:19), `_CaseInsensitive_Multiline` (Unc:10), `_CaseInsensitive_Multiline_NonNewlineSensitive` (Unc:13) | MACHINERY BUG (legend-engine's own word) | `DX:449-458` maps MULTILINE to DuckDB's `m` letter. legend-engine's defect register: "D5 \| `MULTILINE` not honoured — DuckDB ignores the `m` option letter; inline `(?m)` works", under "Known defects … our emission is wrong, the engine is capable" (`RXP:236-248`, demo at `RXP:297-301`). NON_NEWLINE_SENSITIVE alone passes (`s` is right) | LOW (the flag is rare) | MEDIUM (copy the flag mapping) | do not reproduce |

INV's family m names `testRegexpLike_NonNewlineSensitive`. The fourth test is `testRegexpLike_Multiline_NonNewlineSensitive` (`MD/UnclassifiedFunctions_manifest.json:19`); `testRegexpLike_NonNewlineSensitive` passes.

---

## 2. H2: 54 tests (+1 Variant, out of scope)

legend-engine 4.145.0 sends every H2 SELECT through its Postgres-model translation (`RME:440-443` `useDialectTranslation`),
then renders with `H2D`. Where the older path (`H2X` plus `ED`) differs from the live one, that is evidence about intent.
Tests are from `scratchpad/r2/h2wv.txt` (Essential 25, Grammar 5, Relation 1, Standard 22, Unclassified 1).

| Family | Tests | Class | Evidence | Visibility | Cost to match | Recommendation |
|---|---|---|---|---|---|---|
| indexOf, 1-based | 2: `testIndexOfOneElement` (Ess:145), `indexOf::testSimple` (Ess:562) | DELEGATION (ruled) | `PGM:381` `strpos`, rendered `locate` (`H2D:383`); older `H2X:231` `LOCATE`; probe `locate('b','abc')`=2 | HIGH | CHEAP (exists) / MEDIUM placement | match in SQL (ruled) |
| substring, uncorrected | 2: `testStart` (Ess:604), `testStartEnd` (Ess:601) | DELEGATION (ruled) | `PGM:418`, `H2D:382`; flag `P2S:4659-4665`; probe `substring('abcdef',0,3)`='abc' on H2 (so H2 fails at the second assert, `PT/essential/string/slice/substring.pure:75`, not the first) | HIGH | CHEAP (exists) / MEDIUM placement | match in SQL (ruled) |
| round half away from zero | 2: `testNegativeFloatRoundHalfEvenUp` (Ess:499), `testPositiveFloatRoundHalfEvenDown` (Ess:502) | DELEGATION | `PGM:410` `cast(round(x) as bigint)`; older `H2X:256` `round(%s, %s)`; probe `round(16.5, 0)`=17 | HIGH | CHEAP | match in SQL |
| dateDiff in weeks | 1: `testDateDiffWeeks` (Ess:358) | DELEGATION | `PGM:397`, `H2D:549-553`; older `H2X:209`; probe Sat→Sun = 1 (Pure 1) but Sun→Sat = −1 (Pure 0, `PT/.../dateDiff.pure:86`): **a different rule from DuckDB's** | MEDIUM | CHEAP, its own H2 arm | match in SQL |
| `rem` over mixed kinds | 1: `testRemWithMixedIntegersAndFloats` (Ess:496) | DELEGATION | `PGM:426` `rem → mod`; older `ED:267` `mod(%s,%s)`: the same on both paths. H2's MOD types the result as the divisor: probe `mod(7.5, 5)` = `3 [INTEGER]` (§7); Pure `rem(7.5, 5)` = 2.5 (`PT/essential/math/operation/rem.pure:41`) | LOW | CHEAP (plain `mod` under `engine`, H2 only) | match in SQL (it changes values: post it) |
| `toDecimal` scale | 2: `testDecimalToDecimal` (Ess:508), `testDoubleToDecimal` (Ess:511) | DELEGATION | `PGM:406` and older `H2X:267` both `cast(%s as decimal)`; H2 2.x's bare DECIMAL has scale 0: probe `cast(3.8 as decimal)` = `4` (§7) | MEDIUM | CHEAP | **ask**: it rounds 3.8 to 4. A database default that probably changed with the H2 2 upgrade (inferred) |
| `toString` as the database's cast | 6: `testBooleanToString` (Ess:607, `TRUE`), `testDateTimeToString` (Ess:616), `testDateTimeWithTimezoneToString` (Ess:619), `testSimpleDateToString` (Ess:643), `testFloatToStringWith{Negative,Positive}Exponent` (Ess:628, :631) | DELEGATION | `PGM:404` and older `H2X:266` `cast(%s as varchar)`; probe `TRUE`, `2014-01-01 00:00:00`, `1.3421E-8`. (DuckDB passes the Float pair because its Float literal is a DECIMAL, so the cast prints positionally: probe `cast(0.000000013421 as varchar)`) | MEDIUM (dates); LOW (Booleans, Floats) | CHEAP: DateTime = S-16; Float = S-15 (exists); Boolean new | match in SQL |
| ascii of `''` | 1: `testAsciiEmptyChar` (Unc:7) | DELEGATION | `PGM:345` `ascii`; older `ED:189` `ascii(%s)`; probe `ascii('')` = NULL (§7); Pure 0 (`ET/.../core_functions_unclassified/string/ascii/ascii.pure:38-41`) | LOW | 0, likely: lite's H2 dialect already writes plain `ascii(…)` (`L/sql/dialect/H2.java:767`; `L/lowering/Scalars.java:598`), so lite should give H2's NULL too (inferred; no H2 engine-lane measurement) | match in SQL (likely already matched) |
| enumeration vs another enumeration's value | 2: `testEqEnum`, `testEqualEnum` (MH Grammar) | DELEGATION by existing ruling | as DuckDB p | LOW | MEDIUM (S-11) | match in SQL (ruled) |
| DateTime precision | 9: 8 Standard (`greatest`/`least`/`max`/`min` DateTime) + `testAdjustByMicrosecondsBigNumber` (Ess:337) | PRESENTATION | as DuckDB a (`VT:25`; `PCTR:176-177`) | already matched in pure/v1 JSON (`Presenter.java:155-160`) | 0 | match at the edge (done) |
| a number's type | 10: 6 Standard Number tests, 3 Grammar Decimal tests, `testRemWithDecimals` (Ess:493) | PRESENTATION / TYPE | as DuckDB b (`PCTR:181`); H2's `testDecimalPlus` comes back `6`, not `6.0` (spelling not traced) | LOW | 0 on its own (S-10, S-11) | nothing new |
| a Decimal's scale lost in reading | 1: `testParseZero` (Ess:595) | PRESENTATION | harness `PCTR:178`. The one-argument form is `decimal(5,2)` on purpose (`PGM:401`, `H2X:477`; S-7, S365) | LOW | 0 on its own (S-7) | nothing new |
| `hour`/`minute` of an hour- or minute-precision literal | 2: `testHour` (Ess:415), `testMinute` (Ess:421) | MACHINERY BUG | the H2 route's timestamp literal uses the same format (`H2D:607-616` → `SDD:987-997`; `DFP:976-995`); probe `hour(TIMESTAMP '2015-04-15')` = 0 | LOW-MEDIUM (filters) | MEDIUM | do not reproduce |
| `pow` through a lambda | 1: `testNumberPow` (Ess:487) | MACHINERY BUG (inferred) | as DuckDB k | LOW | EXPENSIVE | do not reproduce |
| an aggregate over a literal list | 7: `testAverage_{Floats,Integers,Numbers}`, `testMedian_{Floats,Integers,Numbers}`, `testMode_Number` | MACHINERY BUG | the converters keep only the first argument: `average` `PGM:464` (`$p->at(0)`), median `PGM:700-714`, mode `PGM:717-730`. So `average([5,1,2,8,3])` = 5.0 (probe `avg(1.0 * 5)` = 5.0) and `mode([3, 2, 3.0, …])` = 3, its first element (`ET/.../math/aggregator/mode.pure:72`). On DuckDB the same shape crashes in planning ("Unused format args", SORT class 6, `ED:198`/`:251`/`:255` single-`%s` templates) | LOW (a literal list, or a per-row list of values, aggregated inside a query) | MEDIUM | do not reproduce |
| `mod` of a negative (and `binFloor`, which is `value - value->mod(width)`) | 2: `testModWithNegativeNumbers` (Ess:484), `testBinFloorNegative` (Ess:481) | MACHINERY BUG (a regression of the H2 route; a judgment call) | live: `PGM:314` `mod → mod` (truncating: probe `mod(-12,5)` = −2). legend-engine's own default writes Pure's floor-mod on purpose, `mod(mod(a,b)+b,b)` (`ED:254`), which the older H2 path inherited (`H2X` has no `mod` arm), and DuckDB's arm gives Pure's answer too (`DX:249`; probe `cast(fmod(-7,5) as integer)` = 3, and the DuckDB lane passes this test). `binFloor`'s body is in `PT/essential/math/round/binFloor.pure:52-55` | LOW (H2 in production; negative operands) | CHEAP (plain `mod` under `engine`) | **ask** (§5): recommend do not reproduce |
| `bitShiftLeft` past 31 bits | 1: `testBitShiftLeft_UpTo62Bits` (MH Standard) | MACHINERY BUG | the H2 route writes `lshift(value, shift)` with no cast (`H2D:660-676`), so an INTEGER literal wraps: probe `lshift(1, 46)` = 0, but with a BIGINT = 70368744177664 (§7). DuckDB's arm casts on purpose (`DX:199`). The manifest's "expected 0" is the evaluated side: the assert is written `assertEquals(actual, literal)` (`ET/.../core_functions_standard/tbd/bitShiftLeft.pure:31`) | LOW | CHEAP | do not reproduce |
| `parseDecimal(s, p, s)` | 1: `testParseDecimalWithPrecisionScale` (Ess:589) | MACHINERY BUG | `PGM:401` maps every arity to `castExpression('decimal(5,2)')`, which keeps only `$p->at(0)` (`PGM:597-600`), so precision and scale are dropped: `parseDecimal('123.123', 7, 3)` → `123.12`. The older `H2X:474-480` and `ED:721-727` honour them | LOW-MEDIUM | CHEAP | do not reproduce |
| `equalAll` over an empty relation | 1: `testEqualAll_EmptyRelation` (Rel:82) | MACHINERY BUG (mechanism unread) | legend-engine lowers it to `= ALL (subquery)` (`P2S:6782-6785`; `PGM:525-554`). The test's shape in plain SQL on H2 2.1.214 keeps both rows (probe §7: `= all (… empty …)` is true, count 2), and Pure keeps both (`ET/.../relation/functions/quantification/equalAll.pure:63-67`, `:120-142`). So legend-engine's own H2 SQL must differ from that shape. Passes on DuckDB | LOW (a new relation function) | unknown | do not reproduce; the runner's SQL would show the mechanism |

---

## 3. Totals, and the cost of matching every DELEGATION and PRESENTATION row

**Per class.**

| Class | DuckDB tests | H2 tests | Families (union) |
|---|---:|---:|---|
| DELEGATION | 15 | 19 | indexOf, substring, round, dateDiff weeks, DateTime `toString`, `toDecimal`, enumeration cross-type, splitPart (DuckDB), Variant join (DuckDB, already same), `rem` typing (H2), Boolean/Float `toString` (H2), ascii (H2) |
| PRESENTATION | 23 | 20 | DateTime precision, number type, Decimal scale in reading |
| MACHINERY BUG | 9 | 15 | hour/minute literal, `pow` lambda, LIKE escape (DuckDB), BC read-back (DuckDB), regexp MULTILINE (DuckDB), literal-list aggregates (H2), `mod` regression (H2), lshift cast (H2), `parseDecimal(p,s)` (H2), `equalAll` empty (H2) |
| UNCLEAR | 0 | 0 | three mechanisms to confirm (§6), none changing the class |

**The cost of matching every DELEGATION and PRESENTATION row under `engine`:**

- **PRESENTATION: 0 new work.**
  - DateTime precision is already legend-engine's in pure/v1 JSON (`Presenter.java:155-160`).
  - Number type and Decimal scale differ only after the PCT harness's JSON round trip. What a user can see is already
    priced by S-7 (CHEAP per database), S-10 (ruled K) and S-11 (MEDIUM).
- **DELEGATION:**
  - Already built or ruled: indexOf and substring (`sqlPosition`, `substringAsGiven`), with the MEDIUM placement pass
    that INV §5.2 already counts; the Float text (S-15); the Variant join (lite is already the same).
  - Already planned: the DateTime text (S-16, CHEAP); the enumeration cross-type comparison (S-11, MEDIUM).
  - **New, all CHEAP, each one `engine` substitute in the existing rewrite:**
    1. `round`, both databases;
    2. `dateDiff` WEEKS, one arm per database;
    3. `splitPart` with an empty token (DuckDB);
    4. `toDecimal` as the bare `decimal` (both; ask about H2);
    5. `rem` as plain `mod` (H2);
    6. Boolean `toString` as the cast (H2).
  - Likely already matched: `ascii` on H2 (lite's H2 dialect writes plain `ascii`, `L/sql/dialect/H2.java:767`).
- **In all: 6 new CHEAP substitutes, plus work the inventory already prices (S-16 CHEAP; S-11 and S-3/S-4 placement
  MEDIUM).** The H2 `mod` row adds 1 CHEAP if the user reads it as a delegation.

Each new substitute is a departure from Pure. The upstream-fidelity tenet requires it to be posted to the user and
ledgered as a register row before it lands.

---

## 4. Where this disagrees with the inventory (INV §4) and the PCT sort

| Row | INV says | This classification | Why |
|---|---|---|---|
| a (11 DuckDB, 9 H2) | P/type, MEDIUM | PRESENTATION, **0 for users** | the visible text is legend-engine's nine-digit JSON (`VT:25`), which lite already writes (`Presenter.java:155-160`). INV's MEDIUM prices moving the PCT judge (reversing S399), which no user sees |
| b (10, 10) | P/type, MEDIUM | PRESENTATION, 0 on its own | the type loss is the harness (`PCTR:181`). What remains visible is S-10/S-11, already counted |
| c (3 DuckDB) | one P/type family, MEDIUM | **split**: `testIntToDecimal` = DELEGATION, CHEAP; the other 2 = PRESENTATION, 0 | `toDecimal` → bare `decimal` (`DX:282`) gives a scale of 3 that changes values past 3 places. The other two have the right value and scale in the database (`ED:721-727`); the harness drops the scale (`PCTR:178`) |
| d (3 DuckDB; the H2 texts) | P | **DELEGATION** | the text is the database's cast inside SQL (`DX:284`, `PGM:404`). It can be used further, and the brief names it as a delegation. INV's H2 "P/type 26" counts 6 texts that are delegations here (26 − 6 = 20) |
| e `testPersonToString` | P, "unknown text", EXPENSIVE (ask) | **MACHINERY BUG**, mechanism inferred | the instance text is computed outside the query by compiled Pure (`toString.pure:164`) and is the same on both lanes. H2 passes, DuckDB fails, and the only database part is the LIKE: `\_` with no ESCAPE (`ED:778-781`, `:800-806`, `:896-905`). SORT has the same mechanism (row 125) |
| f, g, h, i, n | G (genuine translation bug) | **DELEGATION** | one-line mappings to the database's own function (`DX:237`, `:277`, `:865-871`, `:213`, `:276`), consistent across the DuckDB and H2 paths, reached by every user query, with a TODO (`DX:237`) or flag comment (`P2S:4659`). INV's costs stand |
| j `testHour`/`testMinute` | G, MEDIUM, "mechanism not read" | MACHINERY BUG, **mechanism read** | `DX:152` / `ED:151` / `SDD:994` format; `DFP:976-995` all-or-nothing optional section; comment `DX:149-150` scopes the format to day, second and sub-second. It also moves filter bounds |
| l `testAdjustByMinutesBigNumber` | G, "an interval overflow in its SQL form (not read)", MEDIUM | MACHINERY BUG **in the result reader**, not the SQL | the forward assert passes; the backward answer is exactly the Julian-calendar, year-of-era misprint of the right BC instant (§1 l; `RC:59`, `VT:25`). Agrees with SORT row 48 and makes it exact |
| m (names) | lists `testRegexpLike_NonNewlineSensitive` | it is `testRegexpLike_Multiline_NonNewlineSensitive` | `MD/UnclassifiedFunctions_manifest.json:19`; the plain NonNewlineSensitive test passes |
| m (class) | G, MEDIUM | MACHINERY BUG, from legend-engine's own defect register | `RXP:248` (D5), `RXP:297-301` |
| o | G, "lite already the same" | DELEGATION (inferred), 0 | the database's join over a JSON null; no action either way |
| p | T, MEDIUM | DELEGATION by the existing rulings (an unchecked comparison), MEDIUM | same substance as INV's T-2/S-11 |
| H2 `mod` / `binFloor` | G (and the brief cites "mod semantics of the database" as a delegation example) | MACHINERY BUG (regression), ask | legend-engine's default writes floor-mod on purpose (`ED:254`); the older H2 path and DuckDB give Pure's answer; only the 4.145.0 Postgres-model route (`PGM:314`) truncates |
| H2 `testMode_Number` | its own family, "mode" | the literal-list aggregate bug | `mode([3, 2, …])` = 3 is the first element; `PGM:717-730` keeps `$p->at(0)` |
| H2 averages and medians | G | MACHINERY BUG | `PGM:464`, `:699-713` keep only the first argument. INV §4.2's "H2 adds 6 genuine families" reduces to 3 machinery families (literal-list aggregates, lshift, `parseDecimal(p,s)`), 1 regression (`mod`) and 2 delegations (`rem` typing, `toDecimal`) |
| H2 `testBitShiftLeft_UpTo62Bits` | G, "`70368744177664` for 0" | MACHINERY BUG; the roles are reversed | the assert is written `assertEquals(actual, literal)`: legend-engine answered 0 and the literal is 70368744177664 (`bitShiftLeft.pure:31`; probe `lshift(1,46)`=0) |
| INV §4.1 "28 presentation / 17 genuine / 2 typing" | — | 23 / 9 machinery / 15 delegation | above |

---

## 5. Judgment calls for the user

1. **H2 `mod` of a negative (2 tests).**
   - Read as a regression (recommended): legend-engine meant Pure's floor-mod (`ED:254`; DuckDB agrees). Its 4.145.0 H2
     route lost it (`PGM:314`). Do not reproduce.
   - Read as a delegation (the brief's "mod semantics of the database" example): one CHEAP arm, H2 only.
   - H2 is rarely a production database, so few users see either answer.
2. **`toDecimal` (DuckDB 1, H2 2).** This is a delegation, but it changes values:
   - DuckDB rounds to 3 places (`123.450021` → `123.450`);
   - H2 rounds to an integer (`3.8` → `4`).
   Matching is CHEAP. Ask before it lands, H2 especially.
3. **`rem` over mixed kinds on H2.** H2's MOD returns the divisor's type (`7.5 rem 5` = 3). It is a consistent
   delegation, CHEAP and rarely reached. It changes a value, so post it before it lands.

---

## 6. Not verified, and what would settle it

- **DuckDB has no default LIKE escape character** (family e). Settle with one DuckDB query, `select 'a_b' like 'a\_%'`,
  or ask the runner for `'a_b'->startsWith('a_')` on DuckDB. If DuckDB does treat `\` as an escape, e moves to UNCLEAR.
- **The SQL legend-engine sends for `testEqualAll_EmptyRelation` on H2.** H2 itself answers the test's shape correctly
  (§7), so the class is clear; only the mechanism is open.
- **The SQL legend-engine sends for `testNumberPow`.** Its parameter binding is inferred from 27 = 3^3.
- **The Variant join null (o).** Its SQL was not read; lite is already the same.
- **The JSON legend-engine writes for a BigDecimal with scale** (c1: `8.000`?). Inferred from DuckDB's DECIMAL(18,3)
  (probed) and its JSON writer (`VT`, `RelationalResultToJsonDefaultSerializer`).
- **H2's `testDecimalPlus` actual `6`.** The spelling legend-engine's H2 path writes was not traced.
- **H2 caveat.** §7 used unpatched H2 2.1.214; legend-engine's is patched (S141). None of the probed constructs are
  among the patched functions (inferred).

---

## 7. The probe run here (H2 2.1.214, in memory, nothing written)

Runner `PROBE/Q.java.txt` (`java --source 17`), jar
`/root/.cache/bazel/_bazel_root/a6663fbd60fad5804a563aa83422814f/external/+product_jars+h2_engine/jar/h2-2.1.214.jar`,
URL `jdbc:h2:mem:;MODE=LEGACY;DEFAULT_NULL_ORDERING=HIGH` (legend-engine's settings), SQL on stdin:

```
select 5 = all (select x from (values (150)) t(x) where x > 1000) as e => true [BOOLEAN]
select count(*) from (values (1, 100), (2, 250)) p(id, price) where price = all (select x from (values (150)) t(x) where x > 1000) => 2 [BIGINT]
select lshift(1, 46) as l => 0 [INTEGER]
select lshift(cast(1 as bigint), 46) as l => 70368744177664 [BIGINT]
select mod(7.5, 5) as r => 3 [INTEGER]
select mod(-12, 5) as r => -2 [INTEGER]
select ascii('') as a => NULL [INTEGER]
select cast(3.8 as decimal) as d => 4 [DECIMAL]
select cast(8 as decimal) as d => 8 [DECIMAL]
select 'a_b' like 'a\_%' as l => true [BOOLEAN]
select 'abc' like 'a\_%' as l => false [BOOLEAN]
select hour(TIMESTAMP '2015-04-15') as h => 0 [INTEGER]
select avg(1.0 * 5) as a => 5.00000000000 [NUMERIC]
```
