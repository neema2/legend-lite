# Independent review: step 1, the edge's DECIMAL Float literal deleted (296732e2..HEAD)

Read-only review. No build or test run. Reviewed the diff through 04216cb4. a8075e82 (goldens and GATES.md) landed
while I was reading; I glanced at it only where it bears on a question below.

## MUST-FIX

1. **The page and the register row say something false: "every database and door" writes the double.**
   - `docs/DIFFERENCES_FROM_LEGEND_ENGINE.md:119` says "lite answers `1.7000000000000002` on every database and door".
   - S20's new paragraph (`docs/SEMANTICS_REGISTER.md:50`) says "every door writes the product's double".
   - Both are wrong on ClickHouse, BigQuery and Snowflake. Their product dialects still return `FloatLit.Kind.DECIMAL`
     (`sql/dialect/ClickHouse.java:900-903`, `BigQuery.java:774-777`, `Snowflake.java:340-343`; the F-row the S20
     row's own column 2 names). Their edge classes inherit that, so lite answers `0.3` there on both doors.
   - Fix: name the five databases the change covers, and keep the ClickHouse/BigQuery/Snowflake exception visible.

2. **Differences from legend-engine on Postgres, Databricks and Trino are not recorded.**
   - The row and the page record one difference, on DuckDB. The deletion covers five databases.
   - `edge/PurePrintForms.java:78-82` (read from legend-engine 4.145.0) says legend-engine's default literal processor,
     which writes the literal bare, is the one used on Postgres, Databricks and Trino as well as DuckDB.
   - Those databases type a bare `0.1` as NUMERIC/DECIMAL. S20's own column 3 already says legend-engine is "DECIMAL on
     ... Postgres". So legend-engine answers `0.3` there, and lite's edge now answers `0.30000000000000004`.
   - The upstream-fidelity tenet: "A divergence that is not a row is a defect."
   - M1 has no edge lane on these three, so the row should say "read, not measured" for them, and the case should be
     posted to the user as the tenet requires. The ruling's words ("delete the float decimal thing") cover the deletion
     on all five, but the "one register row" text only names DuckDB.

## SHOULD-FIX

3. **S20 contradicts itself** (`docs/SEMANTICS_REGISTER.md:50`).
   - Column 3 still says legend-engine types a bare literal "DECIMAL on DuckDB, H2, Postgres".
   - The appended paragraph says H2's processor writes `CAST(x AS FLOAT)`.
   - Column 2 still ends "the edge keeps legend-engine's bare DECIMAL, the edge program's §6 row 21".
   - Fix: correct column 3 for H2, and mark the column-2 clause as superseded on 2026-11-25.

4. **S15 is stale** (`docs/SEMANTICS_REGISTER.md:45`).
   - Its "Recorded gap" says "the compatibility route writes Float literals as decimals, ruling R2", and that Trino's
     compatibility route (DECIMAL kind) is "to be probed".
   - Both are moot or wrong for DuckDB and Trino now. Mark them superseded.

5. **S9 now reads wrongly** (`docs/SEMANTICS_REGISTER.md:39`): "Pure Float ... computed as DECIMAL in SQL for engine
   parity".
   - The page cites S9 for item 4 (`DIFFERENCES...md:127`).
   - Since 2026-11-25 this is untrue on every door except ClickHouse, BigQuery and Snowflake.
   - Annotate it as superseded by S20, or drop the citation.

6. **Stale source comments that still describe the edge's DECIMAL literal:**
   - `sql/dialect/SqlDialect.java:18-19`: "The compatibility edge's is legend-engine's DECIMAL (the edge program's §6
     row 21)."
   - `sql/SqlExpr.java:687-688` (`Kind.DECIMAL`): "legend-engine's bare literal at the edge, and the databases whose
     DOUBLE literal their lanes have not measured yet". The first half is gone.
   - `edge/PurePrintForms.java:73-76`: "Lite writes a Float literal bare already, except at an extreme magnitude". Lite
     writes `1.0E0` on DuckDB and `CAST(x AS DOUBLE PRECISION)` on H2, on both doors now.
   - `ArchitectureTest.java:1393-1395`: "the plan environment's for the edge's engine SQL (row 21: legend-engine's
     DECIMAL)". The entry itself is still needed (`EnginePlans.java:171` still calls `withFloatLiteralKind`); only its
     reason is stale.
   - `sql/dialect/DuckDb.java:1371-1373`: "an assert's expected Float on the compatibility route, whose literal is
     written as a decimal". The code still serves internal DECIMAL-fact literals, so keep it; fix the reason.
   - `core/src/test/java/com/legend/sql/DuckDbRenderTest.java:395`: "the compatibility route writes a Float literal as
     legend-engine does, a decimal".

7. **The reviewed diff's pure/v1 pin was incomplete.**
   - `pure_v1_plan_text.tsv` changed only `raise.sqrtOfNegative`, as M1 said ("1 plan text").
   - The regeneration in a8075e82 also moves `cell.float` and `cell.floatWhole`.
   - So at 04216cb4 `//core:update_edge_pure_v1_1_test` would have been red, and M1's "1 plan text" undercounts.
   - Already corrected by a8075e82. Say so in M1's record or the GATES entry, so nobody reads M1 as complete.

## NITS

8. **Leftover blank lines** in `edge/PureText.java`:
   - a blank line right after the class brace of `DuckDb` (:258-259), `Postgres` (:316-317), `Databricks` (:347-348)
     and `Trino` (:452-453);
   - a double blank line in `H2` (:298-299).
9. **`FloatLiteralKindTest.java:17`** still names "the edge program's §6 row 21" as the test's origin. That is fine as
   history. The edge half (`:40-44`) now only re-checks the product kind on the five databases. It could pin edge kind
   == product kind for every `DatabaseType` directly, but EdgeSpellingHookTest already covers this.
10. **`AnsiSqlRenderer.java:2994-2999`**: two javadocs are stacked on `floatLiteralKind`. The first, "a Float
    literal's bare spelling ... never DOUBLE", is orphaned and contradicts the method. This predates the change.
11. **DIFFERENCES item 4 departs from the page's conventions.**
    - "When: since 2026-11-25" is outside the page's When vocabulary (today, stage A, stage B; page header).
    - The H2 clause and the DuckDB `1.7` are **read** (from the literal processors), not asked of the engine runner.
      The page asks each entry to be marked measured or read.
12. **M1 §3 says "2 shrank"; the pin shows 4 DuckDB rows shrinking.**
    - The rows: `mapping::embedded` -12, `::advanced` -60, `::advanced::inline::nested` -24, `::inline::targetid` -24.
    - Nesting does not fully explain it. `advanced` (-60) contains `nested` + `targetid` (-48) plus one more literal.
      `embedded` (4,884 chars) is smaller than `advanced` (5,329), so it does not contain it: it is a separate -12.
    - M1 undercounted. a8075e82's GATES entry already says so. The pin is fine.

## Verified clean

- **The deletion is complete.**
  - No `floatLiteralKind` override is left in `edge/`.
  - No `ROW_21` or `withRow21` remains in core, pct, spec or tools.
  - `FloatLit.Kind.DECIMAL`, `withFloatLiteralKind`, `PlanEnv.floatLiteralKind` and the ArchitectureTest entry are all
    still live: through ClickHouse, BigQuery and Snowflake, through `EnginePlans`, and through DECIMAL-fact literals the
    lowering writes for itself (`SqlExpr.FloatLit(double)`). None of them is dead.
  - ClickHouse, BigQuery and Snowflake return DECIMAL (the S20 F-row) and are untouched. That is right for this step;
    only the documents overstate (item 1).
- **The guard.**
  - Removing row 21 is a pure shrink. The justification is dated and names the ruling, amendment 11, M1 and S20
    (`EdgeSpellingHookTest.java:174-182`).
  - `floatLiteralKind` is not `@SpellingArm`: neither `AnsiSqlRenderer` nor `SqlDialect` annotates it.
  - `decisionsOverridden` climbs to `AnsiSqlRenderer.floatLiteralKind`, so a re-override is reported.
  - `theEdgesDialectsOverrideSpellingOnly` filters only `ROW_22`, so it fails on a re-override. The exact-arm pin
    `theEdgesDialectsOverrideOnlyThePinnedArms` (OVERRIDES) also fails. Two guards hold it.
  - No count-based ledger pins `PureText.java`: JavaEvalLedgerTest is a file allowlist, and RawSqlLedgerTest counts
    only `raw`.
- **The H2 fail roster.** Exactly the two tests M1 names leave it (`testSubAggregationInQualifier`,
  `testSubAggregationWithIfOnRelationMapping`); none joins.
- **The lean-SQL pins.** 46 rows: 23 DuckDB (19 grow, 4 shrink) and 23 H2 (all grow).
  - No row's `statements`, `subselects` or `refused` moves.
  - Every chars delta fits the literal's spelling: DuckDB +2 per literal (`1.0` to `1.0E0`), or -12 per literal
    (`CAST(x AS DOUBLE)` to `xE0`); H2 +26 per literal.
  - `repeated` fits too: DuckDB +4 is one repeated literal, +2 a copy. H2 +25 is the new `(x AS DOUBLE PRECISION)`
    paren group, 25 characters, repeated once.
- **EqualityCompatEdgeTest.** The new expectations (`holds 0.30000000000000004`, `fails 0.3`) are exactly what the
  deletion predicts on the platform's DuckDB door.
- **`sqrtOfNegative`.** The line's new SQL (`sqrt(CAST(-2.0 AS DOUBLE PRECISION))`) is H2's DOUBLE literal.
- **No other test asserts the edge's old DECIMAL arithmetic.**
  - FloatTextTest:41 is the product's six-decimal text.
  - DecimalKindsTest:164 is column arithmetic.
  - The `0.1 + 0.2` cases in ProductRaiseProbeTest, EqualityWorldsConformanceTest and PureAssertsTest are the product's.
  - The PCT answers moved in a8075e82 (`testFloatPlus` 001, `testFloatTimes` 002) are M1's two.
- **The third review's risk D, for this step.**
  - The edge's DuckDB `toString` of a Float literal is rewritten by `PurePrintForms.floatOf`
    (`PurePrintForms.java:110-112`, `:165-166`). It replaces any `FloatLit` operand with a `DecimalLit` of legend-pure's
    print (PRINT_BARE), whatever the literal's kind.
  - All four asserts of `testFloatToStringWithNegativeExponent` and `...PositiveExponent` are a literal directly under
    `toString` (legend-pure's `toString.pure:75-89`). So they still print `0.000000013421` and `134210000.0` on DuckDB.
    M1 measured no text move.
  - The risk becomes real only when R4 deletes the `PurePrintForms` passes, or for a `toString` over a COMPUTED Float
    at the edge (for example `(0.1 + 0.2)->toString()`, or `(1.3421e-8 * 1.0)->toString()`). DuckDB now casts a double
    there and prints `1e-08`-style or `0.30000000000000004` text, where legend-engine computed with a DECIMAL. No PCT
    case was found that does this; channel B showed 0 moves in M1.

## Tests to watch in the confirming round

- `FloatTextTest.anEdgeFloatLiteralsTextIsLegendEnginesSql`: the core edge lanes, DuckDB, H2 and SQLite.
- `FloatLiteralKindTest`, on every integration lane, including Postgres, Trino and Databricks if they run.
- PCT DuckDB, channels A and B: `toString::testFloatToString`, `...WithNegativeExponent`, `...WithPositiveExponent`,
  `...WithExcessTrailingZeros` and `...WithNoLeadingZero` (`duckdb_essential_answers.tsv:330-343`, and
  `channel_b_duckdb_essential_outcomes.tsv:329-333`).
- Both engine lanes' Float `toString` pair.
- `EqualityCompatEdgeTest`; EdgeSpellingHookTest on `core_edge_tests` and its h2 and sqlite integration lanes;
  `//spec:lean_sql_corpus_{duckdb,h2}`.

## Not verified

- **No build or test was run.** Every pin and expectation above was checked against M1 and the code, not executed.
- **The `plan` (JSON) column in the `mapping::embedded` families shrinks by about 208 characters per literal on both
  DuckDB and H2, apart from the SQL text.** Elsewhere it is about 9 per literal. That is most likely a result-column
  type fact in the plan changing (DECIMAL(p,s) to DOUBLE), not spelling. M1 reports no DuckDB corpus verdict move, but
  one embedded test's plan JSON should be diffed before and after to confirm it is benign.
- **Postgres, Databricks and Trino.** No lane measured the edge on them, here or in M1.
- **legend-engine's answers.** Its DuckDB `1.7` and H2 double answers were read from its literal processors, not asked
  of the engine runner.
