# PCT Failure Analysis — Legend-Lite

**Date:** 2026-02-06  
**Total Tests:** 903 | **Passing:** 462 (51%) | **Failing:** 441 (49%)

## Per-Suite Breakdown

| Suite | Total | Pass | Fail | Pass Rate |
|-------|------:|-----:|-----:|----------:|
| Standard Functions | 173 | 67 | 106 | 39% |
| Essential Functions | 324 | 79 | 245 | 24% |
| Relation Functions | 196 | 195 | 1 | 99.5% |
| Unclassified Functions | 76 | 40 | 36 | 53% |
| Grammar Functions | 134 | 81 | 53 | 60% |

---

## High-Level Failure Categories

| # | Category | Count | % of Failures |
|---|----------|------:|---:|
| 1 | **Expression Compilation Gap** | 134 | 30% |
| 2 | **SQL Generation / DuckDB Errors** | 94 | 21% |
| 3 | **Wrong Result Value (Assert Failures)** | 67 | 15% |
| 4 | **Type Casting / Coercion** | 61 | 14% |
| 5 | **Uncategorized / Misc** | 41 | 9% |
| 6 | **DuckDB Function Missing/Mismatch** | 35 | 8% |
| 7 | **Null Handling** | 7 | 2% |
| 8 | **Error Path Testing** | 4 | 1% |

---

## Detailed Buckets

### Category 1: Expression Compilation Gap (134 failures)

These are Pure expressions that legend-lite's compiler cannot translate to SQL yet.

| Bucket | Count | Suites | Notes |
|--------|------:|--------|-------|
| **ArrayLiteral → RelationNode** | 32 | Essential:30, Grammar:2 | `[1,2,3]->filter(...)` — collections used as inline data sources. Need to compile array literals into `VALUES` clauses or `UNNEST`. |
| **InstanceExpression → SQL** | 30 | Essential:16, Grammar:13, Standard:1 | `^Person(name='x')` — in-memory object construction in test data. The `InstanceExpressionHandler` covers some cases but not all (e.g., `Pair`, `Firm`, `CO_Person`, `FO_Person`). |
| **Lambda Expression → SQL** | 25 | Essential:22, Standard:2, Grammar:1 | Lambdas used as values or in higher-order contexts (e.g., `->map(x\|[1,2,3])`, `->map(x\|'hello')`). These are constant-returning or nested lambdas that the SQL generator can't reduce. |
| **eval() on non-column-spec** | 22 | Essential:19, Standard:3 | `eval(rem_Number_1__Number_1__Number_1_, ...)` — Pure's `eval()` applied to function references, not column specs. Need to resolve the function reference and inline it. |
| **Array/Collection literal compilation** | 10 | Essential:9, Grammar:1 | `List(values=[1,2,3])->drop(1)`, `[1,2,3]->slice(1,2)` — collection operations on literal arrays that aren't relation-shaped. |
| **Nested property in projection** | 9 | Standard:9 | `x.employee.name` — multi-hop property navigation in `->project()` lambdas. Only single-level property access is supported. |
| **ClassReference (fn-as-value)** | 6 | Essential:3, Standard:2, Grammar:1 | `format_String_1__Any_MANY__String_1_` — function references passed as values. Need to resolve to inline calls. |

**Recommended attack order:** ArrayLiteral→RelationNode (32) and InstanceExpression (30) together cover ~46% of this category.

---

### Category 2: SQL Generation / DuckDB Errors (94 failures)

Generated SQL that DuckDB rejects or handles incorrectly.

| Bucket | Count | Suites | Notes |
|--------|------:|--------|-------|
| **SQL syntax: trailing ")"** | 13 | Essential:8, Standard:4, Grammar:1 | Empty argument lists or missing expressions producing `SELECT ... ()`. Likely a function call generating `FUNC_NAME()` when args are empty. |
| **DuckDB integer overflow / range** | 10 | Essential:8, Standard:2 | Values like `12345678912` or `1624268257499000` overflow INT32. Need BIGINT casts or HUGEINT for large number tests. |
| **datediff() signature mismatch** | 9 | Essential:9 | DuckDB `datediff` expects `(part, start, end)` but we're generating `datediff(start, end, part)`. Argument order fix. |
| **DuckDB format/printf errors** | 7 | Essential:7 | `format('%s jumps over %f', ['fox', TRUE])` — Pure's `format()` maps to DuckDB `printf()` but type coercion differs. |
| **timestamp format mismatch** | 6 | Essential:6 | `"2015-04-15 17"` not recognized as timestamp. Pure date literals with partial time need zero-padding. |
| **SQL syntax: unexpected "("** | 6 | Standard:6 | Subquery or function call syntax issues in generated SQL. |
| **SQL syntax: unexpected "AS"** | 6 | Unclassified:6 | Alias in wrong position, likely in `CAST(x AS type) AS alias` patterns. |
| **mode() aggregate missing** | 6 | Standard:6 | DuckDB has `mode()` but our SQL generator uses wrong name or syntax. |
| **DuckDB other conversion errors** | 5 | Unclassified:4, Essential:1 | Various type conversion issues in DuckDB SQL execution. |
| **DuckDB other binder/catalog** | 17 | Essential:13, Standard:3, Grammar:1 | Mixed bag: `list_aggr` function binding, mixed-type lists, missing scalar functions like `hasDay`. |
| **VARIANT type** | 3 | Grammar:2, Essential:1 | DuckDB doesn't have a `VARIANT` type; should use `JSON` instead. |
| **parseDate() format** | 4 | Essential:4 | `parseDate('2024-01-15', 'yyyy-MM-dd')` — format string argument not passed through to DuckDB's `strptime()`. |

**Recommended attack order:** datediff() argument order (9) is a single-line fix. SQL syntax issues (25 total) likely stem from 2-3 code paths in the SQL generator.

---

### Category 3: Wrong Result Value (67 failures)

Tests that execute successfully but produce incorrect values.

| Bucket | Count | Suites | Root Cause |
|--------|------:|--------|------------|
| **Date → DateTime coercion** | 7 | Standard:7 | `expected: %2025-04-09` vs `actual: %2025-04-09T00:00:00+0000`. DuckDB returns TIMESTAMP for DATE columns. Need to strip time component when Pure expects StrictDate. |
| **DateTime nanosecond precision** | 2 | Standard:2 | `expected: .000000000+0000` vs `actual: +0000`. We strip trailing zeros but the test expects them preserved. |
| **Decimal suffix missing** | 3 | Standard:1, Essential:1, Grammar:1 | `expected: 1.0D` vs `actual: 1.0`. Pure's Decimal type suffix `D` not emitted. |
| **Integer → Float coercion** | 1 | Standard:1 | `expected: 2` vs `actual: 2.0`. DuckDB returns DOUBLE for what should be INTEGER. |
| **Collection/Array format** | 7 | Essential:5, Unclassified:1, Grammar:1 | `expected: [a,b,c]` vs `actual: a[b[c`. Array result formatting is broken — brackets/commas not being serialized correctly. |
| **Numeric mismatches** | 18 | Essential, Unclassified, Standard | Various: wrong `indexOf` offset (0-based vs 1-based), wrong `rem/mod` sign, date arithmetic off-by-one, floating point precision (`123456789123456789.99` → `123456789123456780.0`). |
| **String result mismatches** | 15 | Essential, Unclassified, Grammar | format/printf not interpolating (`%s` still in output), `encodeBase64`/`decodeBase64` returning DuckDBBlobResult, `substring` off-by-one, `split` returning wrong element. |
| **Date arithmetic errors** | 3 | Standard:3 | `firstDayOfWeek`, `firstDayOfQuarter` returning wrong dates — likely different week/quarter start conventions. |
| **Boolean mismatches** | 4 | Unclassified, Grammar | Various boolean comparison edge cases. |
| **Assert failed (generic)** | 6 | Essential:4, Unclassified:2 | Generic assertion failures without expected/actual detail. |
| **TDS format mismatch** | 1 | Relation:1 | Column header or row format slightly off for a relation test. |

**Recommended attack order:** String format/printf (15) and collection format (7) are likely simple fixes. Date→DateTime coercion (7) is a targeted fix in the adapter.

---

### Category 4: Type Casting / Coercion (61 failures)

The Pure interpreter expects a specific type but the adapter returns a different one.

| Bucket | Count | Notes |
|--------|------:|-------|
| **Float → Integer** | 24 | DuckDB returns DOUBLE for aggregate results (COUNT, SUM on INT). Need `CAST(... AS INTEGER)` or adapter-side rounding. |
| **String → Integer/Number** | 13 | DuckDB returns VARCHAR for some expressions. Need proper type detection in `mapToLegendType()` or SQL-side casts. |
| **Cast: Other** | 14 | Mix of String→StrictDate, Integer→Date, etc. Various type mapping gaps. |
| **String → Float** | 4 | Similar to String→Integer but for float columns. |
| **Date type mismatch** | 4 | StrictDate↔DateTime confusion in the adapter. |
| **String → Boolean** | 2 | DuckDB returns 'true'/'false' strings instead of boolean. |

**Recommended attack order:** Float→Integer (24) is the biggest single bucket here — a targeted fix in the SQL generator or adapter to emit proper casts would clear ~5% of all failures.

---

### Category 5: Uncategorized / Misc (41 failures)

| Sub-pattern | Count | Notes |
|-------------|------:|-------|
| **`element at offset 0` on empty result** | 8 | Query returns empty result set but adapter tries to get first element. Need empty-result handling. |
| **`date()` method requires month/day** | 2 | `%2015-03` partial dates can't be constructed — DuckDB doesn't support year-only or year-month dates. |
| **Parse errors (`^` operator, `'` char)** | 4 | Pure syntax features not supported by legend-lite's parser (power operator, certain string literals). |
| **`letAsLastStatement` / special forms** | 1 | `let` expression compilation edge case. |
| **Integer overflow in Java** | 1 | `NumberFormatException` for `9223372036854775898` (exceeds Long.MAX). |
| **DuckDB overflow errors** | 2 | `INT64` arithmetic overflow (e.g., `MAX + 3`). |
| **LPAD/RPAD insufficient padding** | 2 | DuckDB errors when pad length < string length. |
| **`date_format` / `strftime` argument issues** | 1 | DuckDB `strftime` argument name mismatch. |
| **Other** | ~20 | Various one-off issues across function implementations. |

---

## Prioritized Attack Plan

| Priority | Fix | Failures Fixed | Effort |
|----------|-----|---------------:|--------|
| **P0** | **Float → Integer cast** (adapter or SQL CAST) | ~24 | Low — add `CAST(x AS INTEGER)` for integer-typed expressions |
| **P0** | **datediff() argument order** | ~9 | Low — swap arg order in SQL generator |
| **P1** | **ArrayLiteral → VALUES/UNNEST** | ~32 | Medium — compile `[1,2,3]` to `(VALUES (1),(2),(3))` |
| **P1** | **InstanceExpression expansion** | ~30 | Medium — extend InstanceExpressionHandler for more class types |
| **P1** | **String → numeric type mapping** | ~13 | Low — fix `mapToLegendType()` or add SQL casts |
| **P2** | **format/printf interpolation** | ~15 | Medium — fix Pure `format()` → DuckDB `printf()` mapping |
| **P2** | **Lambda constant reduction** | ~25 | Medium — evaluate constant lambdas at compile time |
| **P2** | **SQL syntax fixes** (empty args, parens) | ~25 | Medium — audit SQL generation for edge cases |
| **P2** | **eval() function dispatch** | ~22 | Medium — resolve function references inline |
| **P3** | **Date/DateTime precision** | ~16 | Low — handle StrictDate vs DateTime, nanosecond formatting |
| **P3** | **Collection result formatting** | ~7 | Low — fix array serialization in adapter |
| **P3** | **Nested property navigation** | ~9 | Medium — support multi-hop joins in projection |
| **P3** | **mode() aggregate** | ~6 | Low — fix function name mapping |
| **P4** | **DuckDB binder errors** | ~17 | High — various one-off function binding issues |
| **P4** | **Empty result handling** | ~8 | Low — null/empty checks in adapter |
| **P4** | **Other misc** | ~20+ | Varies |

### Quick Wins (Low effort, high impact)

Fixing just **P0 + P1** items would resolve approximately **108 failures (~24% of total)** with relatively low effort. The three single biggest buckets are:

1. **ArrayLiteral → RelationNode** (32) — compile collection literals to SQL VALUES
2. **InstanceExpression** (30) — extend the existing handler to cover more test model classes
3. **Lambda constant reduction** (25) — reduce `x|1` to the literal `1` in SQL
4. **Float → Integer cast** (24) — proper type casting in SQL generation
