# H2, BigQuery, Snowflake, Databricks: why the SQL is long, and what the databases allow now (2026-11-26)

## Summary: the biggest wins, ranked

1. **Snowflake, about 100K characters (12%).** When it explodes a list, Snowflake writes FLATTEN over the list's positions, then reads the element with `GET(<the whole list>, INDEX)`. The whole list expression is written again in every column, in the FLATTEN and in the ORDER BY. That is 146 statements: 184K characters, where DuckDB writes 58K for the same queries. Fix: bind the list once in a one-row derived table and read it by name.
2. **BigQuery, about 70K (7%).** The same defect: `UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(<list>) - 1))` with `<list>[SAFE_OFFSET(t0)]` in each column. That is 62 statements: 124K, against DuckDB's 27K. Fix: `UNNEST(<list>) AS v WITH OFFSET AS t0`.
3. **H2: the list operations it refuses can be written.** H2 2.5.252 refuses a correlated UNNEST, but a correlated scalar subquery over `SYSTEM_RANGE` with a constant bound runs. It reads the outer row's array by index in its WHERE and SELECT, and H2 turns `I <= CARDINALITY(T.XS)` into a range index, so it stays fast: 20,000 rows of 50 elements in about 1 s. I probed filter, map, distinct in first-seen order, sort, reverse, tail, init, slice, range, repeat, position, zip, sum, min, max, avg, median, mode, bool_and, bool_or and joinStrings. All of them answered correctly. This overturns "H2 has neither native lists nor correlated explosion".
4. **H2, about 38K (8%) right away.** A literal list read at a literal position is still written as `CASE WHEN 3 = 1 THEN .. WHEN 3 = 2 THEN .. END`, once per element, so the cost grows with the square of the list's length. Folding it to the element shrinks the 8-element mixed `contains` from 3,148 to about 600 characters (CarrierStrategies.literalPick, then filteredLiteralLength substitutes into it).
5. **BigQuery, about 45K.** Every list operation is wrapped in `(SELECT CASE WHEN __l.__xs IS NULL THEN NULL ELSE .. END FROM UNNEST([STRUCT(<list> AS __xs)]) AS __l)`, which is 102 characters before any nesting (523 wrappers). The wrapper is not needed when the list cannot be NULL (a literal, an ARRAY(...) subquery). 138 element casts turn ARRAY<STRING> into ARRAY<STRING>; they do nothing.
6. **BigQuery and Databricks, about 50K and 30K: backticks on every identifier.** Neither needs them for case (Databricks: case-insensitive names that keep their case, not verified this session; BigQuery: see the dialect's own javadoc). Quote only reserved words and special characters.
7. **Snowflake, about 30K: list statistics.** stdDev, variance and mode are a REDUCE written three times (1,742 characters against DuckDB's 68). For a list with no outer column, an uncorrelated scalar subquery over FLATTEN is allowed anywhere (vendor docs).

Claims found **false or outdated**: H2 "no bit operations / no xor" (false), H2 "no ANY_VALUE" (outdated in 2.5.252), H2 "no correlated explosion, so no list encoding" (false in practice), H2 "no STRUCT" (H2 has ROW, which an array can hold), H2's `LIST_FLATTEN -> flatten(...)` (H2 has no FLATTEN: a wrong spelling), BigQuery "every list operation is a correlated subquery" (ARRAY_SLICE, ARRAY_FIRST and ARRAY_LAST exist), Databricks "no TIME type" (outdated: Databricks Runtime 18 LTS and Databricks SQL, Beta). A new risk: BigQuery's SQL for a mixed list's sort orders by a STRUCT, and the docs say a STRUCT is not orderable.

How this was measured: I re-planned `channel_a.jsonl` on the five databases with the product's own planner, using a throwaway Java program against the Bazel output jars, without running Bazel (scratchpad `hbsd/sql.tsv`). My totals match `baseline.tsv` exactly: DuckDB 529,909; H2 492,904; BigQuery 973,275; Snowflake 846,314; Databricks 655,898. I probed H2 on the shipped jar, `h2-2.5.252.jar` (scratchpad `hbsd/h2probe/`). I could not reach BigQuery, Snowflake or Databricks, so for them every claim comes from vendor docs I read today, or is marked "not verified".

**DuckDB (the baseline):** lite runs `org.duckdb:duckdb_jdbc:1.5.6.0` (tools/deps/jars_table.bzl). The warehouse runs 1.5.5.1.
The latest release is DuckDB v2.0.1 (20 Nov), after v2.0.0 (21 Oct) (https://github.com/duckdb/duckdb/releases).

---

## H2

### 1. Version
- Lite: `com.h2database:h2:2.5.252`, which is lite's one H2 (tools/deps/jars_table.bzl). Probed: `SELECT H2VERSION()` answers `2.5.252`. The engine referee keeps 2.1.214.
- Latest: 2.5.252, released 24 Sep 2026 (https://github.com/h2database/h2database/releases). **Lite is on the latest version.** Nothing newer changes the analysis.
- What H2 has for lists, from the vendor docs (https://h2database.com/html/functions.html):
  - array functions: `ARRAY_GET`, `CARDINALITY`/`ARRAY_LENGTH`, `ARRAY_CONTAINS`, `ARRAY_CAT`/`||`, `ARRAY_APPEND`, `TRIM_ARRAY`, `ARRAY_SLICE`, and element reference `a[i]`;
  - table functions: `UNNEST(array [,...]) [WITH ORDINALITY]` (zips, pads with nulls), `TABLE(...)`, `CSVREAD`;
  - JSON: `JSON_OBJECT` and `JSON_ARRAY` only;
  - lambdas or higher-order functions: none.
- Probed: `SYSTEM_RANGE`/`GENERATE_SERIES`, `ARRAY(subquery)`, `ARRAY_AGG(.. ORDER BY ..)`, `MEDIAN`, `MODE`, `PERCENTILE_CONT`, `LISTAGG`, `ANY_VALUE`, `BOOL_AND`/`BOOL_OR`, `WITH RECURSIVE`, and the ROW type inside an ARRAY.

### 2. Claims (H2.java javadoc and comments, docs/H2_BACKEND.md, docs/records/h2-pct-full-2026-11-26.md)

| Claim | Verdict | Evidence |
|---|---|---|
| No LATERAL (H2.java:140, class javadoc) | TRUE | probe: `CROSS JOIN LATERAL (...)` gives `Function "LATERAL" not found` |
| No correlated table-function arguments / correlated UNNEST | TRUE | probe: `(SELECT SUM(X) FROM UNNEST(T.XS) AS U(X))` and `SYSTEM_RANGE(1, CARDINALITY(T.XS))` both give `Column "T.XS" not found`. A recursive CTE inside a correlated subquery fails the same way, and so does a derived table nested one level below the correlated subquery. |
| "H2 has neither" native lists nor correlated explosion (CarrierStrategies.Caps javadoc); the list family "walls" | **FALSE in practice** | probe: a correlated scalar subquery whose FROM is `SYSTEM_RANGE(1, 2147483647)` with constant arguments can read the outer row in its WHERE and SELECT. EXPLAIN shows `/* range index: X <= CARDINALITY(T.XS) */`. Two levels of correlation (NOT EXISTS inside it) also work. An explosion into rows works as a JOIN: `T JOIN SYSTEM_RANGE(1, (SELECT MAX(CARDINALITY(XS)) FROM T)) R(I) ON R.I <= CARDINALITY(T.XS)`. The probe outputs are below. |
| No lambdas (H2_BACKEND D3) | TRUE, but there is a stand-in | docs list none. The SYSTEM_RANGE subquery stands in for a one-argument lambda, and for two arguments with a second range (comparator sort and dedup). |
| `H2.listCall` refuses every LIST_* function (inherited from AnsiSqlRenderer.listCall) | the refusal is lite's own, not the database's | see the table in part 3 |
| No ANY_VALUE (H2.java:1195, "H2 2.1.214 has no ANY_VALUE") | **OUTDATED** (2.1.214 only) | probe on 2.5.252: `SELECT ANY_VALUE(X) FROM (VALUES 1, 2)` answers `2` |
| "BIT_AND/OR/XOR... reached a dialect without bit-op support", "bitNot reached H2, which has no xor()" (H2.java:818) | **FALSE** | probe: `BITAND(12,10)=8, BITOR=14, BITXOR=6, BITNOT(12)=-13, LSHIFT(1,4)=16, RSHIFT(256,4)=16`; `BIT_XOR_AGG`, `BIT_AND_AGG` exist. 82 refusals in channel_a. |
| No STRUCT type (H2.structTypeName) | **FALSE as worded** | H2 has ROW values, and an array can hold them. probe: `CAST(ARRAY(SELECT ROW(T.XS[I], T.SS[I]) ...) AS VARCHAR)` answers `[ROW (3, b), ROW (1, a)]`. A typed ROW column was not probed. |
| `LIST_FLATTEN -> writer.function("flatten", ..)` (H2.java, plain spellings) | **a wrong spelling** | probe: `FLATTEN(ARRAY[ARRAY[1],ARRAY[2]])` gives `Function "FLATTEN" not found` |
| No time_bucket() (H2.java:826; 109 refusals, the most of any single cause) | TRUE literally, but it can be written | probe: `DATEADD(DAY, FLOOR(DATEDIFF(DAY, DATE '2000-01-03', DATE '2024-05-15') / 7) * 7, DATE '2000-01-03')` answers `2024-05-13`. The h2-pct record also lists this as "B". |
| No make_timestamp / epoch_ms (H2.java:822-824) | TRUE literally, but it can be written | probe: `DATEADD(MILLISECOND, 1500, TIMESTAMP '1970-01-01 00:00:00')` answers `1970-01-01 00:00:01.5` |
| No to_base64 / from_base64 | TRUE | probe: `TO_BASE64`, `BASE64_ENCODE` give "not found" |
| No printf (H2.java:830, 1053) | TRUE | probe: `FORMAT(...)` gives "not found". `TO_CHAR(number, fmt)` exists (see the record). |
| No starts_with (H2.java:610) | TRUE | probe: "not found" |
| No JSON_VALUE (H2.java:1390); JSON_TABLE never exists (H2_BACKEND D2) | TRUE | probe: both give "not found" |
| No map type | TRUE per the record's probe | not re-probed |
| An array holds one type | TRUE | probe: `ARRAY[1, 'a', TRUE]` gives `Data conversion error converting "a"`. A mixed list needs tag arrays, as on BigQuery, or an ARRAY of ROW. |
| H2 refuses an array parameter of more than 65,536 elements (H2.java:73) | not verified | |

**An H2 defect found while probing:** in 2.5.252, `WHERE I <= CARDINALITY(T.XS)` over a NULL array fails with `General error: "java.lang.RuntimeException: Unexpected code path"`. The fix is to write `I <= COALESCE(CARDINALITY(T.XS), 0)`, which every probe below uses.

### The H2 list operations, one by one (probed on 2.5.252)

The table `T(ID, XS INTEGER ARRAY, SS VARCHAR ARRAY, N INT)` has the rows `(1,[3,1,2,1],['b','a'],2)`, `(2,[5,4],['c'],0)`, `(3,[],[],3)` and `(4,NULL,NULL,1)`.

`R` stands for `SYSTEM_RANGE(1, 2147483647) R(I) WHERE I <= COALESCE(CARDINALITY(T.XS), 0)`.

Where a NULL list must answer NULL (DuckDB does), the expression needs `CASE WHEN T.XS IS NULL THEN NULL ELSE ... END` around it. `ARRAY(...)` over a NULL list answers `[]`.

| SqlFn | H2 form | Output (rows 1..4) |
|---|---|---|
| LIST_FILTER | `ARRAY(SELECT T.XS[I] FROM R AND T.XS[I] > 1 ORDER BY I)` | `[3,2]`, `[5,4]`, `[]`, `[]` |
| LIST_TRANSFORM | `ARRAY(SELECT T.XS[I] * 10 FROM R ORDER BY I)` | `[30,10,20,10]`, `[50,40]`, `[]`, `[]` |
| LIST_CONCAT / LIST_APPEND | `T.XS \|\| ARRAY[9]`, `T.XS \|\| 7` | `[3,1,2,1,9]`, `[3,1,2,1,7]` ...; NULL stays NULL |
| LIST_GET / LIST_AT | `CASE WHEN n BETWEEN 1 AND CARDINALITY(T.XS) THEN T.XS[n] END` (an index out of range raises: `Array element error: "5", expected "1..3"`) | `1`, `null`, `null`, `null` |
| LIST_POSITION | `(SELECT MIN(I) - 1 FROM R AND T.XS[I] = 1)` | `1`, `null`, `null`, `null` |
| LIST_ZIP | `ARRAY(SELECT ROW(T.XS[I], T.SS[I]) FROM SYSTEM_RANGE(1, 2147483647) R(I) WHERE I <= COALESCE(LEAST(CARDINALITY(T.XS), CARDINALITY(T.SS)), 0) ORDER BY I)` | `[ROW (3, b), ROW (1, a)]`, `[]` |
| LIST_DISTINCT (first seen) | `ARRAY(SELECT T.XS[I] FROM R AND NOT EXISTS (SELECT 1 FROM SYSTEM_RANGE(1, 2147483647) R2(J) WHERE J < R.I AND T.XS[J] = T.XS[R.I]) ORDER BY I)` | `[3,1,2]`, `[5,4]`, `[]`, `[]` |
| LIST_SUM, MIN, MAX, AVG, MEDIAN, MODE | `(SELECT SUM(T.XS[I]) FROM R)`, and the same with MIN, MAX, AVG(CAST .. AS DOUBLE), MEDIAN, MODE | row 1: `7, 1, 3, 1.75, 1.5, 1`; row 2: `9, 4, 5, 4.5, 4.5, 4`; rows 3-4: NULL |
| LIST_SORT / LIST_SORT_DESC | `ARRAY(SELECT T.XS[I] FROM R ORDER BY T.XS[I] [DESC])` | `[1,1,2,3]` / `[3,2,1,1]` |
| LIST_REVERSE | `... ORDER BY I DESC` | `[1,2,1,3]`, `[4,5]` |
| LIST_TAIL / LIST_INIT / LIST_SLICE | `... AND I > 1`, `... AND I < CARDINALITY(T.XS)`, `... AND I BETWEEN 2 AND 3` | `[1,2,1]` / `[3,1,2]` / `[1,2]` |
| RANGE_FN (computed bound) | `ARRAY(SELECT X FROM SYSTEM_RANGE(0, 2147483647) R(X) WHERE X < T.N ORDER BY X)`; with a step, add `AND MOD(X - start, step) = 0` (not probed) | `[0,1]`, `[]`, `[0,1,2]`, `[0]` |
| REPEAT_VALUE | `ARRAY(SELECT 'v' FROM SYSTEM_RANGE(1, 2147483647) R(I) WHERE I <= T.N)` | `[v,v]`, `[]`, `[v,v,v]`, `[v]` |
| LIST_BOOL_AND / OR | `(SELECT BOOL_AND(T.XS[I] > 0) FROM R)` | `true`, `true`, NULL |
| joinStrings (STRING_AGG) | `(SELECT LISTAGG(T.SS[I], ',') WITHIN GROUP (ORDER BY I) FROM ...)` | `b,a`, `c` |
| LIST_PRODUCT | only `EXP(SUM(LN(x)))`, which answers `6.000000000000001`: not exact. **Keep the refusal.** H2 has no PRODUCT aggregate (none is listed in the docs). | |
| LIST_REDUCE (fold) | **cannot be written per row**: a recursive CTE cannot be correlated (probe: `Column "T.XS" not found`). Over a constant list, an uncorrelated `WITH RECURSIVE` works (probe: answers 10). **Keep the refusal for a list from a column.** | |
| UNNEST (explode a column into rows; 84 refusals) | `T JOIN SYSTEM_RANGE(1, (SELECT MAX(CARDINALITY(XS)) FROM T)) R(I) ON R.I <= COALESCE(CARDINALITY(T.XS), 0)`; LEFT JOIN keeps an empty list's row | rows `(1,3,0) (1,1,1) (1,2,2) (1,1,3) (2,5,0) (2,4,1)` |

Note: an explosion JOINed against the unbounded `SYSTEM_RANGE(1, 2147483647)` did not finish in 8 s. H2 scans the range there. The bound must be the uncorrelated `MAX(CARDINALITY)`. Inside a scalar subquery the unbounded range is fast (0.5 s for the whole probe).

The same form also works over lite's present JSON carrier. probe: `(SELECT COUNT(*) FROM SYSTEM_RANGE(1, 2147483647) R(I) WHERE I <= CARDINALITY(T.J) AND CAST(CAST((T.J)[I] AS VARCHAR) AS INT) > 1)` answers `2` for `[1,2,3]`. So the list operations do not have to wait for a change of carrier.

### 3. Where H2's SQL blows up (channel_a; H2 writes 1,722 statements against DuckDB's 2,266 and refuses 596)

| Family | H2 stmts | H2 chars | DuckDB chars | Ratio | Why, and the code |
|---|---|---|---|---|---|
| contains_Any | 11 | 34,836 | 2,589 | 13.5 | A literal mixed list's `filter` is unrolled per element (CarrierStrategies.filteredLiteralLength). The tag and value at each position stay a CASE over the positions (literalPick), so there are n x n WHENs. |
| removeDuplicatesBy | 4 | 17,225 | 3,048 | 5.7 | The same picks, inside a UNION ALL row per element (the `dedup_src` rows), written twice per row |
| TypedExtendWindow | 201 | 125,790 | 112,474 | 1.12 | Quoting (`"..."`, needed: H2 upper-cases names it does not quote) and the qualify-to-subselect |
| TypedAsOfJoin | 4 | 8,706 | 3,438 | 2.5 | The portable as-of emulation (no ASOF JOIN in H2) |
| TypedGroupBy | 46 | 18,617 | 13,508 | 1.4 | Quoting, plus the AVG cast to double (AggregatesDeliverDouble) |
| round_Number | 14 | 4,834 | 743 | 6.5 | Half-even rounding written out (roundHalfEven: H2's ROUND is half-up) |
| in_Any | 2 | 3,164 | 331 | 9.6 | The same as contains |
| TypedFilter | 29 | 12,569 | 9,807 | 1.3 | Quoting |
| dateDiff | 75 | 13,669 | 11,104 | 1.2 | Calendar arithmetic |
| monthNumber | 17 | 2,783 | 1,057 | 2.6 | EXTRACT spelled out |

**The fix for the top three list rows.** Fold a literal pick at a literal position to the element, so `CASE WHEN 5 = 1 THEN NULL .. WHEN 5 = 5 THEN 'a' .. END` becomes `'a'`. The rule "the compiler may fold constants" allows it: no row data is involved.

Measured over `sql.tsv` (python, scratchpad `hbsd/casefold.py`): **37,892 characters saved on H2.** No other database in this set writes the pattern.

Better still for H2, write the filter as one subquery:
```sql
-- today (3,148 characters): [1, 2, 5, 2, 'a', true, %2014-02-01, 'c']->contains('a')
SELECT coalesce(CASE WHEN CASE WHEN 1 = 1 THEN 1 WHEN 1 = 2 THEN 1 ... END = 2 AND CAST(CASE WHEN 1 = 1 THEN NULL ... END AS VARCHAR) = 'a' THEN 1 ELSE 0 END + ... (8 times) > 0, FALSE) AS "value"
-- probed on 2.5.252, answers TRUE (220 characters):
SELECT COALESCE((SELECT COUNT(*) FROM SYSTEM_RANGE(1, 8) R(I) WHERE ARRAY[1, 1, 1, 1, 2, 3, 4, 2][I] = 2
  AND CAST(ARRAY[NULL, NULL, NULL, NULL, 'a', NULL, NULL, 'c'] AS VARCHAR ARRAY)[I] = 'a') > 0, FALSE) AS "value"
```

### 4. H2's three biggest wins
1. **Fold literal picks (CarrierStrategies.literalPick and filteredLiteralLength).** It uses the constant-folding rule. Evidence: the measurement above. It saves about 38K (8% of H2), and the same fold helps any dialect with `nativeLists=false`. Risk: none in meaning; the comparisons are between literals.
2. **Write `H2.listCall` and `unnestProjection` with correlated scalar subqueries over SYSTEM_RANGE (and a JOIN for an explosion).** It overturns "H2 has neither ... correlated explosion". Evidence: the probes above. It turns most of the record's 128 list pins into SQL, including 8 of its 20 "cannot" (b) items: sort and dedup with a comparator, and max/min with a comparator, using a second range. Fold stays refused. The SQL grows, but stays near DuckDB's size, and refusals become answers.
   - Risks: a NULL list must answer NULL (wrap with CASE). The COALESCE around CARDINALITY is needed (the H2 defect above). The order must come from `ORDER BY I`. Over the JSON carrier, an element needs H2's JSON-to-scalar reading (the dialect's own `variantGet` path).
   - A move from the JSON carrier to typed `T ARRAY` columns is optional.
3. **Write the bit operations, the time bucket and the date-from-parts H2 has (the B list of the record, 36 pins; channel_a refuses about 190 statements for these).** It overturns "no bit-op support / no xor" and uses DATEADD/DATEDIFF. Risk: DATEADD takes an INT count, so counts past 32 bits overflow (the record measured it).

---

## BigQuery

### 1. Version
BigQuery is a service and has no versions. Lite uses Google's JDBC driver `com.google.cloud:google-cloud-bigquery-jdbc:1.6.0:all` (tools/deps/jars_table.bzl).

Array functions available now (https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/array_functions):
- `ARRAY_CONCAT`, `ARRAY_FIRST`, `ARRAY_LAST`, `ARRAY_LENGTH`, `ARRAY_REVERSE`, `ARRAY_SLICE`, `ARRAY_TO_STRING`, `GENERATE_ARRAY`, and the JSON array functions;
- **none takes a lambda.** There is no ARRAY_FILTER or ARRAY_TRANSFORM, and no ARRAY_MIN, MAX, SUM or INCLUDES.

### 2. Claims (BigQuery.java class javadoc and comments)

| Claim | Verdict | Evidence |
|---|---|---|
| No lambdas | TRUE | the array functions page: no function takes a lambda |
| An array holds no array | TRUE | "Arrays of arrays aren't allowed." (https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/data-types) |
| A query RESULT holds no array with a NULL element (inside a query it may) | TRUE | "GoogleSQL for BigQuery raises an error if the query result has an array which contains NULL elements, although such an array can be used inside the query." (same page) |
| A NULL array arrives as an empty one | TRUE | "translates a NULL array into an empty array in the query result" (same page) |
| "every list operation is a correlated subquery over UNNEST(xs) ... WITH OFFSET" | **OUTDATED as a necessity** | `ARRAY_SLICE(a, start, end)` (0-based, inclusive, negative from the end, out-of-range clamped, NULL for a NULL argument), `ARRAY_FIRST`, `ARRAY_LAST` (both raise on empty) exist (array functions page). The dialect uses only ARRAY_REVERSE, ARRAY_CONCAT, ARRAY_LENGTH and ARRAY_TO_STRING. |
| No VALUES clause; no bare UNION; no OFFSET without LIMIT; no lateral derived table; no MAP; WITH RECURSIVE only at the top; an aggregate's ORDER BY takes no NULLS clause; PERCENTILE_* analytic only; no MODE | not verified this session | measured by the lane, per the javadoc |
| A STRUCT literal must name its type per element (the dialect writes `STRUCT<g INT64, ...>(...)` inside an already typed `ARRAY<STRUCT<...>>[...]`) | not a claim, but redundant | the docs give typed-struct syntax and tuple syntax `(e1, e2)`. Whether `ARRAY<STRUCT<..>>[(1,'a'),(2,'b')]` is accepted is **not verified** (the docs page does not say). |
| (new) the mixed list's sort writes `ORDER BY <struct>` (ARRAY_AGG ... ORDER BY the `{g,n,d,s}` key struct) | **probably refused** | "Orderable: applies to all data types except for: ... ARRAY, STRUCT" (data-types page). Seen in the plan for `[1,'a',true]->sort()`, which is not in PCT's set. It should order by the key's fields. **Not verified on the account.** |

### 3. Where BigQuery's SQL blows up (channel_a: 973K against DuckDB's 530K)

| Family | BQ chars | DuckDB | Ratio | Why, and the code |
|---|---|---|---|---|
| let | 287,527 | 172,513 | 1.67 | All of the causes below, inside let blocks |
| TypedExtendWindow | 159,446 | 112,474 | 1.42 | A backtick on every identifier. The VALUES rows emulated as `UNNEST(ARRAY<STRUCT<..>>[STRUCT(1, 2, 'A'), ...])`. Null ordering written as `(k) IS NULL DESC, k DESC`. |
| contains_Any | 37,892 | 2,589 | 14.6 | The filtered-literal unroll (CarrierStrategies.filteredLiteralLength) times a no-op element cast inside the unlessNull wrapper, per element: `((SELECT CASE WHEN __l.__xs IS NULL ... ARRAY(SELECT CAST(__c AS STRING) FROM UNNEST(__l.__xs) ...) END FROM UNNEST([STRUCT(ARRAY<STRING>[...] AS __xs)]) AS __l))[SAFE_OFFSET(CAST(1 AS INT64) - 1)]` |
| removeDuplicatesBy | 18,534 | 3,048 | 6.1 | The same casts and wrappers, plus the root explosion over positions |
| TypedGroupBy | 28,415 | 13,508 | 2.1 | Backticks, the VALUES emulation, and quantiles as a sorted ARRAY_AGG |
| range | 16,534 | 1,828 | 9.0 | `CASE WHEN (CAST(6 + 1 AS INT64) - CAST(1 AS INT64)) * 1 > 0 THEN ARRAY(SELECT __g FROM UNNEST(GENERATE_ARRAY(..)) .. WHERE __g <> stop ..) ELSE ARRAY<INT64>[] END` for every range, its literal guards not folded |
| dateDiff | 20,996 | 11,104 | 1.9 | Calendar arithmetic, plus backticks |
| timeBucket | 13,056 | 3,408 | 3.8 | Bucket arithmetic written out |
| percentile | 9,562 | 1,624 | 5.9 | Sorted-array indexing, the list written per use |
| maxBy / minBy | 8,306 / 8,264 | 375 / 363 | 22 | Five nested list operations, each in the unlessNull wrapper. `array_slice` bounds `CASE WHEN 1 < 0 THEN ARRAY_LENGTH(<the whole inner list>) ...` write the inner list again for a literal condition. The root explosion writes the whole list again in FROM and in each column. |

Cross-cutting counts over the 2,181 statements: 523 unlessNull wrappers (102 characters each before any nesting); 185 wrappers around a literal array, which is never NULL; 138 element casts of a typed array to its own type; 62 root explosions over positions, totalling 124,348 characters against DuckDB's 27,281 for the same queries; and 58,272 backtick characters.

**Before and after, `[1, 'a', true]->contains('a')`** (lite today, 1,179 characters):
```sql
SELECT COALESCE(CASE WHEN (ARRAY<INT64>[1, 2, 3])[SAFE_OFFSET(CAST(1 AS INT64) - 1)] = 2 AND ((SELECT CASE WHEN `__l`.`__xs` IS NULL THEN NULL ELSE ARRAY(SELECT CAST(`__c` AS STRING) FROM UNNEST(`__l`.`__xs`) AS `__c` WITH OFFSET AS `__co` ORDER BY `__co`) END FROM UNNEST([STRUCT(ARRAY<STRING>[NULL, 'a', NULL] AS `__xs`)]) AS `__l`))[SAFE_OFFSET(CAST(1 AS INT64) - 1)] = 'a' THEN 1 ELSE 0 END + ...(x3) > 0, FALSE) AS `value`
```
After, with the tagged arrays kept, one subquery and no casts (about 150 characters; not run on the account):
```sql
SELECT EXISTS(SELECT 1 FROM UNNEST([1, 2, 3]) AS t WITH OFFSET AS o
  WHERE t = 2 AND ARRAY<STRING>[NULL, 'a', NULL][OFFSET(o)] = 'a') AS value
```
After, as ONE `ARRAY<STRUCT<tag, i, s, b>>` (about 190 characters; tuple elements not verified, so write `STRUCT(..)` per element if refused):
```sql
SELECT EXISTS(SELECT 1 FROM UNNEST(ARRAY<STRUCT<t INT64, i INT64, s STRING, b BOOL>>[
  (1, 1, NULL, NULL), (2, NULL, 'a', NULL), (3, NULL, NULL, TRUE)]) AS e WHERE e.t = 2 AND e.s = 'a') AS value
```
Does one struct array shorten the SQL? For a literal list, not much: it holds the same number of literals as the tagged arrays. It does save the index arithmetic at each read: one `UNNEST` gives every channel on one row, as `e.s`, instead of `xs[SAFE_OFFSET(o)]` per channel per use. The large wins come from the single subquery, not from the struct.

Constraints:
- a STRUCT may hold NULL fields in a result; only NULL array *elements* are refused (data-types page);
- an array of structs whose fields are arrays is allowed (`ARRAY<STRUCT<ARRAY<INT64>>>` is the documented way to nest);
- a STRUCT is not orderable, so a sort must name fields (`ORDER BY e.g, e.n, e.s`).

**Before and after, the mixed list's sort `[1, 'a', true]->sort()`.** Lite today writes 10,329 characters (DuckDB 1,517): the sorted position list `(SELECT ARRAY_AGG(p ORDER BY <struct key>) ...)` is written five times (once per output column, in FROM, and in ORDER BY), and each struct key element repeats its full `STRUCT<g INT64, n FLOAT64, d DATETIME, s STRING>` type.

After (about 420 characters; not run on the account). The sorted positions are computed once in FROM, the key is ordered by its fields, and the outer columns read the element:
```sql
SELECT e.p - 1 AS value, e.v0 AS value__0, e.v1 AS value__1, e.v2 AS value__2
FROM UNNEST(ARRAY<STRUCT<p INT64, v0 INT64, v1 STRING, v2 BOOL, g INT64, n FLOAT64, s STRING>>[
  (1, 1, NULL, NULL, 0, 1, NULL), (2, NULL, 'a', NULL, 3, NULL, 'a'), (3, NULL, NULL, TRUE, 2, NULL, 'true')]) AS e
ORDER BY e.g, e.n, e.s, e.p
```

### 4. BigQuery's three biggest wins
1. **The root explosion: `UNNEST(<list>) AS v WITH OFFSET AS t0`, reading `v`** (BigQuery.source / positions, ExplosionMarkers). This is the marker case where there is one list; all 65 of BigQuery's markers in channel_a are single-list.
   - Evidence: 62 statements, 124K against DuckDB's 27K. Expected saving: about 70K.
   - Risk: a NULL element stays a row with a NULL value, as now. A NULL list gives no rows, as now (`IFNULL(.., 0)`). Keep GENERATE_ARRAY for the parallel case of several lists.
2. **No unlessNull wrapper for a list that cannot be NULL; no element cast to the same type** (BigQuery.unlessNull, elements; the `CAST(__c AS T)` subquery). Uses: `ARRAY(subquery)` and a literal are never NULL.
   - Evidence: the counts above. Expected saving: about 45K, more where wrappers nest.
   - Risk: a list from a column can be NULL, so it keeps the wrapper. The decision must come from the typed `nullable` fact or the shape, never a guess.
3. **Fold literal facts, and use EXISTS or COUNT over one UNNEST instead of the per-element unroll** (CarrierStrategies.filteredLiteralLength is applied because `CAPS.nativeLists=false`). The facts are `CASE WHEN 1 < 0`, `CAST(6 + 1 AS INT64) - CAST(1 AS INT64)` and `SAFE_OFFSET(CAST(1 AS INT64) - 1)` with a literal index. Use ARRAY_SLICE for slice, take and drop.
   - Evidence: contains, in, maxBy, minBy and range. Expected saving: about 60K across those families.
   - Risks: ARRAY_SLICE clamps an out-of-range offset, so `tail` of a one-element list needs a length test. A clamped start of 1 on `['a']` may answer `['a']`: **not verified**.
   - Also: backticks only where needed (about 50K); see the summary.

---

## Snowflake

### 1. Version
Snowflake is a service. The lane's account reported version 10.36 through snowflake-jdbc 4.4.0 (docs/CLOUD_BACKENDS_PCT_2026_10_11.md:289). The latest server release is 10.41 (8-12 Nov 2026), with 10.42 listed with no announcements (https://docs.snowflake.com/en/release-notes/new-features). The only entries near arrays are about structured-type schema detection and evolution (GA).

Features that matter:
- higher-order functions FILTER, TRANSFORM and REDUCE (FILTER and TRANSFORM GA May 2024; https://www.snowflake.com/en/engineering-blog/reduce-function-simplify-array-processing);
- **structured ARRAY(T)** in standard tables, whose elements come back typed (https://docs.snowflake.com/en/sql-reference/data-types-structured).

### 2. Claims (Snowflake.java class javadoc and comments; docs/CLOUD_BACKENDS_PCT_2026_10_11.md)

| Claim | Verdict | Evidence |
|---|---|---|
| A lambda takes ONE parameter (two for REDUCE) | TRUE | FILTER and TRANSFORM: "The lambda expression must have only one argument" (https://docs.snowflake.com/en/sql-reference/functions/filter, .../transform) |
| A lambda CANNOT read an enclosing lambda's parameter | not settled by the docs | the docs say lambdas do not support "referencing nested context (such as Snowflake Scripting variables), CTE expressions, arguments in user-defined functions, or correlated subqueries". They also say "You can reference table columns in lambda expressions", and "resolution of identifiers prioritizes lambda arguments first, then column names" (https://docs.snowflake.com/en/user-guide/querying-semistructured). Nested lambdas are not mentioned. The lane's measurement stands. |
| A correlated subquery over FLATTEN is refused | TRUE | "Subqueries with a correlation inside of FLATTEN are currently unsupported." "Correlated scalar subqueries in WHERE clauses" only (https://docs.snowflake.com/en/user-guide/querying-subqueries) |
| (the consequence drawn) a list is reduced only with REDUCE / ARRAY_* | **too broad** | "Uncorrelated scalar subqueries in any place that a value expression can be used" (same page). A list that reads no outer column, which is every PCT literal list, may use `(SELECT agg(..) FROM TABLE(FLATTEN(INPUT => ...)))`. |
| An element is a VARIANT and must be cast wherever it is read | **OUTDATED as a necessity** | structured types: "The returned values and elements have the type specified for the structured value, rather than VARIANT"; built by `ARRAY_CONSTRUCT(..)::ARRAY(NUMBER)` or `ARRAY_CONSTRUCT_STRUCTURED`. FILTER and TRANSFORM accept structured arrays and return the same type (function pages). Limits: "Structured values can't be placed inside VARIANT, semi-structured OBJECT, or semi-structured ARRAY values"; TRY_CAST is unsupported. |
| FLATTEN skips an undefined (SQL NULL) element | not verified (the docs pages read are silent) | the lane's measurement |
| VALUES takes constants only; OFFSET needs LIMIT; no raise function | not verified this session | |
| Snowflake's JAROWINKLER_SIMILARITY differs from DuckDB's (integer percent, case-insensitive) | not verified this session | the lane's measurement, so the 4,556-character REDUCE form is the honest one |

### 3. Where Snowflake's SQL blows up (846K against 530K)

| Family | SF chars | DuckDB | Ratio | Why, and the code |
|---|---|---|---|---|
| removeDuplicates (comparator) | 66,507 | 8,644 | 7.7 | The explosion over positions repeats the whole dedup list per column (Snowflake.markerPositions and explodedColumn: `GET(<list>, "t0"."INDEX")`). The dedup itself is a REDUCE carrying `OBJECT_CONSTRUCT_KEEP_NULL('r', .., 'c0', _rx0)`, because the inner FILTER reads the outer lambda's parameter (Snowflake.carried). |
| let | 214,314 | 172,513 | 1.24 | The same explosion, plus casts |
| TypedExtendWindow | 131,313 | 112,474 | 1.17 | Quotes (needed: Snowflake upper-cases names it does not quote) and explicit frames |
| removeDuplicatesBy | 19,680 | 3,048 | 6.5 | The explosion and the REDUCE as above |
| stdDevSample | 15,968 | 628 | 25.4 | Welford's update as a REDUCE, written three times (n, sum and q are each read from a fresh copy of the REDUCE) |
| removeDuplicates | 13,633 | 1,837 | 7.4 | The explosion and the REDUCE |
| jaroWinklerSimilarity | 9,112 | 134 | 68 | Three nested REDUCEs (Snowflake.jaroWinkler); honest, 2 statements |
| TypedGroupBy | 20,622 | 13,508 | 1.5 | Quotes and casts |
| variance | 7,309 | 216 | 33.8 | As stdDev |
| percentile / mode | 7,785 / 7,114 | 1,624 / 1,654 | 4.8 / 4.3 | REDUCE runs over a sorted array, the array re-written per read |

**Why a mixed list's dedup and sort are so long.** I measured `[1, 'a', true]->removeDuplicates()` at 13,464 characters against DuckDB's 1,406, and `->sort()` at 6,894 against 1,517.
- It is not mainly the casts or OBJECT_CONSTRUCT_KEEP_NULL. The dedup list (about 2,100 characters) is written six times: once per output column (four), inside `ARRAY_GENERATE_RANGE(0, ARRAY_SIZE(..))` and in the ORDER BY. All of that comes from `markerPositions` and `explodedColumn`.
- Second, the REDUCE re-creates `TRANSFORM(xs, x -> ARRAY_CONSTRUCT(x))` three times: as the slice's source, its size, and the start value.
- Third, each GET is wrapped in a cast. That is 2,872 `CAST(GET(` sites in channel_a.

Shorter form (no new feature): bind the list once, then flatten its positions laterally. A lateral FLATTEN over an outer column is the documented use; only a correlation inside a subquery's FLATTEN is refused.
```sql
SELECT CAST(GET(ARRAY_CONSTRUCT(1, 2, 3), CAST(GET("b"."L", "t0"."INDEX") AS INTEGER) - 1) AS BIGINT) - 1 AS "value", ...
FROM (SELECT <the dedup list, once> AS "L") AS "b",
     LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, ARRAY_SIZE("b"."L"))) AS "t0"
ORDER BY "t0"."INDEX"
```
That is about 13,464 - 5 x 2,100, so near 3,000 characters. The REDUCE's three copies of the TRANSFORM can be bound the same way (its start value from the bound list) for another ~400.

For the statistics over a list with no outer column (an uncorrelated scalar subquery, allowed anywhere):
```sql
SELECT (SELECT STDDEV_SAMP(CAST(f.VALUE AS DOUBLE)) FROM TABLE(FLATTEN(INPUT => ARRAY_CONSTRUCT(1.0, 2.0, 3.0))) f) AS "value"
```
That is about 120 characters against 1,742. FLATTEN dropping a NULL element is what an aggregate does anyway. When the list does read an outer column, compute the REDUCE once in a derived table and take n, sum and q from it, instead of three copies.

### 4. Snowflake's three biggest wins
1. **Bind the exploded list once** (Snowflake.source, markerPositions, explodedColumn). It uses a lateral FLATTEN over a bound column.
   - Evidence: 146 statements, 184K against DuckDB's 58K. Expected saving: about 100K (12%).
   - Risk: none in meaning. The positions and NULL elements are handled exactly as now, because the positions are still flattened.
2. **Uncorrelated scalar subqueries over FLATTEN for list statistics and dedups whose list reads no outer row.** It overturns "a list is reduced only with REDUCE". Evidence: the vendor sentence above.
   - Expected saving: about 30K in stdDev, variance, mode and percentile, more if dedup and sort use `QUALIFY ROW_NUMBER()` over a FLATTEN of a literal.
   - Risk: FLATTEN skips a SQL NULL element (the lane's measurement). That is correct for aggregates. For dedup and sort, carry NULLs as JSON null (the dialect's `keepNull`).
3. **Structured arrays `ARRAY(T)` for single-type lists, so a GET needs no cast.** Uses structured types (docs above). Expected saving: up to about 40K (2,872 `CAST(GET(` sites at about 15 characters).
   - Risks: structured values cannot sit inside a VARIANT or a semi-structured array, so a list of lists or a mixed list stays semi-structured. TRY_CAST is unsupported. Whether ARRAY_SORT, ARRAY_DISTINCT and REDUCE accept structured arrays is not in the pages read (FILTER and TRANSFORM do).

---

## Databricks

### 1. Version
- The lane runs a Databricks SQL warehouse (Free Edition). It measured `dbsql_version` 2026.39 with databricks-jdbc 3.4.3 (docs/CLOUD_BACKENDS_PCT_2026_10_11.md:245; tools/deps/jars_table.bzl).
- The version is the warehouse channel's choice, not lite's pin. The current channel is 2026.47 and the preview 2026.48 (https://docs.databricks.com/aws/en/sql/release-notes/).
- The latest Databricks Runtime is 19 (Spark 4.3.0, 23 Jul 2026); the latest LTS is 18 (Spark 4.1.0, 10 Jun 2026) (https://docs.databricks.com/aws/en/release-notes/runtime/).
- Lambdas (transform, filter, aggregate, exists, forall, array_sort with a comparator, zip_with, reduce) are long-standing. Lite uses them, so the SQL is near DuckDB's (656K against 530K).

### 2. Claims (Databricks.java)

| Claim | Verdict | Evidence |
|---|---|---|
| "Databricks has no TIME type" (Databricks.java:139-140, 895, 1244) | **OUTDATED** | "Applies to: Databricks SQL, Databricks Runtime 18 LTS and above", `TIME \| TIME(p)`, p 0-6, Beta (https://docs.databricks.com/aws/en/sql/language-manual/data-types/time-type) |
| A lambda's index parameter is 0-based; Spark refuses explode inside an expression; no as-of join; no quantified comparison; no JSON pretty-printer; no Jaro-Winkler; ANSI mode off by default | not verified this session | measured by the lane, per the javadoc |
| (spelling) `CASE WHEN i = 0 THEN NULL ELSE try_element_at(xs, i) END`, which writes the index twice | a shorter form exists | `get(arrayExpr, index)`, 0-based: "If the index is negative or outside the bounds of the array the result is NULL." Databricks SQL and DBR 11.3 LTS+ (https://docs.databricks.com/aws/en/sql/language-manual/functions/get) |
| (spelling) joinStrings as `CASE WHEN array_size(filter(xs, notnull)) = 0 THEN NULL ELSE array_join(transform(filter(xs, ..), cast), sep) END`, which writes the list twice | a shorter form exists | `array_join` drops NULL elements itself: "If nullReplacement is omitted, null elements are filtered out" (https://docs.databricks.com/aws/en/sql/language-manual/functions/array_join). What it answers for an empty array is not documented: keep a `coalesce(.., '')`. |

### 3. Where Databricks's SQL blows up (656K against 530K)

| Family | DBX chars | DuckDB | Ratio | Why |
|---|---|---|---|---|
| let | 207,883 | 172,513 | 1.21 | Backticks; joinStrings written twice (above); `CAST(x AS VARIANT)` and schema_of_variant type tests |
| TypedExtendWindow | 133,343 | 112,474 | 1.19 | Backticks, BIGINT `L` suffixes |
| dateDiff | 17,957 | 11,104 | 1.6 | Calendar arithmetic (timestampdiff counts whole units) |
| timeBucket | 9,312 | 3,408 | 2.7 | Bucket arithmetic |
| jaroWinkler | 5,750 | 134 | 43 | Written out (no native function, per the javadoc); honest |
| format | 10,751 | 5,315 | 2.0 | Per-type text as a CASE over schema_of_variant |
| TypedGroupBy | 18,360 | 13,508 | 1.4 | Backticks |
| removeDuplicates (comparator) | 13,343 | 8,644 | 1.5 | Near DuckDB's |
| stdDevSample | 5,283 | 628 | 8.4 | Welford in `aggregate` with a finishing lambda (532 against 68 per statement): honest without a list-stddev function |
| range | 4,952 | 1,828 | 2.7 | `CASE WHEN (stop - start) * step > 0 THEN filter(sequence(..), g -> g <> stop) ELSE .. END`; foldable for literal bounds |

### 4. Databricks's three biggest wins
1. **Backticks only where needed.** 36,674 quote characters. Expected saving: about 30K (5%). Risk: a reserved word or special character still needs quoting; case-insensitive names that keep their case are **not verified this session**.
2. **`get(xs, i - 1)` for at/get and `coalesce(array_join(xs, sep), '')` for joinStrings.** Uses get and array_join (docs above). Expected saving: about 10-15K (percentile, at, format, let). Risk: joinStrings on an empty list (undocumented, so keep the coalesce).
3. **Fold literal range guards and write `sequence(a, b - 1)`.** Expected saving: a few K. Also, the TIME type is now there (DBR 18 LTS and Databricks SQL, Beta), so `DATE_AT_TIME` and time-of-day values can be written once the lane's warehouse is checked to have it.

---

## Files
- Re-planned SQL for the five databases (same totals as baseline.tsv): /tmp/claude-0/-home-user-legend-lite/37e604d0-f16d-5c80-a65e-84a7c2121e8e/scratchpad/hbsd/sql.tsv (columns: database, query line, chars, expression, SQL); the dumper is hbsd/src/Dump.java; the extra probes are hbsd/mine.jsonl and hbsd/mine.tsv
- H2 probes and outputs: /tmp/claude-0/-home-user-legend-lite/37e604d0-f16d-5c80-a65e-84a7c2121e8e/scratchpad/hbsd/h2probe/ (p1-p8 .sql; p6.out, p7.out)
- The literal-pick saving estimate: /tmp/claude-0/-home-user-legend-lite/37e604d0-f16d-5c80-a65e-84a7c2121e8e/scratchpad/hbsd/casefold.py
