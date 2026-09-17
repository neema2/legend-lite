# H2 ↔ DuckDB parity — the measured census (2026-09-16)

> **What this is.** The census for the `END_TO_END_PLAN_2026_09_08.md` **Phase 7**
> program ("H2 as compatible as DuckDB") and the `PROGRAM_MAP.md` row **"H2 capability
> parity via Java UDFs"** (NEW 2026-08-29, user). Phase 7 named its input as "the H2
> fail roster at that time"; this document *is* that roster, measured at HEAD, with
> every row attributed to a cause and every cause priced.
>
> **Evidence standard.** Nothing here is quoted from an earlier doc. Every number is
> from a run at `fa8330b75` against the pinned oracles
> (`legend-engine 230c1591`, `legend-pure 7fbc7d6e`), and every capability claim is
> from a statement executed on a real `h2-2.4.240.jar` / `duckdb_jdbc-1.5.0.0.jar`
> in this session. Where this contradicts `H2_BACKEND.md` or `BACKEND_PORTABILITY.md`,
> §3 says so explicitly and shows the probe.
>
> **Headline.** The H2 lane is **332 corpus tests** and **246 PCT tests** behind DuckDB.
> Both failure sets nest exactly — *nothing* passes on H2 and fails on DuckDB. But the
> dominant cause is **not** an H2 limitation: H2 2.4.240 has a complete native `ARRAY`
> type, and legend-lite declares it has none.

---

## 1. The measured gap

### 1.1 PCT — all five suites, both lanes

Gate 7 runs **only** the Relation suite on H2, so the across-the-board number did not
exist before this run. Command in §8.

| Suite | DuckDB pass | H2 pass | H2 fail | H2 error | gap |
|---|---:|---:|---:|---:|---:|
| Standard | 205 / 205 | 116 | 1 | 89 | **89** |
| Essential | 343 / 345 | 257 | 12 | 77 | **86** |
| Relation | 469 / 469 | 442 | 2 | 26 | **27** |
| Unclassified | 93 / 93 | 66 | 1 | 27 | **27** |
| Grammar | 137 / 137 | 120 | 5 | 13 | **17** |
| **total** | **1247 / 1249** | **1001** | **21** | **232** | **246** |

DuckDB's 2 failures are the known Essential ledger rows. Of H2's 21 failures, **5 are
the `PctCensusGate` teardown assertions** (one per suite: "wire divergence grew:
111–288 > 0"), which are lane-census pins rather than PCT rows — so the real PCT
non-passing count is **248**, and the parity gap is **246**.

> The census gate is pinned EQUALITY-0 on both lanes (`PctCensusGate` header: "the
> lanes converged"). It no longer holds on H2 at full-suite composition. That pin was
> measured on the **Relation-only** G7 JVM; counters are cumulative per JVM, so this is
> the wrong-denominator trap that file already documents — not necessarily a regression.
> **It does mean no gate covers PCT-on-H2 outside Relation.**

### 1.2 RCorpus — both lanes, same denominator

`MinimalCorpusTest`, `census declared=2761 excluded=148 discovered=2613`:

| lane | pass | fail | skipped |
|---|---:|---:|---:|
| DuckDB (gate 4) | **2477** | 108 | 14 |
| H2 (gate 5) | **2151** | 440 | 14 |

The committed rosters reproduce exactly (440 / 108), so they are current. Set
arithmetic over them:

- **332** tests fail on H2 and pass on DuckDB
- **0** tests fail on DuckDB and pass on H2
- 108 shared failures

(The pass delta is 326, not 332, because `duckdb-accepted-roster` has 14 entries to
H2's 8 — six rows are ledgered on one lane and not the other.)

### 1.3 The trend is the real alarm

`CARRIER_REDESIGN.md` recorded the H2 lane at **2148/2538 = 98.5% of DuckDB's 2180**
on 2026-08-01. Today it is **2151 / 2477 = 86.8%**.

**H2 gained 3 tests in six weeks; DuckDB gained ~300.** The carrier redesign genuinely
closed the gap once; every subsequent burn has been DuckDB-only, and the gap re-opened
to its widest recorded value. Phase 7 is not a polish item — the lane is diverging.

---

## 2. Anatomy of the gap, by cause

### 2.1 PCT — 248 non-passing rows, every one attributed

| bucket | tests | note |
|---|---:|---|
| **`DialectCapability` walls** | **153** | legend-lite's own loud refusals |
| …`UNNEST` | 45 | correlated explosion |
| …`LIST_GET` | 18 | |
| …`LIST_MAX` / `LIST_MIN` | 14 / 13 | |
| …`LIST_FILTER` | 8 | lambda |
| …`fold` | 6 | lambda |
| …`BIT_AND`/`OR`/`XOR`/`SHIFT_L`/`SHIFT_R` | 11 | **H2 has these natively** — see §4.1 |
| …collection reductions (`STDDEV_SAMP` 4, `VAR_POP`/`VAR_SAMP` 2+2, `STRING_AGG` 2, `QUANTILE_CONT` 1, `STDDEV_POP` 1) | 12 | **H2 has every one natively** as an aggregate |
| …`LIST_AVG`/`MEDIAN`/`MODE`/`POSITION`/`SORT`/`SLICE`/`REVERSE`/`TRANSFORM` | 17 | |
| …`collection forAll` / `exists` / membership | 5 | |
| …`format specifier '%.'` | 3 | no printf on H2 |
| …`signed 64-bit hashCode`, `minimal-fraction date format` | 3 | |
| **Missing SQL function names** | **58** | H2 raises `Function "X" not found` |
| **Other** | **37** | assertion/codec rows, §2.3 |
| **`PctCensusGate`** | 5 | lane pins, not PCT rows |

Missing-function census, by test count (PCT lane):

| function | tests | | function | tests |
|---|---:|---|---|---:|
| `TIME_BUCKET` | 11 | | `MD5` | 2 |
| `LATERAL` | 7 | | `TO_BASE64` | 2 |
| `EPOCH_MS` | 4 | | `JARO_WINKLER_SIMILARITY` | 2 |
| `LEGEND_H2_EXTENSION_SPLIT_PART` | 4 | | `LEVENSHTEIN` | 2 |
| `XOR` | 3 | | `REGEXP_EXTRACT_ALL` | 2 |
| `MAP_CONCAT` | 3 | | `ARG_MAX` / `ARG_MIN` | 1 / 1 |
| `MAKE_TIMESTAMP` | 3 | | `MAKE_DATE`, `CBRT`, `REVERSE` | 1 each |
| `FROM_BASE64` | 3 | | `REGEXP_EXTRACT`, `SHA1`, `SHA256` | 1 each |
| `TIMEZONE` | 2 | | | |

> `LEGEND_H2_EXTENSION_SPLIT_PART` failing is the bug subagent-1 predicted:
> `H2.splitPartCall` emits a UDF call that **only the test harness registers**.
> `H2`/`H2Modern` never override `sessionSetup()`, so on any connection the product
> opens itself, that call cannot resolve. It is already a production defect.

### 2.2 RCorpus — the 332 H2-only rows

| bucket | tests |
|---|---:|
| **Value / codec divergence** (assert shapes, §2.3) | **85** |
| `STRING_SPLIT` not found | 49 |
| `LIST_MIN` wall | 46 |
| `UNNEST` wall | 34 |
| `LIST_GET` wall | 30 |
| variant navigation wall | 19 |
| `REGEXP_EXTRACT` not found | 17 |
| `STRING_AGG` collection reduction wall | 8 |
| sql-text golden assert | 8 |
| `LIST_FILTER` wall | 7 |
| value/text divergence (`expected:`) | 7 |
| nested checked defects wall | 5 |
| struct extraction / literal wall | 4 |
| `TO_BASE64` 3, `MD5` 2, `JSON_PRETTY` 2 | 7 |
| FULL OUTER sort-key, `t.schema` lambda param, misc | 6 |

### 2.3 The 85 value/codec rows are NOT a capability gap

Two root causes dominate, and neither is an H2 feature gap:

1. **NULL in a string column reads back as an empty collection (~45 rows).**
   `groupBy::testMax` — `expected: ['null', 5.0, 'Firm X', …]` vs
   `actual: [[], 5.0, 'Firm X', …]`. Every other cell agrees. The families are
   `calendarAggregations` (48), `groupBy` (30), `tds::groupBy` (15). One codec fix.
2. **Float rendering (~12 rows).** `expected <2 years,1.0> got <2 years,1E+1>` — the
   LEGACY-mode `BigDecimal` carrier that `H2.normalize` already partly handles,
   leaking through the CSV render path.

Plus `no typed conversion for [B` ×7 on the PCT lane — H2 hands `JSON` back as
`byte[]` and the `Pair` decode has no arm for it. **These ~64 rows need three codec
fixes, not one SQL function.**

---

## 2.5 THE STATIC CEILING — what the lowering can emit, not what tests happen to reach

§2 is a **floor**: it counts the first wall each failing test hits. This section is the
**ceiling** — every function the DuckDB dialect can emit, checked against a real H2,
whether or not any test exercises it. It was done because first-wall attribution
provably undercounts (§5.11: `REGEXP_EXTRACT` moved 17→26 untouched), and it found a
class of defect the test-driven view cannot see at all.

**Method.** Extract all 79 `SqlFn → name` rows from `Spellings.java`, execute each name
as a call on `h2-2.1.214` and `h2-2.4.240` and on DuckDB 1.5.0.0, then subtract the
`SqlFn` constants that `H2`/`H2Modern` intercept with a coded arm (17, measured by
reading every `SqlFn.` reference in those two files) and the 3 `Spellings.h2()`
overrides.

| | count |
|---|---:|
| `SqlFn` constants total (`SqlFn.java:10-119`) | **173** |
| …`Spellings` data rows / coded arms | 76 / 97 |
| distinct DuckDB spellings in the table | 79 |
| present on both H2 versions | 39 |
| probe shape failed on DuckDB too (not a finding) | 4 |
| **absent on both H2 versions** | **36** |
| …of those, intercepted by an H2 coded arm or respelling | 12 |
| **…LATENT from the spelling table** | **24** |
| **…LATENT from base coded arms** (executed, below) | **+8** |
| **…LATENT outside the `SqlFn` switch** | **+3** |
| **TOTAL verified silent emissions** | **~35** |
| plus conditional guard fall-throughs | 3 |
| `SqlFn` where H2 raises an honest `DialectCapability` wall | 40 |
| `TypeNames` scalars DuckDB has and H2 lacks | **0 — category empty** |

**None of the 36 differ between 2.1.214 and 2.4.240** — phase Z buys nothing here.

> The **40 walls are fine** — they are honest, loud, and already funnel into the
> declared-gap registry. It is the **~35 silent emissions** that are the ceiling problem,
> and they all trace to one line: `Spellings.h2()` starts from `build()` and overrides
> three rows, instead of starting from an empty, H2-verified map.

### The 24 latent rows, and why they are a distinct problem

| spelling | `SqlFn` | reached by a test today? |
|---|---|---|
| `string_split` | `SPLIT` | 49 corpus |
| `regexp_extract` | `REGEXP_EXTRACT` | 26 corpus |
| `epoch_ms` | `EPOCH_MS` | 4 PCT + 1 corpus |
| `map_concat` | `MAP_CONCAT` | 3 PCT |
| `md5` · `json_pretty` | `MD5` · `JSON_PRETTY` | 2 corpus each |
| `levenshtein` · `jaro_winkler_similarity` · `regexp_extract_all` · `timezone` | | 2 PCT each |
| `cbrt` · `make_date` · `reverse` · `sha1` · `sha256` | | 1 PCT each |
| **`epoch`** | `EPOCH_SECONDS` | **no** |
| **`flatten`** | `LIST_FLATTEN` | **no** |
| **`json_type`** | `JSON_TYPE` | **no** |
| **`map`** | `MAP_FROM_LISTS` | **no** |
| **`map_extract`** | `MAP_EXTRACT` | **no** |
| **`map_from_entries`** | `MAP_FROM_ENTRIES` | **no** |
| **`map_keys`** | `MAP_KEYS` | **no** |
| **`map_values`** | `MAP_VALUES` | **no** |
| **`to_timestamp`** | `FROM_EPOCH_SECONDS` | **no** |

**9 of the 24 are reached by no test on either lane.** They are invisible to §2 and
would have been invisible to the plan. They are not hypothetical: any query using
`toEpochSeconds`, `flatten`, `typeOf` over a variant, or the Map family emits them.

**And they fail the wrong way.** A latent row does not raise `DialectCapability` — it
renders a DuckDB name into H2 SQL and dies at execution with
`Function "MAP_KEYS" not found`. That is a **runtime error masquerading as a data
problem**, and it defeats the project's own tenet: *loud walls over wrong rows*, with
honest gaps in the declared-gap registry. `Spellings.java:27-33` claims these names "are
ABSENT so they fail loud and graduate into the declared-gap registry" — **measured, they
are present in the H2 map** (H2 inherits `build()` and overrides only three), so they
fail at the database instead.

> **Fix the mechanism, not just the rows.** `Spellings.h2()` should start from an
> explicitly H2-verified map rather than inheriting DuckDB's and subtracting three. A
> `SpellingsTest` assertion that every `Spellings.H2` value is a function H2 actually has
> would have caught all 24 at compile time, and would keep catching them as `SqlFn` grows.

### The coded arms — 10 more, found by reading `AnsiSqlRenderer.call()` rather than the table

The spelling table is not the whole vocabulary. `SqlFn` has **173 constants** (not the
~164 `DUCKDB_FUNCTION_COVERAGE.md` claims): **76** are `Spellings` rows and **97** are
coded arms, and `SpellingsTest.everySqlFnClassified` pins that partition exactly. Ten
base coded arms spell DuckDB with no H2 override. **Each was then executed** rather than
inferred:

| `SqlFn` | emitted on H2 | `AnsiSqlRenderer` | verdict |
|---|---|---|---|
| `JSON_MERGE_PATCH` | `json_merge_patch(…)` | `:626` | ❌ not found |
| `MAP_EMPTY` | `MAP {}` | `:694` | ❌ syntax error |
| `BIT_NOT` | `xor(x, -1)` | `:695` | ❌ not found |
| `ENCODE_BASE64` | `to_base64(…)` | `:706` | ❌ not found |
| `DECODE_BASE64` | `decode(from_base64(…))` | `:755` | ❌ not found |
| `MAKE_TIMESTAMP` | `make_timestamp(…)` | `:731` | ❌ not found |
| `TIME_BUCKET` | `time_bucket(to_days(n), …)` | `:744` | ❌ not found |
| `FROM_EPOCH_MS` | `epoch_ms(…)` | `:751` | ❌ not found |
| `GUID` | `CAST(uuid() AS VARCHAR)` | `:710` | ✅ **works — not a gap** |
| `DATE_TRUNC` | `date_trunc('month', x)` | `:723` | ✅ **works — not a gap** |

Plus three silent emissions outside the `SqlFn` switch, all executed:

| node | emitted on H2 | verdict |
|---|---|---|
| `SqlExpr.OrderedListAgg` → `list(x ORDER BY y)` (`:460`, **no dialect hook at all**) | ❌ `Function "LIST" not found` |
| `SqlType.Map` cast → `MAP(K, V)` (`:1051`, ungated) | ❌ `Unknown data type: "MAP"` |
| `SqlType.Array` cast → `INTEGER[]` (`:1050`) | ❌ **syntax error — H2 spells `INTEGER ARRAY`** |

> **The array-cast row is a new finding and it lands on phase G.** The capability matrix
> assumed `T[]` "renders identically" on H2 because H2 has array types. It does not:
> `CAST(ARRAY[1,2] AS INTEGER[])` is a syntax error, while `CARDINALITY(ARRAY[1,2,3])`
> works. So the carrier flip needs a `castTypeName` change for `SqlType.Array` as well as
> an `arrayLit` change — flipping `arrayLit` alone would produce arrays that cannot be cast.

**Five claims from the static read were REFUTED by executing them**, and they are
recorded because a plan built on the unexecuted version would have spent work on
non-problems: `GUID`/`uuid()`, `DATE_TRUNC` with a string unit, `STRING_AGG`, legacy
`MODE(x)`, and `substr` as a `SUBSTRING` alias **all work on H2**. Reading a dialect's
documentation shape is not the same as running it.

**Conditional fall-throughs — the subtlest class of all.** `H2.call()` intercepts
`FORMAT`, `SPLIT_PART` and `DATE_DIFF` **only for literal arguments**:

| `SqlFn` | interception guard | what a non-literal does |
|---|---|---|
| `FORMAT` | `a.get(0) instanceof SqlExpr.StringLit` (`H2:200`) | falls through to `printf(…)` |
| `SPLIT_PART` | 1-char `StringLit` separator **and** `IntLit` index ≥ 1 (`H2:285-288`) | falls through to `split_part(…)` |
| `DATE_DIFF` | `StringLit` unit (`H2:252`) | falls through to `date_diff(…)` |

A static "is it intercepted?" check says yes; at runtime a dynamic template, separator or
unit emits the DuckDB name. **No test currently exercises the dynamic form**, so this is
invisible to every measurement in this document except this one.

### Aggregates and window functions — the same check

All 38 `SqlAgg.Fn` constants executed as SQL text (`AnsiSqlRenderer.reducer` renders the
**enum constant name** as the function name unless a dialect overrides it):

| | |
|---|---|
| accepted by DuckDB and both H2 versions | **30** |
| rejected by both H2 versions | **6** — `LIST`, `QUANTILE_CONT`, `QUANTILE_DISC`, `ARG_MAX`, `ARG_MIN`, `WAVG` |
| 2.1.214 rejects, 2.4.240 accepts | **1** — `ANY_VALUE` *(another phase-Z win)* |

`H2.reducer:491` already special-cases `LIST` and `QUANTILE_CONT`, so the true latent
aggregate set is **four**: `QUANTILE_DISC`, `ARG_MAX`, `ARG_MIN`, `WAVG`. `ARG_MAX` and
`ARG_MIN` need `CREATE AGGREGATE` (proven in §3.5); the other two are expressible.

> **This corrects §5.2 (phase B).** B2 was sized at ~12 PCT rows on the theory that
> `STDDEV_SAMP`, `VAR_POP`, `MEDIAN`, `MODE` and `STRING_AGG` wall because the aggregate
> renderer has no per-dialect table. **Measured, H2 accepts every one of those names.**
> Those rows wall in `reduceCollection` — the *collection* path (`AnsiSqlRenderer:528`),
> not the aggregate path — so they belong to **phase F**, not phase B. B2's real scope is
> the four names above, and its value is the mechanism, not the row count.

---

## 3. What H2 2.4.240 actually does — and three stale claims corrected

Probe battery: 110 statements on H2 2.4.240 and DuckDB 1.5.0.0, same script
(§8). **The central finding:**

### 3.1 H2 has a complete native ARRAY type

| probe | H2 2.4.240 | DuckDB |
|---|---|---|
| `ARRAY[1,2,3]` | ✅ | ✅ |
| `ARRAY[ARRAY[1,2],ARRAY[3]]` (jagged nested) | ✅ | ✅ |
| `CARDINALITY(ARRAY[ARRAY[1,2],ARRAY[3,4]])` | ✅ **= 2** | ❌ no `CARDINALITY` |
| `(ARRAY[10,20,30])[1]` 1-based index | ✅ | ✅ |
| `ARRAY_AGG(id ORDER BY id DESC)` | ✅ | ✅ |
| `UNNEST(ARRAY[1,2,3])` standalone | ✅ | ✅ |
| `UNNEST(...) WITH ORDINALITY` | ✅ | ✅ |
| `ARRAY_CONTAINS`/`ARRAY_SLICE`/`ARRAY_CAT`/`\|\|` | ✅ | ✅ |
| `2 = ANY(ARRAY[1,2,3])` | ✅ | ✅ |
| `SELECT (SELECT SUM(x) FROM UNNEST(ARRAY[1,2,3]) u(x))` | ✅ | ✅ |
| `ARRAY_SORT` / `ARRAY_POSITION` / `ARRAY_REVERSE` | ❌ | ✅ |
| `LIST_TRANSFORM` / `LIST_FILTER` / `LIST_REDUCE` (lambda) | ❌ | ✅ |

`CarrierStrategies.java:56-62` declares `Caps.H2 = (false, false, false)` —
`nativeLists()` false. **That is the single highest-leverage wrong constant in the
tree.** Every `LIST_*` and `UNNEST` wall in §2 dispatches off it. `H2.arrayLit`
correspondingly emits `JSON_ARRAY(...)` instead of `ARRAY[...]`, so the lane runs on a
JSON carrier it does not need, and pays the `byte[]` read-back of §2.3 for it.

**Correction 1.** `BACKEND_PORTABILITY.md` §3 rejects native arrays as a carrier
because "`ARRAY[ARRAY[1,2],ARRAY[3]]` → ERROR, `cardinality` returns 4 not 2,
`unnest` flattens all levels". **Those four measurements are Postgres's**, in a
section arguing against Postgres's array type. They were never true of H2, and H2
gets `CARDINALITY` right where DuckDB has no such function at all. The
`List<List<T>>` objection does not apply to H2.

### 3.2 The one real structural gap is *correlation*

| probe | H2 |
|---|---|
| `FROM t, UNNEST(t.arr) AS u(x)` (real ARRAY column) | ❌ `Column "T.ARR" not found` |
| `FROM t JOIN UNNEST(t.arr) u(x) ON 1=1` | ❌ same |
| `FROM (SELECT id, arr FROM t) s, UNNEST(s.arr)` | ❌ same |
| `SELECT (SELECT SUM(x) FROM UNNEST(t.arr) u(x)) FROM t` | ❌ same |
| `LATERAL` in any position | ❌ `Function "LATERAL" not found` |
| **Java table function** `FROM t, explode(t.arr) e` | ❌ `Column "T.ARR" not found` |

**Correction 2.** This confirms `H2_BACKEND.md` D1 at 2.4.240, and extends it: the
**`CREATE ALIAS` table-function route does not escape it either.** A `ResultSet`-returning
Java alias is as un-correlatable as `UNNEST`. No Java UDF can buy correlated explosion.

### 3.3 …but correlated explosion is reachable anyway

H2 cannot put a row's column *inside* a table function. It can join against a
row-source and index the array — which is the same thing:

```sql
-- correlated explosion, no LATERAL, no UNNEST
SELECT t.id, s.n AS ord, t.arr[s.n] AS x
FROM t JOIN SYSTEM_RANGE(1, 10) s(n)
  ON s.n <= CARDINALITY(t.arr)
```
✅ `1|1|10; 2|1|20; 1|2|11` — with ordinality, for free.

Every shape the carrier needs was executed:

| shape | result |
|---|---|
| explode → filter → re-aggregate | ✅ (must explode in a **subselect**, then filter outside — see hazard) |
| explode → `GROUP BY` → `SUM` | ✅ |
| **empty/NULL collection preserved** | ✅ `1\|10; 1\|11; 2\|20; 3\|null; 4\|null` |
| dynamic bound `SYSTEM_RANGE(1, (SELECT MAX(CARDINALITY(arr)) FROM t))` | ✅ |
| nested `ARRAY ARRAY` outer level | ✅ |
| `SYSTEM_RANGE(1, 100000)` | ✅ |

**Correction 3.** `BACKEND_PORTABILITY.md` §2 says H2 has **"none"** of the
mechanisms for correlated row explosion, and that the empty-collection case — "the one
graph fetch depends on" — holds on the other three backends but not H2. **Both
statements are false at 2.4.240.** The cardinality-bounded ordinal join does it,
including empty preservation, which is exactly the recipe `LateralExplodeToUnion`
already half-implements (it decorrelates to a `_ROWID_`-keyed `UNION ALL`, but only
for *literal* element lists).

**Two hazards, both probed:**
- `arr[n]` **out of bounds raises on H2** (`Array element error: "5", expected "1..2"`)
  where DuckDB returns NULL. Every index must be guarded
  (`CASE WHEN s.n <= CARDINALITY(arr) THEN arr[s.n] END`).
- Filtering on `arr[s.n]` **in the same `WHERE` as the join** lets H2 evaluate the
  index before the join predicate and raise. Explode in a subselect, filter outside.
  Both forms verified green.

### 3.4 H2 2.5.250 buys us NOTHING — measured, not read

H2 **2.5.250** shipped 2026-08-29, after every doc in this corpus was written. It is
not a candidate for a version bump:

- **All four probe batteries re-run on a real `h2-2.5.250.jar` are byte-identical to
  2.4.240.** The 110-statement capability battery diffs clean (45 errors on each, same
  45); `LATERAL`, correlated `UNNEST`, the Java table function, the array signatures,
  the ordinal-join explode and every missing function name behave exactly as on 2.4.240.
- **The whole difference between the two versions' documented SQL surface is a typo
  fix.** Diffing H2's own `help.csv` (270,578 vs 270,598 bytes, extracted from the
  nested `org/h2/util/data.zip`): two lines, "The garbage is run before returning the
  value" → "The garbage **collector** is run…". No function added, no syntax added.

The 2.5.250 changelog is data races, index selection and storage corruption. **Nothing
in §3.2's structural gap moves, and nothing in §4.2's missing-function list is filled.**
Stay on the 2.1.214 / 2.4.240 split the lane already has.

> Method note: the first attempt at this cross-check read `org/h2/res/help.csv`, which
> does not exist in these jars — it returned **0 bytes** and reported every function
> "absent", including `CARDINALITY` and `BITAND`, which §3.1 had already *executed*
> successfully. A check that passes on an empty file proves nothing. The real resource
> is nested inside `org/h2/util/data.zip`.

### 3.45 LANDMINE — aggregating over `UNNEST(<function call>)` returns NULLs, silently

Probed on **2.1.214 and 2.4.240**, with a `String[]`-returning alias `ssplit`:

| | result |
|---|---|
| `SELECT x FROM UNNEST(ssplit('c,a,b', ',')) u(x)` | ✅ `c; a; b` |
| `SELECT COUNT(*), COUNT(x) FROM UNNEST(ssplit(...)) u(x)` | ✅ `3 \| 3` |
| `SELECT ARRAY_AGG(x) FROM UNNEST(ssplit(...)) u(x)` | ❌ **`[null, null, null]`** |
| `SELECT MIN(x) FROM UNNEST(ssplit(...)) u(x)` | ❌ **`null`** |
| `SELECT ARRAY_AGG(v) FROM (SELECT x AS v FROM UNNEST(ssplit(...)) u(x)) q` | ❌ `[null, null, null]` |
| `SELECT ARRAY_AGG(x) FROM UNNEST(ARRAY['c','a','b']) u(x)` *(literal)* | ✅ `[c, a, b]` |
| `... FROM UNNEST(CAST(ssplit(...) AS VARCHAR ARRAY)) u(x)` | ✅ on **2.4.240** · ❌ on 2.1.214 |
| the **ordinal-join** route (`arr[r.n]` + `SYSTEM_RANGE`), then `ARRAY_AGG` | ✅ **both versions** |

**Wrong rows, no error.** `COUNT` sees three non-null rows while `MIN` and `ARRAY_AGG` over the
same column see nulls, so nothing raises and nothing is skipped — the query just returns the
wrong answer. It is specific to `UNNEST` over a **function-call** argument; a literal array is
fine, and interposing a subselect does not rescue it.

**Consequence for the plan, and it is not cosmetic:** the natural reading of phase F — "a Java
UDF returns an array, then `UNNEST` it and aggregate" — is **exactly this bug**. Phase F and
phase H must both explode through the §3.3 ordinal join, which is correct on both versions, and
`UNNEST(<call>)` under an aggregate must never be emitted. Given the project's "loud walls over
wrong rows" tenet, a renderer assertion forbidding that shape is worth more than a comment.

### 3.5 Java-in-SQL: what the `CREATE ALIAS` seam actually supports

| capability | H2 2.4.240 |
|---|---|
| `CREATE ALIAS f FOR "cls.method"`, scalar, called on columns | ✅ |
| Parameter `Integer[]` (an H2 `ARRAY`) | ✅ |
| Parameter `java.sql.Array` | ✅ |
| Parameter `Object[]` / `Object` / `int[]` | ❌ `Data conversion error … JAVA_OBJECT` |
| **Return `Integer[]`, usable as a real ARRAY** (`CARDINALITY`, `UNNEST` over it) | ✅ |
| Fed by `ARRAY_AGG(...)` as the argument | ✅ |
| `CREATE AGGREGATE f FOR "cls$Impl"` (`org.h2.api.Aggregate`) | ✅ |
| Table function (`ResultSet`) in a **correlated** position | ❌ |

**`Integer[]` in, `Integer[]` out is the whole ballgame.** A Java UDF can take an H2
array, return an H2 array, and the result is a first-class array the SQL around it can
index, measure and unnest. That is enough to implement the entire lambda-free `LIST_*`
family in Java without touching the carrier.

---

## 4. The Java-behind-H2 catalogue

Ordered by yield. "Tests" = rows this unblocks, PCT + corpus, from §2.

### 4.1 Tier 0 — no Java at all, just a spelling (≈23 PCT rows)

These fail only because `H2` does not override the rendering. H2 has them natively.

| SqlFn | H2 spelling (probed) | tests |
|---|---|---:|
| `BIT_AND` / `BIT_OR` / `BIT_XOR` | `BITAND(a,b)` / `BITOR` / `BITXOR` | 9 |
| `BIT_SHIFT_LEFT` / `RIGHT` | `LSHIFT(a,n)` / `RSHIFT(a,n)` | 2 |
| `XOR` | `BITXOR(a,b)` | 3 |
| collection reductions `STDDEV_SAMP`, `STDDEV_POP`, `VAR_SAMP`, `VAR_POP`, `MEDIAN`, `MODE`, `STRING_AGG`, `QUANTILE_CONT` | all native aggregates on H2 (`PERCENTILE_CONT … WITHIN GROUP` for the last) | 12 |
| `REVERSE` | no H2 builtin → Tier 1 | — |

`AnsiSqlRenderer.reducer` renders **the `SqlAgg.Fn` enum constant name as the SQL
text** — there is no `Spellings`-equivalent table for aggregates. That is why these
land as "reached a dialect without a list encoding" rather than as a spelling miss.
A per-dialect aggregate spelling map is the fix, and it is the same defect
`BACKEND_PORTABILITY.md` §5.2 filed against every non-H2 backend.

### 4.2 Tier 1 — scalar Java UDFs, direct ports (≈33 PCT + ≈71 corpus)

**"Scalar Java UDF" means exactly this**, and the mechanism is already proven in-tree
(`H2ExtensionFunctions`, test scope) and re-probed in §3.5 — we write a plain static
Java method and name it to H2:

```java
// core/src/main/java/com/legend/sql/dialect/h2/H2Functions.java  (new, src/main)
public static @Nullable String[] string_split(@Nullable String s, @Nullable String sep) {
    return s == null || sep == null ? null : s.split(java.util.regex.Pattern.quote(sep), -1);
}
```
```java
// H2.sessionSetup() — the product-side seam (END_TO_END_PLAN Phase 7)
@Override public List<String> sessionSetup() {
    return List.of(
        "CREATE ALIAS IF NOT EXISTS string_split FOR \"com.legend.sql.dialect.h2.H2Functions.string_split\"",
        ...);
}
```
Then `Spellings.H2` maps `SqlFn.STRING_SPLIT → "string_split"` and the renderer is
unchanged. No H2 fork, no native code, no server; `CREATE ALIAS` is enabled by default
and `h2.allowedClasses` defaults to `*`. Semantics come from the engine's own
`LegendH2Extensions` where one exists (nine of these are already written), and from the
DuckDB function we are matching where one does not — the PCT/corpus row is the oracle
either way.

The one caveat that shapes the tier boundaries: **parameter and return types must be
`Integer[]`/`String[]`/`java.sql.Array`, never `Object[]`, `Object` or `int[]`** (§3.5).
That is why Tier 2 is a separate tier rather than more of this one.

**`STRING_SPLIT` is verified end-to-end on both H2 versions**, since it is the single
biggest corpus lever and rested on an untested return type. With the alias above:

| | |
|---|---|
| `string_split('a,b,c', ',')` | ✅ `[a, b, c]` |
| `string_split(s, ',')` over a column, NULL row included | ✅ `[a,b,c]; [x]; null` |
| `CARDINALITY(string_split(...))` | ✅ `3` |
| `(string_split('a,b,c', ','))[2]` | ✅ `b` |
| explode via the §3.3 ordinal join | ✅ `1\|a; 2\|x; 1\|b; 1\|c` |
| **`ARRAY_AGG` over `UNNEST(string_split(...))`** | ❌ **silent NULLs — §3.45** |

`Double[]`, `BigDecimal[]` and `String[]` parameters all bind, arrays containing NULL
elements round-trip intact, and a `String[]` parameter fed by `ARRAY_AGG(col)` works.

Nine of these **already exist** in `spec/src/test/java/com/legend/harness/H2ExtensionFunctions.java`
and need only to move to `src/main` and be registered from `H2.sessionSetup()`.

| function | Java | status | tests |
|---|---|---|---:|
| `STRING_SPLIT(s, sep) → VARCHAR[]` | `String.split` → `String[]` | new; **return-array proven** | **49 corpus** |
| `TIME_BUCKET(interval, ts[, origin])` | `java.time` floor-to-bucket | new | **11 PCT** |
| `REGEXP_EXTRACT(s, p[, g])` | `Pattern`/`Matcher` | new | 1 PCT + **17 corpus** |
| `REGEXP_EXTRACT_ALL(s, p) → VARCHAR[]` | `Matcher` loop → `String[]` | new | 2 PCT |
| `SPLIT_PART(s, tok, n)` | **exists** (`legend_h2_extension_split_part`) | **port** | 4 PCT |
| `TO_BASE64` / `FROM_BASE64` | **exist** (`_base64_encode/_decode`) | **port** | 5 PCT + 3 corpus |
| `MD5` | **exists** (`_hash_md5`); or H2 `HASH('MD5',…)` | **port** | 2 PCT + 2 corpus |
| `SHA1` / `SHA256` | `MessageDigest`; or H2 `HASH(…)` | new | 2 PCT |
| `LEVENSHTEIN` | **exists** (`_edit_distance`) | **port** | 2 PCT |
| `JARO_WINKLER_SIMILARITY` | **exists** | **port** | 2 PCT |
| `REVERSE` | **exists** (`_reverse_string`) | **port** | 1 PCT |
| `EPOCH_MS(ts)` | `toInstant().toEpochMilli()` | new | 4 PCT |
| `MAKE_DATE` / `MAKE_TIMESTAMP` | `LocalDate.of` / `LocalDateTime.of` | new | 4 PCT |
| `TIMEZONE(zone, ts)` | `ZonedDateTime` | new | 2 PCT |
| `PRINTF` / `%.Nf` format | `String.format` | new | 3 PCT |
| `CBRT` | `Math.cbrt` | new | 1 PCT |
| `JSON_PRETTY` | any JSON writer | new | 2 corpus |
| signed 64-bit `hashCode` | `Long.hashCode` semantics | new | 2 PCT |
| `LPAD` / `RPAD` | **exist** | port (no failing row today) | — |

### 4.3 Tier 2 — Java UDFs over `Integer[]`/`Double[]` (the lambda-free `LIST_*`) — ≈75 PCT + ≈76 corpus

Signature proven in §3.4: `Integer[] → Integer`, `Integer[] → Integer[]`.

| SqlFn | route | tests |
|---|---|---:|
| `LIST_MIN` / `LIST_MAX` | Java over `Integer[]` — or H2 `ARRAY_AGG`+`MIN` after §4.4 | 27 PCT + **46 corpus** |
| `LIST_GET` | native `arr[n]` **with the OOB CASE guard** (§3.3) | 18 PCT + 30 corpus |
| `LIST_AVG` / `LIST_MEDIAN` / `LIST_MODE` / `LIST_SUM` / `LIST_PRODUCT` | Java scalar over the array | 8 PCT + 1 corpus |
| `LIST_SORT` / `LIST_REVERSE` / `LIST_POSITION` / `LIST_SLICE` | Java array→array; `ARRAY_SLICE` is native | 6 PCT |
| `LIST_LENGTH` | native `CARDINALITY` (already in `H2Modern`) | — |
| `LIST_CONTAINS` / membership | native `= ANY(arr)` or `ARRAY_CONTAINS` | 1 PCT |
| `STRING_AGG` over a collection | Java `Integer[]/String[] → String` | 2 PCT + 8 corpus |

`LIST_MIN` alone is 46 corpus rows — the single largest corpus lever after
`STRING_SPLIT`.

### 4.4 Tier 3 — the carrier flip and the ordinal-join explode (≈54 PCT + ≈41 corpus)

This is the only structural work, and §3.3 shows it is reachable.

1. **`Caps.H2 → nativeLists = true`**; `H2.arrayLit` emits `ARRAY[...]` not
   `JSON_ARRAY(...)`. This alone re-points most of Tier 2 at native SQL and removes
   the `byte[]` read-back that causes §2.3's codec rows.
2. **Generalise `LateralExplodeToUnion`** from literal element lists to the
   cardinality-bounded ordinal join of §3.3, with the OOB guard and the
   explode-in-subselect rule. Unblocks `UNNEST` (45 PCT + 34 corpus).
3. **Lambda-carrying `LIST_FILTER` / `LIST_TRANSFORM` / `fold` / `forAll` / `exists`**
   (≈23 PCT + 7 corpus) lower to explode → predicate/projection in ordinary SQL →
   re-`ARRAY_AGG`. H2 has no lambdas and no Java UDF can supply one (the lambda body is
   legend-lite IR, not a value), so this is the genuine rewrite. It is also the shape
   `BACKEND_PORTABILITY.md` §2.1 already argues for on every backend.

### 4.5 Tier 4 — codec fixes, no SQL involved (≈64 rows)

The §2.3 families: NULL-string → `[]` (~45 corpus), `1E+1` float render (~12 corpus),
`byte[]`→`Pair` typed conversion (7 PCT). Three `normalize`/decode fixes.

### 4.6 What no amount of Java buys

| | rows | why |
|---|---:|---|
| Correlated table function / `LATERAL` in a FROM-position derived table | ~4 | probed impossible incl. the Java table-function route (§3.2); the graph-envelope isolation shape |
| sql-text golden asserts | 8 corpus | harness verdict policy, not execution |
| `MAP` type (`MAP_CONCAT`, `map_from_entries`) | 3 PCT | H2 has no `MAP`; a JSON-object encoding via Java UDF is possible but is a carrier decision, not a function |
| Variant/JSON **object field** navigation on stock H2 2.1 | 19 corpus | `H2Modern` has `(x)."key"`; on 2.1.214 the engine's own route is a Java UDF (`legend_h2_extension_json_navigate`) — available to us once the UDF seam exists, so this is Tier 1-able **if** the lane targets 2.4.240 |

---

## 5. THE PLAN

Yields are the §2 attributions summed. They are **upper bounds** — a test can carry two
causes, so a row fixed in phase C may also have needed phase F. Treat them as ordering
evidence, not a forecast. Nothing below is scheduled; the phases are ordered by
dependency and by evidence-per-unit-risk.

### 5.0 The two lanes run DIFFERENT H2 versions — read this first

| lane | gate | H2 version | dialect class |
|---|---|---|---|
| RCorpus | 5 | **2.1.214** (`core/pom.xml`, `spec/pom.xml`) | `H2` |
| PCT | 7 | **2.4.240** (`-Dh2.version`) | `H2Modern` |

`Compiler.dialectOf:654-657` picks `H2` for a connected 2.1/2.2 and `H2Modern`
otherwise. **Every capability claim in this document was re-probed on BOTH versions.**
The batteries are identical except for exactly two things:

| | 2.1.214 | 2.4.240 |
|---|---|---|
| `2 = ANY(ARRAY[...])` | ❌ `ARRAY to BOOLEAN` | ✅ |
| `ARRAY_CONTAINS(arr, v)` | ✅ | ✅ |
| `(JSON '{"a":1}')."a"` (quoted key) | ❌ | ✅ **= 1** |
| `(JSON '[10,20,30]')[2]` | ❌ | ✅ |

So: **native arrays, `ARRAY_AGG`, `UNNEST`, `CARDINALITY`, the ordinal-join explode,
`CREATE ALIAS`, `CREATE AGGREGATE` and the `Integer[]` in/out signature all work
identically on 2.1.214.** Phases A–H below are version-independent. Only membership
must spell `ARRAY_CONTAINS` rather than `= ANY` to serve both.

> **Correction 5 (to `H2_BACKEND.md`'s addendum).** That addendum concluded JSON object
> field access "exists in NO syntax" at 2.4.240 and that the version bump "buys array
> indexing only". Probed here: `(JSON '{"a":1}')."a"` returns `1`. The **quoting is
> load-bearing** — bare `.a` returns NULL silently, which is how the addendum's syntax
> battery missed it. `CARRIER_REDESIGN.md`'s MODERN PROFILE already recorded the quoted
> form; the two docs contradict each other and CARRIER_REDESIGN is right.
>
> Consequence: the **19 variant-navigation corpus rows are impossible natively on
> 2.1.214 and native on 2.4.240.** That makes the corpus lane's H2 version a real
> decision — see §5.9.

### 5.1 Phase A — build the seam, stop the drift *(S; 0 rows directly, unblocks everything)*

**A1. The `sessionSetup()` UDF seam.** Add a `sessionSetup()` override to `H2` that
issues `CREATE ALIAS IF NOT EXISTS` for each registered function, and move
`H2ExtensionFunctions` from `spec/src/test` to `core/src/main`. `H2Modern extends H2`
inherits it. `SqlDialect.sessionSetup()` already exists and is already executed at
`Compiler.dialectOf`'s seam (`:625-629`, `:658-660`) — **no new plumbing**.

*This is a bug fix, not just an enabler.* `H2.splitPartCall:671` already emits
`legend_h2_extension_split_part(...)`, and nothing in `src/main` registers it. Any H2
connection the product opens itself (`ConnectionResolver:194`) fails that call today.

*Verified by:* a new core test that opens a product-path H2 connection and calls each
alias. Not by a corpus delta — A1 alone moves few rows.

**A2. Widen gate 7 to all five PCT suites.** It runs `Test_LegendLite_RelationFunctions_PCT`
only, so **219 of the 246 PCT gap rows are in no gate at all** and the lane can keep
drifting silently. Change `-Dtest=` to the five suites and pin the measured ceilings
(run 1249, fail ≤ 21, err ≤ 232), ratcheting down as phases land.

Do A2 **first**. It is an hour's work and it is what makes every later phase's claim
checkable.

> **Also fix `H2Settings` on the production path.** `ConnectionResolver:194-212` builds
> `jdbc:h2:...` URLs **without** `H2Settings.SETTINGS`, so a product-opened H2 session
> has different keyword, mode and null-ordering semantics than every tested one. Same
> phase, same reason.

### 5.2 Phase B — spellings; no Java at all *(S; ~23 PCT + ~8 corpus)*

**B1. `H2.bitOp()` override.** `AnsiSqlRenderer:807` walls because `H2` never overrides
`bitOp`. H2 has the operations natively, under different names (probed):
`BIT_AND→BITAND`, `BIT_OR→BITOR`, `BIT_XOR→BITXOR`, `BIT_SHIFT_LEFT→LSHIFT`,
`BIT_SHIFT_RIGHT→RSHIFT`, and `XOR→BITXOR`. **11 PCT rows, one small override.**

**B2. A per-dialect aggregate spelling table.** `AnsiSqlRenderer.reducer:950` renders
`r.fn() + "(" + ... + ")"` — **the `SqlAgg.Fn` enum constant name IS the SQL text.**
There is no `Spellings`-equivalent for aggregates, which is why `STDDEV_SAMP`,
`VAR_POP`, `MEDIAN`, `MODE`, `STRING_AGG`, `QUANTILE_CONT` surface as "reached a
dialect without a list encoding" rather than as a spelling miss. H2 has every one of
them natively (`PERCENTILE_CONT(q) WITHIN GROUP (ORDER BY v)` for the last, already
hand-coded at `H2.reducer:491`).

Add `Spellings`-style `Map<SqlAgg.Fn, String>` per dialect and let `reducer()` consult
it. **~12 PCT rows**, and it closes the same defect `BACKEND_PORTABILITY` §5.2 filed
against *every* non-H2 backend — so it is worth more than its H2 yield.

*Verified by:* gate 7 (widened) error count drops by ~23; gate 4 unchanged.

### 5.3 Phase C — scalar Java UDFs *(M; ~33 PCT + ~71 corpus)*

Mechanism and code shape in §4.2. Two batches:

**C1. Port the nine already written** (`split_part`, `base64_encode/decode`, `hash_md5`,
`reverse_string`, `lpad`, `rpad`, `edit_distance`, `jaro_winkler_similarity`) from test
scope to `src/main`, registered by A1. Semantics are already the engine's, verbatim.

**C2. Write the new ones**, in yield order:

| function | rows | note |
|---|---:|---|
| `STRING_SPLIT → VARCHAR[]` | **49 corpus** | the single biggest corpus lever; returns a real ARRAY (§3.5) |
| `TIME_BUCKET` | **11 PCT** | biggest PCT lever in this phase |
| `REGEXP_EXTRACT` | 1 PCT + **17 corpus** | |
| `EPOCH_MS`, `MAKE_DATE`, `MAKE_TIMESTAMP`, `TIMEZONE` | 10 PCT | `java.time` |
| `PRINTF`/`%.Nf` | 3 PCT | H2 has no printf; `String.format` |
| `SHA1`, `SHA256`, `CBRT`, `REGEXP_EXTRACT_ALL`, `JSON_PRETTY`, signed-64 `hashCode` | 10 | |

*Oracle for semantics:* the engine's `LegendH2Extensions` where one exists; otherwise
the DuckDB function being matched. The PCT/corpus row decides either way.

*Verified by:* gates 5 and 7 both move; `Function "X" not found` disappears from both
logs (today: 49 `STRING_SPLIT`, 17 `REGEXP_EXTRACT` on the corpus lane).

### 5.4 Phase D — codec fixes; no SQL involved *(S–M; ~7 PCT + ~57 corpus)*

These are **not** capability gaps and do not need any of the above. They can run in
parallel with B and C by a second person.

**D1. NULL-in-a-string-column reads back as an empty collection — ~45 corpus rows in
one root cause.** `groupBy::testMax` expects `['null', 5.0, 'Firm X', …]` and gets
`[[], 5.0, 'Firm X', …]`; every other cell agrees. Families: `calendarAggregations`
(48), `groupBy` (30), `tds::groupBy` (15). **Diagnose this one row first** — it is the
best rows-per-hour in the entire document.

**D2. Float rendering, ~12 corpus rows.** `expected <2 years,1.0> got <2 years,1E+1>` —
the LEGACY-mode `BigDecimal` carrier `H2.normalize:651` already partly handles, leaking
through the CSV render path.

**D3. `no typed conversion for [B`, 7 PCT rows.** H2 returns `JSON` as `byte[]`; the
`Pair` decode has no arm for it. `H2.normalize` already has the byte[]→String row for
the JSON case; extend it to the typed-conversion path.

### 5.5 Phase E — split the conflated capability *(S; 0 rows, but phase G is unsafe without it)*

**This is the finding that reorders the back half of the plan.**

`CarrierStrategies.Caps` declares three booleans. Across **all** of `core/src/main`
there are exactly three reads, and all three are `nativeLists`:

| site | what it actually gates |
|---|---|
| `CarrierStrategies:83` `select()` | **FULL OUTER JOIN emulation** |
| `CarrierStrategies:232` `source()` | **ASOF join emulation + static PIVOT emulation** |
| `CarrierStrategies:654` `expr()` | the list/carrier strategies |

`correlatedExplode` and `jsonCarrier` are **declared and never read anywhere** — dead
fields that make the record look more expressive than it is.

So `nativeLists` is a **single master switch over four unrelated emulations**. Setting
`Caps.H2.nativeLists = true` (phase G) would silently switch off FULL OUTER, ASOF and
PIVOT emulation for H2 — all three of which H2 genuinely needs (H2 rejects `FULL OUTER
JOIN` outright; probed) — and the FULL OUTER emulation is recent, deliberate work
(batch 125).

**E1.** Replace the record with capabilities that mean what they say —
`nativeLists`, `supportsFullOuterJoin`, `supportsAsOfJoin`, `supportsNativePivot` —
gate each site on its own, and either wire or delete the two dead fields. `Caps.H2`
becomes `(false, false, false, false)` with identical behaviour; `Caps.DUCKDB` all
true. **A pure refactor with no intended behaviour change**, which is exactly why it
should land on its own, gated, before G.

### 5.6 Phase F — array Java UDFs *(M; ~75 PCT + ~76 corpus)*

Signature proven in §3.5 on all three H2 versions: `Integer[]` in, `Integer[]` out, and
the returned array is first-class (`CARDINALITY` and `UNNEST` work over it).

| SqlFn | route | rows |
|---|---|---:|
| `LIST_MIN` / `LIST_MAX` | Java over `Integer[]` | 27 PCT + **46 corpus** |
| `LIST_GET` | native `arr[n]` **with the OOB guard** | 18 PCT + 30 corpus |
| `LIST_AVG`/`MEDIAN`/`MODE`/`SUM`/`PRODUCT` | Java scalar | 8 PCT + 1 corpus |
| `LIST_SORT`/`REVERSE`/`POSITION`/`SLICE` | Java array→array; `ARRAY_SLICE` native | 6 PCT |
| `LIST_CONTAINS` / membership | **`ARRAY_CONTAINS`** (not `= ANY` — §5.0) | 1 PCT |
| `STRING_AGG` over a collection | Java `String[] → String` | 2 PCT + 8 corpus |
| `LIST_LENGTH` | native `CARDINALITY` (already in `H2Modern`) | — |

**The mandatory guard:** `arr[n]` out of range **raises** on H2 (`Array element error:
"5", expected "1..2"`) where DuckDB returns NULL. Every generated index must be
`CASE WHEN n <= CARDINALITY(arr) THEN arr[n] END`. This is a correctness landmine, not
a style note — it turns a NULL into a thrown query.

F can precede G: these are function calls over whatever carrier is in play.

### 5.7 Phase G — the carrier flip *(M; 0 rows directly, high blast radius)*

Set `Caps.H2.nativeLists = true` (safe only after E) and change `H2.arrayLit:355` from
`JSON_ARRAY(...)` to `ARRAY[...]`.

Today the H2 lane carries collections as **JSON**, which is why it pays the `byte[]`
read-back of D3 and the `ABSENT ON NULL` mismatch. H2 has a full native `ARRAY`
(§3.1) on **every** version in play. Flipping the carrier re-points phase F's work at
native SQL and removes a class of codec problems rather than patching them.

**Sequencing note:** `CAST(JSON '[1,2,3]' AS INT ARRAY)` **fails on both H2 versions**
(probed) — there is no cheap JSON↔ARRAY bridge, so the carrier cannot be flipped
half-way. G is a single atomic change with a full two-gate verification, and it is the
one phase that warrants its own batch and its own rollback plan.

### 5.8 Phase H/I — correlated explosion, then lambdas *(L; ~68 PCT + ~41 corpus)*

**H. Generalise `LateralExplodeToUnion` to the ordinal join.** The pass already
decorrelates lateral `UNNEST` into a `_ROWID_`-keyed `UNION ALL`, but only for
*literal* element lists. Replace that with the general, probed recipe:

```sql
SELECT t.id, s.n AS ord,
       CASE WHEN s.n <= CARDINALITY(t.arr) THEN t.arr[s.n] END AS x
FROM t JOIN SYSTEM_RANGE(1, <bound>) s(n)
  ON s.n <= COALESCE(CARDINALITY(t.arr), 0)
```

Bound from `(SELECT MAX(CARDINALITY(arr)) FROM t)` (probed) or a static cap.
Empty/NULL collections are preserved with the `LEFT JOIN` + guard form (probed:
`3|null; 4|null`). **Unblocks `UNNEST`: 45 PCT + 34 corpus.**

Two hazards, both probed, both must be encoded in the pass:
1. the OOB guard of §5.6;
2. **explode in a subselect, filter outside** — filtering on `arr[s.n]` in the join's
   own `WHERE` lets H2 evaluate the index before the join predicate and raise.

**I. Lambda lowering** (`LIST_FILTER`, `LIST_TRANSFORM`, `fold`, `forAll`, `exists` —
~23 PCT + 7 corpus). H2 has no lambdas and **no Java UDF can supply one**: the lambda
body is legend-lite IR, not a value that can cross the JDBC boundary. These must lower
to explode → ordinary SQL predicate/projection → re-`ARRAY_AGG`, which is why I follows
H. This is also the shape `BACKEND_PORTABILITY` §2.1 argues for on every backend, so
the work is not H2-specific.

### 5.9 Phase Z — bump the corpus lane to H2 2.4.240. MEASURED: **+59 net, and it is the single biggest cheap win in this document**

This section originally recommended *not* bumping, on the grounds that the engine's
goldens were produced on a **forked** 2.1.214 (`H2_BACKEND.md` §7: `charPadding =
NEVER`, numeric↔boolean comparison patches) and the corpus lane replays them in the
same session — so changing the engine under the replay oracle looked like an
uncontrolled risk, and a Java `json_navigate` UDF looked like the safer buy.

**That recommendation was wrong, and the experiment was cheap enough that it should
never have been left as a judgement call.** Setting `h2` to 2.4.240 in `core/pom.xml`
and `spec/pom.xml` — which also flips the renderer to `H2Modern` automatically via
`Compiler.dialectOf:654-657` — and re-running gate 5:

| | pass | fail |
|---|---:|---:|
| corpus H2 @ **2.1.214** | 2151 | 440 |
| corpus H2 @ **2.4.240** | **2210** | **381** |

**61 tests fixed, 2 regressed, net +59, from a two-line pom change.** The gap to DuckDB
closes from 326 to 267 — more than `STRING_SPLIT` (49) and `LIST_MIN` (46) combined,
and it requires no new code at all.

**What the bump fixed**, by the cause those tests used to fail with:

| | count |
|---|---:|
| `AssertFailed: assertEquals (TDSRow.values)` — **the §2.3 NULL-string family** | **50** |
| variant navigation wall | 10 |
| other assert | 1 |

**The feared golden-fork breakage did not materialise.** Neither regression is a
`charPadding` or boolean-comparison row. Both are the same known arm:

- `testFilterUsingArcCosFunction`, `testFilterUsingArcSinFunction` —
  `Invalid value "1.1" for parameter "ACOS() argument"`. `DuckDb.call:78` already
  carries the out-of-domain guard (`CASE WHEN x BETWEEN -1 AND 1 THEN acos(x) ELSE
  'NaN' END`); `H2`/`H2Modern` do not. **Porting that one arm should make the bump
  +61 / −0.**

**Verification was substantive, not just a bigger number.** `MinimalCorpusTest` under
2.4.240 fails on exactly one assertion — `pinRoster:600`, "LOST 2, GAINED 61" — which
is the roster *set* pin doing its job. The strength floors (`H2_STRENGTH`, asserted
earlier in the same run at `:302`) **held**, so these are real differential passes and
not verdicts that weakened into spelling-only agreement.

**Two consequences for the rest of this plan:**

1. **Phase D1 is largely subsumed.** The ~45-row NULL-string family was attributed to a
   codec defect and nominated as the best rows-per-hour in the document. It is
   substantially an H2 **2.1.214 engine behaviour**, and 50 of those rows fix
   themselves on 2.4.240. Do not start there; do Phase Z instead, then re-measure what
   is left of D.
2. **The `json_navigate` UDF is off the table**, and rightly so — it would have been a
   second owner for behaviour the platform already has natively, against the project's
   one-owner tenet, to buy 10 rows the bump gives for free.

**Remaining risk to close before landing it:** the bump is a *replay-oracle* change as
well as an execution change, and 2,613 corpus rows is a broad but not exhaustive
witness. The golden-text lane (`EngineStyleH2`, `H2_DIALECT_VERSION = '2.1.214'` at
`RawSqlBoundary:147`) still pins the engine's version string and must stay 2.1.214 —
**the execution engine and the golden-text dialect are separate decisions**, and only
the first is being bumped here.

**Recommendation, reversed on the evidence: do Phase Z first**, with the ACOS/ASIN
guard, and re-measure phases C and D against the new baseline before starting them.

### 5.10 Summary

**Corpus yields below are RE-MEASURED after Z** (§5.11), not carried over from the
2.1.214 baseline. PCT yields are still the 2.4.240 measurement from §2.1, which Z does
not change — PCT already ran 2.4.240.

| phase | work | PCT | corpus | size | depends on |
|---|---|---:|---:|---|---|
| **A2** | widen gate 7 to five suites | — | — | S | — |
| **Z** | **bump corpus lane to H2 2.4.240 + ACOS/ASIN guard — MEASURED** | — | **+61 / −2** | **S** | — |
| **A1** | `sessionSetup()` UDF seam (+ prod `H2Settings`) | — | — | S | — |
| **B** | spellings: `BITAND`/`LSHIFT`; aggregate table (**scope corrected — §2.5**: 4 names, not 12); **+ the `Spellings.H2` verification assertion that closes all 24 latent rows** | ~11 | ~8 | S | — |
| **C+** | the **9 latent functions no test reaches** (§2.5): `epoch`, `to_timestamp`, `flatten`, `json_type`, and the 5-strong Map family | 0 today | 0 today | S–M | A1 |
| **C** | scalar Java UDFs (9 ports + ~12 new) | ~33 | **82** | M | A1 |
| **D** | codec fixes (CSV render, text/value, multiset) | ~7 | **26** | S | Z |
| **E** | split the conflated `Caps` switch | — | — | S | — |
| **F** | array UDFs over `Integer[]` | ~75 | **84** | M | A1 |
| **G** | carrier flip to native `ARRAY` | — | — | M ⚠ | E |
| **H** | ordinal-join explode | ~45 | **37** | L | G |
| **I** | lambda lowering | ~23 | **20** | L | H |

**Z+A+B+C is ~56 PCT and ~90 corpus rows of S/M work with no architectural risk**, and
Z alone is 59 of them for a two-line change. E+G+H+I is the remaining structural half.

*Order to actually start in:* **A2** (an hour; makes every later claim checkable), then
**Z** (measured, +59), then **re-measure the whole H2 roster** — phases C and D were
sized against the 2.1.214 baseline and Z moves it — then **A1 → `STRING_SPLIT`**, then
**B**.

> **The methodological lesson, recorded because it cost a wrong recommendation.** Z was
> first written up as "a decision that is not ours to make", weighing a documented fork
> risk against 19 rows. The experiment that settled it was a two-line pom edit and one
> 23-second gate run. **When a decision is framed as a judgement call, check first
> whether it is cheaper to just measure it.** Here the measurement both reversed the
> recommendation and dissolved a 45-row phase that had been nominated as the best work
> in the plan.

### 5.11 The post-Z corpus roster, re-bucketed — **273 H2-only rows**

Measured from the 2.4.240 run, H2-only = H2 fails minus the 108 DuckDB shares. This
supersedes §2.2, which was the 2.1.214 baseline.

| phase | rows | bucket |
|:---:|---:|---|
| C | 49 | `Function "STRING_SPLIT" not found` |
| F | 46 | `LIST_MIN` wall |
| H | 37 | `UNNEST` wall |
| F | 30 | `LIST_GET` wall |
| C | 26 | `Function "REGEXP_EXTRACT" not found` |
| D | 16 | CSV render divergence |
| I | 15 | `LIST_FILTER` wall |
| — | 8 | sql-text golden assert *(harness policy, not in scope)* |
| F | 8 | `STRING_AGG` collection reduction |
| D | 8 | text/value divergence |
| ? | 6 | `DataError` (other) |
| I | 5 | nested checked defects |
| ? | 4 | grid canonical divergence |
| C | 3 + 2 + 2 | `TO_BASE64`, `MD5`, `JSON_PRETTY` |
| D | 2 | multiset divergence |
| ? | 5 | `assertContains`, ClassCastException, NotImplemented, `EPOCH_MS`, FULL-OUTER sort key |

**Rollup: F 84 · C 82 · H 37 · D 26 · I 20 · unassigned 16 · out of scope 8.**

**Two things this measurement proves that the pre-Z estimates could not:**

1. **Variant navigation is GONE — all 19 rows.** Phase Z closed the family completely;
   there is no residual variant work and no `json_navigate` UDF is needed.
2. **Burning one layer reveals the next, and the yields move.** `REGEXP_EXTRACT` went
   **17 → 26** and `LIST_FILTER` **7 → 15** *without either being touched* — those tests
   previously failed earlier and now get further. This is the "first-wall attribution"
   caveat made concrete: **totals will drift upward as phases land**, and each phase
   should re-bucket rather than trust the number it inherited.

### 5.12 What is still NOT measured — read before committing to a phase

Everything above is executed evidence except these. They are named so a fresh session
does not mistake them for settled:

| # | Unmeasured | Why it matters | How to close it |
|---|---|---|---|
| 1 | **Phase G — the carrier flip has never been run.** | It is the only phase with no empirical basis at all; "M, high blast radius" is a judgement, not a measurement. | A spike: land E, flip `nativeLists`, change `arrayLit`, run gates 4/5/7. Same shape as the Z experiment, which twice overturned a judgement call. **Do the spike before committing to G's size.** |
| 2 | **The 16 unassigned corpus rows** (§5.11) | 6% of the remaining gap has no phase. | Read the six `DataError (other)` and four grid-canonical rows individually. |
| 3 | **Per-function DuckDB semantics for phase C's ~12 new UDFs** | Null handling and edge cases decide whether a row flips or diverges. The census names the *oracle* (engine `LegendH2Extensions`, else the DuckDB function) — not the answers. | Per function, at implementation time; the PCT/corpus row is the test. |
| 4 | **Which named PCT tests each phase flips** | PCT failures are bucketed by error text, not test name — surefire reports every row as `PureTestCase`, so there is no expected-flip list to check against. | Correlate the `[LegendLite PCT] Executing:` line preceding each failure. |
| 5 | **Whether phases interact** | F, H and I all touch collection lowering; their yields are measured independently and may overlap. | Re-bucket after each. |

### 5.14 LEG V — the vocabulary/structure separation (the root cause, not the symptom)

> **Status: RECOMMENDED, user-directed 2026-09-16, not yet ratified.** This is
> platform-wide work, not H2 work; it is written here because this census is the
> evidence for it. `CARRIER_REDESIGN.md` §5 already chartered it as a deferred slice
> ("later slices purify the ANSI base of DuckDB idioms — int-divide `//`, `EXCLUDE`,
> interval spellings, carrier caps"); what is new is that the scope is now **measured**
> rather than estimated.

#### The diagnosis

`AnsiSqlRenderer` does two unrelated jobs, and conflating them is what produces every
silent emission in §2.5:

| job | shared across dialects? | where it should live |
|---|---|---|
| **Traversal** — IR walk, operator precedence and parenthesisation, clause assembly, join nesting, subquery composition, `passes()`, hook dispatch | **yes, identically** | the base |
| **Vocabulary** — which function name, which literal syntax, which type name | **no, never** | per-dialect data |

The 40 honest walls prove the base already knows the right pattern: it declares a hook
and throws when the dialect has not supplied a spelling. The ~35 silent emissions are
arms that took the other path. **The architecture is right; a handful of places break it.**

**The single line that breaks it** (`Spellings.java:36-45`):

```java
private static Map<SqlFn, String> h2() {
    Map<SqlFn, String> m = build();     // ← inherits DuckDB's ENTIRE vocabulary
    m.put(SqlFn.MATCHES,  "regexp_like");
    m.put(SqlFn.STRFTIME, "formatdatetime");
    m.put(SqlFn.STRPTIME, "parsedatetime");
    return m;
}
```

and `Compiler.java:730-733`, which hands SQLite the same table:
`new AnsiSqlRenderer(Lexicon.SQLITE, TypeNames.ANSI, Spellings.DUCKDB)` — **SQLite is the
second victim, and `SQLiteIntegrationTest` runs in gate 1.** Two independent witnesses
make this a platform defect, not an H2 inconvenience.

Measured contamination in the base itself (1,285 lines, 219 distinct string literals) —
the function-shaped lowercase literals are few enough to enumerate:

`current_date` · `current_user` · `date_trunc` · `lpad` · `rpad` · `make_timestamp` ·
`to_weeks` · `rowid`

`rowid` is precisely the DuckDB-ism `CARRIER_REDESIGN` §5 named (H2 spells `_ROWID_`).

#### The rule

> **The base contains only (a) IR traversal and (b) structural keywords identical across
> every supported dialect. Any token that varies between two supported dialects lives in
> per-dialect data or a hook that throws.**

`SELECT`, `FROM`, `JOIN`, `ON`, `GROUP BY` stay — every SQL dialect spells them the same.
`LIMIT` does **not** — it already varies (the H2 budget records "never a LIMIT/OFFSET
pair") and is already hook territory. Vocabulary belongs in the three data tables that
already exist (`Spellings`, `TypeNames`, `Lexicon`); shape logic that cannot be a table
row becomes a hook. **Not 100 new hooks — data, with hooks only where shape varies.**

#### Why not the two alternatives

**Not "make it genuinely ANSI SQL:2016".** There is no ANSI engine to execute against, so
base conformance would be asserted by nothing. That replaces "DuckDB in disguise" with
"aspiration in disguise", which is worse because it looks principled. Every rule above is
testable; conformance to an unrun standard is not.

**Not "one renderer per dialect, no shared root".** Six backends × ~1,200 lines of
traversal is ~7,000 lines of duplicated precedence and parenthesisation logic, with fixes
landing in one copy and missing five. This project has already run that experiment in the
other direction: **F10 slice 1 (`079ebfda`) collapsed three divergent copies of literal
spelling into one owner and got byte-identical output.** Abolishing the root would
deliberately recreate what that slice deleted — and the
`EngineStyleH2 → EngineStyleDB2 → EngineStyleComposite` chain would need three more copies.

#### The work

| # | move | scope |
|---|---|---|
| V1 | `Spellings.h2()` starts from an **empty** map, every row H2-verified | closes 24 |
| V2 | relocate the DuckDB-spelled coded arms from `AnsiSqlRenderer.call()` into `DuckDb.call()`; base throws | ~8 |
| V3 | add hooks where there are none: `SqlExpr.OrderedListAgg` (`:460`, no hook at all), the `SqlType.Map` cast (`:1051`, ungated), the `SqlType.Array` cast (`:1050` — `T[]` is a **syntax error** on H2, which spells `INTEGER ARRAY`) | 3 |
| V4 | purge the enumerated vocabulary literals from the base (`rowid`, `date_trunc`, `lpad`/`rpad`, `make_timestamp`, `to_weeks`, `current_date`, `current_user`) | 8 |
| V5 | re-point the **six `super.call()` fall-throughs** in `EngineStyleH2` (`:1666, 1715, 1730, 1738, 1751, 1783`) — the golden-text renderers inherit base arms, so V2/V4 reach them | 6 sites |
| V6 | **rename** `AnsiSqlRenderer` → `SqlRenderBase` | mechanical |

**V6 is not cosmetic.** The name is a standing invitation to the defect: "AnsiSqlRenderer"
tells a contributor that putting a function name in it is legitimate. It has been arguing
for this bug since it was created.

#### Acceptance tests — all three must hold

1. **Base-literal scan.** A test scans `SqlRenderBase` for string literals against an
   allowlist of universal structural keywords. Anything else fails. This is what stops
   the defect regenerating as `SqlFn` grows — it is already at **173** constants.
2. **DuckDB lane byte-unchanged.** Corpus **2477** and PCT **1247**, exactly. This is the
   invariant that makes the whole leg safe: an arm moved base→`DuckDb` that leaves DuckDB
   at its pins is *proven* behaviour-preserving. **This is the reason the leg is low-risk,
   not high-risk** — the reference lane is a complete regression oracle.
3. **Golden text byte-parity unchanged.** Gate 8 and the corpus sql-text lane pin
   `EngineStyle*` output byte-exact, so any V2/V4/V5 drift is caught immediately and
   loudly. This is the leg's sharpest hazard and its best instrument.

#### Sequencing, and the honest cost

**Split it.** V1 is contained and closes 24 rows — fold it into phase B. V2–V6 is a
program leg of its own: run it **after Z/A/C** (bank the cheap parity first; do not churn
the roster while measuring a burndown) and **before any new backend is added**, which is
when the interest comes due — `DUCKDB_FUNCTION_COVERAGE` already priced widening `SqlFn`
across six planned backends at **~1,770 render arms**, every one of which inherits this
defect today.

**It buys zero passing tests.** Say so plainly. What it buys is that every measurement
taken after it is trustworthy, silent wrong answers become loud walls, and the next
dialect costs a table instead of an archaeology session. Sized from the measured scope —
~35 relocations, 3 new hooks, 8 literals, 6 fall-through sites, one conformance test —
this is an **L: two or three batches, not a month.**

### 5.13 Starting a phase cold — the file map and the house rules

Everything a session needs that is not in the phase text above.

**Where each phase edits.** All paths from the repo root.

| phase | files |
|---|---|
| A2 | `tools/allgates.sh` (`gate7`, `G7_MIN_RUN`/`G7_MAX_FAIL`/`G7_MAX_ERR`) |
| Z | `core/pom.xml`, `spec/pom.xml` (h2 `2.1.214`→`2.4.240`); `core/src/main/java/com/legend/sql/dialect/H2.java` (ACOS/ASIN guard, mirroring `DuckDb.java:78`); `spec/src/test/resources/rcorpus/h2-fail-roster.txt` |
| A1 | `H2.java` (`sessionSetup()` override); `com/legend/harness/H2ExtensionFunctions.java` → `core/src/main/java/com/legend/sql/dialect/h2/H2Functions.java`; `core/src/main/java/com/legend/server/ConnectionResolver.java:194-212` |
| B | `H2.java` (`bitOp()`); `AnsiSqlRenderer.java:950` (`reducer`); `Spellings.java`; `SqlAgg.java` |
| C | `.../dialect/h2/H2Functions.java`; `Spellings.java` (`h2()` builder at `:36-45`) |
| D | `H2.java:651` (`normalize`) |
| E | `CarrierStrategies.java:56-62` (the `Caps` record) and its three read sites `:83`, `:232`, `:654` |
| F | `H2Functions.java`; `H2.java` (`listCall`, `membership`, `reduceCollection`) |
| G | `CarrierStrategies.java` (`Caps.H2`); `H2.java:355` (`arrayLit`) |
| H | `core/src/main/java/com/legend/sql/dialect/LateralExplodeToUnion.java` |
| I | `core/src/main/java/com/legend/lowering/Scalars.java`, `Fold.java`, `ListShapes.java` |

**The three landmines, in one place** — all probed, all silent-wrong-answer or
raise-at-runtime, none of them caught by a type checker:

1. **`arr[n]` out of range RAISES on H2**, returns NULL on DuckDB. Guard every generated
   index: `CASE WHEN n <= CARDINALITY(arr) THEN arr[n] END` (§3.3).
2. **Filtering on `arr[s.n]` in the join's own `WHERE`** lets H2 evaluate the index before
   the join predicate and raise. Explode in a subselect, filter outside (§3.3).
3. **`ARRAY_AGG`/`MIN` over `UNNEST(<function call>)` returns NULLs silently** (§3.45).
   Always explode through the ordinal join; never emit `UNNEST(<call>)` under an aggregate.

**House rules that bind this work** (from `AGENTS.md` / `docs/TENETS.md` / `docs/GATES.md`):

- **Loud walls over wrong rows.** A capability that is not reachable raises
  `DialectCapability`; it never silently degrades. The three landmines above are the
  ways H2 breaks that rule for us, so the renderer has to enforce it.
- **One owner per behaviour.** This is why the `json_navigate` UDF was dropped in favour
  of Z: never a second implementation of something the platform already does.
- **`tools/allgates.sh` has no `set -e` and always exits 0** — read the log, never the
  exit code.
- **`-Dlegend.engine.root` / `-Dlegend.pure.root` are SYSTEM PROPERTIES**, not env vars,
  and `tools/oracle-roots.sh` fails the chain if the checkouts are off
  `tools/oracle-pins.env`. They were off the pins when this census ran — see §7.
- Run gates under `caffeinate -dims`; a slept run once produced a 21× timing error.
- Roster changes carry a written reason in `docs/GATES.md`; strength floors are
  shrink-only.

**The verification each phase owes.** Gate 5 (corpus H2) and the widened gate 7 (PCT H2)
are the scoreboard; gate 4 and gate 6 must not move. A phase is done when its rows flip
*and* the DuckDB lanes are unchanged — a change that moves both lanes is a platform
change wearing an H2 costume.

---

## 6. Standing corrections to the backend docs

1. `BACKEND_PORTABILITY.md` §3's four anti-native-array measurements are **Postgres's**,
   not H2's. H2 handles jagged nesting and `CARDINALITY` correctly (§3.1).
2. `BACKEND_PORTABILITY.md` §2 "H2 has **none**" of the correlated-explosion mechanisms,
   and its empty-collection claim, are **false at 2.4.240** (§3.3).
3. `H2_BACKEND.md` §4.2 / `Spellings.java:27-33` / `H2.java:24` ban the `CREATE ALIAS`
   route. That ban is **already reversed** by `PROGRAM_MAP.md` (user, 2026-08-29) and
   `END_TO_END_PLAN` Phase 7. The three in-code comments still assert it and should be
   updated with the reversal, since `H2.splitPartCall` already violates it.
4. The `H2_BACKEND.md` §2 "80% reachable / 19% impossible" figure and its copies are a
   2026-07-31 snapshot of a *constructs* denominator; the lane's real number is
   §1.2's, and the D-count shrinks materially under §3.1.
5. `H2_BACKEND.md`'s addendum says JSON object field access "exists in NO syntax" at
   2.4.240. **Refuted** — `(JSON '{"a":1}')."a"` returns `1`; the quoting is
   load-bearing, and bare `.a` returning NULL silently is how the battery missed it
   (§5.0). `CARRIER_REDESIGN.md`'s MODERN PROFILE is the correct account.
6. **H2 2.5.250 adds nothing** (§3.4) — four batteries byte-identical to 2.4.240, and
   the two versions' own `help.csv` differs by a two-line typo fix.
7. `CarrierStrategies.Caps` advertises three capabilities; **two are never read**, and
   the third is a master switch over four unrelated emulations (§5.5). Any future
   backend reading that record as a capability model will be misled.

---

## 7. Reproducing this

```bash
export JAVA_HOME=~/jdk/jdk-21.0.11+10/Contents/Home
export PATH="$JAVA_HOME/bin:$HOME/jdk/apache-maven-3.9.9/bin:$PATH"
ENG=<checkout at 230c159196d6512486fd382556c5e9e4fb128ebb>
PUR=<checkout at 7fbc7d6e8d52e2488bdae67280e5f5dfed448d68>

mvn -pl .,core clean install -DskipTests

# PCT, all five suites, H2 (the number gate 7 does not measure)
cd pct && LEGENDLITE_PCT_BACKEND=h2 mvn clean test -Dtest='!ChannelB*' \
  -Dh2.version=2.4.240 -Dlegend.engine.root=$ENG -Dlegend.pure.root=$PUR \
  -Dmaven.test.failure.ignore=true
# same without LEGENDLITE_PCT_BACKEND for the DuckDB baseline

# corpus, both lanes
mvn -pl spec test -Dtest=MinimalCorpusTest -Dsurefire.excludedGroups= \
  [-Drcorpus.backend=h2] -Dlegend.engine.root=$ENG -Dlegend.pure.root=$PUR
```

The capability probes are four standalone JDBC programs (batteries in §3) run against
`h2-2.4.240.jar`, `h2-2.5.250.jar` and `duckdb_jdbc-1.5.0.0.jar` with no legend-lite
code on the classpath, so they measure the engines and not our rendering. The 2.5.250
jar comes straight from Central
(`repo1.maven.org/maven2/com/h2database/h2/2.5.250/h2-2.5.250.jar`); the version
comparison in §3.4 is a diff of the four battery outputs plus a diff of each jar's own
`help.csv`, extracted from the nested `org/h2/util/data.zip`.

**Caveat on the oracle checkouts.** `~/legend/legend-engine` and `~/legend/legend-pure`
were **off the `tools/oracle-pins.env` pins** when this ran (`943d38b3dc2` /
`d00cfd5ba66`); `oracle_roots_check` would have failed the chain. This census used
detached worktrees at the pinned SHAs. The working checkouts should be moved back onto
the pins, or the pins bumped deliberately via `tools/bump.sh`.
