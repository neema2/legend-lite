# Corpus burn-down taxonomy — all remaining failures

Snapshot at `34d436d` (corpus **2223 / 2721 pass**, 297 green classes):
**479 failing tests**, every one bucketed below. Buckets are ordered by
size within each owner. Full per-bucket test lists regenerate with the
classifier in this doc's history (`tools/` candidate).

## A. CORE — resolver/lowering gaps (real work, design-known)

| # | tests | bucket | note |
|---|---|---|---|
| A1 | **55** | `row-read rewrite hit TypedCast` (JsonM2MChain 52, JsonM2M 3) | JSON/variant-SOURCE classes: bindings are `to(get($row.data,'k'), @T)` casts — `Pipelines.rewriteRowReads` lacks a `TypedCast` arm. Plan §F12 says substitution shouldn't care; likely a SMALL arm + variant-lowering follow-through. **Highest single unlock.** |
| A2 | **18+2** | `TypedNewInstance` in scalar/relation position | `^Pair(...)`/instance literals as VALUES (Pair-heavy PCT tests). Needs a value-representation decision (STRUCT?) — pairs with A4. |
| A3 | **13** | `TypedMatch` scalar lowering | `->match([...])` → CASE chain over type/multiplicity tests. Self-contained lowering arm. |
| A4 | **28+10+8** | `Any`/generic `Pair<...>` at the lowering boundary + LUB widening | One G family: heterogeneous collections need common-supertype inference (`concatenate([1], ['a'])`, `add`), and generic classes (`Pair`) need an SQL value story. Kernel change — do deliberately, not rushed. |
| A5 | **11** | multi-hop navigation (`dept.org.name`, 3-hop) | H3 extension: chain prefixes for paths deeper than 2. Design exists (plan §2.3 NavPath); mechanical extension of the assoc-join loop. |
| A6 | **9** | SQL type `JSON` unmapped at output boundary | Variant-typed output columns → `PureSql.type` arm. Small. |
| A7 | **6+5+5+3+2+2+2+2+1** | scalar-position relation ops (`TypedFilter/TypedTds/TypedPropertyAccess/TypedSlice/TypedDrop/TypedLet/TypedLimit/TypedWrite/...`) | Relation values used AS scalars (mostly `->filter(...)` inside scalar exprs over lists, TDS literals in scalar position). Each is a small scalar-arm or an honest wall; triage per node. |
| A8 | **6** | variant family (`toVariant` unknown fn, UNNEST over VARCHAR, ...) | Variant construction/iteration gaps beyond A1/A6. |
| A9 | **4+3+2+1+1** | class query under `TypedNativeCall`/`TypedIf`/`TypedCast`/`TypedExtendAgg` | Class collections flowing through scalar natives (`->first()`, if-branches) before a relation terminal. Resolver terminal-vocabulary extensions; some are honest walls. |
| A10 | **3** | window family (`over` overload, `_range` frame typing) | The window-frame G typing gap (UnboundedFrameValue etc.). |
| A11 | **3** | `country mapped through the target's own join slots` | Nested join demand inside association targets (H4b-adjacent, honest wall today). |
| A12 | **3+3** | `aggregate reduce must be a native reducer` / reducer arg `TypedCast` | Casts inside agg map/reduce lambdas — unwrap or compose. Small. |
| A13 | **2** | flatten over computed projection | Fold gap. |
| A14 | **2** | `nested join has no single alias` | JoinChecker chained-join hosting (pre-existing pin family). |

**A total ≈ 190 tests.**

## B. DuckDB SQL-emission bugs (wrapped in "closed pending query" JDBC noise — 45 tests)

Each carries its real binder error inside; the JDBC wrapper obscured them until now.

| # | tests | bucket |
|---|---|---|
| B1 | 9 | `list_aggr`/`list_aggregate` mis-calls (list fns over wrong arities) |
| B2 | 8 | pivot output column naming (`2011__|__newCol` vs core's `2011_newCol`) — pivot naming convention mismatch |
| B3 | 5 | `round_even` macro arity (2-arg banker's round over decimals) |
| B4 | 4 | `strpos` arg types |
| B5 | 3+2+2 | `rpad`/`lpad` need the explicit pad-char default `' '`; `make_timestamp` variants |
| B6 | ~12 | one-offs: `to_base64`, string `*`, VARCHAR/INTEGER compare, GROUP BY missing col, UNNEST-over-VARCHAR, template-T list_value, sum over strings |

## C. FIXTURE — corpus test bugs (fix the tests, real-pure strictness)

| # | tests | bucket |
|---|---|---|
| C1 | **56** | queries referencing unknown FQNs: `test::Person` (37), `test::Order` (10), `test::Sale` (5), `test::Employee` (2), misc (2) — same disease as the otherwise family we already fixed |
| C2 | 3 | models referencing tables absent from their `Database` defs (`BASE_PERSON`, `T_EMPLOYEE`, `hr.EMPLOYEES` schema-qualified) |
| C3 | 3 | engine-exception-type pins (`PureCompileException`) not yet widened |
| C4 | 2 | enum value / unknown enumeration fixtures |

**C total ≈ 64 tests — highest tests-per-hour anywhere.**

## D. Execution/value-shape divergences (each small, individually diagnosable)

| # | tests | bucket |
|---|---|---|
| D1 | ~7 | partial-date VALUES surviving to execution (`"2014-01"` as DATE) + partial-date PRESERVATION (adjust(%2020,1,YEARS) should print `%2021`) — needs a partial-date value representation, not just arithmetic padding |
| D2 | ~6 | prefixed-join SQL-shape pins (`Right id prefixed`) — pre-existing JoinChecker family |
| D3 | 3+2 | `SELECT 1` in EXISTS pins; timestamp print format (`T` separator, offset) |
| D4 | ~8 | numeric edge semantics: INT64 overflow (Pure promotes to arbitrary precision), `3.14159d` decimal literal suffix, scientific notation print, base64 unpadded input, splitPart/`Hello World` off-by-one |
| D5 | ~6 | trig/hyperbolic natives not in catalog as called (`tanh_Number_1__Float_1_` FQN-suffixed call forms), acos/cbrt variants, sign asymmetries |
| D6 | ~10 | misc single assertions (pivot `total` suffix, `_tds` alias, boolean-list returns `[true,false]` vs scalar — collection-in-scalar shape) |

## E. LEDGERED (deliberate walls, story known)

| # | tests | bucket |
|---|---|---|
| E1 | 8 | H4b/H5c: embedded/otherwise/slot graph children |
| E2 | 6 | H5c: M2M bindings navigating source associations (`$src.projects`) |
| E3 | 6 | multi-column pivot |
| E4 | 2+1+1 | sort(comparator); groupBy(~[]) empty keys; fold sliding-window |
| E5 | 3 | thunk shapes in mappings |
| E6 | 1+1 | `meta::type`; Decimal(38,18) Pure type-name |

## Recommended order (yield × effort)

1. **A1** TypedCast row-reads (55, likely one small arm + follow-through)
2. **C1+C2** fixture FQN sweep (~60, mechanical, same playbook as before)
3. **B** SQL-emission fixes (45, each verifiable in DuckDB directly)
4. **A2+A3** NewInstance/Match scalar lowering (~35)
5. **A4** LUB + Pair story (~46, careful kernel work)
6. **A5** 3-hop navigation (11)
7. D/E as encountered
