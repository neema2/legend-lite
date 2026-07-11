# PCT burn-down — real-pure parity scoreboard

The PCT module runs legend-pure's own compatibility suites (Essential /
Standard / Relation / Grammar / Unclassified) through QueryService → core →
DuckDB, asserting against REAL pure expected results. This is the parity
instrument the corpus cannot be: the corpus pins engine-lite behavior; PCT
pins legend-pure behavior. (PCT is still PARTIAL — function-level only,
almost no class/mapping coverage; the broader legend-engine test-porting
plan is a separate doc.)

**Protocol** per slice: fix → core suite → engine corpus A/B → `mvn -pl
engine install -DskipTests` (PCT consumes the .m2 jar — a stale jar cost us
a fossil baseline once) → PCT A/B → commit code+scoreboards together → push.

## Baseline (2026-07-11, legend-pure 5.88.0 / legend-engine 4.133.0)

Stack upgraded from pure 5.52.4 / engine 4.91.0 (sources checked out at
the matching release tags; engine 4.133.0's own pom pins pure 5.88.0 —
5.88.1 PARs mismatch, stay on the pairing). Two 5.88 adapter fixes:
TDS headers now spell PURE type names (the parser resolves them as
paths — 'VARCHAR not found' was ours) and carry column MULTIPLICITIES
([1]/[0..1] only; richer forms throw in processMultiplicity).

| suite | run | errors | passing |
|---|---|---|---|
| Essential | 327 | 86 | 74% |
| Standard | 204 | 57 | 72% |
| Relation | 348 | 164 | 53% |
| Grammar | 136 | 30 | 78% |
| Unclassified | 94 | 21 | 78% |
| **total** | **1109** | **358** | **68%** |

The suite GREW +206 tests over the old stack (Relation nearly doubled)
— new real-pure coverage, new gaps: `_range` interval arities (52),
`relation::reduce` (37), `lateral` (6) alone are 95 Relation errors
behind three registrations. Old-stack numbers for the record: 903 run,
172 errors, 81%.

### Pre-upgrade baseline (pure 5.52.4 / engine 4.91.0 — superseded)

| suite | run | errors | passing |
|---|---|---|---|
| Essential | 324 | 69 | 79% |
| Standard | 173 | 46 | 73% |
| Relation | 196 | 21 | 89% |
| Grammar | 134 | 33 | 75% |
| Unclassified | 76 | 3 | 96% |
| **total** | **903** | **172** | **81%** |

## Buckets (burn in order)

| # | family | ~count | shape |
|---|---|---|---|
| P-A | Decimal/Float value surface | ~30 | `expected 0.5 actual 0.5D`, `2D actual 2`, adapter `Cast exception: String/Integer/Decimal cannot ...`, `For input string: "41.14"` — the D-suffix (pure Decimal) vs Float vs Integer distinction through lowering + the pct adapter's result decode |
| P-B | ~~TDS column-type imports~~ FIXED by the 5.88 adapter type-name fix | 0 | `VARCHAR not found!` — throws inside LEGEND-PURE'S OWN interpreted runtime (`_Column.getColumnInstance` via `TDSExtension.parse`) while the HARNESS parses the test source, before our adapter runs — a pct-module legend-pure dependency/version issue, not core semantics |
| P-C | PCT model vocabulary | ~16 | `unknown enumeration`, `class has no property`, `not a known class` — the adapter injects classes it extracts from the expression but not enums/associations/properties the PCT fixtures declare |
| P-D | Pair as a value | ~9 | nested `Pair<Pair,...>` SQL type, `.first/.second` property surface, `Pair<U,V> got: Pair` |
| P-E | assertError message parity | 7 | tests assert EXACT real-pure error strings — candidate divergence-ledger entries or adapter remap |
| P-F | overload/function gaps | ~14 | `no overload structurally matches`, `unknown function` — missing registrations |
| P-G | date adjust/interval edges | ~12 | huge-interval overflow (`12345678912 hours`), INT64 casts, far-past years, end-of-month adjust asserts |
| P-H | singles tail | ~20 | LPAD/RPAD strict padding, between() over dates/strings, scalar-subquery cardinality, IN over structs, pivot `__\|__` column naming, parser edges |

Update this file per slice, same as docs/SCOREBOARD.md.
