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

| suite | run | errors | passing | after slice 1 |
|---|---|---|---|---|
| Essential | 327 | 86 | 74% | 86 (74%) |
| Standard | 204 | 57 | 72% | 55 (73%) |
| Relation | 348 | 164 | 53% | **75 (78%)** |
| Grammar | 136 | 30 | 78% | 30 (78%) |
| Unclassified | 94 | 21 | 78% | 21 (78%) |
| **total** | **1109** | **358** | **68%** | **267 (76%)** |

**Slice 1** (reduce + interval ranges): registered `relation::reduce`
(windowed map+agg — lowered by reusing the agg-col machinery through a
synthetic TypedAggCol under windowize), the three `_RangeInterval`
overloads of `_range` + the rangeInterval `over()`s (INTERVAL n UNIT
PRECEDING/FOLLOWING frame bounds; DurationUnit names are valid DuckDB
interval units as-is), `lateral` (registered; lowering pending), and
_RangeInterval as an over() frame argument. TDS literal columns with
FULL ISO datetime cells infer DateTime — real pure's inference is
Deephaven CSV's (DATETIME_AS_LONG), which does NOT take date-only
strings: bare 2024-06-15 stays String (a corpus fixture parseDates such
a column; first attempt inferred StrictDate and broke it). The PCT
adapter returns date columns as STRINGS spelled in pure's print form
(millis + +0000) — the interpreted TestTDS cannot build Date columns and
the PCT asserts compare toString().

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

**Slice 2** (Relation harness + registrations): the adapter's
reEscapeStringLiterals no longer corrupts ALREADY-escaped quotes (\'
doubled its backslash into a string TERMINATOR and shredded pivot's
quoted column names — six parse failures). Pivot decode carries the
EMBEDDED-quote column identity ('UK__|__LDN__|__sum' including quotes;
TDSExtension strips one outer layer) and DATA-DRIVEN multiplicities
([1] when a column has no null cells) so results cast against declared
Relation<(col:T[1])> shapes. relation::eval routes as a CURATED CoreFn
alias to the EVAL checker's ColSpec arm — registering its ⊆-colspec
signature polluted the shared bare-name 'eval' overload set and broke
five higher-order corpus tests (caught by the corpus gate, reverted to
the alias). over() gains the collapsed real-over.pure forms: (frame),
(sortInfo, rows), (ColSpec, rows), (ColSpecArray, rows); frame-only
over binds T to the EMPTY unknown fragment (the over(~city) containment
mechanism, vacuous case). Relation 75 -> 56.

**Slice 3a** (variant cell inference): JSON-shaped TDS cells
([...]/{...}) infer VARIANT. Deephaven (real pure's inference engine)
says String there, but the PCT fixtures pair such cells with
Variant-annotated lambdas — the author's declared intent — and the
String inference sent toMany(@Integer) down the scalar-cast path
(list_sort(BIGINT) and friends: 13 tests). Corpus green — the gate
arbitrated in favor. Relation 56 -> 43; PCT total 874/1109 (79%).
Remaining Relation 43: variant::flatten + fromJson registrations (5),
lateral lowering (3), assertError message parity (10), adapter TDS
decode edges (5), subquery cardinality (2), singles.

Update this file per slice, same as docs/SCOREBOARD.md.
