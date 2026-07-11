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

**Slice 3b** (variant sources + lateral): fromJson(String):Variant
registers and lowers as the JSON cast. relation::variant::flatten
(collection, ~col) — real flatten.pure's scalar/variant-collection →
ONE-COLUMN relation — lands as TypedCollectionRelation lowering to
SELECT UNNEST(value) AS col (variant lists pass through
VARIANT_ELEMENTS). lateral(rel, {row|relationOf(row)}) lowers: the
lambda's relation body compiles with the row param CORRELATED to the
left alias through the enclosing-resolver channel, joined per-row via
CROSS JOIN LATERAL (no ON clause); schema T+V from the checker's
schema algebra. Relation 43 -> 36; PCT total 881/1109 (79%).

**Slice 4** (static pivot + window channel + frames): STATIC pivot
values (`pivot(~year, [2000, 2011], ~agg)`) thread checker → TypedPivot
→ `PIVOT … IN (v…)` — without the IN list DuckDB emitted a column per
DISTINCT data value and the declared 4-column cast saw 5 (the "Row 2
has too many columns" pair + 3 typed-header casts, which also needed
the ESCAPED outer quote pair on quote-bearing declared names).
`size()` after groupBy isolates before COUNT(*) (a zero-key aggregation
over a grouped select was a multi-row scalar subquery). Collection
values inside `lateral` resolve through the ENCLOSING scopes (the
correlated `$x.payload->toMany(@Integer)->flatten(~c)` shape).
windowScalar keeps the WINDOW channel through TypedCast, TypedIf and
zero-param thunks — `if($r.a == $r.b, |$p->lead($r).x->adjust(…),
|$r.y)->cast(@Date)` previously fell to plain scalar lowering and died
on the lead property access. Registered the three missing over()
forms: (ColSpecArray, SortInfo[*], Rows), (ColSpecArray, SortInfo,
_Range), (ColSpecArray, SortInfo, _RangeInterval) — real over.pure has
16 overloads, the 3-arg array-partition forms were the "no overload"
five. Frame-boundary validation message now VERBATIM real pure
('Invalid window frame boundary - lower bound of window frame cannot
be greater than the upper bound!') — 5 assertError parity tests.
Result headers quote non-identifier column names ('other kind').
Relation 36 -> 14; PCT total 903/1109 (81%).
Remaining Relation 14: variant-column semantics family (8: contains /
isEmpty / isNotEmpty / joinStrings value asserts, slice ARRAY_SLICE
binder, ~distinct token, projectModelProperty + modelOutputNotSupported
model types), instance-literal project non-bare paths (2), "Values
list" binder pair (2), joinStrings-on-null (1), precisePrimitives Int
(1).

**Slice 5** (RELATION TO ZERO — 14 -> 0): variant semantics — a TDS
'null' cell is SQL NULL for every non-variant type (String included: a
null name must VANISH from joinStrings window collections) but the JSON
null VALUE for variant columns; 'null' cells are NEUTRAL in column-type
inference (a later JSON-shaped cell still makes the column Variant);
toMany over JSON null stays SQL NULL (the real relation runtime pins
toVariant(NULL)='null' vs toVariant([])='[]' — null PROPAGATES) while
the list CONSUMERS are null-safe (contains COALESCE false; isEmpty/
isNotEmpty are TYPE-aware LIST-length tests, not IS NULL — isEmpty("[]")
is true). collection::distinct registered (= removeDuplicates, real
distinct.pure) with the DistinctChecker falling through to the plain
call for non-relation sources. ~distinct & friends: the lexer's fused
mapping-command tokens parse as ordinary colspecs in EXPRESSION
position. Instance-literal project supports COMPUTED columns ($v.a +
$v.b, coalesce($x.f, ...)) — property accesses resolve to the
instance's literal values. Window channel: value-fn columns resolve
through the select (a folded project's alias substitutes its defining
expr). Pivot dynamic-column identity ('2011__|__newCol', quote-bearing)
normalizes to the bare SQL name in Fold resolution (resolveInto +
sourceColumn). meta::pure::precisePrimitives integer/float family
aliases to base primitives (width-annotated subtypes; a width-carrying
Type is future work). to(@ModelClass) property reads extract the JSON
field (VARIANT_GET + cast); MATERIALIZING the class value throws real
pure's "The type X is not supported yet!". The adapter injects model
classes referenced as @-type arguments (to(@Person)), not just
^-instantiations. ONE expected-failure ledger entry (documented wire
limit, not semantics): testVariantArrayColumn_joinStrings — the TDS
parser's null literals are ["", "null"], so an EMPTY-STRING cell cannot
round-trip.

| suite | run | errors | passing |
|---|---|---|---|
| Essential | 327 | 86 | 74% |
| Standard | 204 | 55 | 73% |
| **Relation** | **348** | **0** | **100%** |
| Grammar | 136 | 30 | 78% |
| Unclassified | 94 | 21 | 78% |
| **total** | **1109** | **192** | **83%** |

**Audit round 4** (four parallel adversarial auditors over
b55cd3f2..003c15a3 — correctness + HACKINESS/overfit; every finding
probe-verified before fixing). FIXED: (1) BLOCKER `flatten()->size()`
counted PRE-explosion rows — COUNT(*) replacement now isolates when any
projection is a row-multiplying UNNEST; (2) BLOCKER
`distinct`/`removeDuplicates` lowered to UNORDERED LIST_DISTINCT — real
removeDuplicates pins FIRST-OCCURRENCE order (its PCT asserts unsorted);
both now emit `list_filter(l, (x,i) -> list_position(l,x) = i)`; (3)
lateral's pushed resolver threw IllegalStateException instead of the
UnfoldableRef SIGNAL, severing the enclosing-scope chain behind lateral;
(4) the colToAgg window-aggregate arm kept a raw FROM-qualified column
where the lead/lag fix used resolveOrThrow — folded-project aliases now
substitute; (5) `to(@Class)` property extraction was ONE-HOP-keyed to
the PCT shape — now collapses whole chains (to(@Firm).boss.name) into
nested VARIANT_GETs, real testToClassAndAccessNestedProperty semantics;
a class-typed LEAF throws the verbatim parity message instead of an
internal crash; (6) computed instance-literal columns with MANY-valued
bodies emitted a list-in-a-cell — now loud; (7) `Fold.sourceColumn`'s
UNCONDITIONAL pivot-identity quote-strip could silently mis-resolve a
genuine quote-bearing column — outputs-claimed exact names now win,
stripping is the fallback; (8) the two SYNTHETIC over() forms
(`over(Frame)`, `over(SortInfo,Rows)`), the OverChecker frame-only
pre-arm, and both kernel empty-fragment escapes are REMOVED — real
over.pure has exactly 16 overloads and rejects those shapes (the
motivating PCT tests use the 3-arg forms); the pre-existing
`over(ColSpec, SortInfo[*], _Range)` tightened to real's `[1]`; (9) TDS
datetime inference now requires seconds+zone (Deephaven's actual
grammar; `T00:32`/no-zone stay String) and maps to the abstract `Date`
(real convertType), not DateTime; (10) JSON-shape→Variant inference
gated on a STRICT JSON parse — bracket-shaped string data (`[tag]`)
stays String; the divergence itself remains (ledgered below); (11)
`annotatedType` resolves FQN-spelled primitives through the one
Primitive table (precisePrimitives::Int annotations work, matching the
cast path); (12) the joinStrings ledger pin is the FULL expected+actual
text verbatim the official DuckDB PCT's pin (the fragment pin could
mask regressions); (13) the adapter never injects classes core knows
natively (Pair redefinition hazard); the dead `""` emission and false
comments removed.

LEDGERED DEBT (documented, deliberately not fixed): the JSON-cell
Variant inference diverges from Deephaven because the PCT wire
(toPureGrammar serialization) DROPS the fixtures' explicit
payload:Variant annotations — the hack is load-bearing until the wire
carries annotations; the typed-header overlay + data-driven
multiplicities mean PCT verifies VALUES ONLY, never core's relation
typing (a core typing every column [0..1] would still go green);
reEscapeStringLiterals guesses at a mixed escape convention and will
corrupt backslash-bearing literals in other suites (needs a
deterministic wire convention); precisePrimitives aliasing drops range
CONSTRAINTS (real casts enforce [-128,128) etc. — a width-carrying Type
is the promised follow-up); pivot identity is recovered by name-shape
at resolution rather than carried as provenance (pivot→cast→pivot
still confused); remapErrorMessage (pre-existing) rewrites DuckDB shift
errors into the pure message. Verified loop: core 1391, corpus 2721
zero regressions, PCT 917/1109 with Relation 348/348 — all UNCHANGED by
the fixes.

Update this file per slice, same as docs/SCOREBOARD.md.
