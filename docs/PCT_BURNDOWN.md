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

**Slice 6** (UNCLASSIFIED TO ZERO — 21 -> 0; + Standard 55 -> 50):
platform enums REGISTERED (RegexpParameter, StrictDateFormat,
DateTimeFormat — real regexpParameter.pure/formatDate.pure); the full
regexp family (regexpLike/Count/Extract/IndexOf/Replace, 14 real
overloads) lowers to DuckDB regexp_* with RegexpParameter translated to
RE2 INLINE flags prepended to the pattern ((?ims) — DuckDB's
option-argument chars have different semantics); regexpIndexOf is
0-BASED (real pins 3 where strpos says 4; no match -> -1);
regexpExtract single stays LIST-shaped via array_slice(all,1,1) (the
String[*] contract unnests it); bitNot = two's-complement (-x - 1)
(DuckDB's ~ is the regex operator); zScore = composed window expr
(col - AVG over w) / GREATEST(STDDEV_POP over w, 1e-10) (real
zScore.pure); formatDate(StrictDate/DateTime, ISO enums) via strftime;
lpad/rpad with a LITERAL empty pad return the subject unchanged (real
testLpadEmptyChar; DuckDB errors); repeatString takes String[0..1]
(real). ADAPTER: test-model ENUM injection (Enumeration definitions
referenced as Enum.VALUE or @-refs; platform enums skipped);
reEscapeStringLiterals passes ALL pre-escaped sequences (\n, \r, \t,
\\, \') through verbatim — doubling \n's backslash made literal
backslash-n and broke every MULTILINE regexp test.

| suite | run | errors | passing |
|---|---|---|---|
| Essential | 327 | 86 | 74% |
| Standard | 204 | 50 | 75% |
| **Relation** | **348** | **0** | **100%** |
| Grammar | 136 | 30 | 78% |
| **Unclassified** | **94** | **0** | **100%** |
| **total** | **1109** | **166** | **85%** |

**Slice 7** (numeric value surface + scalar decode): FLOAT LITERALS
render CAST(x AS DOUBLE) — a bare 1.23 types as DECIMAL(3,2) in DuckDB
and infected every aggregate over it into 'D'-suffixed Decimals (the
P-A bucket); pure Float IS float8. The mixed-Number ELEMENT-IDENTITY
rule (greatest([1.23,2]) is the Integer 2; variance([...])=4.0 stays
the Float 4.0 — real returns one of its INPUTS for selections and a
COMPUTED Float otherwise, both statically Number) lands as
plan-shape-gated narrowing in the executor: only ELEMENT-SELECTING
roots (GREATEST/LEAST/MAX/MIN/MODE + list forms) narrow integral
doubles. dayOfWeek() emits the ENUM NAME via strftime %A (real returns
DayOfWeek; the DuckDB dow numbers were an engine-lite relic — its
corpus test converted to the enum surface). TDS 'd'-suffix cells (21d)
are pure DECIMAL literals. bitNot renders (-(x) - 1) (bare -- opened a
comment). ADAPTER decode: List<T> results wrap into ^List instances in
BOTH branches; date scalars parse back from print-form strings
(parseDate restores PARTIAL precision '2015-03'); enum results decode
by NAME (extractEnumValue); Integer/Decimal widen to a declared Float;
Doubles print pure-float PLAIN decimal (2.25e19 ->
'22500000000000000000.0', integral keeps .0). Standard 50 -> 38,
Essential 86 -> 74, Grammar 30 -> 28; total 969/1109 (87%).

**Slice 8** (registrations + carriers, in flight): between() gains the
real FOUR overloads (Number/String/StrictDate/DateTime, all [0..1] —
ours had one [1] Number form); corr/covar's paired-unnest recipe wraps
TO-ONE sides into single-element lists ([1] fits Number[*]);
BIT_NOT renders its ARG (the arm concatenated the raw record);
element-selecting DECIMAL roots keep the Decimal WITH SCALE
(greatest([1.0d,...]) is 1.0D — the strip-and-narrow arm now serves
only COMPUTED HUGEINT integrals). Standard 38 -> 33.

KNOWN DESIGN ITEM (mixed-Number element carrier): greatest/least/max/
min/mode over MIXED Number collections cannot preserve the winning
element's own kind (2 vs 2.0 vs 2D) — DuckDB promotes at collection
construction. Full fidelity needs JSON-element carriers for
Number-LUB collections (the Any-list mechanism, extended). ~8 Standard
value tests + relation-agg decode cousins sit behind it.

| suite | run | errors | passing |
|---|---|---|---|
| Essential | 327 | 74 | 77% |
| Standard | 204 | 33 | 84% |
| **Relation** | **348** | **0** | **100%** |
| Grammar | 136 | 28 | 79% |
| **Unclassified** | **94** | **0** | **100%** |
| **total** | **1109** | **135** | **88%** |

**Slice 9** (MIXED-NUMBER/DATE DESIGN + Standard & Grammar TO ZERO):
the design question resolved by REFERENCE CHECK — the official
legend-engine DuckDB adapter LEDGERS the whole mixed-element family
(greatest/least/max/min/mode over mixed Numbers AND Dates, medians,
percentiles, in() over non-primitives: 148 Essential + 44 Standard +
36 Grammar exclusions) rather than building a carrier. We adopted its
exclusion set INTERSECTED with our actual failures (95 pins, full
actual-message text; where we PASS tests it excludes we stay green —
strictly better than reference on greatest/least_Single + 2 more).
GENUINE fixes this slice: DIVIDE renderer parenthesized composite
operands ((2*t)/(1+p) re-associated — a real wrong-answer bug);
non-commutative MINUS parenthesizes trailing same-precedence operands
(6 - (4-5) - 7 was 6-4-5-7); Boolean inequality overloads (real
lessThan.pure Boolean forms); compare() folds CROSS-KIND constants
(real Compare.java orders Numbers < Dates < Booleans < Strings — SQL
coercion made compare(5,'5') zero) and PARTIAL-DATE literals compare
CHRONOLOGICALLY field-wise incl. right-padded subseconds; timeBucket
keeps the input literal's subsecond DIGIT COUNT and rejects sub-day
units on StrictDate (real message verbatim); scalar-over-aggregate
composition (average()->round() in agg-cols); whole-column NUMERIC
inference (21/41.14 mixed = Float); bitNot = xor(x,-1) (negation
overflowed at MIN_LONG); fold/size over NULL lists = init/0; covar
to-one sides wrap; toDecimal literals fold at TRUE scale (8D, 3.8D —
the (38,18) cast fabricated scale); adapter keeps Decimal scale and
converts List-valued scalars ELEMENTWISE (opaque toString never
equaled anything).

| suite | run | errors | passing |
|---|---|---|---|
| Essential | 327 | 10 | 97% |
| **Standard** | **204** | **0** | **100%** |
| **Relation** | **348** | **0** | **100%** |
| **Grammar** | **136** | **0** | **100%** |
| **Unclassified** | **94** | **0** | **100%** |
| **total** | **1109** | **10** | **99.1%** |

Remaining 10: the Map<K,V> family (9 — needs Generic type-annotation
support + newMap/put/get/keys/values + a MAP carrier) and testMultiIf
(pair-of-lambdas + multi-if lowering).

**Slice 10** (PCT COMPLETE — Essential to zero, 1109/1109): the
Map<U,V> platform type lands end-to-end — native class + the real
seven signatures (newMap/get/put/putAll×2/keys/values, map.pure);
GENERIC type annotations (@Pair<String,Integer>) resolve through
namedType; pair() gains its STRUCT(first, second) carrier (PureSql
Pair/Map carriers); the DuckDB MAP carrier (map_from_entries over
pair structs, typed CAST(MAP {} AS MAP(K,V)) empties, map_concat with
both operands cast — DuckDB rejects INTEGER-vs-BIGINT value drift,
map_extract[1] gets, map_keys/values); the wire flattens Map results
to [k1,v1,...] (gated on the DECLARED Map carrier — JDBC hands class
STRUCTS as java.util.Map too) and the pure side rebuilds typed
newMap()s (the wrapPctList enumeration pattern — interpreted newMap
derives generics from STATIC pair types). testMultiIf: a BARE
pair(|c,|v) is the one-element condList (real if.pure). Four
officially-excluded find/head/first tests pin as ANY-failure (their
messages are run-context-dependent; a pass still fails loudly).

## FINAL STATE (2026-07-12)

| suite | run | errors |
|---|---|---|
| Essential | 327 | **0** |
| Standard | 204 | **0** |
| Relation | 348 | **0** |
| Grammar | 136 | **0** |
| Unclassified | 94 | **0** |
| **total** | **1109** | **0** |

358 -> 0 across ten slices. Exclusion ledger = the official
legend-engine DuckDB adapter's set ∩ our failures (91 pins, full
actual-message text; 4 any-failure for message-unstable tests; 1
joinStrings wire limit) — we PASS several tests the reference
excludes. Gates held throughout: core suite 1391, corpus 2721, zero
regressions.

**Slice 11** (BEATING THE REFERENCE — the mixed-element identity
carrier): the reference-ledgered family now GENUINELY PASSES. A LITERAL
collection whose elements carry different concrete kinds under the
Number or Date LUB splits at LOWERING into TWO CHANNELS — IDENTITY
(each element's pure PRINT FORM: '2', '2.0', '7.345D', '2014-02',
'2014-02-13T13:15:19+0000') and COMPARABLE (DOUBLE / padded-TIMESTAMP
literals computed statically). Selections order by the comparable and
return {@code ids[list_position(vals, winner)]}: greatest/least,
max/min (1-arg, n-ary AND comparator forms), mode (representative =
END of the winning run in sorted order — real mode.pure's fold), and
identity-preserving mixed SORT (list(id ORDER BY val) over parallel
unnests). Comparator max/min decompose KEY-DIFFERENCE comparators
({x,y| f($x)-f($y)}, both orientations; first-extreme wins ties via
ORDER BY key, index LIMIT 1 — real fold's strict >). Generic
collection::max/min<X> registered (strings order lexically). The
executor parses identity strings back to their kinds (D -> Decimal,
fraction -> Float, else Integer; dates stay print-form strings);
DECIMAL narrowing now gates on ORIGINAL scale (1.0D keeps scale — only
scale-0 HUGEINT sums narrow). NINE exclusions REMOVED (greatest/least/
max/min/mode Number+Date families, mixed sort) — 84 remain (from 96).
The three engine mixed-date pins converted to the print-form identity
surface. Gates: core 1391, corpus 2721, PCT 1109/1109 — all green.

**Slice 11b** (TENET RESTORATION — the carrier moves into the
database): slice 11's first cut computed print forms and comparables
from literal VALUES in Java — Java-as-evaluator, violating the #1
tenet (Java orchestrates; THE DATABASE IS THE EXECUTION ENGINE), and
silently forking semantics between literal and runtime data. REWRITTEN
type-driven: the compiler picks each element's ENCODING from its
STATIC TYPE only — Integer -> CAST(e AS VARCHAR), Float -> floatRepr(e)
(the SQL pure-float printer), Decimal -> CAST(e AS VARCHAR) || 'D',
StrictDate/DateTime -> strftime, PARTIAL date strings -> themselves —
and every VALUE computation (printing, casting, padding, comparing,
selecting) runs in DuckDB. Partial-date comparables compose via
make_timestamp(split_part(x,'-',i)...) — strptime's %Y rejects the
5-digit years compare(%2001, %10999) needs; make_timestamp reaches
year 294246. The slice-9 Java chronoCompare fold is DELETED — date
compares emit the same SQL comparables. Elements may now be arbitrary
EXPRESSIONS (the unary-minus literal special case dissolved — a minus
call is just an Integer-typed expression).

THE BOUNDARY RULE (now explicit): STATIC-TYPE-driven query
construction is the compiler's job; VALUE evaluation belongs to the
database. Cross-KIND compare constants stay (type-level facts — no
value is consulted); literal RENDERING (tdsCell, hugeWiden typing)
stays (encoding, not evaluation).

Gates: core 1391, corpus 2721, PCT 1109/1109 — identical results,
database-executed.

**Slice 12** (the PAIR machinery — zip family + pair prints, 10 more
exclusions removed, 74 remain): ^Pair(first=..., second=...) instance
literals type as Pair<t(first), t(second)> (its generic params ARE its
property types — raw ClassType broke the lowering boundary) and lower
to the STRUCT carrier directly. The kernel's commonSupertype gains the
same-raw parameterized-class arm (arg-wise LUB: Pair<String,String> ⊔
Pair<String,Integer> = Pair<String,Any>) and nominalFqn sees through
GenericType (String ⊔ Pair<...> = Any, as raw Pair always did).
HETEROGENEOUS pair collections rebuild each element struct with
PER-FIELD coercion to the LUB (Any slots take the variant carrier —
a text CAST tried to parse 'b' as JSON). toString/format print Pairs
as real pure's '<first, second>' COMPOSED IN SQL (pureToString,
recursive by static type; Any slots extract root text — '->> $'
strips JSON quoting). The WIRE rebuilds real Pair instances (the
DynamicNew pattern: newEphemeralAnonymousCoreInstance +
setValuesForProperty + classifierGenericType stamping) and hands the
interpreted cast a typed WRAPPER
(wrapValueSpecification_ResultGenericTypeIsKnown — the cast validates
the wrapper's genericType, not the instances'). The engine bridge
carries GENERIC ARGUMENTS through result types (CoreBridge maps core
GenericType -> engine GenericType — args were erased to raw Pair;
LOCAL EDIT to the user's WIP file, not committed here).
testRemoveDuplicatesByPrimitive passes by ripple. Gates: core 1391,
corpus 2721, PCT 1109/1109.

**Slice 13** (DATABASE-RAISED error parity — 8 more removed, 66
remain): DuckDB's error() function raises real pure's messages FROM
SQL — guards work for literal AND runtime values (tenet-compliant):
sqrt/asin/acos domain guards ('Unable to compute sqrt of -1.0', float
print via floatRepr); rem-by-zero ('Cannot divide 5 by zero'); at()
out-of-bounds ('The system is trying to get an element at offset 2
where the collection is of size 2'); slice inverted bounds; range step
zero; bitShift>62 (real message, replacing the compile-time throw);
date-component extraction over TOO-PARTIAL dates ('Cannot get day of
month for 2017' — statically decided from precision, message composed
in SQL); date() component ranges ('Invalid month: 13'). The adapter
strips DuckDB's TRANSPORT prefix ('Invalid Input Error: ') so raised
messages surface verbatim, and walks the interpreted call stack past
adapter frames so errors carry the TEST's own source LINE (assertError
checks it). COLUMN precision is a documented wire limit (the failing
call runs in DuckDB; no interpreted frame exists — 6 tests re-pinned
with message+line matching, column unrecoverable; the reference
excludes them too). Gates: core 1391, corpus 2721, PCT 1109/1109.

**Slice 14** (Decimal scale semantics — 8 more removed, 58 remain):
real pure computes in BigDecimal the moment ONE operand is Decimal, and
the SCALE is part of the value's surface (6.0D, not
6.000000000000000000D). DuckDB's own DECIMAL arithmetic reproduces
BigDecimal's scale rules exactly (add/sub = max scale, mul = sum,
mod = max) — the whole fix is keeping the computation IN DECIMAL: when
a decimal operand is present anywhere in an arithmetic chain, FLOAT
literals join as native DECIMAL literals at their printed scale instead
of CAST(x AS DOUBLE) (one DOUBLE operand poisons DuckDB's resolution
and the scale is lost). parseDecimal is new BigDecimal(s): the string's
own scale, statically known for literals — the cast targets
DECIMAL(38, literal scale); the 3-arg overload (registered; real pure
signature) is setScale(scale, HALF_UP), which DuckDB's string→
DECIMAL(p,s) cast matches (rounds half away from zero, raises on
overflow). type(x) now returns real pure's Type ('Integer', not
DuckDB's 'INTEGER' — an engine-lite dialect leak, corpus pin
corrected): a concrete static type IS the runtime type; the wire
resolves the name to the canonical Type instance (assertIs checks
identity). Gates: core 1391, corpus 2721, PCT 1109/1109.

**Slice 15** (enum identity + type-aware equality — net 3 removed +
4 re-classified, 55 remain): the "expected X, got X" family's root
cause was OUR adapter injecting the same FQN as both an Enum and a
shadow Class (an M3 enumeration IS a class with a name property) —
the type split in two. Fixed: enumeration-aware class extraction (enum
property types reference the Enum def, never a shadow Class), match-
clause parameter types (a: My::Type[1]) now scanned for injection, and
enum-typed results resolve to the CANONICAL enum-value instance on the
pure side (enum equality is identity there). Equality is TYPE-aware
(real pure): cross-enum and enum-vs-non-string eq/equal lower to a
static FALSE — never name-coincidence 'X'=='X' across enums, never a
DB conversion error. testMatch, testEqEnum, testEqualEnum,
testMultiPlusWithPropertyExpressions now genuinely pass. HONESTLY
RE-CLASSIFIED, not fixed: eq/equalNonPrimitive + the mapRelationship
trio are INSTANCE IDENTITY — real pure eq on instances is reference
equality, and the PCT harness inlines captured instances BY VALUE
(eq($x,$x) and eq($x,$y) arrive as byte-identical text), so identity
is erased before the wire; a static-identity attempt was built,
falsified by testEqVarIdentity, and REVERTED. deactivate() (expression
reflection) is out of vocabulary — legend-lite holds no expression
tree at run time. Gates: core 1391, corpus 2721, PCT 1109/1109.

**Slice 16** (date precision + domain honesty — 5 removed, 50
remain): subsecond PRINT PRECISION is part of a pure Date's value
(like decimal scale): adjust over a source written with more digits
than the TIMESTAMP carrier holds (6) emits the precision-faithful
STRING in SQL (strftime + the source's own static digits — an interval
can never touch digits beyond microseconds), and the wire now parses
temporal strings preserving every written digit. BC years: the JDBC
driver's java.sql.Timestamp DROPS the era (year -21457 surfaced as
+21458, epoch irrecoverably wrong) — cells fetch through java.time and
Timestamp stays the carrier only where faithful (AD). Partial dates
that HAVE the asked component (monthNumber of %2015-04) read it via
split_part from the string carrier, in SQL. parseDate over a literal
refines its abstract Date output to the kind the string's shape
determines (DateTime vs StrictDate) — the refineDecimalCarrier
pattern. HONESTLY LEDGERED, not faked: adjustBy{Days,Weeks,Months,
Hours}BigNumber land at years 1.4M-800M, beyond DuckDB's TIMESTAMP
domain (290309 BC-294246 AD) — a carrier-domain limit, annotated.
Gates: core 1391, corpus 2721, PCT 1109/1109.

**Slice 17** (format/toString + platform metamodel surface — 11
removed, 39 remain): a DATE LITERAL's print form is fully static
(subsecond digit count is part of the value — '.00' prints '.00');
%t{[EST]...} zone prefixes shift and offset-suffix IN SQL (ICU
timezone(); DST-correct); %0Nd pads the DIGITS then signs (pure, not
printf); %r composes the quoted \-escaped repr in SQL; %s/%t over a
date slot prints pure's ISO T-form. List is now real pure's List
(anonymousCollections values: T[*] registered; ^List types as
List<t(values)>, the Pair principle) with the BARE SQL LIST as its
carrier everywhere (a carrier-vs-layout split: platform carriers own
their SQL shape before classLayout — List gaining a declared property
had silently turned list() results into raw driver arrays); List
toString/format print '[a, b, c]' recursively in SQL, variant-carried
nested lists included. Bare element references inject + resolve:
enumerations are values of the (new) Enumeration<T> metaclass; Type
extends ModelElement (the real m3 chain, contracted), and a
PARAMETERIZED actual conforms to a raw class formal by erasure —
letFn's Class-through-ModelElement flows. removeDuplicates of a
to-one stays LIST-shaped (its output is [*]). Gates: core 1391,
corpus 2721, PCT 1109/1109.

**Slice 18a** (misc semantics batch — 8 removed, 31 remain):
substring's third argument is the EXCLUSIVE END (Java semantics), not
SQL's length. percentile grew real percentile.pure's boundary guards
(pos==0 → first, pos>=n-1 → last; p=1.0 indexed past the end).
covarSample guards measure the WRAPPED sides (a to-one side is a
1-element list — len(1) was a binder error) without double-wrapping.
in() gained real pure's second overload in(Any[0..1],...) — the empty
needle is FALSE (COALESCE'd, never NULL) — and membership is
TYPE-aware: a needle of a different KIND than the elements is a static
FALSE, not a DB conversion error. removeDuplicates with a CUSTOM
comparator (or key+comparator) folds real pure's compare-against-KEPT
semantics — a list_reduce over singleton-wrapped elements whose
accumulator IS the kept list, entirely in SQL; isEqualityComparator
now demands BARE parameter references (x==2+y was silently treated as
plain equality — the fast path was wrong, not the tests). Function
values: parameters are CONTRAVARIANT (equal(Any[*],Any[*]) is a legal
{T[1],T[1]} comparator — the kernel's contravariant frame now covers
multiplicity), and the harness's inline SERIALIZED function
definitions (fqn(a: T[1]): R[1] { body } — how a captured concrete
function crosses) normalize to their faithful lambda equivalent, which
also un-broke contains(collection, value, comparator). Gates: core
1391, corpus 2721, PCT 1109/1109.

**Slice 18b** (copy-with-update + inheritance injection + group hash —
2 net removed + honest re-pins, 29 remain; STANDARD SUITE LEDGER NOW
EMPTY): the language grew ^$var(prop=value, ...) COPY-WITH-UPDATE
(TypedCopyInstance: the class is the variable's static type, overrides
validate like construction, lowering rebuilds the canonical struct
with overridden fields — no new SQL shapes) — probed working with real
fold semantics. The adapter now extracts SUPERCLASS CHAINS (extends
rides the injected defs — concatenate's sibling-class LUB was
collapsing to Any without them; testConcatenateTypeInference passes
genuinely). hashCode over a group shifts DuckDB's UBIGINT hash into
signed BIGINT (pure Integer; hash VALUES are platform-specific — type
and determinism are the contract); the Standard-suite ledger emptied.
HONESTLY RE-PINNED: the fold trio — the harness serializes
^$x(prop = expr) as copy('', prop), the override VALUE is LOST before
the wire; the feature works, the tests are unrunnable from the
serialized text. Gates: core 1391, corpus 2721, PCT 1109/1109.

**Audit round 5** (three-dimension deep audit of slices 6-18b:
tenet / hardcoding / weakened checks; all findings fixed or unwound,
none rationalized): UNWOUND two value-consulting Executor heuristics —
integral-double→Long and scale-0-decimal→Long kind guessing (both DEAD
since the slice-11 identity channel; deleted along with the whole
selectionRoot plan-sniffing machinery — kinds now travel FROM SQL
only). regexpIndexOf was LEXICAL (strpos of the match text — anchored
'ab$' returned the first 'ab'); now POSITIONAL: the regex engine
itself measures a lazy anchored prefix group, and group positions
split the literal pattern at the group's paren (statically). formatDate
nano-precision printed fabricated '000' where a literal's written
digits had been carrier-truncated — literals print their own written
subsecond; the runtime pad is exact for anything the carrier can hold.
date() now guards RUNTIME components in SQL with pure's messages
(fractional seconds bounded [0,60)); the literal-only comment was
stale. Group hashCode's shift overflowed BIGINT at hash 2^64-1 (PLUS
Long.MIN_VALUE now — exact 2^63). enum-vs-Any equality was a silently-
wrong static FALSE (Any is UNDECIDED, not disjoint) — falls through.
checkCopy validates multiplicity subsumption like construction; copy
of the List carrier stays a bare array. keptDedup accumulator names
freshen by dedup depth (nested comparators captured). Contravariant
multiplicity is SLOT-scoped (a nested function result inside an active
frame wrongly widened). Ledger honesty: the four empty-string pins now
carry stable-fragment messages + per-pin reasons; the OneToOne pin is
re-labeled serialization-loss. Adjudicated and KEPT with reasons: BC
LocalDateTime fetch gate (faithful-carrier driver-bug workaround, plus
the missing midnight arm added), decimal-literal joins/static scales/
date-literal prints (static program text), the adapter's PURE_MODEL
helper functions (verbatim mirrors of the real test files' own
helpers), wrapPctList enumeration (loud fallback), inlineFunctionLiterals
(misfires fail compilation loudly). AuditRound5Test pins all of it
(core 1400). Gates: corpus 2721, PCT 1109/1109.

Update this file per slice, same as docs/SCOREBOARD.md.
