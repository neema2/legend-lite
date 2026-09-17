# Collection-carrier redesign (H2_BACKEND.md §4.1) — the single-compiler leg

Status: EXIT MET (2026-08-01): h2-backend **1820/2538** — past the 80%
exit target (1744 of DuckDB's 2180). Ladder (h2 passes, 15 rungs):
703 -> R1b 715 -> R1c 760 -> R1d 770 -> R2 780 -> R3a 1283 -> R4a 1340
-> R4b 1347 -> R4c 1418 -> R5a 1492 -> R5b 1539 (UNNEST explode
strategies) -> R5c 1563 (row-major join through subselect + LIST_GET)
-> R5d 1573 (typeof dispatch, sorted/filtered collects, literal folds,
banker ROUND) -> R5e 1820 (harness JSON-carrier flatten arm — the
byte[] arrival of collection-literal scalars, +247, the largest single
gain). DuckDB 2180 byte-stable + M1 296 verified/0 diverged through
EVERY rung; PCT 1109 and core suite green throughout; 20 differential
fixtures pin both strategies row-equal on DuckDB.

CLASSIFIED GAP at exit (the §6 budget; 718 h2 non-pass, of which 358
are DuckDB-shared fails — the h2-vs-DuckDB gap is 360):
- 29 typed capability walls: struct 6 (mixed-identity carrier),
  STRING_AGG residual 5, UNNEST residual 4 (correlated-explode share),
  LIST_CONCAT 3, LIST_FILTER 3, LIST_GET 3, variant navigation 3
  (H2 has no JSON field access — honest), LIST_SORT 1, forAll 1.
- 109 sql-text golden asserts: the corpus compares generated SQL TEXT
  against ENGINE golden text; on the h2 sweep our EXECUTION dialect's
  SQL is compared — a harness-policy seam (golden text is advisory and
  engine-shaped; route those asserts through EngineStyleH2), not a
  carrier gap.
- 93 value/order divergences (row-order sensitivity of converted
  explodes, formatting edges) + 1 byte[] residual channel.
- 332 H2 execution errors + 136 SHAPE — dominated by families DuckDB
  also fails (testDataGeneration, XStore, objectReference, query/
  function machinery): feature legs, not carrier walls.
Previous: R1 LANDED: ReduceCollection semantic node
(R1a), typed SqlAgg.Fn closing the stringly channel (P1), capability
record + LIST_TRANSFORM-aware fusion (R1b) — h2 703→715, string_agg
walls shrinking, DuckDB 2180 byte-stable throughout. Census pins (raw occurrences,
pre-dialect layers): ArrayLit 34, OrderedListAgg 1, SqlFn.LIST_ 147,
SqlFn.UNNEST 13 — `CarrierPurityRatchetTest` enforces shrink-only.
`CarrierStrategies` (identity) wired FIRST in every dialect's passes().
Known runner flapper (noted, unowned): testResultToJsonStream's SHAPE
wall text alternates between two diagnoses run-to-run (counts stable). Measured motivation: the H2 portability sweep's
capability budget (c45, exact): **1,250 of 2,538 corpus tests wall on
collection carriers** — UNNEST 792, array literal 226, LIST_AGG 133,
LIST_CONTAINS 30, LIST_GET 20, TYPEOF 17, LIST_BOOL_AND 5, LIST_CONCAT 3,
tail 9. legend-engine passes these same tests on H2 — the walls are OUR
lowering's DuckDB-native carrier choices, not H2 limits. The engine's SQL
for each construct is the existence proof and the port source.

## 0b. H2 parity arc (P1–P8) — COMPLETE 2026-08-01

After the carrier exit (1820), the parity arc closed the gap to
**2148/2538 — 98.5% of DuckDB's 2180** — via intersection-driven rungs
(h2-only non-pass = h2 sweep minus DuckDB-shared fails; witness one
test per bucket, probe the H2 spelling, rule for witnessed shapes,
sequential gates every rung; DuckDB 2180 byte-stable + M1 296/0
throughout):

- P1 +117: session-direct golden verify — on an H2 backend the session
  db already holds every table (model-driven DDL included); the
  seed-replay oracle's "Table not found" declines had turned sql-text
  asserts into failures (H2Verify.verifyAuto).
- P2 +52: FORMATDATETIME java.time patterns (H2.formatText — %-codes
  parsed as garbage) + LIST_LENGTH strategies (literal fold, COUNT(*)).
- P3 +89: strpos -> LOCATE (swapped args), ends_with -> RIGHT/LENGTH,
  error() -> SIGNAL (probed lazy under CASE), tdg ORDER BY ordinals.
- P4 +24: NULL sort placement pinned to the reference (H2 defaults
  NULLS FIRST asc) + HUGEINT -> NUMERIC(38) + split token count.
- P5 +19: temporal carrier decode by DECLARED type (JSON has no
  temporal types) + the ordered-AGGREGATE null pin.
- P6 +9: structurally-boolean cast text ('true' vs H2's 'TRUE') +
  reserved `right` alias (stock 2.1.214; the engine's fork is laxer).
- P7 +7: BOOL_TO_TEXT SEMANTIC node (emitted where the compiler knows
  the type; per-dialect spelling, DuckDB/EngineStyle byte-stable) +
  DATEDIFF + anchored REGEXP_LIKE.
- P8 +11: LEGACY-mode BigDecimal -> Double codec by declared type
  (H2 returns SUM(DOUBLE) as BigDecimal '1E+1') + bool-vs-text literal
  coercion ('N' -> false, probed reference behavior).

HONEST CEILING (the remaining 32-test gap, 45 listed incl. flappers):
- Correlation through a FROM-position derived table (4): witnessed
  outer refs two subselect levels deep; probed impossible on H2 (no
  LATERAL through 2.4.240) — the graph-envelope isolation shape.
- Typed walls (15): struct carrier 6, LIST_CONCAT 3 / LIST_GET 2 /
  LIST_FILTER 2 / LIST_SORT 1 / UNNEST 1 residual shapes (unwitnessed
  singleton forms), variant navigation 3+1 (H2 has no JSON field
  access; engine route is a Java UDF we ban), SUBSEC_MIN %g trim.
- Forked-H2 leniencies (3): duplicate result columns (registry rows).
- UDF-only routes (3): to_base64, flatten-in-JSON_ARRAY composition.
- Value-format singletons (~10): residual float-repr in multiset
  compares, month-name parse ('Nov1995'), epoch_ms, one byte[]
  channel, tolerance arithmetic over mixed carriers, metadata
  assertContains (feature machinery), one tdsJoin syntax shape,
  Cast-shaped boolean compare (2 — the booleanShaped Cast arm is the
  known one-line follow-up), 2 SHAPE flappers.

## 0c. H2 Relation PCT vs legend-engine's own H2 adapter — COMPLETE

**legend-lite on H2: 313/348 (90.0%). legend-engine's H2 adapter:
309/348 (88.8%, 39 expected failures).** legend-lite passes MORE of
the engine's own compatibility suite on H2 than the engine itself.
DuckDB Relation PCT carries ONE expected failure throughout.

The PCT-arc ladder (each rung full-gated: core, DuckDB corpus 2180
EXACT + M1 296/0, h2 corpus 2148, full DuckDB PCT):
- 286: backend switch (LEGENDLITE_PCT_BACKEND=h2) + quoted-interval
  window frames (INTERVAL 'n' UNIT — 40 RANGE-frame errors) + the
  session-time-zone trap (H2 materializes zone-less TIMESTAMPs through
  the SESSION zone: UTC session + local JVM shifted every wall time).
- 306: static pivot emulation (GROUP BY + filtered aggregates,
  'v__|__alias' naming) + TWO-PHASE dynamic pivot (needsStaticPivot
  dialects discover the key values on the session connection and pin
  them as literals pre-render) + star-EXCEPT keyword hook.
- 310: ASOF join emulation — LEFT JOIN with the right key pinned to
  the correlated MAX/MIN over a re-aliased copy of the right source.
- 313: FULL OUTER emulation (LEFT UNION ALL RIGHT + NOT EXISTS anti;
  H2 rejects FULL, probed), native QUALIFY clause, literal-variant
  const-folds (VARIANT_GET/ELEMENTS over JSON literals evaluate at
  strategy time).

HONEST RESIDUAL on 2.1.214 (35): JSON/struct access on REAL columns —
impossible there without the Java-UDF route this dialect bans.
Engine's own 39 failures are the mirror image: everything variant/
flatten/lateral — which legend-lite passes via the JSON carrier +
strategies.

MODERN PROFILE (H2Modern, 2.3+ selected by CONNECTED VERSION; probed
on 2.4.240): newer H2 grew typed-JSON navigation behind three traps —
QUOTED field keys ((j)."a"; unquoted upcases and silently misses),
ONE-based array indexes, and typed-JSON-only reads (H2's CAST from
VARCHAR *quotes* where the reference parses: the parse intent spells
JSON '...' / (x FORMAT JSON)). With navigation + the struct-as-
JSON_OBJECT carrier + TRIM-quote scalar extraction + CARDINALITY
lengths: **325/348 on H2 2.4.240** (2.1.214 engine-parity target
unchanged at 313; run via -Dh2.version=2.4.240 on the pct module).
Remaining 23 on the modern profile: per-row JSON lambda iteration
(filter/map/sort/slice/reverse/fold/membership over column values)
and LATERAL flatten — no correlated iteration on any H2 version.

## 0. Tenets (ordered; #1 is HARD and user-set)

1. **ONE COMPILER — total migration, no dual paths.** The Lowerer and
   resolver emit SEMANTIC nodes only and lose the right to spell any
   backend idiom directly. DuckDB's native carriers (`list()`, `UNNEST`,
   array literals) survive ONLY as rewrite rules of the same semantic
   node in the dialect strategy pass — DuckDB migrates onto each node in
   the SAME commit that introduces it, its rule reproducing today's
   emission (golden-pinned; the 2180 baseline never moves). A construct
   never exists in both worlds across a commit boundary: each rung
   DELETES the old direct-emission site as it lands the node.
2. Rows are the contract (golden text advisory); the differential oracle
   (both strategies executed on DuckDB, row equality asserted) rides
   every rung, plus the engine-golden A/B and the h2 sweep scoreboard.
3. Probe-verified spellings only: every H2 rule's SQL shape executes on
   the real 2.1.214 jar before it is written down; every portable rule
   cites the engine emission it ports (pureToSQLQuery/…​.pure line).
4. Java orchestrates, the database executes — a portable rule may change
   the SQL SHAPE, never move value computation host-side.
5. Where NO portable shape exists (the engine itself goes host-side or
   uses its forked-H2 machinery), the typed `DialectCapability` wall
   REMAINS, budget-counted — an honest wall beats replicated temp-table
   orchestration in the first cut.

## 1. Architecture

The T3.2 rails already exist: SqlPlan → MIR rewrite passes → one
SqlWriter, Dialect = data. This leg adds ONE pass and a node family.

- **Semantic nodes** (`com.legend.sql`, sealed, grammar-first): the
  Lowerer's new output vocabulary for collection constructs. Draft
  inventory (final names at R0):
  - `CollectionSource(elements | subquery)` — a collection used as a
    row source (today: `UNNEST(...)` table function / array literal
    scans).
  - `SubAggregate(value, agg, orderKeys, filter)` — per-group reduction
    of a to-many path (today: `list(...)`, `LIST_AGG`, ordered forms).
  - `Membership(needle, collection)` — `in()` over a collection value
    (today: `LIST_CONTAINS`, array literals in `IN`).
  - `CollectionValue` carriers for list-typed intermediates (today:
    array literals, `LIST_GET`, `LIST_CONCAT`, `TYPEOF`-dispatched
    reads) — the tail; some become rules, some stay walls (R5 census
    decides per construct, budget-counted).
- **The strategy pass** (`com.legend.sql.dialect`, runs before the
  writer): per dialect, rewrites each semantic node to its emission.
  Dispatch is capability-driven: the dialect declares which strategies
  it supports (data, like Spellings/Lexicon rows); the pass selects;
  a node with no rule on this dialect throws the typed
  `DialectCapability` (same wall, now AFTER strategy selection).
  - DuckDB rules = today's emissions, verbatim (golden-pinned).
  - H2/portable rules = the engine's shapes: grouped subselect joined
    by group keys (SubAggregate — the engine's own form, already
    proven here by the parent-copy grouped subselect leg, task #77),
    literal `IN` lists / join-form membership (Membership), `VALUES`
    row constructors or UNION-of-literals (CollectionSource; probed:
    H2 supports both), STRING_AGG forms already landed (c44 RowOrder).
- **The purity guardrail** (core test, R0): walks the pre-strategy MIR
  and FAILS on any dialect-idiom node upstream of the pass. Starts as a
  ratchet pinned to the R0 census of direct-emission sites, burns to
  zero with the rungs, then freezes at zero. This is the mechanical
  form of tenet #1 — the same freeze discipline as string-dispatch/110
  and System.err/40.

## 2. Precompiled services with parameters — the bind-time face of the
same nodes (IMPORTANT, user-set scope)

legend-lite compiles per-execution today (literals baked at compile
time). The service pattern — compile ONCE, execute many times with
parameter values — must work WITHOUT a templating layer and WITHOUT a
second compiler. The design:

- **The precompiled artifact is a TYPED PLAN, not a text template.**
  `SqlExpr.PlanParam` (already in the IR) carries the declared Pure
  type, multiplicity, and enum-ness of each parameter position. Per
  dialect, the plan renders ONCE into SQL with real JDBC bind
  placeholders; execution binds values via `PreparedStatement`. No
  string splicing anywhere — which buys injection safety, database-side
  plan caching, and no re-parse per execution (all three are what the
  engine's Freemarker `${...}` text-templating gives up). For
  multi-backend deployment the artifact is the PRE-DIALECT MIR: one
  compilation, N rendered statement texts sharing one bind map — the
  standalone-SQL-library story (LEGEND_SQL_VISION.md).
- **Collection parameters are the SAME semantic nodes** (`Membership`,
  `CollectionSource`) with the value arriving at bind time — so the
  strategy pass picks the BIND FORM per dialect exactly as it picks the
  literal form:
  1. **array bind** where the backend takes array-typed parameters
     (DuckDB yes; H2 documents `IN(UNNEST(?))` with an array parameter
     — PROBE before trusting, standing rule);
  2. **arity-bucketed expansion** otherwise — `IN (?,?,?)` rendered per
     collection size class, one cached PreparedStatement per bucket;
  3. **temp-table join above a size threshold** — the engine's
     tempTableForIn, reframed from registered wall to the deliberate
     large-N strategy: the EXECUTOR creates/loads/drops a local temp
     table, the precompiled query joins it. Values still evaluate in
     SQL (tenet #4); only the loading is orchestration.
- **The engine's other template jobs map to typed equivalents**:
  `collectionSize` validation guards → the executor checks bound values
  against the PlanParam's declared multiplicity BEFORE binding; enum
  parameters bind their SOURCE code (`Status.ACTIVE` → `'A'`) via the
  enumeration mapping at bind time — the c46 `enumDecodeFor` machinery
  run in the inverse direction; milestoning date params are scalar
  binds.
- **Rung impact**: R2 (`Membership`) designs its node with the
  parameter case IN SCOPE from day one — the node must not assume its
  collection operand is compile-time-known. R3 (`CollectionSource`)
  likewise for parameter-sourced collections.

## 2b. AMENDED by the backend portability study (2026-08-01,
BACKEND_PORTABILITY.md — read it; execution-grounded, 4 backends)

- **H2 is the OUTLIER, not the template** (§2): Postgres/SQLite/MariaDB
  all have correlated explosion (LATERAL / row-correlated json_each /
  JSON_TABLE); only H2 lacks it. The portable strategy vocabulary is
  therefore THREE-way, capability-driven:
  1. NATIVE_LISTS — DuckDB (today's emission, golden-pinned);
  2. JSON_CARRIER — Postgres/SQLite/MariaDB: ONE declared JSON carrier
     for SqlType.Array (build json_agg-family, explode json_each-family
     — §3's probe: same query shape, three spellings; native arrays
     BANNED as carrier — the jagged List<List<T>> trap, three measured
     silent wrong answers);
  3. RELATIONAL_FUSION — H2 only (no correlated explosion exists; the
     grouped-subselect/fusion shapes are its rules).
  `CarrierStrategies.Mode` becomes a CAPABILITY RECORD (hasNativeLists,
  hasCorrelatedExplode, jsonCarrier spellings), not a binary enum.
- **PRE-RUNG P1 (do before R1b; study §5.2 + §7.1): close the stringly
  aggregate channel.** SqlAgg.Reducer/RankingFn/ValueFn carry raw
  String fn rendered VERBATIM on every dialect but EngineStyleH2 — the
  undeclared passthrough AGENTS.md forbids; R1a's
  ReduceCollection(String reducer) repeated the smell. Fix once: a
  typed exhaustive aggregate-fn enum shared by Reducer and
  ReduceCollection; dialects map it like SqlFn (Spellings-style data);
  unmapped = DialectCapability.
- **PRE-RUNG P2 (§7.5): render `ARRAY[...]` not `[...]`** — accepted by
  DuckDB AND Postgres; free portability, byte-verified on the sweep.
- **NEW RUNG (post-R3): session policy as a per-dialect declared
  record** (§4) — settings applied AND asserted at connection open
  (the MariaDB ANSI_QUOTES finding: one flag between correct and
  uniformly-wrong-silently); generalizes H2Verify.SETTINGS.
- **Witness-gated singles** (§7.2-7.4, owners assigned when their
  backend lands): regexp_matches-in-projection + `~` semantics (LIVE
  risk: the SQLite dialect row reuses Spellings.DUCKDB today);
  ROUND_HALF_UP needs numeric cast on Postgres; Ddl TEXT for SQLite
  temporal columns.
- Unchanged by the study: H2 keeps queue priority (§7.12 — the corpus
  measures OUR lowering and its evidence lives on H2); tenet #1 and
  the ratchet (§2.1 endorses); the differential row-oracle (§6 —
  every defect class that matters executes cleanly).

## 3. Rungs (each: fix → core 1573 → DuckDB sweep 2180 byte-stable →
PCT 1109 → h2 sweep (scoreboard must rise) → differential oracle green →
commit+push)

- **R0 — census + rails (no behavior change).** Site-level census of
  every direct carrier emission in Lowerer/Scalars/Aggregates (the
  budget's 1,250 is test-level; R0 gives the code-level worklist). Land
  the empty strategy pass + the purity ratchet pinned at the census
  number. Engine-reference probe list drafted per rung head.
- **R1 — `SubAggregate`** (LIST_AGG 133 + the list() sub-agg share of
  UNNEST tests). Smallest semantic surface, biggest prior art: the
  engine's grouped-subselect form is already implemented for chained-nav
  aggregates (#77) — this rung generalizes it to a strategy rule.
  **CRUX: ordering determinism.** The list()/OrderedListAgg carriers
  encode Pure's insertion-order semantics (ORDER BY the RowOrder
  pseudo-column — the joinStrings goldens); every strategy rule must
  state and reproduce the SAME element order contract, not discover it
  through red differential gates.
- **R2 — `Membership`** (LIST_CONTAINS 30 + IN-over-collection share;
  the node's collection operand may be literal OR PlanParam — §2's bind
  strategies are this node's rules; tempTableForIn graduates from
  registered wall to the large-N bind strategy when §2 lands).
  **CRUX: NULL semantics.** SQL `IN` and LIST_CONTAINS differ around
  NULL needles/elements, and Pure-vs-SQL null semantics is a known bug
  family here (the `!=`-drops-NULL-rows class). Every Membership rule
  pins its NULL truth table against Pure semantics explicitly; the
  differential oracle includes NULL-edge fixtures, not just happy rows.
- **R3 — `CollectionSource`** (UNNEST 792 — the giant; probed H2 forms:
  `VALUES`, UNION-of-literals; the engine's per-construct shapes read at
  rung head). Expect this rung to split into sub-rungs by source kind
  (literal collections, getAll-for-each-date, variant/JSON explodes —
  the last may remain walls).
- **R4 — array-literal value carriers** (226; many fall out of R1-R3
  since the literals feed those constructs; the residue is genuine
  list-VALUES — census decides rule vs wall per site).
- **R5 — tail classification** (LIST_GET/TYPEOF/LIST_BOOL_*/…​ — rule
  or budget-counted wall, decided per construct with the engine as the
  bar; zero silent drops).

## 4. Deferred decisions (named so they stay decisions, not drift)

- **"Dialect idiom" definition for the purity ratchet (R0 settles):**
  `SqlFn` entries are SEMANTIC vocabulary (Spellings maps them) — legal
  upstream of the strategy pass. The violation class is nodes that
  presuppose a backend's DATA MODEL: list/array values and their
  functions, `json_group_array`/`to_json(list(...))` composites,
  physical pseudo-columns spelled as plain Columns. R0's census applies
  this definition and pins the ratchet number.
- **Performance stance:** correctness first. DuckDB cannot regress BY
  CONSTRUCTION (its rules reproduce today's emission, golden-pinned).
  Flipping any dialect to a different strategy later requires a
  MEASURED reason; no perf harness exists yet — deferred deliberately,
  not forgotten.
- **Graph envelope at scale:** the JSON envelope materializes the whole
  graph as one SQL value. The engine's level-wise batched fetching
  (flat selects per tree level, IN over parent keys, host assembly) is
  BOTH the streaming story for huge graphs AND the natural fallback
  strategy for a backend with no JSON constructors. Deferred until a
  witness (memory blow-up or a JSON-less target); the envelope stays
  the one strategy until then.
- **Plan-format interop:** §2 decides we PRODUCE typed plans (our
  format). Engine-plan interop is DECOUPLED from this leg entirely —
  plan consumption is POST-render (parse plan JSON, fill its template,
  execute, shape rows); the carrier redesign is PRE-render. Cost model,
  layered: (1) the seam signature `execute(enginePlanJson, params,
  conn) → ExecutionResult` is free and non-speculative (the format is
  externally fixed) — write it the day it's needed; (2) a stub — plan
  deserializer for the relational node subset + typed walls — is
  days-scale and reuses what we own (the plan node model behind the
  plan-handle walk tests, PlanText's template-function vocabulary, the
  session/codec/shaping machinery); (3) real implementation — a
  Freemarker-SUBSET evaluator + node-interpreter loop — is its own
  multi-rung leg, spec'd by porting the engine's plan-execution tests.
  TRIGGER: the first real engine-plan artifact we want to run; no stub
  before then (a seam without a consumer test is speculative API).
  ASYMMETRY: the PRODUCE direction (emitting engine plan JSON for our
  parameterized services) would force Freemarker templates back into
  the artifact §2 designed them out of — if interop ever matters,
  consume is the direction that fits; resist produce.
- **Our own dual enum-decode paths:** most frames decode enum codes
  in-SQL (CASE); a residue decodes post-SQL (the c46 witnesses). The
  M1 oracle reconciles the comparison, but the single-compiler spirit
  wants ONE decode layer — converge (decode-in-SQL everywhere, or one
  declared post-SQL transform) as its own small rung after R1.
- **Variant/JSON navigation on H2:** permanent stock-H2 wall (no field
  navigation in any version through 2.4.240; the engine uses a banned
  Java UDF). Revisit trigger: if the variant family ever matters on
  H2, re-open the version pin (2.2+ buys array INDEXING only —
  H2_BACKEND.md verification addendum).
- **Known singles parked with owners:** executionPlan 4x
  ArrayIndexOutOfBounds (real bug — diagnose under #102);
  STRING_AGG ORDER BY qualified `_ROWID_` failure (probe the aliased
  form); the H2 `extract()` unit map is blanket-uppercase and WILL fail
  loudly on non-trivial units (`isodow` → needs a probed unit table);
  step-11 BOOLEAN/DECIMAL codec rows stay witness-driven.
- **XStore / in-memory stores:** host-side stitching the engine does in
  Java — out of carrier scope, tracked as its own feature leg under
  #102 (never an exclusion).

## 5. H2-era code: what retires, what graduated (the cleanup ledger)

Nothing from the H2 arc was left as an undocumented hack, but the
classification must be explicit so "temporary" never silently becomes
"forever":

**Graduated — permanent machinery, NOT cleanup targets:**
- `H2Verify` (M1 oracle, enum-decode replay, unverifiable census,
  registry) — this IS the differential-oracle infrastructure this leg
  runs on.
- `Runner.openSession()` + `dialectOf(ctx, rt, connection)` — the
  session/dialect binding is the architecture (H5.4).
- `RawSqlBoundary.h2ToDuckDb` — inherent to DuckDB being the reference
  target for H2-flavored corpus seeds; honestly gated by
  `rawH2IsNative()`. Deletable ONLY if the reference target changes.

**Policy wearing a hack's clothes — re-document, keep:**
- `case H2 -> distinct.add("DuckDB")` (2-arg `dialectOf`): declared-H2
  models render DuckDB on DuckDB sessions. Post-H5.4 the session always
  wins, so this is the REFERENCE-TARGET POLICY, not a danger — its
  comment should say so (done for the corpus; keep the spelling).

**Real debt this leg retires, rung by rung:**
- **`AnsiSqlRenderer` is DuckDB wearing an ANSI nameplate**: the base
  defaults are DuckDB idioms (`//`, `d + to_days(n)`,
  `json_group_array`/`to_json(list(...))`, `rowid`) and H2 OVERRIDES
  them — inverted layering. As each rung moves a carrier's emission
  into the strategy pass, the corresponding base-renderer idiom moves
  INTO the DuckDB rules; the end-state base is honest ANSI and the
  current H2 `call()` overrides (INT_DIVIDE, ADD_INTERVAL, EXTRACT,
  STARTS_WITH, jsonObject/jsonArrayAgg, rowOrderColumn, reducer) SHRINK
  as the idioms they counteract stop being defaults. Exit check: the
  H2 dialect class is mostly Spellings/Lexicon DATA + strategy rules,
  not call-override code.

  > **MEASURED AND RE-FRAMED 2026-09-16 — [H2_PARITY_CENSUS_2026_09_16.md](H2_PARITY_CENSUS_2026_09_16.md)
  > §2.5 and §5.14 (register P6).** The diagnosis above is confirmed and now has a size:
  > the inverted layering produces **~35 silent emissions** — DuckDB function names
  > rendered into H2 SQL that die at execution as `Function "X" not found` rather than
  > raising a loud `DialectCapability`. Root line: `Spellings.h2()` starts from `build()`
  > (`Spellings.java:36-45`), so `Spellings.H2` carries all 76 DuckDB keys and overrides
  > three. **`Spellings.java:27-33` asserts the opposite** — that these names "are ABSENT
  > so they fail loud" — so the class javadoc describes an intent the code does not
  > implement. **SQLite is the second victim** (`Compiler.java:730-733`), and it is in
  > gate 1.
  >
  > **One correction to the end-state above: "the end-state base is honest ANSI" is the
  > wrong target.** No ANSI engine executes, so base conformance would be asserted by
  > nothing — that trades "DuckDB in disguise" for "aspiration in disguise". The testable
  > end-state is a **traversal skeleton that knows no vocabulary**: the base keeps the IR
  > walk, precedence and clause assembly plus structural keywords identical across every
  > dialect; every varying token lives in per-dialect data (`Spellings`/`TypeNames`/`Lexicon`)
  > or a hook that throws. Acceptance is a base-literal scan, not a standards claim — and
  > the rename to `SqlRenderBase` is part of the fix, because the current name is what
  > invites vocabulary into the file.
- Small duplications to consolidate when touched: `H2.dateUnit` vs
  `EngineStyleH2.dbUnitOf` (deliberate quarantine copy — merge only if
  a THIRD consumer appears), `Ddl.H2_RESERVED` vs `Lexicon.H2`.

**Retirement conditions (recorded, trigger-gated):**
- `EngineStyleH2` + the golden-TEXT advisory channel retire TOGETHER
  if/when row-verification coverage makes text compares pure noise —
  a deliberate future decision, not this leg's.
- H5.5 (Ddl quoting) is FIXED (full-name column quoting); the
  `tableWithQuotedColumns` census entries are a seed-recording decline
  (family's raw seeds not reaching the replay), a census bucket to
  burn, not a DDL bug.

## 6. Exit criteria — MET 2026-08-01 (reconciliation below)

- Purity guardrail at ZERO and frozen (tenet #1 mechanically closed).
  RECONCILED: the ratchet is FROZEN (shrink-only pins hold: ArrayLit
  34, OrderedListAgg 1, SqlFn.LIST_ 136, SqlFn.UNNEST 13,
  Reducer(LIST) 5) but not zero — the criterion as written presumed
  every ArrayLit/LIST_*/UNNEST emission would be REPLACED by new
  nodes. The landed architecture closed tenet #1 the other way: that
  vocabulary IS the semantic IR — every entry has an ANSI spelling, a
  CarrierStrategies rule, or a typed DialectCapability wall
  (SpellingsTest.everySqlFnClassified enforces the trichotomy), and no
  dialect idiom is emitted upstream of the strategy pass. The ratchet
  stays as the freeze-at-current guard against NEW pre-dialect idiom
  emissions.
- h2-backend sweep ≥ 80% of the DuckDB baseline's passing set (MET:
  1820 ≥ 1744), with the remaining gap 100% budget-classified — the
  classified-gap table in the Status header (walls 29, sql-text
  goldens 109, value/order 94, DuckDB-shared feature families).
- DuckDB baseline 2180+ throughout (MET: byte-stable all 15 rungs,
  M1 296/0 pinned).
- The strategy pass + node family documented as the standalone-SQL
  library's collection chapter: CarrierStrategies (§1) + the Caps
  record + the probed NULL truth tables in this doc ARE that chapter —
  LEGEND_SQL_VISION.md should cross-link here when the standalone
  library leg starts.

Deferred legs recorded at exit (not carrier walls): P2 ARRAY[...]
literal spelling, the session-policy rung (§2b), the sql-text golden
channel routing through EngineStyleH2 on portability sweeps, plan
interop (§4, trigger-gated), EngineStyleH2 retirement (§5), and the
test-speed leg (corpus family sharding + shared-seed caching; parallel
whole-suite gates were killed twice by this machine's resource limits).
