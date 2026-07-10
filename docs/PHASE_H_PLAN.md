# Phase H — StoreResolver: the plan

THE HOLE IN THE MIDDLE (PIPELINE.md §"The hole"): between G and I for CLASS
queries. This doc is the execution plan; PHASE_HIJ_LOWERING.md §5 and the
join-elision placement note (§119) are the prior design anchors.

## What the probe changed (2026-07-09)

G has NEVER type-checked the synthesized mapping bodies — they compile on
demand and nothing demanded them (queries die at TypedGetAll in lowering
first). Probing SpecCompiler.compile on model::M$class$model::Person fails
at construct #1: the normalizer emits tableReference("store::DB", "T")
with a CString database while TableReferenceChecker (built against the
query parser) expects a PackageableElementPtr. Known further gaps: the
G-audit's "legacyNavigate cannot pass the generic path" (FuncColSpec raw
vs ColSpec<A> param), 26 corpus "unbound type variable T" errors.

SO: Phase H is a TWO-FRONT arc — G-completion for mapping bodies FIRST,
then the resolver/lowering. Sequencing rationale: typed bodies are the
resolver's INPUT; designing the resolver against untypeable bodies means
designing against fiction.

## Architecture (per PIPELINE.md): a TypedSpec -> TypedSpec rewriter

StoreResolver sits BETWEEN G and I: input = the typed query (TypedGetAll
nodes intact) + the model context; output = a typed query whose class
sources are replaced by RELATION pipelines. The lowering (I) then sees
only what it already handles — THE central bet of this plan: the
synthesized mapping bodies ARE relation pipelines (tableReference ->
filter -> map(row|^Class(...))), so resolution is TYPED INLINING plus a
small set of rewrites, and Phase I/J/K need almost nothing new.

Reference material, in order of authority:
1. The master plangen (docs/reference/plangen-legacy-pre-port.java.txt)
   — 100% green historically; the BEHAVIORAL gold standard.
2. MappingResolver (V1, engine/src/main/java/com/gs/legend/compiler/
   MappingResolver.java, 1327 lines, committed) — the mostly-working
   resolver: read it for the resolution mechanics that actually pass
   tests (which constructs it handles, join emission, elision decisions).
3. MappingResolverV2 (same dir, 1398 lines, uncommitted WIP under
   DO-NOT-COMMIT) — in-flight redesign; learnings only.
4. Real legend-engine pureToSQLQuery — canonical semantics when the
   above disagree.
READ V1 AND V2 (and the relevant plangen stages) BEFORE designing the
core resolver — H2's design step starts with a structured read of all
three.

## Milestones (each: strong-assertion tests -> core suite -> corpus
## scoreboard + A/B vs runs/ archive -> commit; explain-first per item)

H0. THE CENSUS HARNESS. An eager test that compiles EVERY synthesized
    function of every corpus-shaped mapping fixture through G and buckets
    the failures. Deliverable: the complete, enumerated G-gap list (no
    extrapolation). Also wire `compileReachable` into a corpus-side probe
    if cheap. Exit: the list, committed as a doc table.

### H0→H1 CENSUS (2026-07-09, PhaseHCensusTest — 14/14 GREEN: H1 EXIT.
### The ratchet collapsed to the permanent green==total invariant.
### H0 baseline was 1/14.)

H1 fixes so far: (1) the ×11 tableReference bucket — normalizer now emits
`PackageableElementPtr(db)` (query-parser parity, option (a) below); this
unmasked a ×9 property-multiplicity bucket ("[0..1] column into [1]
property"), cleared by EMISSION: the normalizer wraps store reads bound
to [1]-declared properties in toOne(...) (buildNewInstanceToOne; navigate/
legacyNavigate/otherwise/new values and mapping-local properties exempt).
NewChecker keeps real pure's FULL NewValidator subsumption — the
hand-written surface stays pure-compatible ([0..1] into [1] is a static
error; the writer spells ->toOne()), and synthesized bodies conform
because the mapping IS the to-one assertion, said explicitly. (A checker
relaxation was tried first and rejected: it weakened a shared guarantee
to spare the normalizer an edit.) The m2m/PCM path does NOT auto-wrap:
those lambdas are user-written pure and real engine makes the user write
the coercion.

All buckets cleared (in order): tableReference emission parity (x11);
NewChecker strict subsumption + toOne EMISSION (x9); sum Integer/Float
overloads from real pure (B5); [] as Nil bottom type (A5); lite concat
retired -> plus-chain emission + fixture upper->toUpper (A3);
legacyAssocPredicate re-spelled with explicit src/tgt Relation args so
the adapter lambda's rows TYPE (C); slot-join JoinChecker arm + lite
FuncColSpec signature + typeLambda binding structured returns into b
(A2). Every fix was emission-or-real-signature — no checker was
weakened. NOT exercised by the battery: legacyNavigate typing (no
class-typed Join PM fixture emits it; its registration still carries
the free-T ColSpec shape and will fail LOUD at G until H3, which
rewrites it into join emission anyway).

Census-process findings (not G gaps): (a) `[db]T.col` without a space
after `]` fails to parse while `[db] T.col` works — juxtaposition
sensitivity, feeds the corpus parse family; (b) ~groupBy key matching
appears form-sensitive (unprefixed key + same-form PM was rejected as
per-row formula — verify intended). H0's single green body was the
derived property ($prop$ hat — query-shaped, no store constructs).

H1. G-COMPLETION for mapping bodies. DONE 2026-07-09 (14/14 census;
    b's legacyNavigate seam deferred to H3 — see note above). Was:
    a. tableReference emission/checker shape (decide: normalizer emits
       PackageableElementPtr — parser-parity — OR checker accepts both;
       prefer fixing the EMISSION to the query-side shape).
    b. legacyNavigate typing: registered signature vs FuncColSpec reality
       (the G-audit seam); likely a dedicated checker like NavigateChecker.
    c. legacyAssocPredicate / otherwise / traverse / sourceUrl / tds
       typing paths as the census demands.
    d. unbound-T family: mapping fns are generic-free — find where T leaks.
    Exit: census harness 100% green; corpus unchanged (G-only).

### H2/H3 DETAILED PLAN: see docs/PHASE_H2_H3_RESOLVER_PLAN.md
(the full research dossier + implementation plan: real-engine functional
spec, V1 join-cancellation prior art, positional rule table, milestones
M-H2a..M-H4b, fixture matrix, audit catches incl. the driver-runtime
blocker. This section below is the earlier three-reference summary.)

### H2 DESIGN (2026-07-09, from the three-reference structured read)

CONSENSUS of plangen (stage-I consumer, 100% green), V1 (committed,
mostly-working), V2 (WIP learnings):

1. RESOLUTION = replace each TypedGetAll(class) with the mapping body's
   relation pipeline; the map(row|^Class(...)) TERMINAL IS STRIPPED and
   survives only as a property->expression BINDING TABLE. plangen's
   whole consumption contract (generateGetAll 509-535, resolveColumnExpr
   1075-1097): (a) source = the pipeline with mapping ~filter baked in
   BELOW any user op; (b) resolveProperty(name) -> column-or-expression;
   (c) propertyToColumn() wholesale for whole-class output (H4).
2. ORDERING: mapping filter -> user filter -> user project; filter is a
   schema pass-through; project builds a fresh alias-keyed TDS row; a
   flat filter->project over a simple class must land as ONE
   SELECT cols FROM t WHERE pred (plangen 774-789, 847-854).
3. OBJECT-SPACE vs RELATION-SPACE: a filter BEFORE projection resolves
   $p.prop through the binding table; after projection, columns are
   literal output names (plangen 808-812). The resolver rewrites only
   object-space positions.
4. DISPATCH BY CLASS per active mapping (V1 623-654): memo per class
   FQN, `resolving` cycle guard; V1's shallowResolution is the H3 cycle
   answer. H2 has no cross-class traversal.
5. V2's ONE great idea, adopted wholesale: PURE TypedSpec->TypedSpec
   rewrite, NO SIDECAR — resolution lives in the returned tree; row
   schema stays DERIVABLE from the AST, never stored beside it (V2's
   sidecar-elimination thesis; V1's forwardStamp/restamp bug family is
   the cautionary tale). V2's cautionary tale in turn: association->join
   lifting must be UNIFORM across lambda relops, not per-relop opt-in
   bags (its filter/project asymmetry) — that constraint SHAPES H3.
6. LOUD on unmapped property. V1+plangen silently fall back to the
   property name as a column name (StoreResolution.columnFor null ->
   PropertyAccessLowering "col != null ? col : property", their own
   "Phase C.2" leak). We throw, naming property, class, and mapping.

OUR STRUCTURAL ADVANTAGE: their normalizer pre-shredded ^Class(...)
into extend-column chains, so V1 reconstructs bindings by pattern-
matching lambda shapes (forwardRelationalRename etc.). OUR synthesized
bodies keep the typed map(row|^Class(prop = expr...)) terminal — the
binding table is read directly off ONE TypedNewInstance (expressions
already typed, toOne wrappers explicit). No archaeology.

H2 COMPONENTS (new leaf package com.legend.resolver):
- ClassSource: per (mapping, class) — the typed pipeline WITHOUT its
  map terminal + rowVar + Map<String propName, TypedSpec expr> binding
  table + the source row type. Built by compiling the ClassBinding's
  realizing function (SpecCompiler — 14/14 green, H1's whole point)
  and splitting its TypedMap. Memoized per class FQN; cycle guard.
- StoreResolver: the pure rewriter. H2 arms: TypedGetAll under
  TypedFilter (object-space predicate: substitute rowVar, rewrite
  property reads through the binding table), under TypedProject
  (object-space colspec lambdas -> relation project with alias-named
  output columns), and chains thereof. TypedFrom supplies the mapping.
  Everything else: recurse structurally; store-only leftovers reach
  the lowerer's loud walls.
- Driver: resolver runs BETWEEN G and I at the driver seam (the engine
  query path calls core's Lowerer today — the 521-error TypedGetAll
  wall IS core's frontier default); wiring lands last, after the core
  tests are green.

H2 NON-GOALS (H3+): association navigation/joins/elision, embedded/
otherwise, multi-set-ID, milestoning, graph/whole-class output (H4),
M2M chains.

H2. RESOLVER SKELETON + SIMPLE CLASS QUERIES. StoreResolver rewrites
    TypedGetAll(class) -> the mapping fn's TYPED BODY (beta-inlined,
    memoized per (mapping, class)); TypedNewInstance map-terminals stay
    UNTOUCHED at this stage — instead the resolver keeps the relation
    pipeline UP TO the map and re-schemas it as Relation<(cols)> for TDS
    queries (project/filter over class properties read through the
    ^Class bindings: property -> bound column expression). Property
    access on instances = the binding lookup. Exit: `Person.all()
    ->project(...)` / `->filter(...)` end-to-end on DuckDB; the corpus
    TypedGetAll wall starts falling (expect the largest single unlock of
    the project; watch class-fetch/serialize progress too).

H3. NAVIGATION + JOINS. navigate/legacyNavigate -> join emission against
    the mapping's join chains; association ends via legacyAssocPredicate
    -> EXISTS (filter position, per PHASE_HIJ_LOWERING §133) or LEFT JOIN
    (projection position). JOIN ELISION lives HERE (mapping-implied joins
    only; user-written joins are NEVER elided — standing rule). Exit:
    multi-class corpus tests (AssociationIntegrationTest,
    RelationalMappingIntegrationTest) green.

H4. GRAPH OUTPUT. TypedGetAll without projection -> ResultShape.GRAPH:
    serialize/graphFetch lower to a projection of the fetch tree's
    columns + row->object assembly in Executor.Graph (PCT contract;
    check master plangen's graph assembly). Exit: serialize corpus family
    (~78 errors) converts.

H5. LONG TAIL per corpus demand: embedded/otherwise/M2M mappings,
    milestoning (parse currently drops it), multi-set-ID dispatch,
    the mapping-operation parse gaps (69 parse-bucket errors feed H
    fixtures), unbound-T stragglers.

## Design rules

- The resolver NEVER emits strings: TypedSpec -> TypedSpec, dispatch on
  sealed types, exhaustiveness per the root invariant.
- Reuse relation lowering; if H needs a new lowering construct, first ask
  whether the resolver can rewrite it into an existing one.
- Inline-then-simplify over special-casing: the mapping body is the
  single source of truth; resolver rewrites are local and pinned.
- Loop rule + A/B archives + javac-verify (IDE ECJ trap) throughout.
- Fixture gaps (T_EVENTS/EventDatabase 24, test::Person unknown 28) get
  diagnosed during H0 — they are likely fixture-model bugs, not H work.
