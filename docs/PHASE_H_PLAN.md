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

H1. G-COMPLETION for mapping bodies. Fix each census bucket; known ones:
    a. tableReference emission/checker shape (decide: normalizer emits
       PackageableElementPtr — parser-parity — OR checker accepts both;
       prefer fixing the EMISSION to the query-side shape).
    b. legacyNavigate typing: registered signature vs FuncColSpec reality
       (the G-audit seam); likely a dedicated checker like NavigateChecker.
    c. legacyAssocPredicate / otherwise / traverse / sourceUrl / tds
       typing paths as the census demands.
    d. unbound-T family: mapping fns are generic-free — find where T leaks.
    Exit: census harness 100% green; corpus unchanged (G-only).

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
