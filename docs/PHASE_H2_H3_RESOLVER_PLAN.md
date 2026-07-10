# Phase H2+ — StoreResolver: detailed plan (with real-engine functional requirements, join cancellation, lean SQL)

## Context

Phase H fills "the hole in the middle": between G (type check) and I (SQL lowering) for CLASS queries. H1 is DONE (census 14/14 — every synthesized mapping body type-checks; committed `c9405bb`). The corpus wall is `TypedGetAll` (~521 errors): class queries die at lowering because nothing resolves `Class.all()` against a mapping.

This is the crucial seam of the whole platform. Before building we (a) read our three internal references (master plangen = behavioral gold standard; MappingResolver V1 = committed mostly-working; V2 = WIP learnings), and now (b) read the REAL legend-engine `pureToSQLQuery` + its optimization passes for the FUNCTIONAL requirements — especially join cancellation/deduplication and SQL leanness, which the internal references under-specify.

Design invariants already settled (docs/PHASE_H_PLAN.md "H2 DESIGN" section, written this session):
- Pure `TypedSpec -> TypedSpec` rewrite, NO sidecar (V2's one great idea; V1's identity-keyed sidecar caused the forwardStamp/restamp bug family).
- The synthesized body `tableReference -> [mapping ~filter] -> map(row|^Class(prop=expr...))`: the map TERMINAL IS STRIPPED into a property->typed-expression binding table (our advantage: bindings sit in ONE TypedNewInstance, already typed, toOne explicit — no V1-style lambda-shape archaeology).
- Ordering: mapping filter -> user filter -> user project. Filter = schema pass-through; project = fresh alias-keyed TDS row.
- Object-space vs relation-space: filter BEFORE projection resolves `$p.prop` via bindings; after projection columns are literal names.
- Dispatch by class per active mapping (from TypedFrom); memo per class FQN; `resolving` cycle guard (V1's shallowResolution is the H3 cycle answer).
- LOUD on unmapped property (V1+plangen silently fall back to property-name-as-column — their own tracked leak; we throw naming property, class, mapping).
- Association->join lifting must be UNIFORM across lambda-bearing relops (V2's filter/project asymmetry is its cautionary tale).
- Store-only typed nodes reaching the lowerer = resolver bug, said loudly (TypedJoinSlot arm already landed).

## PENDING INPUT (explorers running)

- [x] REAL ENGINE §A: DONE — see below
- [x] REAL ENGINE §B: DONE — see below
- [x] OURS §C: DONE — see below
- [x] V1 §D: DONE — see below

## §D findings — V1 join cancellation (the prior art; line refs = engine/src/main/java/com/gs/legend/...)

1. DEMAND ANALYSIS — two feeds, both produced during type-checking, keyed by class FQN:
   (a) associationNavigations: every `$x.<assocProp>` access anywhere (projection/filter/keys/graphFetch — one funnel, TypeChecker resolvePropertyOnClass :1509-1512) records the assoc property name; also seeds the touched-class closure so `$p.children->isEmpty()` still compiles the target's mapping.
   (b) classPropertyAccesses: every scalar property read (:1500-1501); drives the touched-mapping compile fixpoint AND scalar extend-col pruning.
2. GATING: MappingResolver :840-846/:1028-1031 — an association extend whose alias ∉ neededAssocs yields SKIP: its JoinResolution descriptor is never built, no recursion into the target mapping. Embedded is UNGATED (always available — costs nothing, see 4).
3. LAZY INSTALL (the actual non-emission site): association/embedded extend cols NEVER emit SQL themselves (ExtendLowering :177-178 → null). A FkJoin descriptor becomes a physical LEFT JOIN only when a lowering rule walks through it (NavScope.navigate → Relations.install); empty NavScope → source unchanged (Relations :53). Filter path: crossing an association lowers to SqlExpr.Exists with INNER joins INSIDE the exists (FilterLowering :130/:141-189) — the EXISTS wrapper is the to-many guard; projection paths always LEFT JOIN regardless of isToMany.
4. EMBEDDED: JoinResolution.Embedded = same-table sub-column map (className=null, no joins); property access advances the store WITHOUT changing rowAlias — a parent-alias column read, never a join (PropertyAccessLowering :93-99; NavScope rejects Embedded as caller bug).
5. OTHERWISE: JoinResolution.Otherwise(embeddedSubCols, fkFallback) — PER-LEAF dispatch at the hop site (PropertyAccessLowering :77-86, mirrored in GroupByAggregateLowering :237-244): leaf ∈ embeddedSubCols => parent-alias column, no join; else unwrap fallback and fire the FK join. Same association can go both ways in one query. GraphFetch always takes the FK fallback.
6. DEDUP: per-NavScope registry keyed by full path PREFIX (List<String>) — $p.firm.name + $p.firm.city share one alias/join; longer prefixes chain off shorter. Scope caveat: per-relop (and per-filter-leaf) NavScopes, so cross-operator sharing doesn't happen in V1.
7. SCALAR PRUNING: pruneUnusedExtendCols (:1305-1326) = declared scalar aliases ∩ classPropertyAccesses -> non-destructive ExtendNodeCols marker consumed ONLY by ExtendLowering (full-cancel drops the whole extend node INCLUDING traversal hops; partial = per-col skip). Assoc/embedded cols never count toward active (their eager join is ALWAYS redundant given lazy install).
8. FK-SHORTCUT: confirmed ABSENT in V1 as well ($p.firm.id fires the join and reads target-side ID). Genuine advance if we build it.
9. THE 33-REGRESSION LESSON (progress/mr-rewrite-progress.md:63-74): structural pruning of the SHARED AST broke ≥8 other consumers reading extend cols off the AST — fix = non-destructive marker + exactly one emission-owning consumer. Clean-sheet translation: our rewriter RETURNS A NEW TREE consumed only by the lowerer, so structural omission is safe in our architecture — the lesson is about who else reads the tree you mutate, not about pruning per se (V2's in-walk structural pruning worked).

## §A findings — real engine pureToSQLQuery (behavioral spec; all L#### = pureToSQLQuery.pure)

1. JOIN DEDUP BY IDENTITY (the engine's whole leanness story): each projected column / filter operand processes into its OWN select thread carrying joins as named JoinTreeNodes; `mergeSQLQueryData`(L8855)/`merge`(L8732) folds threads, matching children BY `join.name` EQUALITY (L8739) — match => absorb (recurse into the surviving node, re-point aliases via OldAliasToNewAlias); no match => new join node. Two properties through the same association = ONE JOIN, two columns. PORT RULE: deterministic join identity (source node, Join, target) + merge-by-identity.
2. NO FK-SHORTCUT ELISION in the reference engine — confirmed absence ($p.firm.id always joins). `isSimpleJoinToPk`(L1696: single `equal` on the target table's SOLE primary key) exists but is used ONLY to skip re-applying MultiGrainFilter (L1667-1669), never to drop a join. If we build FK-shortcut, it is an optimization BEYOND the reference — gate on isSimpleJoinToPk's condition and compose with merge-by-identity.
3. NAVIGATION JOIN TYPE: LEFT_OUTER everywhere (L1466/1474 columns-through-joins; L5401 filter lambda; L5563 exists). Filters constrained via isolation strategies (isolateSubJoins L7815): MoveFilterInOnClause / BuildCorrelatedSubQuery / MoveFilterOnTop (upgraded to correlated if tree containsInnerJoin, L7862) — NOT by switching join type. Exception: a mapping ~filter declared INNER wraps the class source as an inner-join subselect (getRelationalElementWithInnerJoin L5061/L5086).
4. PM RESULT CONTRACT: simple column PM => alias.COL re-pointed to current node (L1374/1435); dynafunction PM => the function rebuilt inline over recursively-resolved params, params' joins merged+deduped (L1439-1445) — inline, never a subselect; enum PM => CASE(col=src -> 'Name', ..., NULL) in PROJECTION position (pushdown L5205-5216) but toSourceValues comparison (`col = src` / `col IN (...)`) in FILTER position (L5866-5890). [Note: our normalizer's if-chain emission == the CASE form; the filter-position source-value comparison is a lean-SQL refinement to adopt.]
5. EXISTS/isEmpty (to-many nav in filter): two emissions (L5607-5609) — predicate fully local to the navigated table => LEFT JOIN to a DISTINCT subselect on the join keys + IS [NOT] NULL (buildExistsAsJoinWithNullCheck L5636); else correlated `[NOT] EXISTS(SELECT 1 FROM target WHERE join-cond AND inner-filter)` (buildExistsPredicate L5749). isEmpty = negation.
6. getAll: root = mainRelation as RootJoinTreeNode 'root' alias (L5084-5090); mapping ~filter applied DURING getAll (before user ops; INNER => subselect-wrap, else LEFT_OUTER saved-filter L5273-5288); distinct forces all-properties materialization + join-key collection (L5100/5135) and downstream navigations join the DISTINCT SUBSELECT not the table (L1688-1691); PKs added only when not distinct/grouped (L5106). Milestoning: gate off (H-scope exclusion; entry points noted L4986/L5157).
7. OUTPUT SHAPE: filter+project over one class = ONE flat SELECT (threads merged into one tree; L7444-7535). Nesting caused ONLY by: source distinct, group-by-with-agg-filter, to-many isolation in projection (correlated subquery/self-join), INNER mapping filter, correlated exists, navigation into a distinct class. A to-ONE navigation is always a flat JOIN.

## §B findings — real engine optimization catalog (post-processors + construction-time)

CONFIRMED ABSENCES (searched exhaustively — the reference engine does NOT have):
- FK-shortcut ($p.firm.id off the source FK column without joining) — never; every navigation materializes a JoinTreeNode.
- Row-count-preserving unused-LEFT-JOIN cancellation — never; placed joins always render.
- DISTINCT removal via PK/uniqueness — DISTINCT only ever ADDED.
- Post-hoc nested-SELECT collapse — flatness is PROPHYLACTIC (isolation rules decide not to nest), never repaired after.
=> Real join cancellation is an advance BEYOND the reference engine; the prior art is V1's demand analysis (§D pending).

WHAT THE ENGINE DOES HAVE:
1. Post-processor pipeline (defaultPostProcessor.pure:58-67, in order): CTE hoisting; pushFiltersDownToJoins; removeUnionOrJoins (Snowflake-gated bridge-union rewrite for subclass-mapping union+join topology — PK-correctness-based, NOT join deletion); replaceAliasName; prependSQLComments.
2. pushFiltersDownToJoins (pushFiltersDownToJoin.pure:27-63): copy WHERE predicates across join equalities into ON clauses and into subselects. Guards: bail on RIGHT/FULL OUTER anywhere; don't push through windows/limit/multi-predicate selects; aggregated targets go to HAVING. Pushable ops: comparisons/startsWith/endsWith/contains/in with literal operand + null-tests. Predicates are ADDED not moved (transitive over equality — safe).
3. PK reasoning inventory: isSimpleJoinToPk (single equal on target's SOLE PK) => skip MultiGrainFilter only; exists via DISTINCT-PK subselect (L5694); removeUnionOrJoins bridge correctness; resolvePrimaryKey helper (mapping PK -> Table.primaryKey fallback).
4. Isolation gate (manageIsolation L7655-7720): isolate iff shouldIsolate && (savedFilteringOperation nonempty || tree containsInnerJoin). Other nesting triggers: distinct-before-window, non-terminal groupBy w/ empty keys, limit/window/slice not-last, nested filter depth, distinct/grouped class reached via navigation (join target becomes subselect).
5. Cosmetic leanness: SELECT * only as empty-list fallback (no pruning pass anywhere); minimal quoting (only reserved words/spaces/config); deterministic re-aliasing pass `<table>_<i>` preserving root/unionBase/subselect; column-alias trimming for DB limits only.

## §C findings — our lowering layer (established)

- DRIVER SEAM: `Compiler.execute()` (core Compiler.java:107-117) — G output at :111, `new Lowerer().lower(body)` at :113; `StoreResolver.resolve(body)` slots between them. Pipeline docstring already names the slot ("compile-spec (G) -> resolve-mapping (H) -> build-sql (I)", Compiler.java:21). TypedFrom already flows through the Lowerer untouched (Lowerer.java:183).
- FOLD POLICY (Fold.java, 201 lines, stateless predicates): filter AND-merges into WHERE (or HAVING/QUALIFY per slot), project/extend/sort/limit/distinct fold with per-op guards; isolation = uniform `SqlSelect.starOf(Subselect)` (Lowerer.java:1262). `#>{}#->filter->select->sort->limit` is ONE flat SELECT, pinned (LowerRelationTest.wholeChainOneSelect:111-125). Reference substitution: Fold.resolveInto (:138-157) substitutes plain-Column projections, isolates on computed ones.
- JOINS: `SqlSource.Join` binary tree renders FLAT (chains stay flat via asLeftJoinSide/asRightSide bare-scan unwrapping, Lowerer.java:752-772); prefix overload re-aliases right columns; alias policy = deterministic t0..tN (one shared counter). Join dedup/elision/column pruning: CONFIRMED ABSENT by design — §119 (PHASE_HIJ_LOWERING.md:116-124): elision lives in Phase H; "the I-layer fold policy treats every join it receives as required"; user-written joins NEVER elided.
- EXISTS: SqlExpr.Exists IR node exists and is DuckDB-executed in tests (DuckDbValidityTest:140-153) — §133: to-many nav in FILTER position = scalar EXISTS; projection/sort/groupBy position = LEFT JOIN.
- GOLDEN TEST STYLE to follow: `sqlOf(query)` helper + exact-string golden + `count(sql,"SELECT")` flatness pins + real DuckDB `exec()` dual assertion (LowerRelationTest, JoinTortureTest).
- Store-only walls already loud: TypedJoinSlot named wall (Lowerer.java:198-202); TypedGetAll/TypedNavigate hit the frontier default (:207-208) = the 521-error corpus wall.
- Column pruning: greenfield; if built, it's resolver-side (which mapping hops/columns the query consumes), possibly reusing Fold.sourceColumn/claims (:167-200) mechanics for reference discovery.

## USER SCOPE DECISIONS (2026-07-09, confirmed via Q&A)

- FK-SHORTCUT ELISION (revised after design review): seam designed NOW, build DEFERRED past H3. The design surfaced the correctness crux — sound only when the join is a simple equi-join to the target's SOLE PK AND the target has no ~filter/distinct/groupBy (dangling FK to a filtered-out row must yield NULL like the LEFT JOIN, not the raw FK value) — and both references lack it, so there is no gold standard to A/B a novel bug against. JoinDescriptor carries the parsed condition (sourceKeyColumns/targetKeyColumns derivable); the future gate is a ~50-line pinned addition at the descriptor-registration site, built as its own milestone once H3 is corpus-proven.
- ENUM FILTER-POSITION: adopt the real engine's source-value translation in H2/H3 — `$p.status == Status.ACTIVE` => `CODE = 'A'` (toSourceValues; equal/in), CASE stays for projection position.

## FINAL IMPLEMENTATION PLAN

### Architecture (committed)
StoreResolver = pure TypedSpec->TypedSpec rewriter, no sidecar, inserted at the ONE driver seam `Compiler.execute()` between :111 (G output) and :113 (Lowerer). Engine bridge (QueryService -> Compiler.execute) means this one edit wires the corpus. Verified enablers already in the lowerer: correlated lambda scopes (Lowerer :58-65/:482-500), exists-family -> SqlExpr.Exists (:1213-1249), prefix joins folding through Fold.resolveInto, toOne erasure (Scalars :77).

### New files — `com.legend.resolver` (core leaf package)
- `package-info.java` — phase contract: input/output invariants; "no store-only node escapes".
- `StoreResolver.java` — context walk (TypedFrom => active mapping: explicit mapping FQN, else runtime's sole mapping, else loud "add ->from(mapping, runtime)"); op-chain collection over TypedGetAll; materialization.
- `ClassSource.java` — record(mappingFqn, classFqn, setId, pipeline /*minus map terminal*/, rowVar, LinkedHashMap<String,TypedSpec> bindings, Type.RelationType rowType).
- `ClassSources.java` — loader: findMapping -> ClassBinding (walk includes; multi-set-ID loud until H5) -> functionFqn -> SpecCompiler.compile -> split TypedMap terminal (any other terminal = loud ISE, contract violation). Build-time assert: each binding's ExprType conforms to the declared property (H1 guarantees; drift caught loud). Memo per mapping::class; `resolving` LinkedHashSet cycle guard printing the cycle path.
- `Subst.java` — the β-substitution engine; ONE path-extraction funnel shared with DemandScan (they cannot drift). Rules: TypedPropertyAccess on the user lambda var (or let-alias) => bindings.get(prop) with rowVar freshened (_rN, one counter — capture-proof); same-ExprType replacement so no restamping; null binding => MappingResolutionException naming property+class+mapping; bare `$p` misuse => loud; derived props ($p.full()) => loud until H5; shadowing lambdas stop substitution.
- `MappingResolutionException.java` — USER-facing errors; IllegalStateException stays "resolver bug".
- H3: `NavPath.java` (join identity = full path prefix from chain root), `JoinDescriptor.java` (path, targetPipeline, targetRow, condition lambda over physical cols, deterministic prefix = path.last()+"_"+ordinal-on-collision, kind=LEFT), `JoinRegistry.java` (per-op-chain, LinkedHashMap first-demand order), `DemandScan.java` (read-only pre-pass over the chain's user lambdas), `ObjectValue.java` (per-leaf dispatch: Embedded(TypedNewInstance) / Joined(NavPath) / Otherwise(embedded, fallbackNavPath)).

### Modified files
- `core/.../Compiler.java` — execute() seam; share the SpecCompiler instance (hoist local) so G memo is shared.
- `core/.../compiler/element/ModelContext.java` + `PureModelContext.java` — widen façade: findMapping, findRuntime, (H3) findAssociationNav(classFqn, prop) — all delegating to ModelBuilder (surfaces exist at :519/:538/:470).
- H3 G-completion: `legacyNavigate` dedicated checker arm (mirror the H1 slot-join JoinChecker work) + a census fixture for it (census 14->15).

### H2 rewrite algorithm
1. Collect op-chain above TypedGetAll (maximal object-space stack, ending at TypedProject or unsupported op — unsupported = loud naming op + owning phase).
2. Instantiate ClassSource (fresh _rN). H2 pipeline admissibility: tableReference/filter/groupBy/distinct pass through; TypedJoinSlot/legacyNavigate in pipeline ALLOWED to exist — only bindings that DEMAND them are loud "H3-pending" => H2 already gets slot ELISION for main-table-only queries (fixture 8, 0-JOIN pin).
3. User filter: Subst(pred); emit TypedFilter(pipeline, λ_rN.pred', info = pipeline relation type) — re-schema object->relation; mapping ~filter already below (ordering free).
4. User project: Subst each FuncCol; emit TypedProject(pipeline, cols', info UNCHANGED) — downstream needs zero rewriting. Exits object space.
5. Object-space limit/take/slice/drop: binding-free pass-through arms — stack on the pipeline like filter, no substitution (plangen has class-space take at :477).
6. Bare getAll at root => loud until H4 (graph output).

### THE MAP-TERMINAL INVARIANT (named contract, goes in package-info + pins)
The mapping body's TypedNewInstance survives resolution ONLY beneath a serialization boundary (TypedSerialize / graph root). Every relation-returning consumer (project/select/filter-chains/groupBy/sortBy/user map with scalar body) consumes the terminal as the BINDING TABLE — the resolver never materializes an object (no JSON functions, no struct packing) that a downstream op would immediately flatten. User `->map(p|$p.name)` over instances = single-column projection through bindings. Negative pin: `count(sql,"json") == 0` on every relation-shaped fixture.

Golden result (fixture 3): `SELECT t0.NAME AS name FROM T AS t0 WHERE t0.ACTIVE = 1 AND t0.AGE > 30` — 1 SELECT, existing fold policy, zero new lowering code.

### H3 join cancellation (the V1-lineage design, no-sidecar)
- DEMAND: DemandScan over the op-chain's lambdas (we hold the whole tree — no type-checker side channel; V1's touched-class feed subsumed by lazy ClassSources.get). SCAN-THEN-REWRITE: registry completes, final row type computed, THEN materialize + Subst with correct infos — no restamp pass (V1's restamp bug family designed out). Rewrite-time registry miss = loud ISE "undemanded navigation" — never silent SQL.
- DESCRIPTOR != EMISSION: descriptors registered by demand; emission once at materialization — fold descriptors in first-demand order into TypedJoin(acc, targetPipeline, LEFT, cond, prefix). Un-demanded assoc ends / joinslots never become descriptors (structural omission of a fresh tree = safe; the 33-regression lesson is about mutating SHARED trees).
- DEDUP: whole-op-chain registry keyed by NavPath (beats V1's per-relop scope — filter-side and project-side $p.employer share ONE join; = real engine's merge-by-identity outcome without a merge pass). Longer prefixes chain off shorter. Nested EXISTS gets a child ObjectRelation (local joins — correct).
- UNIFORM LIFTING SET (explicit): {filter, project, sortBy/sort, groupBy/aggregate} — ALL object-space lambdas run through the one Subst/DemandScan funnel. Plangen evidence: class-source sortBy compiles its key through the mapping "same pattern as generateFilter" (:1291-1360); class-source groupBy takes ASSOCIATION-PATH KEYS with chain-prefix LEFT-JOIN dedup (:1615-1700 — an independent reinvention of our NavPath registry, validating the identity scheme). Sort/groupBy arms land in M-H3b/M-H3c; treating them as relation-space-only was the draft's error and exactly where V2's asymmetry would have re-bitten.
- JOIN AGGRESSIVENESS BOUNDARY (sharpened §119 wording for package-info): OBJECT-SPACE (mapping-implied) joins are access paths, not row-set definitions — demand-gating is semantically free and the demand model is already maximally aggressive (a join emits iff a consumed expression reads through it; no further row-count-preserving cancellation exists). EXEMPT even in object space (row-set-defining, dropping = wrong answers): joins feeding a JoinMediated mapping ~filter and joins feeding mapping ~groupBy keys/aggs — already demanded via the pipeline. RELATION-SPACE (user-written) joins: never touched. FK-shortcut is the only aggression beyond this and it can silently change answers — hence the deferred-build decision.
- THREE NAVIGATION SOURCES, ONE LIFTING (uniform across all lambda relops by construction — single funnel):
  1. Association nav: findAssociationNav -> AssociationBinding predicate fn -> extract legacyAssocPredicate condition (loud on shape surprise); target = ClassSources.get(target) instantiated (its ~filter rides along; its joinslots demand recursively).
  2. TypedJoinSlot: demanded iff slot alias occurs in a consumed binding or mapping ~filter; conversion slot->prefix TypedJoin; $row.alias.COL -> $row.alias_COL; demanded slot pulls transitive predecessors. legacyNavigate: target joined as its PIPELINE (V1 shallowResolution answer); deeper hops resolve only when the demanded path continues.
  3. Embedded/otherwise per-leaf dispatch at each access site: TypedNewInstance-valued binding = Embedded => inner-binding substitution, parent-alias read, NO join, ungated; otherwise => leaf ∈ embedded sub-bindings ? embedded arm : register fallback descriptor. Same association both ways in one query (pinned).
- EXISTS (to-many in filter): SINGLE form — correlated exists/isEmpty natives: exists(TypedFilter(targetPipeline, λ.assocCond), λ.pred') -> SqlExpr.Exists via existing lowering. Engine's LEFT-JOIN-to-DISTINCT+null-check second form REJECTED (serves weak-correlation DBs; DuckDB decorrelates; costs a SELECT+DISTINCT) — noted in package-info as a dialect strategy seam.
- JOIN TYPES: LEFT for every navigation join (§A.3; NULL comparisons filter naturally). INNER only where the engine's INNER-mapping-filter rule forces it (pipeline filter; filtered target wraps via asRightSide — pinned 2-SELECT).
- POSITIONAL RULE TABLE (from the FULL plangen read §F1 — pinned in package-info):
  * projection / sortBy-key / groupBy-key navigation => LEFT JOIN through the ONE whole-chain NavPath registry. DELIBERATE BEATS-PLANGEN: plangen used an UNSHARED correlated scalar subquery for sort keys (sort_j*) — we commit to LEFT JOIN so sortBy($p.employer.legal) + project of the same path = ONE join (pinned fixture).
  * filter-position to-many navigation AND class-typed isEmpty/isNotEmpty (any multiplicity, incl. to-one-optional) => correlated [NOT] EXISTS via nested ObjectRelation.
  * SCALAR-property isEmpty => IS NULL (existing scalar lowering) — plangen's exists-wrapping of null-tests was semantically dubious (§F10).
  * Multi-hop association MAPPINGS (@J1 > @J2) => one JoinDescriptor PER HOP, chained by NavPath prefix (V1's single-condition FkJoin + generator-side chain rendering was split-brain — §E9).
- CYCLE POLICY (sharpened, §E8): association target already on the resolving stack => SHALLOW instantiation (main pipeline + scalar bindings; joins load on continued demand — V1's shallowResolution, kept); any OTHER cycle => throw with the cycle path printed. NEVER null-and-skip (V1's silent-leak family).
- OTHERWISE canonicalization (§E7): the normalizer may emit embedded-then-FK or FK-then-embedded; ClassSource extraction CANONICALIZES the order so H3c has exactly one recognizer.
- TypedUserCall stance (§F11): H2 = structural recursion over args; a getAll inside a callee body dies LOUD at the lowerer wall (corpus buckets it). Resolver-side β-inlining of user calls (same Subst machinery, callee body from specs.compile, fresh vars) = its own later milestone, corpus-gated. No separate pre-pass inliner unless scale demands (V1's design noted as fallback).
- LOUD CLASS-KEYED LOOKUPS (§F3): plangen's findJoinResolution global fallback (search EVERY store by property name) is a confirmed silent-leak anti-pattern — cross-class name collisions resolved arbitrarily. Ours: always keyed by receiver class; negative fixture with same-named association properties on two classes.

### Lean SQL placement (the wall stays)
- Resolver-side: join demand/elision/dedup (THE mechanism); PM inlining (falls out); mapping-filter placement (falls out); ENUM toSourceValues in filter position (M-H3d: conservative recognizer over the normalizer's if-chain shape -> col = 'A' / IN(...); unrecognized shape falls back to if-chain; projection keeps if-chain=CASE).
- NO standalone column-pruning pass: project IS the pruner for TDS; H4 graph output prunes by construction (demand==emission). Mirrors §B (engine has none).
- Lowerer-side unchanged: fold policy, AND-merge, t0..tN aliases, toOne erasure. No mapping/PK knowledge crosses into com.legend.lowering/com.legend.sql.
- FK-SHORTCUT: seam designed NOW — JoinDescriptor carries the parsed condition, sourceKeyColumns()/targetKeyColumns() derivable; gate = simple-equi-join to target's SOLE PK AND target has NO ~filter/distinct/groupBy AND leaf ∈ target PK columns (the ~filter guard is the correctness crux: dangling FK to a filtered-out row must yield NULL, not the raw FK). BUILD DEFERRED past H3 (user-confirmed): lands as its own post-H3 milestone behind its own golden suite, once the corpus proves the join machinery.

### Test strategy
Style: LowerRelationTest's — sqlOf() + exact goldens + count(sql,"SELECT")/count(sql,"LEFT JOIN") pins + DuckDB exec() on EVERY test.
New: ClassSourceTest (extraction over the census battery: all 14 extract or fail loud-by-design), StoreResolverTest (typed-tree unit: re-schema, fresh-var hygiene, loud messages pinned exactly), ResolveSimpleClassTest (H2 goldens), ResolveNavigationTest (H3: JOIN counts, ABSENCE pins when un-navigated, EXISTS shapes, LEFT-vs-INNER data checks — NULL-firm row).
Fixture matrix (19 pins): H2 = project-only / filter+project / mapping-~filter ordering / dynafunction PM both positions / enum PM projected / filter->project->sort->limit 1-SELECT / unmapped-property message golden / join-carrying mapping main-table-only 0-JOIN / distinct+groupBy pass-through. H3 = slot consumed 1-JOIN / two props one assoc 1-JOIN / filter+project same nav 1-JOIN (beats-V1 pin) / multi-hop 2 chained JOINs flat / self-association distinct aliases / exists+isEmpty correlated EXISTS / embedded 0-JOIN / otherwise both-ways / declared-not-navigated 0-JOIN / enum filter toSourceValues.
Census 14/14 (->15 with legacyNavigate fixture) standing invariant; corpus A/B per milestone with delta write-ups; regression = stop-the-line.

### H4 skeleton (designed now — ported from the engine module's graph stack, verified in code)
- Two-mode result contract: SNAPSHOT (DB aggregates: json_group_array(json_object(...)) one row) vs STREAMING (one json_object per row, executor byte-passthrough) — a Mode flag on the execute path, non-graph plans identical in both.
- graphFetch is SOURCE-PRESERVING at SQL layer (engine's GraphFetchLowering = 25 lines: lower(source)); the envelope lives in the SERIALIZE lowering rule. Port verbatim: TypedGraphFetch lowers to its source; TypedSerialize owns the envelope.
- The envelope consumes a resolver-built TypedProject of the fetch tree's leaves — H4 column pruning is BY CONSTRUCTION (no pruning pass, confirming §3.5).
- Nested tree hops = correlated scalar subqueries reusing the SAME JoinDescriptor extraction as H3 (different consumer); to-many children wrapped JsonArrayAgg. Wall: resolver supplies flattened source + per-child correlated relations; lowerer supplies SqlExpr.JsonObject/JsonArrayAgg IR + rendering.
- Bare class root => implicit serialize with a synthesized leaf-only tree (one entry per MAPPED property), output column named `result`.
- New files: resolver/FetchTree.java; SqlExpr.JsonObject/JsonArrayAgg + serialize rule in the lowerer; port source = engine JsonEnvelope.java (201 lines) + SerializeRelLowering/GraphFetchLowering.

### Fixture additions (beyond the 19)
20. `sortBy($p.employer.legal)` + projection of the same path — 1 LEFT JOIN shared across sort+project (the beats-plangen pin).
21. Class-source groupBy with an association-path key — GROUP BY on the joined column; 1 SELECT, 1 JOIN.
22. json-absence negative pins on every relation-shaped fixture (`count(sql,"json") == 0`).
23. Multiple distinct nav paths inside ONE computed projection expression (`$o.customer.name + $o.product.name`) + the same path twice in one expression (§F2).
24. Two classes with SAME-NAMED association properties in one query — resolves per receiver class (§F3 negative).
25. Two-hop association mapping (@J1 > @J2) — one descriptor/join per hop, chained (§E9).
26. To-one-OPTIONAL navigation isEmpty => NOT EXISTS; scalar-property isEmpty => IS NULL (§F10 pair).

### H5 design sketches (recorded now so they're designed-in, not retrofitted)
- M2M = recursive substitution: the M2M pipeline is getAll(Upstream)->...->map(^Target(...)); resolving the inner getAll and substituting composes the two binding tables automatically — bare-passthrough bindings compose by β-transitivity, zero special-casing (V1 needed structural pattern-detection + Both-contribution forwarding, §E5).
- Instance-literal sources (^Class(...) collections): synthesized identity ClassSource over a VALUES/TypedTds-shaped relation; class-typed [*] props = LATERAL UNNEST family (§E4). Loud until built.
- JSON/variant-source classes: just different binding expressions (get($row.data,'p') + to(...)) — substitution doesn't care; remaining work is Phase-I variant-native lowering parity, not resolver work (§F12).
- H4 refinements: implicit-serialize leaf tree enumerates SCALAR-typed bindings only (class-typed bindings excluded from the bare-root envelope, §E10); root-store threading looks through statement lists/lets to the last expression (class-typed `if` roots loud until a corpus pin, §E3); scalar-many roots UNNEST at the Executor/ResultShape layer (§F13).

### FINAL AUDIT CATCHES (design agent's plan-stack audit — all folded into milestones below)

1. BLOCKER — driver runtime threading: corpus queries carry NO ->from(...) (qs.execute(model, query, "test::RT", conn)); QueryService's core bridge DROPS runtimeName (QueryService.java:82). Fix (sanctioned by PHASE_K_EXECUTION.md §4): M-H2c adds the 4-arg Compiler.execute(model, query, runtimeName, Connection) overload + QueryService passes runtimeName through. Context precedence: TypedFrom.mapping > TypedFrom.runtime->mappings > DRIVER-SUPPLIED runtime->mappings > loud "class query requires an execution context". Runtime with ≠1 mappings: the one that binds the fetched class wins; zero/ambiguous => loud. New fixture: no-from query with driver runtime (the corpus shape).
2. TypedSortBy relation lowering does not exist (27-error scoreboard line; TypedSort is column-keyed). M-H3b includes it as a small Phase-I item (ORDER BY over the lowered key expr; Fold.sortFolds decides fold/isolate) — object-space sortBy rewrites into it, and the 27-line burns.
3. Scalar TypedEnumValue lowering missing (18-error line): one Scalars arm (enum value => name string literal, plangen :2591 parity) at the head of M-H3d.
4. Milestoned TypedGetAll (non-empty milestoning): loud MappingResolutionException — never silently dropped (silent drop = wrong rows).
5. H2 slot handling sharpened: normalizer pipeline order is source -> join* -> legacyNavigate* -> filter -> groupBy -> distinct -> map, so mapping ~filters sit ABOVE slots. Rule: any consumed binding OR the mapping ~filter references a slot alias => loud "H3-pending" (whole-mapping granularity); else strip slots AND rebuild the infos of nodes above (the same rebuild helper H3 materialization needs — build once in H2).
6. Multi-hop associations are per-end legacyNavigate injections (Option A), NOT chained predicates — legacyAssocPredicate is single-hop with tables read FROM the call (the H1 emission we built). §E9's fixture becomes a per-end-navigate fixture; legacyNavigate G-typing is squarely on M-H3b's critical path (checker + census fixture first, census -> 15/15).
7. Corpus protocol explicit per milestone: core suite -> engine corpus -> scoreboard --record -> delta review vs previous row -> code+scoreboard co-commit. Burn-down targets (diagnosed if missed, never rationalized): M-H2c first large cut of the 520 TypedGetAll line; M-H3a/b association families + TypedSortBy ~0; M-H3c/d exists families + TypedEnumValue ~0; M-H4 TypedSerialize (69) burns.

Contradiction sweep: clean (to-one filter nav = LEFT JOIN hoist vs §133's EXISTS = to-many only; project info stability vs re-schema'd filters consistent; whole-chain dedup vs EXISTS-local registries intended; §119 boundary stated once in package-info). Remaining unknowns all converted to verify-first probes at milestone heads (prefix-join fold pins, correlated-exists pins, legacyNavigate checker shape, assoc-predicate extraction arm) — nothing blocks starting.

### Milestones (each: loop rule — fix -> suite -> corpus A/B -> present -> commit)
- M-H2a ClassSource extraction (+ milestoning-loud rule; slot-strip info-rebuild helper). Exit: extraction green over census battery; corpus unchanged (scoreboard still runs — pins "no accidental coupling").
- M-H2b Rewriter simple queries + limit/take pass-through + sharpened slot admissibility + json-absence pins (goldens pinned BEFORE wiring via direct resolver+Lowerer composition in tests). Exit: fixtures 1-9, 22 + execution; corpus unchanged.
- M-H2c Driver wiring + 4-arg Compiler.execute + QueryService runtimeName threading (the audit BLOCKER fix) + no-from fixture. Exit: the 520 TypedGetAll line takes its first large cut, zero regressions; measured write-up.
- M-H3a Joinslot conversion + elision (FIRST pin the lowerer on hand-built resolver-shaped prefix-join trees). Exit: fixtures 8/10/13-slot; absence pins; A2-family corpus.
- M-H3b Association nav to-one (legacyNavigate checker FIRST, census 15/15; TypedSortBy lowering; embedded dispatch; object-space sortBy arm; per-end-navigate multi-hop fixture). Exit: fixtures 11/12/14/16/18/20/23/24/25; TypedSortBy ~0; AssociationIntegrationTest family.
- M-H3c To-many + otherwise (correlated-exists lowerer pin FIRST; per-leaf dispatch canonicalized at extraction; object-space groupBy/aggregate arm; isEmpty three-way rule). Exit: fixtures 15/17/21/26; §133 pinned.
- M-H4a Implicit serialize + snapshot envelope (bare roots, leaf-only scalar-binding trees) — ordered before M-H3d if the serialize corpus family (~69 errors) outweighs the enum peephole on the A/B numbers. Exit: serialize family converting; envelope goldens DuckDB-executed.
- M-H4b Nested tree hops (to-one object, to-many array) + streaming mode.
- M-H3d Lean peepholes (scalar TypedEnumValue arm FIRST — burns its 18-error line; enum toSourceValues; flatness audit of all goldens vs §A.7 — every intentional nesting documented in test names). Exit: fixture 19; TypedEnumValue ~0; no golden fatter.
- (Post-H3, corpus-proven: FK-shortcut as its own milestone behind its own golden suite — per user decision. TypedUserCall inlining milestone corpus-gated.)

### FIRST UNIT OF WORK (M-H2a, first commit — fully specified)
0. Persist this plan into the repo as `docs/PHASE_H2_H3_RESOLVER_PLAN.md` (committed with M-H2a) so it survives sessions and PHASE_H_PLAN.md links to it.
1. `ModelContext.java` + `PureModelContext.java`: add findMapping/findRuntime (delegate to ModelBuilder :519/:538).
2. New `core/src/main/java/com/legend/resolver/`: package-info.java (contracts: positional rule table, never-elide boundary, map-terminal invariant), MappingResolutionException.java, ClassSource.java, ClassSources.java (compile -> split TypedMap terminal -> binding table; otherwise-order canonicalization; milestoning-loud; memo + cycle policy; conformance assert).
3. New `core/src/test/java/com/legend/resolver/ClassSourceTest.java` over the PhaseHCensusTest fixture battery (all 14 extract or fail loud-by-design).
4. Loop rule: core suite -> engine corpus -> scoreboard --record (corpus unchanged IS the assertion) -> present -> commit code + scoreboard row together.

Traceability: all V1 full-read learnings (E1-E10) and all plangen full-read learnings (F1-F13) verified incorporated by the design agent's item-by-item check; the seven audit catches folded into the milestone table above.

### Top-5 risks -> mitigations
1. β-subst breaks ExprType invariants -> same-type replacement asserted at build; fresh-var counter; scan-before-materialize (no restamp exists to get wrong); test-only integrity walk asserting parent/child info consistency.
2. Multiplicity mismatch after inlining -> H1's explicit toOne + subsumption = conformance by construction; build-time assert; NULL-row execution fixtures catch value-level errors.
3. Demand misses an access path -> DemandScan and Subst share ONE extractor (cannot drift); registry miss = loud; let-alias + nested-lambda fixtures; corpus backstop.
4. Join identity collision / nondeterminism -> NavPath identity (never target class); ordinal prefixes; LinkedHashMaps everywhere; exact-string goldens fail on any nondeterminism.
5. Resolver shapes defeat the fold policy -> M-H3a pins the lowerer on hand-built resolver-shaped trees FIRST; intentional nestings are named pins; fixes land as mapping-agnostic Fold rules (wall stands).
