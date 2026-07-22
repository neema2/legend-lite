package com.legend.resolver;

import com.legend.builtin.Pure;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedAggCol;
import com.legend.compiler.spec.typed.TypedAggregate;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedConcatenate;
import com.legend.compiler.spec.typed.TypedDistinct;
import com.legend.compiler.spec.typed.TypedDrop;
import com.legend.compiler.spec.typed.TypedEnumValue;
import com.legend.compiler.spec.typed.TypedExtend;
import com.legend.compiler.spec.typed.TypedExtendAgg;
import com.legend.compiler.spec.typed.TypedExtendWindow;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedGraphFetch;
import com.legend.compiler.spec.typed.TypedGraphTree;
import com.legend.compiler.spec.typed.TypedGroupBy;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedJoin;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedMilestonedAccess;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNavigate;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedRename;
import com.legend.compiler.spec.typed.TypedSelect;
import com.legend.compiler.spec.typed.TypedSerialize;
import com.legend.compiler.spec.typed.TypedSerializeGraph;
import com.legend.compiler.spec.typed.TypedSlice;
import com.legend.compiler.spec.typed.TypedSort;
import com.legend.compiler.spec.typed.TypedSortBy;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.MappingResolutionException;
import com.legend.error.NotImplementedException;
import com.legend.model.RuntimeDefinition;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
/**
 * Phase H &mdash; the pure {@code TypedSpec -> TypedSpec} rewriter replacing
 * object-space class queries with relation pipelines resolved against the
 * active mapping (contract: {@link com.legend.resolver} package doc; design:
 * {@code docs/PHASE_H2_H3_RESOLVER_PLAN.md}).
 *
 * <p>H2 scope: {@code getAll -> [filter|limit|take|slice|drop]* -> project}
 * chains over a single class. The projection boundary exits object space
 * with its {@code info} UNCHANGED, so everything downstream (relation
 * space) passes through untouched. Unsupported object-space constructs are
 * loud, naming the construct and the owning phase.
 */
public final class StoreResolver {

    private final ClassSources sources;
    private final SpecCompiler specs;
    private int freshVarCounter;
    /** Synthetic head registry (filter-lifted '#f' + date-split '#d'
     * identities) — append-only across nested resolutions. */
    private final SyntheticHeads synthetics = new SyntheticHeads();
    /** Recursive navigate-target materialization (stateless service). */
    private final NavMaterializer navMaterializer;
    /** Association-route join material (stateless service). */
    private final AssociationJoins assocMaterial;
    private final CorrelatedSubselects corrSubs;
    /** THE per-resolution temporal frame (root context + chain specs +
     * stamping machinery) — set at op-chain collection, specs attached
     * after the demand scan; nested sibling resolutions overwrite at
     * their own entry (audit 10 semantics). */
    private TemporalFrame temporal;

    private final ModelContext ctx;

    public StoreResolver(ModelContext ctx, SpecCompiler specs) {
        this.ctx = Objects.requireNonNull(ctx, "ctx");
        this.specs = Objects.requireNonNull(specs, "specs");
        this.sources = new ClassSources(ctx, specs);
        // an EMPTY frame until the op-chain phase constructs the real one —
        // pre-resolution consumers (lift walkers, resolveNode shape checks)
        // see NO context, exactly the old fields' initial values
        this.temporal = new TemporalFrame(ctx, sources, TemporalContext.NONE,
                Map.of());
        this.assocMaterial = new AssociationJoins(ctx, sources, specs,
                synthetics);
        this.corrSubs = new CorrelatedSubselects(sources, assocMaterial);
        this.navMaterializer = new NavMaterializer(sources, assocMaterial,
                synthetics, corrSubs);
    }

    /** Resolve every statement of a query body (lets + final expression). */
    public List<TypedSpec> resolve(List<TypedSpec> body) {
        return resolve(body, null);
    }

    /**
     * Resolve with a DRIVER-SUPPLIED execution context (the corpus shape:
     * queries carry no {@code ->from(...)}; the runtime arrives via the
     * service API). Precedence per the plan: an explicit {@code from()} in
     * the query always wins; the driver runtime is the outermost fallback.
     */
    public List<TypedSpec> resolve(List<TypedSpec> body, String driverRuntimeFqn) {
        return resolve(body, driverRuntimeFqn, null);
    }

    /** {@code explicitMappingFqn}: resolve class fetches against THIS
     * mapping (the ~func-pipeline recursion — ClassSources) — an explicit
     * from() in the body still wins. */
    public List<TypedSpec> resolve(List<TypedSpec> body, String driverRuntimeFqn,
            String explicitMappingFqn) {
        // LAZY: the runtime is consulted only when a class fetch needs a
        // mapping — a pure relation query with an unusable runtime must not
        // fail (the corpus's date-literal regression).
        Context context = explicitMappingFqn != null
                ? new Context(explicitMappingFqn, null)
                : driverRuntimeFqn == null ? Context.NONE
                : Context.ofRuntime(driverRuntimeFqn);
        List<TypedSpec> out = new ArrayList<>(body.size());
        for (TypedSpec stmt : body) {
            out.add(resolveNode(stmt, context));
        }
        for (TypedSpec stmt : out) {
            assertNoStoreOnlyEscapees(stmt);
        }
        return out;
    }

    /**
     * POST-CONDITION (core/README rule 9): no {@code TypedGetAll} or
     * {@code TypedUserCall} survives store resolution. An escapee here is
     * a RESOLVER-phase gap named as such — before this walk they died at
     * the lowerer's generic wall with the wrong phase attribution.
     */
    static void assertNoStoreOnlyEscapees(TypedSpec n) {
        if (n instanceof TypedGetAll ga) {
            throw new com.legend.error.NotImplementedException(
                    "store resolution left getAll(" + ga.classFqn()
                    + ") unresolved — the query shape around it is not"
                    + " supported by the resolver yet");
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedUserCall uc) {
            throw new com.legend.error.NotImplementedException(
                    "store resolution left user call '" + uc.callee().qualifiedName()
                    + "' uninlined — the call shape is not supported by the"
                    + " resolver yet");
        }
        for (TypedSpec c : n.children()) {
            assertNoStoreOnlyEscapees(c);
        }
    }

    /**
     * The execution context: an explicit mapping, or a runtime whose
     * candidate mappings are dispatched PER FETCHED CLASS — the candidate
     * that binds the class wins; zero or several binders is loud (plan
     * audit catch 1's precedence rule).
     */
    record Context(String explicitMapping, String runtimeFqn) {
        static final Context NONE = new Context(null, null);
        static Context ofMapping(String fqn) { return new Context(fqn, null); }
        static Context ofRuntime(String fqn) { return new Context(null, fqn); }
        boolean isNone() { return explicitMapping == null && runtimeFqn == null; }
    }

    // =====================================================================
    // The context walk
    // =====================================================================

    private TypedSpec resolveNode(TypedSpec n, Context context) {
        return switch (n) {
            case TypedFrom from -> {
                Context inner = from.mapping().map(m -> Context.ofMapping(m.fullPath()))
                        .orElseGet(() -> from.runtime()
                                .map(r -> Context.ofRuntime(r.fullPath()))
                                .orElse(context));
                yield new TypedFrom(resolveNode(from.source(), inner),
                        from.mapping(), from.runtime(), from.info());
            }
            // Bare class fetch: GRAPH output — implicit serialize with a
            // leaf-only tree over the class's SCALAR bindings (plan §E10).
            case TypedGetAll g -> resolveChain(g, context);
            case TypedFilter f when isObjectSpace(f.source()) ->
                    resolveChain(f, context);
            case TypedProject p when isObjectSpace(p.source()) ->
                    resolveChain(p, context);
            // a CLASS-TERMINAL hop chain at the ROOT (bare
            // Firm.all().employees): object space — the flatten composes in
            // collectOpChain and the envelope roots at the target.
            case TypedPropertyAccess pa
                    when pa.info().type() instanceof Type.ClassType
                    && isObjectSpace(pa) ->
                    resolveChain(pa, context);
            // ->map(f|$f.assocEnd->filter(...)) — a CLASS-RESULT mapper:
            // the auto-map flatten IS the mapper body with the source
            // spliced for the param (flatten composition is associative);
            // the resulting hop chain re-enters resolution.
            case TypedMap m
                    when isObjectSpace(m.source())
                    && ((Type.FunctionType) m.mapper().info().type()).result()
                            .type() instanceof Type.ClassType ->
                    resolveNode(substituteParam(m.mapper(), m.source()), context);
            // a BARE object-space chain HEADED by toOne/first/at/distinct
            // (the eager run of a class-typed let: filter(...)->toOne()):
            // the chain resolver owns these in-pipeline (toOne = the
            // documented pass-through stand-in; at(k) = slice) — routing
            // here keeps the eager run off the envelope-in-scalar wall.
            case TypedNativeCall nc when isObjectSpace(nc) ->
                    resolveChain(nc, context);
            // project DISTRIBUTES over a class-collection concatenate
            // (UNION ALL semantics): each side resolves as its own
            // object-space chain, sharing the projection columns.
            case TypedProject p when classConcatOf(p.source()) != null -> {
                TypedNativeCall c = classConcatOf(p.source());
                yield new TypedConcatenate(
                        resolveNode(new TypedProject(c.args().get(0), p.columns(),
                                p.info()), context),
                        resolveNode(new TypedProject(c.args().get(1), p.columns(),
                                p.info()), context),
                        p.info());
            }
            // if() over class queries: the condition must be STATICALLY
            // decidable (literal, or equal/eq over literals) — the chosen
            // branch's thunk body resolves; a truly runtime condition over
            // graph output has no SQL shape yet.
            case TypedIf i when containsGetAll(i) -> {
                Boolean cond = staticBool(i.condition());
                if (cond == null) {
                    throw new NotImplementedException("class query under if()"
                            + " with a runtime condition is not resolvable yet");
                }
                TypedSpec branch = cond ? i.thenBranch()
                        : i.elseBranch().orElseThrow(() -> new NotImplementedException(
                                "class query under if() without an else branch"));
                yield resolveNode(unthunk(branch), context);
            }
            // size()/count() over a class extent = the ROW COUNT of the
            // resolved pipeline: project ONE constant column (no slot
            // demand — engine emits select count(*)) and count the relation.
            case TypedNativeCall nc
                    when nc.args().size() == 1 && isObjectSpace(nc.args().get(0))
                    && (nc.callee().qualifiedName().equals(
                                    "meta::pure::functions::collection::size")
                            || nc.callee().qualifiedName().equals(
                                    "meta::pure::functions::collection::count")) ->
                    classExtentCount(nc, context);
            // ->map(p|$p.scalarExpr) over instances IS the single-column
            // projection (the map-terminal invariant); Person.all().prop is
            // its property-access spelling (to-many paths explode via the
            // projection funnel's positional rules).
            case TypedMap m
                    when isObjectSpace(m.source())
                    && !(((Type.FunctionType) m.mapper().info().type()).result().type()
                            instanceof Type.ClassType) -> {
                TypedMap m2 = synthetics.liftValueMapFilter(m);
                yield resolveChain(scalarMapAsProject(m2.source(), m2.mapper(),
                        m2.info().multiplicity()), context);
            }
            case TypedFilter f
                    when containsGetAll(f.source())
                    && !(f.source().info().type() instanceof Type.ClassType)
                    && !(f.source().info().type() instanceof Type.RelationType)
                    && f.source() instanceof TypedPropertyAccess ->
                    foldScalarHopFilter(f, context);
            case TypedPropertyAccess pa when isObjectSpace(pa.source())
                    && !(pa.info().type() instanceof Type.ClassType) ->
                    scalarReadAsProject(pa, context);
            case TypedLimit l when isObjectSpace(l.source()) ->
                    resolveChain(l, context);
            case TypedDrop d when isObjectSpace(d.source()) ->
                    resolveChain(d, context);
            case TypedSlice s when isObjectSpace(s.source()) ->
                    resolveChain(s, context);
            case TypedSortBy sb when isObjectSpace(sb.source()) ->
                    resolveChain(sb, context);
            // Class-source groupBy (tds::groupBy cl:C[*] overload; the legacy
            // 4-arg form desugars into it): a relation-shaping TERMINAL like
            // project — key/map lambdas read the object and substitute
            // through the one funnel (plan: uniform lifting set). aggregate
            // is Relation-only in real pure — no class-source arm exists.
            case TypedGroupBy g when isObjectSpace(g.source()) ->
                    resolveChain(g, context);
            // serialize / graphFetch->serialize: the GRAPH terminal. The
            // graphFetch wrapper is SOURCE-PRESERVING (engine parity) —
            // serialize's tree governs the envelope.
            case TypedSerialize sz when containsGetAll(sz.source()) ->
                    resolveChain(sz, context);
            // Relation-space wrappers over a chain that bottoms at a getAll:
            // rebuild with the resolved source. (Each wrapper keeps its own
            // info — relation-space types are stable across resolution.)
            // the Typer's `.rows` MARKER (identity over a relation value):
            // it exists so the K-side result frame can tell row-index reads
            // from Result-envelope reads; EVERY lowering path passes through
            // this resolver, so erasure here reaches all of them (audit 20c
            // H1 — the K-hook-only erasure leaked on the plain compile path)
            case TypedPropertyAccess pa
                    when pa.property().equals(com.legend.compiler
                            .element.type.PlatformTypes.ROWS_MARKER)
                    && pa.source().info().type() instanceof Type.RelationType ->
                    resolveNode(pa.source(), context);
            // a COLUMN READ over a relation-shaped chain ($tds.rows.id —
            // the TDS getter desugar): rebuild over the resolved source
            case TypedPropertyAccess pa
                    when containsGetAll(pa.source())
                    && pa.source().info().type()
                            instanceof Type
                                    .RelationType ->
                    new TypedPropertyAccess(
                            resolveNode(pa.source(), context), pa.property(),
                            pa.info());
            case TypedFilter f when containsGetAll(f.source()) -> new TypedFilter(
                    resolveNode(f.source(), context), f.predicate(), f.info());
            case TypedProject p when containsGetAll(p.source()) -> new TypedProject(
                    resolveNode(p.source(), context), p.columns(), p.info());
            case TypedSort s when containsGetAll(s.source()) -> new TypedSort(
                    resolveNode(s.source(), context), s.keys(), s.info());
            case TypedCast c
                    when containsGetAll(c.source())
                    && c.info().type() instanceof Type
                            .RelationType ->
                    new TypedCast(
                            resolveNode(c.source(), context), c.target(), c.info());
            case TypedSortBy sb when containsGetAll(sb.source()) -> new TypedSortBy(
                    resolveNode(sb.source(), context), sb.key(), sb.ascending(), sb.info());
            case TypedLimit l when containsGetAll(l.source()) -> new TypedLimit(
                    resolveNode(l.source(), context), l.count(), l.info());
            case TypedDrop d when containsGetAll(d.source()) -> new TypedDrop(
                    resolveNode(d.source(), context), d.count(), d.info());
            case TypedSlice s when containsGetAll(s.source()) -> new TypedSlice(
                    resolveNode(s.source(), context), s.start(), s.stop(), s.info());
            case TypedDistinct d when containsGetAll(d.source()) -> new TypedDistinct(
                    resolveNode(d.source(), context), d.columns(), d.info());
            case TypedGroupBy g when containsGetAll(g.source()) -> new TypedGroupBy(
                    resolveNode(g.source(), context), g.keys(), g.aggs(), g.info());
            case TypedAggregate a when containsGetAll(a.source()) -> new TypedAggregate(
                    resolveNode(a.source(), context), a.aggs(), a.info());
            case TypedExtend e when containsGetAll(e.source()) -> new TypedExtend(
                    resolveNode(e.source(), context), e.columns(), e.info());
            case TypedExtendWindow w when containsGetAll(w.source()) ->
                    new TypedExtendWindow(resolveNode(w.source(), context),
                            w.window(), w.columns(), w.aggs(), w.info());
            case TypedExtendAgg e when containsGetAll(e.source()) ->
                    new TypedExtendAgg(resolveNode(e.source(), context),
                            e.aggs(), e.info());
            case TypedRename r when containsGetAll(r.source()) -> new TypedRename(
                    resolveNode(r.source(), context), r.renames(), r.info());
            case TypedSelect s when containsGetAll(s.source()) -> new TypedSelect(
                    resolveNode(s.source(), context), s.columns(), s.info());
            case TypedConcatenate c when containsGetAll(c) -> new TypedConcatenate(
                    resolveNode(c.left(), context), resolveNode(c.right(), context),
                    c.info());
            case TypedNavigate nav
                    when containsGetAll(nav.source())
                    && nav.target().info().type()
                            instanceof Type.RelationType ->
                    new TypedNavigate(
                            resolveNode(nav.source(), context), nav.alias(),
                            nav.target(), nav.predicate(), nav.form(), nav.info());
            case TypedJoin j when containsGetAll(j) ->
                    new TypedJoin(
                            resolveNode(j.left(), context), resolveNode(j.right(), context),
                            j.kind(), j.condition(), j.prefix(), j.info());
            // map over RELATION rows above a class chain (the object-space
            // map arm matched earlier; this is the relation-space wrapper)
            case TypedMap m
                    when containsGetAll(m.source())
                    && m.source().info().type()
                            instanceof Type.RelationType ->
                    new TypedMap(
                            resolveNode(m.source(), context), m.mapper(), m.info());
            // scalar/relation NATIVES over chains bottoming at a getAll
            // (size()/equal()/isEmpty() tails of assert expressions): the
            // object-space native arms matched earlier; here every arg
            // resolves structurally
            case TypedNativeCall nc
                    when containsGetAll(nc) ->
                    new TypedNativeCall(nc.callee(),
                            nc.args().stream().map(a2 -> resolveNode(a2, context))
                                    .toList(), nc.info());
            // collection literal whose ELEMENTS carry class chains
            // (assert args, 3-arg groupBy key/agg arrays): each element
            // resolves independently, structurally
            case com.legend.compiler.spec.typed.TypedCollection col
                    when containsGetAll(col) ->
                    new com.legend.compiler.spec.typed.TypedCollection(
                            col.elements().stream()
                                    .map(e2 -> resolveNode(e2, context))
                                    .toList(), col.info());
            // a CAST over a chain bottoming at a getAll (typed reads like
            // getFloat = cast(columnRead(chain))): the source resolves
            // structurally, the cast rides along
            case com.legend.compiler.spec.typed.TypedCast tc
                    when containsGetAll(tc) ->
                    new com.legend.compiler.spec.typed.TypedCast(
                            resolveNode(tc.source(), context),
                            tc.target(), tc.info());
            default -> {
                if (containsGetAll(n)) {
                    throw new NotImplementedException("class query under "
                            + n.getClass().getSimpleName()
                            + " is not resolvable yet (H2 vocabulary)");
                }
                yield n;   // no class fetch anywhere beneath: pure relation query
            }
        };
    }

    /** An op whose source chain is still in OBJECT space (class-typed). */
    /** Collection distinct/removeDuplicates over instances (no comparator). */
    private static boolean isClassDistinct(TypedNativeCall c) {
        return c.args().size() == 1
                && (c.callee().qualifiedName().equals(
                                "meta::pure::functions::collection::distinct")
                        || c.callee().qualifiedName().equals(
                                "meta::pure::functions::collection::removeDuplicates"));
    }

    /**
     * INTERIM temporal-propagation wall (audit S1): the engine applies the
     * fetch date to EVERY milestoned table in the query
     * ({@code getAppliedJoinMilestoningFilters}); lite filters only the
     * root — joining a temporal class's UNFILTERED extent would silently
     * multiply rows across its versions, so navigation to one stays loud
     * until propagation lands.
     */

    /**
     * Any colspec body reading its row parameter? A constant-only project
     * (the synthetic count column) is NOT a function of the row — mapping
     * {@code ~distinct} must not defer past it.
     */
    private static boolean projectReadsRow(TypedProject p) {
        for (var fc : p.columns()) {
            TypedLambda l = fc.fn();
            String param = l.parameters().isEmpty() ? null : l.parameters().get(0);
            if (param == null) {
                return true;    // shape surprise: keep the verified behavior
            }
            for (TypedSpec b : l.body()) {
                if (readsVar(b, param)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** {@code $var} read anywhere beneath {@code n}; shadowing lambdas stop the walk. */
    private static boolean readsVar(TypedSpec n, String var) {
        if (n instanceof TypedVariable v
                && v.name().equals(var)) {
            return true;
        }
        if (n instanceof TypedLambda l && l.parameters().contains(var)) {
            return false;
        }
        for (TypedSpec c : n.children()) {
            if (readsVar(c, var)) {
                return true;
            }
        }
        return false;
    }

    /** The element CLASS of an object-space chain (for synthetic lambdas). */
    /** The NAV-SLOT correlation pass: a demanded navigate step whose lifted
     * head carries a CORRELATED predicate gets it ANDed into the step\u0027s
     * own condition (parentRow, targetTableRow — both in scope) BEFORE
     * materialization, via the association route\u0027s exact composition. */
    private TypedSpec augmentNavPredicates(TypedSpec pipe, ClassSource cs,
            Map<String, String> navHeadByAlias, Set<String> demandedNavs,
            Set<String> composed,
            Map<String, Substitution.AssocSub> parentAssocs,
            Map<String, NavMaterializer.NavMat> navMats) {
        // audit 21b F1: this walk must reach every navigate step
        // Pipelines.navSteps reaches — a scalar-through-join PM declared
        // after the class-typed Join PM leaves the TypedNavigate below a
        // TypedJoinSlot, and skipping it silently DROPPED the correlated
        // conjunct (PM-declaration-order wrong rows). The spine arms here
        // mirror navSteps' node set; materializeRoot's composed-or-loud
        // check backstops any spine node this walk still misses.
        if (pipe instanceof TypedNavigate nav && nav.alias().isPresent()) {
            TypedSpec src = augmentNavPredicates(nav.source(), cs,
                    navHeadByAlias, demandedNavs, composed, parentAssocs,
                    navMats);
            String head = navHeadByAlias.getOrDefault(nav.alias().get(),
                    nav.alias().get());
            TypedLambda corr = synthetics.correlatedPred(head);
            if (corr != null && demandedNavs.contains(nav.alias().get())
                    && nav.target() instanceof TypedGetAll ga) {
                ClassSource target = sources.get(cs.mappingFqn(), ga.classFqn());
                NavMaterializer.NavMat mat = navMats.get(nav.alias().get());
                TypedLambda aug = assocMaterial.andCorrelatedIntoCondition(
                        nav.predicate(), corr, cs, target,
                        mat != null ? mat.slotPrefixes() : Map.of(),
                        parentAssocs,
                        mat != null ? mat.subNavs() : Map.of());
                composed.add(nav.alias().get());
                return new TypedNavigate(src, nav.alias(), nav.target(),
                        aug, nav.form(), nav.info());
            }
            return src == nav.source() ? pipe
                    : new TypedNavigate(src, nav.alias(), nav.target(),
                            nav.predicate(), nav.form(), nav.info());
        }
        if (pipe instanceof TypedFilter f) {
            TypedSpec src = augmentNavPredicates(f.source(), cs,
                    navHeadByAlias, demandedNavs, composed, parentAssocs,
                    navMats);
            return src == f.source() ? pipe
                    : new TypedFilter(src, f.predicate(), f.info());
        }
        if (pipe instanceof com.legend.compiler.spec.typed.TypedJoinSlot js) {
            TypedSpec src = augmentNavPredicates(js.source(), cs,
                    navHeadByAlias, demandedNavs, composed, parentAssocs,
                    navMats);
            return src == js.source() ? pipe
                    : new com.legend.compiler.spec.typed.TypedJoinSlot(src,
                            js.alias(), js.target(), js.condition(), js.info());
        }
        return pipe;
    }

    /** A CORRELATED lifted predicate is only applicable at the association
     * route's join CONDITION (both rows in scope). Every OTHER consumer of
     * a synthetic head must refuse LOUDLY — applying only the closed
     * predicates would silently DROP the correlation (wrong rows). */
    private void requireNoCorrelatedPred(String head, String where) {
        if (synthetics.correlatedPred(head) != null) {
            throw new NotImplementedException("correlated filtered navigation"
                    + " '" + SyntheticHeads.realHead(head) + "' is not"
                    + " supported on the " + where + " route yet (the"
                    + " predicate reads the outer row)");
        }
    }

    /** A scalar property read over an object-space chain as the
     * single-column projection: EMBEDDED (non-assoc) class-hop prefixes
     * peel INTO the reading lambda (the funnel's embedded dispatch owns
     * them); ASSOCIATION hops stay in the chain for the flatten
     * (collectOpChain re-roots at their target). */
    private TypedSpec scalarReadAsProject(TypedPropertyAccess pa,
            Context context) {
        java.util.Deque<TypedPropertyAccess> path = new java.util.ArrayDeque<>();
        TypedSpec src = pa.source();
        // ALL class hops (association AND embedded) peel into the lambda:
        // the funnel's positional rules own scalar path explosion - it
        // routes associations through the full demand machinery (union
        // dispatch, otherwise, navigate slots), which the flatten's direct
        // AssociationBinding lookup cannot yet match. The flatten serves
        // only the CLASS-RESULT shapes (bare class root, class-result
        // maps), where no consuming lambda exists.
        while (src instanceof TypedPropertyAccess hp
                && hp.info().type() instanceof Type.ClassType) {
            path.addFirst(hp);
            src = hp.source();
        }
        Type rootClass = sourceClassType(src);
        TypedSpec read = new TypedVariable("p", ExprType.one(rootClass));
        for (TypedPropertyAccess hp : path) {
            read = new TypedPropertyAccess(read, hp.property(), hp.info());
        }
        read = new TypedPropertyAccess(read, pa.property(), pa.info());
        TypedLambda fn = new TypedLambda(List.of("p"), List.of(read),
                new ExprType(
                        new Type.FunctionType(
                                List.of(new Type.Param(rootClass,
                                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE)),
                                new Type.Param(pa.info().type(),
                                        pa.info().multiplicity())),
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
        return resolveChain(scalarMapAsProject(src, fn,
                pa.info().multiplicity()), context);
    }

    /** The NAVIGATE-SLOT flatten route: materialize the source pipeline
     * with the hop's TypedNavigate step DEMANDED (the same machinery the
     * funnel uses), then re-root at the step's target class with bindings
     * re-pointed through the slot prefix. */
    private ClassSource flattenNavSlot(ClassSource src, String alias,
            TypedNavigate step, Set<String> downstreamHeads,
            Map<String, Substitution.AssocSub> provOut) {
        if (!(step.target() instanceof TypedGetAll tg)) {
            throw new NotImplementedException("class flatten through a"
                    + " CHAINED navigate step ('" + alias
                    + "') is not supported yet");
        }
        String targetClass = tg.classFqn();
        ClassSource t = sources.get(src.mappingFqn(), targetClass);
        // DOWNSTREAM demand (task #63): heads the chain reads off the
        // re-rooted target dispatch through the target's OWN nav/slot
        // steps — materialize them INTO the hop so the composed prefixes
        // (employees_firm_*) exist; provenance AssocSubs below give the
        // substitution the dispatch route. Un-demanded steps still strip.
        var tNavSteps = Pipelines.navSteps(t.pipeline());
        Set<String> tSlots = Pipelines.slotAliases(t.pipeline());
        Set<String> tSlotDemand = new LinkedHashSet<>();
        Set<String> tNavDemand = new LinkedHashSet<>();
        Map<String, String> headNavAlias = new LinkedHashMap<>();
        for (String h : downstreamHeads) {
            TypedSpec hb = t.bindings().get(SyntheticHeads.realHead(h));
            if (hb == null) {
                continue;
            }
            String na = navSlotAlias(hb, t.rowVar(), tNavSteps.keySet());
            if (na != null) {
                tNavDemand.add(na);
                headNavAlias.put(h, na);
                continue;
            }
            CorrelatedSubselects.collectAliasReads(hb, t.rowVar(), tSlots,
                    tSlotDemand);
        }
        final Set<String> fSlotDemand =
                Pipelines.closeOverConditions(t.pipeline(), tSlotDemand);
        final Set<String> fNavDemand = tNavDemand;
        Pipelines.Materialized[] innerM = new Pipelines.Materialized[1];
        Pipelines.Materialized m = Pipelines.materialize(
                src.pipeline(), java.util.Set.of(), java.util.Set.of(alias),
                src.classFqn(),
                (a, tc) -> {
                    Pipelines.Materialized im = Pipelines.materialize(
                            sources.get(src.mappingFqn(), tc).pipeline(),
                            tc.equals(targetClass) ? fSlotDemand : java.util.Set.of(),
                            tc.equals(targetClass) ? fNavDemand : java.util.Set.of(),
                            tc,
                            (a2, tc2) -> Pipelines.materialize(
                                    sources.get(src.mappingFqn(), tc2).pipeline(),
                                    java.util.Set.of(), tc2).pipeline());
                    if (tc.equals(targetClass)) {
                        innerM[0] = im;
                    }
                    return im.pipeline();
                });
        String prefix = m.slotPrefixes().get(alias);
        if (prefix == null) {
            throw new IllegalStateException("resolver bug: demanded navigate"
                    + " slot '" + alias + "' produced no prefix");
        }
        Map<String, String> innerPrefixes = innerM[0] == null
                ? Map.of() : innerM[0].slotPrefixes();
        // binding pre-rewrite uses JOINSLOT prefixes only: a nav-HEAD
        // binding is a BARE class-typed slot read (dispatched via the
        // provenance AssocSub below) — the row-read rewriter would throw
        // its unrecognized-shape guard on it
        Map<String, String> innerSlotOnly = new LinkedHashMap<>(innerPrefixes);
        innerSlotOnly.keySet().removeAll(fNavDemand);
        for (var he : headNavAlias.entrySet()) {
            String ip = innerPrefixes.get(he.getValue());
            if (ip == null) {
                continue;   // step not materialized: the read stays loud
            }
            var navT = tNavSteps.get(he.getValue()).target();
            if (!(navT instanceof TypedGetAll ng)) {
                continue;
            }
            ClassSource sub = sources.get(src.mappingFqn(), ng.classFqn());
            provOut.put(he.getKey(), new Substitution.AssocSub(
                    prefix + ip, sub.rowVar(), sub.bindings(),
                    sub.classFqn(),
                    Pipelines.slotAliases(sub.pipeline())));
        }
        // audit 21b F3: the flatten contract is INNER ≡ the engine's LEFT +
        // reader null-skip — a childless parent contributes NOTHING.
        // Pipelines.materialize emits the demanded navigate join LEFT
        // (projection semantics: parent rows survive); riding it unchanged
        // serialized a phantom all-null object and ->size() counted it.
        // Re-stamp the flattened hop's join INNER, like the assoc arm.
        TypedSpec innerized = innerizeFlattenJoin(m.pipeline(), prefix);
        m = new Pipelines.Materialized(innerized, m.slotPrefixes(),
                m.stripped());
        Type.RelationType row =
                (Type.RelationType) m.pipeline().info().type();
        ExprType rowInfo = new ExprType(row,
                com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
        Map<String, TypedSpec> bindings = new LinkedHashMap<>();
        for (var e : t.bindings().entrySet()) {
            // scalar-through-slot bindings flatten onto the materialized
            // target row FIRST (inner slot prefixes), then the hop prefix
            TypedSpec b = e.getValue();
            if (!innerSlotOnly.isEmpty()
                    && !(Pipelines.unwrapToOne(b) instanceof TypedNewInstance)) {
                b = Pipelines.rewriteRowReads(b, t.rowVar(), innerSlotOnly,
                        Set.of(), java.util.function.UnaryOperator.identity());
            }
            bindings.put(e.getKey(), prefixBinding(b,
                    t.rowVar(), prefix, src.rowVar(), rowInfo));
        }
        return new ClassSource(src.mappingFqn(), targetClass, t.setId(),
                m.pipeline(), src.rowVar(), bindings, row);
    }

    /** Re-stamp the join carrying {@code prefix} INNER (audit 21b F3 —
     * the flatten's row-set contract). Walks the materialized spine
     * (joins + filters); not finding the join is a loud resolver bug,
     * never a silent LEFT. */
    private static TypedSpec innerizeFlattenJoin(TypedSpec pipe, String prefix) {
        TypedSpec out = innerizeOrNull(pipe, prefix);
        if (out == null) {
            throw new IllegalStateException("resolver bug: flatten inner-stamp"
                    + " did not find the navigate join '" + prefix
                    + "' in the materialized pipeline");
        }
        return out;
    }

    /** {@code pipe} with the prefix-matching join INNER, or null if the
     * spine holds no such join. */
    private static TypedSpec innerizeOrNull(TypedSpec pipe, String prefix) {
        if (pipe instanceof TypedJoin j) {
            if (j.prefix().isPresent() && j.prefix().get().equals(prefix)) {
                return new TypedJoin(j.left(), j.right(), innerKind(),
                        j.condition(), j.prefix(), j.info());
            }
            TypedSpec left = innerizeOrNull(j.left(), prefix);
            return left == null ? null
                    : new TypedJoin(left, j.right(), j.kind(), j.condition(),
                            j.prefix(), j.info());
        }
        if (pipe instanceof TypedFilter f) {
            TypedSpec src = innerizeOrNull(f.source(), prefix);
            return src == null ? null
                    : new TypedFilter(src, f.predicate(), f.info());
        }
        return null;
    }

    /** One re-pointed binding for the flatten's composed source: scalar
     * bindings ride {@link Pipelines#prefixColumns}; an EMBEDDED binding
     * (TypedNewInstance ctor over parent-alias columns) re-points each
     * inner property expression, keeping the ctor. */
    private static TypedSpec prefixBinding(TypedSpec b, String targetRowVar,
            String prefix, String newRowVar, ExprType rowInfo) {
        TypedSpec inner = b;
        if (inner instanceof TypedNativeCall c && c.args().size() == 1
                && c.callee().qualifiedName().equals(
                        "meta::pure::functions::multiplicity::toOne")
                && c.args().get(0) instanceof TypedNewInstance) {
            inner = c.args().get(0);
        }
        if (inner instanceof TypedNewInstance ctor) {
            Map<String, TypedSpec> props = new LinkedHashMap<>();
            for (var pe : ctor.properties().entrySet()) {
                props.put(pe.getKey(), prefixBinding(pe.getValue(),
                        targetRowVar, prefix, newRowVar, rowInfo));
            }
            return new TypedNewInstance(ctor.classFqn(), props, ctor.info());
        }
        return Pipelines.prefixColumns(b, targetRowVar, prefix,
                v -> new com.legend.compiler.spec.typed.TypedVariable(
                        newRowVar, rowInfo));
    }

    /**
     * The AUTO-MAP FLATTEN's composed source (slice 3): the class-terminal
     * hop {@code Source.all().assocEnd} re-roots the chain at the TARGET
     * class over the JOINED pipeline — source extent &#8904; target pipeline
     * on the association condition (row explosion = projection semantics),
     * target bindings re-pointed through the join prefix. INNER join: the
     * engine spells LEFT and its reader skips null-pk rows; the row sets
     * are identical (documented emission divergence, golden advisory).
     */
    private ClassSource flattenSource(ClassSource src, String hop,
            Context context, List<TypedSpec> ops, TypedSpec top,
            Map<String, Substitution.AssocSub> provOut) {
        // ROUTE by the hop's MAPPING: a class-typed Join PM
        // (employees: @Firm_Person) is a NAVIGATE SLOT — the pipeline
        // already carries its TypedNavigate step; an AssociationMapping
        // end routes through the association-binding predicate.
        TypedSpec hopBinding = src.bindings().get(hop);
        var navSteps = Pipelines.navSteps(src.pipeline());
        String alias = hopBinding == null ? null
                : navSlotAlias(hopBinding, src.rowVar(), navSteps.keySet());
        if (alias != null) {
            return flattenNavSlot(src, alias, navSteps.get(alias),
                    downstreamHeads(ops, top), provOut);
        }
        AssociationJoins.AssocJoin aj = assocMaterial.associationJoin(
                temporal, src, hop, context, false,
                downstreamHeads(ops, top));
        Pipelines.Materialized m = Pipelines.materialize(
                src.pipeline(), java.util.Set.of(), src.classFqn());
        Type.RelationType leftRow =
                (Type.RelationType) m.pipeline().info().type();
        List<Type.Column> cols = new ArrayList<>(leftRow.columns());
        for (Type.Column c : aj.targetRow().columns()) {
            cols.add(new Type.Column(aj.prefix() + c.name(),
                    c.type(), c.multiplicity()));
        }
        Type.RelationType row = new Type.RelationType(cols);
        ExprType rowInfo = new ExprType(row,
                com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
        TypedSpec joined = new TypedJoin(m.pipeline(), aj.targetPipeline(),
                innerKind(), aj.condition(),
                Optional.of(aj.prefix()), rowInfo);
        Map<String, TypedSpec> bindings = new LinkedHashMap<>();
        for (var e : aj.target().bindings().entrySet()) {
            // scalar-through-slot bindings first flatten onto the
            // MATERIALIZED target row (W4 demandedLeaves), then prefix.
            // Ctor (embedded) bindings skip: the rewriter refuses them and
            // prefixBinding walks their props itself.
            TypedSpec b = e.getValue();
            if (!aj.targetSlotPrefixes().isEmpty()
                    && !(Pipelines.unwrapToOne(b) instanceof TypedNewInstance)) {
                b = Pipelines.rewriteRowReads(b, aj.target().rowVar(),
                        aj.targetSlotPrefixes(), Set.of(),
                        java.util.function.UnaryOperator.identity());
            }
            bindings.put(e.getKey(), prefixBinding(b,
                    aj.target().rowVar(), aj.prefix(), src.rowVar(), rowInfo));
        }
        return new ClassSource(src.mappingFqn(), aj.target().classFqn(),
                aj.target().setId(), joined, src.rowVar(), bindings, row);
    }

    /** Heads read off the RE-ROOTED target class in the chain's lambdas —
     * the flatten's downstream demand (task #63: the hop target must
     * materialize WITH the nav/slot steps those heads dispatch through). */
    private static Set<String> downstreamHeads(List<TypedSpec> ops,
            TypedSpec top) {
        Set<String> heads = new LinkedHashSet<>();
        collectLambdaHeads(ops == null ? List.of() : ops, heads);
        if (top != null) {
            collectLambdaHeads(List.of(top), heads);
        }
        return heads;
    }

    private static void collectLambdaHeads(List<TypedSpec> nodes,
            Set<String> out) {
        for (TypedSpec n : nodes) {
            if (n instanceof TypedLambda lam && !lam.parameters().isEmpty()) {
                collectParamPathHeads(lam, lam.parameters().get(0), out);
            }
            collectLambdaHeads(n.children(), out);
        }
    }

    private static Type sourceClassType(TypedSpec chain) {
        Type t = chain.info().type();
        if (!(t instanceof Type.ClassType)) {
            throw new IllegalStateException("resolver bug: object-space chain typed "
                    + t.typeName());
        }
        return t;
    }

    /**
     * {@code map(chain, λp.scalar)} as the equivalent single-column
     * projection. The column takes the leaf property's name when the body
     * is a straight property read, else {@code value}.
     */
    /** The synthetic single-column projection for a scalar map/property
     * read over instances. {@code valueMult} is the ORIGINAL expression's
     * multiplicity — a to-many read is a VALUE COLLECTION and the scalar
     * lowering must LIST-aggregate it (contains/in consumers), while a
     * to-one read stays the bare scalar subquery. */
    private static TypedProject scalarMapAsProject(TypedSpec source, TypedLambda mapper,
            com.legend.compiler.element.type.Multiplicity valueMult) {
        TypedSpec body = mapper.body().get(mapper.body().size() - 1);
        String name = body instanceof TypedPropertyAccess bpa
                ? bpa.property() : "value";
        Type.Param result =
                ((Type.FunctionType) mapper.info().type()).result();
        Type.RelationType row =
                new Type.RelationType(List.of(
                        new Type.Column(
                                name, result.type(), result.multiplicity())));
        return new TypedProject(source,
                List.of(new TypedFuncCol(name, mapper)),
                new ExprType(row, valueMult));
    }

    /** A run of CLASS-typed property hops bottoming at an object-space
     * chain (the auto-map family's `Firm.all().employees` prefix). */
    private record HopChain(TypedSpec root, List<TypedPropertyAccess> hops) {
    }

    private static HopChain hopChainOf(TypedSpec n) {
        java.util.ArrayDeque<TypedPropertyAccess> hops = new java.util.ArrayDeque<>();
        TypedSpec cur = n;
        while (cur instanceof TypedPropertyAccess pa
                && pa.info().type() instanceof Type.ClassType) {
            hops.addFirst(pa);
            cur = pa.source();
        }
        return !hops.isEmpty() && isObjectSpace(cur)
                ? new HopChain(cur, List.copyOf(hops)) : null;
    }

    /** size()/count() over a class extent = the ROW COUNT of the resolved
     * pipeline: project ONE constant column (no slot demand — engine emits
     * select count(*)) and count the relation. */
    private TypedSpec classExtentCount(TypedNativeCall nc, Context context) {

                Type intType =
                        Type.Primitive.INTEGER;
                ExprType oneInt =
                        new ExprType(intType,
                                com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
                ExprType rowParam =
                        new ExprType(
                                nc.args().get(0).info().type(),
                                com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
                TypedLambda one = new TypedLambda(List.of("p"),
                        List.of(new TypedCInteger(1L, oneInt)),
                        new ExprType(
                                new Type.FunctionType(
                                        List.of(new Type.Param(
                                                rowParam.type(), rowParam.multiplicity())),
                                        new Type.Param(
                                                intType,
                                                com.legend.compiler.element.type.Multiplicity.Bounded.ONE)),
                                com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
                Type.RelationType relType =
                        new Type.RelationType(List.of(
                                new Type.Column(
                                        "c", intType,
                                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE)));
                TypedProject proj = new TypedProject(nc.args().get(0),
                        List.of(new TypedFuncCol("c", one)),
                        new ExprType(relType,
                                com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
                TypedSpec rel = resolveChain(proj, context);
                var relSize = ctx.findFunction("meta::pure::functions::relation::size")
                        .stream().findFirst().orElseThrow(() -> new IllegalStateException(
                                "relation size overload missing from the catalog"));
                return new TypedNativeCall(relSize,
                        List.of(rel), nc.info());
                }

    /** Scalar-space filter over an exploded hop column: the folded source
     * is a ONE-COLUMN relation; the predicate's scalar param becomes a
     * read of that column. */
    private TypedSpec foldScalarHopFilter(TypedFilter f, Context context) {
        TypedSpec rel = resolveNode(f.source(), context);
        if (!(rel.info().type() instanceof Type.RelationType rt)
                || rt.columns().size() != 1) {
            throw new NotImplementedException("scalar filter over a"
                    + " class-derived collection did not fold to a"
                    + " one-column relation");
        }
        Type.RelationType.Column col = rt.columns().get(0);
        TypedSpec rowRead = new TypedPropertyAccess(
                new com.legend.compiler.spec.typed.TypedVariable(
                        "r", ExprType.one(rt)),
                col.name(), new ExprType(col.type(), col.multiplicity()));
        TypedSpec pred = substituteParam(f.predicate(), rowRead);
        TypedLambda fn = new TypedLambda(List.of("r"), List.of(pred),
                f.predicate().info());
        return new TypedFilter(rel, fn, rel.info());
    }

    /** &beta;-substitute a one-param lambda's variable with {@code read} —
     * via the inliner's LET reduction (one substitution engine, no second
     * walker). */
    private TypedSpec substituteParam(TypedLambda lam, TypedSpec read) {
        // audit 21b F6 (named wall): the let-reduction splices `read` at
        // EVERY param read. For a row-rooted read that is plain multi-eval;
        // for a source CHAIN it is DECORRELATION — a second fresh extent
        // replaces the row-correlated value. Refusing here is by design,
        // not an accident of downstream vocabulary walls.
        if (!rowRootedRead(read)) {
            int reads = 0;
            for (TypedSpec b : lam.body()) {
                reads += countParamReads(b, lam.parameters().get(0));
            }
            if (reads > 1) {
                throw new NotImplementedException("class-result mapper reads"
                        + " its parameter " + reads + " times; splicing the"
                        + " source chain at each read would decorrelate the"
                        + " later reads (a fresh extent replaces the"
                        + " row-correlated value)");
            }
            if (reads == 0) {
                // audit 22a L7: a mapper that IGNORES its parameter has
                // per-element semantics in pure (one result per source
                // element); the let-splice collapses that to ONE evaluation
                // — wrong cardinality, silently.
                throw new NotImplementedException("class-result mapper"
                        + " ignores its parameter — the splice would collapse"
                        + " per-element semantics to one evaluation");
            }
        }
        java.util.List<TypedSpec> body = new java.util.ArrayList<>();
        body.add(new com.legend.compiler.spec.typed.TypedLet(
                lam.parameters().get(0), read, read.info()));
        body.addAll(lam.body());
        return new com.legend.compiler.spec.UserCallInliner(specs)
                .inlineBody(body).get(0);
    }

    /** A read rooted at a VARIABLE (row-correlated): duplication is
     * multi-eval of the same row's value, never decorrelation. */
    private static boolean rowRootedRead(TypedSpec read) {
        return switch (read) {
            case TypedVariable ignored -> true;
            case TypedPropertyAccess pa -> rowRootedRead(pa.source());
            default -> false;
        };
    }

    /** Shadow-aware count of {@code $var} reads beneath {@code n}. */
    private static int countParamReads(TypedSpec n, String var) {
        if (n instanceof TypedVariable v) {
            return v.name().equals(var) ? 1 : 0;
        }
        if (n instanceof TypedLambda l && l.parameters().contains(var)) {
            return 0;
        }
        int c = 0;
        for (TypedSpec ch : n.children()) {
            c += countParamReads(ch, var);
        }
        return c;
    }

    private static boolean isObjectSpace(TypedSpec source) {
        return switch (source) {
            case TypedGetAll ignored -> true;
            // a CLASS-typed property HOP over an object-space chain IS
            // object space (the auto-map flatten re-roots at its target —
            // collectOpChain composes the joined source)
            case TypedPropertyAccess pa
                    when pa.info().type() instanceof Type.ClassType ->
                    isObjectSpace(pa.source());
            // ->map with a CLASS-result mapper stays in object space (the
            // flatten's map spelling — collectOpChain beta-folds it)
            case TypedMap m
                    when ((Type.FunctionType) m.mapper().info().type()).result()
                            .type() instanceof Type.ClassType ->
                    isObjectSpace(m.source());
            case TypedFrom fr ->
                    isObjectSpace(fr.source());
            case TypedFilter f -> isObjectSpace(f.source());
            case TypedLimit l -> isObjectSpace(l.source());
            case TypedDrop d -> isObjectSpace(d.source());
            case TypedSlice s -> isObjectSpace(s.source());
            case TypedSortBy sb -> isObjectSpace(sb.source());
            case TypedNativeCall c when isFirstLike(c) ->
                    isObjectSpace(c.args().get(0));
            case TypedNativeCall c when isStaticAt(c) ->
                    isObjectSpace(c.args().get(0));
            case TypedNativeCall c when isClassToOne(c) ->
                    isObjectSpace(c.args().get(0));
            case TypedNativeCall c when isClassDistinct(c) ->
                    isObjectSpace(c.args().get(0));
            case TypedNativeCall c when classSortOf(c) != null ->
                    isObjectSpace(c.args().get(0));
            default -> false;
        };
    }

    /** {@code at(coll, k)} with a LITERAL index — class-space slice. */
    private static boolean isStaticAt(TypedNativeCall c) {
        return c.args().size() == 2
                && "meta::pure::functions::collection::at"
                        .equals(c.callee().qualifiedName())
                && c.args().get(1)
                        instanceof TypedCInteger;
    }

    /** {@code toOne(instances)}: multiplicity coercion over a class
     * collection — PASS-THROUGH in the pipeline (the engine raises on
     * N&ne;1; here the value compare sees all N and fails loud — a
     * documented, weaker-but-never-silent stand-in). */
    private static boolean isClassToOne(TypedNativeCall c) {
        return c.args().size() == 1
                && "meta::pure::functions::multiplicity::toOne"
                        .equals(c.callee().qualifiedName());
    }

    private static final String FIRST_FQN = "meta::pure::functions::collection::first";
    private static final String HEAD_FQN = "meta::pure::functions::collection::head";
    private static final String SORT_FQN = "meta::pure::functions::collection::sort";
    private static final String CONCAT_FQN =
            "meta::pure::functions::collection::concatenate";
    private static final String COMPARE_FQN = "meta::pure::functions::lang::compare";
    private static final String EQUAL_FQN = "meta::pure::functions::boolean::equal";
    private static final String EQ_FQN = "meta::pure::functions::boolean::eq";

    /** first()/head() over an object-space chain — LIMIT 1 in disguise. */
    private static boolean isFirstLike(TypedNativeCall c) {
        String fqn = c.callee().qualifiedName();
        return c.args().size() == 1
                && (FIRST_FQN.equals(fqn) || HEAD_FQN.equals(fqn));
    }

    /**
     * Class-space {@code sort(key, {x,y|compare})}: the comparator must be a
     * BARE compare over the two parameters — its argument order IS the
     * direction ({@code $x->compare($y)} ascending, {@code $y->compare($x)}
     * descending). Anything richer has no relation sort shape.
     */
    private static TypedSortBy classSortOf(TypedSpec n) {
        if (!(n instanceof TypedNativeCall c) || c.args().size() != 3
                || !SORT_FQN.equals(c.callee().qualifiedName())
                || !(c.args().get(1) instanceof TypedLambda key)
                || !(c.args().get(2) instanceof TypedLambda cmp)) {
            return null;
        }
        Boolean ascending = comparatorDirection(cmp);
        return ascending == null ? null
                : new TypedSortBy(c.args().get(0), key, ascending, c.info());
    }

    private static Boolean comparatorDirection(TypedLambda cmp) {
        if (cmp.parameters().size() != 2 || cmp.body().size() != 1
                || !(cmp.body().get(0) instanceof TypedNativeCall cc)
                || !COMPARE_FQN.equals(cc.callee().qualifiedName())
                || cc.args().size() != 2
                || !(cc.args().get(0) instanceof TypedVariable a)
                || !(cc.args().get(1) instanceof TypedVariable b)) {
            return null;
        }
        String p0 = cmp.parameters().get(0);
        String p1 = cmp.parameters().get(1);
        if (a.name().equals(p0) && b.name().equals(p1)) {
            return Boolean.TRUE;
        }
        if (a.name().equals(p1) && b.name().equals(p0)) {
            return Boolean.FALSE;
        }
        return null;
    }

    /** concatenate over two class-collection chains, both fetch-bearing. */
    private static TypedNativeCall classConcatOf(TypedSpec n) {
        return n instanceof TypedNativeCall c && c.args().size() == 2
                && CONCAT_FQN.equals(c.callee().qualifiedName())
                && containsGetAll(c.args().get(0)) && containsGetAll(c.args().get(1))
                ? c : null;
    }

    /** Statically decide an if() condition, or null when genuinely runtime. */
    private static Boolean staticBool(TypedSpec cond) {
        return switch (cond) {
            case TypedCBoolean b -> b.value();
            case TypedNativeCall c when c.args().size() == 2
                    && (EQUAL_FQN.equals(c.callee().qualifiedName())
                            || EQ_FQN.equals(c.callee().qualifiedName())) -> {
                Object l = literalValue(c.args().get(0));
                Object r = literalValue(c.args().get(1));
                yield l == null || r == null ? null : (Boolean) literalEquals(l, r);
            }
            default -> null;
        };
    }

    private static Object literalValue(TypedSpec n) {
        return switch (n) {
            case TypedCBoolean b -> b.value();
            case TypedCInteger i -> i.value();
            case TypedCFloat f -> f.value();
            case TypedCString st -> st.value();
            default -> null;
        };
    }

    /**
     * Literal equality with SQL's semantics: NUMBERS compare numerically
     * (1 == 1.0 — SQL '=' would say true, so the fold must too; audit),
     * cross-kind value/string is a plain equals.
     */
    private static boolean literalEquals(Object l, Object r) {
        if (l instanceof Number ln && r instanceof Number rn) {
            return new java.math.BigDecimal(ln.toString())
                    .compareTo(new java.math.BigDecimal(rn.toString())) == 0;
        }
        return l.equals(r);
    }

    /** An if() branch is a zero-arg thunk; its single body statement is the value. */
    private static TypedSpec unthunk(TypedSpec branch) {
        if (branch instanceof TypedLambda l && l.parameters().isEmpty()
                && l.body().size() == 1) {
            return l.body().get(0);
        }
        return branch;
    }

    private static boolean containsGetAll(TypedSpec n) {
        if (n instanceof TypedGetAll) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (containsGetAll(c)) {
                return true;
            }
        }
        return false;
    }

    // =====================================================================
    // Object-space chain resolution (the H2 heart)
    // =====================================================================

    private TypedSpec resolveChain(TypedSpec top, Context context) {
        if (context.isNone()) {
            throw new MappingResolutionException(
                    "class query requires an execution context: add"
                            + " ->from(mapping, runtime) or supply a runtime");
        }
        return resolveObject(top, context);
    }

    /**
     * Resolve one object-space chain (SCAN-THEN-MATERIALIZE, plan §2.2):
     * collect the ops, scan the user lambdas for demand, materialize the
     * pipeline (demanded slots -> prefixed LEFT joins; un-demanded slots
     * CANCELLED), THEN fold the ops back on with substitution against the
     * final row type. No restamp pass exists.
     */

    /** PHASE output: navigate-slot registration — the demanded slots and
     * nav steps, their materialized targets (NavMaterializer.NavMat trees), the per-head
     * substitution material, and the SECOND-identity extras routed to the
     * association fold (per-use join identity). */
    private record NavPlan(Set<String> demanded, Set<String> demandedNavs,
            Map<String, Substitution.AssocSub> assocs,
            Map<String, NavMaterializer.NavMat> navMats,
            Map<String, List<List<String>>> navTails,
            Map<String, String> navHeadByAlias,
            Map<String, String> extraNavHeads,
            Map<String, List<List<String>>> extraNavTails,
            Map<String, TypedNavigate> navSteps,
            Map<String, String> corrNavHeads) {}

    /** PHASE — slot + navigate-step demand: heads whose bindings read
     * join slots demand them; class-typed Join PM heads materialize their
     * targets (recursively, with per-hop temporal context) and register
     * the head substitution material under the chain key. */
    private NavPlan registerNavigations(ClassSource cs,
            Set<List<String>> paths) {
        // Slot demand (heads whose bindings read join slots).
        Set<String> slotAliases = Pipelines.slotAliases(cs.pipeline());
        Set<String> demanded = new LinkedHashSet<>();
        if (!slotAliases.isEmpty()) {
            for (List<String> path : paths) {
                TypedSpec binding = cs.bindings().get(SyntheticHeads.realHead(path.get(0)));
                if (binding != null) {
                    CorrelatedSubselects.collectAliasReads(binding, cs.rowVar(), slotAliases, demanded);
                }
            }
        }

        // Navigate-step (class-typed Join PM) demand: a 2-hop path whose
        // head binding reads a navigate slot ($row.alias, class-typed)
        // demands that step; its target joins as the class's own pipeline.
        var navSteps = Pipelines.navSteps(cs.pipeline());
        Set<String> demandedNavs = new LinkedHashSet<>();
        Map<String, Substitution.AssocSub> assocs = new LinkedHashMap<>();
        Map<String, List<List<String>>> navTails =
                new LinkedHashMap<>();
        Map<String, String> navHeadByAlias = new LinkedHashMap<>();
        // SECOND identities on one physical slot (date-fingerprinted /
        // filter-lifted synthetic heads beside the base): the slot
        // materializes once for the FIRST identity; every other identity
        // emits its OWN prefixed join from the same nav material (engine:
        // joins keyed by date / per-use). headKey → slot alias, + tails.
        Map<String, String> extraNavHeads = new LinkedHashMap<>();
        Map<String, List<List<String>>> extraNavTails =
                new LinkedHashMap<>();
        Map<String, String> corrNavHeads = new LinkedHashMap<>();
        for (List<String> path : paths) {
            if (path.size() < 2) {
                continue;
            }
            TypedSpec headBinding = cs.bindings().get(SyntheticHeads.realHead(path.get(0)));
            if (headBinding == null) {
                continue;   // association heads (below)
            }
            // OTHERWISE per-leaf dispatch (V1 §D.5): a leaf mapped by the
            // embedded partial reads the PARENT row — no demand; any other
            // leaf demands the FALLBACK's navigate slot. Same head can go
            // both ways in one query. The normalizer's emission is the one
            // canonical shape: otherwise(^Inner(...), $row.<slot>).
            TypedSpec navRead = headBinding;
            var ow = Substitution.otherwiseOf(headBinding);
            if (ow != null) {
                var partial = (TypedNewInstance)
                        ow.args().get(0);
                if (partial.properties().containsKey(path.get(1))) {
                    continue;   // embedded leaf: parent-alias read, no join
                }
                navRead = ow.args().get(1);
            }
            // EMBEDDED head: the path walks INTO the ^Inner ctor — the
            // navigate-slot demand comes from the ctor's MID property expr
            // ($p.classification.system.name where classification is
            // embedded and system a class-typed Join sub-PM hoisted onto
            // the owner pipeline); the AssocSub registers under the DOTTED
            // key, which rewritePath's chain lookup already consumes.
            int mid = 1;
            TypedSpec drill = navRead;
            while (true) {
                TypedSpec inner = drill;
                if (inner instanceof TypedNativeCall tc1 && tc1.args().size() == 1
                        && tc1.callee().qualifiedName().equals(
                                "meta::pure::functions::multiplicity::toOne")) {
                    inner = tc1.args().get(0);
                }
                if (inner instanceof TypedNewInstance ni
                        && mid + 1 < path.size()
                        && ni.properties().containsKey(
                                SyntheticHeads.realHead(path.get(mid)))) {
                    // a SYNTHETIC (filter-lifted) mid component drills by
                    // its REAL property; the parked pred applies at the
                    // slot materialization below (#70 embedded sub-chains)
                    drill = ni.properties().get(
                            SyntheticHeads.realHead(path.get(mid)));
                    mid++;
                } else {
                    drill = inner;
                    break;
                }
            }
            String alias = navSlotAlias(drill, cs.rowVar(), navSteps.keySet());
            if (alias == null) {
                continue;
            }
            String headKey = String.join(".", path.subList(0, mid));
            // a lifted head's predicate reads are TAILS too: they pull the
            // target's own slots exactly like demanded leaves
            List<List<String>> predTails =
                    synthetics.predTailsFor(path, mid);
            // JOIN IDENTITY: a SECOND head identity on one physical slot
            // routes through its own prefixed join (below) — the slot
            // itself materializes once for the first identity.
            String priorHead = navHeadByAlias.get(alias);
            if (priorHead != null && !priorHead.equals(headKey)) {
                extraNavHeads.putIfAbsent(headKey, alias);
                List<List<String>> et = extraNavTails
                        .computeIfAbsent(headKey, k -> new ArrayList<>());
                et.add(path.subList(mid, path.size()));
                et.addAll(predTails);
                continue;
            }
            // the head's TAIL paths drive the target's OWN slot demand
            // (nested navigation: $a.b.c.pk materializes b's target WITH
            // its c slot; the leaf reads the composed prefix b_c_pk)
            navTails.computeIfAbsent(alias, k -> new ArrayList<>())
                    .add(path.subList(mid, path.size()));
            navTails.get(alias).addAll(predTails);
            navHeadByAlias.put(alias, headKey);
            // #69 EXPLODING parent-copy reroute: a correlated pred whose
            // OUTER reads hop a parent NAV can never compose on the flat
            // navigate step's ON — the head leaves the slot spine (the
            // slot stays unmaterialized) and joins as an AssocJoin whose
            // target is the parent-copy subselect (fold 2b).
            TypedLambda cpH = mid == 1
                    ? synthetics.correlatedPred(path.get(0)) : null;
            if (cpH != null && assocMaterial.corrPredDemandsParentNav(cpH)
                    && !demandedNavs.contains(alias)) {
                corrNavHeads.putIfAbsent(headKey, alias);
                continue;
            }
            if (mid > 1 && path.subList(0, mid).stream()
                    .anyMatch(c -> synthetics.correlatedPred(c) != null)) {
                // a CORRELATED pred on an embedded-drilled chain component
                // has no composition route yet — leave the step undemanded
                // (loud read), never an unfiltered join
                continue;
            }
            if (demandedNavs.contains(alias)) {
                continue;
            }
            demandedNavs.add(alias);
            var nav = navSteps.get(alias);
            // The nav condition may read joinslot sub-rows: demand them too.
            for (TypedSpec b : nav.predicate().body()) {
                for (String slot : slotAliases) {
                    if (Pipelines.referencesAliasOn(b,
                            nav.predicate().parameters().get(0), Set.of(slot))) {
                        demanded.add(slot);
                    }
                }
            }
        }
        demanded = Pipelines.closeOverConditions(cs.pipeline(), demanded);
        // Materialize each demanded navigate TARGET with the slot demand its
        // tail paths imply (recursively — a tail through the target's own
        // class-typed slot materializes THAT slot's target too), then
        // register the head's substitution material with the REAL slot
        // prefixes (audit: Map.of() here walled every nested slot read).
        Map<String, NavMaterializer.NavMat> navMats = new LinkedHashMap<>();
        for (String alias : demandedNavs) {
            var nav = navSteps.get(alias);
            String targetClass = ((TypedGetAll)
                    nav.target()).classFqn();
            // the lifted head's parked preds thread as parkedPreds: their
            // DIRECT slot-alias reads join the demand (the sub-route's
            // rule, applied to the TOP materialization too — the Fork
            // concat-branch preds read the target's address slot)
            String bareKey0 = navHeadByAlias.getOrDefault(alias, alias);
            bareKey0 = bareKey0.substring(bareKey0.lastIndexOf('.') + 1);
            navMats.put(alias, navMaterializer.navTargetMaterialized(temporal, cs.mappingFqn(), targetClass,
                    navTails.getOrDefault(alias, List.of()),
                    navHeadByAlias.getOrDefault(alias, alias),
                    TemporalContext.NONE,
                    synthetics.allPreds(bareKey0)));
            // a LIFTED head's predicate applies INSIDE the join target
            // (engine: the chain filter parks on the navigation's join-tree
            // node); the composite right side carries its own filters, so
            // the outer join-stamping never double-stamps it
            String liftedHead = navHeadByAlias.getOrDefault(alias, alias);
            // preds park under the BARE synthetic head — a DOTTED chain
            // key (embedded drill: money.usdRates#f0) keys the pred by
            // its LAST component (silent-skip here = unfiltered join =
            // wrong rows)
            String predKey = liftedHead.substring(
                    liftedHead.lastIndexOf('.') + 1);
            if (synthetics.hasPred(predKey)
                    && synthetics.correlatedPred(predKey) == null) {
                ClassSource target = sources.get(cs.mappingFqn(), targetClass);
                NavMaterializer.NavMat mat = navMats.get(alias);
                navMats.put(alias, new NavMaterializer.NavMat(
                        synthetics.applyToPipe(predKey, mat.pipeline(),
                                (p, pred) -> CorrelatedSubselects.predFilteredPipe(p, target,
                                        mat.slotPrefixes(), mat.subNavs(),
                                        pred, cs.mappingFqn())),
                        mat.slotPrefixes(), mat.stripped(), mat.subNavs()));
            }
        }
        for (String alias : demandedNavs) {
            var nav = navSteps.get(alias);
            String targetClass = ((TypedGetAll)
                    nav.target()).classFqn();
            ClassSource target = sources.get(cs.mappingFqn(), targetClass);
            // SUB-navigation material: for each 3-hop tail, the mid
            // property's minted sub-alias, its materialized prefix, and the
            // SUB-TARGET's binding table (leaves resolve through it —
            // audit 12 F1). Un-materialized sub-steps (temporal/filtered
            // gates) are absent: their reads stay loud.
            Map<String, Substitution.SubNav> subNavs =
                    navMats.get(alias).subNavs();
            assocs.put(navHeadByAlias.getOrDefault(alias, alias),
                    new Substitution.AssocSub(alias + "_",
                    target.rowVar(), target.bindings(), target.classFqn(),
                    Pipelines.slotAliases(target.pipeline()),
                    navMats.get(alias).slotPrefixes(), null, null,
                    temporal.milestoneColumnsOf(target.pipeline(), target.classFqn()),
                    subNavs));
        }

        return new NavPlan(demanded, demandedNavs, assocs, navMats, navTails,
                navHeadByAlias, extraNavHeads, extraNavTails, navSteps,
                corrNavHeads);
    }



    /** #70 composite chain-backed exists/scalar target: the pipeline with
     * the sibling slot's table joined IN, and hop-1's condition oriented
     * onto the composite row. Null when the shape does not apply. */
            /** True when the expression reads {@code var} other than through the
     * {@code slot} sub-row ({@code $var.slot.x}). */
        /** PHASE 2a'' — CLASS-TYPED LEAF under an emptiness call:
     * registers the DOTTED-path correlated-EXISTS material (engine:
     * semi-join + key null check on the exploded chain row); details in
     * the body comments. Mutates {@code existsSubs}. */
    private void registerDottedExistsSubs(ClassSource cs,
            List<TypedSpec> ops, TypedSpec top,
            List<TypedGraphTree> tree, Context context,
            Map<String, Substitution.AssocSub> assocs,
            Map<String, Substitution.ExistsSub> existsSubs) {
        // 2a''. CLASS-TYPED LEAF under an emptiness call — isNotEmpty(
        // $p.a.b) where b is itself a navigation step on the CHAIN TARGET:
        // correlated EXISTS on the exploded chain row (engine: semi-join +
        // key null check — row-per-row identical truth value). The leaf's
        // nav material registers under the DOTTED path; the oriented
        // condition's parent-side reads PRE-PREFIX onto the chain's joined
        // columns so the exists rewrite's plain var swap lands them on the
        // row. Registered after join-key collection: these conditions read
        // CHAIN columns, never root keys. ONLY paths actually under an
        // emptiness call register — eager registration stamped temporal
        // context on chains the ordinary (inheritance-aware) route owns
        // (regressed testBiTemporalToBiTemporalDatePropagation).
        Set<List<String>> emptinessChainPaths =
                new LinkedHashSet<>();
        for (TypedSpec op : ops) {
            if (op instanceof TypedFilter f) {
                for (TypedSpec b : f.predicate().body()) {
                    collectEmptinessChainPaths(b,
                            f.predicate().parameters().get(0),
                            emptinessChainPaths);
                }
            }
            if (op instanceof TypedSortBy sb) {
                for (TypedSpec b : sb.key().body()) {
                    collectEmptinessChainPaths(b, sb.key().parameters().get(0),
                            emptinessChainPaths);
                }
            }
        }
        if (tree == null) {
            for (TypedLambda fn : terminalLambdas(top)) {
                for (TypedSpec b : fn.body()) {
                    collectEmptinessChainPaths(b, fn.parameters().get(0),
                            emptinessChainPaths);
                }
            }
        }
        for (List<String> path : emptinessChainPaths) {
            if (path.size() < 2) {
                continue;
            }
            String dotted = String.join(".", path);
            Substitution.AssocSub chain = assocs.get(
                    String.join(".", path.subList(0, path.size() - 1)));
            if (chain == null || existsSubs.containsKey(dotted)) {
                continue;
            }
            String leafName = path.get(path.size() - 1);
            String leaf = SyntheticHeads.realHead(leafName);
            ClassSource parent = sources.get(cs.mappingFqn(),
                    chain.targetClassFqn());
            TypedLambda cond;
            TypedSpec tPipe;
            ClassSource t;
            Map<String, String> tPrefixes;
            TypedSpec leafBinding = parent.bindings().get(leaf);
            if (leafBinding != null) {
                TypedSpec inner = leafBinding;
                if (inner instanceof TypedNativeCall c1 && c1.args().size() == 1
                        && c1.callee().qualifiedName().equals(
                                "meta::pure::functions::multiplicity::toOne")) {
                    inner = c1.args().get(0);
                }
                var pNavSteps = Pipelines.navSteps(parent.pipeline());
                String alias = navSlotAlias(inner, parent.rowVar(),
                        pNavSteps.keySet());
                var nav = alias == null ? null : pNavSteps.get(alias);
                if (nav == null || !(nav.target()
                        instanceof TypedGetAll tg)
                        || !sources.binds(cs.mappingFqn(), tg.classFqn())) {
                    continue;
                }
                t = sources.get(cs.mappingFqn(), tg.classFqn());
                Pipelines.Materialized tm = Pipelines.materialize(
                        t.pipeline(), Set.of(), t.classFqn());
                TypedSpec p0 = tm.pipeline();
                cond = nav.predicate();
                if (cond.parameters().size() == 2) {
                    Set<String> tgtReads = new LinkedHashSet<>();
                    for (TypedSpec b0 : cond.body()) {
                        Pipelines.collectVarReads(b0,
                                cond.parameters().get(1), tgtReads);
                    }
                    p0 = Pipelines.widenConcatenateForKeys(p0, tgtReads);
                }
                tPipe = temporal.temporalTargetPipe(parent, t, dotted,
                        temporal.applyJoinTemporalFilters(p0, t, Map.of()));
                tPrefixes = tm.slotPrefixes();
                final TypedSpec tp = tPipe;
                final var tpx = tPrefixes;
                final ClassSource tt = t;
                requireNoCorrelatedPred(leafName, "exists-navigation");
                tPipe = synthetics.applyToPipe(leafName, tp, (p, pred) ->
                        CorrelatedSubselects.predFilteredPipe(p, tt, tpx, pred, cs.mappingFqn()));
            } else if (ctx.findAssociationOf(parent.classFqn(), leaf)
                    .isPresent()) {
                // the nested predicate's path HEADS demand target slots —
                // without them, a slot-backed read inside the exists
                // predicate ($e.address.name) finds an unmaterialized slot
                Set<String> innerDemand = new LinkedHashSet<>();
                for (TypedLambda il : InnerDemand.lambdas(ops, path)) {
                    if (!il.parameters().isEmpty()) {
                        collectParamPathHeads(il, il.parameters().get(0),
                                innerDemand);
                    }
                }
                AssociationJoins.AssocJoin aj = assocMaterial.associationJoin(temporal, parent, leafName, context,
                        true, innerDemand, dotted);
                t = aj.target();
                cond = aj.condition();
                tPipe = aj.targetPipeline();
                tPrefixes = aj.targetSlotPrefixes();
            } else {
                continue;
            }
            // audit 23 #75: a MISSING property must not silently default
            // to to-many semantics — the demand scan and G disagree
            if (ctx.findProperty(parent.classFqn(), leaf).isEmpty()) {
                throw new IllegalStateException("resolver bug: exists-leaf"
                        + " property '" + leaf + "' is not declared on '"
                        + parent.classFqn() + "' — G admitted a read the"
                        + " model does not carry");
            }
            boolean leafToMany = !(ctx.findProperty(parent.classFqn(), leaf)
                    .map(pr -> pr.multiplicity())
                    .filter(mm -> mm instanceof com.legend.compiler.element.type
                            .Multiplicity.Bounded bb
                            && Integer.valueOf(1).equals(bb.upper()))
                    .isPresent());
            if (chain.readVar() != null) {
                throw new IllegalStateException("resolver bug: dotted-path"
                        + " EXISTS registration over a read-var AssocSub —"
                        + " the chain prefix pre-prefixing assumes joined-row"
                        + " reads (audit 14 B-F7)");
            }
            String pv = cond.parameters().get(0);
            TypedSpec cbody = Pipelines.prefixColumns(
                    cond.body().get(cond.body().size() - 1), pv,
                    chain.prefix(), v -> v);
            TypedLambda chainedCond = new TypedLambda(cond.parameters(),
                    List.of(cbody), cond.info());
            NestedScope dns = nestedScope(t, ops, path, context, tPipe);
            existsSubs.put(dotted, new Substitution.ExistsSub(dns.pipeline(),
                    chainedCond, t.rowVar(), t.bindings(),
                    dns.row(),
                    t.classFqn(), Pipelines.slotAliases(t.pipeline()),
                    tPrefixes, leafToMany)
                    .withInnerRegs(dns.regs()));
        }

    }

    /** PHASE 2b-i output: the root pipeline materialized (demanded
     * slots -> prefixed LEFT joins, un-demanded slots CANCELLED), every
     * milestoned join alias stamped by the ambient context, and the root
     * fetch's own milestoning filter applied. */
    private record RootPipe(Pipelines.Materialized m,
            TypedSpec materializedPipe) {}

    /** PHASE 2b-i — materialize the root pipeline and stamp it: slot
     * demand -> Pipelines.materialize; the join-walk applies the ambient
     * temporal context per alias (class-governed nav targets, chained-PM
     * mid tables by their OWN milestoning, physical slots); then the
     * fetch's own point/pair/range filter. */
    private RootPipe materializeRoot(ClassSource cs, TypedGetAll g,
            Set<String> demanded, Set<String> demandedNavs,
            Map<String, NavMaterializer.NavMat> navMats, Map<String, String> navHeadByAlias,
            Map<String, Substitution.AssocSub> parentAssocs) {
        Set<String> corrComposed = new LinkedHashSet<>();
        TypedSpec csPipe = augmentNavPredicates(cs.pipeline(), cs,
                navHeadByAlias, demandedNavs, corrComposed, parentAssocs,
                navMats);
        // audit 21b F1 backstop: a demanded navigate head carrying a
        // correlated predicate that the augment walk did NOT compose has
        // exactly one fate — loud. The lifted-pred apply site skips
        // correlated preds on the assumption they were composed here;
        // proceeding would silently drop the correlation (wrong rows).
        for (String alias : demandedNavs) {
            String head = navHeadByAlias.getOrDefault(alias, alias);
            if (synthetics.correlatedPred(head) != null
                    && !corrComposed.contains(alias)) {
                throw new IllegalStateException("resolver bug: correlated"
                        + " predicate on navigate step '" + alias + "' (head '"
                        + SyntheticHeads.realHead(head) + "') was not composed"
                        + " into the join condition — the augment walk missed"
                        + " the step (spine node unhandled?); proceeding would"
                        + " silently drop the correlation");
            }
        }
        Pipelines.Materialized m = Pipelines.materialize(
                csPipe, demanded, demandedNavs, cs.classFqn(),
                (alias, targetClass) -> navMats.containsKey(alias)
                        ? navMats.get(alias).pipeline()
                        : Pipelines.materialize(
                                sources.get(cs.mappingFqn(), targetClass).pipeline(),
                                Set.of(), targetClass).pipeline());
        Map<String, String> navPrefixToClass = new LinkedHashMap<>();
        Map<String, String> navPrefixToChain = new LinkedHashMap<>();
        Map<String, String> midPrefixToChain = new LinkedHashMap<>();
        Map<String, String> midPrefixToDim = new LinkedHashMap<>();
        Set<String> slotAliases = Pipelines.slotAliases(cs.pipeline());
        for (var navE : Pipelines.navSteps(cs.pipeline()).entrySet()) {
            if (navE.getValue().target()
                    instanceof TypedGetAll tg2) {
                String chain = navHeadByAlias.getOrDefault(navE.getKey(),
                        navE.getKey());
                navPrefixToClass.put(navE.getKey() + "_", tg2.classFqn());
                navPrefixToChain.put(navE.getKey() + "_", chain);
                // MID slots of a CHAINED PM (@J1 > @J2): the nav condition
                // reads their sub-rows — a milestoned mid table filters by
                // its OWN milestoning against the CHAIN's context (engine
                // applyMilestoningFilters stamps every milestoned join-tree
                // node with the ambient date; the TARGET class's temporality
                // never governs the mid table — audit 14 F1: keying mid
                // slots by target class left them unstamped for
                // non-temporal targets). Two chains claiming one slot with
                // DIFFERENT specs is loud — first-writer-wins would stamp
                // the second chain's rows with the wrong date.
                for (TypedSpec b : navE.getValue().predicate().body()) {
                    for (String slot : slotAliases) {
                        if (Pipelines.referencesAliasOn(b,
                                navE.getValue().predicate().parameters().get(0),
                                Set.of(slot))) {
                            String priorChain = midPrefixToChain
                                    .putIfAbsent(slot + "_", chain);
                            if (priorChain != null && !priorChain.equals(chain)
                                    && !Objects.equals(
                                            temporal.spec(priorChain),
                                            temporal.spec(chain))) {
                                throw new NotImplementedException(
                                        "physical slot '" + slot + "' is shared"
                                        + " by chains '" + priorChain + "' and '"
                                        + chain + "' carrying different"
                                        + " milestoning dates — per-chain mid"
                                        + " joins are not supported yet");
                            }
                            midPrefixToDim.putIfAbsent(slot + "_",
                                    temporal.temporalStrategy(tg2.classFqn()));
                        }
                    }
                }
            }
        }
        final TypedSpec basePipe =
                temporal.applyJoinTemporalFilters(m.pipeline(), cs, navPrefixToClass,
                        navPrefixToChain, midPrefixToChain, midPrefixToDim);
        m = new Pipelines.Materialized(basePipe, m.slotPrefixes(), m.stripped());
        final TypedSpec materializedPipe;
        if (g.versionSweep()) {
            // allVersions(): the RAW extent — every version row, no filter.
            // allVersionsInRange(s, e): versions whose validity window
            // overlaps the range (engine getTemporalMilestoneRangeFilter).
            materializedPipe = g.milestoning().isEmpty() ? basePipe
                    : temporal.rangeMilestonedPipe(basePipe, g.milestoning().get(0),
                            g.milestoning().get(1), g.classFqn());
        } else if (g.milestoning().size() == 2
                && "bitemporal".equals(temporal.temporalStrategy(g.classFqn()))) {
            // BI-TEMPORAL fetch: .all(processingDate, businessDate) — real
            // pure's getAll(Class, processingDate, businessDate) signature;
            // both dimensions filter.
            materializedPipe = temporal.milestonedPipeByStrategy(
                    temporal.milestonedPipeByStrategy(basePipe, g.milestoning().get(0),
                            "processingtemporal", g.classFqn()),
                    g.milestoning().get(1), "businesstemporal", g.classFqn());
        } else if (g.milestoning().size() == 2) {
            // SINGLE-dimension class with two dates: the RANGE fetch —
            // engine getAll(Class, start, end), same filter as
            // allVersionsInRange
            materializedPipe = temporal.rangeMilestonedPipe(basePipe,
                    g.milestoning().get(0), g.milestoning().get(1), g.classFqn());
        } else {
            materializedPipe = g.milestoning().isEmpty()
                    ? basePipe
                    : temporal.milestonedPipe(basePipe, g.milestoning().get(0), g.classFqn());
        }

        return new RootPipe(m, materializedPipe);
    }


    /** PHASE 2b-ii output: the pipeline with association joins folded
     * (descriptor -> emission, first-demand order) plus the aggregated-
     * navigation materials the fold and substitution both consume. */
    private record JoinedPipe(Pipelines.Materialized m,
            List<AssociationJoins.AssocJoin> aggAssocJoins,
            Map<TypedSpec, Substitution.AggRead> aggReads) {}

    /** PHASE 2b-ii — fold the association joins and the aggregated-
     * navigation grouped subselects onto the materialized pipeline. */
    private JoinedPipe foldAssociationJoins(ClassSource cs,
            Pipelines.Materialized m, TypedSpec keyWidenedPipe,
            List<AssociationJoins.AssocJoin> assocJoins,
            Map<String, AssociationJoins.AssocJoin> aggMaterials,
            Map<String, List<AggDemand>> aggDemands) {
        // 2b. Materialize the association joins (descriptor -> emission,
        //     first-demand order) onto the pipeline.
        TypedSpec withJoins = keyWidenedPipe;
        for (AssociationJoins.AssocJoin aj : assocJoins) {
            TypedSpec joinTarget = aj.targetPipeline();
            Type.RelationType joinTargetRow = aj.targetRow();
            TypedLambda joinCond = aj.condition();
            if (aj.corrSubPred() != null) {
                CorrelatedSubselects.ExplodingSub ex =
                        corrSubs.explodingSubselect(cs, aj,
                                (Type.RelationType) withJoins.info().type());
                joinTarget = ex.target();
                joinTargetRow = ex.row();
                joinCond = ex.cond();
            }
            Type.RelationType leftRow =
                    (Type.RelationType)
                            withJoins.info().type();
            List<Type.Column> cols =
                    new ArrayList<>(leftRow.columns());
            for (Type.Column c
                    : joinTargetRow.columns()) {
                cols.add(new Type.Column(
                        aj.prefix() + c.name(), c.type(), c.multiplicity()));
            }
            withJoins = new TypedJoin(withJoins,
                    joinTarget, leftKind(), joinCond,
                    Optional.of(aj.prefix()),
                    new ExprType(
                            new Type.RelationType(cols),
                            com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
        }
        // 2c. AGGREGATED navigations (the engine's subAggregation shape):
        // per to-many head, ONE grouped subselect — the target pipeline
        // grouped by the association's target-side equi-key columns (names
        // preserved, so the association condition joins it VERBATIM), one
        // aggregate column per demand. Each aggregate node then reads its
        // column off the joined row; no row explosion reaches the
        // projection, and the aggregate itself runs IN the database.
        Map<TypedSpec, Substitution.AggRead> aggReads =
                new IdentityHashMap<>();
        List<AssociationJoins.AssocJoin> aggAssocJoins = new ArrayList<>();
        for (var entry : aggDemands.entrySet()) {
            String head = entry.getKey();
            Set<String> leaves = new LinkedHashSet<>();
            for (AggDemand d : entry.getValue()) {
                leaves.addAll(d.demandLeaves());
            }
            AssociationJoins.AssocJoin aj = aggMaterials.get(head);
            aggAssocJoins.add(aj);
            // #69 THE CORRELATED-AGGREGATE SUBSELECT (engine parent-copy
            // architecture): a correlated pred's parent-nav reads can
            // never resolve in the outer ON — the subselect re-joins the
            // PARENT extent (with the navs the pred demands), filters by
            // the pred over the joined row, groups by the PARENT-side
            // equi keys, and joins back on key equality.
            TypedLambda corrAgg = synthetics.correlatedPred(head);
            CorrelatedSubselects.CorrAggSub cas =
                    corrSubs.corrAggSubSource(cs, head, aj, corrAgg);
            String corrRowVar = cas.rowVar();
            String corrTp = cas.targetPrefix();
            Type.RelationType corrJoinedRow = cas.joinedRow();
            TypedSpec subSource = cas.subSource();
            List<String> keyCols = cas.keyCols();
            Type.RelationType keyRow = cas.keyRow();
            CorrelatedSubselects.ParentCopy pc = cas.pc();
            List<TypedGroupBy.GroupKey>
                    keys = new ArrayList<>();
            List<Type.Column>
                    subCols = new ArrayList<>();
            for (String k : keyCols) {
                var col = keyRow.columns().stream()
                        .filter(c -> c.name().equals(k)).findFirst()
                        .orElseThrow(() -> new IllegalStateException(
                                "resolver bug: equi-key column '" + k
                                        + "' missing from the "
                                        + "grouping row"));
                keys.add(new TypedGroupBy.GroupKey(
                        k, Optional.empty()));
                subCols.add(col);
            }
            List<TypedAggCol> aggs =
                    new ArrayList<>();
            int ord = 0;
            var targetRowType = new ExprType(
                    aj.targetRow(), com.legend.compiler.element.type.Multiplicity
                            .Bounded.ONE);
            for (AggDemand d : entry.getValue()) {
                String alias = "agg_" + ord++;
                aggs.add(corrSubs.aggColFor(cs, head, aj, d, alias,
                        corrAgg, corrTp, corrRowVar, corrJoinedRow,
                        cas.pc()));
                subCols.add(new Type.RelationType
                        .Column(alias, d.node().info().type(),
                        com.legend.compiler.element.type.Multiplicity
                                .Bounded.ZERO_ONE));
            }
            var subRow = new Type.RelationType(subCols);
            TypedSpec sub = new TypedGroupBy(
                    subSource, keys, aggs,
                    new ExprType(subRow,
                            com.legend.compiler.element.type.Multiplicity
                                    .Bounded.ONE));
            String prefix = AssociationJoins.prefixFor(head + "_agg", cs);
            Type.RelationType leftRow =
                    (Type.RelationType)
                            withJoins.info().type();
            List<Type.Column>
                    cols = new ArrayList<>(leftRow.columns());
            for (var c : subRow.columns()) {
                cols.add(new Type.RelationType
                        .Column(prefix + c.name(), c.type(), c.multiplicity()));
            }
            // A JOINED-ROW sub (correlated OR chained parent-copy shape,
            // targetPrefix set) already carries the association condition
            // INSIDE — the outer joins back on parent-key equality only.
            TypedLambda backCond = cas.targetPrefix() == null ? aj.condition()
                    : assocMaterial.pkEqualityCond(keyCols,
                            (Type.RelationType)
                                    withJoins.info().type(), subRow);
            withJoins = new TypedJoin(withJoins,
                    sub, leftKind(), backCond,
                    Optional.of(prefix),
                    new ExprType(
                            new Type.RelationType(cols),
                            com.legend.compiler.element.type.Multiplicity
                                    .Bounded.ONE));
            ord = 0;
            for (AggDemand d : entry.getValue()) {
                aggReads.put(d.node(), new Substitution.AggRead(
                        prefix + "agg_" + ord++,
                        CorrelatedSubselects.isCountFamily(d.node())));
            }
        }
        m = new Pipelines.Materialized(withJoins, m.slotPrefixes(), m.stripped());

        return new JoinedPipe(m, aggAssocJoins, aggReads);
    }

    /** PHASE — to-many/navigate heads under exists/isEmpty: the
     * correlated-EXISTS material per head (target pipeline + oriented
     * condition, NO join emitted; the positional rule table §133). */
    /** ops + the terminal: value-position filtered navigation lives in
     * the map/project TERMINAL ($f.emps->filter(..).name — the qualifier
     * family), and its inner predicates demand target slots too. */
    private static List<TypedSpec> withTerminal(List<TypedSpec> ops,
            TypedSpec top) {
        List<TypedSpec> out = new ArrayList<>(ops);
        out.add(top);
        return out;
    }

    private Map<String, Substitution.ExistsSub> registerExistsSubs(
            ClassSource cs, Set<List<String>> paths,
            Set<List<String>> filterPaths, List<TypedSpec> ops,
            Context context, Map<String, Substitution.AssocSub> parentAssocs) {
        Map<String, Substitution.ExistsSub> existsSubs = new LinkedHashMap<>();
        for (List<String> path : paths) {
            String head = path.get(0);
            boolean filterTwoHop = path.size() == 2 && filterPaths.contains(path);
            if ((path.size() != 1 && !filterTwoHop) || existsSubs.containsKey(head)) {
                continue;
            }
            if (cs.bindings().containsKey(SyntheticHeads.realHead(head))) {
                // A NAVIGATE-SLOT head (class-typed Join PM): the nav step
                // carries the target extent + oriented (s, t) predicate —
                // the same correlated-EXISTS material as an association end.
                var nav = Pipelines.navSteps(cs.pipeline()).get(SyntheticHeads.realHead(head));
                if (nav == null || !(nav.target()
                        instanceof TypedGetAll tg)
                        // eager material only when the target class IS mapped
                        // here (an M2M chain's nav target lives upstream —
                        // registration must not throw for a rewrite that may
                        // never fire)
                        || !sources.binds(cs.mappingFqn(), tg.classFqn())) {
                    continue;
                }
                ClassSource t = sources.get(cs.mappingFqn(), tg.classFqn());
                Set<String> tSlots0 = Pipelines.slotAliases(t.pipeline());
                Set<String> tDemand0 = new LinkedHashSet<>();
                Set<String> innerLeaves = new LinkedHashSet<>(
                        InnerDemand.leaves(ops, head));
                for (TypedLambda liftedPred0 : synthetics.allPreds(head)) {
                    for (TypedSpec b : liftedPred0.body()) {
                        collectParamPathHeads(b,
                                liftedPred0.parameters().get(0), innerLeaves);
                    }
                }
                for (String leaf : innerLeaves) {
                    TypedSpec lb = t.bindings().get(leaf);
                    if (lb != null) {
                        CorrelatedSubselects.collectAliasReads(lb, t.rowVar(), tSlots0, tDemand0);
                    }
                }
                tDemand0 = Pipelines.closeOverConditions(t.pipeline(), tDemand0);
                // #69: a CORRELATED pred's TARGET-side reads may hop the
                // target's OWN class-typed navigate steps ($e.address.name
                // over the navigated rows) — demand those steps, let the
                // materialization join them, and build depth-1 SubNavs for
                // the composition's pass-1 dispatch. Deeper hops stay loud
                // (empty children).
                TypedLambda corrNav0 = synthetics.correlatedPred(head);
                Map<String, String> predNavAliases = new LinkedHashMap<>();
                Set<String> tNavDemand = new LinkedHashSet<>();
                {
                    var tNavSteps = Pipelines.navSteps(t.pipeline());
                    Set<List<String>> predPaths0 = new LinkedHashSet<>();
                    if (corrNav0 != null) {
                        for (TypedSpec b : corrNav0.body()) {
                            consumedPaths(b, corrNav0.parameters().get(0),
                                    predPaths0);
                        }
                    }
                    // CLOSED parked preds read target navs too ($e.address
                    // .name — the Fork family): demand their steps for the
                    // pred substitution's SubNav dispatch (identity dedup
                    // at the lift keeps the join count engine-equal).
                    for (TypedLambda cp : synthetics.allPreds(head)) {
                        for (TypedSpec b : cp.body()) {
                            consumedPaths(b, cp.parameters().get(0),
                                    predPaths0);
                        }
                    }
                    for (List<String> pp : predPaths0) {
                        if (pp.size() < 2) {
                            continue;
                        }
                        TypedSpec hb = t.bindings().get(pp.get(0));
                        String al = hb == null ? null
                                : navSlotAlias(hb, t.rowVar(), tNavSteps.keySet());
                        if (al != null) {
                            tNavDemand.add(al);
                            predNavAliases.put(pp.get(0), al);
                        }
                    }
                }
                Pipelines.Materialized tMat0 = tNavDemand.isEmpty()
                        ? Pipelines.materialize(
                                t.pipeline(), tDemand0, t.classFqn())
                        : Pipelines.materialize(
                                t.pipeline(), tDemand0, tNavDemand, t.classFqn(),
                                (al2, tc2) -> Pipelines.materialize(
                                        sources.get(cs.mappingFqn(), tc2)
                                                .pipeline(),
                                        java.util.Set.of(), tc2).pipeline());
                Map<String, Substitution.SubNav> tSubNavs = new LinkedHashMap<>();
                for (var pne : predNavAliases.entrySet()) {
                    String pfx = tMat0.slotPrefixes().get(pne.getValue());
                    var stepT = Pipelines.navSteps(t.pipeline())
                            .get(pne.getValue()).target();
                    if (pfx == null || !(stepT instanceof TypedGetAll stg)) {
                        continue;
                    }
                    ClassSource sub = sources.get(cs.mappingFqn(), stg.classFqn());
                    tSubNavs.put(pne.getKey(), new Substitution.SubNav(
                            pfx, sub.rowVar(), sub.bindings()));
                }
                // UNION target: member threads carry the key columns the
                // navigate predicate binds on (mirrors the assoc route)
                TypedSpec tPipe0 = tMat0.pipeline();
                if (nav.predicate().parameters().size() == 2) {
                    Set<String> tgtReads = new LinkedHashSet<>();
                    for (TypedSpec b : nav.predicate().body()) {
                        Pipelines.collectVarReads(b,
                                nav.predicate().parameters().get(1), tgtReads);
                    }
                    tPipe0 = Pipelines.widenConcatenateForKeys(tPipe0, tgtReads);
                }
                TypedSpec tTemporal = temporal.temporalTargetPipe(cs, t, head,
                        temporal.applyJoinTemporalFilters(tPipe0, t, Map.of()));
                final ClassSource ft = t;
                // a CORRELATED lifted pred composes into the nav step's own
                // (parent, target) condition — both rows in scope, the same
                // composition as the association route
                TypedLambda navCond = nav.predicate();
                TypedLambda corrNav = corrNav0;
                if (corrNav != null) {
                    navCond = assocMaterial.andCorrelatedIntoCondition(
                            navCond, corrNav, cs, t, tMat0.slotPrefixes(),
                            parentAssocs, tSubNavs);
                }
                final Map<String, Substitution.SubNav> ftSubNavs = tSubNavs;
                tTemporal = synthetics.applyToPipe(head, tTemporal,
                        (p, pred) -> CorrelatedSubselects.predFilteredPipe(p, ft,
                                tMat0.slotPrefixes(), ftSubNavs,
                                pred, cs.mappingFqn()));
                Pipelines.Materialized tMat = new Pipelines.Materialized(
                        tTemporal, tMat0.slotPrefixes(), tMat0.stripped());
                boolean navToMany = !(ctx.findProperty(cs.classFqn(), SyntheticHeads.realHead(head))
                        .map(pr -> pr.multiplicity())
                        .filter(mm -> mm instanceof com.legend.compiler.element.type
                                .Multiplicity.Bounded bb
                                && Integer.valueOf(1).equals(bb.upper()))
                        .isPresent());
                // #70 COMPOSITE chain-backed target (the tree family): the
                // navigate step's condition reads a SIBLING JOINSLOT
                // sub-row on the parent ($s.<optSlot>.ancestor == $t.id) —
                // unresolvable on the outer correlation row (demanding the
                // slot at parent level explodes 1:N hops — probed).
                // ENGINE: the subselect contains BOTH tables, correlated
                // OUTWARD by hop-1's condition only. The composite joins
                // the slot's table INTO the target pipeline; the oriented
                // condition becomes hop-1's, its target reads landing on
                // the composite's prefixed slot columns.
                TypedSpec chainPipe = tMat.pipeline();
                if (corrNav == null && navCond.parameters().size() == 2) {
                    CorrelatedSubselects.CompositeChain cc =
                            corrSubs.compositeChainTarget(
                                    cs, navCond, chainPipe);
                    if (cc != null) {
                        chainPipe = cc.pipeline();
                        navCond = cc.orientedCond();
                    }
                }
                NestedScope navNs = nestedScope(t, ops, head, context,
                        chainPipe);
                existsSubs.put(head, new Substitution.ExistsSub(navNs.pipeline(),
                        navCond, t.rowVar(), t.bindings(),
                        navNs.row(),
                        t.classFqn(), Pipelines.slotAliases(t.pipeline()),
                        tMat0.slotPrefixes(), navToMany)
                        .withInnerRegs(navNs.regs()));
                continue;
            }
            var assocOpt = ctx.findAssociationOf(cs.classFqn(), SyntheticHeads.realHead(head));
            if (assocOpt.isEmpty()) {
                continue;   // not an association — plain unmapped (loud later)
            }
            // ANY multiplicity: the EXISTS material is consumed only under
            // emptiness calls (plan rule: class-typed isEmpty/isNotEmpty of
            // any multiplicity, incl. to-one-optional, => [NOT] EXISTS); a
            // bare head not under an emptiness call still gets the honest
            // H4 story at substitution.
            AssociationJoins.AssocJoin aj = assocMaterial.associationJoin(temporal, cs, head, context, true,
                    InnerDemand.leaves(ops, head));
            var assocEnd = assocOpt.get().property1().propertyName()
                    .equals(SyntheticHeads.realHead(head))
                    ? assocOpt.get().property1() : assocOpt.get().property2();
            boolean isToMany = !assocEnd.isToOne();
            // the SCALAR (slot-undemanded) pipeline serves value-position
            // consumers (filteredNavLeafRead): other consumers' slot demand
            // must not fan a single-row subquery out (audit 13 B3)
            // B3 (scalar fan-out) DEFERRED: a separate slot-undemanded
            // scalar pipeline regressed real corpus value-leaf reads
            // (testConstraintTargetingMultipleJoinsInPropertyMapping); the
            // engineered fan-out shape stays data-dependent-loud for now —
            // the plumbing (scalarPipeline field) is in place for the fix.
            NestedScope assocNs = nestedScope(aj.target(), ops, head, context,
                    aj.targetPipeline());
            existsSubs.put(head, new Substitution.ExistsSub(assocNs.pipeline(),
                    aj.condition(), aj.target().rowVar(), aj.target().bindings(),
                    assocNs.row(), aj.target().classFqn(),
                    Pipelines.slotAliases(aj.target().pipeline()),
                    aj.targetSlotPrefixes(), isToMany)
                    .withInnerRegs(assocNs.regs()));
        }

        return existsSubs;
    }

    /** PHASE output: the association-route joins (one LEFT join per
     * hop, deduped by chain key) plus per-chain leaf demand. */
    private record AssocPlan(List<AssociationJoins.AssocJoin> assocJoins,
            Map<String, AssociationJoins.AssocJoin> joinsByChain,
            Map<String, Set<String>> leavesByChain) {}

    /** PHASE — association demand (paths whose head is NOT a binding):
     * one LEFT join per hop chained by path prefix, plus the SECOND
     * head identities on shared physical slots (2a-x: per-use extra
     * joins from the same nav material). */
    private AssocPlan registerAssociationJoins(ClassSource cs,
            Set<List<String>> paths, Context context,
            Map<String, TypedNavigate> navSteps,
            Map<String, String> extraNavHeads,
            Map<String, List<List<String>>> extraNavTails,
            Map<String, Substitution.AssocSub> assocs,
            Map<String, String> corrNavHeads,
            Map<String, List<List<String>>> navTailsByAlias) {
        List<AssociationJoins.AssocJoin> assocJoins = new ArrayList<>();
        Map<String, AssociationJoins.AssocJoin> joinsByChain = new LinkedHashMap<>();
        // Per chain-prefix leaf demand: for [firm, country], hop 'firm'
        // must materialize firm's OWN slots feeding 'country'.
        Map<String, Set<String>> leavesByChain = new LinkedHashMap<>();
        for (List<String> path : paths) {
            for (int i = 0; i + 1 < path.size(); i++) {
                leavesByChain.computeIfAbsent(String.join(".", path.subList(0, i + 1)),
                        k -> new LinkedHashSet<>()).add(path.get(i + 1));
            }
        }
        for (List<String> path : paths) {
            if (path.size() < 2
                    || cs.bindings().containsKey(SyntheticHeads.realHead(path.get(0)))) {
                continue;   // 1-hop, or embedded/slot heads (substitution-side)
            }
            String head = path.get(0);
            // EVERY to-many crossing joins with ROW EXPLOSION — filter
            // position included (engine testInNegated golden: a bare LEFT
            // JOIN whose surviving rows DUPLICATE the parent value; the
            // distinct-subselect semi-join is reserved for EXPLICIT
            // exists/isEmpty calls, whose paths are 1-hop and never reach
            // this loop). AUDIT 9: the previous filter-only EXISTS diversion
            // was set-correct but cardinality-wrong.
            ClassSource parent = cs;
            String parentPrefix = "";
            // $p.assoc.milestoning.from: the struct is a COLUMN read on the
            // assoc target, not a further hop. Audit 23 B3: the GENERATED
            // struct exists only on a TEMPORAL hop class with no DECLARED
            // property of that name (declared wins — audit 10); a user
            // property literally named 'milestoning' keeps its hop.
            int effectiveSize = path.size();
            if (path.size() >= 2
                    && path.get(path.size() - 2).equals("milestoning")) {
                String hopCls = classAtHop(cs, path, path.size() - 2);
                if (hopCls != null
                        && com.legend.compiler.element.Temporal
                                .strategyOf(ctx, hopCls) != null
                        && ctx.findProperty(hopCls, "milestoning").isEmpty()) {
                    effectiveSize = path.size() - 1;
                }
            }
            for (int hop = 0; hop + 1 < effectiveSize; hop++) {
                String chainKey = String.join(".", path.subList(0, hop + 1));
                AssociationJoins.AssocJoin known = joinsByChain.get(chainKey);
                if (known != null) {
                    parent = known.target();
                    parentPrefix = known.prefix();
                    continue;
                }
                if (hop > 0 && synthetics.hasPred(path.get(hop))
                        && synthetics.correlatedPred(path.get(hop)) != null) {
                    // a CORRELATED pred on a chained MID hop: the
                    // parent-copy reroute serves only hop-0 heads so far —
                    // loud until the chained variant is built. CLOSED
                    // preds fall through: associationJoin parks them on
                    // the hop's target pipeline (engine golden
                    // testQualifierInLambdaDeep — the filtered subselect
                    // joins the chain).
                    throw new com.legend.error.NotImplementedException(
                            "correlated filtered navigation as a chained"
                            + " association hop ('"
                            + SyntheticHeads.realHead(path.get(hop))
                            + "' at '" + chainKey + "') is not supported yet");
                }
                AssociationJoins.AssocJoin aj = assocMaterial.associationJoin(temporal, parent, path.get(hop), context, false,
                        leavesByChain.getOrDefault(chainKey, Set.of()), chainKey);
                if (hop > 0 && containsConcatenate(aj.targetPipeline())) {
                    // a CHAINED hop into a UNION-mapped target needs
                    // per-member routed conditions (V4's z[y0,z0]/z[y1,z1]
                    // pairs) — the single predicate returns PARTIAL rows;
                    // stays loud until that rung is built
                    throw new com.legend.error.NotImplementedException(
                            "chained association hop '" + chainKey
                            + "' navigates INTO a union-mapped class — "
                            + "per-member route dispatch is not built yet");
                }
                if (hop > 0) {
                    // A CHAINED hop: the parent's columns live PREFIXED on the
                    // accumulated joined row — re-point the condition's LEFT
                    // param reads (dept's raw $d.ID becomes dept_ID), and the
                    // hop's own prefix extends the chain (dept_org_) with the
                    // SAME collision guard hop 0 gets (audit: a physical
                    // dept.org_id FK would collide with the chained prefix).
                    String chainPrefix = AssociationJoins.chainedPrefix(
                            parentPrefix + path.get(hop), cs, joinsByChain);
                    final String pp2 = parentPrefix;
                    TypedLambda cond = aj.condition();
                    List<Type.Column>
                            leftCols = new ArrayList<>();
                    for (Type.Column c
                            : ((Type.RelationType)
                                    parent.rowType()).columns()) {
                        leftCols.add(new Type.Column(
                                pp2 + c.name(), c.type(), c.multiplicity()));
                    }
                    var leftRow = new Type.RelationType(leftCols);
                    String leftParam = cond.parameters().get(0);
                    TypedSpec body = Pipelines.prefixColumns(
                            cond.body().get(cond.body().size() - 1), leftParam, pp2,
                            v -> new TypedVariable(leftParam,
                                    new ExprType(leftRow,
                                            com.legend.compiler.element.type.Multiplicity
                                                    .Bounded.ONE)));
                    cond = new TypedLambda(cond.parameters(), List.of(body),
                            cond.info());
                    aj = new AssociationJoins.AssocJoin(chainPrefix, aj.target(), aj.targetPipeline(),
                            aj.targetRow(), cond, aj.targetSlotPrefixes());
                }
                assocJoins.add(aj);
                joinsByChain.put(chainKey, aj);
                assocs.put(chainKey, new Substitution.AssocSub(aj.prefix(),
                        aj.target().rowVar(), aj.target().bindings(),
                        aj.target().classFqn(),
                        Pipelines.slotAliases(aj.target().pipeline()),
                        aj.targetSlotPrefixes(), null, null,
                        temporal.milestoneColumnsOf(aj.target().pipeline(),
                                aj.target().classFqn())));
                parent = aj.target();
                parentPrefix = aj.prefix();
            }
        }

        // 2a-x. SECOND head identities on one physical slot: an extra
        // prefixed join per identity, from the SAME nav material — the
        // dotted chainPrefix keys its own temporal spec, a lifted
        // predicate parks inside the target (the slot route's exact
        // semantics, join identity aside).
        for (var extra : extraNavHeads.entrySet()) {
            String headKey = extra.getKey();
            String alias = extra.getValue();
            var nav = navSteps.get(alias);
            String targetClass = ((TypedGetAll)
                    nav.target()).classFqn();
            ClassSource target = sources.get(cs.mappingFqn(), targetClass);
            NavMaterializer.NavMat mat = navMaterializer.navTargetMaterialized(temporal, cs.mappingFqn(),
                    targetClass,
                    extraNavTails.getOrDefault(headKey, List.of()),
                    headKey, null);
            // the slot route's root stamp comes from the outer join-walk
            // (navPrefixToChain); an extra join never passes it — stamp
            // here, exactly the association route's emission
            TypedSpec tPipe = temporal.temporalTargetPipe(cs, target, headKey,
                    temporal.applyJoinTemporalFilters(mat.pipeline(), target,
                            Map.of()));
            // preds park under the BARE synthetic component — a dotted
            // chain key must strip to it here or the identity's pred is
            // silently dropped (unfiltered join = wrong values; the
            // isolation 'filters without alias' family)
            String exPredKey = headKey.substring(headKey.lastIndexOf('.') + 1);
            requireNoCorrelatedPred(exPredKey, "navigate-step chain");
            tPipe = synthetics.applyToPipe(exPredKey, tPipe, (p, pred) ->
                    CorrelatedSubselects.predFilteredPipe(p, target, mat.slotPrefixes(),
                            mat.subNavs(), pred, cs.mappingFqn()));
            AssociationJoins.AssocJoin aj = new AssociationJoins.AssocJoin(AssociationJoins.prefixFor(headKey, cs), target, tPipe,
                    (Type.RelationType)
                            tPipe.info().type(),
                    nav.predicate(), mat.slotPrefixes());
            assocJoins.add(aj);
            assocs.put(headKey, new Substitution.AssocSub(aj.prefix(),
                    target.rowVar(), target.bindings(), target.classFqn(),
                    Pipelines.slotAliases(target.pipeline()),
                    mat.slotPrefixes(), null, null,
                    temporal.milestoneColumnsOf(target.pipeline(), target.classFqn()),
                    mat.subNavs()));
        }

        // 2a-c. #69 CORRELATED-slot reroute: heads whose correlated pred
        // demands a parent NAV left the slot spine (registerNavigations) —
        // each joins as an AssocJoin carrying the pred for the fold's
        // exploding parent-copy subselect. The head's CLOSED preds still
        // apply in-target (applyToPipe passes corr-only heads through).
        for (var ch : corrNavHeads.entrySet()) {
            String headKey = ch.getKey();
            String alias = ch.getValue();
            var nav = navSteps.get(alias);
            String targetClass = ((TypedGetAll)
                    nav.target()).classFqn();
            ClassSource target = sources.get(cs.mappingFqn(), targetClass);
            NavMaterializer.NavMat mat = navMaterializer.navTargetMaterialized(
                    temporal, cs.mappingFqn(), targetClass,
                    navTailsByAlias.getOrDefault(alias, List.of()),
                    headKey, null);
            TypedSpec tPipe = temporal.temporalTargetPipe(cs, target, headKey,
                    temporal.applyJoinTemporalFilters(mat.pipeline(), target,
                            Map.of()));
            tPipe = synthetics.applyToPipe(headKey, tPipe, (p, pred) ->
                    CorrelatedSubselects.predFilteredPipe(p, target, mat.slotPrefixes(),
                            mat.subNavs(), pred, cs.mappingFqn()));
            AssociationJoins.AssocJoin aj = new AssociationJoins.AssocJoin(
                    AssociationJoins.prefixFor(headKey, cs), target, tPipe,
                    (Type.RelationType)
                            tPipe.info().type(),
                    nav.predicate(), mat.slotPrefixes(), mat.subNavs(),
                    synthetics.correlatedPred(headKey));
            assocJoins.add(aj);
            joinsByChain.put(headKey, aj);
            assocs.put(headKey, new Substitution.AssocSub(aj.prefix(),
                    target.rowVar(), target.bindings(), target.classFqn(),
                    Pipelines.slotAliases(target.pipeline()),
                    mat.slotPrefixes(), null, null,
                    temporal.milestoneColumnsOf(target.pipeline(), target.classFqn()),
                    mat.subNavs()));
        }

        return new AssocPlan(assocJoins, joinsByChain, leavesByChain);
    }

    /** Phase 1 output: the collected object-space chain — the (lifted)
     * terminal, the op stack down to the getAll, the effective execution
     * context after in-chain from() re-scoping, and the bound source.
     * Field side-effects (temporal.root(), temporalByHead reset) happen in
     * {@link #collectOpChain} — one construction site. */
    private record OpChain(TypedSpec top, List<TypedGraphTree> tree,
            boolean implicitSerialize, List<TypedSpec> ops, TypedGetAll getAll,
            Context context, ClassSource cs,
            Map<String, Substitution.AssocSub> flattenAssocs) {}

    /** PHASE 1 — collect the op chain (terminal detection, native-shape
     * normalization, from() re-scoping), validate the fetch, construct
     * THE root temporal context, bind the class source. */
    private OpChain collectOpChain(TypedSpec top, Context context) {
        // PRE-REWRITE (before the demand scan, ledger design): filtered
        // navigations consumed as bare collections lift into SYNTHETIC
        // 2-hop heads whose join target carries the predicate.
        final Context canonCtx = context;
        synthetics.setCanonicalizer(nn -> corrSubs.subTypeNavCastCanon(nn,
                fqn -> dispatch(canonCtx, fqn), isNotEmptyCallee()));
        top = synthetics.liftFilteredHeads(top);
        // The relation-shaping TERMINAL: project or class-source groupBy
        // (lambdas through the one funnel), or the GRAPH terminals —
        // explicit serialize (graphFetch is source-preserving; serialize's
        // tree governs) and every other class-shaped root, which is the
        // IMPLICIT serialize over the class's scalar bindings (plan §E10).
        List<TypedGraphTree> tree = null;   // non-null => graph terminal
        boolean implicitSerialize = false;
        Context chainContext = context;     // an in-chain from() re-scopes
        TypedSpec cur;
        if (top instanceof TypedSerialize sz) {
            tree = sz.tree();
            cur = sz.source() instanceof TypedGraphFetch gf ? gf.source() : sz.source();
        } else if (top instanceof TypedProject t) {
            cur = t.source();
        } else if (top instanceof TypedGroupBy t) {
            cur = t.source();
        } else {
            implicitSerialize = true;
            cur = top;
        }
        // 1. Collect the below-boundary op chain (top-down) to the getAll.
        List<TypedSpec> ops = new ArrayList<>();
        String flattenHop = null;
        while (!(cur instanceof TypedGetAll)) {
            // Normalize collection natives with relation shapes BEFORE
            // collecting: first()/head() IS limit 1; class-space
            // sort(key, comparator) IS sortBy with a direction.
            if (cur instanceof TypedNativeCall nc && isClassDistinct(nc)) {
                // instance distinct = dedup by the SERIALIZED VALUE. Over a
                // single-table extent rows are pk-unique and DISTINCT is a
                // no-op; over a UNION/concatenate extent duplicates are
                // REAL (audit 19d B7 — the old drop-the-node assumption
                // pushed this to a host-side JSON dedup in the harness).
                // Empty column list = the whole materialized row (§A.6).
                cur = new TypedDistinct(nc.args().get(0), List.of(), nc.info());
                continue;
            }
            if (cur instanceof TypedNativeCall nc && isFirstLike(nc)) {
                cur = new TypedLimit(nc.args().get(0),
                        new TypedCInteger(1L, com.legend.compiler.element.type
                                .ExprType.one(com.legend.compiler.element.type
                                        .Type.Primitive.INTEGER)),
                        nc.info());
                continue;
            }
            if (cur instanceof TypedNativeCall nc && isClassToOne(nc)) {
                cur = nc.args().get(0);
                continue;
            }
            if (cur instanceof TypedNativeCall nc && isStaticAt(nc)) {
                // at(k) over instances = the k-th row: slice(k, k+1)
                long k = ((TypedCInteger)
                        nc.args().get(1)).value().longValue();
                cur = new TypedSlice(nc.args().get(0),
                        new TypedCInteger(k, com.legend.compiler.element.type
                                .ExprType.one(com.legend.compiler.element.type
                                        .Type.Primitive.INTEGER)),
                        new TypedCInteger(k + 1, com.legend.compiler.element.type
                                .ExprType.one(com.legend.compiler.element.type
                                        .Type.Primitive.INTEGER)),
                        nc.info());
                continue;
            }
            TypedSortBy asSort = classSortOf(cur);
            if (asSort != null) {
                cur = asSort;
                continue;
            }
            // an in-chain from() re-scopes the execution context for the
            // rest of the walk and contributes NO op
            if (cur instanceof TypedFrom fr) {
                if (fr.mapping().isPresent()) {
                    context = Context.ofMapping(fr.mapping().get().fullPath());
                } else if (fr.runtime().isPresent()) {
                    context = Context.ofRuntime(fr.runtime().get().fullPath());
                }
                cur = fr.source();
                continue;
            }
            // ->map(f|$f.assocEnd->...) with a CLASS-result mapper: the
            // flatten IS the mapper body with the source spliced for the
            // param (flatten composition is associative) - normalize and
            // keep walking.
            if (cur instanceof TypedMap cm
                    && ((Type.FunctionType) cm.mapper().info().type()).result()
                            .type() instanceof Type.ClassType) {
                cur = substituteParam(cm.mapper(), cm.source());
                continue;
            }
            // CLASS-TERMINAL ASSOCIATION HOP: the flatten boundary — the
            // chain re-roots at the target over the JOIN. EMBEDDED class
            // hops never reach this loop: consumers compose them into the
            // reading lambda (the funnel's embedded dispatch owns them).
            if (cur instanceof TypedPropertyAccess hp
                    && hp.info().type() instanceof Type.ClassType
                    && hp.source().info().type() instanceof Type.ClassType oc) {
                if (ctx.findAssociationOf(oc.fqn(), hp.property()).isEmpty()) {
                    throw new NotImplementedException("embedded class hop '"
                            + hp.property() + "' in CHAIN position without a"
                            + " scalar consumer is not supported yet");
                }
                if (flattenHop != null) {
                    throw new NotImplementedException("multi-hop class flatten ('"
                            + hp.property() + "." + flattenHop
                            + "') is not supported yet");
                }
                flattenHop = hp.property();
                cur = hp.source();
                continue;
            }
            if (flattenHop != null) {
                throw new NotImplementedException(
                        "a class flatten over a FILTERED/transformed source"
                        + " chain is not supported yet (op below the '"
                        + flattenHop + "' hop)");
            }
            ops.add(cur);
            cur = switch (cur) {
                case TypedFilter f -> f.source();
                case TypedLimit l -> l.source();
                case TypedDrop d -> d.source();
                case TypedSlice sl -> sl.source();
                case TypedSortBy sb -> sb.source();
                // instance removeDuplicates: whole-row DISTINCT (at worst
                // UNDER-dedups on joined helper columns — honest FAIL)
                case TypedDistinct d -> d.source();
                default -> throw new NotImplementedException("object-space operation "
                        + cur.getClass().getSimpleName() + " is not supported yet");
            };
        }
        TypedGetAll g = (TypedGetAll) cur;
        if (g.milestoning().size() > 2) {
            throw new MappingResolutionException("class fetch of '"
                    + g.classFqn() + "' with " + g.milestoning().size()
                    + " milestoning arguments is not supported", g.classFqn());
        }

        if (g.milestoning().isEmpty() && !g.versionSweep()
                && temporal.temporalStrategy(g.classFqn()) != null) {
            // engine: .all() on a temporal class REQUIRES a date argument
            // (allVersions() is the version-sweep spelling) — an unfiltered
            // extent would silently return every version as a row
            throw new MappingResolutionException("fetch of temporal class '"
                    + g.classFqn() + "' requires a milestoning date argument"
                    + " (use allVersions() for the unfiltered extent)",
                    g.classFqn());
        }
        // M3 temporal context: this fetch's dates propagate to same-strategy
        // targets navigated through temporal parents (set per getAll; nested
        // sibling resolutions overwrite at their own entry).
        // audit 10: never read the PREVIOUS getAll's context — a fresh
        // frame per resolution entry, ONE construction site
        temporal = new TemporalFrame(ctx, sources, TemporalContext.NONE,
                Map.of());
        {
            List<TypedSpec> nd =
                    temporal.normalizeContextDates(g.milestoning());
            String rootStrat = temporal.temporalStrategy(g.classFqn());
            TemporalContext rc = TemporalContext.NONE;
            if (g.versionSweep()) {
                // allVersions() = NONE; allVersionsInRange(s, e) = RANGE
                rc = nd.size() == 2
                        ? TemporalContext.range(rootStrat, nd.get(0), nd.get(1))
                        : TemporalContext.NONE;
            } else if (nd.size() == 2 && "bitemporal".equals(rootStrat)) {
                rc = TemporalContext.bitemporal(nd.get(0), nd.get(1));
            } else if (nd.size() == 2) {
                // getAll(Class, start, end) — the allVersionsInRange spelling
                rc = TemporalContext.range(rootStrat, nd.get(0), nd.get(1));
            } else if (nd.size() == 1 && rootStrat != null) {
                rc = TemporalContext.single(rootStrat, nd.get(0));
            }
            temporal = new TemporalFrame(ctx, sources, rc, Map.of());
        }
        final Context fctx = chainContext;
        ClassSource cs = sources.get(dispatch(fctx, g.classFqn()), g.classFqn(),
                target -> dispatch(fctx, target),
                (fctx.explicitMapping() == null ? "" : fctx.explicitMapping())
                        + '\u0000'
                        // audit 23: the KEY must be fctx's runtime — the
                        // old mixed read (null-check fctx, value from
                        // context) could poison the cache across an
                        // in-chain from() rescope
                        + (fctx.runtimeFqn() == null ? "" : fctx.runtimeFqn()));

        Map<String, Substitution.AssocSub> flattenAssocs = new LinkedHashMap<>();
        if (flattenHop != null) {
            cs = flattenSource(cs, flattenHop, fctx, ops, top, flattenAssocs);
        }
        return new OpChain(top, tree, implicitSerialize, ops, g, context, cs,
                flattenAssocs);
    }

    /** PHASE 1b — TWO-DATES-PER-HEAD (engine keys separate joins by
     * date): when ONE chain is navigated with DIFFERENT temporal
     * arguments, each distinct date-set beyond the first renames to a
     * date-fingerprinted synthetic head ('product#d1') — a separate join
     * identity carrying its own spec; SyntheticHeads.realHead() keeps every model lookup
     * transparent. Same-date accesses keep sharing one join
     * (merge-by-identity). Runs BEFORE spec collection so the conflict
     * throw never fires for split chains. Mutates {@code ops} in place;
     * returns the (possibly rewritten) terminal. */
    private TypedSpec splitDatedHeads(List<TypedSpec> ops, TypedSpec top) {
        Map<String, List<com.legend.compiler.spec.typed
                .TypedMilestonedAccess>> datedByChain =
                new LinkedHashMap<>();
        for (TypedSpec op : ops) {
            if (op instanceof TypedFilter f) {
                for (TypedSpec b : f.predicate().body()) {
                    collectDatedNodes(b, f.predicate().parameters().get(0),
                            datedByChain);
                }
            }
            if (op instanceof TypedSortBy sb) {
                for (TypedSpec b : sb.key().body()) {
                    collectDatedNodes(b, sb.key().parameters().get(0),
                            datedByChain);
                }
            }
        }
        if (top instanceof TypedProject || top instanceof TypedGroupBy) {
            for (TypedLambda fn : terminalLambdas(top)) {
                for (TypedSpec b : fn.body()) {
                    collectDatedNodes(b, fn.parameters().get(0), datedByChain);
                }
            }
        }
        IdentityHashMap<TypedSpec, String> dateRenames =
                new IdentityHashMap<>();
        for (var chainDates : datedByChain.entrySet()) {
            Map<TemporalFrame.TemporalSpec, String> byArgs =
                    new LinkedHashMap<>();
            for (var ma : chainDates.getValue()) {
                TemporalFrame.TemporalSpec spec = new TemporalFrame.TemporalSpec(
                        temporal.normalizeContextDates(ma.dates()), ma.sweep());
                String name = byArgs.get(spec);
                if (name == null) {
                    name = byArgs.isEmpty() ? ma.property()
                            : synthetics.mintDateName(ma.property());
                    byArgs.put(spec, name);
                }
                if (!name.equals(ma.property())) {
                    dateRenames.put(ma, name);
                }
            }
        }
        if (!dateRenames.isEmpty()) {
            for (int i = 0; i < ops.size(); i++) {
                ops.set(i, synthetics.replaceDatedNodes(ops.get(i), dateRenames));
            }
            top = synthetics.replaceDatedNodes(top, dateRenames);
        }
        return top;
    }

    /** Milestoned property functions: each head's temporal arguments,
     * chain-keyed (conflicting dates for one chain are loud — the date
     * split renamed genuine two-date heads before this runs). */
    private Map<String, TemporalFrame.TemporalSpec> collectChainSpecs(
            List<TypedSpec> ops, TypedSpec top, List<TypedGraphTree> tree) {
        Map<String, TemporalFrame.TemporalSpec> specs =
                new LinkedHashMap<>();
        for (TypedSpec op : ops) {
            if (op instanceof TypedFilter f) {
                temporal.collectTemporalSpecs(f.predicate(), specs);
            }
            if (op instanceof TypedSortBy sb) {
                temporal.collectTemporalSpecs(sb.key(), specs);
            }
        }
        if (tree == null) {
            for (TypedLambda fn : terminalLambdas(top)) {
                temporal.collectTemporalSpecs(fn, specs);
            }
        }
        return specs;
    }

    private TypedSpec resolveObject(TypedSpec top, Context context) {
        OpChain phase1 = collectOpChain(top, context);
        List<TypedSpec> ops = phase1.ops();
        top = splitDatedHeads(ops, phase1.top());
        List<TypedGraphTree> tree = phase1.tree();
        boolean implicitSerialize = phase1.implicitSerialize();
        TypedGetAll g = phase1.getAll();
        context = phase1.context();
        ClassSource cs = phase1.cs();

        // 2. Demand scan over ALL the chain's user lambdas (one funnel with
        //    the substitution — they cannot drift), close over slot
        //    conditions, materialize.
        // POSITION-AWARE demand (the positional rule table): to-many paths
        // in PROJECTION position explode via LEFT JOIN; in FILTER position
        // they become implicit EXISTS per boolean leaf.
        // ENTRY RULE (learned three times now): scans enter through the
        // lambda's BODY — entering via the lambda itself trips the shadow
        // stop on its own parameter.
        Set<List<String>> filterPaths = new LinkedHashSet<>();
        Set<List<String>> projectionPaths = new LinkedHashSet<>();
        Map<String, List<AggDemand>> aggDemands =
                new LinkedHashMap<>();
        for (TypedSpec op : ops) {
            if (op instanceof TypedFilter f) {
                for (TypedSpec b : f.predicate().body()) {
                    memberScan(b, f.predicate().parameters().get(0), cs, filterPaths);
                }
                // #69: outer reads in corrPreds may root at THIS filter's
                // param too (the RhsFilter family) — same parent demand
                // as the terminal-lambda scan
                synthetics.corrPredOuterDemand(f.predicate(), filterPaths);
            }
            if (op instanceof TypedSortBy sb) {
                for (TypedSpec b : sb.key().body()) {
                    CorrelatedSubselects.aggScan(b, sb.key().parameters().get(0), cs,
                            aggDemands, projectionPaths,
                            this::isToManyAssocHead);
                }
            }
        }
        if (tree == null && implicitSerialize) {
            tree = new GraphEmission(ctx, sources, assocMaterial, temporal, this::dispatch, () -> freshVarCounter++).synthesizeScalarTree(cs);
        }
        if (tree != null) {
            // GRAPH terminal: tree LEAF paths feed slot demand (a leaf's
            // binding may read a demanded join slot); class-typed children
            // correlate — never join — and are materialized by
            // buildGraphNode, not the demand scan.
            for (TypedGraphTree node : tree) {
                if (node.children().isEmpty()) {
                    projectionPaths.add(List.of(node.property()));
                }
            }
        } else {
            for (TypedLambda fn : terminalLambdas(top)) {
                for (TypedSpec b : fn.body()) {
                    CorrelatedSubselects.aggScan(b, fn.parameters().get(0), cs,
                            aggDemands, projectionPaths,
                            this::isToManyAssocHead);
                }
                synthetics.corrPredOuterDemand(fn, projectionPaths);
            }
        }
        Set<List<String>> paths = new LinkedHashSet<>(filterPaths);
        paths.addAll(projectionPaths);

        temporal = temporal.withSpecs(collectChainSpecs(ops, top, tree));

        NavPlan navPlan = registerNavigations(cs, paths);
        Set<String> demanded = navPlan.demanded();
        Set<String> demandedNavs = navPlan.demandedNavs();
        Map<String, Substitution.AssocSub> assocs = navPlan.assocs();
        assocs.putAll(phase1.flattenAssocs());   // #63 flatten provenance
        Map<String, NavMaterializer.NavMat> navMats = navPlan.navMats();
        Map<String, String> navHeadByAlias = navPlan.navHeadByAlias();
        Map<String, String> extraNavHeads = navPlan.extraNavHeads();
        Map<String, List<List<String>>> extraNavTails =
                navPlan.extraNavTails();
        var navSteps = navPlan.navSteps();

        // Mapping ~distinct stays IN the pipeline (a distinct subselect —
        // the engine's own emission, its "could optimize to collapse" TODO
        // notwithstanding): deferring it to the projected output dedups
        // over the PROJECTED SUBSET of columns, which changes row counts
        // whenever the projection is not injective on the distinct tuple
        // (corpus testDistinctMappingSimpleProjectSelectOneOfTheDistinct-
        // Properties: name-only projection must keep BOTH 'IF 2' rows).
        RootPipe rootPipe = materializeRoot(cs, g, demanded, demandedNavs,
                navMats, navHeadByAlias, assocs);
        Pipelines.Materialized m = rootPipe.m();
        final TypedSpec materializedPipe = rootPipe.materializedPipe();

        Map<String, Substitution.ExistsSub> existsSubs =
                registerExistsSubs(cs, paths, filterPaths,
                        withTerminal(ops, top), context, assocs);

        AssocPlan assocPlan = registerAssociationJoins(cs, paths, context,
                navSteps, extraNavHeads, extraNavTails, assocs,
                navPlan.corrNavHeads(), navPlan.navTails());
        List<AssociationJoins.AssocJoin> assocJoins = assocPlan.assocJoins();
        Map<String, AssociationJoins.AssocJoin> joinsByChain = assocPlan.joinsByChain();

        // subType(@Sub) casts dispatch through the SUBTYPE's binding
        // table (same-source inheritance) — registered after
        // materialization so the scan never perturbs join demand
        CorrelatedSubselects.registerSubTypeSubs(cs, top, sources, assocs);

        // 2a'. JOIN-KEY COLLECTION under mapping ~distinct (engine L5135):
        // demanded joins' source-side key columns must survive the
        // ~distinct narrowing select — widen it (the distinct then dedups
        // over the widened row, exactly the engine's query-dependent
        // distinct tuple). Aggregated-navigation materials build here so
        // their conditions participate.
        Map<String, AssociationJoins.AssocJoin> aggMaterials =
                corrSubs.buildAggMaterials(temporal, cs, context, aggDemands);
        Set<String> joinKeyReads = new LinkedHashSet<>();
        for (AssociationJoins.AssocJoin aj : assocJoins) {
            CorrelatedSubselects.collectParamColumnReads(aj.condition(), joinKeyReads);
        }
        for (AssociationJoins.AssocJoin aj : aggMaterials.values()) {
            CorrelatedSubselects.collectParamColumnReads(aj.condition(), joinKeyReads);
        }
        for (Substitution.ExistsSub ex : existsSubs.values()) {
            CorrelatedSubselects.collectParamColumnReads(ex.orientedCond(), joinKeyReads);
        }
        TypedSpec keyWidenedPipe = joinKeyReads.isEmpty() ? materializedPipe
                : Pipelines.widenDistinctForKeys(materializedPipe, joinKeyReads);
        if (!joinKeyReads.isEmpty()) {
            // UNION root: member threads carry the demanded join keys
            // through the union projection (engine partial-union goldens)
            keyWidenedPipe = Pipelines.widenConcatenateForKeys(
                    keyWidenedPipe, joinKeyReads);
        }
        if (keyWidenedPipe != materializedPipe) {
            m = new Pipelines.Materialized(keyWidenedPipe, m.slotPrefixes(),
                    m.stripped());
        }

        registerDottedExistsSubs(cs, ops, top, tree, context, assocs,
                existsSubs);

        JoinedPipe joined = foldAssociationJoins(cs, m, keyWidenedPipe,
                assocJoins, aggMaterials, aggDemands);
        m = joined.m();
        List<AssociationJoins.AssocJoin> aggAssocJoins = joined.aggAssocJoins();
        Map<TypedSpec, Substitution.AggRead> aggReads = joined.aggReads();

        // Association-end names for honest bare-head errors (audit R3).
        Set<String> assocEnds = new LinkedHashSet<>(assocs.keySet());
        for (List<String> path : paths) {
            if (!cs.bindings().containsKey(path.get(0))
                    && ctx.findAssociationOf(cs.classFqn(), path.get(0)).isPresent()) {
                assocEnds.add(path.get(0));
            }
        }

        // 3. Fold the ops back on, bottom-up, substituting filter lambdas.
        // The fresh row var must not collide with ANY lambda parameter in
        // reach (user lambdas may legally be named _rN — audit capture
        // finding); scan and skip.
        String fresh = CorrelatedSubselects.freshRowVar(cs, ops, top,
                assocJoins, aggAssocJoins, existsSubs,
                () -> freshVarCounter++);
        TypedSpec pipeline = m.pipeline();
        for (int i = ops.size() - 1; i >= 0; i--) {
            pipeline = switch (ops.get(i)) {
                case TypedFilter f -> new TypedFilter(pipeline,
                        substitution(cs, m, assocs, assocEnds, existsSubs, aggReads, true, fresh, f.predicate())
                                .rewriteLambda(f.predicate()),
                        pipeline.info());
                case TypedLimit l -> new TypedLimit(pipeline, l.count(), pipeline.info());
                case TypedDrop d -> new TypedDrop(pipeline, d.count(), pipeline.info());
                case TypedSlice sl -> new TypedSlice(pipeline, sl.start(), sl.stop(),
                        pipeline.info());
                case TypedSortBy sb -> new TypedSortBy(pipeline,
                        substitution(cs, m, assocs, assocEnds, existsSubs, aggReads, false, fresh, sb.key()).rewriteLambda(sb.key()),
                        sb.ascending(), pipeline.info());
                case TypedDistinct d -> {
                    // dedup by the MAPPED row (engine instance identity):
                    // narrow to the class's own columns so joined helper
                    // columns cannot defeat the dedup (the two-exists
                    // family's 'expected 1, got 5'); downstream demands on
                    // dropped columns fail loud at projection resolution.
                    Type.RelationType pipeRow =
                            (Type.RelationType) pipeline.info().type();
                    Set<String> own = new LinkedHashSet<>();
                    for (Type.Column c : cs.rowType().columns()) {
                        own.add(c.name());
                    }
                    List<Type.Column> kept = pipeRow.columns().stream()
                            .filter(c -> own.contains(c.name())).toList();
                    if (kept.isEmpty()) {
                        yield new TypedDistinct(pipeline, List.of(),
                                pipeline.info());
                    }
                    yield new TypedDistinct(pipeline,
                            kept.stream().map(Type.Column::name).toList(),
                            new ExprType(new Type.RelationType(kept),
                                    pipeline.info().multiplicity()));
                }
                default -> throw new IllegalStateException("resolver bug: uncollected op");
            };
        }

        // 4a. GRAPH terminal: the envelope — leaves substituted over the
        //     row, children as correlated per-hop nodes (plan H4a SNAPSHOT).
        if (tree != null) {
            return new GraphEmission(ctx, sources, assocMaterial, temporal, this::dispatch, () -> freshVarCounter++).buildGraphNode(cs, pipeline, m.slotPrefixes(), m.stripped(),
                    fresh, tree, context, /*arrayWrap*/ true, g.info());
        }

        // 4. The relation-shaping boundary: info UNCHANGED.
        final TypedSpec base = pipeline;
        final Pipelines.Materialized fm = m;
        final String fv = fresh;
        Function<TypedLambda, TypedLambda> sub = fn ->
                substitution(cs, fm, assocs, assocEnds, existsSubs, aggReads, false, fv, fn)
                        .rewriteLambda(fn);
        // An agg map may be the BARE instance var (x|$x : y|$y->count()) —
        // COUNT(*)-style; it becomes the identity over the row.
        Function<TypedAggCol, TypedAggCol> subAgg = a ->
                new TypedAggCol(a.name(),
                        isBareUserVar(a.map())
                                ? substitution(cs, fm, assocs, assocEnds, existsSubs,
                                        aggReads, false, fv, a.map()).identityLambda(a.map())
                                : sub.apply(a.map()),
                        a.reduce());
        return switch (top) {
            case TypedProject p -> new TypedProject(base,
                    p.columns().stream().map(col -> new TypedFuncCol(col.name(),
                            sub.apply(col.fn()))).toList(),
                    p.info());
            case TypedGroupBy gb -> new TypedGroupBy(base,
                    gb.keys().stream().map(k -> new TypedGroupBy.GroupKey(k.column(),
                            Optional.of(sub.apply(k.fn().orElseThrow(() ->
                                    new NotImplementedException("class-source groupBy"
                                            + " key '" + k.column() + "' without an"
                                            + " extraction lambda is not supported"
                                            + " yet")))))).toList(),
                    gb.aggs().stream().map(subAgg).toList(),
                    gb.info());
            default -> throw new IllegalStateException("unreachable");
        };
    }

    /** The object-space lambdas a relation-shaping terminal carries. */
    private static List<TypedLambda> terminalLambdas(TypedSpec top) {
        List<TypedLambda> out = new ArrayList<>();
        switch (top) {
            case TypedProject p -> p.columns().forEach(c -> out.add(c.fn()));
            case TypedGroupBy g -> {
                g.keys().forEach(k -> k.fn().ifPresent(out::add));
                g.aggs().forEach(a -> out.add(a.map()));
            }
            default -> throw new IllegalStateException("unreachable");
        }
        return out;
    }

    /** {@code x|$x} — the whole-instance map of a COUNT(*)-style aggregate. */
    private static boolean isBareUserVar(TypedLambda l) {
        return l.body().size() == 1
                && l.body().get(0) instanceof TypedVariable v
                && l.parameters().size() == 1
                && v.name().equals(l.parameters().get(0));
    }

    static void collectLambdaParams(TypedSpec n, Set<String> out) {
        if (n instanceof TypedLambda l) {
            out.addAll(l.parameters());
        }
        for (TypedSpec c : n.children()) {
            collectLambdaParams(c, out);
        }
    }

    /** The navigate-slot alias a class-typed head binding reads, or null. */
    static String navSlotAlias(TypedSpec binding, String rowVar,
                                       Set<String> navAliases) {
        TypedSpec inner = binding;
        if (inner instanceof TypedNativeCall c
                && c.args().size() == 1
                && c.callee().qualifiedName().equals("meta::pure::functions::multiplicity::toOne")) {
            inner = c.args().get(0);
        }
        if (inner instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable v
                && v.name().equals(rowVar)
                && navAliases.contains(pa.property())) {
            return pa.property();
        }
        return null;
    }

    /** Milestoned accesses grouped by their dotted chain (mirrors
     * {@link #collectTemporalNodes}'s walk — anything the conflict
     * detector would see, the date splitter sees first). */
    private static void collectDatedNodes(TypedSpec n, String userVar,
            Map<String, List<com.legend.compiler.spec.typed
                    .TypedMilestonedAccess>> out) {
        if (n instanceof TypedMilestonedAccess ma) {
            List<String> p = Substitution.pathOf(ma, userVar);
            if (p != null) {
                out.computeIfAbsent(String.join(".", p),
                        k -> new ArrayList<>()).add(ma);
            }
        }
        if (n instanceof TypedLambda l && l.parameters().contains(userVar)) {
            return;
        }
        for (TypedSpec c : n.children()) {
            collectDatedNodes(c, userVar, out);
        }
    }

    /**
     * A lifted head's user predicate, substituted over the TARGET's
     * bindings (the {@code rewriteExists} cfSub pattern applied at the
     * materialization site) and wrapped around the finished target
     * pipeline — the join's composite right side carries the filter
     * INSIDE (engine JTN parity), so unmatched parents keep their NULL
     * row and the outer join-stamping never double-stamps.
     */
        /** As above with the target's MATERIALIZED nav-step SubNavs: a pred's
     * depth-2 reads through class-typed slot heads ($e.address.name — the
     * Fork family) dispatch through AssocSub registries built from them
     * (the corrPredOnJoinedRow pass-1 rule, shared). */
            /** The demand half of the shared funnel: every $p.<path> read in a lambda. */
    static void consumedPaths(TypedSpec n, String userVar,
                                      Set<List<String>> out) {
        List<String> path = Substitution.pathOf(n, userVar);
        if (path != null) {
            out.add(path);
        }
        if (n instanceof TypedLambda l && l.parameters().contains(userVar)) {
            return;   // shadowing: the substitution stops here too (one funnel)
        }
        for (TypedSpec c : n.children()) {
            consumedPaths(c, userVar, out);
        }
    }

    /** Aggregate natives that reduce a to-many navigation in projection
     * position (exact FQNs from the catalog — never name suffixes). */
            /** COUNT over no children is pure 0 — the LEFT join delivers NULL. */
        /** One aggregate call over a to-many association path in projection
     * position: {@code $f.employees.age->max()}. Substitutes as a column
     * read off the head's grouped-subselect join (engine subAggregation
     * shape) — the path is NOT bare-demanded, so no row explosion. */
    /** The class FQN reached after {@code upto} property hops from the
     * source class; null when any hop is not a class-typed property. */
    private String classAtHop(ClassSource cs, List<String> path, int upto) {
        String cur = cs.classFqn();
        for (int i = 0; i < upto; i++) {
            var pr = ctx.findProperty(cur,
                    SyntheticHeads.realHead(path.get(i))).orElse(null);
            if (pr == null || !(pr.type() instanceof Type.ClassType ct)) {
                return null;
            }
            cur = ct.fqn();
        }
        return cur;
    }

    record AggDemand(TypedNativeCall node,
            String leaf, TypedLambda mapper,
            TypedLambda orderKey, boolean orderAsc) {

        AggDemand(TypedNativeCall node, String leaf) {
            this(node, leaf, null, null, true);
        }

        AggDemand(TypedNativeCall node, String leaf, TypedLambda mapper) {
            this(node, leaf, mapper, null, true);
        }

        /** Target-side property heads this demand reads (leaf name, or
         * the computed mapper's / order key's own-param path heads) —
         * feeds the target's slot demand. */
        List<String> demandLeaves() {
            List<String> out = new ArrayList<>();
            if (leaf != null) {
                out.add(leaf);
            } else {
                for (List<String> pth : lambdaHeads(mapper)) {
                    out.add(pth.get(0));
                }
            }
            if (orderKey != null) {
                for (List<String> pth : lambdaHeads(orderKey)) {
                    out.add(pth.get(0));
                }
            }
            return out;
        }

        private static Set<List<String>> lambdaHeads(TypedLambda fn) {
            Set<List<String>> ps = new LinkedHashSet<>();
            for (TypedSpec b : fn.body()) {
                consumedPaths(b, fn.parameters().get(0), ps);
            }
            return ps;
        }
    }

    /**
     * The agg-aware projection-position scan: aggregates over TO-MANY
     * association paths register {@link AggDemand}s; every OTHER path is
     * bare demand exactly as {@link #consumedPaths} records it (one
     * traversal — the two demand kinds cannot double-count a path).
     */

    /**
     * The FILTER-position scan: a bare to-many crossing consumed AS A
     * COLLECTION by contains/in is set MEMBERSHIP (EXISTS route — engine
     * testContainsOnToManyProperty golden) and demands only its HEAD's
     * exists material, never the explosion join; everything else records
     * bare demand exactly as {@link #consumedPaths}.
     */
    private void memberScan(TypedSpec n, String userVar, ClassSource cs,
                            Set<List<String>> out) {
        if (n instanceof TypedNativeCall mc
                && mc.args().size() == 2) {
            String key = mc.callee().signatureKey();
            boolean isContains = Pure.nativeNamed("contains", key);
            boolean isIn = Pure.nativeNamed("in", key);
            if (isContains || isIn) {
                TypedSpec coll = isContains ? mc.args().get(0) : mc.args().get(1);
                TypedSpec other = isContains ? mc.args().get(1) : mc.args().get(0);
                List<String> cp = coll
                        instanceof TypedPropertyAccess
                        ? Substitution.pathOf(coll, userVar) : null;
                if (cp != null && cp.size() == 2 && isToManyAssocHead(cs, cp.get(0))) {
                    out.add(List.of(cp.get(0)));
                    memberScan(other, userVar, cs, out);
                    return;
                }
            }
        }
        // LOUD (audit 9): an aggregate over a to-many crossing in FILTER
        // position would join-explode and the reducer's to-one identity
        // silently eats the aggregate (max() > 30 becoming any-match).
        if (n instanceof TypedNativeCall ac
                && !ac.args().isEmpty()
                && CorrelatedSubselects.AGG_FQNS.contains(ac.callee().qualifiedName())
                && CorrelatedSubselects.containsToManyCrossing(
                        ac.args().get(0), userVar, cs,
                        this::isToManyAssocHead)) {
            throw new NotImplementedException("aggregate '"
                    + ac.callee().qualifiedName() + "' over a to-many"
                    + " navigation in FILTER position is not supported yet");
        }
        List<String> path = Substitution.pathOf(n, userVar);
        if (path != null) {
            out.add(path);
        }
        if (n instanceof TypedLambda l && l.parameters().contains(userVar)) {
            return;
        }
        for (TypedSpec c : n.children()) {
            memberScan(c, userVar, cs, out);
        }
    }

    /** {@code head} is a to-many navigation: an unbound association end, or
     * a navigate-slot binding (class-typed Join PM), with to-many
     * multiplicity on the class property. */
    private boolean isToManyAssocHead(ClassSource cs, String head) {
        // synthetic identities (#fN/#cN/#dN) route by their REAL property —
        // an aggregate over a lifted head must take the grouped-subselect
        // route, never bare-explode (wrong row counts, silent)
        String real = SyntheticHeads.realHead(head);
        boolean toMany = ctx.findProperty(cs.classFqn(), real)
                .map(pr -> !(pr.multiplicity()
                        instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                        && Integer.valueOf(1).equals(b.upper())))
                .orElse(false);
        if (!toMany) {
            return false;
        }
        TypedSpec binding = cs.bindings().get(real);
        if (binding != null) {
            var navSteps = Pipelines.navSteps(cs.pipeline());
            String alias = navSlotAlias(binding, cs.rowVar(), navSteps.keySet());
            if (alias == null) {
                return false;   // embedded/otherwise heads keep their routes
            }
            var nav = navSteps.get(alias);
            return nav.target() instanceof TypedGetAll tg
                    && sources.binds(cs.mappingFqn(), tg.classFqn());
        }
        return ctx.findAssociationOf(cs.classFqn(), real).isPresent();
    }

    /**
     * The TARGET-side key columns of a conjunctive equi-join condition —
     * the columns that pin each source row to AT MOST ONE group of the
     * aggregated subselect. Any other condition shape is loud: joining a
     * grouped subselect on it could match multiple groups (fan-out).
     */
    /** PARENT-side equi columns of the association condition (roles
     * swapped vs {@link #targetEquiKeys}) — the group/join-back keys of
     * the correlated aggregated subselect (#69 parent-copy emission). */



    /** Aggregated-navigation materials: per head, the target join material
     * with the demands' leaf + computed-mapper nav paths (#69). */
        /** #69 correlated-aggregate sub SOURCE (fold 2c): uncorrelated heads
     * group the plain target; a correlated pred re-joins the PARENT extent
     * (parent-copy), filters by the pred over the joined row, and groups
     * by the PARENT-side equi keys. */
            /** #69 EXPLODING parent-copy subselect emission (fold 2b) — the
     * rerouted correlated head's join material. */
            /** One projected column {@code name := $rowVar.readCol} of the
     * exploding subselect (#69). */
                /** The parent-extent COPY inside a correlated aggregated subselect:
     * the parent pipeline materialized with the slots and navigate steps
     * the correlated pred's OUTER reads demand, plus depth-1 SubNavs for
     * the substitution (deeper hops stay loud). */
        /** Per-getAll temporal context (M3 propagation): the root fetch's dates
     * and strategy flow to SAME-STRATEGY targets navigated through temporal
     * parents (engine: milestoning context does NOT propagate through
     * non-temporal intermediates). Set at each getAll resolution entry. */
    static TypedEnumValue leftKind() {
        String fqn = "meta::pure::functions::relation::JoinKind";
        return new TypedEnumValue(fqn, "LEFT",
                new ExprType(
                        new Type.EnumType(fqn),
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
    }

    /** INNER — the flatten's null-row guard by construction: the engine
     * spells LEFT and its READER skips null-pk rows; the row sets are
     * identical (deliberate documented emission divergence). */
    static TypedEnumValue innerKind() {
        String fqn = "meta::pure::functions::relation::JoinKind";
        return new TypedEnumValue(fqn, "INNER",
                new ExprType(
                        new Type.EnumType(fqn),
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
    }

    /**
     * Build the association join for {@code $parent.head}: the mapping's
     * AssociationBinding predicate function carries the condition
     * (legacyAssocPredicate(a, b, srcRows, tgtRows, {srcRow,tgtRow|cond}) —
     * H1's emission); the target is the class's own pipeline (its ~filter
     * rides along; its slots strip under empty demand — leaf bindings that
     * read them are loud). Orientation: the predicate's cond params are
     * (classA-row, classB-row) with classA = property1's target; navigating
     * property1 means the PARENT is classB, so the params REVERSE (the
     * TypedJoin condition binds (leftRow=parent, rightRow=target)).
     */
    /**
     * Leaf properties read INSIDE exists/isEmpty predicates over {@code
     * head} anywhere in the chain's filters — the inner-lambda demand that
     * materializes the exists target's own slot joins (N1).
     */
    /**
     * R1 — RECURSIVE SCOPE DEMAND: the exists/filter predicates nested
     * under {@code head} get their own registered materials against the
     * TARGET class, so navigation INSIDE a nested predicate resolves
     * instead of staying loud (Registries.NONE was a blanket stop).
     * Terminates on expression depth; assoc/agg materials stay top-level
     * for now (R1a: exists materials only).
     */
    /** The nested scope's registries PLUS the target pipeline widened with
     * the nested association joins (their prefixed columns must ride the
     * exists relation for assocLeaf reads — R2 arm 2). */
    record NestedScope(Substitution.Registries regs, TypedSpec pipeline,
                       Type.RelationType row) {
    }

    private NestedScope nestedScope(ClassSource t,
            List<TypedSpec> ops, String head, Context context,
            TypedSpec targetPipe) {
        return nestedScope(t, ops, List.of(head), context, targetPipe);
    }

    private NestedScope nestedScope(ClassSource t,
            List<TypedSpec> ops, List<String> pathKey, Context context,
            TypedSpec targetPipe) {
        Substitution.Registries none = Substitution.Registries.NONE;
        List<TypedLambda> inner = InnerDemand.lambdas(ops, pathKey);
        Type.RelationType row =
                (Type.RelationType) targetPipe.info().type();
        if (inner.isEmpty()) {
            return new NestedScope(none, targetPipe, row);
        }
        Set<List<String>> innerPaths = new LinkedHashSet<>();
        List<TypedSpec> innerOps = new ArrayList<>();
        for (TypedLambda lam : inner) {
            if (lam.parameters().isEmpty()) {
                continue;
            }
            Set<String> heads = new LinkedHashSet<>();
            collectParamPathHeads(lam, lam.parameters().get(0), heads);
            heads.forEach(h -> innerPaths.add(List.of(h)));
            innerOps.add(new TypedFilter(targetPipe, lam, targetPipe.info()));
        }
        if (innerPaths.isEmpty()) {
            return new NestedScope(none, targetPipe, row);
        }
        // nested EXISTS materials (emptiness consumption)
        Map<String, Substitution.ExistsSub> nested =
                registerExistsSubs(t, innerPaths, Set.of(), innerOps, context, Map.of());
        // nested ASSOC materials (leaf reads): widen the exists relation
        // with each demanded association's LEFT join, prefix-renamed —
        // the same descriptor->emission fold the root pipeline uses
        Map<String, Substitution.AssocSub> nestedAssocs = new LinkedHashMap<>();
        TypedSpec pipe = targetPipe;
        var tNavSteps = Pipelines.navSteps(t.pipeline());
        for (List<String> path : innerPaths) {
            String h = path.get(0);
            // an exists material and an assoc material COEXIST for one
            // head: emptiness consumption reads existsSubs, leaf reads
            // read assocs — different arms of rewritePath/rewriteCallArms
            if (nestedAssocs.containsKey(h)) {
                continue;
            }
            // association heads AND nav-slot-backed heads both resolve
            // through associationJoin (its binding!=null arm is the
            // navigate-slot route — $e.address.name where address is a
            // Join-PM property)
            TypedSpec hb = t.bindings().get(SyntheticHeads.realHead(h));
            boolean slotBacked = hb != null
                    && navSlotAlias(hb, t.rowVar(), tNavSteps.keySet()) != null;
            if (!slotBacked && (hb != null
                    || ctx.findAssociationOf(t.classFqn(),
                            SyntheticHeads.realHead(h)).isEmpty())) {
                continue;
            }
            Set<String> hLeaves = new LinkedHashSet<>();
            for (List<String> p2 : innerPaths) {
                if (p2.size() >= 2 && p2.get(0).equals(h)) {
                    hLeaves.add(p2.get(1));
                }
            }
            // aggJoinMaterial is the nav-slot-aware entry (binding-backed
            // heads route through the navigate slot; associations fall
            // through to the assoc route)
            AssociationJoins.AssocJoin aj2 = assocMaterial.aggJoinMaterial(
                    temporal, t, h, context, hLeaves, Set.of());
            List<Type.Column> cols = new ArrayList<>(
                    ((Type.RelationType) pipe.info().type()).columns());
            for (Type.Column c : aj2.targetRow().columns()) {
                cols.add(new Type.Column(aj2.prefix() + c.name(),
                        c.type(), c.multiplicity()));
            }
            Type.RelationType widened = new Type.RelationType(cols);
            pipe = new TypedJoin(pipe, aj2.targetPipeline(), leftKind(),
                    aj2.condition(), Optional.of(aj2.prefix()),
                    new ExprType(widened,
                            com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
            nestedAssocs.put(h, new Substitution.AssocSub(aj2.prefix(),
                    aj2.target().rowVar(), aj2.target().bindings(),
                    aj2.target().classFqn(),
                    Pipelines.slotAliases(aj2.target().pipeline()),
                    aj2.targetSlotPrefixes(),
                    /*readVar*/ null, /*readRowType*/ null,
                    Map.of(), Map.of()));
        }
        if (nested.isEmpty() && nestedAssocs.isEmpty()) {
            return new NestedScope(none, targetPipe, row);
        }
        return new NestedScope(
                new Substitution.Registries(nestedAssocs, Set.of(), nested,
                        Map.of(), isNotEmptyCallee(), equalCallee()),
                pipe, (Type.RelationType) pipe.info().type());
    }

    /** Multi-hop paths consumed DIRECTLY under an emptiness-family call
     * ({@code isEmpty}/{@code isNotEmpty}/{@code exists} first arg) —
     * the class-typed-leaf EXISTS registration keys off these. */
    private static void collectEmptinessChainPaths(TypedSpec n, String userVar,
            Set<List<String>> out) {
        if (n instanceof TypedNativeCall c && !c.args().isEmpty()) {
            String key = c.callee().signatureKey();
            if (Pure.nativeNamed("isEmpty", key)
                    || Pure.nativeNamed("isNotEmpty", key)
                    || Pure.nativeNamed("exists", key)) {
                List<String> p =
                        Substitution.pathOf(c.args().get(0), userVar);
                if (p != null && p.size() >= 2) {
                    out.add(p);
                }
            }
        }
        if (n instanceof TypedLambda l && l.parameters().contains(userVar)) {
            return;   // shadowing: the substitution stops here too
        }
        for (TypedSpec c : n.children()) {
            collectEmptinessChainPaths(c, userVar, out);
        }
    }

    /** Heads of property paths over {@code param} in the lambda's body. */
    static void collectParamPathHeads(TypedSpec n, String param,
            Set<String> out) {
        List<String> p = Substitution.pathOf(n, param);
        if (p != null && !p.isEmpty()) {
            out.add(p.get(0));
        }
        for (TypedSpec ch : n.children()) {
            collectParamPathHeads(ch, param, out);
        }
    }

    /** Whether the pipeline contains a UNION (concatenate) anywhere. */
    static boolean containsConcatenate(TypedSpec pipeline) {
        if (pipeline instanceof TypedConcatenate) {
            return true;
        }
        for (TypedSpec c : pipeline.children()) {
            if (containsConcatenate(c)) {
                return true;
            }
        }
        return false;
    }

    /** Whether the pipeline carries a mapping ~filter anywhere. */
    static boolean containsFilter(TypedSpec pipeline) {
        if (pipeline instanceof TypedFilter) {
            return true;
        }
        for (TypedSpec c : pipeline.children()) {
            if (containsFilter(c)) {
                return true;
            }
        }
        return false;
    }

    /** Column names a join condition reads off its SOURCE param (param 0). */
    static void collectVarColumnReads(TypedSpec n, String var, Set<String> out) {
        if (n instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable v
                && v.name().equals(var)) {
            out.add(pa.property());
        }
        for (TypedSpec c : n.children()) {
            collectVarColumnReads(c, var, out);
        }
    }

    private Substitution substitution(ClassSource cs, Pipelines.Materialized m,
                                      Map<String, Substitution.AssocSub> assocs,
                                      Set<String> assocEnds,
                                      Map<String, Substitution.ExistsSub> existsSubs,
                                      Map<TypedSpec, Substitution.AggRead> aggReads,
                                      boolean filterPosition,
                                      String freshRowVar, TypedLambda userLambda) {
        return new Substitution(new Substitution.Target(
                new Substitution.RowScope(userLambda.parameters().get(0),
                        freshRowVar, cs.classFqn(), cs.mappingFqn(),
                        cs.rowVar(), cs.bindings(),
                        (Type.RelationType)
                                m.pipeline().info().type(),
                        m.stripped(), m.slotPrefixes(),
                        temporal.milestoneColumnsOf(cs.pipeline(),
                                cs.classFqn())),
                new Substitution.Registries(assocs, assocEnds, existsSubs,
                        aggReads, isNotEmptyCallee(), equalCallee()),
                new Substitution.TemporalView(temporal.root().legacyDates(),
                        temporal.headTemporalDates(), temporal.root()),
                filterPosition, false));
    }

    /** Any registered equal overload — membership-crossing emission. */
    private TypedFunction equalCallee() {
        // exact FQN (audit 23 A3): a user-defined 'equal' must never
        // become the membership callee
        var fns = ctx.findFunction("meta::pure::functions::boolean::equal");
        return fns.isEmpty() ? null : fns.get(0);
    }

    /** Any registered isNotEmpty overload — the lowerer dispatches by family. */
    private TypedFunction isNotEmptyCallee() {
        var fns = ctx.findFunction(
                "meta::pure::functions::collection::isNotEmpty");
        if (fns.isEmpty()) {
            throw new IllegalStateException("resolver bug: no isNotEmpty registration");
        }
        return fns.get(0);
    }

    /** Per-class dispatch: the runtime candidate that BINDS the class wins. */
    private String dispatch(Context context, String classFqn) {
        if (context.explicitMapping() != null) {
            return context.explicitMapping();
        }
        RuntimeDefinition rt = ctx.findRuntime(context.runtimeFqn()).orElseThrow(() ->
                new MappingResolutionException("unknown runtime '"
                        + context.runtimeFqn() + "'", context.runtimeFqn()));
        List<String> binders = rt.mappings().stream()
                .distinct()   // a runtime listing a mapping twice is not ambiguity
                .filter(m -> sources.binds(m, classFqn))
                .toList();
        if (binders.size() != 1) {
            // a poisoned class mapping (per-class normalization failure)
            // explains a ZERO-binder miss — surface the recorded reason,
            // walking includes (the poisoned set may live in an included
            // mapping). A 2-binder error is ambiguity, not poisoning.
            StringBuilder why = new StringBuilder();
            if (binders.isEmpty()) {
                Set<String> seen = new LinkedHashSet<>();
                ArrayDeque<String> queue = new ArrayDeque<>(rt.mappings());
                while (!queue.isEmpty()) {
                    String m = queue.poll();
                    if (!seen.add(m)) {
                        continue;
                    }
                    ctx.mappingPoison(m, classFqn).ifPresent(reason ->
                            why.append("; '").append(m).append("' failed to normalize "
                                    + "this class: ").append(reason));
                    ctx.findMapping(m).ifPresent(def -> def.includes().forEach(inc ->
                            queue.add(inc.mappingPath())));
                }
            }
            throw new MappingResolutionException("runtime '" + context.runtimeFqn()
                    + "' has " + binders.size() + " mappings binding class '"
                    + classFqn + "' (of " + rt.mappings().size()
                    + " candidates); class-query dispatch needs exactly one" + why,
                    classFqn);
        }
        return binders.get(0);
    }
}
