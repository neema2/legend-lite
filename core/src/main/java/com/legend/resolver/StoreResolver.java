package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedAggCol;
import com.legend.compiler.spec.typed.TypedAggregate;
import com.legend.compiler.spec.typed.TypedConcatenate;
import com.legend.compiler.spec.typed.TypedDistinct;
import com.legend.compiler.spec.typed.TypedDrop;
import com.legend.compiler.spec.typed.TypedExtend;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedExtendAgg;
import com.legend.compiler.spec.typed.TypedExtendWindow;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedGroupBy;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedRename;
import com.legend.compiler.spec.typed.TypedGraphFetch;
import com.legend.compiler.spec.typed.TypedGraphTree;
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
import com.legend.parser.element.RuntimeDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
                java.util.Map.of());
        this.navMaterializer = new NavMaterializer(sources);
        this.assocMaterial = new AssociationJoins(ctx, sources, specs,
                synthetics);
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
        // LAZY: the runtime is consulted only when a class fetch needs a
        // mapping — a pure relation query with an unusable runtime must not
        // fail (the corpus's date-literal regression).
        Context context = driverRuntimeFqn == null ? Context.NONE
                : Context.ofRuntime(driverRuntimeFqn);
        List<TypedSpec> out = new ArrayList<>(body.size());
        for (TypedSpec stmt : body) {
            out.add(resolveNode(stmt, context));
        }
        return out;
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
            case com.legend.compiler.spec.typed.TypedNativeCall nc
                    when nc.args().size() == 1 && isObjectSpace(nc.args().get(0))
                    && (nc.callee().qualifiedName().equals(
                                    "meta::pure::functions::collection::size")
                            || nc.callee().qualifiedName().equals(
                                    "meta::pure::functions::collection::count")) -> {
                com.legend.compiler.element.type.Type intType =
                        com.legend.compiler.element.type.Type.Primitive.INTEGER;
                com.legend.compiler.element.type.ExprType oneInt =
                        new com.legend.compiler.element.type.ExprType(intType,
                                com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
                com.legend.compiler.element.type.ExprType rowParam =
                        new com.legend.compiler.element.type.ExprType(
                                nc.args().get(0).info().type(),
                                com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
                TypedLambda one = new TypedLambda(java.util.List.of("p"),
                        java.util.List.of(new com.legend.compiler.spec.typed.TypedCInteger(1L, oneInt)),
                        new com.legend.compiler.element.type.ExprType(
                                new com.legend.compiler.element.type.Type.FunctionType(
                                        java.util.List.of(new com.legend.compiler.element.type.Type.Param(
                                                rowParam.type(), rowParam.multiplicity())),
                                        new com.legend.compiler.element.type.Type.Param(
                                                intType,
                                                com.legend.compiler.element.type.Multiplicity.Bounded.ONE)),
                                com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
                com.legend.compiler.element.type.Type.RelationType relType =
                        new com.legend.compiler.element.type.Type.RelationType(java.util.List.of(
                                new com.legend.compiler.element.type.Type.Column(
                                        "c", intType,
                                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE)));
                TypedProject proj = new TypedProject(nc.args().get(0),
                        java.util.List.of(new com.legend.compiler.spec.typed.TypedFuncCol("c", one)),
                        new com.legend.compiler.element.type.ExprType(relType,
                                com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
                TypedSpec rel = resolveChain(proj, context);
                var relSize = ctx.findFunction("meta::pure::functions::relation::size")
                        .stream().findFirst().orElseThrow(() -> new IllegalStateException(
                                "relation size overload missing from the catalog"));
                yield new com.legend.compiler.spec.typed.TypedNativeCall(relSize,
                        java.util.List.of(rel), nc.info());
            }
            // ->map(p|$p.scalarExpr) over instances IS the single-column
            // projection (the map-terminal invariant); Person.all().prop is
            // its property-access spelling (to-many paths explode via the
            // projection funnel's positional rules).
            case com.legend.compiler.spec.typed.TypedMap m
                    when isObjectSpace(m.source())
                    && !(((com.legend.compiler.element.type.Type.FunctionType) m.mapper().info().type()).result().type()
                            instanceof com.legend.compiler.element.type.Type.ClassType) -> {
                com.legend.compiler.spec.typed.TypedMap m2 = synthetics.liftValueMapFilter(m);
                yield resolveChain(scalarMapAsProject(m2.source(), m2.mapper(),
                        m2.info().multiplicity()), context);
            }
            case com.legend.compiler.spec.typed.TypedPropertyAccess pa when isObjectSpace(pa.source())
                    && !(pa.info().type() instanceof com.legend.compiler.element.type.Type.ClassType) -> {
                com.legend.compiler.element.type.ExprType elem =
                        new com.legend.compiler.element.type.ExprType(pa.info().type(),
                                com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
                String v = "p";
                TypedLambda fn = new TypedLambda(java.util.List.of(v),
                        java.util.List.of(new com.legend.compiler.spec.typed.TypedPropertyAccess(
                                new com.legend.compiler.spec.typed.TypedVariable(v,
                                        new com.legend.compiler.element.type.ExprType(
                                                sourceClassType(pa.source()),
                                                com.legend.compiler.element.type.Multiplicity.Bounded.ONE)),
                                pa.property(), elem)),
                        new com.legend.compiler.element.type.ExprType(
                                new com.legend.compiler.element.type.Type.FunctionType(
                                        java.util.List.of(new com.legend.compiler.element.type.Type.Param(
                                                sourceClassType(pa.source()),
                                                com.legend.compiler.element.type.Multiplicity.Bounded.ONE)),
                                        new com.legend.compiler.element.type.Type.Param(pa.info().type(),
                                                pa.info().multiplicity())),
                                com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
                yield resolveChain(scalarMapAsProject(pa.source(), fn,
                        pa.info().multiplicity()), context);
            }
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
            // a COLUMN READ over a relation-shaped chain ($tds.rows.id —
            // the TDS getter desugar): rebuild over the resolved source
            case com.legend.compiler.spec.typed.TypedPropertyAccess pa
                    when containsGetAll(pa.source())
                    && pa.source().info().type()
                            instanceof com.legend.compiler.element.type.Type
                                    .RelationType ->
                    new com.legend.compiler.spec.typed.TypedPropertyAccess(
                            resolveNode(pa.source(), context), pa.property(),
                            pa.info());
            case TypedFilter f when containsGetAll(f.source()) -> new TypedFilter(
                    resolveNode(f.source(), context), f.predicate(), f.info());
            case TypedProject p when containsGetAll(p.source()) -> new TypedProject(
                    resolveNode(p.source(), context), p.columns(), p.info());
            case TypedSort s when containsGetAll(s.source()) -> new TypedSort(
                    resolveNode(s.source(), context), s.keys(), s.info());
            case com.legend.compiler.spec.typed.TypedCast c
                    when containsGetAll(c.source())
                    && c.info().type() instanceof com.legend.compiler.element.type.Type
                            .RelationType ->
                    new com.legend.compiler.spec.typed.TypedCast(
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
            case com.legend.compiler.spec.typed.TypedNavigate nav
                    when containsGetAll(nav.source())
                    && nav.target().info().type()
                            instanceof com.legend.compiler.element.type.Type.RelationType ->
                    new com.legend.compiler.spec.typed.TypedNavigate(
                            resolveNode(nav.source(), context), nav.alias(),
                            nav.target(), nav.predicate(), nav.form(), nav.info());
            case com.legend.compiler.spec.typed.TypedJoin j when containsGetAll(j) ->
                    new com.legend.compiler.spec.typed.TypedJoin(
                            resolveNode(j.left(), context), resolveNode(j.right(), context),
                            j.kind(), j.condition(), j.prefix(), j.info());
            // map over RELATION rows above a class chain (the object-space
            // map arm matched earlier; this is the relation-space wrapper)
            case com.legend.compiler.spec.typed.TypedMap m
                    when containsGetAll(m.source())
                    && m.source().info().type()
                            instanceof com.legend.compiler.element.type.Type.RelationType ->
                    new com.legend.compiler.spec.typed.TypedMap(
                            resolveNode(m.source(), context), m.mapper(), m.info());
            // scalar/relation NATIVES over chains bottoming at a getAll
            // (size()/equal()/isEmpty() tails of assert expressions): the
            // object-space native arms matched earlier; here every arg
            // resolves structurally
            case com.legend.compiler.spec.typed.TypedNativeCall nc
                    when containsGetAll(nc) ->
                    new com.legend.compiler.spec.typed.TypedNativeCall(nc.callee(),
                            nc.args().stream().map(a2 -> resolveNode(a2, context))
                                    .toList(), nc.info());
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
    private static boolean isClassDistinct(com.legend.compiler.spec.typed.TypedNativeCall c) {
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
        if (n instanceof com.legend.compiler.spec.typed.TypedVariable v
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
    private static com.legend.compiler.element.type.Type sourceClassType(TypedSpec chain) {
        com.legend.compiler.element.type.Type t = chain.info().type();
        if (!(t instanceof com.legend.compiler.element.type.Type.ClassType)) {
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
        String name = body instanceof com.legend.compiler.spec.typed.TypedPropertyAccess bpa
                ? bpa.property() : "value";
        com.legend.compiler.element.type.Type.Param result =
                ((com.legend.compiler.element.type.Type.FunctionType) mapper.info().type()).result();
        com.legend.compiler.element.type.Type.RelationType row =
                new com.legend.compiler.element.type.Type.RelationType(java.util.List.of(
                        new com.legend.compiler.element.type.Type.Column(
                                name, result.type(), result.multiplicity())));
        return new TypedProject(source,
                java.util.List.of(new com.legend.compiler.spec.typed.TypedFuncCol(name, mapper)),
                new com.legend.compiler.element.type.ExprType(row, valueMult));
    }

    private static boolean isObjectSpace(TypedSpec source) {
        return switch (source) {
            case TypedGetAll ignored -> true;
            case com.legend.compiler.spec.typed.TypedFrom fr ->
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
                        instanceof com.legend.compiler.spec.typed.TypedCInteger;
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
            Map<String, java.util.List<java.util.List<String>>> navTails,
            Map<String, String> navHeadByAlias,
            Map<String, String> extraNavHeads,
            Map<String, java.util.List<java.util.List<String>>> extraNavTails,
            Map<String, com.legend.compiler.spec.typed.TypedNavigate> navSteps) {}

    /** PHASE — slot + navigate-step demand: heads whose bindings read
     * join slots demand them; class-typed Join PM heads materialize their
     * targets (recursively, with per-hop temporal context) and register
     * the head substitution material under the chain key. */
    private NavPlan registerNavigations(ClassSource cs,
            Set<java.util.List<String>> paths) {
        // Slot demand (heads whose bindings read join slots).
        Set<String> slotAliases = Pipelines.slotAliases(cs.pipeline());
        Set<String> demanded = new java.util.LinkedHashSet<>();
        if (!slotAliases.isEmpty()) {
            for (java.util.List<String> path : paths) {
                TypedSpec binding = cs.bindings().get(SyntheticHeads.realHead(path.get(0)));
                if (binding != null) {
                    collectAliasReads(binding, cs.rowVar(), slotAliases, demanded);
                }
            }
        }

        // Navigate-step (class-typed Join PM) demand: a 2-hop path whose
        // head binding reads a navigate slot ($row.alias, class-typed)
        // demands that step; its target joins as the class's own pipeline.
        var navSteps = Pipelines.navSteps(cs.pipeline());
        Set<String> demandedNavs = new java.util.LinkedHashSet<>();
        Map<String, Substitution.AssocSub> assocs = new java.util.LinkedHashMap<>();
        Map<String, java.util.List<java.util.List<String>>> navTails =
                new java.util.LinkedHashMap<>();
        Map<String, String> navHeadByAlias = new java.util.LinkedHashMap<>();
        // SECOND identities on one physical slot (date-fingerprinted /
        // filter-lifted synthetic heads beside the base): the slot
        // materializes once for the FIRST identity; every other identity
        // emits its OWN prefixed join from the same nav material (engine:
        // joins keyed by date / per-use). headKey → slot alias, + tails.
        Map<String, String> extraNavHeads = new java.util.LinkedHashMap<>();
        Map<String, java.util.List<java.util.List<String>>> extraNavTails =
                new java.util.LinkedHashMap<>();
        for (java.util.List<String> path : paths) {
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
                var partial = (com.legend.compiler.spec.typed.TypedNewInstance)
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
                if (inner instanceof com.legend.compiler.spec.typed.TypedNewInstance ni
                        && mid + 1 < path.size()
                        && ni.properties().containsKey(path.get(mid))) {
                    drill = ni.properties().get(path.get(mid));
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
            java.util.List<java.util.List<String>> predTails =
                    new java.util.ArrayList<>();
            TypedLambda liftedPred = synthetics.pred(path.get(0));
            if (liftedPred != null) {
                Set<java.util.List<String>> predPaths =
                        new java.util.LinkedHashSet<>();
                for (TypedSpec b : liftedPred.body()) {
                    consumedPaths(b, liftedPred.parameters().get(0), predPaths);
                }
                predTails.addAll(predPaths);
            }
            // JOIN IDENTITY: a SECOND head identity on one physical slot
            // routes through its own prefixed join (below) — the slot
            // itself materializes once for the first identity.
            String priorHead = navHeadByAlias.get(alias);
            if (priorHead != null && !priorHead.equals(headKey)) {
                extraNavHeads.putIfAbsent(headKey, alias);
                java.util.List<java.util.List<String>> et = extraNavTails
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
        Map<String, NavMaterializer.NavMat> navMats = new java.util.LinkedHashMap<>();
        for (String alias : demandedNavs) {
            var nav = navSteps.get(alias);
            String targetClass = ((com.legend.compiler.spec.typed.TypedGetAll)
                    nav.target()).classFqn();
            navMats.put(alias, navMaterializer.navTargetMaterialized(temporal, cs.mappingFqn(), targetClass,
                    navTails.getOrDefault(alias, java.util.List.of()),
                    navHeadByAlias.getOrDefault(alias, alias), null));
            // a LIFTED head's predicate applies INSIDE the join target
            // (engine: the chain filter parks on the navigation's join-tree
            // node); the composite right side carries its own filters, so
            // the outer join-stamping never double-stamps it
            TypedLambda liftedPred = synthetics.pred(
                    navHeadByAlias.getOrDefault(alias, alias));
            if (liftedPred != null) {
                ClassSource target = sources.get(cs.mappingFqn(), targetClass);
                NavMaterializer.NavMat mat = navMats.get(alias);
                navMats.put(alias, new NavMaterializer.NavMat(
                        predFilteredPipe(mat.pipeline(), target,
                                mat.slotPrefixes(), liftedPred, cs.mappingFqn()),
                        mat.slotPrefixes(), mat.stripped(), mat.subNavs()));
            }
        }
        for (String alias : demandedNavs) {
            var nav = navSteps.get(alias);
            String targetClass = ((com.legend.compiler.spec.typed.TypedGetAll)
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
                navHeadByAlias, extraNavHeads, extraNavTails, navSteps);
    }


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
        Set<java.util.List<String>> emptinessChainPaths =
                new java.util.LinkedHashSet<>();
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
        for (java.util.List<String> path : emptinessChainPaths) {
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
                        instanceof com.legend.compiler.spec.typed.TypedGetAll tg)
                        || !sources.binds(cs.mappingFqn(), tg.classFqn())) {
                    continue;
                }
                t = sources.get(cs.mappingFqn(), tg.classFqn());
                Pipelines.Materialized tm = Pipelines.materialize(
                        t.pipeline(), Set.of(), t.classFqn());
                TypedSpec p0 = tm.pipeline();
                cond = nav.predicate();
                if (cond.parameters().size() == 2) {
                    Set<String> tgtReads = new java.util.LinkedHashSet<>();
                    for (TypedSpec b0 : cond.body()) {
                        Pipelines.collectVarReads(b0,
                                cond.parameters().get(1), tgtReads);
                    }
                    p0 = Pipelines.widenConcatenateForKeys(p0, tgtReads);
                }
                tPipe = temporal.temporalTargetPipe(parent, t, dotted,
                        temporal.applyJoinTemporalFilters(p0, t, java.util.Map.of()));
                tPrefixes = tm.slotPrefixes();
                TypedLambda liftedLeafPred = synthetics.pred(leafName);
                if (liftedLeafPred != null) {
                    tPipe = predFilteredPipe(tPipe, t, tPrefixes,
                            liftedLeafPred, cs.mappingFqn());
                }
            } else if (ctx.findAssociationOf(parent.classFqn(), leaf)
                    .isPresent()) {
                AssociationJoins.AssocJoin aj = assocMaterial.associationJoin(temporal, parent, leafName, context,
                        true, Set.of(), dotted);
                t = aj.target();
                cond = aj.condition();
                tPipe = aj.targetPipeline();
                tPrefixes = aj.targetSlotPrefixes();
            } else {
                continue;
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
                    java.util.List.of(cbody), cond.info());
            existsSubs.put(dotted, new Substitution.ExistsSub(tPipe,
                    chainedCond, t.rowVar(), t.bindings(),
                    (com.legend.compiler.element.type.Type.RelationType)
                            tPipe.info().type(),
                    t.classFqn(), Pipelines.slotAliases(t.pipeline()),
                    tPrefixes, leafToMany));
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
            Map<String, NavMaterializer.NavMat> navMats, Map<String, String> navHeadByAlias) {
        TypedSpec csPipe = cs.pipeline();
        Pipelines.Materialized m = Pipelines.materialize(
                csPipe, demanded, demandedNavs, cs.classFqn(),
                (alias, targetClass) -> navMats.containsKey(alias)
                        ? navMats.get(alias).pipeline()
                        : Pipelines.materialize(
                                sources.get(cs.mappingFqn(), targetClass).pipeline(),
                                Set.of(), targetClass).pipeline());
        Map<String, String> navPrefixToClass = new java.util.LinkedHashMap<>();
        Map<String, String> navPrefixToChain = new java.util.LinkedHashMap<>();
        Map<String, String> midPrefixToChain = new java.util.LinkedHashMap<>();
        Map<String, String> midPrefixToDim = new java.util.LinkedHashMap<>();
        Set<String> slotAliases = Pipelines.slotAliases(cs.pipeline());
        for (var navE : Pipelines.navSteps(cs.pipeline()).entrySet()) {
            if (navE.getValue().target()
                    instanceof com.legend.compiler.spec.typed.TypedGetAll tg2) {
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
                                    && !java.util.Objects.equals(
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
            java.util.List<AssociationJoins.AssocJoin> aggAssocJoins,
            Map<TypedSpec, Substitution.AggRead> aggReads) {}

    /** PHASE 2b-ii — fold the association joins and the aggregated-
     * navigation grouped subselects onto the materialized pipeline. */
    private JoinedPipe foldAssociationJoins(ClassSource cs,
            Pipelines.Materialized m, TypedSpec keyWidenedPipe,
            java.util.List<AssociationJoins.AssocJoin> assocJoins,
            Map<String, AssociationJoins.AssocJoin> aggMaterials,
            Map<String, java.util.List<AggDemand>> aggDemands) {
        // 2b. Materialize the association joins (descriptor -> emission,
        //     first-demand order) onto the pipeline.
        TypedSpec withJoins = keyWidenedPipe;
        for (AssociationJoins.AssocJoin aj : assocJoins) {
            com.legend.compiler.element.type.Type.RelationType leftRow =
                    (com.legend.compiler.element.type.Type.RelationType)
                            withJoins.info().type();
            java.util.List<com.legend.compiler.element.type.Type.Column> cols =
                    new ArrayList<>(leftRow.columns());
            for (com.legend.compiler.element.type.Type.Column c
                    : aj.targetRow().columns()) {
                cols.add(new com.legend.compiler.element.type.Type.Column(
                        aj.prefix() + c.name(), c.type(), c.multiplicity()));
            }
            withJoins = new com.legend.compiler.spec.typed.TypedJoin(withJoins,
                    aj.targetPipeline(), leftKind(), aj.condition(),
                    java.util.Optional.of(aj.prefix()),
                    new com.legend.compiler.element.type.ExprType(
                            new com.legend.compiler.element.type.Type.RelationType(cols),
                            com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
        }
        // 2c. AGGREGATED navigations (the engine's subAggregation shape):
        // per to-many head, ONE grouped subselect — the target pipeline
        // grouped by the association's target-side equi-key columns (names
        // preserved, so the association condition joins it VERBATIM), one
        // aggregate column per demand. Each aggregate node then reads its
        // column off the joined row; no row explosion reaches the
        // projection, and the aggregate itself runs IN the database.
        java.util.Map<TypedSpec, Substitution.AggRead> aggReads =
                new java.util.IdentityHashMap<>();
        java.util.List<AssociationJoins.AssocJoin> aggAssocJoins = new ArrayList<>();
        for (var entry : aggDemands.entrySet()) {
            String head = entry.getKey();
            Set<String> leaves = new java.util.LinkedHashSet<>();
            for (AggDemand d : entry.getValue()) {
                leaves.add(d.leaf());
            }
            AssociationJoins.AssocJoin aj = aggMaterials.get(head);
            aggAssocJoins.add(aj);
            java.util.List<String> keyCols = targetEquiKeys(aj.condition(), head);
            java.util.List<com.legend.compiler.spec.typed.TypedGroupBy.GroupKey>
                    keys = new ArrayList<>();
            java.util.List<com.legend.compiler.element.type.Type.Column>
                    subCols = new ArrayList<>();
            for (String k : keyCols) {
                var col = aj.targetRow().columns().stream()
                        .filter(c -> c.name().equals(k)).findFirst()
                        .orElseThrow(() -> new IllegalStateException(
                                "resolver bug: equi-key column '" + k
                                        + "' missing from the target row"));
                keys.add(new com.legend.compiler.spec.typed.TypedGroupBy.GroupKey(
                        k, java.util.Optional.empty()));
                subCols.add(col);
            }
            java.util.List<com.legend.compiler.spec.typed.TypedAggCol> aggs =
                    new ArrayList<>();
            int ord = 0;
            var targetRowType = new com.legend.compiler.element.type.ExprType(
                    aj.targetRow(), com.legend.compiler.element.type.Multiplicity
                            .Bounded.ONE);
            for (AggDemand d : entry.getValue()) {
                TypedSpec leafBinding = aj.target().bindings().get(d.leaf());
                if (leafBinding == null) {
                    throw new MappingResolutionException("property '" + d.leaf()
                            + "' of class '" + aj.target().classFqn()
                            + "' has no binding in mapping '" + cs.mappingFqn()
                            + "' (aggregated navigation leaf)",
                            aj.target().classFqn());
                }
                String alias = "agg_" + ord++;
                TypedLambda map = new TypedLambda(
                        java.util.List.of(aj.target().rowVar()),
                        java.util.List.of(leafBinding),
                        new com.legend.compiler.element.type.ExprType(
                                new com.legend.compiler.element.type.Type.FunctionType(
                                        java.util.List.of(new com.legend.compiler
                                                .element.type.Type.Param(aj.targetRow(),
                                                com.legend.compiler.element.type
                                                        .Multiplicity.Bounded.ONE)),
                                        new com.legend.compiler.element.type.Type.Param(
                                                leafBinding.info().type(),
                                                leafBinding.info().multiplicity())),
                                com.legend.compiler.element.type.Multiplicity
                                        .Bounded.ONE));
                String yv = "_y";
                java.util.List<TypedSpec> reduceArgs = new ArrayList<>();
                reduceArgs.add(new com.legend.compiler.spec.typed.TypedVariable(yv,
                        new com.legend.compiler.element.type.ExprType(
                                leafBinding.info().type(),
                                com.legend.compiler.element.type.Multiplicity
                                        .Bounded.ZERO_MANY)));
                for (int i = 1; i < d.node().args().size(); i++) {
                    TypedSpec extra = d.node().args().get(i);
                    if (referencesVar(extra, aj.target().rowVar())) {
                        throw new NotImplementedException("aggregate '"
                                + d.node().callee().qualifiedName()
                                + "' over navigation '" + head + "' with an"
                                + " instance-dependent extra argument is not"
                                + " supported");
                    }
                    reduceArgs.add(extra);
                }
                TypedSpec reduceCall = new com.legend.compiler.spec.typed
                        .TypedNativeCall(d.node().callee(), reduceArgs,
                        d.node().info());
                TypedLambda reduce = new TypedLambda(java.util.List.of(yv),
                        java.util.List.of(reduceCall),
                        new com.legend.compiler.element.type.ExprType(
                                new com.legend.compiler.element.type.Type.FunctionType(
                                        java.util.List.of(new com.legend.compiler
                                                .element.type.Type.Param(
                                                leafBinding.info().type(),
                                                com.legend.compiler.element.type
                                                        .Multiplicity.Bounded.ZERO_MANY)),
                                        new com.legend.compiler.element.type.Type.Param(
                                                d.node().info().type(),
                                                d.node().info().multiplicity())),
                                com.legend.compiler.element.type.Multiplicity
                                        .Bounded.ONE));
                aggs.add(new com.legend.compiler.spec.typed.TypedAggCol(alias,
                        map, reduce));
                subCols.add(new com.legend.compiler.element.type.Type.RelationType
                        .Column(alias, d.node().info().type(),
                        com.legend.compiler.element.type.Multiplicity
                                .Bounded.ZERO_ONE));
            }
            var subRow = new com.legend.compiler.element.type.Type.RelationType(subCols);
            TypedSpec sub = new com.legend.compiler.spec.typed.TypedGroupBy(
                    aj.targetPipeline(), keys, aggs,
                    new com.legend.compiler.element.type.ExprType(subRow,
                            com.legend.compiler.element.type.Multiplicity
                                    .Bounded.ONE));
            String prefix = AssociationJoins.prefixFor(head + "_agg", cs);
            com.legend.compiler.element.type.Type.RelationType leftRow =
                    (com.legend.compiler.element.type.Type.RelationType)
                            withJoins.info().type();
            java.util.List<com.legend.compiler.element.type.Type.Column>
                    cols = new ArrayList<>(leftRow.columns());
            for (var c : subRow.columns()) {
                cols.add(new com.legend.compiler.element.type.Type.RelationType
                        .Column(prefix + c.name(), c.type(), c.multiplicity()));
            }
            withJoins = new com.legend.compiler.spec.typed.TypedJoin(withJoins,
                    sub, leftKind(), aj.condition(),
                    java.util.Optional.of(prefix),
                    new com.legend.compiler.element.type.ExprType(
                            new com.legend.compiler.element.type.Type.RelationType(cols),
                            com.legend.compiler.element.type.Multiplicity
                                    .Bounded.ONE));
            ord = 0;
            for (AggDemand d : entry.getValue()) {
                aggReads.put(d.node(), new Substitution.AggRead(
                        prefix + "agg_" + ord++, isCountFamily(d.node())));
            }
        }
        m = new Pipelines.Materialized(withJoins, m.slotPrefixes(), m.stripped());

        return new JoinedPipe(m, aggAssocJoins, aggReads);
    }


    /** PHASE — to-many/navigate heads under exists/isEmpty: the
     * correlated-EXISTS material per head (target pipeline + oriented
     * condition, NO join emitted; the positional rule table §133). */
    private Map<String, Substitution.ExistsSub> registerExistsSubs(
            ClassSource cs, Set<java.util.List<String>> paths,
            Set<java.util.List<String>> filterPaths, List<TypedSpec> ops,
            Context context) {
        Map<String, Substitution.ExistsSub> existsSubs = new java.util.LinkedHashMap<>();
        for (java.util.List<String> path : paths) {
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
                        instanceof com.legend.compiler.spec.typed.TypedGetAll tg)
                        // eager material only when the target class IS mapped
                        // here (an M2M chain's nav target lives upstream —
                        // registration must not throw for a rewrite that may
                        // never fire)
                        || !sources.binds(cs.mappingFqn(), tg.classFqn())) {
                    continue;
                }
                ClassSource t = sources.get(cs.mappingFqn(), tg.classFqn());
                Set<String> tSlots0 = Pipelines.slotAliases(t.pipeline());
                Set<String> tDemand0 = new java.util.LinkedHashSet<>();
                Set<String> innerLeaves = new java.util.LinkedHashSet<>(
                        existsInnerLeaves(ops, head));
                TypedLambda liftedPred0 = synthetics.pred(head);
                if (liftedPred0 != null) {
                    for (TypedSpec b : liftedPred0.body()) {
                        collectParamPathHeads(b,
                                liftedPred0.parameters().get(0), innerLeaves);
                    }
                }
                for (String leaf : innerLeaves) {
                    TypedSpec lb = t.bindings().get(leaf);
                    if (lb != null) {
                        collectAliasReads(lb, t.rowVar(), tSlots0, tDemand0);
                    }
                }
                tDemand0 = Pipelines.closeOverConditions(t.pipeline(), tDemand0);
                Pipelines.Materialized tMat0 = Pipelines.materialize(
                        t.pipeline(), tDemand0, t.classFqn());
                // UNION target: member threads carry the key columns the
                // navigate predicate binds on (mirrors the assoc route)
                TypedSpec tPipe0 = tMat0.pipeline();
                if (nav.predicate().parameters().size() == 2) {
                    Set<String> tgtReads = new java.util.LinkedHashSet<>();
                    for (TypedSpec b : nav.predicate().body()) {
                        Pipelines.collectVarReads(b,
                                nav.predicate().parameters().get(1), tgtReads);
                    }
                    tPipe0 = Pipelines.widenConcatenateForKeys(tPipe0, tgtReads);
                }
                TypedSpec tTemporal = temporal.temporalTargetPipe(cs, t, head,
                        temporal.applyJoinTemporalFilters(tPipe0, t, java.util.Map.of()));
                TypedLambda liftedPred = synthetics.pred(head);
                if (liftedPred != null) {
                    tTemporal = predFilteredPipe(tTemporal, t,
                            tMat0.slotPrefixes(), liftedPred, cs.mappingFqn());
                }
                Pipelines.Materialized tMat = new Pipelines.Materialized(
                        tTemporal, tMat0.slotPrefixes(), tMat0.stripped());
                boolean navToMany = !(ctx.findProperty(cs.classFqn(), SyntheticHeads.realHead(head))
                        .map(pr -> pr.multiplicity())
                        .filter(mm -> mm instanceof com.legend.compiler.element.type
                                .Multiplicity.Bounded bb
                                && Integer.valueOf(1).equals(bb.upper()))
                        .isPresent());
                existsSubs.put(head, new Substitution.ExistsSub(tMat.pipeline(),
                        nav.predicate(), t.rowVar(), t.bindings(),
                        (com.legend.compiler.element.type.Type.RelationType)
                                tMat.pipeline().info().type(),
                        t.classFqn(), Pipelines.slotAliases(t.pipeline()),
                        tMat0.slotPrefixes(), navToMany));
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
                    existsInnerLeaves(ops, head));
            var assocEnd = assocOpt.get().property1().propertyName()
                    .equals(SyntheticHeads.realHead(head))
                    ? assocOpt.get().property1() : assocOpt.get().property2();
            boolean isToMany = !(assocEnd.multiplicity()
                    instanceof com.legend.parser.Multiplicity.Concrete emc
                    && Integer.valueOf(1).equals(emc.upperBound()));
            // the SCALAR (slot-undemanded) pipeline serves value-position
            // consumers (filteredNavLeafRead): other consumers' slot demand
            // must not fan a single-row subquery out (audit 13 B3)
            // B3 (scalar fan-out) DEFERRED: a separate slot-undemanded
            // scalar pipeline regressed real corpus value-leaf reads
            // (testConstraintTargetingMultipleJoinsInPropertyMapping); the
            // engineered fan-out shape stays data-dependent-loud for now —
            // the plumbing (scalarPipeline field) is in place for the fix.
            existsSubs.put(head, new Substitution.ExistsSub(aj.targetPipeline(),
                    aj.condition(), aj.target().rowVar(), aj.target().bindings(),
                    aj.targetRow(), aj.target().classFqn(),
                    Pipelines.slotAliases(aj.target().pipeline()),
                    aj.targetSlotPrefixes(), isToMany));
        }

        return existsSubs;
    }

    /** PHASE output: the association-route joins (one LEFT join per
     * hop, deduped by chain key) plus per-chain leaf demand. */
    private record AssocPlan(java.util.List<AssociationJoins.AssocJoin> assocJoins,
            Map<String, AssociationJoins.AssocJoin> joinsByChain,
            Map<String, Set<String>> leavesByChain) {}

    /** PHASE — association demand (paths whose head is NOT a binding):
     * one LEFT join per hop chained by path prefix, plus the SECOND
     * head identities on shared physical slots (2a-x: per-use extra
     * joins from the same nav material). */
    private AssocPlan registerAssociationJoins(ClassSource cs,
            Set<java.util.List<String>> paths, Context context,
            Map<String, com.legend.compiler.spec.typed.TypedNavigate> navSteps,
            Map<String, String> extraNavHeads,
            Map<String, java.util.List<java.util.List<String>>> extraNavTails,
            Map<String, Substitution.AssocSub> assocs) {
        java.util.List<AssociationJoins.AssocJoin> assocJoins = new ArrayList<>();
        Map<String, AssociationJoins.AssocJoin> joinsByChain = new java.util.LinkedHashMap<>();
        // Per chain-prefix leaf demand: for [firm, country], hop 'firm'
        // must materialize firm's OWN slots feeding 'country'.
        Map<String, Set<String>> leavesByChain = new java.util.LinkedHashMap<>();
        for (java.util.List<String> path : paths) {
            for (int i = 0; i + 1 < path.size(); i++) {
                leavesByChain.computeIfAbsent(String.join(".", path.subList(0, i + 1)),
                        k -> new java.util.LinkedHashSet<>()).add(path.get(i + 1));
            }
        }
        for (java.util.List<String> path : paths) {
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
            // assoc target, not a further hop
            int effectiveSize = path.size() >= 2
                    && path.get(path.size() - 2).equals("milestoning")
                    ? path.size() - 1 : path.size();
            for (int hop = 0; hop + 1 < effectiveSize; hop++) {
                String chainKey = String.join(".", path.subList(0, hop + 1));
                AssociationJoins.AssocJoin known = joinsByChain.get(chainKey);
                if (known != null) {
                    parent = known.target();
                    parentPrefix = known.prefix();
                    continue;
                }
                AssociationJoins.AssocJoin aj = assocMaterial.associationJoin(temporal, parent, path.get(hop), context, false,
                        leavesByChain.getOrDefault(chainKey, Set.of()), chainKey);
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
                    java.util.List<com.legend.compiler.element.type.Type.Column>
                            leftCols = new ArrayList<>();
                    for (com.legend.compiler.element.type.Type.Column c
                            : ((com.legend.compiler.element.type.Type.RelationType)
                                    parent.rowType()).columns()) {
                        leftCols.add(new com.legend.compiler.element.type.Type.Column(
                                pp2 + c.name(), c.type(), c.multiplicity()));
                    }
                    var leftRow = new com.legend.compiler.element.type.Type.RelationType(leftCols);
                    String leftParam = cond.parameters().get(0);
                    TypedSpec body = Pipelines.prefixColumns(
                            cond.body().get(cond.body().size() - 1), leftParam, pp2,
                            v -> new com.legend.compiler.spec.typed.TypedVariable(leftParam,
                                    new com.legend.compiler.element.type.ExprType(leftRow,
                                            com.legend.compiler.element.type.Multiplicity
                                                    .Bounded.ONE)));
                    cond = new TypedLambda(cond.parameters(), java.util.List.of(body),
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
            String targetClass = ((com.legend.compiler.spec.typed.TypedGetAll)
                    nav.target()).classFqn();
            ClassSource target = sources.get(cs.mappingFqn(), targetClass);
            NavMaterializer.NavMat mat = navMaterializer.navTargetMaterialized(temporal, cs.mappingFqn(),
                    targetClass,
                    extraNavTails.getOrDefault(headKey, java.util.List.of()),
                    headKey, null);
            // the slot route's root stamp comes from the outer join-walk
            // (navPrefixToChain); an extra join never passes it — stamp
            // here, exactly the association route's emission
            TypedSpec tPipe = temporal.temporalTargetPipe(cs, target, headKey,
                    temporal.applyJoinTemporalFilters(mat.pipeline(), target,
                            java.util.Map.of()));
            TypedLambda lp = synthetics.pred(headKey);
            if (lp != null) {
                tPipe = predFilteredPipe(tPipe, target, mat.slotPrefixes(),
                        lp, cs.mappingFqn());
            }
            AssociationJoins.AssocJoin aj = new AssociationJoins.AssocJoin(AssociationJoins.prefixFor(headKey, cs), target, tPipe,
                    (com.legend.compiler.element.type.Type.RelationType)
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

        return new AssocPlan(assocJoins, joinsByChain, leavesByChain);
    }

    /** Phase 1 output: the collected object-space chain — the (lifted)
     * terminal, the op stack down to the getAll, the effective execution
     * context after in-chain from() re-scoping, and the bound source.
     * Field side-effects (temporal.root(), temporalByHead reset) happen in
     * {@link #collectOpChain} — one construction site. */
    private record OpChain(TypedSpec top, List<TypedGraphTree> tree,
            boolean implicitSerialize, List<TypedSpec> ops, TypedGetAll getAll,
            Context context, ClassSource cs) {}

    /** PHASE 1 — collect the op chain (terminal detection, native-shape
     * normalization, from() re-scoping), validate the fetch, construct
     * THE root temporal context, bind the class source. */
    private OpChain collectOpChain(TypedSpec top, Context context) {
        // PRE-REWRITE (before the demand scan, ledger design): filtered
        // navigations consumed as bare collections lift into SYNTHETIC
        // 2-hop heads whose join target carries the predicate.
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
        while (!(cur instanceof TypedGetAll)) {
            // Normalize collection natives with relation shapes BEFORE
            // collecting: first()/head() IS limit 1; class-space
            // sort(key, comparator) IS sortBy with a direction.
            if (cur instanceof TypedNativeCall nc && isClassDistinct(nc)) {
                // instance distinct over a relational extent dedups by
                // OBJECT IDENTITY = primary key — rows are already unique
                cur = nc.args().get(0);
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
                long k = ((com.legend.compiler.spec.typed.TypedCInteger)
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
            ops.add(cur);
            cur = switch (cur) {
                case TypedFilter f -> f.source();
                case TypedLimit l -> l.source();
                case TypedDrop d -> d.source();
                case TypedSlice sl -> sl.source();
                case TypedSortBy sb -> sb.source();
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
                java.util.Map.of());
        {
            java.util.List<TypedSpec> nd =
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
            temporal = new TemporalFrame(ctx, sources, rc, java.util.Map.of());
        }
        final Context fctx = chainContext;
        ClassSource cs = sources.get(dispatch(fctx, g.classFqn()), g.classFqn(),
                target -> dispatch(fctx, target),
                (fctx.explicitMapping() == null ? "" : fctx.explicitMapping())
                        + '\u0000'
                        + (fctx.runtimeFqn() == null ? "" : context.runtimeFqn()));

        return new OpChain(top, tree, implicitSerialize, ops, g, context, cs);
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
        java.util.Map<String, java.util.List<com.legend.compiler.spec.typed
                .TypedMilestonedAccess>> datedByChain =
                new java.util.LinkedHashMap<>();
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
        java.util.IdentityHashMap<TypedSpec, String> dateRenames =
                new java.util.IdentityHashMap<>();
        for (var chainDates : datedByChain.entrySet()) {
            java.util.Map<TemporalFrame.TemporalSpec, String> byArgs =
                    new java.util.LinkedHashMap<>();
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
        Set<java.util.List<String>> filterPaths = new java.util.LinkedHashSet<>();
        Set<java.util.List<String>> projectionPaths = new java.util.LinkedHashSet<>();
        Map<String, java.util.List<AggDemand>> aggDemands =
                new java.util.LinkedHashMap<>();
        for (TypedSpec op : ops) {
            if (op instanceof TypedFilter f) {
                for (TypedSpec b : f.predicate().body()) {
                    memberScan(b, f.predicate().parameters().get(0), cs, filterPaths);
                }
            }
            if (op instanceof TypedSortBy sb) {
                for (TypedSpec b : sb.key().body()) {
                    aggScan(b, sb.key().parameters().get(0), cs,
                            aggDemands, projectionPaths);
                }
            }
        }
        if (tree == null && implicitSerialize) {
            tree = synthesizeScalarTree(cs);
        }
        if (tree != null) {
            // GRAPH terminal: tree LEAF paths feed slot demand (a leaf's
            // binding may read a demanded join slot); class-typed children
            // correlate — never join — and are materialized by
            // buildGraphNode, not the demand scan.
            for (TypedGraphTree node : tree) {
                if (node.children().isEmpty()) {
                    projectionPaths.add(java.util.List.of(node.property()));
                }
            }
        } else {
            for (TypedLambda fn : terminalLambdas(top)) {
                for (TypedSpec b : fn.body()) {
                    aggScan(b, fn.parameters().get(0), cs,
                            aggDemands, projectionPaths);
                }
            }
        }
        Set<java.util.List<String>> paths = new java.util.LinkedHashSet<>(filterPaths);
        paths.addAll(projectionPaths);

        // Milestoned property functions: collect each head's temporal
        // arguments (conflicting dates for ONE head in one query are loud —
        // engine keys separate joins by date, a roadmap refinement).
        java.util.Map<String, TemporalFrame.TemporalSpec> specs = new java.util.LinkedHashMap<>();
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
        temporal = temporal.withSpecs(specs);

        NavPlan navPlan = registerNavigations(cs, paths);
        Set<String> demanded = navPlan.demanded();
        Set<String> demandedNavs = navPlan.demandedNavs();
        Map<String, Substitution.AssocSub> assocs = navPlan.assocs();
        Map<String, NavMaterializer.NavMat> navMats = navPlan.navMats();
        Map<String, String> navHeadByAlias = navPlan.navHeadByAlias();
        Map<String, String> extraNavHeads = navPlan.extraNavHeads();
        Map<String, java.util.List<java.util.List<String>>> extraNavTails =
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
                navMats, navHeadByAlias);
        Pipelines.Materialized m = rootPipe.m();
        final TypedSpec materializedPipe = rootPipe.materializedPipe();

        Map<String, Substitution.ExistsSub> existsSubs =
                registerExistsSubs(cs, paths, filterPaths, ops, context);

        AssocPlan assocPlan = registerAssociationJoins(cs, paths, context,
                navSteps, extraNavHeads, extraNavTails, assocs);
        java.util.List<AssociationJoins.AssocJoin> assocJoins = assocPlan.assocJoins();
        Map<String, AssociationJoins.AssocJoin> joinsByChain = assocPlan.joinsByChain();

        // 2a'. JOIN-KEY COLLECTION under mapping ~distinct (engine L5135):
        // demanded joins' source-side key columns must survive the
        // ~distinct narrowing select — widen it (the distinct then dedups
        // over the widened row, exactly the engine's query-dependent
        // distinct tuple). Aggregated-navigation materials build here so
        // their conditions participate.
        Map<String, AssociationJoins.AssocJoin> aggMaterials = new java.util.LinkedHashMap<>();
        for (var entry : aggDemands.entrySet()) {
            Set<String> leaves = new java.util.LinkedHashSet<>();
            for (AggDemand dm : entry.getValue()) {
                leaves.add(dm.leaf());
            }
            aggMaterials.put(entry.getKey(),
                    assocMaterial.aggJoinMaterial(temporal, cs, entry.getKey(),
                            context, leaves));
        }
        Set<String> joinKeyReads = new java.util.LinkedHashSet<>();
        for (AssociationJoins.AssocJoin aj : assocJoins) {
            collectParamColumnReads(aj.condition(), joinKeyReads);
        }
        for (AssociationJoins.AssocJoin aj : aggMaterials.values()) {
            collectParamColumnReads(aj.condition(), joinKeyReads);
        }
        for (Substitution.ExistsSub ex : existsSubs.values()) {
            collectParamColumnReads(ex.orientedCond(), joinKeyReads);
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
        java.util.List<AssociationJoins.AssocJoin> aggAssocJoins = joined.aggAssocJoins();
        Map<TypedSpec, Substitution.AggRead> aggReads = joined.aggReads();

        // Association-end names for honest bare-head errors (audit R3).
        Set<String> assocEnds = new java.util.LinkedHashSet<>(assocs.keySet());
        for (java.util.List<String> path : paths) {
            if (!cs.bindings().containsKey(path.get(0))
                    && ctx.findAssociationOf(cs.classFqn(), path.get(0)).isPresent()) {
                assocEnds.add(path.get(0));
            }
        }

        // 3. Fold the ops back on, bottom-up, substituting filter lambdas.
        // The fresh row var must not collide with ANY lambda parameter in
        // reach (user lambdas may legally be named _rN — audit capture
        // finding); scan and skip.
        Set<String> paramsInReach = new java.util.LinkedHashSet<>();
        for (TypedSpec op : ops) {
            collectLambdaParams(op, paramsInReach);
        }
        collectLambdaParams(top, paramsInReach);
        for (TypedSpec b : cs.bindings().values()) {
            collectLambdaParams(b, paramsInReach);
        }
        for (AssociationJoins.AssocJoin aj : assocJoins) {
            for (TypedSpec b : aj.target().bindings().values()) {
                collectLambdaParams(b, paramsInReach);
            }
        }
        for (AssociationJoins.AssocJoin aj : aggAssocJoins) {
            paramsInReach.add(aj.target().rowVar());
            paramsInReach.add("_y");
            for (TypedSpec b : aj.target().bindings().values()) {
                collectLambdaParams(b, paramsInReach);
            }
        }
        for (Substitution.ExistsSub ex : existsSubs.values()) {
            paramsInReach.addAll(ex.orientedCond().parameters());
            for (TypedSpec b : ex.targetBindings().values()) {
                collectLambdaParams(b, paramsInReach);
            }
        }
        String fresh;
        do {
            fresh = "_r" + freshVarCounter++;
        } while (paramsInReach.contains(fresh));
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
                default -> throw new IllegalStateException("resolver bug: uncollected op");
            };
        }

        // 4a. GRAPH terminal: the envelope — leaves substituted over the
        //     row, children as correlated per-hop nodes (plan H4a SNAPSHOT).
        if (tree != null) {
            return buildGraphNode(cs, pipeline, m.slotPrefixes(), m.stripped(),
                    fresh, tree, context, /*arrayWrap*/ true, g.info());
        }

        // 4. The relation-shaping boundary: info UNCHANGED.
        final TypedSpec base = pipeline;
        final Pipelines.Materialized fm = m;
        final String fv = fresh;
        java.util.function.Function<TypedLambda, TypedLambda> sub = fn ->
                substitution(cs, fm, assocs, assocEnds, existsSubs, aggReads, false, fv, fn)
                        .rewriteLambda(fn);
        // An agg map may be the BARE instance var (x|$x : y|$y->count()) —
        // COUNT(*)-style; it becomes the identity over the row.
        java.util.function.Function<TypedAggCol, TypedAggCol> subAgg = a ->
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
                            java.util.Optional.of(sub.apply(k.fn().orElseThrow(() ->
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

    /**
     * The implicit-serialize tree for a BARE class root: one leaf per
     * SCALAR binding, declaration order — class-typed bindings (embedded
     * ctors, navigation slots) are graph CHILDREN territory and stay out
     * of the bare-root envelope (plan §E10).
     */
    private List<TypedGraphTree> synthesizeScalarTree(ClassSource cs) {
        List<TypedGraphTree> tree = new ArrayList<>();
        for (Map.Entry<String, TypedSpec> e : cs.bindings().entrySet()) {
            TypedSpec inner = e.getValue();
            if (inner instanceof com.legend.compiler.spec.typed.TypedNativeCall c
                    && c.args().size() == 1
                    && c.callee().qualifiedName().equals("meta::pure::functions::multiplicity::toOne")) {
                inner = c.args().get(0);
            }
            if (inner instanceof com.legend.compiler.spec.typed.TypedNewInstance
                    || inner.info().type()
                            instanceof com.legend.compiler.element.type.Type.ClassType) {
                continue;
            }
            tree.add(new TypedGraphTree(e.getKey(), List.of()));
        }
        // GENERATED temporal-context properties ride the implicit envelope
        // (engine: temporal instances serialize their dates; sweeps read
        // each version row's own validity-start)
        String strat = temporal.temporalStrategy(cs.classFqn());
        if (strat != null) {
            if (!"processingtemporal".equals(strat)
                    && !cs.bindings().containsKey("businessDate")) {
                tree.add(new TypedGraphTree("businessDate", List.of()));
            }
            if (!"businesstemporal".equals(strat)
                    && !cs.bindings().containsKey("processingDate")) {
                tree.add(new TypedGraphTree("processingDate", List.of()));
            }
        }
        return tree;
    }

    /** The generated date's VALUE for an envelope leaf: the fetch context
     * date when one exists (point fetch), else the row's own
     * validity-start milestone column (version sweep); {@code null} when
     * the property is not a generated date here. */
    private TypedSpec generatedDateLeaf(ClassSource cs, String prop,
            com.legend.compiler.element.type.Type.RelationType rowType,
            String rowVar) {
        if ((!prop.equals("businessDate") && !prop.equals("processingDate"))
                || temporal.temporalStrategy(cs.classFqn()) == null) {
            return null;
        }
        // the point-fetch CONSTANT needs no milestone columns — a temporal
        // class on a capability-tolerance (non-milestoned) table still has
        // a well-defined context date (audit 14 B-F8: the column check
        // walled it needlessly)
        TypedSpec ctxDate = prop.equals("businessDate")
                ? temporal.root().business() : temporal.root().processing();
        if (ctxDate != null) {
            return ctxDate;
        }
        Map<String, String> mc = temporal.milestoneColumnsOf(cs.pipeline(), cs.classFqn());
        if (mc.isEmpty()) {
            return null;
        }
        String col = mc.get(prop.equals("processingDate")
                ? "genProcessingDate" : "genBusinessDate");
        if (col == null) {
            return null;
        }
        var colDef = rowType.columns().stream()
                .filter(c -> c.name().equals(col)).findFirst().orElse(null);
        if (colDef == null) {
            return null;
        }
        return new com.legend.compiler.spec.typed.TypedPropertyAccess(
                new TypedVariable(rowVar,
                        new com.legend.compiler.element.type.ExprType(rowType,
                                com.legend.compiler.element.type.Multiplicity
                                        .Bounded.ONE)),
                col, new com.legend.compiler.element.type.ExprType(
                        colDef.type(), colDef.multiplicity()));
    }

    /**
     * One envelope node (recursive): {@code leaves} substitute the class's
     * bindings over the row var; class-typed children become correlated
     * per-hop nodes — the child pipeline FILTERED by the association
     * condition with the PARENT row var free (the EXISTS material shape),
     * to-many children array-wrapped. Same-leaf pruning is by construction:
     * only tree entries are emitted.
     */
    private TypedSerializeGraph buildGraphNode(ClassSource cs, TypedSpec pipeline,
            Map<String, String> slotPrefixes, Set<String> stripped, String rowVar,
            List<TypedGraphTree> tree, Context context, boolean arrayWrap,
            com.legend.compiler.element.type.ExprType info) {
        var rowType = (com.legend.compiler.element.type.Type.RelationType)
                pipeline.info().type();
        java.util.function.UnaryOperator<TypedSpec> toRow = v -> new TypedVariable(
                rowVar, new com.legend.compiler.element.type.ExprType(rowType,
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
        List<TypedFuncCol> leaves = new ArrayList<>();
        List<TypedSerializeGraph.Child> children = new ArrayList<>();
        for (TypedGraphTree node : tree) {
            if (!node.children().isEmpty()
                    || (!cs.bindings().containsKey(node.property())
                            && ctx.findAssociationOf(cs.classFqn(), node.property())
                                    .isPresent())) {
                children.add(graphChild(cs, node, context, rowVar, rowType));
                continue;
            }
            TypedSpec binding = cs.bindings().get(node.property());
            if (binding == null) {
                // GENERATED temporal-context property on the envelope:
                // point fetch = the context date constant; VERSION SWEEP =
                // each row's own validity-start column (engine: the
                // property maps to BUS_FROM / PROCESSING_IN / snapshot)
                TypedSpec gen = generatedDateLeaf(cs, node.property(),
                        rowType, rowVar);
                if (gen == null) {
                    throw new MappingResolutionException("property '"
                            + node.property() + "' of class '" + cs.classFqn()
                            + "' is not mapped in mapping '" + cs.mappingFqn()
                            + "'", cs.classFqn());
                }
                var genFn = new com.legend.compiler.element.type.Type.FunctionType(
                        List.of(new com.legend.compiler.element.type.Type.Param(
                                rowType,
                                com.legend.compiler.element.type.Multiplicity
                                        .Bounded.ONE)),
                        new com.legend.compiler.element.type.Type.Param(
                                gen.info().type(), gen.info().multiplicity()));
                leaves.add(new TypedFuncCol(node.property(),
                        new TypedLambda(List.of(rowVar), List.of(gen),
                                new com.legend.compiler.element.type.ExprType(genFn,
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ONE))));
                continue;
            }
            TypedSpec inner = binding;
            if (inner instanceof com.legend.compiler.spec.typed.TypedNativeCall c
                    && c.args().size() == 1
                    && c.callee().qualifiedName().equals("meta::pure::functions::multiplicity::toOne")) {
                inner = c.args().get(0);
            }
            if (inner instanceof com.legend.compiler.spec.typed.TypedNewInstance) {
                throw new NotImplementedException("graph leaf '" + node.property()
                        + "' is an EMBEDDED class property — embedded graph"
                        + " children are not supported yet (H4b)");
            }
            if (inner instanceof com.legend.compiler.spec.typed.TypedNewInstanceCast) {
                throw new NotImplementedException("graph property '" + node.property()
                        + "' is a MODEL-TO-MODEL cast binding — M2M graph"
                        + " children are not supported yet (H5c)");
            }
            // A leaf mapped through STRIPPED join slots (a nested child's
            // own joins materialize with empty demand) is a feature gap,
            // not a resolver bug (audit F5).
            if (Pipelines.referencesAliasOn(binding, cs.rowVar(), stripped)) {
                throw new NotImplementedException("graph leaf '" + node.property()
                        + "' of class '" + cs.classFqn() + "' is mapped through"
                        + " the class's own join slots — nested join demand"
                        + " inside a graph child is not supported yet (H4b)");
            }
            TypedSpec body = Pipelines.rewriteRowReads(binding, cs.rowVar(),
                    slotPrefixes, stripped, toRow);
            var fnType = new com.legend.compiler.element.type.Type.FunctionType(
                    List.of(new com.legend.compiler.element.type.Type.Param(rowType,
                            com.legend.compiler.element.type.Multiplicity.Bounded.ONE)),
                    new com.legend.compiler.element.type.Type.Param(
                            body.info().type(), body.info().multiplicity()));
            leaves.add(new TypedFuncCol(node.property(),
                    new TypedLambda(List.of(rowVar), List.of(body),
                            new com.legend.compiler.element.type.ExprType(fnType,
                                    com.legend.compiler.element.type.Multiplicity.Bounded.ONE))));
        }
        return new TypedSerializeGraph(pipeline, rowVar, leaves, children,
                arrayWrap, info);
    }

    /** One nested hop: correlated child pipeline + the child's own envelope. */
    private TypedSerializeGraph.Child graphChild(ClassSource cs, TypedGraphTree node,
            Context context, String parentRowVar,
            com.legend.compiler.element.type.Type.RelationType parentRowType) {
        if (node.children().isEmpty()) {
            throw new NotImplementedException("graph child '" + node.property()
                    + "' of class '" + cs.classFqn() + "' has no sub-tree — a"
                    + " class-typed leaf serializes nothing; list its properties");
        }
        // A BINDING-backed head: the M2M SOURCE-NAV MARKER ($src.assocProp,
        // re-pointed at the composed row var by ClassSources) fans out as a
        // correlated child through the UPSTREAM association; every other
        // binding kind (embedded ctor, navigate slot, otherwise) stays loud.
        if (cs.bindings().containsKey(node.property())) {
            TypedSpec b0 = cs.bindings().get(node.property());
            TypedSpec inner = b0;
            // Unwrap the M2M cast (^Target($src.assocProp)) and toOne.
            if (inner instanceof com.legend.compiler.spec.typed.TypedNewInstanceCast nic) {
                inner = nic.source();
            }
            if (inner instanceof com.legend.compiler.spec.typed.TypedNativeCall c1
                    && c1.args().size() == 1
                    && c1.callee().qualifiedName().equals("meta::pure::functions::multiplicity::toOne")) {
                inner = c1.args().get(0);
            }
            if (inner instanceof com.legend.compiler.spec.typed.TypedNewInstanceCast nic2) {
                inner = nic2.source();
            }
            if (inner instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                    && pa.source() instanceof TypedVariable v
                    && v.name().equals(cs.rowVar())
                    && v.info().type() instanceof com.legend.compiler.element.type.Type.ClassType srcCls
                    && ctx.findAssociationOf(srcCls.fqn(), pa.property()).isPresent()) {
                return m2mAssocChild(cs, node, srcCls.fqn(), pa.property(),
                        context, parentRowVar, parentRowType);
            }
            // A NAVIGATE-SLOT read ($row.<alias>, the relational
            // association injected into the source pipeline): the slot's
            // TypedNavigate carries the raw target and the join predicate.
            if (inner instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa2
                    && pa2.source() instanceof TypedVariable v2
                    && v2.name().equals(cs.rowVar())) {
                var navSteps = Pipelines.navSteps(cs.pipeline());
                var nav = navSteps.get(pa2.property());
                if (nav != null) {
                    return navSlotChild(cs, node, nav,
                            b0 instanceof com.legend.compiler.spec.typed.TypedNewInstanceCast nic0
                                    ? nic0.classFqn() : null,
                            context, parentRowVar, parentRowType);
                }
            }
            throw new NotImplementedException("graph child '" + node.property()
                    + "' of class '" + cs.classFqn() + "' is mapped as an"
                    + " embedded/join-slot/otherwise/M2M binding — only"
                    + " association children are supported yet (H4b/H5c)");
        }
        AssociationJoins.AssocJoin aj = assocMaterial.associationJoin(temporal, cs, node.property(), context, /*forExists*/ true);
        var assoc = ctx.findAssociationOf(cs.classFqn(), node.property()).orElseThrow();
        var end = assoc.property1().propertyName().equals(node.property())
                ? assoc.property1() : assoc.property2();
        boolean toMany = !(end.multiplicity()
                instanceof com.legend.parser.Multiplicity.Concrete emc
                && Integer.valueOf(1).equals(emc.upperBound()));
        return correlatedGraphChild(aj.target(), aj.targetPipeline(), aj.targetRow(),
                aj.condition(), toMany, node, parentRowVar, parentRowType, context);
    }

    /**
     * A graph child through a NAVIGATE SLOT: the slot's raw target composes
     * into the child's declared (possibly M2M) class; the slot predicate
     * λ(sourceRow, targetRow) correlates them.
     */
    private TypedSerializeGraph.Child navSlotChild(ClassSource cs, TypedGraphTree node,
            com.legend.compiler.spec.typed.TypedNavigate nav, String castClassFqn,
            Context context, String parentRowVar,
            com.legend.compiler.element.type.Type.RelationType parentRowType) {
        String key = (context.explicitMapping() == null ? "" : context.explicitMapping())
                + '\u0000'
                + (context.runtimeFqn() == null ? "" : context.runtimeFqn());
        String rawTarget = ((TypedGetAll) nav.target()).classFqn();
        var prop = ctx.findProperty(cs.classFqn(), node.property()).orElseThrow(
                () -> new IllegalStateException("resolver bug: graph child '"
                        + node.property() + "' is not a property of '"
                        + cs.classFqn() + "'"));
        String childClass = castClassFqn != null ? castClassFqn
                : prop.type() instanceof com.legend.compiler.element.type.Type.ClassType cc
                        ? cc.fqn() : null;
        if (childClass == null) {
            throw new IllegalStateException("resolver bug: navigate-slot graph child '"
                    + node.property() + "' is not class-typed");
        }
        boolean toMany = !(prop.multiplicity()
                instanceof com.legend.compiler.element.type.Multiplicity.Bounded bm
                && Integer.valueOf(1).equals(bm.upper()));
        ClassSource child = childClass.equals(rawTarget)
                ? sources.get(dispatch(context, rawTarget), rawTarget,
                        target -> dispatch(context, target), key)
                : sources.get(cs.mappingFqn(), childClass,
                        target -> dispatch(context, target), key);
        // The slot predicate's right side reads the RAW TARGET's physical
        // columns — the child's composed pipeline must bottom at that same
        // row or the correlation filters the wrong relation (audit: the
        // m2mAssocChild guard, applied to this sibling too).
        if (!childClass.equals(rawTarget)) {
            ClassSource rawSource = sources.get(dispatch(context, rawTarget), rawTarget,
                    target -> dispatch(context, target), key);
            if (!child.rowVar().equals(rawSource.rowVar())) {
                throw new NotImplementedException("navigate-slot graph child '"
                        + node.property() + "': the child class '" + childClass
                        + "' does not compose the slot target '" + rawTarget
                        + "' — cross-source children are not supported yet");
            }
        }
        Pipelines.Materialized cMat = Pipelines.materialize(
                child.pipeline(), Set.of(), childClass);
        return correlatedGraphChild(child, cMat.pipeline(),
                (com.legend.compiler.element.type.Type.RelationType)
                        cMat.pipeline().info().type(),
                nav.predicate(), toMany, node, parentRowVar, parentRowType, context);
    }

    /**
     * An M2M child through the SOURCE class's association: the upstream
     * association supplies the condition and the RAW target; the child
     * serialized is the node property's DECLARED M2M class, whose composed
     * pipeline bottoms at that same raw target row (M2M composition
     * preserves the inner pipeline and row var — the correlation aligns by
     * construction, asserted loudly).
     */
    private TypedSerializeGraph.Child m2mAssocChild(ClassSource cs, TypedGraphTree node,
            String srcClassFqn, String assocProp, Context context,
            String parentRowVar,
            com.legend.compiler.element.type.Type.RelationType parentRowType) {
        String key = (context.explicitMapping() == null ? "" : context.explicitMapping())
                + '\u0000'
                + (context.runtimeFqn() == null ? "" : context.runtimeFqn());
        ClassSource rawParent = sources.get(dispatch(context, srcClassFqn), srcClassFqn,
                target -> dispatch(context, target), key);
        AssociationJoins.AssocJoin aj = assocMaterial.associationJoin(temporal, rawParent, assocProp, context, /*forExists*/ true);
        var prop = ctx.findProperty(cs.classFqn(), node.property()).orElseThrow(
                () -> new IllegalStateException("resolver bug: graph child '"
                        + node.property() + "' is not a property of '"
                        + cs.classFqn() + "'"));
        // Cardinality from the DECLARED property — the spec the consumer
        // typed against — not the source association's end (consistent with
        // navSlotChild; audit).
        boolean toMany = !(prop.multiplicity()
                instanceof com.legend.compiler.element.type.Multiplicity.Bounded bm
                && Integer.valueOf(1).equals(bm.upper()));
        if (!(prop.type() instanceof com.legend.compiler.element.type.Type.ClassType childCls)) {
            throw new IllegalStateException("resolver bug: M2M graph child '"
                    + node.property() + "' is not class-typed");
        }
        ClassSource child = sources.get(cs.mappingFqn(), childCls.fqn(),
                target -> dispatch(context, target), key);
        if (!child.rowVar().equals(aj.target().rowVar())) {
            throw new NotImplementedException("M2M graph child '" + node.property()
                    + "': the child class '" + childCls.fqn() + "' does not compose"
                    + " the association target '" + aj.target().classFqn()
                    + "' — cross-source M2M children are not supported yet");
        }
        Pipelines.Materialized cMat = Pipelines.materialize(
                child.pipeline(), Set.of(), childCls.fqn());
        return correlatedGraphChild(child, cMat.pipeline(),
                (com.legend.compiler.element.type.Type.RelationType)
                        cMat.pipeline().info().type(),
                aj.condition(), toMany, node, parentRowVar, parentRowType, context);
    }

    /**
     * The correlated-child tail shared by ASSOCIATION children and M2M
     * source-association children: the condition λ(parent, target) frees its
     * parent reads onto the enclosing row var, filters the target pipeline,
     * and the child's own envelope builds beneath.
     */
    private TypedSerializeGraph.Child correlatedGraphChild(ClassSource target,
            TypedSpec targetPipeline,
            com.legend.compiler.element.type.Type.RelationType targetRow,
            TypedLambda condition, boolean toMany, TypedGraphTree node,
            String parentRowVar,
            com.legend.compiler.element.type.Type.RelationType parentRowType,
            Context context) {
        // The association condition λ(parent, target): parent reads become
        // the FREE parent row var (the lowerer's enclosing-scope channel);
        // the target param stays as the child filter's own row.
        TypedLambda cond = condition;
        String pVar = cond.parameters().get(0);
        String tVar = cond.parameters().get(1);
        List<TypedSpec> corrBody = cond.body().stream().map(b ->
                Pipelines.rewriteRowReads(b, pVar, Map.of(), java.util.Set.of(),
                        v -> new TypedVariable(parentRowVar,
                                new com.legend.compiler.element.type.ExprType(parentRowType,
                                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE))))
                .toList();
        TypedLambda corr = new TypedLambda(List.of(tVar), corrBody,
                new com.legend.compiler.element.type.ExprType(
                        new com.legend.compiler.element.type.Type.FunctionType(
                                List.of(new com.legend.compiler.element.type.Type.Param(
                                        targetRow,
                                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE)),
                                new com.legend.compiler.element.type.Type.Param(
                                        com.legend.compiler.element.type.Type.Primitive.BOOLEAN,
                                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE)),
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
        TypedSpec childRel = new TypedFilter(targetPipeline, corr,
                targetPipeline.info());
        Set<String> childParams = new java.util.LinkedHashSet<>();
        for (TypedSpec b : target.bindings().values()) {
            collectLambdaParams(b, childParams);
        }
        childParams.addAll(cond.parameters());
        String childVar;
        do {
            childVar = "_r" + freshVarCounter++;
        } while (childParams.contains(childVar));
        var childInfo = new com.legend.compiler.element.type.ExprType(
                new com.legend.compiler.element.type.Type.ClassType(target.classFqn()),
                toMany ? com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_MANY
                        : com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_ONE);
        TypedSerializeGraph child = buildGraphNode(target, childRel, Map.of(),
                Pipelines.slotAliases(target.pipeline()), childVar,
                node.children(), context, toMany, childInfo);
        return new TypedSerializeGraph.Child(node.property(), child);
    }

    /** {@code x|$x} — the whole-instance map of a COUNT(*)-style aggregate. */
    private static boolean isBareUserVar(TypedLambda l) {
        return l.body().size() == 1
                && l.body().get(0) instanceof com.legend.compiler.spec.typed.TypedVariable v
                && l.parameters().size() == 1
                && v.name().equals(l.parameters().get(0));
    }

    private static void collectLambdaParams(TypedSpec n, Set<String> out) {
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
        if (inner instanceof com.legend.compiler.spec.typed.TypedNativeCall c
                && c.args().size() == 1
                && c.callee().qualifiedName().equals("meta::pure::functions::multiplicity::toOne")) {
            inner = c.args().get(0);
        }
        if (inner instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                && pa.source() instanceof com.legend.compiler.spec.typed.TypedVariable v
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
            java.util.Map<String, java.util.List<com.legend.compiler.spec.typed
                    .TypedMilestonedAccess>> out) {
        if (n instanceof com.legend.compiler.spec.typed.TypedMilestonedAccess ma) {
            java.util.List<String> p = Substitution.pathOf(ma, userVar);
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
    static TypedSpec predFilteredPipe(TypedSpec tPipe, ClassSource target,
            java.util.Map<String, String> slotPrefixes, TypedLambda pred,
            String mappingFqn) {
        Set<String> unconverted = new java.util.LinkedHashSet<>(
                Pipelines.slotAliases(target.pipeline()));
        unconverted.removeAll(slotPrefixes.keySet());
        Substitution predSub = new Substitution(new Substitution.Target(
                new Substitution.RowScope(pred.parameters().get(0),
                        target.rowVar(), target.classFqn(), mappingFqn,
                        target.rowVar(), target.bindings(),
                        (com.legend.compiler.element.type.Type.RelationType)
                                tPipe.info().type(),
                        unconverted, slotPrefixes, java.util.Map.of()),
                Substitution.Registries.NONE, Substitution.TemporalView.NONE,
                true, true));
        return new TypedFilter(tPipe, predSub.rewriteLambda(pred),
                tPipe.info());
    }




    private static void scanLambda(TypedLambda lambda, Set<java.util.List<String>> out) {
        for (TypedSpec b : lambda.body()) {
            consumedPaths(b, lambda.parameters().get(0), out);
        }
    }

    /** The demand half of the shared funnel: every $p.<path> read in a lambda. */
    private static void consumedPaths(TypedSpec n, String userVar,
                                      Set<java.util.List<String>> out) {
        java.util.List<String> path = Substitution.pathOf(n, userVar);
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
    private static final Set<String> AGG_FQNS = aggFqns();

    private static Set<String> aggFqns() {
        Set<String> out = new java.util.LinkedHashSet<>();
        for (String name : java.util.List.of("average", "mean", "sum", "max",
                "min", "joinStrings", "percentile", "median",
                "stdDevPopulation", "stdDevSample",
                "variancePopulation", "varianceSample", "count", "size")) {
            for (var f : com.legend.builtin.Pure.nativeFunctionsAt(name)) {
                out.add(f.qualifiedName());
            }
        }
        return out;
    }

    /** COUNT over no children is pure 0 — the LEFT join delivers NULL. */
    private static boolean isCountFamily(com.legend.compiler.spec.typed.TypedNativeCall nc) {
        String q = nc.callee().qualifiedName();
        return q.equals("meta::pure::functions::collection::count")
                || q.equals("meta::pure::functions::collection::size");
    }

    /** One aggregate call over a to-many association path in projection
     * position: {@code $f.employees.age->max()}. Substitutes as a column
     * read off the head's grouped-subselect join (engine subAggregation
     * shape) — the path is NOT bare-demanded, so no row explosion. */
    private record AggDemand(com.legend.compiler.spec.typed.TypedNativeCall node,
                             String leaf) {}

    /**
     * The agg-aware projection-position scan: aggregates over TO-MANY
     * association paths register {@link AggDemand}s; every OTHER path is
     * bare demand exactly as {@link #consumedPaths} records it (one
     * traversal — the two demand kinds cannot double-count a path).
     */
    private void aggScan(TypedSpec n, String userVar, ClassSource cs,
                         Map<String, java.util.List<AggDemand>> aggOut,
                         Set<java.util.List<String>> bareOut) {
        if (n instanceof com.legend.compiler.spec.typed.TypedNativeCall nc
                && !nc.args().isEmpty()
                && AGG_FQNS.contains(nc.callee().qualifiedName())) {
            java.util.List<String> path =
                    Substitution.pathOf(nc.args().get(0), userVar);
            if (path != null && path.size() == 2
                    && isToManyAssocHead(cs, path.get(0))) {
                aggOut.computeIfAbsent(path.get(0), k -> new ArrayList<>())
                        .add(new AggDemand(nc, path.get(1)));
                for (int i = 1; i < nc.args().size(); i++) {
                    aggScan(nc.args().get(i), userVar, cs, aggOut, bareOut);
                }
                return;   // the path is agg-consumed, not bare
            }
            // LOUD FALLTHROUGH (audit 9): any other aggregate whose argument
            // crosses a to-many would bare-demand the path — the join
            // explodes and the scalar reducer's to-one identity silently
            // EATS the aggregate. Never silent.
            if (path != null && path.size() > 2
                    && isToManyAssocHead(cs, path.get(0))) {
                throw new NotImplementedException("aggregate '"
                        + nc.callee().qualifiedName() + "' over the multi-hop"
                        + " to-many navigation " + String.join(".", path)
                        + " is not supported yet");
            }
            if (path == null && containsToManyCrossing(nc.args().get(0), userVar, cs)) {
                throw new NotImplementedException("aggregate '"
                        + nc.callee().qualifiedName() + "' over an expression"
                        + " containing a to-many navigation is not supported yet");
            }
        }
        java.util.List<String> path = Substitution.pathOf(n, userVar);
        if (path != null) {
            bareOut.add(path);
        }
        if (n instanceof TypedLambda l && l.parameters().contains(userVar)) {
            return;   // shadowing: same stop as consumedPaths
        }
        for (TypedSpec c : n.children()) {
            aggScan(c, userVar, cs, aggOut, bareOut);
        }
    }

    /** Any {@code $p.<toManyHead>.<...>} read anywhere under {@code n}. */
    private boolean containsToManyCrossing(TypedSpec n, String userVar, ClassSource cs) {
        java.util.List<String> path = Substitution.pathOf(n, userVar);
        if (path != null && path.size() >= 2 && isToManyAssocHead(cs, path.get(0))) {
            return true;
        }
        if (n instanceof TypedLambda l && l.parameters().contains(userVar)) {
            return false;
        }
        for (TypedSpec c : n.children()) {
            if (containsToManyCrossing(c, userVar, cs)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The FILTER-position scan: a bare to-many crossing consumed AS A
     * COLLECTION by contains/in is set MEMBERSHIP (EXISTS route — engine
     * testContainsOnToManyProperty golden) and demands only its HEAD's
     * exists material, never the explosion join; everything else records
     * bare demand exactly as {@link #consumedPaths}.
     */
    private void memberScan(TypedSpec n, String userVar, ClassSource cs,
                            Set<java.util.List<String>> out) {
        if (n instanceof com.legend.compiler.spec.typed.TypedNativeCall mc
                && mc.args().size() == 2) {
            String key = mc.callee().signatureKey();
            boolean isContains = com.legend.builtin.Pure.nativeNamed("contains", key);
            boolean isIn = com.legend.builtin.Pure.nativeNamed("in", key);
            if (isContains || isIn) {
                TypedSpec coll = isContains ? mc.args().get(0) : mc.args().get(1);
                TypedSpec other = isContains ? mc.args().get(1) : mc.args().get(0);
                java.util.List<String> cp = coll
                        instanceof com.legend.compiler.spec.typed.TypedPropertyAccess
                        ? Substitution.pathOf(coll, userVar) : null;
                if (cp != null && cp.size() == 2 && isToManyAssocHead(cs, cp.get(0))) {
                    out.add(java.util.List.of(cp.get(0)));
                    memberScan(other, userVar, cs, out);
                    return;
                }
            }
        }
        // LOUD (audit 9): an aggregate over a to-many crossing in FILTER
        // position would join-explode and the reducer's to-one identity
        // silently eats the aggregate (max() > 30 becoming any-match).
        if (n instanceof com.legend.compiler.spec.typed.TypedNativeCall ac
                && !ac.args().isEmpty()
                && AGG_FQNS.contains(ac.callee().qualifiedName())
                && containsToManyCrossing(ac.args().get(0), userVar, cs)) {
            throw new NotImplementedException("aggregate '"
                    + ac.callee().qualifiedName() + "' over a to-many"
                    + " navigation in FILTER position is not supported yet");
        }
        java.util.List<String> path = Substitution.pathOf(n, userVar);
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
        boolean toMany = ctx.findProperty(cs.classFqn(), head)
                .map(pr -> !(pr.multiplicity()
                        instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                        && Integer.valueOf(1).equals(b.upper())))
                .orElse(false);
        if (!toMany) {
            return false;
        }
        TypedSpec binding = cs.bindings().get(head);
        if (binding != null) {
            var navSteps = Pipelines.navSteps(cs.pipeline());
            String alias = navSlotAlias(binding, cs.rowVar(), navSteps.keySet());
            if (alias == null) {
                return false;   // embedded/otherwise heads keep their routes
            }
            var nav = navSteps.get(alias);
            return nav.target() instanceof com.legend.compiler.spec.typed.TypedGetAll tg
                    && sources.binds(cs.mappingFqn(), tg.classFqn());
        }
        return ctx.findAssociationOf(cs.classFqn(), head).isPresent();
    }


    /**
     * The TARGET-side key columns of a conjunctive equi-join condition —
     * the columns that pin each source row to AT MOST ONE group of the
     * aggregated subselect. Any other condition shape is loud: joining a
     * grouped subselect on it could match multiple groups (fan-out).
     */
    private static java.util.List<String> targetEquiKeys(TypedLambda cond,
                                                         String head) {
        java.util.List<String> keys = new ArrayList<>();
        if (!collectEquiKeys(cond.body().get(cond.body().size() - 1),
                cond.parameters().get(0), cond.parameters().get(1), keys)
                || keys.isEmpty()) {
            throw new NotImplementedException("aggregate over navigation '"
                    + head + "' requires a conjunctive equi-join association"
                    + " condition (grouped-subselect emission)");
        }
        return keys;
    }

    private static boolean collectEquiKeys(TypedSpec n, String srcVar,
                                           String tgtVar,
                                           java.util.List<String> out) {
        if (!(n instanceof com.legend.compiler.spec.typed.TypedNativeCall c)) {
            return false;
        }
        String q = c.callee().qualifiedName();
        if (q.equals("meta::pure::functions::boolean::and")) {
            return c.args().stream()
                    .allMatch(a -> collectEquiKeys(a, srcVar, tgtVar, out));
        }
        if (q.equals("meta::pure::functions::boolean::equal")
                && c.args().size() == 2) {
            TypedSpec a = c.args().get(0);
            TypedSpec b = c.args().get(1);
            String aCol = bareColumnOn(a, tgtVar);
            String bCol = bareColumnOn(b, tgtVar);
            if (bCol != null && !referencesVar(a, tgtVar)) {
                out.add(bCol);
                return true;
            }
            if (aCol != null && !referencesVar(b, tgtVar)) {
                out.add(aCol);
                return true;
            }
        }
        return false;
    }

    private static String bareColumnOn(TypedSpec n, String var) {
        return n instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                && pa.source() instanceof com.legend.compiler.spec.typed.TypedVariable v
                && v.name().equals(var) ? pa.property() : null;
    }

    private static boolean referencesVar(TypedSpec n, String var) {
        if (n instanceof com.legend.compiler.spec.typed.TypedVariable v
                && v.name().equals(var)) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (referencesVar(c, var)) {
                return true;
            }
        }
        return false;
    }

    /** Per-getAll temporal context (M3 propagation): the root fetch's dates
     * and strategy flow to SAME-STRATEGY targets navigated through temporal
     * parents (engine: milestoning context does NOT propagate through
     * non-temporal intermediates). Set at each getAll resolution entry. */
    private java.util.Map<String, TemporalFrame.TemporalSpec> temporalByHead = java.util.Map.of();






    private static com.legend.compiler.spec.typed.TypedEnumValue leftKind() {
        String fqn = "meta::pure::functions::relation::JoinKind";
        return new com.legend.compiler.spec.typed.TypedEnumValue(fqn, "LEFT",
                new com.legend.compiler.element.type.ExprType(
                        new com.legend.compiler.element.type.Type.EnumType(fqn),
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
    private static Set<String> existsInnerLeaves(java.util.List<TypedSpec> ops,
            String head) {
        Set<String> out = new java.util.LinkedHashSet<>();
        for (TypedSpec op : ops) {
            if (op instanceof TypedFilter f) {
                for (TypedSpec b : f.predicate().body()) {
                    collectExistsInnerLeaves(b,
                            f.predicate().parameters().get(0), head, out);
                }
            }
        }
        return out;
    }

    private static void collectExistsInnerLeaves(TypedSpec n, String userVar,
            String head, Set<String> out) {
        if (n instanceof com.legend.compiler.spec.typed.TypedNativeCall c
                && !c.args().isEmpty()) {
            // unwrap ->filter(f) chains on the receiver: their lambdas
            // demand target leaves too (filter-wrapped emptiness)
            TypedSpec recv = c.args().get(0);
            java.util.List<TypedLambda> chainLams = new ArrayList<>();
            while (recv instanceof TypedFilter tf) {
                chainLams.add(tf.predicate());
                recv = tf.source();
            }
            java.util.List<String> p = Substitution.pathOf(recv, userVar);
            if (p != null && p.size() == 1 && p.get(0).equals(head)) {
                if (c.args().size() == 2
                        && c.args().get(1) instanceof TypedLambda lam
                        && !lam.parameters().isEmpty()) {
                    collectParamPathHeads(lam, lam.parameters().get(0), out);
                }
                for (TypedLambda cl : chainLams) {
                    if (!cl.parameters().isEmpty()) {
                        collectParamPathHeads(cl, cl.parameters().get(0), out);
                    }
                }
            }
        }
        // NO shadow-stop here (unlike the path funnel): the exists rewrite
        // resolves nested predicates through FRESH substitution scopes, so
        // the scan must reach them too — a blanket stop under-demanded a
        // constraint-derived shape (audit-13 B7 reverted; over-demand is
        // duplicate-safe under EXISTS).
        for (TypedSpec ch : n.children()) {
            collectExistsInnerLeaves(ch, userVar, head, out);
        }
    }

    /** Multi-hop paths consumed DIRECTLY under an emptiness-family call
     * ({@code isEmpty}/{@code isNotEmpty}/{@code exists} first arg) —
     * the class-typed-leaf EXISTS registration keys off these. */
    private static void collectEmptinessChainPaths(TypedSpec n, String userVar,
            Set<java.util.List<String>> out) {
        if (n instanceof TypedNativeCall c && !c.args().isEmpty()) {
            String key = c.callee().signatureKey();
            if (com.legend.builtin.Pure.nativeNamed("isEmpty", key)
                    || com.legend.builtin.Pure.nativeNamed("isNotEmpty", key)
                    || com.legend.builtin.Pure.nativeNamed("exists", key)) {
                java.util.List<String> p =
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
        java.util.List<String> p = Substitution.pathOf(n, param);
        if (p != null && !p.isEmpty()) {
            out.add(p.get(0));
        }
        for (TypedSpec ch : n.children()) {
            collectParamPathHeads(ch, param, out);
        }
    }








    /** Whether the pipeline contains a UNION (concatenate) anywhere. */
    static boolean containsConcatenate(TypedSpec pipeline) {
        if (pipeline instanceof com.legend.compiler.spec.typed.TypedConcatenate) {
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
    private static void collectParamColumnReads(TypedLambda cond, Set<String> out) {
        String src = cond.parameters().get(0);
        for (TypedSpec b : cond.body()) {
            collectVarColumnReads(b, src, out);
        }
    }

    private static void collectVarColumnReads(TypedSpec n, String var, Set<String> out) {
        if (n instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                && pa.source() instanceof com.legend.compiler.spec.typed.TypedVariable v
                && v.name().equals(var)) {
            out.add(pa.property());
        }
        for (TypedSpec c : n.children()) {
            collectVarColumnReads(c, var, out);
        }
    }

    /** Slot aliases a binding expression reads ($row.alias...). */
    static void collectAliasReads(TypedSpec n, String rowVar,
                                          Set<String> slotAliases, Set<String> out) {
        if (n instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                && pa.source() instanceof com.legend.compiler.spec.typed.TypedVariable v
                && v.name().equals(rowVar)
                && slotAliases.contains(pa.property())) {
            out.add(pa.property());
        }
        for (TypedSpec c : n.children()) {
            collectAliasReads(c, rowVar, slotAliases, out);
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
                        (com.legend.compiler.element.type.Type.RelationType)
                                m.pipeline().info().type(),
                        m.stripped(), m.slotPrefixes(),
                        temporal.milestoneColumnsOf(cs.pipeline(),
                                cs.classFqn())),
                new Substitution.Registries(assocs, assocEnds, existsSubs,
                        aggReads, isNotEmptyCallee(), equalCallee()),
                new Substitution.TemporalView(temporal.root().legacyDates(),
                        temporal.headTemporalDates()),
                filterPosition, false));
    }





    /** Any registered equal overload — membership-crossing emission. */
    private com.legend.compiler.element.TypedFunction equalCallee() {
        var fns = ctx.findFunction("equal");
        return fns.isEmpty() ? null : fns.get(0);
    }

    /** Any registered isNotEmpty overload — the lowerer dispatches by family. */
    private com.legend.compiler.element.TypedFunction isNotEmptyCallee() {
        var fns = ctx.findFunction("isNotEmpty");
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
                java.util.Set<String> seen = new java.util.LinkedHashSet<>();
                java.util.ArrayDeque<String> queue = new java.util.ArrayDeque<>(rt.mappings());
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
