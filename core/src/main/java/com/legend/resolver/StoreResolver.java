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

    private final ModelContext ctx;

    public StoreResolver(ModelContext ctx, SpecCompiler specs) {
        this.ctx = Objects.requireNonNull(ctx, "ctx");
        this.specs = Objects.requireNonNull(specs, "specs");
        this.sources = new ClassSources(ctx, specs);
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
    private record Context(String explicitMapping, String runtimeFqn) {
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
    private static boolean isObjectSpace(TypedSpec source) {
        return switch (source) {
            case TypedGetAll ignored -> true;
            case TypedFilter f -> isObjectSpace(f.source());
            case TypedLimit l -> isObjectSpace(l.source());
            case TypedDrop d -> isObjectSpace(d.source());
            case TypedSlice s -> isObjectSpace(s.source());
            case TypedSortBy sb -> isObjectSpace(sb.source());
            case TypedNativeCall c when isFirstLike(c) ->
                    isObjectSpace(c.args().get(0));
            case TypedNativeCall c when classSortOf(c) != null ->
                    isObjectSpace(c.args().get(0));
            default -> false;
        };
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
    private TypedSpec resolveObject(TypedSpec top, Context context) {
        // The relation-shaping TERMINAL: project or class-source groupBy
        // (lambdas through the one funnel), or the GRAPH terminals —
        // explicit serialize (graphFetch is source-preserving; serialize's
        // tree governs) and every other class-shaped root, which is the
        // IMPLICIT serialize over the class's scalar bindings (plan §E10).
        List<TypedGraphTree> tree = null;   // non-null => graph terminal
        boolean implicitSerialize = false;
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
            if (cur instanceof TypedNativeCall nc && isFirstLike(nc)) {
                cur = new TypedLimit(nc.args().get(0),
                        new TypedCInteger(1L, com.legend.compiler.element.type
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
        if (!g.milestoning().isEmpty()) {
            throw new MappingResolutionException("milestoned class fetch of '"
                    + g.classFqn() + "' is not supported yet (H-scope exclusion)",
                    g.classFqn());
        }
        ClassSource cs = sources.get(dispatch(context, g.classFqn()), g.classFqn(),
                target -> dispatch(context, target),
                (context.explicitMapping() == null ? "" : context.explicitMapping())
                        + '\u0000'
                        + (context.runtimeFqn() == null ? "" : context.runtimeFqn()));

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
        for (TypedSpec op : ops) {
            if (op instanceof TypedFilter f) {
                scanLambda(f.predicate(), filterPaths);
            }
            if (op instanceof TypedSortBy sb) {
                scanLambda(sb.key(), projectionPaths);
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
                scanLambda(fn, projectionPaths);
            }
        }
        Set<java.util.List<String>> paths = new java.util.LinkedHashSet<>(filterPaths);
        paths.addAll(projectionPaths);

        // Slot demand (heads whose bindings read join slots).
        Set<String> slotAliases = Pipelines.slotAliases(cs.pipeline());
        Set<String> demanded = new java.util.LinkedHashSet<>();
        if (!slotAliases.isEmpty()) {
            for (java.util.List<String> path : paths) {
                TypedSpec binding = cs.bindings().get(path.get(0));
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
        for (java.util.List<String> path : paths) {
            if (path.size() < 2) {
                continue;
            }
            TypedSpec headBinding = cs.bindings().get(path.get(0));
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
            String alias = navSlotAlias(navRead, cs.rowVar(), navSteps.keySet());
            if (alias == null || demandedNavs.contains(alias)) {
                continue;
            }
            demandedNavs.add(alias);
            var nav = navSteps.get(alias);
            String targetClass = ((com.legend.compiler.spec.typed.TypedGetAll)
                    nav.target()).classFqn();
            ClassSource target = sources.get(cs.mappingFqn(), targetClass);
            // The nav condition may read joinslot sub-rows: demand them too.
            for (TypedSpec b : nav.predicate().body()) {
                for (String slot : slotAliases) {
                    if (Pipelines.referencesAliasOn(b,
                            nav.predicate().parameters().get(0), Set.of(slot))) {
                        demanded.add(slot);
                    }
                }
            }
            assocs.put(path.get(0), new Substitution.AssocSub(alias + "_",
                    target.rowVar(), target.bindings(), target.classFqn(),
                    Pipelines.slotAliases(target.pipeline())));
        }
        demanded = Pipelines.closeOverConditions(cs.pipeline(), demanded);

        // Mapping ~distinct under a PROJECT terminal: the engine stamps
        // DISTINCT on the GENERATED select, not the class rows — defer the
        // pipeline's whole-row distinct (a no-op over a keyed table) to the
        // projected output. Deferral is sound ONLY when nothing between the
        // getAll and the project is row-count- or row-order-sensitive:
        // limit/drop/slice must run over the DEDUPED rows, and a sortBy's
        // ORDER BY would be buried beneath the deferred DISTINCT (audit —
        // both produce verified wrong answers). Filters commute with
        // whole-row dedup and stay deferral-safe.
        TypedSpec csPipe = cs.pipeline();
        boolean deferredDistinct = false;
        if (top instanceof TypedProject
                && ops.stream().allMatch(op -> op instanceof TypedFilter)
                && csPipe instanceof com.legend.compiler.spec.typed.TypedDistinct dd) {
            deferredDistinct = true;
            csPipe = dd.source();
        }
        Pipelines.Materialized m = Pipelines.materialize(
                csPipe, demanded, demandedNavs, cs.classFqn(),
                targetClass -> Pipelines.materialize(
                        sources.get(cs.mappingFqn(), targetClass).pipeline(),
                        Set.of(), targetClass).pipeline());

        // To-many association heads (1-hop, exists/isEmpty position):
        // correlated-EXISTS material — target pipeline + oriented condition,
        // NO join emitted (§133's single form).
        Map<String, Substitution.ExistsSub> existsSubs = new java.util.LinkedHashMap<>();
        for (java.util.List<String> path : paths) {
            String head = path.get(0);
            boolean filterTwoHop = path.size() == 2 && filterPaths.contains(path);
            if ((path.size() != 1 && !filterTwoHop)
                    || cs.bindings().containsKey(head)
                    || existsSubs.containsKey(head)) {
                continue;
            }
            var assocOpt = ctx.findAssociationOf(cs.classFqn(), head);
            if (assocOpt.isEmpty()) {
                continue;   // not an association — plain unmapped (loud later)
            }
            // ANY multiplicity: the EXISTS material is consumed only under
            // emptiness calls (plan rule: class-typed isEmpty/isNotEmpty of
            // any multiplicity, incl. to-one-optional, => [NOT] EXISTS); a
            // bare head not under an emptiness call still gets the honest
            // H4 story at substitution.
            AssocJoin aj = associationJoin(cs, head, context, true);
            var assocEnd = assocOpt.get().property1().propertyName().equals(head)
                    ? assocOpt.get().property1() : assocOpt.get().property2();
            boolean isToMany = !(assocEnd.multiplicity()
                    instanceof com.legend.parser.Multiplicity.Concrete emc
                    && Integer.valueOf(1).equals(emc.upperBound()));
            existsSubs.put(head, new Substitution.ExistsSub(aj.targetPipeline(),
                    aj.condition(), aj.target().rowVar(), aj.target().bindings(),
                    aj.targetRow(), aj.target().classFqn(),
                    Pipelines.slotAliases(aj.target().pipeline()), isToMany));
        }

        // Association demand: paths whose head is NOT a binding are
        // association navigations — ONE LEFT join PER HOP, chained by path
        // prefix (plan §2.3: longer prefixes chain off shorter; dedup by
        // chain key, so $p.dept.name and $p.dept.org.name share the dept
        // join). Target of each hop = its class's own pipeline.
        java.util.List<AssocJoin> assocJoins = new ArrayList<>();
        Map<String, AssocJoin> joinsByChain = new java.util.LinkedHashMap<>();
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
            if (path.size() < 2 || cs.bindings().containsKey(path.get(0))) {
                continue;   // 1-hop, or embedded/slot heads (substitution-side)
            }
            String head = path.get(0);
            // Filter-ONLY TO-MANY paths take the implicit-EXISTS route; a
            // projection-position to-many path joins with ROW EXPLOSION
            // (the unanimous reference semantics). A TO-ONE head always gets
            // its flat LEFT join even though ExistsSub material exists for it
            // (that material serves only explicit emptiness calls).
            boolean projectionPosition = projectionPaths.stream()
                    .anyMatch(pp -> pp.size() >= 2 && pp.get(0).equals(head));
            if (!projectionPosition && existsSubs.containsKey(head)
                    && existsSubs.get(head).toMany()) {
                continue;
            }
            ClassSource parent = cs;
            String parentPrefix = "";
            for (int hop = 0; hop + 1 < path.size(); hop++) {
                String chainKey = String.join(".", path.subList(0, hop + 1));
                AssocJoin known = joinsByChain.get(chainKey);
                if (known != null) {
                    parent = known.target();
                    parentPrefix = known.prefix();
                    continue;
                }
                AssocJoin aj = associationJoin(parent, path.get(hop), context, false,
                        leavesByChain.getOrDefault(chainKey, Set.of()));
                if (hop > 0) {
                    // A CHAINED hop: the parent's columns live PREFIXED on the
                    // accumulated joined row — re-point the condition's LEFT
                    // param reads (dept's raw $d.ID becomes dept_ID), and the
                    // hop's own prefix extends the chain (dept_org_) with the
                    // SAME collision guard hop 0 gets (audit: a physical
                    // dept.org_id FK would collide with the chained prefix).
                    String chainPrefix = chainedPrefix(
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
                    aj = new AssocJoin(chainPrefix, aj.target(), aj.targetPipeline(),
                            aj.targetRow(), cond, aj.targetSlotPrefixes());
                }
                assocJoins.add(aj);
                joinsByChain.put(chainKey, aj);
                assocs.put(chainKey, new Substitution.AssocSub(aj.prefix(),
                        aj.target().rowVar(), aj.target().bindings(),
                        aj.target().classFqn(),
                        Pipelines.slotAliases(aj.target().pipeline()),
                        aj.targetSlotPrefixes()));
                parent = aj.target();
                parentPrefix = aj.prefix();
            }
        }

        // 2b. Materialize the association joins (descriptor -> emission,
        //     first-demand order) onto the pipeline.
        TypedSpec withJoins = m.pipeline();
        for (AssocJoin aj : assocJoins) {
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
        m = new Pipelines.Materialized(withJoins, m.slotPrefixes(), m.stripped());

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
        for (AssocJoin aj : assocJoins) {
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
                        substitution(cs, m, assocs, assocEnds, existsSubs, true, fresh, f.predicate())
                                .rewriteLambda(f.predicate()),
                        pipeline.info());
                case TypedLimit l -> new TypedLimit(pipeline, l.count(), pipeline.info());
                case TypedDrop d -> new TypedDrop(pipeline, d.count(), pipeline.info());
                case TypedSlice sl -> new TypedSlice(pipeline, sl.start(), sl.stop(),
                        pipeline.info());
                case TypedSortBy sb -> new TypedSortBy(pipeline,
                        substitution(cs, m, assocs, assocEnds, existsSubs, false, fresh, sb.key()).rewriteLambda(sb.key()),
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
                substitution(cs, fm, assocs, assocEnds, existsSubs, false, fv, fn)
                        .rewriteLambda(fn);
        // An agg map may be the BARE instance var (x|$x : y|$y->count()) —
        // COUNT(*)-style; it becomes the identity over the row.
        java.util.function.Function<TypedAggCol, TypedAggCol> subAgg = a ->
                new TypedAggCol(a.name(),
                        isBareUserVar(a.map())
                                ? substitution(cs, fm, assocs, assocEnds, existsSubs,
                                        false, fv, a.map()).identityLambda(a.map())
                                : sub.apply(a.map()),
                        a.reduce());
        final boolean distinctOutput = deferredDistinct;
        return switch (top) {
            case TypedProject p -> {
                TypedSpec proj = new TypedProject(base,
                        p.columns().stream().map(col -> new TypedFuncCol(col.name(),
                                sub.apply(col.fn()))).toList(),
                        p.info());
                if (!distinctOutput) {
                    yield proj;
                }
                List<String> names = ((com.legend.compiler.element.type.Type.RelationType)
                        p.info().type()).columns().stream()
                        .map(com.legend.compiler.element.type.Type.Column::name).toList();
                yield new com.legend.compiler.spec.typed.TypedDistinct(proj, names, p.info());
            }
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
    private static List<TypedGraphTree> synthesizeScalarTree(ClassSource cs) {
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
        return tree;
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
                throw new MappingResolutionException("property '" + node.property()
                        + "' of class '" + cs.classFqn() + "' is not mapped in"
                        + " mapping '" + cs.mappingFqn() + "'", cs.classFqn());
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
        AssocJoin aj = associationJoin(cs, node.property(), context, /*forExists*/ true);
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
        AssocJoin aj = associationJoin(rawParent, assocProp, context, /*forExists*/ true);
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
    private static String navSlotAlias(TypedSpec binding, String rowVar,
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

    /** Scan entry: the lambda's BODY under its own parameter (never the lambda node). */
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

    /** A demanded association navigation, ready to emit as a prefixed LEFT join. */
    private record AssocJoin(String prefix, ClassSource target,
                             TypedSpec targetPipeline,
                             com.legend.compiler.element.type.Type.RelationType targetRow,
                             TypedLambda condition,
                             Map<String, String> targetSlotPrefixes) {}

    /**
     * A chained hop's prefix, ordinal-bumped against the ACCUMULATED column
     * set: the source row plus every already-registered join's prefixed
     * columns — the same guard {@link #prefixFor} gives hop 0.
     */
    private static String chainedPrefix(String base, ClassSource cs,
                                        Map<String, AssocJoin> joinsByChain) {
        Set<String> taken = new java.util.LinkedHashSet<>();
        for (com.legend.compiler.element.type.Type.Column c : cs.rowType().columns()) {
            taken.add(c.name());
        }
        for (AssocJoin aj : joinsByChain.values()) {
            for (com.legend.compiler.element.type.Type.Column c : aj.targetRow().columns()) {
                taken.add(aj.prefix() + c.name());
            }
        }
        String prefix = base + "_";
        int ordinal = 2;
        while (hasPrefixCollision(prefix, taken)) {
            prefix = base + "_" + ordinal++ + "_";
        }
        return prefix;
    }

    /** Deterministic prefix with ordinal bump on collision against the parent row (plan §2.3). */
    private static String prefixFor(String head, ClassSource cs) {
        Set<String> taken = new java.util.LinkedHashSet<>();
        for (com.legend.compiler.element.type.Type.Column c : cs.rowType().columns()) {
            taken.add(c.name());
        }
        String prefix = head + "_";
        int ordinal = 2;
        while (hasPrefixCollision(prefix, taken)) {
            prefix = head + "_" + ordinal++ + "_";
        }
        return prefix;
    }

    private static boolean hasPrefixCollision(String prefix, Set<String> taken) {
        for (String t : taken) {
            if (t.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

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
    private AssocJoin associationJoin(ClassSource cs, String head, Context context,
                                      boolean forExists) {
        return associationJoin(cs, head, context, forExists, Set.of());
    }

    private AssocJoin associationJoin(ClassSource cs, String head, Context context,
                                      boolean forExists, Set<String> demandedLeaves) {
        var assoc = ctx.findAssociationOf(cs.classFqn(), head).orElseThrow(() ->
                new MappingResolutionException("property '" + head + "' of class '"
                        + cs.classFqn() + "' is not mapped in mapping '"
                        + cs.mappingFqn() + "'", cs.classFqn()));
        // The end from the SAME association object — a separate index lookup
        // was a split-brain with findAssociationOf (audit blocker).
        var end = assoc.property1().propertyName().equals(head)
                ? assoc.property1() : assoc.property2();
        // A CONCRETE end joins: to-one flat, to-many with ROW EXPLOSION
        // (projection semantics — engine/V1/plangen unanimous). A
        // Parameter-multiplicity end stays denied (unknown cardinality).
        if (!forExists
                && !(end.multiplicity() instanceof com.legend.parser.Multiplicity.Concrete)) {
            throw new NotImplementedException("navigation of association end '$"
                    + head + "' with non-concrete multiplicity "
                    + end.multiplicity() + " is not supported");
        }
        String targetClass = ((com.legend.parser.TypeExpression.NameRef)
                end.targetClass()).name();
        ClassSource target = sources.get(cs.mappingFqn(), targetClass);
        // The TARGET's own join slots materialize on demand too: a demanded
        // leaf whose binding reads a slot ($p.firm.country where country is
        // @FirmCountry-mapped) pulls that slot's LEFT join into the target
        // pipeline — nested navigation joins, the W4 slice.
        Set<String> targetSlots = Pipelines.slotAliases(target.pipeline());
        Set<String> targetDemand = new java.util.LinkedHashSet<>();
        if (!targetSlots.isEmpty()) {
            for (String leaf : demandedLeaves) {
                TypedSpec b = target.bindings().get(leaf);
                if (b != null) {
                    collectAliasReads(b, target.rowVar(), targetSlots, targetDemand);
                }
            }
        }
        targetDemand = Pipelines.closeOverConditions(target.pipeline(), targetDemand);
        Pipelines.Materialized tMat = Pipelines.materialize(
                target.pipeline(), targetDemand, target.classFqn());

        // The predicate function: mapping's AssociationBinding for the assoc.
        var mapping = ctx.findMapping(cs.mappingFqn()).orElseThrow();
        var binding = mapping.associationBindings().stream()
                .filter(ab -> ab.associationFqn().equals(assoc.qualifiedName()))
                .findFirst()
                .orElseThrow(() -> new MappingResolutionException("association '"
                        + assoc.qualifiedName() + "' is not mapped in mapping '"
                        + cs.mappingFqn() + "'", assoc.qualifiedName()));
        var fns = ctx.findFunction(binding.predicateFunctionFqn());
        if (fns.size() != 1) {
            throw new IllegalStateException("resolver bug: association predicate '"
                    + binding.predicateFunctionFqn() + "' has " + fns.size() + " overloads");
        }
        var cf = specs.compile(fns.get(0));
        TypedSpec last = cf.body().get(cf.body().size() - 1);
        if (!(last instanceof com.legend.compiler.spec.typed.TypedNativeCall call)
                || !call.callee().qualifiedName().equals("meta::legend::lite::legacyAssocPredicate")
                || call.args().size() != 5
                || !(call.args().get(4) instanceof TypedLambda cond)) {
            throw new IllegalStateException("resolver bug: association predicate body"
                    + " for '" + assoc.qualifiedName() + "' is not the"
                    + " legacyAssocPredicate(a,b,src,tgt,cond) emission: "
                    + last.getClass().getSimpleName());
        }
        // ORIENTATION: the predicate fn's params are (a: classA, b: classB)
        // and the cond's (srcRow, tgtRow) are their tables' rows in that
        // order (H1's emission). The TypedJoin condition binds
        // (leftRow=PARENT, rightRow=TARGET): if the parent is classB the
        // params reverse. Self-associations (parent == target) cannot
        // orient by class — the emission convention puts {target} (the
        // navigated destination when traversing property1) on tgtRow, so
        // property1 keeps the order and property2 reverses (pinned by the
        // executing self-association fixture).
        String classAFqn = ((com.legend.compiler.element.type.Type.ClassType)
                fns.get(0).parameters().get(0).type()).fqn();
        if (!classAFqn.equals(cs.classFqn()) && !classAFqn.equals(targetClass)) {
            throw new IllegalStateException("resolver bug: association predicate '"
                    + binding.predicateFunctionFqn() + "' first param class '"
                    + classAFqn + "' is neither parent '" + cs.classFqn()
                    + "' nor target '" + targetClass + "'");
        }
        boolean reverse = cs.classFqn().equals(targetClass)
                ? !assoc.property1().propertyName().equals(head)
                : !cs.classFqn().equals(classAFqn);
        TypedLambda oriented = cond;
        if (reverse) {
            var ft = (com.legend.compiler.element.type.Type.FunctionType)
                    cond.info().type();
            var swapped = new com.legend.compiler.element.type.Type.FunctionType(
                    java.util.List.of(ft.params().get(1), ft.params().get(0)),
                    ft.result());
            oriented = new TypedLambda(java.util.List.of(cond.parameters().get(1),
                    cond.parameters().get(0)), cond.body(),
                    new com.legend.compiler.element.type.ExprType(swapped,
                            com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
        }
        return new AssocJoin(prefixFor(head, cs), target, tMat.pipeline(),
                (com.legend.compiler.element.type.Type.RelationType)
                        tMat.pipeline().info().type(),
                oriented, tMat.slotPrefixes());
    }

    /** Slot aliases a binding expression reads ($row.alias...). */
    private static void collectAliasReads(TypedSpec n, String rowVar,
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
                                      boolean filterPosition,
                                      String freshRowVar, TypedLambda userLambda) {
        return new Substitution(new Substitution.Target(
                userLambda.parameters().get(0), freshRowVar,
                cs.classFqn(), cs.mappingFqn(), cs.rowVar(), cs.bindings(),
                (com.legend.compiler.element.type.Type.RelationType)
                        m.pipeline().info().type(),
                m.stripped(), m.slotPrefixes(), assocs, assocEnds, existsSubs,
                isNotEmptyCallee(), filterPosition, false));
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
