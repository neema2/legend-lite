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
                            instanceof com.legend.compiler.element.type.Type.ClassType) ->
                    resolveChain(scalarMapAsProject(m.source(), m.mapper(),
                            m.info().multiplicity()), context);
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
     * Temporal fetch {@code Class.all(%date)}: filter the materialized
     * pipeline by the main table's milestoning columns for the CLASS's
     * temporal dimension (engine {@code milestoningCanSupportTemporalStrategy}
     * — a processing-temporal class on a bi-temporal table must filter the
     * PROCESSING columns, never whichever block happens to be declared
     * first). Range form {@code from <= d AND thru > d} flips to
     * {@code from < d AND thru >= d} under the block's inclusivity flag;
     * {@code %latest} selects {@code thru = INFINITY_DATE}, which the
     * engine REQUIRES the table to declare (milestoning.pure
     * getInfinityDate assert) — never a hardcoded constant.
     */
    private TypedSpec milestonedPipe(TypedSpec pipe, TypedSpec date, String classFqn) {
        String strategy = temporalStrategy(classFqn);
        if (strategy == null) {
            throw new MappingResolutionException("milestoned fetch of '" + classFqn
                    + "': the class declares no temporal stereotype", classFqn);
        }
        return milestonedPipeByStrategy(pipe, date, strategy, classFqn);
    }

    /**
     * The temporal point filter for a pipeline whose ROOT TABLE carries a
     * milestoning block for {@code strategy} — shared by class fetches
     * (strategy from the class stereotype) and PHYSICAL join targets
     * (strategy from the QUERY's temporal context: the engine filters
     * EVERY milestoned table alias in the generated query).
     */
    private TypedSpec milestonedPipeByStrategy(TypedSpec pipe, TypedSpec date,
            String strategy, String classFqn) {
        com.legend.compiler.spec.typed.TypedTableReference root = rootTable(pipe);
        var ms = root == null ? null
                : ctx.findTableMilestoning(root.store(), root.table()).orElse(null);
        String fromCol;
        String thruCol;
        String snapCol;
        boolean inclusive;
        String infinity;
        if (strategy.equals("businesstemporal")) {
            var b = ms == null ? null : ms.business();
            if (b == null) {
                // CAPABILITY TOLERANCE (engine relationalElementCanSupport-
                // Strategy + testLatestIgnoredForNonMilestonedMapped
                // goldens): a table that cannot support the strategy is
                // silently UNFILTERED, never an error
                return pipe;
            }
            fromCol = b.from();
            thruCol = b.thru();
            snapCol = b.snapshotDate();
            inclusive = b.thruIsInclusive();
            infinity = b.infinityDate();
        } else if (strategy.equals("processingtemporal")) {
            var p = ms == null ? null : ms.processing();
            if (p == null) {
                return pipe;   // capability tolerance — see above
            }
            fromCol = p.in();
            thruCol = p.out();
            snapCol = p.snapshotDate();
            inclusive = p.outIsInclusive();
            infinity = p.infinityDate();
        } else {
            throw new MappingResolutionException("bi-temporal class fetch of '"
                    + classFqn + "' is not supported yet", classFqn);
        }
        if (snapCol == null && (fromCol == null || thruCol == null)) {
            return pipe;   // capability tolerance — see above
        }
        if (!(date instanceof com.legend.compiler.spec.typed.TypedCDate
                || date instanceof com.legend.compiler.spec.typed.TypedCLatestDate)) {
            throw new MappingResolutionException("milestoned fetch of '" + classFqn
                    + "' with a non-literal date is not supported yet", classFqn);
        }
        // VIEW-backed pipes: the view row does not carry the milestone
        // columns — the engine filters every TABLE ALIAS, so the filter
        // pushes down to the internal scan (whose row has them)
        if (!pipeRowHasMilestoneCols(pipe, fromCol, thruCol, snapCol)
                && root != null
                && pipeRowHasMilestoneCols(root, fromCol, thruCol, snapCol)) {
            // TOLERANT per-scan wrap: a PARTIALLY milestoned union filters
            // only its milestoned members (engine: per-table-alias filters)
            final TypedSpec fdate = date;
            // tolerant: only members whose table declares THIS dimension's
            // block filter (a partially milestoned union keeps its other
            // members raw — audit 10 dropped the over-broad disjunct that
            // admitted other-dimension tables into a throwing path)
            return replaceScan(pipe, sc -> tableHasBlock(sc, strategy)
                    ? milestonedPipeByStrategy(sc, fdate, strategy, classFqn)
                    : sc);
        }
        com.legend.compiler.element.type.Type.RelationType row = (com.legend.compiler.element.type.Type.RelationType) pipe.info().type();
        String v = "ms_row";
        com.legend.compiler.element.type.ExprType rowT =
                new com.legend.compiler.element.type.ExprType(row,
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
        java.util.function.Function<String, TypedSpec> col = name -> {
            com.legend.compiler.element.type.Type.Column c = row.columns().stream()
                    .filter(x -> x.name().equalsIgnoreCase(name)).findFirst()
                    .orElseThrow(() -> new MappingResolutionException(
                            "milestoning column '" + name + "' is not on the"
                                    + " pipeline row of '" + classFqn + "'", classFqn));
            return new com.legend.compiler.spec.typed.TypedPropertyAccess(
                    new com.legend.compiler.spec.typed.TypedVariable(v, rowT),
                    c.name(), new com.legend.compiler.element.type.ExprType(
                            c.type(), c.multiplicity()));
        };
        com.legend.compiler.element.type.ExprType boolT =
                new com.legend.compiler.element.type.ExprType(
                        com.legend.compiler.element.type.Type.Primitive.BOOLEAN,
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
        TypedSpec cond;
        if (snapCol != null) {
            // SNAPSHOT milestoning: the fetch date selects its snapshot rows.
            // A DATETIME param TRUNCATES to the date (engine golden:
            // `snapshotDate = cast(truncate(ts) as date)`).
            if (date instanceof com.legend.compiler.spec.typed.TypedCLatestDate) {
                throw new MappingResolutionException("%latest over a SNAPSHOT-"
                        + "milestoned table is not supported yet", classFqn);
            }
            TypedSpec snapDate = date;
            boolean snapColIsDate = row.columns().stream()
                    .filter(x -> x.name().equalsIgnoreCase(snapCol)).findFirst()
                    .map(x -> x.type() == com.legend.compiler.element.type.Type
                            .Primitive.STRICT_DATE)
                    .orElse(true);
            if (snapColIsDate
                    && date instanceof com.legend.compiler.spec.typed.TypedCDate cd
                    && !(cd.value()
                            instanceof com.legend.values.PureDateLiteral.StrictDate)) {
                String iso = cd.value().toEngineString();
                if (iso.length() >= 10) {
                    snapDate = new com.legend.compiler.spec.typed.TypedCDate(
                            com.legend.values.PureDateLiteral.parse(
                                    iso.substring(0, 10)),
                            new com.legend.compiler.element.type.ExprType(
                                    com.legend.compiler.element.type.Type
                                            .Primitive.STRICT_DATE,
                                    com.legend.compiler.element.type
                                            .Multiplicity.Bounded.ONE));
                }
            }
            cond = cmpCall("meta::pure::functions::boolean::equal",
                    col.apply(snapCol), snapDate, boolT);
        } else if (date instanceof com.legend.compiler.spec.typed.TypedCLatestDate) {
            if (infinity == null) {
                // engine: getInfinityDate ASSERTS the declaration — a
                // defaulted constant would silently return zero rows for
                // any table milestoned with a different infinity date
                throw new MappingResolutionException("%latest usage for"
                        + " temporal fetch of '" + classFqn + "' requires"
                        + " table '" + root.table() + "' to specify a"
                        + " milestoning 'INFINITY_DATE'", classFqn);
            }
            com.legend.compiler.element.type.ExprType dt =
                    new com.legend.compiler.element.type.ExprType(
                            com.legend.compiler.element.type.Type.Primitive.DATE_TIME,
                            com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
            cond = cmpCall("meta::pure::functions::boolean::equal",
                    col.apply(thruCol),
                    new com.legend.compiler.spec.typed.TypedCDate(
                            com.legend.values.PureDateLiteral.parse(
                                    infinity.startsWith("%")
                                            ? infinity.substring(1) : infinity),
                            dt),
                    boolT);
        } else if (inclusive) {
            // THRU/OUT_IS_INCLUSIVE=true: engine flips both boundary
            // operators — from < d AND thru >= d
            cond = cmpCall("meta::pure::functions::boolean::and",
                    dateCmpCall("meta::pure::functions::boolean::lessThan",
                            col.apply(fromCol), date, boolT),
                    dateCmpCall("meta::pure::functions::boolean::greaterThanEqual",
                            col.apply(thruCol), date, boolT),
                    boolT);
        } else {
            cond = cmpCall("meta::pure::functions::boolean::and",
                    dateCmpCall("meta::pure::functions::boolean::lessThanEqual",
                            col.apply(fromCol), date, boolT),
                    dateCmpCall("meta::pure::functions::boolean::greaterThan",
                            col.apply(thruCol), date, boolT),
                    boolT);
        }
        TypedLambda pred = new TypedLambda(java.util.List.of(v),
                java.util.List.of(cond),
                new com.legend.compiler.element.type.ExprType(
                        new com.legend.compiler.element.type.Type.FunctionType(
                                java.util.List.of(new com.legend.compiler.element.type.Type.Param(row,
                                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE)),
                                new com.legend.compiler.element.type.Type.Param(
                                        com.legend.compiler.element.type.Type.Primitive.BOOLEAN,
                                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE)),
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
        return new com.legend.compiler.spec.typed.TypedFilter(pipe, pred, pipe.info());
    }

    /**
     * ENGINE RULE: every milestoned TABLE alias in the generated query gets
     * the temporal filter for the query's temporal context — including
     * PHYSICAL joinslot targets (join PMs like {@code @Product_Classification}
     * feeding a scalar/enum read) and demanded navigate targets, which are
     * not class pipelines and so never pass temporalTargetPipe. Applied to
     * the MATERIALIZED pipeline before the association joins append (those
     * targets are class-filtered separately — no double filter).
     */
    private TypedSpec applyJoinTemporalFilters(TypedSpec n, ClassSource cs,
            Map<String, String> navPrefixToClass) {
        // ROOT context absent: physical joinslot targets have nothing to
        // filter by, but CLASS-typed navigate targets may carry EXPLICIT
        // property-function dates (temporalByHead) — those still apply
        // (a non-temporal root navigating $p.firm(%d) filters firm's
        // versions; audit: the lifted union navigate joined unfiltered)
        if ((rootMilestoning.isEmpty() || rootStrategy == null)
                && (navPrefixToClass.isEmpty() || temporalByHead.isEmpty())) {
            return n;
        }
        return switch (n) {
            case com.legend.compiler.spec.typed.TypedJoin j -> {
                TypedSpec right = j.right();
                String navClass = j.prefix()
                        .map(navPrefixToClass::get).orElse(null);
                TypedSpec filtered;
                if (navClass != null) {
                    // CLASS-typed navigate target: governed by the TARGET
                    // CLASS's temporality (a non-temporal class mapped to a
                    // temporal table gets NO filter — corpus
                    // testMilestoningFiltersNotPropogated... golden)
                    String head = j.prefix().get().substring(0,
                            j.prefix().get().length() - 1);
                    filtered = temporalTargetPipe(cs,
                            sources.get(cs.mappingFqn(), navClass), head, right);
                } else if ("bitemporal".equals(rootStrategy)
                        && rootMilestoning.size() == 2) {
                    filtered = right;
                    if (tableHasBlock(filtered, "processingtemporal")) {
                        filtered = milestonedPipeByStrategy(filtered,
                                rootMilestoning.get(0), "processingtemporal",
                                "join target");
                    }
                    if (tableHasBlock(filtered, "businesstemporal")) {
                        filtered = milestonedPipeByStrategy(filtered,
                                rootMilestoning.get(1), "businesstemporal",
                                "join target");
                    }
                } else if (rootStrategy != null && !rootMilestoning.isEmpty()) {
                    // PHYSICAL joinslot target: every milestoned table alias
                    // in the query filters by the temporal context
                    filtered = tableHasBlock(right, rootStrategy)
                            ? milestonedPipeByStrategy(right,
                                    rootMilestoning.get(0), rootStrategy,
                                    "join target")
                            : right;
                } else {
                    filtered = right;   // no root context; no head date here
                }
                yield new com.legend.compiler.spec.typed.TypedJoin(
                        applyJoinTemporalFilters(j.left(), cs, navPrefixToClass),
                        filtered, j.kind(), j.condition(), j.prefix(), j.info());
            }
            case TypedFilter f -> new TypedFilter(
                    applyJoinTemporalFilters(f.source(), cs, navPrefixToClass),
                    f.predicate(), f.info());
            case com.legend.compiler.spec.typed.TypedDistinct d ->
                    new com.legend.compiler.spec.typed.TypedDistinct(
                            applyJoinTemporalFilters(d.source(), cs, navPrefixToClass),
                            d.columns(), d.info());
            case TypedSelect sel -> new TypedSelect(
                    applyJoinTemporalFilters(sel.source(), cs, navPrefixToClass),
                    sel.columns(), sel.info());
            case TypedProject pr -> new TypedProject(
                    applyJoinTemporalFilters(pr.source(), cs, navPrefixToClass),
                    pr.columns(), pr.info());
            case com.legend.compiler.spec.typed.TypedConcatenate cc ->
                    new com.legend.compiler.spec.typed.TypedConcatenate(
                            applyJoinTemporalFilters(cc.left(), cs, navPrefixToClass),
                            applyJoinTemporalFilters(cc.right(), cs, navPrefixToClass),
                            cc.info());
            default -> {
                // LOUD on unrecognized shapes carrying joins (audit 10): a
                // silently skipped milestoned join target fans out versions
                if (containsJoinToMilestoned(n)) {
                    throw new MappingResolutionException("temporal join-target"
                            + " filtering through "
                            + n.getClass().getSimpleName()
                            + " is not supported yet", "");
                }
                yield n;
            }
        };
    }

    private boolean containsJoinToMilestoned(TypedSpec n) {
        if (n instanceof com.legend.compiler.spec.typed.TypedJoin j
                && (tableHasBlock(j.right(), "businesstemporal")
                        || tableHasBlock(j.right(), "processingtemporal"))) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (containsJoinToMilestoned(c)) {
                return true;
            }
        }
        return false;
    }

    /** The pipe's root table declares a milestoning block for {@code strategy}. */
    private boolean tableHasBlock(TypedSpec pipe, String strategy) {
        com.legend.compiler.spec.typed.TypedTableReference root = rootTable(pipe);
        var ms = root == null ? null
                : ctx.findTableMilestoning(root.store(), root.table()).orElse(null);
        if (ms == null) {
            return false;
        }
        return strategy.equals("businesstemporal") ? ms.business() != null
                : strategy.equals("processingtemporal") && ms.processing() != null;
    }

    /** The pipe's TOP row carries the milestone columns the block needs. */
    private static boolean pipeRowHasMilestoneCols(TypedSpec pipe, String fromCol,
            String thruCol, String snapCol) {
        if (!(pipe.info().type()
                instanceof com.legend.compiler.element.type.Type.RelationType row)) {
            return false;
        }
        java.util.function.Predicate<String> has = name -> name != null
                && row.columns().stream()
                        .anyMatch(c -> c.name().equalsIgnoreCase(name));
        return snapCol != null ? has.test(snapCol)
                : has.test(fromCol) && has.test(thruCol);
    }

    /** Rebuild {@code pipe} with its deepest LEFT-spine scan wrapped. */
    private static TypedSpec replaceScan(TypedSpec pipe,
            java.util.function.UnaryOperator<TypedSpec> wrap) {
        return switch (pipe) {
            case com.legend.compiler.spec.typed.TypedTableReference t -> wrap.apply(t);
            case TypedFilter f -> new TypedFilter(replaceScan(f.source(), wrap),
                    f.predicate(), f.info());
            case TypedSelect sel -> new TypedSelect(replaceScan(sel.source(), wrap),
                    sel.columns(), sel.info());
            case com.legend.compiler.spec.typed.TypedDistinct d ->
                    new com.legend.compiler.spec.typed.TypedDistinct(
                            replaceScan(d.source(), wrap), d.columns(), d.info());
            case TypedProject pr -> new TypedProject(replaceScan(pr.source(), wrap),
                    pr.columns(), pr.info());
            case com.legend.compiler.spec.typed.TypedJoin j ->
                    new com.legend.compiler.spec.typed.TypedJoin(
                            replaceScan(j.left(), wrap),
                            j.right() instanceof com.legend.compiler.spec.typed
                                    .TypedTableReference rt
                                    ? wrap.apply(rt) : j.right(),
                            j.kind(), j.condition(), j.prefix(), j.info());
            case com.legend.compiler.spec.typed.TypedJoinSlot js ->
                    new com.legend.compiler.spec.typed.TypedJoinSlot(
                            replaceScan(js.source(), wrap), js.alias(), js.target(),
                            js.condition(), js.info());
            // a UNION pipeline: the temporal filter applies to EACH member
            // (every table alias filters — engine rule, per member scan)
            case com.legend.compiler.spec.typed.TypedConcatenate c ->
                    new com.legend.compiler.spec.typed.TypedConcatenate(
                            replaceScan(c.left(), wrap),
                            replaceScan(c.right(), wrap), c.info());
            default -> throw new MappingResolutionException(
                    "milestone filter pushdown through "
                            + pipe.getClass().getSimpleName()
                            + " is not supported yet", "");
        };
    }

    /**
     * {@code Class.allVersionsInRange(start, end)}: versions whose validity
     * window OVERLAPS the range — engine getTemporalMilestoneRangeFilter:
     * inclusive-thru blocks use {@code from < end AND thru >= start}, else
     * {@code from <= end AND thru > start}; snapshot milestoning selects
     * {@code start <= snap AND snap <= end}. %latest is not a valid range
     * bound (the engine asserts).
     */
    private TypedSpec rangeMilestonedPipe(TypedSpec pipe, TypedSpec start,
            TypedSpec end, String classFqn) {
        com.legend.compiler.spec.typed.TypedTableReference root = rootTable(pipe);
        var ms = root == null ? null
                : ctx.findTableMilestoning(root.store(), root.table()).orElse(null);
        String strategy = temporalStrategy(classFqn);
        if (strategy == null) {
            throw new MappingResolutionException("allVersionsInRange of '" + classFqn
                    + "': the class declares no temporal stereotype", classFqn);
        }
        String fromCol;
        String thruCol;
        String snapCol;
        boolean inclusive;
        if (strategy.equals("businesstemporal")) {
            var b = ms == null ? null : ms.business();
            if (b == null) {
                return pipe;   // capability tolerance (engine gating)
            }
            fromCol = b.from();
            thruCol = b.thru();
            snapCol = b.snapshotDate();
            inclusive = b.thruIsInclusive();
        } else if (strategy.equals("processingtemporal")) {
            var pr = ms == null ? null : ms.processing();
            if (pr == null) {
                return pipe;   // capability tolerance (engine gating)
            }
            fromCol = pr.in();
            thruCol = pr.out();
            snapCol = pr.snapshotDate();
            inclusive = pr.outIsInclusive();
        } else {
            throw new MappingResolutionException("bi-temporal allVersionsInRange"
                    + " of '" + classFqn + "' is not supported yet", classFqn);
        }
        if (start instanceof com.legend.compiler.spec.typed.TypedCLatestDate
                || end instanceof com.legend.compiler.spec.typed.TypedCLatestDate) {
            // engine: '%latest not a valid parameter for allVersionsInRange'
            throw new MappingResolutionException("%latest is not a valid"
                    + " parameter for allVersionsInRange", classFqn);
        }
        com.legend.compiler.element.type.Type.RelationType row =
                (com.legend.compiler.element.type.Type.RelationType) pipe.info().type();
        String v = "ms_row";
        com.legend.compiler.element.type.ExprType rowT =
                new com.legend.compiler.element.type.ExprType(row,
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
        java.util.function.Function<String, TypedSpec> col = name -> {
            com.legend.compiler.element.type.Type.Column c = row.columns().stream()
                    .filter(x -> x.name().equalsIgnoreCase(name)).findFirst()
                    .orElseThrow(() -> new MappingResolutionException(
                            "milestoning column '" + name + "' is not on the"
                                    + " pipeline row of '" + classFqn + "'", classFqn));
            return new com.legend.compiler.spec.typed.TypedPropertyAccess(
                    new com.legend.compiler.spec.typed.TypedVariable(v, rowT),
                    c.name(), new com.legend.compiler.element.type.ExprType(
                            c.type(), c.multiplicity()));
        };
        com.legend.compiler.element.type.ExprType boolT =
                new com.legend.compiler.element.type.ExprType(
                        com.legend.compiler.element.type.Type.Primitive.BOOLEAN,
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
        TypedSpec cond;
        if (snapCol != null) {
            cond = cmpCall("meta::pure::functions::boolean::and",
                    dateCmpCall("meta::pure::functions::boolean::greaterThanEqual",
                            col.apply(snapCol), start, boolT),
                    dateCmpCall("meta::pure::functions::boolean::lessThanEqual",
                            col.apply(snapCol), end, boolT),
                    boolT);
        } else if (inclusive) {
            cond = cmpCall("meta::pure::functions::boolean::and",
                    dateCmpCall("meta::pure::functions::boolean::lessThan",
                            col.apply(fromCol), end, boolT),
                    dateCmpCall("meta::pure::functions::boolean::greaterThanEqual",
                            col.apply(thruCol), start, boolT),
                    boolT);
        } else {
            cond = cmpCall("meta::pure::functions::boolean::and",
                    dateCmpCall("meta::pure::functions::boolean::lessThanEqual",
                            col.apply(fromCol), end, boolT),
                    dateCmpCall("meta::pure::functions::boolean::greaterThan",
                            col.apply(thruCol), start, boolT),
                    boolT);
        }
        TypedLambda pred = new TypedLambda(java.util.List.of(v),
                java.util.List.of(cond),
                new com.legend.compiler.element.type.ExprType(
                        new com.legend.compiler.element.type.Type.FunctionType(
                                java.util.List.of(new com.legend.compiler.element
                                        .type.Type.Param(row,
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ONE)),
                                new com.legend.compiler.element.type.Type.Param(
                                        com.legend.compiler.element.type.Type
                                                .Primitive.BOOLEAN,
                                        com.legend.compiler.element.type
                                                .Multiplicity.Bounded.ONE)),
                        com.legend.compiler.element.type.Multiplicity.Bounded.ONE));
        return new com.legend.compiler.spec.typed.TypedFilter(pipe, pred, pipe.info());
    }

    /**
     * The class's temporal stereotype ({@code <<temporal.businesstemporal>>}
     * etc., inherited through superclasses), or {@code null} for a
     * non-temporal class. Drives which milestoning block filters the fetch
     * — engine {@code milestoningCanSupportTemporalStrategy}.
     */
    private String temporalStrategy(String classFqn) {
        return com.legend.compiler.element.Temporal.strategyOf(ctx, classFqn);
    }

    /**
     * The SOLE 2-arg overload of {@code fqn} ({@code and}/{@code equal}) —
     * loud if the catalog ever gains a second one, so a reorder can never
     * silently stamp a different signature.
     */
    private TypedSpec cmpCall(String fqn, TypedSpec a, TypedSpec b,
            com.legend.compiler.element.type.ExprType out) {
        var fns = ctx.findFunction(fqn).stream()
                .filter(f -> f.parameters().size() == 2)
                .toList();
        if (fns.size() != 1) {
            throw new IllegalStateException("resolver bug: expected exactly"
                    + " one 2-arg overload of " + fqn + ", found " + fns.size());
        }
        return new com.legend.compiler.spec.typed.TypedNativeCall(fns.get(0),
                java.util.List.of(a, b), out);
    }

    /**
     * The Date×Date comparison overload of {@code fqn} — pinned by
     * parameter TYPE, never catalog order (the comparison family carries
     * Date, Number, String and Boolean overloads).
     */
    private TypedSpec dateCmpCall(String fqn, TypedSpec a, TypedSpec b,
            com.legend.compiler.element.type.ExprType out) {
        var fn = ctx.findFunction(fqn).stream()
                .filter(f -> f.parameters().size() == 2
                        && f.parameters().stream().allMatch(p ->
                                com.legend.compiler.element.type.Type.Primitive.DATE
                                        .equals(p.type())))
                .findFirst().orElseThrow(() -> new IllegalStateException(
                        "resolver bug: no Date,Date overload of " + fqn));
        return new com.legend.compiler.spec.typed.TypedNativeCall(fn,
                java.util.List.of(a, b), out);
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

    /** The LEFTMOST physical table of a materialized pipeline. */
    private static com.legend.compiler.spec.typed.TypedTableReference rootTable(TypedSpec n) {
        if (n instanceof com.legend.compiler.spec.typed.TypedTableReference tr) {
            return tr;
        }
        for (TypedSpec c : n.children()) {
            com.legend.compiler.spec.typed.TypedTableReference r = rootTable(c);
            if (r != null) {
                return r;
            }
        }
        return null;
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
    private TypedSpec resolveObject(TypedSpec top, Context context) {
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
                && temporalStrategy(g.classFqn()) != null) {
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
        rootMilestoning = java.util.List.of();   // audit 10: never read the
        rootStrategy = null;                     // PREVIOUS getAll's context
        rootMilestoning = g.versionSweep() ? java.util.List.of()
                : normalizeContextDates(g.milestoning());
        rootStrategy = temporalStrategy(g.classFqn());
        temporalByHead = java.util.Map.of();
        final Context fctx = chainContext;
        ClassSource cs = sources.get(dispatch(fctx, g.classFqn()), g.classFqn(),
                target -> dispatch(fctx, target),
                (fctx.explicitMapping() == null ? "" : fctx.explicitMapping())
                        + '\u0000'
                        + (fctx.runtimeFqn() == null ? "" : context.runtimeFqn()));

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
        java.util.Map<String, TemporalSpec> specs = new java.util.LinkedHashMap<>();
        for (TypedSpec op : ops) {
            if (op instanceof TypedFilter f) {
                collectTemporalSpecs(f.predicate(), specs);
            }
            if (op instanceof TypedSortBy sb) {
                collectTemporalSpecs(sb.key(), specs);
            }
        }
        if (tree == null) {
            for (TypedLambda fn : terminalLambdas(top)) {
                collectTemporalSpecs(fn, specs);
            }
        }
        temporalByHead = specs;

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
                    Pipelines.slotAliases(target.pipeline()), Map.of(), null, null,
                    milestoneColumnsOf(target.pipeline(), target.classFqn())));
        }
        demanded = Pipelines.closeOverConditions(cs.pipeline(), demanded);

        // Mapping ~distinct stays IN the pipeline (a distinct subselect —
        // the engine's own emission, its "could optimize to collapse" TODO
        // notwithstanding): deferring it to the projected output dedups
        // over the PROJECTED SUBSET of columns, which changes row counts
        // whenever the projection is not injective on the distinct tuple
        // (corpus testDistinctMappingSimpleProjectSelectOneOfTheDistinct-
        // Properties: name-only projection must keep BOTH 'IF 2' rows).
        TypedSpec csPipe = cs.pipeline();
        Pipelines.Materialized m = Pipelines.materialize(
                csPipe, demanded, demandedNavs, cs.classFqn(),
                targetClass -> Pipelines.materialize(
                        sources.get(cs.mappingFqn(), targetClass).pipeline(),
                        Set.of(), targetClass).pipeline());
        Map<String, String> navPrefixToClass = new java.util.LinkedHashMap<>();
        for (var navE : Pipelines.navSteps(cs.pipeline()).entrySet()) {
            if (navE.getValue().target()
                    instanceof com.legend.compiler.spec.typed.TypedGetAll tg2) {
                navPrefixToClass.put(navE.getKey() + "_", tg2.classFqn());
            }
        }
        final TypedSpec basePipe =
                applyJoinTemporalFilters(m.pipeline(), cs, navPrefixToClass);
        m = new Pipelines.Materialized(basePipe, m.slotPrefixes(), m.stripped());
        final TypedSpec materializedPipe;
        if (g.versionSweep()) {
            // allVersions(): the RAW extent — every version row, no filter.
            // allVersionsInRange(s, e): versions whose validity window
            // overlaps the range (engine getTemporalMilestoneRangeFilter).
            materializedPipe = g.milestoning().isEmpty() ? basePipe
                    : rangeMilestonedPipe(basePipe, g.milestoning().get(0),
                            g.milestoning().get(1), g.classFqn());
        } else if (g.milestoning().size() == 2
                && "bitemporal".equals(temporalStrategy(g.classFqn()))) {
            // BI-TEMPORAL fetch: .all(processingDate, businessDate) — real
            // pure's getAll(Class, processingDate, businessDate) signature;
            // both dimensions filter.
            materializedPipe = milestonedPipeByStrategy(
                    milestonedPipeByStrategy(basePipe, g.milestoning().get(0),
                            "processingtemporal", g.classFqn()),
                    g.milestoning().get(1), "businesstemporal", g.classFqn());
        } else if (g.milestoning().size() == 2) {
            // SINGLE-dimension class with two dates: the RANGE fetch —
            // engine getAll(Class, start, end), same filter as
            // allVersionsInRange
            materializedPipe = rangeMilestonedPipe(basePipe,
                    g.milestoning().get(0), g.milestoning().get(1), g.classFqn());
        } else {
            materializedPipe = g.milestoning().isEmpty()
                    ? basePipe
                    : milestonedPipe(basePipe, g.milestoning().get(0), g.classFqn());
        }

        // To-many association heads (1-hop, exists/isEmpty position):
        // correlated-EXISTS material — target pipeline + oriented condition,
        // NO join emitted (§133's single form).
        Map<String, Substitution.ExistsSub> existsSubs = new java.util.LinkedHashMap<>();
        for (java.util.List<String> path : paths) {
            String head = path.get(0);
            boolean filterTwoHop = path.size() == 2 && filterPaths.contains(path);
            if ((path.size() != 1 && !filterTwoHop) || existsSubs.containsKey(head)) {
                continue;
            }
            if (cs.bindings().containsKey(head)) {
                // A NAVIGATE-SLOT head (class-typed Join PM): the nav step
                // carries the target extent + oriented (s, t) predicate —
                // the same correlated-EXISTS material as an association end.
                var nav = Pipelines.navSteps(cs.pipeline()).get(head);
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
                Pipelines.Materialized tMat0 = Pipelines.materialize(
                        t.pipeline(), Set.of(), t.classFqn());
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
                Pipelines.Materialized tMat = new Pipelines.Materialized(
                        temporalTargetPipe(cs, t, head,
                                applyJoinTemporalFilters(tPipe0, t,
                                        java.util.Map.of())),
                        tMat0.slotPrefixes(), tMat0.stripped());
                boolean navToMany = !(ctx.findProperty(cs.classFqn(), head)
                        .map(pr -> pr.multiplicity())
                        .filter(mm -> mm instanceof com.legend.compiler.element.type
                                .Multiplicity.Bounded bb
                                && Integer.valueOf(1).equals(bb.upper()))
                        .isPresent());
                existsSubs.put(head, new Substitution.ExistsSub(tMat.pipeline(),
                        nav.predicate(), t.rowVar(), t.bindings(),
                        (com.legend.compiler.element.type.Type.RelationType)
                                tMat.pipeline().info().type(),
                        t.classFqn(), Pipelines.slotAliases(t.pipeline()), navToMany));
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
                        aj.targetSlotPrefixes(), null, null,
                        milestoneColumnsOf(aj.target().pipeline(),
                                aj.target().classFqn())));
                parent = aj.target();
                parentPrefix = aj.prefix();
            }
        }

        // 2a'. JOIN-KEY COLLECTION under mapping ~distinct (engine L5135):
        // demanded joins' source-side key columns must survive the
        // ~distinct narrowing select — widen it (the distinct then dedups
        // over the widened row, exactly the engine's query-dependent
        // distinct tuple). Aggregated-navigation materials build here so
        // their conditions participate.
        Map<String, AssocJoin> aggMaterials = new java.util.LinkedHashMap<>();
        for (var entry : aggDemands.entrySet()) {
            Set<String> leaves = new java.util.LinkedHashSet<>();
            for (AggDemand dm : entry.getValue()) {
                leaves.add(dm.leaf());
            }
            aggMaterials.put(entry.getKey(),
                    aggJoinMaterial(cs, entry.getKey(), context, leaves));
        }
        Set<String> joinKeyReads = new java.util.LinkedHashSet<>();
        for (AssocJoin aj : assocJoins) {
            collectParamColumnReads(aj.condition(), joinKeyReads);
        }
        for (AssocJoin aj : aggMaterials.values()) {
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

        // 2b. Materialize the association joins (descriptor -> emission,
        //     first-demand order) onto the pipeline.
        TypedSpec withJoins = keyWidenedPipe;
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
        // 2c. AGGREGATED navigations (the engine's subAggregation shape):
        // per to-many head, ONE grouped subselect — the target pipeline
        // grouped by the association's target-side equi-key columns (names
        // preserved, so the association condition joins it VERBATIM), one
        // aggregate column per demand. Each aggregate node then reads its
        // column off the joined row; no row explosion reaches the
        // projection, and the aggregate itself runs IN the database.
        java.util.Map<TypedSpec, Substitution.AggRead> aggReads =
                new java.util.IdentityHashMap<>();
        java.util.List<AssocJoin> aggAssocJoins = new ArrayList<>();
        for (var entry : aggDemands.entrySet()) {
            String head = entry.getKey();
            Set<String> leaves = new java.util.LinkedHashSet<>();
            for (AggDemand d : entry.getValue()) {
                leaves.add(d.leaf());
            }
            AssocJoin aj = aggMaterials.get(head);
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
            String prefix = prefixFor(head + "_agg", cs);
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
        for (AssocJoin aj : aggAssocJoins) {
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
    private void collectTemporalSpecs(TypedLambda lambda,
            java.util.Map<String, TemporalSpec> out) {
        collectTemporalSpecs(lambda.body(), lambda.parameters().get(0), out);
    }

    private void collectTemporalSpecs(java.util.List<TypedSpec> body, String userVar,
            java.util.Map<String, TemporalSpec> out) {
        for (TypedSpec b : body) {
            collectTemporalNodes(b, userVar, out);
        }
    }

    private void collectTemporalNodes(TypedSpec n, String userVar,
            java.util.Map<String, TemporalSpec> out) {
        if (n instanceof com.legend.compiler.spec.typed.TypedMilestonedAccess ma
                && !(ma.source()
                        instanceof com.legend.compiler.spec.typed.TypedVariable v0
                        && v0.name().equals(userVar))) {
            // audit 10: a NESTED milestoned access — dated OR sweep — would
            // silently take the ROOT date via propagation (a nested
            // ...AllVersions hop must be the RAW extent, engine
            // pureToSQLQuery isAllVersions skip); specs are keyed by bare
            // head today, so loud until chain-keyed specs land
            throw new NotImplementedException("milestoned property access '"
                    + ma.property() + "' on a NESTED navigation is not"
                    + " supported yet");
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedMilestonedAccess ma
                && ma.source() instanceof com.legend.compiler.spec.typed.TypedVariable v
                && v.name().equals(userVar)) {
            TemporalSpec spec = new TemporalSpec(
                    normalizeContextDates(ma.dates()), ma.sweep());
            TemporalSpec prior = out.putIfAbsent(ma.property(), spec);
            if (prior != null && !prior.equals(spec)) {
                throw new NotImplementedException("navigation '" + ma.property()
                        + "' with two different milestoning dates in one query"
                        + " is not supported yet");
            }
        }
        if (n instanceof TypedLambda l && l.parameters().contains(userVar)) {
            return;
        }
        for (TypedSpec c : n.children()) {
            collectTemporalNodes(c, userVar, out);
        }
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

    /** The join material for an aggregated to-many head: the association
     * route, or the navigate-slot route (class-typed Join PM). */
    private AssocJoin aggJoinMaterial(ClassSource cs, String head, Context context,
                                      Set<String> leaves) {
        TypedSpec binding = cs.bindings().get(head);
        if (binding == null) {
            return associationJoin(cs, head, context, false, leaves);
        }
        var navSteps = Pipelines.navSteps(cs.pipeline());
        String alias = navSlotAlias(binding, cs.rowVar(), navSteps.keySet());
        var nav = navSteps.get(alias);
        String targetClass = ((com.legend.compiler.spec.typed.TypedGetAll)
                nav.target()).classFqn();
        ClassSource t = sources.get(cs.mappingFqn(), targetClass);
        Set<String> targetSlots = Pipelines.slotAliases(t.pipeline());
        Set<String> targetDemand = new java.util.LinkedHashSet<>();
        if (!targetSlots.isEmpty()) {
            for (String leaf : leaves) {
                TypedSpec b = t.bindings().get(leaf);
                if (b != null) {
                    collectAliasReads(b, t.rowVar(), targetSlots, targetDemand);
                }
            }
        }
        targetDemand = Pipelines.closeOverConditions(t.pipeline(), targetDemand);
        Pipelines.Materialized tMat = Pipelines.materialize(
                t.pipeline(), targetDemand, t.classFqn());
        TypedSpec tPipe0 = temporalTargetPipe(cs, t, head,
                applyJoinTemporalFilters(tMat.pipeline(), t, java.util.Map.of()));
        return new AssocJoin(prefixFor(head, cs), t, tPipe0,
                (com.legend.compiler.element.type.Type.RelationType)
                        tPipe0.info().type(),
                nav.predicate(), tMat.slotPrefixes());
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

    /** The temporal arguments a milestoned property function supplied for a
     * navigation head ({@code product(%d)} / sweep / range spellings). */
    private record TemporalSpec(java.util.List<TypedSpec> dates, boolean sweep) {}

    /** Per-getAll temporal context (M3 propagation): the root fetch's dates
     * and strategy flow to SAME-STRATEGY targets navigated through temporal
     * parents (engine: milestoning context does NOT propagate through
     * non-temporal intermediates). Set at each getAll resolution entry. */
    private java.util.Map<String, TemporalSpec> temporalByHead = java.util.Map.of();
    private java.util.List<TypedSpec> rootMilestoning = java.util.List.of();
    private String rootStrategy = null;

    /**
     * A temporal TARGET's pipeline filtered by its milestoning columns —
     * explicit spec (property-function dates) wins; else the ROOT context
     * propagates when the immediate parent is temporal and the strategies
     * match; else LOUD (the engine compiles this to an error too).
     */
    private TypedSpec temporalTargetPipe(ClassSource parent, ClassSource target,
            String head, TypedSpec pipe) {
        String strat = temporalStrategy(target.classFqn());
        if (strat == null) {
            return pipe;
        }
        TemporalSpec spec = temporalByHead.get(head);
        if (spec != null && spec.sweep() && spec.dates().isEmpty()) {
            return pipe;   // propAllVersions(): the RAW extent, any dimension
        }
        if (strat.equals("bitemporal")) {
            java.util.List<TypedSpec> dates =
                    spec != null && !spec.sweep() && spec.dates().size() == 2
                            ? spec.dates()
                            : "bitemporal".equals(rootStrategy)
                                    && rootMilestoning.size() == 2
                                    && temporalStrategy(parent.classFqn()) != null
                                    ? rootMilestoning : null;
            // the engine-generated 1-DATE bitemporal property: the param is
            // the dimension the OWNER lacks; the owner's own dimension
            // fills from $this.<date> = the propagated context
            if (dates == null && spec != null && !spec.sweep()
                    && spec.dates().size() == 1 && rootMilestoning.size() == 1) {
                String parentStrat = temporalStrategy(parent.classFqn());
                if ("businesstemporal".equals(parentStrat)
                        && parentStrat.equals(rootStrategy)) {
                    dates = java.util.List.of(spec.dates().get(0),
                            rootMilestoning.get(0));
                } else if ("processingtemporal".equals(parentStrat)
                        && parentStrat.equals(rootStrategy)) {
                    dates = java.util.List.of(rootMilestoning.get(0),
                            spec.dates().get(0));
                }
            }
            if (dates == null) {
                throw new MappingResolutionException("navigation '" + head
                        + "' to bi-temporal class '" + target.classFqn()
                        + "' requires processing and business dates",
                        target.classFqn());
            }
            return milestonedPipeByStrategy(
                    milestonedPipeByStrategy(pipe, dates.get(0),
                            "processingtemporal", target.classFqn()),
                    dates.get(1), "businesstemporal", target.classFqn());
        }
        if (spec != null) {
            if (spec.sweep() && spec.dates().isEmpty()) {
                return pipe;   // propAllVersions(): the raw extent
            }
            if (spec.sweep()) {
                return rangeMilestonedPipe(pipe, spec.dates().get(0),
                        spec.dates().get(1), target.classFqn());
            }
            return milestonedPipe(pipe, spec.dates().get(0), target.classFqn());
        }
        // PROPAGATION: same-dimension context through temporal parents; a
        // BI-TEMPORAL root supplies (processing, business) — each single-
        // dimension target takes its own.
        if (!rootMilestoning.isEmpty()
                && temporalStrategy(parent.classFqn()) != null) {
            if (strat.equals(rootStrategy)) {
                return milestonedPipe(pipe, rootMilestoning.get(0), target.classFqn());
            }
            if ("bitemporal".equals(rootStrategy) && rootMilestoning.size() == 2) {
                TypedSpec d = strat.equals("processingtemporal")
                        ? rootMilestoning.get(0) : rootMilestoning.get(1);
                return milestonedPipe(pipe, d, target.classFqn());
            }
        }
        throw new MappingResolutionException("navigation '" + head
                + "' to temporal class '" + target.classFqn() + "' requires a"
                + " milestoning date (property function argument, or a"
                + " propagated temporal context through temporal parents)",
                target.classFqn());
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
        Pipelines.Materialized tMat0 = Pipelines.materialize(
                target.pipeline(), targetDemand, target.classFqn());
        Pipelines.Materialized tMat = new Pipelines.Materialized(
                temporalTargetPipe(cs, target, head,
                        applyJoinTemporalFilters(tMat0.pipeline(), target,
                                java.util.Map.of())),
                tMat0.slotPrefixes(), tMat0.stripped());

        // The predicate function: mapping's AssociationBinding for the assoc.
        var mapping = ctx.findMapping(cs.mappingFqn()).orElseThrow();
        var binding = mapping.associationBindings().stream()
                .filter(ab -> ab.associationFqn().equals(assoc.qualifiedName()))
                .findFirst()
                .orElseThrow(() -> new MappingResolutionException("association '"
                        + assoc.qualifiedName() + "' is not mapped in mapping '"
                        + cs.mappingFqn() + "'"
                        // a dropped/poisoned property route often lands here
                        // (the assoc fallback) — surface the recorded reason
                        + ctx.mappingPoison(cs.mappingFqn(), cs.classFqn())
                                .map(r -> " (" + r + ")").orElse(""),
                        assoc.qualifiedName()));
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
        // TARGET-SIDE join-key collection: a distinct-narrowed target must
        // expose the key columns the association condition binds on.
        Set<String> tgtReads = new java.util.LinkedHashSet<>();
        for (TypedSpec b : oriented.body()) {
            Pipelines.collectVarReads(b, oriented.parameters().get(1), tgtReads);
        }
        TypedSpec tPipe = Pipelines.widenDistinctForKeys(tMat.pipeline(), tgtReads);
        // UNION target: member threads carry the key columns the
        // association condition binds on (engine partial-union goldens)
        tPipe = Pipelines.widenConcatenateForKeys(tPipe, tgtReads);
        // audit 10: the target pipeline's OWN materialized slot joins to
        // milestoned tables filter by the temporal context too (every
        // milestoned table alias filters — the dead wall this replaces)
        tPipe = applyJoinTemporalFilters(tPipe, target, java.util.Map.of());
        return new AssocJoin(prefixFor(head, cs), target, tPipe,
                (com.legend.compiler.element.type.Type.RelationType)
                        tPipe.info().type(),
                oriented, tMat.slotPrefixes());
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
                                      Map<TypedSpec, Substitution.AggRead> aggReads,
                                      boolean filterPosition,
                                      String freshRowVar, TypedLambda userLambda) {
        return new Substitution(new Substitution.Target(
                userLambda.parameters().get(0), freshRowVar,
                cs.classFqn(), cs.mappingFqn(), cs.rowVar(), cs.bindings(),
                (com.legend.compiler.element.type.Type.RelationType)
                        m.pipeline().info().type(),
                m.stripped(), m.slotPrefixes(), assocs, assocEnds, existsSubs,
                aggReads, isNotEmptyCallee(), equalCallee(),
                rootMilestoning, headTemporalDates(),
                milestoneColumnsOf(cs.pipeline(), cs.classFqn()),
                filterPosition, false));
    }

    /** The generated milestone-struct leaf -> physical column map for the
     * pipe's root table, by the class's temporal dimension. */
    private Map<String, String> milestoneColumnsOf(TypedSpec pipe, String classFqn) {
        String strat = temporalStrategy(classFqn);
        com.legend.compiler.spec.typed.TypedTableReference root = rootTable(pipe);
        var ms = root == null || strat == null ? null
                : ctx.findTableMilestoning(root.store(), root.table()).orElse(null);
        if (ms == null) {
            return Map.of();
        }
        Map<String, String> out = new java.util.LinkedHashMap<>();
        if (!"processingtemporal".equals(strat) && ms.business() != null) {
            var b = ms.business();
            if (b.from() != null) {
                out.put("from", b.from());
            }
            if (b.thru() != null) {
                out.put("thru", b.thru());
            }
            if (b.snapshotDate() != null) {
                out.put("snapshotDate", b.snapshotDate());
            }
        }
        if (!"businesstemporal".equals(strat) && ms.processing() != null) {
            var pr = ms.processing();
            if (pr.in() != null) {
                out.putIfAbsent("in", pr.in());
                out.putIfAbsent("from", pr.in());
            }
            if (pr.out() != null) {
                out.putIfAbsent("out", pr.out());
                out.putIfAbsent("thru", pr.out());
            }
            if (pr.snapshotDate() != null) {
                out.putIfAbsent("snapshotDate", pr.snapshotDate());
            }
        }
        return out;
    }

    /** A DATE argument spelled as a temporal-context read
     * ({@code $this.businessDate} in a milestoned qualified property, or
     * any instance's businessDate/processingDate) IS the context date —
     * normalize to it so the literal-only filters apply. */
    private java.util.List<TypedSpec> normalizeContextDates(java.util.List<TypedSpec> dates) {
        java.util.List<TypedSpec> out = new ArrayList<>(dates.size());
        for (TypedSpec d : dates) {
            out.add(normalizeContextDate(d));
        }
        return out;
    }

    private TypedSpec normalizeContextDate(TypedSpec d) {
        if (d instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                && (pa.property().equals("businessDate")
                        || pa.property().equals("processingDate"))
                // ONLY the GENERATED property on a temporal receiver — an
                // ordinary user property legally named businessDate must
                // not be rewritten (audit 10)
                && pa.source().info().type()
                        instanceof com.legend.compiler.element.type.Type.ClassType rc
                && temporalStrategy(rc.fqn()) != null
                && !rootMilestoning.isEmpty()) {
            return rootMilestoning.size() == 2 && pa.property().equals("businessDate")
                    ? rootMilestoning.get(1) : rootMilestoning.get(0);
        }
        return d;
    }

    /** Explicit per-head property-function dates for the substitution. */
    private java.util.Map<String, java.util.List<TypedSpec>> headTemporalDates() {
        java.util.Map<String, java.util.List<TypedSpec>> out =
                new java.util.LinkedHashMap<>();
        for (var e : temporalByHead.entrySet()) {
            if (!e.getValue().dates().isEmpty()) {
                out.put(e.getKey(), e.getValue().dates());
            }
        }
        return out;
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
