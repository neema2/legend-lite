package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedDistinct;
import com.legend.compiler.spec.typed.TypedDrop;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedSlice;
import com.legend.compiler.spec.typed.TypedSort;
import com.legend.compiler.spec.typed.TypedSortBy;
import com.legend.compiler.spec.typed.TypedSpec;
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
            case TypedGetAll g -> throw new NotImplementedException(
                    "bare class fetch '" + g.classFqn() + ".all()' without a"
                            + " relation-shaping operation is graph output (Phase H4)");
            case TypedFilter f when isObjectSpace(f.source()) ->
                    resolveChain(f, context);
            case TypedProject p when isObjectSpace(p.source()) ->
                    resolveChain(p, context);
            case TypedLimit l when isObjectSpace(l.source()) ->
                    resolveChain(l, context);
            case TypedDrop d when isObjectSpace(d.source()) ->
                    resolveChain(d, context);
            case TypedSlice s when isObjectSpace(s.source()) ->
                    resolveChain(s, context);
            case TypedSortBy sb when isObjectSpace(sb.source()) ->
                    resolveChain(sb, context);
            // Relation-space wrappers over a chain that bottoms at a getAll:
            // rebuild with the resolved source. (Each wrapper keeps its own
            // info — relation-space types are stable across resolution.)
            case TypedFilter f when containsGetAll(f.source()) -> new TypedFilter(
                    resolveNode(f.source(), context), f.predicate(), f.info());
            case TypedProject p when containsGetAll(p.source()) -> new TypedProject(
                    resolveNode(p.source(), context), p.columns(), p.info());
            case TypedSort s when containsGetAll(s.source()) -> new TypedSort(
                    resolveNode(s.source(), context), s.keys(), s.info());
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
            default -> false;
        };
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
        if (!(top instanceof TypedProject p)) {
            throw new NotImplementedException("class-shaped result at the query root"
                    + " (" + top.getClass().getSimpleName() + ") is graph output (Phase H4)");
        }
        // 1. Collect the below-boundary op chain (top-down) to the getAll.
        List<TypedSpec> ops = new ArrayList<>();
        TypedSpec cur = p.source();
        while (!(cur instanceof TypedGetAll)) {
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
        ClassSource cs = sources.get(dispatch(context, g.classFqn()), g.classFqn());

        // 2. Demand scan over ALL the chain's user lambdas (one funnel with
        //    the substitution — they cannot drift), close over slot
        //    conditions, materialize.
        Set<java.util.List<String>> paths = new java.util.LinkedHashSet<>();
        // ENTRY RULE (learned three times now): scans enter through the
        // lambda's BODY — entering via the lambda itself trips the shadow
        // stop on its own parameter.
        for (TypedSpec op : ops) {
            if (op instanceof TypedFilter f) {
                scanLambda(f.predicate(), paths);
            }
            if (op instanceof TypedSortBy sb) {
                scanLambda(sb.key(), paths);
            }
        }
        for (TypedFuncCol col : p.columns()) {
            scanLambda(col.fn(), paths);
        }

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
            String alias = navSlotAlias(headBinding, cs.rowVar(), navSteps.keySet());
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

        Pipelines.Materialized m = Pipelines.materialize(
                cs.pipeline(), demanded, demandedNavs, cs.classFqn(),
                targetClass -> Pipelines.materialize(
                        sources.get(cs.mappingFqn(), targetClass).pipeline(),
                        Set.of(), targetClass).pipeline());

        // Association demand: 2-hop paths whose head is NOT a binding are
        // association navigations — one LEFT join per head (dedup by head:
        // the whole-chain registry), target = the class's own pipeline.
        java.util.List<AssocJoin> assocJoins = new ArrayList<>();
        for (java.util.List<String> path : paths) {
            if (path.size() < 2 || cs.bindings().containsKey(path.get(0))) {
                continue;   // 1-hop, or embedded/slot heads (substitution-side)
            }
            String head = path.get(0);
            if (path.size() > 2) {
                throw new NotImplementedException("multi-hop navigation "
                        + String.join(".", path) + " is not supported yet");
            }
            if (assocs.containsKey(head)) {
                continue;
            }
            AssocJoin aj = associationJoin(cs, head, context);
            assocJoins.add(aj);
            assocs.put(head, new Substitution.AssocSub(aj.prefix(),
                    aj.target().rowVar(), aj.target().bindings(),
                    aj.target().classFqn(),
                    Pipelines.slotAliases(aj.target().pipeline())));
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
        for (TypedFuncCol col : p.columns()) {
            collectLambdaParams(col.fn(), paramsInReach);
        }
        for (TypedSpec b : cs.bindings().values()) {
            collectLambdaParams(b, paramsInReach);
        }
        for (AssocJoin aj : assocJoins) {
            for (TypedSpec b : aj.target().bindings().values()) {
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
                        substitution(cs, m, assocs, assocEnds, fresh, f.predicate())
                                .rewriteLambda(f.predicate()),
                        pipeline.info());
                case TypedLimit l -> new TypedLimit(pipeline, l.count(), pipeline.info());
                case TypedDrop d -> new TypedDrop(pipeline, d.count(), pipeline.info());
                case TypedSlice sl -> new TypedSlice(pipeline, sl.start(), sl.stop(),
                        pipeline.info());
                case TypedSortBy sb -> new TypedSortBy(pipeline,
                        substitution(cs, m, assocs, assocEnds, fresh, sb.key()).rewriteLambda(sb.key()),
                        sb.ascending(), pipeline.info());
                default -> throw new IllegalStateException("resolver bug: uncollected op");
            };
        }

        // 4. The projection boundary: info UNCHANGED.
        List<TypedFuncCol> cols = new ArrayList<>(p.columns().size());
        for (TypedFuncCol col : p.columns()) {
            cols.add(new TypedFuncCol(col.name(),
                    substitution(cs, m, assocs, assocEnds, fresh, col.fn()).rewriteLambda(col.fn())));
        }
        return new TypedProject(pipeline, cols, p.info());
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
                && c.callee().qualifiedName().endsWith("toOne")) {
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
                             TypedLambda condition) {}

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
    private AssocJoin associationJoin(ClassSource cs, String head, Context context) {
        var assoc = ctx.findAssociationOf(cs.classFqn(), head).orElseThrow(() ->
                new MappingResolutionException("property '" + head + "' of class '"
                        + cs.classFqn() + "' is not mapped in mapping '"
                        + cs.mappingFqn() + "'", cs.classFqn()));
        // The end from the SAME association object — a separate index lookup
        // was a split-brain with findAssociationOf (audit blocker).
        var end = assoc.property1().propertyName().equals(head)
                ? assoc.property1() : assoc.property2();
        // DEFAULT-DENY multiplicity: only a concrete to-one end navigates
        // here (a Parameter-multiplicity end must not silently duplicate rows).
        if (!(end.multiplicity() instanceof com.legend.parser.Multiplicity.Concrete mc)
                || !Integer.valueOf(1).equals(mc.upperBound())) {
            throw new NotImplementedException("navigation of association end '$"
                    + head + "' (multiplicity " + end.multiplicity() + ") is not"
                    + " supported yet: filter-position to-many becomes EXISTS in"
                    + " H3c; other positions are H4+");
        }
        String targetClass = ((com.legend.parser.TypeExpression.NameRef)
                end.targetClass()).name();
        ClassSource target = sources.get(cs.mappingFqn(), targetClass);
        Pipelines.Materialized tMat = Pipelines.materialize(
                target.pipeline(), Set.of(), target.classFqn());

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
                || !call.callee().qualifiedName().endsWith("legacyAssocPredicate")
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
                oriented);
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
                                      String freshRowVar, TypedLambda userLambda) {
        return new Substitution(new Substitution.Target(
                userLambda.parameters().get(0), freshRowVar,
                cs.classFqn(), cs.mappingFqn(), cs.rowVar(), cs.bindings(),
                (com.legend.compiler.element.type.Type.RelationType)
                        m.pipeline().info().type(),
                m.stripped(), m.slotPrefixes(), assocs, assocEnds));
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
            throw new MappingResolutionException("runtime '" + context.runtimeFqn()
                    + "' has " + binders.size() + " mappings binding class '"
                    + classFqn + "' (of " + rt.mappings().size()
                    + " candidates); class-query dispatch needs exactly one",
                    classFqn);
        }
        return binders.get(0);
    }
}
