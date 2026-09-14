package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.CompiledFunction;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCDate;
import com.legend.compiler.spec.typed.TypedCDecimal;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedEnumValue;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedNewInstanceCast;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.MappingResolutionException;
import com.legend.error.NotImplementedException;
import com.legend.model.MappingDefinition;
import com.legend.model.MappingInclude;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;
/**
 * Loads and memoizes {@link ClassSource}s: (mapping, class) &rarr; compiled
 * mapping body &rarr; split at the {@code map(row|^Class(...))} terminal.
 *
 * <p>Dispatch is by class within the active mapping (its own bindings, then
 * included mappings transitively). Multi-set-ID classes are loud until H5.
 * The body compiles through the shared {@link SpecCompiler} (every
 * synthesized body type-checks &mdash; the H1 census invariant), so a
 * terminal that is not a single-statement {@code map} over a
 * {@code ^Class(...)} lambda is a resolver-vs-normalizer CONTRACT violation
 * ({@code IllegalStateException}), not a user error.
 *
 * <p>Cycle policy (H3, per the plan): an association target already on the
 * {@code resolving} stack gets a SHALLOW instantiation; any other re-entry
 * throws with the cycle path printed. Never null-and-skip &mdash; V1's
 * silent-leak family. (In H2 loading never recurses; the guard is
 * structural insurance.)
 */
public final class ClassSources {

    private final ModelContext ctx;
    private final SpecCompiler specs;
    private final Map<String, ClassSource> memo = new LinkedHashMap<>();
    /** class FQN -> data: URL from the execution context's
     * JsonModelConnections (XStore §1) — set per from()-scope by the
     * resolver; STATEMENT-scoped like this instance itself. The memo key
     * does not include it: two from() scopes with DIFFERENT json sources
     * for the SAME class in ONE statement would collide — accepted and
     * documented (no corpus shape does this; a collision surfaces as a
     * wrong-rows FAIL, never silence). */
    private java.util.Map<String, String> jsonSources = java.util.Map.of();

    void setJsonSources(java.util.Map<String, String> sources) {
        this.jsonSources = sources;
    }

    private final LinkedHashSet<String> resolving = new LinkedHashSet<>();

    /** The model context (H5 set-ID hint lookups ride it). */
    ModelContext ctx() {
        return ctx;
    }

    /** Navigate-target resolution with the H5 SET-ID DISPATCH hint baked
     * in: the head's sole routed set (mapping-closure table) selects the
     * set-discriminated binding when present; class-level serves
     * otherwise (union targets always fall back — member set ids never
     * bind class-level). */
    ClassSource getForNav(String mappingFqn, String classFqn, String head,
            @com.legend.Nullable String scope) {
        return get(mappingFqn, classFqn,
                ctx.routedTargetSetOf(mappingFqn,
                        SyntheticHeads.realHead(head)).orElse(null),
                null, "", scope);
    }

    /** tree id -> (store table -> rows): the resolver's constructed
     * instances, read as inline rows under their tree's scope. */
    private java.util.function.Function<String, Map<String, List<List<String>>>>
            constructedRows = id -> Map.of();

    void setConstructedRows(
            java.util.function.Function<String, Map<String, List<List<String>>>> rows) {
        this.constructedRows = rows;
    }

    /**
     * A class source UNDER A CONSTRUCTED SCOPE: the system store's table
     * leaves become inline relations of the tree's rows, typed with the
     * leaf's own row type (user ruling 2026-09-02: a query carries its own
     * constants; the system database is read-only after the graph's rows).
     * A table the tree has no rows for keeps its scan (a real element
     * inside a constructed tree reads the graph).
     */
    private ClassSource scoped(ClassSource built, String scope) {
        Map<String, List<List<String>>> rows = constructedRows.apply(scope);
        if (rows.isEmpty()) {
            return built.withScope(scope);
        }
        return new ClassSource(built.mappingFqn(), built.classFqn(), built.setId(),
                inlineStoreLeaves(built.pipeline(), rows), built.rowVar(),
                built.bindings(), built.rowType(), built.sourceClass(),
                built.deferredWalls(), built.composedPrefix(), built.castGate(), scope);
    }

    private static TypedSpec inlineStoreLeaves(TypedSpec n,
            Map<String, List<List<String>>> rows) {
        if (n instanceof com.legend.compiler.spec.typed.TypedTableReference tr
                && com.legend.builtin.SystemMetamodel.STORE_FQN.equals(tr.store())) {
            String table = tr.table().substring(tr.table().lastIndexOf('.') + 1);
            List<List<String>> mine = rows.get(table);
            if (mine != null) {
                List<List<String>> cells = new ArrayList<>(mine.size());
                for (List<String> row : mine) {
                    List<String> r = new ArrayList<>(row.size());
                    for (String c : row) {
                        // the literal node copies rows null-rejecting: an
                        // absent fact is the TDS null cell (lowered to NULL)
                        r.add(c == null ? com.legend.compiler.element.type.PlatformTypes
                                .TDS_NULL_CELL : c);
                    }
                    cells.add(r);
                }
                return new com.legend.compiler.spec.typed.TypedTds(cells, tr.info());
            }
            return n;
        }
        List<TypedSpec> kids = n.children();
        if (kids.isEmpty()) {
            return n;
        }
        List<TypedSpec> out = new ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec nk = inlineStoreLeaves(k, rows);
            changed |= nk != k;
            out.add(nk);
        }
        return changed ? n.withChildren(out) : n;
    }

    public ClassSources(ModelContext ctx, SpecCompiler specs) {
        this.ctx = Objects.requireNonNull(ctx, "ctx");
        this.specs = Objects.requireNonNull(specs, "specs");
    }

    /** The compiled body of a SYNTHESIZED function (unique FQN — derived
     * property bodies for graph-leaf inlining, task #78). */
    CompiledFunction compileSynthFn(String fqn) {
        List<TypedFunction> fns = ctx.findFunction(fqn);
        if (fns.size() != 1) {
            throw new IllegalStateException("resolver bug: synthesized"
                    + " function '" + fqn + "' has " + fns.size()
                    + " overloads; synthesized FQNs are unique");
        }
        return specs.compile(fns.get(0));
    }


    /** The memoized extraction for {@code classFqn} under {@code mappingFqn},
     * resolved under {@code scope} — the SOURCE's scope when fetching a
     * target for it ({@link ClassSource#scope()}), null at a graph root. */
    public ClassSource get(String mappingFqn, String classFqn,
            @com.legend.Nullable String scope) {
        return get(mappingFqn, classFqn, null, "", scope);
    }

    /**
     * {@code upstreamMapping} dispatches a MODEL-TO-MODEL source class to
     * the mapping that binds it (the runtime's candidate set) when this
     * mapping doesn't — the corpus lists relational base mappings and M2M
     * mappings side by side in one runtime. {@code null} restricts
     * resolution to this mapping (+ includes).
     */
    public ClassSource get(String mappingFqn, String classFqn,
            @com.legend.Nullable java.util.function.BiFunction<String, String, String> upstreamMapping,
            String contextKey, @com.legend.Nullable String scope) {
        return get(mappingFqn, classFqn, null, upstreamMapping, contextKey, scope);
    }

    /** {@code setId} non-null = H5 SET-ID DISPATCH: the navigate's route
     * names a specific set of a (possibly rootless) multi-set class — that
     * set's binding realizes the target; absent, the class-level lookup
     * serves (union targets: member set ids never bind class-level, the
     * fallback lands on the union). */
    public ClassSource get(String mappingFqn, String classFqn,
            @com.legend.Nullable String setId,
            @com.legend.Nullable java.util.function.BiFunction<String, String, String> upstreamMapping,
            String contextKey, @com.legend.Nullable String scope) {
        // The context key participates in memoization because an M2M
        // composition resolves its UPSTREAM through the runtime dispatch —
        // the same mapping::class composed under different runtimes reads
        // different stores (audit F1: memo poisoning was silent wrong data).
        String key = mappingFqn + '\u0000' + classFqn + '\u0000' + contextKey
                + (setId == null ? "" : '\u0000' + setId)
                + (scope == null ? "" : "\u0000scope=" + scope);
        ClassSource cached = memo.get(key);
        if (cached != null) {
            return cached;
        }
        if (!resolving.add(key)) {
            throw new IllegalStateException("resolver bug: class-source cycle "
                    + String.join(" -> ", resolving) + " -> " + key
                    + " (association targets mid-cycle must take the SHALLOW path)");
        }
        try {
            ClassSource built = build(mappingFqn, classFqn, setId,
                    upstreamMapping, contextKey);
            if (scope != null) {
                built = scoped(built, scope);
            }
            memo.put(key, built);
            return built;
        } finally {
            resolving.remove(key);
        }
    }

    /**
     * MIXED-KIND UNION extent (route b, docs/XSTORE_LEG.md): each member
     * set resolves to its OWN ClassSource (Relational members compose as
     * ever; Pure members ride the M2M/JSON-frame composition), each arm
     * projects the class's declared SCALAR properties to property-named
     * columns, and the arms concatenate in DECLARATION order (the
     * engine's member ordinal). Class-typed navigation off the mixed
     * extent is per-member dispatch — not built yet, loud downstream.
     */
    private ClassSource mixedUnionSource(String mappingFqn, String classFqn,
            List<String> memberSetIds, @com.legend.Nullable java.util.function.BiFunction<String, String, String> upstreamMapping,
            String contextKey) {
        var cls = ctx.findClass(classFqn).orElseThrow(() ->
                new IllegalStateException("resolver bug: mixed-union class '"
                        + classFqn + "' unknown to the model"));
        List<Type.Column> cols = new ArrayList<>();
        for (var p : cls.properties()) {
            if (!(Type.asClassType(p.type()) instanceof Type.ClassType)) {
                cols.add(new Type.Column(p.name(), p.type(), p.multiplicity()));
            }
        }
        var one = com.legend.compiler.element.type.Multiplicity.Bounded.ONE;
        var optional = com.legend.compiler.element.type.Multiplicity.Bounded
                .ZERO_ONE;
        List<String> ordered = mixedArmOrder(mappingFqn, classFqn,
                memberSetIds);
        // per-arm CHILD-ROUTE KEY columns (design: per-member children) —
        // each member's class-typed routes contribute member-suffixed key
        // columns, NULL in other arms; the child-union side mirrors the
        // names via mixedKeyCol (one discipline, cannot drift)
        List<ClassSource> members = new ArrayList<>();
        List<List<MixedRoute>> routesPer = new ArrayList<>();
        List<Type.Column> keyCols = new ArrayList<>();
        for (int i = 0; i < ordered.size(); i++) {
            ClassSource m = get(mappingFqn, classFqn, ordered.get(i),
                    upstreamMapping, contextKey, null);
            members.add(m);
            List<MixedRoute> rs = mixedMemberRoutes(m, mappingFqn);
            routesPer.add(rs);
            for (MixedRoute r : rs) {
                for (int k = 0; k < r.memberKeys().size(); k++) {
                    keyCols.add(new Type.Column(
                            mixedKeyCol(r.prop(), i, k),
                            r.memberKeys().get(k).info().type(), optional));
                }
            }
        }
        List<Type.Column> allCols = new ArrayList<>(cols);
        allCols.addAll(keyCols);
        Type.RelationType rowType = new Type.RelationType(allCols);
        var many = com.legend.compiler.element.type.Multiplicity.Bounded
                .ZERO_MANY;
        TypedSpec union = null;
        for (int i = 0; i < ordered.size(); i++) {
            ClassSource m = members.get(i);
            TypedSpec pipe = Pipelines.materialize(m.pipeline(),
                    java.util.Set.of(), classFqn).pipeline();
            Type.RelationType mRow = Type.requireRelationSchema(pipe.info().type());
            List<com.legend.compiler.spec.typed.TypedFuncCol> pcols =
                    new ArrayList<>(allCols.size());
            for (Type.Column c : cols) {
                TypedSpec b = m.bindings().get(c.name());
                if (b == null) {
                    throw new NotImplementedException("mixed-kind union of '"
                            + classFqn + "': member set '" + ordered.get(i)
                            + "' does not bind shared property '" + c.name()
                            + "' — NULL-column arms are not built yet"
                            + " (mapping=" + mappingFqn + ")");
                }
                pcols.add(mixedCol(c.name(), b, mRow, m.rowVar()));
            }
            for (int j = 0; j < ordered.size(); j++) {
                for (MixedRoute r : routesPer.get(j)) {
                    for (int k = 0; k < r.memberKeys().size(); k++) {
                        TypedSpec v = j == i ? r.memberKeys().get(k)
                                : new TypedCollection(List.of(),
                                        new ExprType(r.memberKeys().get(k)
                                                .info().type(), optional));
                        pcols.add(mixedCol(mixedKeyCol(r.prop(), j, k),
                                v, mRow, m.rowVar()));
                    }
                }
            }
            TypedSpec arm = new com.legend.compiler.spec.typed.TypedProject(
                    pipe, pcols, new ExprType(Type.relation(rowType), many));
            union = union == null ? arm
                    : new com.legend.compiler.spec.typed.TypedConcatenate(
                            union, arm, new ExprType(Type.relation(rowType), many));
        }
        ExprType rowInfo = new ExprType(rowType, one);
        String rowVar = "u_row";
        Map<String, TypedSpec> bindings = new LinkedHashMap<>();
        for (Type.Column c : cols) {
            bindings.put(c.name(), new TypedPropertyAccess(
                    new TypedVariable(rowVar, rowInfo), c.name(),
                    new ExprType(c.type(), c.multiplicity())));
        }
        return new ClassSource(mappingFqn, classFqn, ClassSource.UNION_SET_ID,
                java.util.Objects.requireNonNull(union,
                        "union with zero members"),
                rowVar, bindings, rowType);
    }

    /**
     * THE ROUTED UNION SOURCE (legacy routes as composition, docs/LEGACY_
     * ROUTES_AS_COMPOSITION_2026_09_13.md §5): the target of a several-route
     * navigate, built from the routes the NAVIGATOR wrote — one arm per
     * route over that route's target set (its own function, resolved under
     * this mapping), each arm projecting the class's scalar properties by
     * the set's bindings plus the union-row keys: its own route's target
     * reads under the route's key names, a typed NULL for every other key.
     * The union never learned who navigates to it; the navigator composed
     * it. Same shape as {@link #mixedUnionSource}, keys from the routes.
     */
    ClassSource routedUnionSource(String mappingFqn, String classFqn,
            List<com.legend.compiler.spec.typed.TypedNavigate.Route> routes,
            @com.legend.Nullable String scope) {
        // one source per (navigate step's routes, mapping, scope): every
        // lookup for the step — materialization, substitution, predicates —
        // reads the same bindings
        String memoKey = mappingFqn + '\u0000' + classFqn + (scope == null ? "" : "\u0000scope=" + scope);
        Map<String, ClassSource> perRoutes = routedSources.computeIfAbsent(routes,
                k -> new LinkedHashMap<>());
        ClassSource memo = perRoutes.get(memoKey);
        if (memo != null) {
            return memo;
        }
        ClassSource built = buildRoutedUnionSource(mappingFqn, classFqn, routes, scope);
        perRoutes.put(memoKey, built);
        return built;
    }

    private final java.util.IdentityHashMap<List<com.legend.compiler.spec.typed.TypedNavigate.Route>,
            Map<String, ClassSource>> routedSources = new java.util.IdentityHashMap<>();

    /**
     * THE TARGET OF A NAVIGATE STEP — the one lookup (legacy routes as
     * composition, design §8.2): a step that carries routes answers with
     * its routed union (built from the navigator's own routes, memoized per
     * step); a step without routes resolves its class through the set-id
     * dispatch, exactly as before. {@code source} is the class source
     * whose pipeline holds the step (its mapping and scope are the
     * resolution context); {@code head} keys the set-id dispatch for the
     * route-less case. No caller routes on its own.
     */
    ClassSource navTarget(ClassSource source, String classFqn,
            com.legend.compiler.spec.typed.@com.legend.Nullable TypedNavigate step, String head) {
        if (step != null && !step.routes().isEmpty()) {
            return routedUnionSource(source.mappingFqn(), classFqn, step.routes(), source.scope());
        }
        return getForNav(source.mappingFqn(), classFqn, head, source.scope());
    }

    /** The navigate step {@code alias} names in {@code source}'s pipeline,
     * or null (a synthetic head the pipeline does not spell). */
    static com.legend.compiler.spec.typed.@com.legend.Nullable TypedNavigate stepOf(
            ClassSource source, String alias) {
        // the OUTERMOST step of that alias: a union's lifted navigate above
        // its members' own same-named steps inside the threads
        return Pipelines.outerNavSteps(source.pipeline()).get(alias);
    }

    private ClassSource buildRoutedUnionSource(String mappingFqn, String classFqn,
            List<com.legend.compiler.spec.typed.TypedNavigate.Route> routes,
            @com.legend.Nullable String scope) {
        var cls = ctx.findClass(classFqn).orElseThrow(() ->
                new IllegalStateException("resolver bug: routed navigate class '"
                        + classFqn + "' unknown to the model"));
        var one = com.legend.compiler.element.type.Multiplicity.Bounded.ONE;
        var optional = com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_ONE;
        var many = com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_MANY;
        List<Type.Column> cols = new ArrayList<>();
        for (var p : cls.properties()) {
            if (!(Type.asClassType(p.type()) instanceof Type.ClassType)) {
                cols.add(new Type.Column(p.name(), p.type(), p.multiplicity()));
            }
        }
        // the union-row keys, in route order, once per name
        List<Type.Column> keyCols = new ArrayList<>();
        java.util.Set<String> seenKeys = new LinkedHashSet<>();
        List<ClassSource> members = new ArrayList<>();
        for (var r : routes) {
            ClassSource m = routeTarget(mappingFqn, classFqn, r.target(), scope);
            members.add(m);
        }
        // each arm: the member's own pipeline, with the route's mids joined
        // on top of it (the route's rows, re-rooted from the member's table
        // onto the member's pipeline)
        List<TypedSpec> armPipes = new ArrayList<>();
        for (int i = 0; i < routes.size(); i++) {
            TypedSpec base = Pipelines.materialize(members.get(i).pipeline(), java.util.Set.of(), classFqn)
                    .pipeline();
            armPipes.add(rebaseRows(routes.get(i).rows(), base));
        }
        for (int i = 0; i < routes.size(); i++) {
            var r = routes.get(i);
            Type.RelationType mRow = Type.requireRelationSchema(armPipes.get(i).info().type());
            for (int k = 0; k < r.keyNames().size(); k++) {
                String read = r.targetReads().get(k);
                if (seenKeys.add(r.keyNames().get(k))) {
                    Type kt = pathType(mRow, read);
                    if (kt == null) {
                        throw new MappingResolutionException("route condition reads '" + read
                                + "', which the target set's rows do not carry"
                                + " (class '" + classFqn + "', mapping '" + mappingFqn + "')", classFqn);
                    }
                    keyCols.add(new Type.Column(r.keyNames().get(k), kt, optional));
                }
            }
        }
        List<Type.Column> allCols = new ArrayList<>(cols);
        allCols.addAll(keyCols);
        Type.RelationType rowType = new Type.RelationType(allCols);
        TypedSpec union = null;
        for (int i = 0; i < routes.size(); i++) {
            ClassSource m = members.get(i);
            var r = routes.get(i);
            TypedSpec pipe = armPipes.get(i);
            Type.RelationType mRow = Type.requireRelationSchema(pipe.info().type());
            var mInfo = new ExprType(mRow, one);
            List<com.legend.compiler.spec.typed.TypedFuncCol> pcols = new ArrayList<>();
            for (Type.Column c : cols) {
                TypedSpec bnd = m.bindings().get(c.name());
                TypedSpec v = bnd != null ? bnd
                        : new TypedCollection(List.of(), new ExprType(c.type(), optional));
                pcols.add(mixedCol(c.name(), v, mRow, m.rowVar()));
            }
            for (Type.Column kc : keyCols) {
                int k = r.keyNames().indexOf(kc.name());
                TypedSpec v = k >= 0
                        ? pathRead(new TypedVariable(m.rowVar(), mInfo), mRow,
                                r.targetReads().get(k), new ExprType(kc.type(), optional))
                        : new TypedCollection(List.of(), new ExprType(kc.type(), optional));
                pcols.add(mixedCol(kc.name(), v, mRow, m.rowVar()));
            }
            TypedSpec arm = new com.legend.compiler.spec.typed.TypedProject(
                    pipe, pcols, new ExprType(Type.relation(rowType), many));
            union = union == null ? arm
                    : new com.legend.compiler.spec.typed.TypedConcatenate(
                            union, arm, new ExprType(Type.relation(rowType), many));
        }
        ExprType rowInfo = new ExprType(rowType, one);
        String rowVar = "u_row";
        Map<String, TypedSpec> bindings = new LinkedHashMap<>();
        for (Type.Column c : cols) {
            bindings.put(c.name(), new TypedPropertyAccess(
                    new TypedVariable(rowVar, rowInfo), c.name(),
                    new ExprType(c.type(), c.multiplicity())));
        }
        return new ClassSource(mappingFqn, classFqn, ClassSource.UNION_SET_ID,
                java.util.Objects.requireNonNull(union, "routed navigate with no routes"),
                rowVar, bindings, rowType);
    }

    /** The route's rows re-rooted: its base table reference (the member's
     * main table, as the navigator spelled it) replaced by the member's own
     * pipeline, so the mids the route joined ride the member's arm. */
    private static TypedSpec rebaseRows(TypedSpec rows, TypedSpec base) {
        if (rows instanceof com.legend.compiler.spec.typed.TypedTableReference) {
            return base;
        }
        List<TypedSpec> kids = rows.children();
        if (kids.isEmpty()) {
            return rows;
        }
        List<TypedSpec> rebased = new ArrayList<>(kids.size());
        for (int i = 0; i < kids.size(); i++) {
            rebased.add(i == 0 ? rebaseRows(kids.get(0), base) : kids.get(i));
        }
        return rows.withChildren(rebased);
    }

    /** The type of {@code col} or {@code slot.col} in {@code row}; null when absent. */
    private static @com.legend.Nullable Type pathType(Type.RelationType row, String path) {
        Type.RelationType at = row;
        Type found = null;
        String[] parts = path.split("\\.");
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            Type.Column c = at.columns().stream().filter(x -> x.name().equals(part)).findFirst().orElse(null);
            if (c == null) {
                return null;
            }
            found = c.type();
            if (i + 1 < parts.length) {
                Type.RelationType sub = Type.relationSchema(c.type());
                if (sub == null) {
                    return null;
                }
                at = sub;
            }
        }
        return found;
    }

    /** {@code $m.col} or {@code $m.slot.col} over the member row. */
    private static TypedSpec pathRead(TypedVariable m, Type.RelationType row, String path,
            ExprType leafInfo) {
        String[] parts = path.split("\\.");
        if (parts.length == 1) {
            return new TypedPropertyAccess(m, path, leafInfo);
        }
        Type.Column slot = row.columns().stream().filter(x -> x.name().equals(parts[0])).findFirst()
                .orElseThrow();
        TypedSpec sub = new TypedPropertyAccess(m, parts[0], new ExprType(slot.type(), slot.multiplicity()));
        return new TypedPropertyAccess(sub, parts[1], leafInfo);
    }

    /** A route's target: the set's own FUNCTION (a user call — the binding
     * whose realizing function it is, under this mapping's closure), or a
     * class extent (class-level dispatch). */
    private ClassSource routeTarget(String mappingFqn, String classFqn, TypedSpec target,
            @com.legend.Nullable String scope) {
        if (target instanceof com.legend.compiler.spec.typed.TypedUserCall uc) {
            MappingDefinition mapping = ctx.findMapping(mappingFqn).orElseThrow(() ->
                    new MappingResolutionException("unknown mapping '" + mappingFqn + "'",
                            mappingFqn));
            MappingDefinition.ClassBinding cb = findBindingByFunction(mapping,
                    uc.callee().qualifiedName(), new LinkedHashSet<>());
            if (cb == null) {
                throw new MappingResolutionException("route target '"
                        + uc.callee().qualifiedName() + "' is not a set's function in"
                        + " mapping '" + mappingFqn + "' or its includes", classFqn);
            }
            return get(mappingFqn, cb.classFqn(), cb.setId(), null, "", scope);
        }
        if (target instanceof com.legend.compiler.spec.typed.TypedGetAll ga) {
            return get(mappingFqn, ga.classFqn(), scope);
        }
        throw new NotImplementedException("route target must be a set's function or a"
                + " class extent, got " + target.getClass().getSimpleName());
    }

    /** The binding realized by {@code functionFqn}: this mapping's own, else
     * an include's, depth-first. */
    private MappingDefinition.@com.legend.Nullable ClassBinding findBindingByFunction(
            MappingDefinition mapping, String functionFqn, java.util.Set<String> seen) {
        if (!seen.add(mapping.qualifiedName())) {
            return null;
        }
        for (MappingDefinition.ClassBinding cb : mapping.classBindings()) {
            if (functionFqn.equals(cb.functionFqn())) {
                return cb;
            }
        }
        for (MappingInclude inc : mapping.includes()) {
            MappingDefinition included = ctx.findMapping(inc.mappingPath()).orElse(null);
            if (included == null) {
                continue;
            }
            MappingDefinition.ClassBinding found = findBindingByFunction(included, functionFqn, seen);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /** ARM ORDER = the engine's cross-store BATCH order: the relational
     * store's members first (declaration order within), the in-memory
     * (Pure) members after — both XStoreUnion fixture declaration orders
     * pin this. */
    private List<String> mixedArmOrder(String mappingFqn, String classFqn,
            List<String> memberSetIds) {
        MappingDefinition mdef = ctx.findMapping(mappingFqn).orElseThrow();
        List<String> ordered = new ArrayList<>();
        List<String> pureArms = new ArrayList<>();
        for (String memberId : memberSetIds) {
            MappingDefinition.ClassBinding mcb = findBinding(mdef, classFqn,
                    memberId, new LinkedHashSet<>());
            if (mcb instanceof MappingDefinition.ClassBinding.Pure) {
                pureArms.add(memberId);
            } else {
                ordered.add(memberId);
            }
        }
        ordered.addAll(pureArms);
        return ordered;
    }

    private static com.legend.compiler.spec.typed.TypedFuncCol mixedCol(
            String name, TypedSpec value, Type.RelationType mRow,
            String rowVar) {
        var one = com.legend.compiler.element.type.Multiplicity.Bounded.ONE;
        var lFn = new Type.FunctionType(
                List.of(new Type.Param(mRow, one)),
                new Type.Param(value.info().type(),
                        value.info().multiplicity()));
        return new com.legend.compiler.spec.typed.TypedFuncCol(name,
                new com.legend.compiler.spec.typed.TypedLambda(
                        List.of(rowVar), List.of(value),
                        new ExprType(lFn, one)));
    }

    /**
     * THE MAPPING-SEAM WINDOW RULE: every window inside a class extent's
     * pipeline (a Relation {@code ~func} set — the only mapping kind whose
     * extent can carry one) is stamped as an extent boundary, so the
     * lowerer isolates its select and the query's operators over the
     * mapped class (its filter above all) land OUTSIDE it. The engine
     * treats the mapped relation as a non-mergeable view (corpus
     * testMappingWithWindowColumn: the rank counts every group member,
     * the class filter {@code age > 25} only drops rows); in plain
     * relation composition it folds the same predicate to WHERE under the
     * window (PCT testExtendFilterOutNull) — the fold rules keep that, and
     * this stamp is the resolver's decision the Fold contract names.
     * Structural walk over the whole extent (lambdas included: a window
     * nested in a correlated subquery of the extent is an extent fact too;
     * isolation is always sound).
     */
    private static TypedSpec sealExtentWindows(TypedSpec n) {
        TypedSpec rebuilt = n.mapChildren(ClassSources::sealExtentWindows);
        return switch (rebuilt) {
            case com.legend.compiler.spec.typed.TypedExtendWindow w -> w.withExtentBoundary();
            case com.legend.compiler.spec.typed.TypedExtendAgg a -> a.withExtentBoundary();
            default -> rebuilt;
        };
    }

    /** Deterministic key-column name for parent-member ordinal {@code ord}'s
     * child route on {@code prop} — shared by the extent arms and the
     * child-union arms. */
    private static String mixedKeyCol(String prop, int ord, int k) {
        return "k__" + prop + "__" + ord + "_" + k;
    }

    /** A mixed-union MEMBER's class-typed child route: the property, the
     * declared TARGET set, this member's join-key expressions (over the
     * member row var), and — for navigate routes — the raw condition (the
     * child side extracts its own operands from it). */
    record MixedRoute(String prop,
            @com.legend.Nullable String targetSetId,
            List<TypedSpec> memberKeys,
            com.legend.compiler.spec.typed.@com.legend.Nullable TypedLambda navCond) {}

    private List<MixedRoute> mixedMemberRoutes(ClassSource member,
            String mappingFqn) {
        var one = com.legend.compiler.element.type.Multiplicity.Bounded.ONE;
        List<MixedRoute> out = new ArrayList<>();
        ExprType mri = new ExprType(member.rowType(), one);
        for (var e : Pipelines.outerNavSteps(member.pipeline()).entrySet()) {
            var nav = e.getValue();
            String tgtSet = ctx.routedTargetSetOf(mappingFqn, e.getKey())
                    .orElse(null);
            if (tgtSet == null) {
                continue;   // unrouted navigate: not a per-member route
            }
            List<TypedSpec> pk = new ArrayList<>();
            List<TypedSpec> tk = new ArrayList<>();
            splitEqualCond(nav.predicate(), pk, tk);
            String p0 = nav.predicate().parameters().get(0);
            List<TypedSpec> rebased = pk.stream().map(x ->
                    Pipelines.rewriteRowReads(x, p0, Map.of(),
                            java.util.Set.of(),
                            v -> new TypedVariable(member.rowVar(), mri)))
                    .toList();
            out.add(new MixedRoute(e.getKey(), tgtSet, rebased,
                    nav.predicate()));
        }
        for (var b : member.bindings().entrySet()) {
            TypedSpec inner = b.getValue();
            if (inner instanceof TypedNativeCall w && w.args().size() == 1) {
                inner = w.args().get(0);
            }
            if (inner instanceof TypedNewInstanceCast nic
                    && nic.targetSetId() != null
                    && nic.source() instanceof TypedVariable v
                    && v.name().equals(member.rowVar())) {
                out.add(new MixedRoute(b.getKey(), nic.targetSetId(),
                        List.of(frameOrdinalRead(member)), null));
            }
        }
        return out;
    }

    private static TypedSpec frameOrdinalRead(ClassSource member) {
        var one = com.legend.compiler.element.type.Multiplicity.Bounded.ONE;
        for (Type.Column c : member.rowType().columns()) {
            if (c.name().equals(JsonSourceFrame.FRAME_ORDINAL)) {
                return new TypedPropertyAccess(new TypedVariable(
                        member.rowVar(),
                        new ExprType(member.rowType(), one)),
                        c.name(), new ExprType(c.type(), c.multiplicity()));
            }
        }
        throw new NotImplementedException("mixed-union member set of '"
                + member.classFqn() + "' has a whole-source child route but"
                + " its row carries no frame ordinal — only JSON-frame-backed"
                + " Pure members support per-member children yet");
    }

    /** The join condition split into aligned (parent, target) operand
     * lists — AND-of-equalities only, loud otherwise. */
    private static void splitEqualCond(
            com.legend.compiler.spec.typed.TypedLambda cond,
            List<TypedSpec> parentSide, List<TypedSpec> targetSide) {
        collectEqualPairs(cond.body().get(cond.body().size() - 1),
                cond.parameters().get(0), cond.parameters().get(1),
                parentSide, targetSide);
    }

    private static void collectEqualPairs(TypedSpec n, String p0, String p1,
            List<TypedSpec> ps, List<TypedSpec> ts) {
        if (n instanceof TypedNativeCall c) {
            String q = c.callee().qualifiedName();
            if (q.equals("meta::pure::functions::boolean::and")) {
                for (TypedSpec a : c.args()) {
                    collectEqualPairs(a, p0, p1, ps, ts);
                }
                return;
            }
            if (q.equals("meta::pure::functions::boolean::equal")
                    && c.args().size() == 2) {
                TypedSpec a = c.args().get(0);
                TypedSpec b = c.args().get(1);
                boolean a0 = readsVar(a, p0);
                boolean a1 = readsVar(a, p1);
                boolean b0 = readsVar(b, p0);
                boolean b1 = readsVar(b, p1);
                if (a0 && !a1 && b1 && !b0) {
                    ps.add(a);
                    ts.add(b);
                    return;
                }
                if (b0 && !b1 && a1 && !a0) {
                    ps.add(b);
                    ts.add(a);
                    return;
                }
            }
        }
        throw new NotImplementedException("mixed-union child route join"
                + " condition is not an AND-of-two-sided-equalities shape: "
                + n.getClass().getSimpleName());
    }


    /** A LINK KEY read on the pair's own arm row (B3.1b): the child union
     * member publishes the key under the navigation's name; on the arm the
     * read is the member's own physical column (the stamped fact
     * {@code linkKeys}), a typed NULL when the set publishes no such key.
     * Plain reads pass through. */
    private TypedSpec linkKeyOnArm(TypedSpec n, String mappingFqn, String setId,
            Type.RelationType armRow) {
        if (n instanceof TypedPropertyAccess pa) {
            String col = ctx.linkKeys(mappingFqn, setId).get(pa.property());
            if (col != null) {
                Type.Column c = armRow.columns().stream()
                        .filter(x -> x.name().equals(col)).findFirst().orElse(null);
                if (c == null) {
                    throw new NotImplementedException("mixed-union child arm '"
                            + setId + "' does not carry its link key column '"
                            + col + "' (" + pa.property() + ")");
                }
                return new TypedPropertyAccess(pa.source(), c.name(),
                        new ExprType(c.type(), c.multiplicity()));
            }
        }
        return SyntheticHeads.rebuildChildren(n,
                c -> linkKeyOnArm(c, mappingFqn, setId, armRow));
    }
    /** The KEYED CHILD UNION for a class-typed property over a mixed
     * extent: one arm per parent member (paired by the route's declared
     * target set), each projecting the child class's scalar properties
     * plus the pair's key columns (target-side operands; NULL elsewhere).
     * {@code keysPerPair} aligns with the parent's arm order. */
    record MixedChild(ClassSource target, List<List<String>> keysPerPair) {}

    @com.legend.Nullable MixedChild mixedChildMaterial(String mappingFqn, String classFqn,
            String prop, String childClassFqn) {
        List<String> memberIds = ctx.mixedUnionMembers(mappingFqn, classFqn);
        if (memberIds == null) {
            return null;
        }
        var one = com.legend.compiler.element.type.Multiplicity.Bounded.ONE;
        var optional = com.legend.compiler.element.type.Multiplicity.Bounded
                .ZERO_ONE;
        var many = com.legend.compiler.element.type.Multiplicity.Bounded
                .ZERO_MANY;
        List<String> ordered = mixedArmOrder(mappingFqn, classFqn, memberIds);
        List<MixedRoute> routes = new ArrayList<>();
        for (String memberId : ordered) {
            ClassSource mem = get(mappingFqn, classFqn, memberId, null, "", null);
            routes.add(mixedMemberRoutes(mem, mappingFqn).stream()
                    .filter(r -> r.prop().equals(prop)).findFirst()
                    .orElseThrow(() -> new NotImplementedException(
                            "mixed-union member set '" + memberId + "' of '"
                            + classFqn + "' has no child route for '" + prop
                            + "' — NULL-child arms are not built yet")));
        }
        var ccls = ctx.findClass(childClassFqn).orElseThrow();
        List<Type.Column> cCols = new ArrayList<>();
        for (var p : ccls.properties()) {
            if (!(Type.asClassType(p.type()) instanceof Type.ClassType)) {
                cCols.add(new Type.Column(p.name(), p.type(),
                        p.multiplicity()));
            }
        }
        List<Type.Column> keyCols = new ArrayList<>();
        List<List<String>> keysPerPair = new ArrayList<>();
        for (int i = 0; i < routes.size(); i++) {
            MixedRoute r = routes.get(i);
            List<String> names = new ArrayList<>();
            for (int k = 0; k < r.memberKeys().size(); k++) {
                String name = mixedKeyCol(prop, i, k);
                names.add(name);
                keyCols.add(new Type.Column(name,
                        r.memberKeys().get(k).info().type(), optional));
            }
            keysPerPair.add(names);
        }
        List<Type.Column> allCols = new ArrayList<>(cCols);
        allCols.addAll(keyCols);
        Type.RelationType rowType = new Type.RelationType(allCols);
        java.util.Set<String> distinctTargets = new LinkedHashSet<>();
        TypedSpec union = null;
        for (int i = 0; i < routes.size(); i++) {
            MixedRoute r = routes.get(i);
            if (!distinctTargets.add(r.targetSetId())) {
                throw new NotImplementedException("mixed-union child '" + prop
                        + "': two parent members route to the same target set"
                        + " '" + r.targetSetId() + "' — duplicate child arms"
                        + " would double rows");
            }
            ClassSource arm = get(mappingFqn, childClassFqn, r.targetSetId(),
                    null, "", null);
            TypedSpec pipe = Pipelines.materialize(arm.pipeline(),
                    java.util.Set.of(), childClassFqn).pipeline();
            Type.RelationType aRow = Type.requireRelationSchema(pipe.info().type());
            ExprType ari = new ExprType(aRow, one);
            List<TypedSpec> tKeys;
            if (r.navCond() == null) {
                tKeys = List.of(frameOrdinalRead(arm));
            } else {
                List<TypedSpec> pk = new ArrayList<>();
                List<TypedSpec> tk = new ArrayList<>();
                splitEqualCond(r.navCond(), pk, tk);
                String p1 = r.navCond().parameters().get(1);
                // the navigate cond's target-side reads are MEMBER COLUMNS
                // (routed per set): on the pair's OWN arm each is the
                // column its target set names, a typed NULL when the set
                // is not named
                String targetSet = java.util.Objects.requireNonNull(r.targetSetId());
                tKeys = tk.stream().map(x -> linkKeyOnArm(
                        Pipelines.rewriteRowReads(x, p1,
                                Map.of(), java.util.Set.of(),
                                v -> new TypedVariable(arm.rowVar(), ari)),
                        mappingFqn, targetSet, aRow)).toList();
            }
            List<com.legend.compiler.spec.typed.TypedFuncCol> pcols =
                    new ArrayList<>(allCols.size());
            for (Type.Column c : cCols) {
                TypedSpec b = arm.bindings().get(c.name());
                if (b == null) {
                    throw new NotImplementedException("mixed-union child arm"
                            + " '" + r.targetSetId() + "' does not bind"
                            + " property '" + c.name() + "'");
                }
                pcols.add(mixedCol(c.name(), b, aRow, arm.rowVar()));
            }
            for (int j = 0; j < routes.size(); j++) {
                List<String> names = keysPerPair.get(j);
                for (int k = 0; k < names.size(); k++) {
                    TypedSpec v = j == i ? tKeys.get(k)
                            : new TypedCollection(List.of(), new ExprType(
                                    routes.get(j).memberKeys().get(k)
                                            .info().type(), optional));
                    pcols.add(mixedCol(names.get(k), v, aRow, arm.rowVar()));
                }
            }
            TypedSpec armProj = new com.legend.compiler.spec.typed
                    .TypedProject(pipe, pcols, new ExprType(Type.relation(rowType), many));
            union = union == null ? armProj
                    : new com.legend.compiler.spec.typed.TypedConcatenate(
                            union, armProj, new ExprType(Type.relation(rowType), many));
        }
        ExprType rowInfo = new ExprType(rowType, one);
        String rowVar = "uc_row";
        Map<String, TypedSpec> bindings = new LinkedHashMap<>();
        for (Type.Column c : cCols) {
            bindings.put(c.name(), new TypedPropertyAccess(
                    new TypedVariable(rowVar, rowInfo), c.name(),
                    new ExprType(c.type(), c.multiplicity())));
        }
        return new MixedChild(new ClassSource(mappingFqn, childClassFqn,
                ClassSource.UNION_SET_ID, java.util.Objects.requireNonNull(union,
                        "mixed child with zero arms"),
                rowVar, bindings, rowType), keysPerPair);
    }

    /**
     * The property names a relational set maps ITSELF (batch 68, the
     * engine rule): binding metadata stamped before the implicit
     * same-extent inheritance pre-pass merged an ancestor's mappings in.
     * Empty = no restriction — an explicit {@code extends} set carries its
     * parent's mappings by contract, a function-form or unknown binding
     * declares none.
     */
    java.util.Set<String> ownPropertiesOf(String mappingFqn, String classFqn,
            @com.legend.Nullable String setId) {
        MappingDefinition mapping = ctx.findMapping(mappingFqn).orElse(null);
        if (mapping == null) {
            return java.util.Set.of();
        }
        MappingDefinition.ClassBinding binding = setId != null
                ? findBinding(mapping, classFqn, setId, new LinkedHashSet<>())
                : null;
        if (binding == null) {
            binding = findBinding(mapping, classFqn, new LinkedHashSet<>());
        }
        if (binding instanceof MappingDefinition.ClassBinding.Relational r
                && r.extendsSetId() == null) {
            return new java.util.LinkedHashSet<>(r.declared().ownProperties());
        }
        return java.util.Set.of();
    }

    private ClassSource build(String mappingFqn, String classFqn,
            @com.legend.Nullable String setId,
            @com.legend.Nullable java.util.function.BiFunction<String, String, String> upstreamMapping,
            String contextKey) {
        MappingDefinition mapping = ctx.findMapping(mappingFqn).orElseThrow(() ->
                new MappingResolutionException(
                        "unknown mapping '" + mappingFqn + "'", mappingFqn));
        MappingDefinition.ClassBinding binding = setId != null
                ? findBinding(mapping, classFqn, setId, new LinkedHashSet<>())
                : null;
        if (binding == null) {
            binding = findBinding(mapping, classFqn, new LinkedHashSet<>());
        }
        if (binding == null) {
            // MIXED-KIND UNION (route b, XSTORE_LEG design note): a union
            // with a Pure (M2M) member has no eager synthesis — the member
            // list rides the normalized model and the arms build HERE, per
            // member, over composed ClassSources. CLASS-LEVEL lookups only:
            // a setId-qualified miss must stay the loud not-mapped wall —
            // falling into this route would re-request the member and cycle.
            List<String> mixed = setId == null
                    ? ctx.mixedUnionMembers(mappingFqn, classFqn) : null;
            if (mixed != null) {
                return mixedUnionSource(mappingFqn, classFqn, mixed,
                        upstreamMapping, contextKey);
            }
            // JSON SOURCE FRAME (XStore §1): an unmapped class carried by a
            // JsonModelConnection in the execution context realizes as a
            // typed VALUES relation — the class declaration is the schema.
            String jsonUrl = jsonSources.get(classFqn);
            if (jsonUrl != null) {
                return JsonSourceFrame.sourceUrlFrame(ctx, mappingFqn, classFqn,
                        jsonUrl);
            }
            throw new MappingResolutionException("class '" + classFqn
                    + "' is not mapped in mapping '" + mappingFqn + "'"
                    + ctx.mappingPoison(mappingFqn, classFqn)
                            .map(r -> " (" + r + ")").orElse(""), classFqn);
        }

        List<TypedFunction> fns = ctx.findFunction(binding.functionFqn());
        if (fns.size() != 1) {
            throw new IllegalStateException("resolver bug: realizing function '"
                    + binding.functionFqn() + "' for class '" + classFqn
                    + "' has " + fns.size() + " overloads; synthesized FQNs are unique");
        }
        CompiledFunction cf = specs.compile(fns.get(0));

        // The terminal contract: a single-statement body whose last statement
        // is map(pipeline, row | ^Class(...)). Anything else is a normalizer
        // contract violation — the H1 census guarantees these bodies compile,
        // and the normalizer emits exactly this shape.
        TypedSpec last = cf.body().get(cf.body().size() - 1);
        if (!(last instanceof TypedMap map)) {
            throw new IllegalStateException("resolver bug: mapping body terminal for '"
                    + classFqn + "' in '" + mappingFqn + "' is "
                    + last.getClass().getSimpleName() + ", expected TypedMap"
                    + " (normalizer contract: pipeline -> map(row|^Class(...)))");
        }
        TypedLambda mapper = map.mapper();
        TypedSpec mapperBody = mapper.body().get(mapper.body().size() - 1);
        if (!(mapperBody instanceof TypedNewInstance ctor)
                || mapper.parameters().size() != 1) {
            throw new IllegalStateException("resolver bug: map terminal for '"
                    + classFqn + "' in '" + mappingFqn + "' is not a 1-param"
                    + " ^Class(...) constructor lambda: "
                    + mapperBody.getClass().getSimpleName());
        }

        TypedSpec pipeline = sealExtentWindows(map.source());
        Type.RelationType rowType = Type.relationSchema(pipeline.info().type());
        if (rowType == null) {
            // A CLASS-typed pipeline is a MODEL-TO-MODEL mapping: the body is
            // getAll(Upstream)->map(src|^Target(...)). Composition is pure
            // β-transitivity (plan H5): resolve the UPSTREAM class through
            // this same mapping (memo + cycle guard ride along), then
            // substitute every $src.prop read with the upstream's binding —
            // the composed table sits over the upstream's own pipeline.
            if (Type.asClassType(pipeline.info().type()) instanceof Type.ClassType src) {
                return composeModelToModel(mappingFqn, classFqn, binding,
                        pipeline, mapper, ctor, src, upstreamMapping, contextKey);
            }
            throw new IllegalStateException("resolver bug: mapping pipeline for '"
                    + classFqn + "' in '" + mappingFqn + "' types as "
                    + pipeline.info().type().typeName() + ", expected a relation row");
        }

        // Binding-table conformance: every ^Class key is a declared property.
        // (Full type/multiplicity conformance is G's guarantee — the body
        // compiled through NewChecker's strict subsumption. This assert
        // catches property-set drift loudly at the extraction seam.)
        Map<String, TypedSpec> bindings = new LinkedHashMap<>();
        for (Map.Entry<String, TypedSpec> e : ctor.properties().entrySet()) {
            // NewChecker is the construction gate: a ctor key that is not a
            // class property can ONLY be a validated mapping-LOCAL property
            // (+id: Integer[1]: COL — owned by the mapping); it binds like
            // any other (XStore predicates read locals through bindings)
            bindings.put(e.getKey(), e.getValue());
        }

        // SUBTYPE-DISPATCH pseudo-bindings: a union/inheritance synthesis
        // carries class-qualified thread-local subtype columns
        // (ClassMapping.subTypeColumn contract) in its row — expose each as
        // a binding keyed by its own column name so subType(@Sub).prop
        // reads dispatch through the ordinary binding table (nav positions
        // included: assocLeaf resolves the synthetic leaf like any other)
        for (Type.Column c : rowType.columns()) {
            if (com.legend.model.ClassMapping.isSubTypeColumn(c.name())) {
                bindings.put(c.name(), new TypedPropertyAccess(
                        new TypedVariable(mapper.parameters().get(0),
                                ExprType.one(rowType)),
                        c.name(), new ExprType(c.type(), c.multiplicity())));
            }
        }
        // PRIMARY-KEY pseudo-bindings (ClassMapping.primaryKeyBinding): the
        // declared ~primaryKey columns the row carries, keyed by the D3
        // contract name — the element-reference rule filters a metaclass
        // extent on them (StoreResolver.elementRow); never a property.
        for (String pkCol : binding.primaryKeyColumns()) {
            for (Type.Column c : rowType.columns()) {
                if (c.name().equals(pkCol)) {
                    bindings.putIfAbsent(
                            com.legend.model.ClassMapping.primaryKeyBinding(pkCol),
                            new TypedPropertyAccess(
                                    new TypedVariable(mapper.parameters().get(0),
                                            ExprType.one(rowType)),
                                    c.name(), new ExprType(c.type(), c.multiplicity())));
                }
            }
        }

        ForeignKeyIdentity.register(bindings, ctor, mapper.parameters().get(0),
                pipeline, rowType, mapping, ctx);

        // SAME-SOURCE SUBTYPE DISPATCH for nav targets (#71, extends
        // family): a subclass mapped over the SAME root table (F[f]
        // extends [e]) contributes stc_<F>___<prop> pseudo-bindings from
        // its OWN ctor fields renamed onto this row — subType(@F).prop
        // casts through associations read them as ordinary leaves (engine
        // golden testExtendsForPropertyMapping.pure result4: the cast is
        // a plain same-row column read). Different-source, slot-reading,
        // or ctor-valued sub bindings stay un-synthesized (loud
        // downstream). Local bindings only — extends is same-mapping.
        Map<String, com.legend.compiler.spec.typed.TypedNavigate>
                stcNavTransplants = new LinkedHashMap<>();
        for (MappingDefinition.ClassBinding cb : mapping.classBindings()) {
            if (cb.classFqn().equals(classFqn)
                    || !ctx.isSubtype(cb.classFqn(), classFqn)) {
                continue;
            }
            ClassSource sub;
            try {
                sub = get(mappingFqn, cb.classFqn(), null);
            } catch (RuntimeException notBuildable) {
                // a broken subclass mapping must not poison the PARENT
                // source — casts to it stay loud at their own read
                continue;
            }
            if (!sameRootTable(sub.pipeline(), pipeline)
                    && !sameRootTableUnderSubstitution(mapping,
                            sub.pipeline(), pipeline)) {
                continue;
            }
            java.util.Set<String> subSlots =
                    Pipelines.slotAliases(sub.pipeline());
            String pfx = com.legend.model.ClassMapping
                    .subTypeColumnPrefix(cb.classFqn());
            ExprType rowInfo = ExprType.one(rowType);
            var subNavSteps = Pipelines.navSteps(sub.pipeline());
            for (Map.Entry<String, TypedSpec> be : sub.bindings().entrySet()) {
                if (com.legend.model.ClassMapping.isSubTypeColumn(be.getKey())) {
                    continue;
                }
                // ctor-valued (EMBEDDED) sub bindings transplant like any
                // other same-row read when they touch no slot alias — the
                // generic rename below re-roots the ctor's column reads
                // and Substitution's ctor drill serves the cast leaves
                // (#71 single-table hierarchies); slot-reading ctors stay
                // un-synthesized (loud downstream)
                // a CLASS-typed slot read (rating:@Product_Rating on the
                // subclass): the cast hop navigates the SUB's slot — the
                // step TRANSPLANTS into this pipeline under the stc alias
                // (same root table, so its condition reads this row), and
                // the pseudo-binding is an ordinary slot read the demand /
                // SubNav machinery walks. Sibling-reading conditions stay
                // un-synthesized (their aliases don't exist here — loud).
                TypedSpec inner = Pipelines.unwrapToOne(be.getValue());
                if (inner instanceof TypedPropertyAccess spa
                        && spa.source() instanceof TypedVariable spv
                        && spv.name().equals(sub.rowVar())
                        && subNavSteps.containsKey(spa.property())
                        && Type.asClassType(inner.info().type()) instanceof Type.ClassType) {
                    var st = subNavSteps.get(spa.property());
                    java.util.Set<String> siblings =
                            new java.util.LinkedHashSet<>(subSlots);
                    siblings.remove(spa.property());
                    boolean readsSibling = false;
                    for (TypedSpec pb : st.predicate().body()) {
                        for (String sp0 : st.predicate().parameters()) {
                            if (Pipelines.referencesAliasOn(pb, sp0, siblings)) {
                                readsSibling = true;
                            }
                        }
                    }
                    if (!readsSibling) {
                        String stcAlias = pfx + be.getKey();
                        stcNavTransplants.putIfAbsent(stcAlias, st);
                        bindings.putIfAbsent(stcAlias, new TypedPropertyAccess(
                                new TypedVariable(mapper.parameters().get(0),
                                        rowInfo),
                                stcAlias, inner.info()));
                    }
                    continue;
                }
                if (Pipelines.referencesAliasOn(be.getValue(),
                        sub.rowVar(), subSlots)) {
                    continue;
                }
                bindings.putIfAbsent(pfx + be.getKey(),
                        Pipelines.rewriteRowReads(be.getValue(), sub.rowVar(),
                                Map.of(), java.util.Set.of(),
                                v -> new TypedVariable(
                                        mapper.parameters().get(0), rowInfo)));
            }
        }
        for (var tr : stcNavTransplants.entrySet()) {
            var st = tr.getValue();
            pipeline = new com.legend.compiler.spec.typed.TypedNavigate(
                    pipeline, java.util.Optional.of(tr.getKey()), st.target(),
                    st.predicate(), st.form(), pipeline.info());
        }

        // A ~func Relation pipeline may ITSELF be a class query
        // (PersonWithFirmId.all()->filter->project — the relation-family
        // MixedMapping): resolve it recursively with a FRESH resolver
        // instance (own per-resolution state — never the caller's frame)
        // against this same mapping. Self-referential ~funcs would recurse
        // across instances — no corpus shape does; a cycle dies by stack,
        // loudly, not silently.
        if (anchoredInFlow(pipeline)) {
            var nested = new StoreResolver(ctx, specs)
                    .resolve(java.util.List.of(pipeline), null, mappingFqn);
            pipeline = nested.get(0);
        }

        return new ClassSource(mappingFqn, classFqn, binding.setId(),
                pipeline, mapper.parameters().get(0), bindings, rowType);
    }

    private static boolean anchoredInFlow(TypedSpec n) {
        return Anchors.anchoredInFlow(n);
    }

    /**
     * MODEL-TO-MODEL composition (plan H5, scalar slice): the target's
     * binding table substitutes through the upstream class's — a binding
     * {@code fullName: $src.firstName + ' ' + $src.lastName} composes to
     * the upstream's own row expressions, so the result is an ordinary
     * relation-backed {@link ClassSource} and NOTHING downstream knows M2M
     * existed. Association navigation and whole-instance uses of the
     * source are the class-typed slice (H5b) — loud.
     */
    private ClassSource composeModelToModel(String mappingFqn, String classFqn,
            MappingDefinition.ClassBinding binding, TypedSpec pipeline,
            TypedLambda mapper, TypedNewInstance ctor, Type.ClassType srcType,
            @com.legend.Nullable java.util.function.BiFunction<String, String, String> upstreamMapping,
            String contextKey) {
        // Ops between the extent and the constructor: instance-space
        // FILTERS compose (their predicates substitute through the
        // upstream bindings like everything else); other ops are loud.
        List<TypedFilter> filters = new ArrayList<>();
        TypedSpec cur = pipeline;
        while (!(cur instanceof TypedGetAll)) {
            if (cur instanceof TypedFilter f) {
                filters.add(f);
                cur = f.source();
                continue;
            }
            throw new NotImplementedException("model-to-model pipeline of '"
                    + classFqn + "' in '" + mappingFqn + "' carries a "
                    + cur.getClass().getSimpleName() + " between the source"
                    + " extent and the constructor — not supported yet (H5c)");
        }
        // The upstream class resolves in THIS mapping when bound here (or
        // via includes); otherwise through the runtime dispatch — corpus
        // runtimes list the relational base and the M2M layers side by side.
        // SELF-SOURCED M2M (Trade FROM Trade — the self-edge leaf idiom,
        // walkM2MChain's rule): the source NEVER re-resolves in this
        // mapping; the dispatch names the upstream layer EXCLUDING self.
        boolean selfSourced = srcType.fqn().equals(classFqn)
                || binding.classFqn().equals(srcType.fqn());
        ClassSource inner;
        String jsonUrl = selfSourced ? jsonSources.get(srcType.fqn()) : null;
        if (jsonUrl != null) {
            // SELF-SOURCED M2M whose upstream is the execution context's
            // JsonModelConnection (TradeLinkage cross-store golden): the
            // JSON frame IS the source layer — never this mapping again
            inner = JsonSourceFrame.sourceUrlFrame(ctx, mappingFqn,
                    srcType.fqn(), jsonUrl);
        } else {
            String srcMapping = !selfSourced
                    && (binds(mappingFqn, srcType.fqn())
                            || upstreamMapping == null)
                    ? mappingFqn
                    : upstreamMapping == null ? mappingFqn
                            : upstreamMapping.apply(srcType.fqn(),
                                    selfSourced ? mappingFqn : null);
            inner = get(srcMapping, srcType.fqn(), upstreamMapping,
                    contextKey, null);
        }
        String srcVar = mapper.parameters().get(0);
        TypedSpec composedPipeline = inner.pipeline();
        for (int i = filters.size() - 1; i >= 0; i--) {
            TypedLambda lam = filters.get(i).predicate();
            String v = lam.parameters().get(0);
            List<TypedSpec> body = lam.body().stream().map(b ->
                    substituteSourceReads(b, v, inner, classFqn, mappingFqn, false)).toList();
            var fnType = new Type.FunctionType(
                    List.of(new Type.Param(inner.rowType(),
                            Multiplicity.Bounded.ONE)),
                    new Type.Param(Type.Primitive.BOOLEAN,
                            Multiplicity.Bounded.ONE));
            composedPipeline = new TypedFilter(
                    composedPipeline,
                    new TypedLambda(List.of(inner.rowVar()), body,
                            new ExprType(fnType,
                                    Multiplicity.Bounded.ONE)),
                    composedPipeline.info());
        }
        Map<String, TypedSpec> composed = new LinkedHashMap<>();
        Map<String, String> deferred = new LinkedHashMap<>();
        for (Map.Entry<String, TypedSpec> e : ctor.properties().entrySet()) {
            // a key that is NOT a class property is a mapping-LOCAL (+prop)
            // binding (the XStore assoc-key idiom): the normalizer emits it
            // with the isLocal KeyExpression and NewChecker types it by its
            // own value — unknown NON-local keys were already rejected at
            // NORMALIZE (synthM2M's deep property check), so it composes
            // as an extra binding column here.
            try {
                composed.put(e.getKey(), substituteSourceReads(e.getValue(),
                        srcVar, inner, classFqn, mappingFqn));
            } catch (NotImplementedException wall) {
                // PER-KEY deferral (ledger cluster 21): the wall throws at
                // READ time via ClassSource.throwIfDeferred — a query that
                // never demands this property composes cleanly (the H5b
                // eager loop sank whole class mappings for bindings no
                // query read).
                deferred.put(e.getKey(), wall.getMessage());
            }
        }
        // audit 24 F4: the composition's FRAME IDENTITY — the deep source
        // class (jsonSources key); two sets sharing it share the frame
        return new ClassSource(mappingFqn, classFqn, binding.setId(),
                composedPipeline, inner.rowVar(), composed, inner.rowType(),
                inner.sourceClass() != null ? inner.sourceClass()
                        : inner.classFqn(), deferred);
    }

    /**
     * β-substitute {@code $src.prop} reads with the upstream's bindings.
     * Closed vocabulary with a LOUD default — a node this rewriter does not
     * know is a normalizer contract change, never silent.
     */
    private TypedSpec substituteSourceReads(TypedSpec n, String srcVar,
            ClassSource inner, String classFqn, String mappingFqn) {
        return substituteSourceReads(n, srcVar, inner, classFqn, mappingFqn, true);
    }

    private TypedSpec substituteSourceReads(TypedSpec n, String srcVar,
            ClassSource inner, String classFqn, String mappingFqn,
            boolean bindingPosition) {
        if (n instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable v
                && v.name().equals(srcVar)) {
            TypedSpec bound = inner.bindings().get(pa.property());
            if (bound == null) {
                // An ASSOCIATION property of the source class: in BINDING
                // position the read becomes a SOURCE-NAV MARKER — the access
                // re-pointed at the composed row var, source-class-typed —
                // consumed only by the GRAPH-CHILD path (address:
                // $src.rawAddresses fans out as a correlated child). A read
                // THROUGH the association ($src.boss.age) stays loud: it
                // would need a scalar join this composition cannot emit.
                if (bindingPosition
                        && ctx.findAssociationOf(inner.classFqn(), pa.property()).isPresent()) {
                    return new TypedPropertyAccess(
                            new TypedVariable(
                                    inner.rowVar(),
                                    ExprType.one(
                                            new Type
                                                    .ClassType(inner.classFqn()))),
                            pa.property(), pa.info());
                }
                throw new NotImplementedException("model-to-model binding of '"
                        + classFqn + "' in '" + mappingFqn + "' navigates '$"
                        + srcVar + "." + pa.property() + "' — an unmapped"
                        + " non-association property of source class '"
                        + inner.classFqn() + "' is not supported yet (H5b)");
            }
            return bound;
        }
        return switch (n) {
            case TypedVariable v
                    when v.name().equals(srcVar) -> {
                // WHOLE-SOURCE instance in BINDING position
                // (trader[trader_set]: $src): the SAME source row seen
                // through another set — re-point at the composed row var,
                // SOURCE-CLASS-typed (the assoc-marker discipline); sole
                // consumer is the graph-child path (wholeSrcChild), every
                // query-position read stays loud downstream.
                if (bindingPosition) {
                    yield new TypedVariable(inner.rowVar(),
                            ExprType.one(new Type.ClassType(inner.classFqn())));
                }
                throw new NotImplementedException("model-to-model binding of '"
                        + classFqn + "' uses the whole source instance '$"
                        + srcVar + "' — not supported yet (H5b)");
            }
            case TypedVariable v -> v;
            // ^Target($src.prop): the M2M CAST — substitute within its
            // source; the cast survives as a CLASS-TYPED binding (read
            // sites give it graph-child / H4 stories, never silent SQL).
            case TypedNewInstanceCast nic ->
                    new TypedNewInstanceCast(
                            nic.classFqn(),
                            substituteSourceReads(nic.source(), srcVar, inner,
                                    classFqn, mappingFqn, bindingPosition),
                            nic.info(), nic.targetSetId());
            // a LEAF read through the MILESTONED step keeps binding
            // position (the marker chain: PropertyAccess over
            // MilestonedAccess over the source marker — the graph
            // child's primitive-array arm consumes it); ordinary
            // chains still query-position their sources
            case TypedPropertyAccess pa ->
                    new TypedPropertyAccess(
                            substituteSourceReads(pa.source(), srcVar, inner,
                                    classFqn, mappingFqn,
                                    bindingPosition && pa.source()
                                            instanceof com.legend.compiler
                                                    .spec.typed
                                                    .TypedMilestonedAccess),
                            pa.property(), pa.info());
            // a MILESTONED property access ($src.product(%d)) is a
            // property step with temporal arguments — source and dates
            // substitute independently, the step survives verbatim
            // BINDING position passes THROUGH the milestoned step: a
            // top-level $src.<generatedNav>(…) binding composes as the
            // source-class MARKER under the temporal step (the graph-
            // child channel's shape); query-position chains still
            // arrive with false from the enclosing access arm.
            case com.legend.compiler.spec.typed.TypedMilestonedAccess ma ->
                    new com.legend.compiler.spec.typed.TypedMilestonedAccess(
                            substituteSourceReads(ma.source(), srcVar, inner,
                                    classFqn, mappingFqn, bindingPosition),
                            ma.property(),
                            ma.dates().stream().map(d -> substituteSourceReads(
                                    d, srcVar, inner, classFqn, mappingFqn,
                                    false)).toList(),
                            ma.sweep(), ma.info());
            case TypedNativeCall c ->
                    c.withChildren(c.args().stream().map(a -> substituteSourceReads(a,
                                    srcVar, inner, classFqn, mappingFqn, false)).toList());
            case TypedCollection c -> c.withChildren(c.elements().stream().map(a -> substituteSourceReads(a,
                                    srcVar, inner, classFqn, mappingFqn, false)).toList());
            case TypedIf i ->
                    new TypedIf(
                            substituteSourceReads(i.condition(), srcVar, inner,
                                    classFqn, mappingFqn, false),
                            substituteSourceReads(i.thenBranch(), srcVar, inner,
                                    classFqn, mappingFqn, false),
                            i.elseBranch().map(e2 -> substituteSourceReads(e2,
                                    srcVar, inner, classFqn, mappingFqn, false)),
                            i.info());
            case TypedLambda l -> {
                if (l.parameters().contains(srcVar)) {
                    yield l;   // shadowing: substitution stops (capture rule)
                }
                // CAPTURE guard (audit F2): a lambda parameter named like
                // the UPSTREAM row var would capture the substituted
                // binding's row reads — loud, never silently mis-scoped.
                if (l.parameters().contains(inner.rowVar())
                        && readsVar(l, srcVar)) {
                    throw new NotImplementedException("model-to-model binding of '"
                            + classFqn + "' in '" + mappingFqn + "' has a lambda"
                            + " parameter named '" + inner.rowVar() + "' shadowing"
                            + " the upstream mapping's row variable — rename the"
                            + " parameter");
                }
                yield new TypedLambda(l.parameters(),
                        l.body().stream().map(b -> substituteSourceReads(b,
                                srcVar, inner, classFqn, mappingFqn, false)).toList(),
                        l.info());
            }
            case TypedCString ignored -> n;
            case TypedCInteger ignored -> n;
            case TypedCFloat ignored -> n;
            case TypedCDecimal ignored -> n;
            case TypedCBoolean ignored -> n;
            case TypedCDate ignored -> n;
            case TypedEnumValue ignored -> n;
            default -> throw new NotImplementedException(
                    "model-to-model binding node "
                            + n.getClass().getSimpleName()
                            + " is not substitutable yet (H5 vocabulary)");
        };
    }

    /** Whether any {@code $var} read occurs in {@code n}'s subtree. */
    private static boolean readsVar(TypedSpec n, String var) {
        return com.legend.compiler.spec.typed.VarUse.reads(n, var);
    }

    /**
     * Whether {@code mappingFqn} (or its includes) binds {@code classFqn} —
     * the runtime-dispatch probe: a multi-mapping runtime picks the ONE
     * candidate that binds the fetched class. Never throws (a multi-set-ID
     * binding still counts as "binds"; the loud path is {@link #get}).
     */
    public boolean binds(String mappingFqn, String classFqn) {
        return ctx.findMapping(mappingFqn)
                .map(m -> bindsIn(m, classFqn, new LinkedHashSet<>()))
                .orElse(false);
    }

    private boolean bindsIn(MappingDefinition mapping, String classFqn,
                            LinkedHashSet<String> visited) {
        if (!visited.add(mapping.qualifiedName())) {
            return false;
        }
        for (MappingDefinition.ClassBinding cb : mapping.classBindings()) {
            if (cb.classFqn().equals(classFqn)) {
                return true;
            }
        }
        for (MappingInclude inc : mapping.includes()) {
            MappingDefinition inner = ctx.findMapping(inc.mappingPath()).orElseThrow(() ->
                    new MappingResolutionException("mapping '" + mapping.qualifiedName()
                            + "' includes unknown mapping '" + inc.mappingPath()
                            + "' (a silently-unresolved include hid class bindings)"));
            if (bindsIn(inner, classFqn, visited)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The class binding within {@code mapping} or its includes: hits are
     * collected across the WHOLE include closure — a class bound by two
     * included mappings is a loud ambiguity (real Legend errors on
     * duplicate class mappings), never a silent depth-first pick. A local
     * binding shadows included ones (checked first, include semantics).
     * Multi-set-ID within one mapping is a legal-but-unbuilt feature (H5).
     */
    /** As {@link #sameRootTable}, but stores equated through the mapping's
     * include-closure STORE SUBSTITUTIONS ({@code include m[db->MyDb]}):
     * the included set's pipeline keeps the ORIGINAL store name while a
     * local subclass extends it against the substituted one — same
     * physical table, two spellings of the store. */
    private boolean sameRootTableUnderSubstitution(MappingDefinition mapping,
            TypedSpec a, TypedSpec b) {
        var ra = rootTableOf(a);
        var rb = rootTableOf(b);
        if (ra == null || rb == null || !ra.table().equals(rb.table())) {
            return false;
        }
        java.util.LinkedHashSet<String> visited = new java.util.LinkedHashSet<>();
        java.util.ArrayDeque<MappingDefinition> work = new java.util.ArrayDeque<>();
        work.add(mapping);
        while (!work.isEmpty()) {
            MappingDefinition m = work.poll();
            if (!visited.add(m.qualifiedName())) {
                continue;
            }
            for (MappingInclude inc : m.includes()) {
                for (MappingInclude.StoreSubstitution sub
                        : inc.substitutions()) {
                    if ((sub.originalStore().equals(ra.store())
                                && sub.replacementStore().equals(rb.store()))
                            || (sub.originalStore().equals(rb.store())
                                && sub.replacementStore().equals(ra.store()))) {
                        return true;
                    }
                }
                ctx.findMapping(inc.mappingPath()).ifPresent(work::add);
            }
        }
        return false;
    }

    /** Both pipelines scan the SAME leftmost physical table. */
    private static boolean sameRootTable(TypedSpec a, TypedSpec b) {
        com.legend.compiler.spec.typed.TypedTableReference ra = rootTableOf(a);
        com.legend.compiler.spec.typed.TypedTableReference rb = rootTableOf(b);
        return ra != null && rb != null
                && ra.store().equals(rb.store())
                && ra.table().equals(rb.table());
    }

    private static com.legend.compiler.spec.typed.@com.legend.Nullable TypedTableReference
            rootTableOf(TypedSpec n) {
        if (n instanceof com.legend.compiler.spec.typed.TypedTableReference tr) {
            return tr;
        }
        for (TypedSpec c : n.children()) {
            var r = rootTableOf(c);
            if (r != null) {
                return r;
            }
        }
        return null;
    }

    private MappingDefinition.@com.legend.Nullable ClassBinding findBinding(MappingDefinition mapping,
                                                       String classFqn,
                                                       LinkedHashSet<String> visited) {
        return findBinding(mapping, classFqn, null, visited);
    }

    /** {@code setId} non-null = H5 SET-ID DISPATCH: a route naming a set
     * resolves to THAT set's binding, root-ness irrelevant. Null = the
     * class-level lookup: the ROOT binding wins among a class's set
     * bindings (engine .all() = root only); a rootless multi-set class
     * yields no class-level binding (the normalizer's implicit-union
     * poison explains the 0-binder error). */
    private MappingDefinition.@com.legend.Nullable ClassBinding findBinding(MappingDefinition mapping,
                                                       String classFqn,
                                                       @com.legend.Nullable String setId,
                                                       LinkedHashSet<String> visited) {
        List<MappingDefinition.ClassBinding> local = new ArrayList<>();
        for (MappingDefinition.ClassBinding cb : mapping.classBindings()) {
            if (cb.classFqn().equals(classFqn)
                    && (setId == null || setId.equals(cb.setId()))) {
                local.add(cb);
            }
        }
        if (setId == null && local.size() > 1) {
            // an AggregationAware main's VIEWS are sets the ROUTER picks by
            // id (AggregationAwareRouting) — never class-level candidates
            // (engine: .all() is the main set; a view is a rewrite target)
            java.util.Set<String> viewIds = new java.util.HashSet<>();
            for (MappingDefinition.ClassBinding cb : local) {
                if (cb instanceof MappingDefinition.ClassBinding.Relational rb) {
                    for (var v : rb.aggregateViews()) {
                        viewIds.add(v.setId());
                    }
                }
            }
            if (!viewIds.isEmpty()) {
                local = local.stream()
                        .filter(cb -> cb.setId() == null || !viewIds.contains(cb.setId()))
                        .toList();
            }
        }
        if (setId == null && local.size() > 1) {
            List<MappingDefinition.ClassBinding> roots = local.stream()
                    .filter(MappingDefinition.ClassBinding::root).toList();
            if (roots.size() == 1) {
                local = roots;
            } else if (roots.isEmpty()) {
                local = List.of();   // rootless multi-set: 0-binder + poison
            } else {
                throw new MappingResolutionException("class '" + classFqn
                        + "' has " + roots.size() + " ROOT set bindings in"
                        + " mapping '" + mapping.qualifiedName() + "'",
                        classFqn);
            }
        }
        if (local.size() == 1) {
            // the mapping's own binding beats every include's (the engine's
            // rootClassMappingByClass: includes' answers first, then own,
            // the LAST root wins — clean-sheet homework R1); duplicate set
            // ids across a closure are rejected at Phase E (R5), as the
            // engine's compiler rejects them
            return local.get(0);
        }
        visited.add(mapping.qualifiedName());
        // R1 among the includes: each include answers with its own rule
        // (own beats its includes'); the LATER include beats the earlier.
        // A class mapped in two included mappings is legal in the engine;
        // the old "ambiguously mapped" wall here was ours alone.
        MappingDefinition.ClassBinding last = null;
        for (MappingInclude inc : mapping.includes()) {
            if (visited.contains(inc.mappingPath())) {
                continue;
            }
            MappingDefinition inner = ctx.findMapping(inc.mappingPath()).orElseThrow(() ->
                    new MappingResolutionException("mapping '" + mapping.qualifiedName()
                            + "' includes unknown mapping '" + inc.mappingPath() + "'"));
            MappingDefinition.ClassBinding found = findBinding(inner, classFqn, setId, visited);
            if (found != null) {
                last = found;
            }
        }
        return last;
    }

    /** Per-class dispatch: the runtime candidate that BINDS the class wins. */
    String dispatch(@com.legend.Nullable String explicitMapping,
            @com.legend.Nullable String runtimeFqn,
            java.util.List<String> chainMappings, String classFqn) {
        return dispatch(explicitMapping, runtimeFqn, chainMappings, classFqn,
                null);
    }

    /** {@code exclude} non-null names a mapping the dispatch must NOT
     * pick — a SELF-SOURCED M2M's upstream layer (composeModelToModel). */
    String dispatch(@com.legend.Nullable String explicitMapping,
            @com.legend.Nullable String runtimeFqn,
            java.util.List<String> chainMappings, String classFqn,
            @com.legend.Nullable String exclude) {
        if (explicitMapping != null) {
            // MAPPING CHAIN (XStore leg slice 1): a class the explicit
            // mapping does NOT bind resolves through the runtime value's
            // ModelChainConnection mappings — the M2M ~src route (engine:
            // the ModelStore's connection IS another mapping). Exactly-one
            // binder, loud otherwise; no chain = the explicit mapping's
            // own downstream wall stays.
            if (!chainMappings.isEmpty()
                    && (!binds(explicitMapping, classFqn)
                            || explicitMapping.equals(exclude))) {
                List<String> chainBinders = chainMappings.stream()
                        .distinct()
                        .filter(m -> !m.equals(exclude) && binds(m, classFqn))
                        .toList();
                if (chainBinders.size() == 1) {
                    return chainBinders.get(0);
                }
                throw new MappingResolutionException("class '" + classFqn
                        + "' is not mapped in '" + explicitMapping
                        + "' and its ModelChainConnection "
                        + chainMappings + " has "
                        + chainBinders.size() + " binders — chain dispatch"
                        + " needs exactly one", classFqn);
            }
            if (!explicitMapping.equals(exclude)) {
                return explicitMapping;
            }
            // the EXCLUDED explicit mapping (self-sourced M2M upstream)
            // with no chain: fall through to the runtime candidates
            if (runtimeFqn == null) {
                throw new MappingResolutionException("self-sourced class '"
                        + classFqn + "' needs an upstream layer, but the"
                        + " context names only mapping '" + explicitMapping
                        + "' (no runtime candidate set)", classFqn);
            }
        }
        // NO explicit mapping but a CHAIN channel (query-side
        // withChainedMappings->from(rt), or a ModelChainConnection-only
        // runtime): the chain mappings ARE the dispatch candidates —
        // engine semantics, same exactly-one rule as the explicit arm
        // (slice-1 job 1: this arm existed only behind an explicit
        // mapping, so runtime-only chains fell to the ambient
        // candidate list).
        if (explicitMapping == null && !chainMappings.isEmpty()) {
            List<String> chainBinders = chainMappings.stream()
                    .distinct()
                    .filter(m -> !m.equals(exclude) && binds(m, classFqn))
                    .toList();
            if (chainBinders.size() == 1) {
                return chainBinders.get(0);
            }
            throw new MappingResolutionException("class '" + classFqn
                    + "' chain dispatch over " + chainMappings + " has "
                    + chainBinders.size() + " binders — needs exactly one",
                    classFqn);
        }
        // NO explicit mapping, NO chain: dispatch by the DECLARED
        // runtime's mapping list — a model-declared Runtime naming its
        // mappings is real engine API (the unit fixtures' test::
        // TestRuntime). The HARNESS compensation is gone regardless:
        // the corpus overlay runtime (rcorpus::Rt) now declares an
        // EMPTY mapping list (slice-1 job 1 — every corpus consumer
        // threads the call site's own mapping), so a corpus query
        // landing here walls loudly on zero candidates.
        com.legend.model.RuntimeDefinition rt = ctx.findRuntime(runtimeFqn).orElseThrow(() ->
                new MappingResolutionException("unknown runtime '"
                        + runtimeFqn + "'", runtimeFqn));
        List<String> binders = rt.mappings().stream()
                .distinct()   // a runtime listing a mapping twice is not ambiguity
                .filter(m -> !m.equals(exclude) && binds(m, classFqn))
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
            throw new MappingResolutionException("runtime '" + runtimeFqn
                    + "' has " + binders.size() + " mappings binding class '"
                    + classFqn + "' (of " + rt.mappings().size()
                    + " candidates); class-query dispatch needs exactly one" + why,
                    classFqn);
        }
        return binders.get(0);
    }
}