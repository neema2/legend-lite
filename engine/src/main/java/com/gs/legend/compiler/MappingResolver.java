package com.gs.legend.compiler;

import com.gs.legend.compiled.CompiledExpression;
import com.gs.legend.compiled.CompiledFunction;
import com.gs.legend.compiled.ResolvedExpression;
import com.gs.legend.compiler.typed.*;
import com.gs.legend.model.ModelContext;
import com.gs.legend.model.m3.PureClass;
import com.gs.legend.model.m3.Type;
import com.gs.legend.model.mapping.ClassMapping;
import com.gs.legend.model.mapping.PureClassMapping;
import com.gs.legend.model.mapping.RelationalMapping;
import com.gs.legend.model.store.PropertyMapping;

import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pass 3: Resolves mappings to physical store concepts over the typed HIR.
 *
 * <p>Runs AFTER {@link TypeChecker} (Pass 2), BEFORE PlanGenerator (Pass 4).
 * Consumes {@link CompiledExpression} (typed HIR + compiled mapping functions
 * exposed via {@link com.gs.legend.compiled.CompiledDependencies#mappingFunctions})
 * and {@link NormalizedMapping} (mapping-layer metadata). Holds no
 * {@link TypeChecker} reference — everything needed has already been compiled
 * and stashed in {@code CompiledDependencies} by the time Pass 3 runs.
 *
 * <h3>Mapping-function lookup</h3>
 *
 * <p>Mapping functions (synthesized by {@code MappingNormalizer}, or
 * user-authored in the future) have already been compiled: the query-root
 * fetch is compiled by {@code GetAllChecker} and a pass-2 fan-out over
 * {@code associationNavigations} compiles every target class's mapping
 * function. All results live in {@code CompiledDependencies.mappingFunctions}
 * keyed by class FQN. {@link #resolveClassFetch(String)} reads the compiled
 * body from there and walks it.
 *
 * <p>The same recursion primitive handles {@code TypedUserCall} — there is
 * no structural distinction between user-written and synthesized functions
 * at this layer.
 *
 * <h3>Walker shape</h3>
 * <ul>
 *   <li><b>Anchors</b> ({@code TypedGetAll}, {@code TypedNewInstance}) create
 *       new {@link StoreResolution} entries.</li>
 *   <li><b>Propagators</b> (relation operators: Filter, Extend, Select, etc.)
 *       inherit the resolution from their source operand.</li>
 *   <li><b>Recursion</b> ({@code TypedUserCall}, and {@code TypedGetAll} via
 *       its synthetic function) resolves callee bodies.</li>
 *   <li><b>Descent-only</b> (literals, variables, lambdas, collections,
 *       control flow) walks children without producing a resolution.</li>
 * </ul>
 *
 * <h3>Pipeline</h3>
 * <pre>
 * PureParser → MappingNormalizer → TypeChecker → MappingResolver → PlanGenerator
 *  (Pass 1)      (Pass 1.5)         (Pass 2)       (Pass 3)          (Pass 4)
 * </pre>
 */
public final class MappingResolver {

    private final CompiledExpression typeResult;
    private final ModelContext modelContext;
    private final NormalizedMapping normalized;

    /** Sidecar keyed by typed HIR node identity. */
    private final IdentityHashMap<TypedSpec, StoreResolution> resolutions = new IdentityHashMap<>();
    /** Self-join recursion guard, keyed by class FQN currently being resolved. */
    private final Set<String> resolving = new HashSet<>();
    /** Memoized class-level resolutions to short-circuit repeated lookups in one query. */
    private final Map<String, StoreResolution> classResolutionMemo = new LinkedHashMap<>();

    public MappingResolver(CompiledExpression typeResult, NormalizedMapping normalized,
                           ModelContext modelContext) {
        this.typeResult = typeResult;
        this.normalized = normalized;
        this.modelContext = modelContext;
    }

    /**
     * Resolves all mapping-dependent nodes in the typed HIR and returns a
     * {@link ResolvedExpression} pairing the input {@link CompiledExpression}
     * with its committed {@link ResolvedMappings} sidecar.
     *
     * <p>Today only populates {@code storeResolutions}; {@code navigations}
     * and {@code accessBindings} are left empty and populated in later
     * migration steps. The wrapper type enforces phase ordering:
     * PlanGenerator accepts a {@code ResolvedExpression}, not a bare
     * {@code CompiledExpression}.
     */
    public ResolvedExpression resolve() {
        walk(typeResult.hir(), null);
        return new ResolvedExpression(typeResult, ResolvedMappings.ofStoreResolutions(resolutions));
    }

    // ==================== Typed HIR walk ====================

    /**
     * Walks the typed HIR, propagating the active {@link StoreResolution} from
     * source operand to downstream operators. {@link TypedGetAll} and
     * {@link TypedNewInstance} create new resolutions; relation operators
     * inherit from their first operand; {@link TypedUserCall} recurses into
     * the callee's compiled body (shared primitive with synthesized sourceSpec
     * functions — user and synthesized are indistinguishable).
     */
    private void walk(TypedSpec node, StoreResolution active) {
        if (node == null) return;

        switch (node) {
            // ----- Anchors: create a new resolution -----
            case TypedGetAll ga -> {
                StoreResolution res = resolveClassFetch(ga.className());
                if (res != null) resolutions.put(node, res);
            }
            case TypedNewInstance ni -> {
                StoreResolution res = resolveIdentity(ni.className());
                if (res != null) {
                    resolutions.put(node, res);
                    active = res;
                }
                for (var v : ni.values().values()) walk(v, null);
            }

            // ----- Recursion: user call inlines through the callee's compiled body -----
            case TypedUserCall uc -> {
                TypedSpec body = uc.callee().body().hir();
                walk(body, active);
                StoreResolution bodyRes = resolutions.get(body);
                if (bodyRes != null) resolutions.put(node, bodyRes);
                for (var arg : uc.args()) walk(arg, null);
            }

            // ----- Relation source terminals -----
            case TypedTableReference ref -> {
                // Direct table reference (#>{db.TABLE} syntax) — bypasses
                // any class mapping; the schema lives on the node's typed
                // info as a {@link Type.Relation}. Build an identity store
                // keyed by the column names from that schema so downstream
                // property accesses (which reference physical column names
                // for table refs) resolve cleanly.
                if (!(ref.info().type() instanceof Type.Relation rel)) {
                    throw new IllegalStateException(
                            "[mapping-resolver] TypedTableReference info is not a Relation type: "
                                    + ref.info().type());
                }
                resolutions.put(node, buildSchemaStore(rel.schema().columns().keySet()));
            }
            case TypedTdsLiteral lit -> {
                // Inline TDS literal: identity store keyed by declared column
                // names. The literal IS its own physical schema — VALUES (...)
                // exposes columns by their declared names.
                List<String> cols = lit.data().columns().stream()
                        .map(com.gs.legend.ast.TdsLiteral.TdsColumn::name).toList();
                resolutions.put(node, buildSchemaStore(cols));
            }
            case TypedSourceUrl src -> {
                // External URL source: identity store keyed by the relation's
                // declared column names. For JSON-source classes the schema
                // is a single {@code data} VARIANT column; downstream extends
                // (synthesised by {@code MappingNormalizer.variantIdentity})
                // fan it into property columns via {@code get($row.data, '<prop>')}.
                if (!(src.info().type() instanceof Type.Relation rel)) {
                    throw new IllegalStateException(
                            "[mapping-resolver] TypedSourceUrl info is not a Relation type: "
                                    + src.info().type());
                }
                resolutions.put(node, buildSchemaStore(rel.schema().columns().keySet()));
            }

            // ----- Pass-through relation operators (no SQL rename) -----
            case TypedFilter op -> propagate(node, op.source(), List.of(op.predicate()), active);
            case TypedSort op -> propagate(node, op.source(), List.of(), active);

            // ----- Schema-changing relation operators -----
            // Each builds a NEW StoreResolution reflecting the output schema:
            // post-schema rows are TDSes whose columns are the operator's
            // declared output aliases. Inner expressions (project bodies,
            // group keys, agg lambdas, etc.) walk against the UPSTREAM store
            // because they reference the source's columns/properties.
            case TypedProject op -> {
                walk(op.source(), active);
                StoreResolution upstream = requireResolution(op.source(), active);
                for (var c : op.projections()) walk(c.expression(), upstream);
                List<String> aliases = op.projections().stream()
                        .map(TypedProjectionCol::alias).toList();
                resolutions.put(node, buildSchemaStore(aliases));
            }
            case TypedGroupBy op -> {
                walk(op.source(), active);
                StoreResolution upstream = requireResolution(op.source(), active);
                for (var agg : op.aggs()) {
                    walk(agg.fn1(), upstream);
                    walk(agg.fn2(), upstream);
                }
                List<String> aliases = new java.util.ArrayList<>();
                for (TypedGroupKey k : op.keys()) aliases.add(groupKeyAlias(k));
                for (TypedAggCall a : op.aggs()) aliases.add(a.alias());
                resolutions.put(node, buildSchemaStore(aliases));
            }
            case TypedAggregate op -> {
                walk(op.source(), active);
                StoreResolution upstream = requireResolution(op.source(), active);
                for (var agg : op.aggs()) {
                    walk(agg.fn1(), upstream);
                    walk(agg.fn2(), upstream);
                }
                List<String> aliases = op.aggs().stream()
                        .map(TypedAggCall::alias).toList();
                resolutions.put(node, buildSchemaStore(aliases));
            }
            case TypedPivot op -> {
                walk(op.source(), active);
                // Static schema = pivot grouping columns + agg aliases. The
                // pivot-spread columns (one per distinct pivot value × agg)
                // are dynamic and not represented statically; static Pure
                // code cannot reference them by name anyway.
                List<String> aliases = new java.util.ArrayList<>();
                aliases.addAll(op.pivotColumns());
                for (TypedAggCall a : op.aggs()) aliases.add(a.alias());
                resolutions.put(node, buildSchemaStore(aliases));
            }
            case TypedJoin op -> propagate(node, op.left(), List.of(op.right(), op.condition()), active);
            case TypedAsOfJoin op -> {
                walk(op.left(), active);
                walk(op.right(), active);
                walk(op.matchCondition(), active);
                StoreResolution activeFinal = active;
                op.keyCondition().ifPresent(k -> walk(k, activeFinal));
                inherit(node, op.left(), active);
            }
            case TypedExtend op -> {
                walk(op.source(), active);
                inherit(node, op.source(), active);
                // Extend columns are structural — typed variants (scalar,
                // window, traverse, association, embedded) are lowered to
                // StoreResolution fields by the caller of resolveClassFetch,
                // not by walking here. We still descend into scalar/window
                // expressions so downstream nodes inside them (user calls,
                // property accesses) get visited.
                StoreResolution ctx = resolutions.get(node);
                for (var col : op.extensions()) {
                    switch (col) {
                        case TypedScalarExtendCol s -> walk(s.expression(), ctx);
                        case TypedWindowExtendCol w -> {
                            for (var a : w.funcArgs()) walk(a, ctx);
                            w.reducer().ifPresent(l -> walk(l, ctx));
                            w.outerWrapper().ifPresent(ow -> walk(ow.expr(), ctx));
                        }
                        case TypedTraverseExtendCol t -> walk(t.expression(), ctx);
                        case TypedAssociationExtendCol ignored -> { /* no expression to walk */ }
                        case TypedEmbeddedExtendCol ignored -> { /* no expression to walk */ }
                    }
                }
            }
            case TypedSelect op -> {
                walk(op.source(), active);
                StoreResolution upstream = requireResolution(op.source(), active);
                resolutions.put(node, restrictStore(upstream, op.cols()));
            }
            case TypedRename op -> {
                walk(op.source(), active);
                StoreResolution upstream = requireResolution(op.source(), active);
                resolutions.put(node, renameStore(upstream, op.renames()));
            }
            case TypedSlice op -> propagate(node, op.source(), List.of(), active);
            case TypedDistinct op -> propagate(node, op.source(), List.of(), active);
            case TypedFlatten op -> propagate(node, op.source(), List.of(), active);
            case TypedConcatenate op -> {
                walk(op.left(), active);
                walk(op.right(), active);
                inherit(node, op.left(), active);
            }
            case TypedFrom op -> propagate(node, op.source(), List.of(), active);
            case TypedGraphFetch op -> propagate(node, op.source(), List.of(), active);

            // ----- Scalar operators + structural extract -----
            case TypedPropertyAccess pa -> {
                walk(pa.source(), active);
                // property access on a typed row is scalar; no resolution to produce
            }
            case TypedMap op -> {
                walk(op.source(), active);
                walk(op.mapper(), active);
            }
            case TypedFold op -> {
                walk(op.source(), active);
                walk(op.init(), active);
                walk(op.reducer(), active);
            }
            case TypedNativeCall nc -> {
                for (var a : nc.args()) walk(a, active);
            }
            case TypedStructExtract se -> walk(se.source(), active);
            case TypedEval ev -> {
                walk(ev.applicable(), active);
                for (var a : ev.args()) walk(a, active);
            }

            // ----- Control flow / IO / misc -----
            case TypedIf i -> {
                walk(i.condition(), active);
                walk(i.thenBranch(), active);
                walk(i.elseBranch(), active);
            }
            case TypedLet let -> walk(let.value(), active);
            case TypedBlock b -> {
                for (var s : b.stmts()) walk(s, active);
            }
            case TypedMatch m -> {
                walk(m.subject(), active);
                for (var arm : m.cases()) walk(arm, active);
            }
            case TypedCast c -> walk(c.expr(), active);
            case TypedZip z -> {
                for (var s : z.sources()) walk(s, active);
            }
            case TypedWrite w -> walk(w.source(), active);
            case TypedSerialize s -> walk(s.source(), active);

            // ----- Bindings / collections -----
            case TypedLambda lam -> {
                for (var stmt : lam.body()) walk(stmt, active);
            }
            case TypedCollection coll -> {
                for (var v : coll.values()) walk(v, active);
            }

            // ----- Terminals: literals, variables, element refs -----
            case TypedVariable ignored -> { }
            case TypedPackageableRef ignored -> { }
            case TypedCInteger ignored -> { }
            case TypedCFloat ignored -> { }
            case TypedCDecimal ignored -> { }
            case TypedCString ignored -> { }
            case TypedCBoolean ignored -> { }
            case TypedCDateTime ignored -> { }
            case TypedCStrictDate ignored -> { }
            case TypedCStrictTime ignored -> { }
            case TypedCLatestDate ignored -> { }
            case TypedCByteArray ignored -> { }
            case TypedEnumValue ignored -> { }
        }
    }

    /**
     * Walks {@code source}, propagates its resolution to {@code node}, then
     * walks {@code rest}. Used by relation operators that have one primary
     * source operand and N secondary operands (predicates, keys, etc.) that
     * inherit the same active resolution context.
     */
    private void propagate(TypedSpec node, TypedSpec source, List<TypedSpec> rest,
                           StoreResolution active) {
        walk(source, active);
        inherit(node, source, active);
        StoreResolution ctx = resolutions.get(node);
        for (var r : rest) walk(r, ctx);
    }

    /**
     * Inherits the active resolution from {@code source} (or the incoming
     * {@code active}) onto {@code node}. No-op if neither is present.
     */
    private void inherit(TypedSpec node, TypedSpec source, StoreResolution active) {
        StoreResolution fromSource = resolutions.get(source);
        StoreResolution chosen = fromSource != null ? fromSource : active;
        if (chosen != null) {
            resolutions.put(node, chosen);
        }
    }

    /**
     * Returns the upstream relation's stamped resolution, falling back to the
     * incoming {@code active} context when the source has no stamp. Used by
     * schema-changing operators that need to walk inner expressions against
     * the upstream schema. The {@code active} fallback handles cases where the
     * source is a node kind not yet stamping its resolution (e.g., a literal
     * or expression source not in the relation hierarchy).
     *
     * <p>TODO: Phase C.2 — once every relation node stamps a faithful
     * resolution, drop the {@code active} fallback and throw on null.
     */
    private StoreResolution requireResolution(TypedSpec source, StoreResolution active) {
        StoreResolution res = resolutions.get(source);
        return res != null ? res : active;
    }

    /**
     * Build an identity StoreResolution from a list of column aliases. Each
     * alias is its own physical column. No className, no tableName, no joins
     * — the row is a TDS, not a class instance.
     *
     * <p>Used by:
     * <ul>
     *   <li>TDS literal sources (the literal IS its own physical schema).</li>
     *   <li>Schema-changing operators (project, groupBy, aggregate, pivot)
     *       whose SQL output renames columns to the operator's declared
     *       aliases.</li>
     * </ul>
     */
    private static StoreResolution buildSchemaStore(java.util.Collection<String> aliases) {
        Map<String, String> propToCol = new LinkedHashMap<>();
        Map<String, StoreResolution.PropertyResolution> properties = new LinkedHashMap<>();
        for (String alias : aliases) {
            propToCol.put(alias, alias);
            properties.put(alias, new StoreResolution.PropertyResolution.Column(alias));
        }
        return new StoreResolution(null, null,
                java.util.Collections.unmodifiableMap(propToCol),
                java.util.Collections.unmodifiableMap(properties),
                Map.of(), false);
    }

    /**
     * Apply a list of column renames to an upstream store. Each renamed
     * property's entry is replaced: the new name becomes its own physical
     * column (the SQL emits {@code OLD AS NEW}, exposing the column under
     * the new name downstream). Other properties pass through unchanged.
     * Joins keyed by the renamed property are removed (an association can't
     * be renamed and remain navigable as a join in our model).
     */
    private static StoreResolution renameStore(StoreResolution upstream,
                                               List<com.gs.legend.compiler.typed.ColRename> renames) {
        Map<String, String> propToCol = new LinkedHashMap<>(upstream.propertyToColumn());
        Map<String, StoreResolution.PropertyResolution> properties =
                new LinkedHashMap<>(upstream.properties());
        Map<String, StoreResolution.JoinResolution> joins =
                new LinkedHashMap<>(upstream.joins());
        for (var r : renames) {
            propToCol.remove(r.from());
            properties.remove(r.from());
            joins.remove(r.from());
            propToCol.put(r.to(), r.to());
            properties.put(r.to(), new StoreResolution.PropertyResolution.Column(r.to()));
        }
        return new StoreResolution(
                upstream.tableName(), upstream.className(),
                java.util.Collections.unmodifiableMap(propToCol),
                java.util.Collections.unmodifiableMap(properties),
                java.util.Collections.unmodifiableMap(joins),
                upstream.nested(),
                upstream.extendOverride());
    }

    /**
     * Restrict an upstream store to a subset of columns ({@code select(...)}).
     * Properties / propToCol entries / joins not in the kept set are dropped.
     */
    private static StoreResolution restrictStore(StoreResolution upstream,
                                                 List<String> keptColumns) {
        Set<String> kept = Set.copyOf(keptColumns);
        Map<String, String> propToCol = new LinkedHashMap<>();
        Map<String, StoreResolution.PropertyResolution> properties = new LinkedHashMap<>();
        Map<String, StoreResolution.JoinResolution> joins = new LinkedHashMap<>();
        for (var e : upstream.propertyToColumn().entrySet()) {
            if (kept.contains(e.getKey())) propToCol.put(e.getKey(), e.getValue());
        }
        for (var e : upstream.properties().entrySet()) {
            if (kept.contains(e.getKey())) properties.put(e.getKey(), e.getValue());
        }
        for (var e : upstream.joins().entrySet()) {
            if (kept.contains(e.getKey())) joins.put(e.getKey(), e.getValue());
        }
        return new StoreResolution(
                upstream.tableName(), upstream.className(),
                java.util.Collections.unmodifiableMap(propToCol),
                java.util.Collections.unmodifiableMap(properties),
                java.util.Collections.unmodifiableMap(joins),
                upstream.nested(),
                upstream.extendOverride());
    }

    /** Extract the output alias for a typed group-by key. */
    private static String groupKeyAlias(TypedGroupKey k) {
        return switch (k) {
            case TypedColumnGroupKey c      -> c.alias();
            case TypedExpressionGroupKey e  -> e.alias();
            case TypedAssociationGroupKey a -> a.alias();
        };
    }

    // ==================== TypedGetAll / synthetic-function resolution ====================

    /**
     * The one recursion primitive: given a class FQN, resolve its
     * {@link StoreResolution} by walking the synthetic sourceSpec function's
     * compiled body. Memoized per class FQN.
     *
     * <p>Dispatch is by class — per the cross-project-joins principle the
     * caller never names a specific mapping's sourceSpec. Which mapping
     * materializes the class is decided by the active {@link NormalizedMapping}
     * (via {@code findSourceSpecFunctionFqn}).
     */
    private StoreResolution resolveClassFetch(String classFqn) {
        String fqn = canonicalize(classFqn);
        StoreResolution memo = classResolutionMemo.get(fqn);
        if (memo != null) return memo;
        if (resolving.contains(fqn)) return null; // self-join recursion guard

        ClassMapping mapping = normalized.findClassMapping(fqn).orElse(null);
        if (mapping == null) return null;

        resolving.add(fqn);
        StoreResolution result;
        try {
            result = switch (mapping) {
                case RelationalMapping rm -> resolveRelational(rm, fqn);
                case PureClassMapping pcm -> resolveM2M(pcm, fqn);
            };
        } finally {
            resolving.remove(fqn);
        }
        if (result != null) classResolutionMemo.put(fqn, result);
        return result;
    }

    private String canonicalize(String name) {
        return modelContext.findClass(name).map(PureClass::qualifiedName).orElse(name);
    }

    // ==================== ClassMapping → StoreResolution ====================

    /**
     * Materializes a relational class mapping as a {@link StoreResolution}.
     * Seeds from the mapping's property mappings, then walks the synthetic
     * sourceSpec function's typed body to layer extend overrides, join
     * resolutions, embedded columns, and traverse columns.
     */
    private StoreResolution resolveRelational(RelationalMapping rm, String classFqn) {
        String tableName = rm.table().name();
        Map<String, String> propToCol = new LinkedHashMap<>();
        Map<String, StoreResolution.PropertyResolution> properties = new LinkedHashMap<>();
        Map<String, StoreResolution.JoinResolution> joins = new LinkedHashMap<>();

        // 1. Seed: simple column PMs → Column(physicalCol);
        //    join-chain PMs → Column(propName) (traverse extend names them directly).
        for (PropertyMapping pm : rm.propertyMappings()) {
            String prop = pm.propertyName();
            String col = pm.hasJoinChain() ? pm.propertyName() : pm.columnName();
            propToCol.put(prop, col);
            properties.put(prop, new StoreResolution.PropertyResolution.Column(col));
        }

        // 2. Inline struct-array properties (UNNEST candidates) from the model.
        addStructArrayJoins(classFqn, joins);

        // 3. Build the store now; 'propToCol', 'properties' and 'joins' are
        //    the live mutable maps inside it — the single synth-body walk
        //    below layers extensions directly into them.
        var store = new StoreResolution(tableName, classFqn, propToCol, properties, joins,
                rm.nested());

        // 4. One walk over the synth sourceSpec body: layers extend-derived
        //    overrides (scalar → DynaFunction, traverse → column, association
        //    → JoinResolution, embedded → embedded) AND stamps every inner
        //    node so PlanGenerator has a resolution lookup for each TypedSpec
        //    it walks while rendering this class fetch.
        TypedSpec body = compiledMappingBody(classFqn);
        if (body != null) {
            walkSourceSpec(body, store, /* upstream */ null);
        }

        return store;
    }

    /**
     * Materializes an M2M class mapping by resolving its upstream class
     * (late-binding through the active {@link NormalizedMapping}) and
     * layering the PCM's property expressions — carried in the synthesized
     * sourceSpec function's {@code TypedExtend} nodes — on top of the
     * upstream resolution.
     *
     * <p>Key passthrough behavior: a target property whose expression is a
     * bare {@code $src.propName} access inherits the upstream resolution
     * verbatim — column passthrough stays column passthrough, association
     * JoinResolution stays association JoinResolution (with multiplicity
     * adjusted from the target property's declared multiplicity). This is
     * structurally detected on the typed HIR during extend-col lowering via
     * the {@code upstream} parameter to {@link #layerSourceSpecExtensions}.
     */
    private StoreResolution resolveM2M(PureClassMapping pcm, String targetClassFqn) {
        String upstreamClassFqn = canonicalize(pcm.sourceClassName());
        StoreResolution upstream = resolveClassFetch(upstreamClassFqn);
        if (upstream == null) {
            throw new PureCompileException(
                    "M2M mapping for '" + pcm.targetClassName()
                            + "' references source class '" + pcm.sourceClassName()
                            + "' which has no mapping in the active scope");
        }

        // Inherit upstream's propToCol / properties; typed-extend lowering
        // below replaces entries where the M2M declares a transform.
        Map<String, String> propToCol = new LinkedHashMap<>(upstream.propertyToColumn());
        Map<String, StoreResolution.PropertyResolution> properties =
                new LinkedHashMap<>(upstream.properties());
        Map<String, StoreResolution.JoinResolution> joins = new LinkedHashMap<>();

        var store = new StoreResolution(upstream.tableName(), targetClassFqn,
                propToCol, properties, joins, false);

        // One walk over the synth sourceSpec body: layers PCM-derived
        // property overrides (with passthrough-forwarding from upstream for
        // bare $src.prop bodies) AND stamps inner nodes so PlanGenerator can
        // render the M2M chain. The inner TypedGetAll on the upstream class
        // re-enters resolveClassFetch and hits the memoized upstream
        // resolution — equivalent to the old stampM2MSourceSpecStores
        // "innermost getAll gets sourceStore" behavior, produced here by
        // the generic walker instead of an ad-hoc chain pass.
        TypedSpec body = compiledMappingBody(targetClassFqn);
        if (body != null) {
            walkSourceSpec(body, store, upstream);
        }

        return store;
    }

    private StoreResolution.JoinResolution adjustJoinMultiplicity(
            StoreResolution.JoinResolution srcJoin, String targetClassFqn, String propName) {
        var targetClassOpt = modelContext.findClass(targetClassFqn);
        if (targetClassOpt.isEmpty()) return srcJoin;
        var propOpt = targetClassOpt.get().findProperty(propName, modelContext);
        if (propOpt.isEmpty()) return srcJoin;
        boolean isToMany = !propOpt.get().multiplicity().isSingular();
        if (isToMany == srcJoin.isToMany()) return srcJoin;
        return new StoreResolution.JoinResolution(
                srcJoin.targetTable(), srcJoin.sourceParam(), srcJoin.targetParam(),
                isToMany, srcJoin.joinCondition(), srcJoin.sourceColumns(),
                srcJoin.targetResolution(), srcJoin.embedded());
    }

    /**
     * Identity mapping for {@code ^Class(...)} struct literals and inline
     * struct arrays. Query-local — these are NOT registered in the scope's
     * synthetic functions; they are produced on the fly from the model.
     */
    private StoreResolution resolveIdentity(String classFqn) {
        PureClass pc = modelContext.findClass(classFqn).orElse(null);
        if (pc == null) return null;
        RelationalMapping identity = RelationalMapping.identity(pc, modelContext);
        // Seed from the identity PMs directly (don't recurse through the
        // synthetic-function channel — identity mappings are query-local).
        String tableName = identity.table().name();
        Map<String, String> propToCol = new LinkedHashMap<>();
        Map<String, StoreResolution.PropertyResolution> properties = new LinkedHashMap<>();
        for (PropertyMapping pm : identity.propertyMappings()) {
            String prop = pm.propertyName();
            String col = pm.columnName();
            propToCol.put(prop, col);
            properties.put(prop, new StoreResolution.PropertyResolution.Column(col));
        }
        Map<String, StoreResolution.JoinResolution> joins = new LinkedHashMap<>();
        addStructArrayJoins(classFqn, joins);
        return new StoreResolution(tableName, classFqn, propToCol, properties, joins,
                identity.nested());
    }

    // ==================== Synthetic sourceSpec body walking ====================

    /**
     * Retrieves the compiled typed body of the mapping function for the given
     * class from {@link com.gs.legend.compiled.CompiledDependencies#mappingFunctions},
     * or {@code null} if no mapping function was compiled for this class.
     *
     * <p>TypeChecker populates this map for every class reached from the root
     * fetch — including association targets via the pass-2 fan-out. If a class
     * is missing, it was not reachable during Pass 2; returning {@code null}
     * lets the caller decline to stamp a resolution rather than exploding.
     */
    private TypedSpec compiledMappingBody(String classFqn) {
        CompiledFunction cf = typeResult.dependencies().mappingFunctions().get(classFqn);
        return cf == null ? null : cf.body().hir();
    }

    /**
     * Walk B — synthetic sourceSpec body walker.
     *
     * <p>Invoked exclusively from {@link #resolveClassFetch}. Walks the
     * compiled body of a class's synthetic sourceSpec {@code PureFunction}
     * once, and in that single pass:
     * <ol>
     *   <li><b>Stamps</b> every visited {@link TypedSpec} node with
     *       {@code store}, so PlanGenerator has a resolution lookup for
     *       each node it walks while rendering the class fetch.</li>
     *   <li><b>Layers</b> extend-derived overrides into {@code store}'s
     *       mutable property / join maps when it encounters a
     *       {@link TypedExtend}. The store's maps are shared with every
     *       stamped node by reference, so later reads see the fully
     *       populated resolution.</li>
     *   <li><b>Applies</b> column-level override pruning on each
     *       {@link TypedExtend} via
     *       {@link #stampExtendOverrideIfNeeded}.</li>
     *   <li><b>Recurses</b> into inner {@link TypedGetAll}s (M2M chain)
     *       by calling {@link #resolveClassFetch} on the upstream class —
     *       memoized, so cycles / repeated references are cheap.</li>
     * </ol>
     *
     * <p>Contract: this walker only ever sees nodes that
     * {@link MappingNormalizer} synthesizes — {@code tableReference},
     * {@code getAll}, {@code filter}, {@code extend} (scalar / traverse /
     * association / embedded / window), plus inherited relation operators.
     * It does <em>not</em> descend into {@link TypedUserCall} —
     * MappingNormalizer never emits user calls in synthesized source specs
     * (mapping DSL is closed over structural relational primitives).
     *
     * <p>The generic {@link #walk} walker (Walk A) is the user-query walker
     * and never triggers layering — preserving the distinction between a
     * mapping-defined extend (sourceSpec-internal, layers into the class
     * store) and a query-defined extend (user's ad-hoc
     * {@code Person.all()->extend(...)}, which is a query-time projection
     * and must not fold into the class store).
     */
    private void walkSourceSpec(TypedSpec node, StoreResolution store,
                                StoreResolution upstream) {
        if (node == null) return;
        // Stamp upfront — relation operators, TypedTableReference, and
        // TypedExtend all take 'store' as their resolution. Inner TypedGetAll
        // overwrites this stamp with the upstream class's store after
        // recursive resolution.
        resolutions.put(node, store);

        switch (node) {
            case TypedExtend ext -> {
                String className = store.className();
                String tableName = store.tableName();
                Set<String> neededAssocs = typeResult.dependencies()
                        .associationNavigations()
                        .getOrDefault(className, Set.of());
                for (var col : ext.extensions()) {
                    lowerExtensionCol(col, className, tableName,
                            store.propertyToColumn(), store.properties(),
                            store.joins(), neededAssocs, upstream);
                }
                walkSourceSpec(ext.source(), store, upstream);
                // Override pruning lives in the same pass: its inputs
                // (extend aliases, query's used-prop set for this class)
                // are all at hand, and stamping here happens-after the
                // class-store stamp above — so it replaces that stamp
                // with the override-marker resolution when there's any
                // unused column. PlanGenerator detects the override via
                // StoreResolution.hasExtendOverride().
                stampExtendOverrideIfNeeded(ext, className);
            }
            case TypedGetAll innerGa -> {
                // M2M chain: inner fetch resolves to the upstream class's
                // own store. resolveClassFetch is memoized per class FQN,
                // so repeated / cyclic references are cheap and terminal.
                StoreResolution innerRes = resolveClassFetch(innerGa.className());
                if (innerRes != null) resolutions.put(innerGa, innerRes);
            }
            case TypedTableReference ignored -> { /* terminal */ }
            case TypedSourceUrl ignored -> { /* terminal — external URL source */ }
            default -> {
                TypedSpec src = relationSource(node);
                if (src != null) walkSourceSpec(src, store, upstream);
            }
        }
    }

    /**
     * Returns the relation-source operand of a relational operator, or
     * {@code null} if {@code node} is terminal (table reference, getAll) or
     * not a relation operator.
     */
    private TypedSpec relationSource(TypedSpec node) {
        return switch (node) {
            case TypedFilter f -> f.source();
            case TypedSelect s -> s.source();
            case TypedRename r -> r.source();
            case TypedSlice sl -> sl.source();
            case TypedDistinct d -> d.source();
            case TypedSort s -> s.source();
            case TypedGroupBy g -> g.source();
            case TypedProject p -> p.source();
            case TypedAggregate a -> a.source();
            case TypedPivot p -> p.source();
            case TypedFlatten fl -> fl.source();
            case TypedFrom fr -> fr.source();
            case TypedGraphFetch gf -> gf.source();
            default -> null;
        };
    }

    /**
     * Lowers one {@link TypedExtendCol} to its {@link StoreResolution}
     * contribution:
     * <ul>
     *   <li>{@link TypedScalarExtendCol} / window / traverse-scalar → override
     *       {@code propToCol} + {@code properties} with DynaFunction or Column.</li>
     *   <li>{@link TypedAssociationExtendCol} → build {@link StoreResolution.JoinResolution}
     *       for the associated class.</li>
     *   <li>{@link TypedEmbeddedExtendCol} → build embedded JoinResolution
     *       (sub-properties on the parent row, no physical JOIN).</li>
     * </ul>
     */
    private void lowerExtensionCol(
            TypedExtendCol col, String className, String tableName,
            Map<String, String> propToCol,
            Map<String, StoreResolution.PropertyResolution> properties,
            Map<String, StoreResolution.JoinResolution> joins,
            Set<String> neededAssocs,
            StoreResolution upstream) {
        switch (col) {
            case TypedScalarExtendCol s -> {
                TypedSpec bodyExpr = extractLambdaBody(s.expression());
                // M2M passthrough optimization: when the extend body is a bare
                // property access on the lambda's row parameter, forward the
                // upstream resolution verbatim — simple columns stay simple
                // columns, association JoinResolutions stay JoinResolutions.
                // Falling through to DynaFunction for a trivial passthrough
                // would force PlanGenerator to re-resolve the referenced
                // property via a computed-column path and would lose
                // association multiplicity / target-resolution wiring.
                if (upstream != null && forwardPassthrough(s, bodyExpr, className,
                        propToCol, properties, joins, upstream)) {
                    return;
                }
                // Per-row computed column. Override the seeded Column with a
                // DynaFunction carrying the typed lambda body.
                propToCol.put(s.alias(), s.alias());
                if (bodyExpr != null) {
                    properties.put(s.alias(),
                            new StoreResolution.PropertyResolution.DynaFunction(bodyExpr));
                }
            }
            case TypedWindowExtendCol w -> {
                // Window-computed column — treated as a DynaFunction over a
                // representative sub-expression. Downstream PlanGenerator
                // interprets window-ness from the original
                // {@link TypedWindowExtendCol} node, not here; this dyna-body
                // only exists so downstream property access can recurse into
                // the expression. Priority:
                //   1. outerWrapper (the whole scalar expression surrounding the window), if present;
                //   2. first funcArg (for aggregate / value windows it's the value expression);
                //   3. none — emit a bare column-alias property (ranking
                //      windows have no inner expression to introspect).
                propToCol.put(w.alias(), w.alias());
                TypedSpec bodyExpr = w.outerWrapper()
                        .map(ow -> ow.expr())
                        .orElse(w.funcArgs().isEmpty() ? null : w.funcArgs().get(0));
                if (bodyExpr != null) {
                    properties.put(w.alias(),
                            new StoreResolution.PropertyResolution.DynaFunction(bodyExpr));
                }
            }
            case TypedTraverseExtendCol t -> {
                // Per-column traverse (join-chain PM). The extend itself
                // produces a column alias on the terminal row; the physical
                // column is recoverable from the lambda body when it's a
                // simple property access, else treat as DynaFunction.
                TypedSpec bodyExpr = extractLambdaBody(t.expression());
                if (bodyExpr instanceof TypedPropertyAccess tpa) {
                    propToCol.put(t.alias(), tpa.property());
                    properties.put(t.alias(),
                            new StoreResolution.PropertyResolution.Column(tpa.property()));
                } else if (bodyExpr != null) {
                    propToCol.put(t.alias(), t.alias());
                    properties.put(t.alias(),
                            new StoreResolution.PropertyResolution.DynaFunction(bodyExpr));
                }
            }
            case TypedAssociationExtendCol a -> {
                if (!neededAssocs.contains(a.alias())) return;
                var joinRes = buildAssociationJoin(a, className);
                if (joinRes != null) joins.put(a.alias(), joinRes);
            }
            case TypedEmbeddedExtendCol e -> {
                joins.put(e.alias(), mergeOrBuildEmbeddedJoin(e, tableName, joins.get(e.alias())));
            }
        }
    }

    /**
     * Extracts the single-body expression of a {@link TypedLambda}, or
     * {@code null} if the lambda has no usable body.
     */
    private TypedSpec extractLambdaBody(TypedLambda lambda) {
        if (lambda == null || lambda.body().isEmpty()) return null;
        return lambda.body().get(0);
    }

    /**
     * Detects the M2M passthrough pattern — a scalar extend whose lambda body
     * is {@code $row.propName} where {@code $row} is the lambda's single
     * parameter — and forwards the upstream resolution for {@code propName}
     * to {@code col.alias()} in the target resolution.
     *
     * <p>Forwards:
     * <ul>
     *   <li>Upstream {@code joins.get(propName)} → target joins (with
     *       multiplicity re-derived from the target property's declaration).</li>
     *   <li>Upstream {@code properties.get(propName)} → target properties.</li>
     *   <li>Upstream {@code propertyToColumn.get(propName)} → target
     *       propertyToColumn under the new alias.</li>
     * </ul>
     *
     * @return {@code true} if passthrough was applied; {@code false} to fall
     *         back to DynaFunction.
     */
    private boolean forwardPassthrough(
            TypedScalarExtendCol col, TypedSpec bodyExpr, String targetClassFqn,
            Map<String, String> propToCol,
            Map<String, StoreResolution.PropertyResolution> properties,
            Map<String, StoreResolution.JoinResolution> joins,
            StoreResolution upstream) {
        if (!(bodyExpr instanceof TypedPropertyAccess tpa)) return false;
        if (!(tpa.source() instanceof TypedVariable rowVar)) return false;
        // The lambda's row parameter name — passthrough requires the property
        // access to target that exact row variable.
        if (col.expression().parameters().isEmpty()) return false;
        String rowParam = col.expression().parameters().get(0).name();
        if (!rowParam.equals(rowVar.name())) return false;

        String srcProp = tpa.property();
        String alias = col.alias();

        var upstreamJoin = upstream.joins().get(srcProp);
        if (upstreamJoin != null) {
            joins.put(alias, adjustJoinMultiplicity(upstreamJoin, targetClassFqn, alias));
            propToCol.put(alias, alias);
            // Remove any stale inherited simple-column entry for this alias.
            properties.remove(alias);
            return true;
        }

        var upstreamProp = upstream.properties().get(srcProp);
        if (upstreamProp != null) {
            properties.put(alias, upstreamProp);
        }
        String upstreamCol = upstream.propertyToColumn().get(srcProp);
        propToCol.put(alias, upstreamCol != null ? upstreamCol : srcProp);
        return true;
    }

    /**
     * Builds a {@link StoreResolution.JoinResolution} for an association
     * extend. Resolves the target class in the current scope; falls back to
     * a shallow resolution if the target is currently on the resolution stack
     * (self-join / cycle).
     */
    private StoreResolution.JoinResolution buildAssociationJoin(
            TypedAssociationExtendCol col, String ownerClassFqn) {
        var nav = modelContext.findAssociationByProperty(ownerClassFqn, col.alias()).orElse(null);
        if (nav == null) return null;

        String targetClassFqn = canonicalize(nav.targetClassName());
        ClassMapping targetMapping = normalized.findClassMapping(targetClassFqn)
                .orElseThrow(() -> new PureCompileException(
                        "Association '" + col.alias() + "' on '" + ownerClassFqn
                                + "' targets class '" + targetClassFqn
                                + "' which has no mapping in the active scope"));

        StoreResolution targetResolution = resolving.contains(targetClassFqn)
                ? shallowResolution(targetMapping, targetClassFqn)
                : resolveClassFetch(targetClassFqn);
        if (targetResolution == null) return null;

        String targetTable = targetMapping instanceof RelationalMapping trm ? trm.table().name() : null;
        if (col.hops().isEmpty()) return null;

        // Use the first hop's typed condition — for chained hops this is the
        // outermost predicate; the full chain is preserved in col.hops() for
        // PlanGenerator to render.
        var lastHop = col.hops().get(col.hops().size() - 1);
        TypedLambda cond = lastHop.condition();
        String sourceParam = cond.parameters().isEmpty() ? null : cond.parameters().get(0).name();
        String targetParam = cond.parameters().size() < 2 ? null : cond.parameters().get(1).name();

        TypedSpec condBody = extractLambdaBody(cond);
        Set<String> sourceCols = sourceParam == null
                ? Set.of() : extractSourceColumns(condBody, sourceParam);

        return new StoreResolution.JoinResolution(
                targetTable != null ? targetTable : lastHop.tableName(),
                sourceParam, targetParam,
                nav.isToMany(),
                condBody,
                sourceCols,
                targetResolution);
    }

    /**
     * Walks a typed join condition collecting column names referenced on the
     * source side — i.e., {@code $sourceParam.COLUMN} accesses. Used by
     * graphFetch to project only the source columns the correlated subquery
     * actually needs. Mirrors the old {@code extractSourceColumns} VS walk,
     * but dispatches on TypedSpec variants.
     */
    private Set<String> extractSourceColumns(TypedSpec expr, String sourceParam) {
        Set<String> out = new java.util.LinkedHashSet<>();
        collectSourceColumns(expr, sourceParam, out);
        return out;
    }

    private void collectSourceColumns(TypedSpec node, String sourceParam, Set<String> out) {
        if (node == null) return;
        if (node instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable v
                && sourceParam.equals(v.name())) {
            out.add(pa.property());
            return;
        }
        // Descend into children of expressions that can nest property accesses.
        switch (node) {
            case TypedNativeCall nc -> {
                for (var a : nc.args()) collectSourceColumns(a, sourceParam, out);
            }
            case TypedPropertyAccess pa -> collectSourceColumns(pa.source(), sourceParam, out);
            case TypedIf i -> {
                collectSourceColumns(i.condition(), sourceParam, out);
                collectSourceColumns(i.thenBranch(), sourceParam, out);
                collectSourceColumns(i.elseBranch(), sourceParam, out);
            }
            case TypedCast c -> collectSourceColumns(c.expr(), sourceParam, out);
            case TypedCollection coll -> {
                for (var v : coll.values()) collectSourceColumns(v, sourceParam, out);
            }
            case TypedLambda lam -> {
                for (var s : lam.body()) collectSourceColumns(s, sourceParam, out);
            }
            case TypedEval ev -> {
                collectSourceColumns(ev.applicable(), sourceParam, out);
                for (var a : ev.args()) collectSourceColumns(a, sourceParam, out);
            }
            case TypedUserCall uc -> {
                for (var a : uc.args()) collectSourceColumns(a, sourceParam, out);
            }
            default -> { /* leaf / irrelevant */ }
        }
    }

    /**
     * Builds an embedded {@link StoreResolution.JoinResolution}, merging
     * sub-columns into an existing association JoinResolution for the same
     * property if present.
     *
     * <p>Two scenarios:
     * <ul>
     *   <li><b>No existing join for this alias</b> — create a fresh embedded
     *       JoinResolution: sub-properties live on the parent's row (no
     *       physical JOIN), so the target resolution uses the parent table
     *       and EmbeddedColumn entries.</li>
     *   <li><b>Association already produced a JoinResolution for this alias</b>
     *       (e.g., the class declares both an association <em>and</em> an
     *       embedded column group for the same property name) — layer the
     *       embedded sub-columns into the association's target resolution
     *       and keep the physical JOIN. Matches the old
     *       {@code resolveEmbeddedExtend} merge behavior.</li>
     * </ul>
     */
    private StoreResolution.JoinResolution mergeOrBuildEmbeddedJoin(
            TypedEmbeddedExtendCol col, String parentTable,
            StoreResolution.JoinResolution existing) {
        Map<String, String> subPropToCol = new LinkedHashMap<>();
        Map<String, StoreResolution.PropertyResolution> subProperties = new LinkedHashMap<>();
        for (var sub : col.subColumns()) {
            subPropToCol.put(sub.propertyName(), sub.columnName());
            subProperties.put(sub.propertyName(),
                    new StoreResolution.PropertyResolution.EmbeddedColumn(sub.columnName()));
        }

        if (existing != null && !existing.embedded()) {
            // Association + embedded collision: fold sub-cols into the
            // association's target resolution, keeping the physical JOIN.
            var assocTarget = existing.targetResolution();
            var mergedPropToCol = new LinkedHashMap<>(assocTarget.propertyToColumn());
            mergedPropToCol.putAll(subPropToCol);
            var mergedProperties = new LinkedHashMap<>(assocTarget.properties());
            mergedProperties.putAll(subProperties);
            var mergedTarget = new StoreResolution(
                    assocTarget.tableName(), assocTarget.className(),
                    mergedPropToCol, mergedProperties,
                    assocTarget.joins(), assocTarget.nested());
            return new StoreResolution.JoinResolution(
                    existing.targetTable(), existing.sourceParam(),
                    existing.targetParam(), existing.isToMany(),
                    existing.joinCondition(), existing.sourceColumns(),
                    mergedTarget, /* embedded */ false);
        }

        StoreResolution embedded = new StoreResolution(
                parentTable, null, subPropToCol, subProperties, Map.of(), false);
        return new StoreResolution.JoinResolution(
                null, null, null, false, null, Set.of(), embedded, true);
    }

    /**
     * Shallow resolution for self-join / cycle targets: tableName +
     * simple-column property mappings only, no recursive association joins.
     */
    private StoreResolution shallowResolution(ClassMapping mapping, String classFqn) {
        if (!(mapping instanceof RelationalMapping rm)) {
            throw new PureCompileException(
                    "Self-join / back-reference target must be relational, got: "
                            + mapping.getClass().getSimpleName());
        }
        String tableName = rm.table().name();
        Map<String, String> propToCol = new LinkedHashMap<>();
        Map<String, StoreResolution.PropertyResolution> properties = new LinkedHashMap<>();
        for (PropertyMapping pm : rm.propertyMappings()) {
            String prop = pm.propertyName();
            String col = pm.hasJoinChain() ? pm.propertyName() : pm.columnName();
            propToCol.put(prop, col);
            properties.put(prop, new StoreResolution.PropertyResolution.Column(col));
        }
        return new StoreResolution(tableName, classFqn, propToCol, properties,
                Map.of(), rm.nested());
    }

    /**
     * Adds inline struct-array properties (UNNEST candidates) from the model.
     * For every class property whose type is a user class and multiplicity is
     * [*], add an identity-mapped JoinResolution so graphFetch can project
     * sub-rows without requiring a mapping entry.
     */
    private void addStructArrayJoins(String className,
                                     Map<String, StoreResolution.JoinResolution> joins) {
        var pureClass = modelContext.findClass(className).orElse(null);
        if (pureClass == null) return;
        for (var prop : pureClass.properties()) {
            if (joins.containsKey(prop.name())) continue;
            if (prop.multiplicity().isSingular()) continue;
            if (!(prop.type() instanceof Type.ClassType targetRef)) continue;
            String targetFqn = targetRef.qualifiedName();
            if (modelContext.findClass(targetFqn).isEmpty()) continue;
            StoreResolution targetResolution = resolveIdentity(targetFqn);
            if (targetResolution == null) continue;
            joins.put(prop.name(), new StoreResolution.JoinResolution(
                    null, null, null, true, null, Set.of(), targetResolution));
        }
    }

    // ==================== Extend Override ====================

    /**
     * Stamps a {@link StoreResolution.ExtendOverride} on {@code ext} when its
     * projected column aliases are not all consumed by the query. Used-props
     * come from {@code typeResult.dependencies().classPropertyAccesses()} —
     * populated by TypeChecker while compiling the user query.
     *
     * <p>Called inline from {@link #walkSynthNode} so override computation
     * happens in the same pass as class-store stamping and extension
     * layering. When an override applies, the forExtend resolution
     * overwrites the class-store stamp previously placed on {@code ext};
     * PlanGenerator sees the override via {@code hasExtendOverride()} and
     * skips rendering un-touched computed columns.
     *
     * <p>Association and embedded extend cols contribute no aliases here —
     * they are lowered to {@link StoreResolution.JoinResolution}s and are
     * gated independently by graphFetch / association navigation.
     */
    /**
     * Stamp an {@link StoreResolution.ExtendOverride} on this extend node
     * when any of its extension columns is unused by the query — so
     * {@code ExtendLowering} can skip the cancelled cols (and, for
     * {@link TypedAssociationExtendCol} / {@link TypedEmbeddedExtendCol},
     * their corresponding traversal joins).
     *
     * <p>Two pools of "used" aliases:
     * <ul>
     *   <li><b>Property accesses</b>: scalar/window/traverse extends whose
     *       alias is read as {@code $row.alias} downstream.</li>
     *   <li><b>Association navigations</b>: association/embedded extends
     *       whose alias is navigated as {@code $row.alias.something}
     *       downstream.</li>
     * </ul>
     *
     * <p>Critical for synthetic-body extends from
     * {@code MappingNormalizer.addAssociationExtends}: each such extend
     * carries exactly one association col plus its traversal hops, and
     * unconditionally emitting all of them produces spurious LEFT JOINs
     * in the SQL even when the query never navigates the association.
     * Marking unused association extends as fully cancelled prunes the
     * JOIN at lowering time.
     */
    private void stampExtendOverrideIfNeeded(TypedExtend ext, String className) {
        Set<String> propAliases = new HashSet<>();
        Set<String> assocAliases = new HashSet<>();
        for (var col : ext.extensions()) {
            switch (col) {
                case TypedScalarExtendCol s -> propAliases.add(s.alias());
                case TypedWindowExtendCol w -> propAliases.add(w.alias());
                case TypedTraverseExtendCol t -> propAliases.add(t.alias());
                case TypedAssociationExtendCol a -> assocAliases.add(a.alias());
                case TypedEmbeddedExtendCol e -> assocAliases.add(e.alias());
            }
        }
        int total = propAliases.size() + assocAliases.size();
        if (total == 0) return;

        Set<String> usedProps = typeResult.dependencies().classPropertyAccesses()
                .getOrDefault(className, Set.of());

        // Synth-body association/embedded extends are always cancellable: every
        // downstream consumer that genuinely needs an association traversal
        // installs its own LEFT JOIN on demand via NavScope (project) or its
        // own EXISTS (filter SemiJoin). The eager extend's LEFT JOIN is purely
        // redundant — and worse, for to-many associations it inflates rows
        // before the SemiJoin can de-duplicate. So we don't pair assocAliases
        // with associationNavigations here; only propAliases count toward the
        // active set. (Empty active = isFullyCancelled() in ExtendLowering.)
        Set<String> active = new HashSet<>();
        for (String a : propAliases)  if (usedProps.contains(a))    active.add(a);
        if (active.size() == total) return; // all used — no override needed

        resolutions.put(ext, StoreResolution.forExtend(
                new StoreResolution.ExtendOverride(active)));
    }
}
