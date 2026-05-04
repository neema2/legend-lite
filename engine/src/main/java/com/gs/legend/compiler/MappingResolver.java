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
 * Pass 4: Resolves mappings to physical store concepts over the typed HIR.
 *
 * <p>Runs AFTER {@link UserCallInliner} (Pass 3), BEFORE PlanGenerator (Pass 5).
 * Consumes {@link CompiledExpression} (typed HIR + compiled mapping functions
 * exposed via {@link com.gs.legend.compiled.CompiledDependencies#mappingFunctions})
 * and {@link NormalizedMapping} (mapping-layer metadata). Holds no
 * {@link TypeChecker} reference — everything needed has already been compiled
 * and stashed in {@code CompiledDependencies} by the time Pass 4 runs.
 *
 * <p><b>Precondition: input HIR must contain no {@link TypedUserCall}.</b>
 * Inlining is the responsibility of {@link UserCallInliner}, which runs as
 * its own pipeline stage immediately before this one. We assert this with a
 * defensive {@code IllegalStateException} on any {@code TypedUserCall} the
 * walker encounters.
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
 * PureParser → MappingNormalizer → TypeChecker → UserCallInliner → MappingResolver → PlanGenerator
 *  (Pass 1)      (Pass 1.5)         (Pass 2)       (Pass 3)           (Pass 4)          (Pass 5)
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
        // 1. Inline class-fetch synth bodies. Each TypedGetAll in the user
        //    HIR is replaced by its compiled mapping function body. Side
        //    effect: each splice triggers resolveClassFetch(fqn), which
        //    builds the class StoreResolution via Walk B and stamps the
        //    ORIGINAL synth-body nodes in the sidecar. After this pass no
        //    TypedGetAll survives. The kernel rebuilds compound parents
        //    whose children got rewritten (M2M chain); rebuilt parents
        //    lose their identity-keyed sidecar entry. LoweringContext.storeFor
        //    falls back to source recursion to recover stamps from the
        //    nearest still-stamped descendant.
        TypedSpec rewrittenHir = inlineClassFetches(typeResult.hir());

        // 2. Walk A — stamp user-query operators above (and around) the
        //    spliced subtrees. Pre-stamp short-circuit at resolveQuery's
        //    head makes Walk A idempotent over already-stamped Walk B nodes.
        StoreResolution rootStore = resolveQuery(rewrittenHir);

        // 3. Phase 2 — populate TypedPropertyAccess.physicalColumn for direct
        //    accesses ($var.prop with no associationPath). After this,
        //    PropertyAccessLowering reads the field from the AST instead
        //    of recomputing via storeFor + columnFor for the common case.
        //    Path-bearing accesses are left untouched and still resolved
        //    via the lowering-side fallback until Phase 3 rewrites them
        //    into TypedJoin chains.
        TypedSpec hirWithPhysCols = new PropertyAccessPopulator(resolutions).populate(rewrittenHir);

        // 4. Elaborate implicit serialize over the rewritten HIR.
        CompiledExpression rewrittenUnit = hirWithPhysCols == typeResult.hir()
                ? typeResult
                : new CompiledExpression(hirWithPhysCols, typeResult.dependencies());
        return new ResolvedExpression(
                elaborateImplicitSerialize(rewrittenUnit, rootStore),
                ResolvedMappings.ofStoreResolutions(resolutions));
    }

    /**
     * Pass 4a — class-fetch inliner. Rewrites the user HIR to splice each
     * {@link TypedGetAll}'s class with its compiled mapping function body.
     * After this pass no {@link TypedGetAll} survives in the rewritten tree.
     *
     * <p>Inner TypedGetAlls (M2M chain) are recursively expanded by threading
     * the kernel's recursion: the rewriter walks the spliced body, and any
     * inner TypedGetAll re-fires this hook.
     *
     * <p><strong>Sidecar transitional behavior</strong>: triggering
     * {@link #resolveClassFetch} for each FQN runs Walk B over the original
     * synth body, populating the sidecar with stamps on the ORIGINAL nodes.
     * After splicing, leaf nodes (TypedTableReference, TypedSourceUrl) keep
     * those stamps because the kernel doesn't rebuild leaves. Compound
     * parent nodes whose children changed are rebuilt; their stamps are
     * lost. LoweringContext.storeFor's source-recursion fallback recovers
     * the right store by walking down to the still-stamped descendant.
     */
    private TypedSpec inlineClassFetches(TypedSpec root) {
        var rewriter = new com.gs.legend.compiler.HirRewriter(
                new com.gs.legend.compiler.HirRewriter.Hooks() {
                    @Override
                    public TypedSpec rewriteGetAll(
                            TypedGetAll ga,
                            com.gs.legend.compiler.HirRewriter.Scope scope,
                            com.gs.legend.compiler.HirRewriter rec) {
                        // Trigger Walk B for this class to populate the
                        // sidecar with original-node stamps.
                        StoreResolution outerStore = resolveClassFetch(ga.className());

                        TypedSpec body = compiledMappingBody(ga.className());
                        if (body == null) return ga;

                        // Recurse so any inner TypedGetAll (M2M chain) is
                        // also expanded. The kernel rebuilds compound
                        // parents whose children got rewritten — those
                        // rebuilds lose their identity-keyed sidecar entry.
                        TypedSpec spliced = rec.rewrite(body, scope);

                        // Re-stamp rebuilt parents in the spliced subtree.
                        // Walk top-down, stamping each unstamped relational
                        // node with this class's store. Stops at nodes that
                        // are already stamped — those are either still-
                        // identity-preserved originals or boundaries to an
                        // inner splice (which has its own class store).
                        if (outerStore != null) restampSubtree(spliced, outerStore);

                        return spliced;
                    }
                });
        return rewriter.rewrite(root, com.gs.legend.compiler.HirRewriter.Scope.EMPTY);
    }

    /**
     * Top-down stamping pass over a spliced synth-body subtree. Stamps each
     * unstamped relational node with {@code store}; halts recursion at any
     * node that already has a stamp (either an original leaf preserved by
     * the kernel's identity-preserving rewrite, or the root of an inner
     * splice that owns a different class store).
     *
     * <p>Only relational children are followed — scalar lambdas don't need
     * stamps for storeFor to work; the lambda's relational source carries
     * the store via {@code env}'s Rel binding.
     */
    private void restampSubtree(TypedSpec node, StoreResolution store) {
        if (node == null) return;
        if (resolutions.containsKey(node)) return;
        resolutions.put(node, store);
        switch (node) {
            case com.gs.legend.compiler.typed.TypedFilter n -> restampSubtree(n.source(), store);
            case TypedExtend n -> restampSubtree(n.source(), store);
            case com.gs.legend.compiler.typed.TypedProject n -> restampSubtree(n.source(), store);
            case com.gs.legend.compiler.typed.TypedSort n -> restampSubtree(n.source(), store);
            case com.gs.legend.compiler.typed.TypedSlice n -> restampSubtree(n.source(), store);
            case com.gs.legend.compiler.typed.TypedDistinct n -> restampSubtree(n.source(), store);
            case com.gs.legend.compiler.typed.TypedFlatten n -> restampSubtree(n.source(), store);
            default -> { /* leaf or non-relational; nothing to recurse into */ }
        }
    }

    /**
     * Wraps a bare {@code Class[*]} root in {@link TypedSerializeImplicit}
     * so legend-lite's "DB assembles JSON" semantics route through the same
     * envelope mechanism as explicit {@code ->serialize()}. {@link TypedGraphFetch}
     * is excluded — it self-envelopes via {@code GraphFetchLowering}.
     */
    private CompiledExpression elaborateImplicitSerialize(CompiledExpression unit, StoreResolution rootStore) {
        TypedSpec root = unit.hir();
        if (root instanceof TypedGraphFetch) return unit;
        if (!(root.type() instanceof com.gs.legend.model.m3.Type.ClassType)) return unit;
        List<TypedGraphTree> tree = rootStore == null ? List.of()
                : rootStore.propertyToColumn().keySet().stream().map(TypedGraphTree::leaf).toList();
        return new CompiledExpression(new TypedSerializeImplicit(root, tree), unit.dependencies());
    }

    // ==================== Query side: typed HIR walk ====================

    /**
     * Walks the typed HIR returning each node's {@link StoreResolution}.
     * {@link TypedGetAll} and {@link TypedNewInstance} create new resolutions;
     * relation operators inherit from their first operand; {@link TypedUserCall}
     * is forbidden — it must be inlined upstream by {@code UserCallInliner}.
     *
     * <p>The wrapper unconditionally stamps {@code resolutions[node]} when the
     * computed resolution is non-null. Each switch case yields the node's
     * resolution; children are walked recursively and their resolutions
     * propagate via the recursive return value.
     */
    private StoreResolution resolveQuery(TypedSpec node) {
        if (node == null) return null;
        // Short-circuit: synth-body nodes are pre-stamped by Walk B during
        // class-fetch inlining. Re-walking them here would compute different
        // (schema-store) stamps for nodes that should keep their class-store
        // stamps. The pre-stamp wins.
        StoreResolution preStamped = resolutions.get(node);
        if (preStamped != null) return preStamped;
        StoreResolution res = switch (node) {
            // ----- Anchors: create a new resolution -----
            case TypedGetAll ga -> throw new IllegalStateException(
                    "[mapping-resolver] TypedGetAll reached resolveQuery — "
                            + "inlineClassFetches should have spliced it; fqn="
                            + ga.className());
            case TypedNewInstance ni -> {
                StoreResolution r = resolveIdentity(ni.className());
                for (var v : ni.values().values()) resolveQuery(v);
                yield r;
            }

            // ----- TypedUserCall must not survive UserCallInliner -----
            case TypedUserCall uc -> throw new IllegalStateException(
                    "[mapping-resolver] TypedUserCall reached resolution walk — "
                            + "UserCallInliner should have inlined it; fqn=" + uc.functionFqn());

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
                yield buildSchemaStore(rel.schema().columns().keySet());
            }
            case TypedTdsLiteral lit -> {
                // Inline TDS literal: identity store keyed by declared column
                // names. The literal IS its own physical schema — VALUES (...)
                // exposes columns by their declared names.
                List<String> cols = lit.data().columns().stream()
                        .map(com.gs.legend.ast.TdsLiteral.TdsColumn::name).toList();
                yield buildSchemaStore(cols);
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
                yield buildSchemaStore(rel.schema().columns().keySet());
            }

            // ----- Pass-through relation operators (inherit source's resolution) -----
            case TypedFilter op -> {
                StoreResolution src = resolveQuery(op.source());
                resolveQuery(op.predicate());
                yield src;
            }
            case TypedSort op -> resolveQuery(op.source());
            case TypedSlice op -> resolveQuery(op.source());
            case TypedDistinct op -> resolveQuery(op.source());
            case TypedFlatten op -> resolveQuery(op.source());
            case TypedFrom op -> resolveQuery(op.source());
            case TypedGraphFetch op -> resolveQuery(op.source());

            // ----- Schema-changing relation operators -----
            // Each builds a NEW StoreResolution reflecting the output schema:
            // post-schema rows are TDSes whose columns are the operator's
            // declared output aliases. Inner expressions are walked against
            // the upstream's stamp via the recursive return value.
            case TypedProject op -> {
                resolveQuery(op.source());
                for (var c : op.projections()) resolveQuery(c.expression());
                List<String> aliases = op.projections().stream()
                        .map(TypedProjectionCol::alias).toList();
                yield buildSchemaStore(aliases);
            }
            case TypedGroupBy op -> {
                resolveQuery(op.source());
                for (var agg : op.aggs()) {
                    resolveQuery(agg.fn1());
                    resolveQuery(agg.fn2());
                }
                List<String> aliases = new java.util.ArrayList<>();
                for (TypedGroupKey k : op.keys()) aliases.add(groupKeyAlias(k));
                for (TypedAggCall a : op.aggs()) aliases.add(a.alias());
                yield buildSchemaStore(aliases);
            }
            case TypedAggregate op -> {
                resolveQuery(op.source());
                for (var agg : op.aggs()) {
                    resolveQuery(agg.fn1());
                    resolveQuery(agg.fn2());
                }
                List<String> aliases = op.aggs().stream()
                        .map(TypedAggCall::alias).toList();
                yield buildSchemaStore(aliases);
            }
            case TypedPivot op -> {
                resolveQuery(op.source());
                // Static schema = pivot grouping columns + agg aliases. The
                // pivot-spread columns (one per distinct pivot value × agg)
                // are dynamic and not represented statically; static Pure
                // code cannot reference them by name anyway.
                List<String> aliases = new java.util.ArrayList<>();
                aliases.addAll(op.pivotColumns());
                for (TypedAggCall a : op.aggs()) aliases.add(a.alias());
                yield buildSchemaStore(aliases);
            }
            case TypedJoin op -> {
                StoreResolution leftRes = resolveQuery(op.left());
                resolveQuery(op.right());
                resolveQuery(op.condition());
                yield leftRes;
            }
            case TypedAsOfJoin op -> {
                // Inherits left side's resolution (matches TypedJoin's
                // documented semantic). Previously walked siblings with the
                // incoming {@code active} regardless — accidental hand-roll.
                StoreResolution leftRes = resolveQuery(op.left());
                resolveQuery(op.right());
                resolveQuery(op.matchCondition());
                op.keyCondition().ifPresent(MappingResolver.this::resolveQuery);
                yield leftRes;
            }
            case TypedExtend op -> {
                StoreResolution src = resolveQuery(op.source());
                // Extend columns are structural — typed variants (scalar,
                // window, traverse, association, embedded) are lowered to
                // StoreResolution fields by the caller of resolveClassFetch,
                // not by walking here. We still descend into scalar/window
                // expressions so downstream nodes inside them (property
                // accesses) get visited.
                for (var col : op.extensions()) {
                    switch (col) {
                        case TypedScalarExtendCol s -> resolveQuery(s.expression());
                        case TypedWindowExtendCol w -> {
                            for (var a : w.funcArgs()) resolveQuery(a);
                            w.reducer().ifPresent(MappingResolver.this::resolveQuery);
                            w.outerWrapper().ifPresent(ow -> resolveQuery(ow.expr()));
                        }
                        case TypedTraverseExtendCol t -> resolveQuery(t.expression());
                        case TypedAssociationExtendCol ignored -> { /* no expression to walk */ }
                        case TypedEmbeddedExtendCol ignored -> { /* no expression to walk */ }
                    }
                }
                yield src;
            }
            case TypedSelect op -> {
                StoreResolution upstream = resolveQuery(op.source());
                yield upstream != null ? restrictStore(upstream, op.cols()) : null;
            }
            case TypedRename op -> {
                StoreResolution upstream = resolveQuery(op.source());
                yield upstream != null ? renameStore(upstream, op.renames()) : null;
            }
            case TypedConcatenate op -> {
                // Inherits left's resolution (matches TypedJoin's documented
                // semantic). Previously walked right with the incoming
                // {@code active} — accidental hand-roll.
                StoreResolution leftRes = resolveQuery(op.left());
                resolveQuery(op.right());
                yield leftRes;
            }

            // ----- Scalar operators + structural extract -----
            case TypedPropertyAccess pa -> {
                resolveQuery(pa.source());
                yield null;     // property access on a typed row is scalar
            }
            case TypedMap op -> {
                resolveQuery(op.source());
                resolveQuery(op.mapper());
                yield null;
            }
            case TypedFold op -> {
                resolveQuery(op.source());
                resolveQuery(op.init());
                resolveQuery(op.reducer());
                yield null;
            }
            case TypedNativeCall nc -> {
                for (var a : nc.args()) resolveQuery(a);
                yield null;
            }
            case TypedStructExtract se -> {
                resolveQuery(se.source());
                yield null;
            }
            case TypedEval ev -> {
                resolveQuery(ev.applicable());
                for (var a : ev.args()) resolveQuery(a);
                yield null;
            }

            // ----- Control flow / IO / misc -----
            case TypedIf i -> {
                resolveQuery(i.condition());
                StoreResolution thenRes = resolveQuery(i.thenBranch());
                StoreResolution elseRes = resolveQuery(i.elseBranch());
                // Inherit one branch's resolution onto the if-node so a
                // class-typed if expression presents as relational. Both
                // branches must produce the same class for the wrap to be
                // well-defined; we pick the then-branch arbitrarily and
                // assume type-checker uniformity. Without this stamp,
                // implicit-serialize elaboration would synthesize an empty
                // tree for {@code if(c, |P.all(), |Q.all())} roots.
                yield thenRes != null ? thenRes : elseRes;
            }
            case TypedLet let -> {
                resolveQuery(let.value());
                yield null;
            }
            case TypedBlock b -> {
                StoreResolution last = null;
                for (var s : b.stmts()) last = resolveQuery(s);
                // The block's result is its last statement's value, so its
                // store resolution is the last statement's. Without this,
                // an implicit-serialize root over a {@code let; expr} block
                // would resolve to no store and skip the JSON envelope.
                yield last;
            }
            case TypedMatch m -> {
                resolveQuery(m.subject());
                for (var arm : m.cases()) resolveQuery(arm);
                yield null;
            }
            case TypedCast c -> {
                resolveQuery(c.expr());
                yield null;
            }
            case TypedZip z -> {
                for (var s : z.sources()) resolveQuery(s);
                yield null;
            }
            case TypedWrite w -> {
                resolveQuery(w.source());
                yield null;
            }
            case TypedSerialize s -> {
                resolveQuery(s.source());
                yield null;
            }
            case TypedSerializeImplicit s -> resolveQuery(s.source());

            // ----- Bindings / collections -----
            case TypedLambda lam -> {
                StoreResolution last = null;
                for (var stmt : lam.body()) last = resolveQuery(stmt);
                // A 0-parameter lambda is the standard query-root shape for
                // multi-statement queries. Inherit the last body stmt's store
                // so the implicit-serialize elaboration can find a resolved
                // store on the root and synthesize a non-empty fetch tree.
                yield last;
            }
            case TypedCollection coll -> {
                // Class-typed collection (`[^Class(...), ^Class(...)]`) →
                // multi-row VALUES of that class. Stamp the identity store
                // so downstream sees flat property columns. Primitive /
                // enum collections (`[1,2,3]`, `[E.A, E.B]`) keep no store
                // and lower as scalar arrays; the instanceof is the
                // legitimate class-vs-non-class dispatch.
                StoreResolution r = (coll.info().type() instanceof Type.ClassType ct)
                        ? resolveIdentity(ct.qualifiedName()) : null;
                for (var v : coll.values()) resolveQuery(v);
                yield r;
            }

            // ----- Terminals: literals, variables, element refs -----
            case TypedVariable ignored -> null;
            case TypedPackageableRef ignored -> null;
            case TypedCInteger ignored -> null;
            case TypedCFloat ignored -> null;
            case TypedCDecimal ignored -> null;
            case TypedCString ignored -> null;
            case TypedCBoolean ignored -> null;
            case TypedCDateTime ignored -> null;
            case TypedCStrictDate ignored -> null;
            case TypedCStrictTime ignored -> null;
            case TypedCLatestDate ignored -> null;
            case TypedCByteArray ignored -> null;
            case TypedEnumValue ignored -> null;
        };
        if (res != null) resolutions.put(node, res);
        return res;
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
        for (String alias : aliases) {
            propToCol.put(alias, alias);
        }
        return new StoreResolution(null, null,
                java.util.Collections.unmodifiableMap(propToCol),
                Map.of());
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
        Map<String, StoreResolution.JoinResolution> joins =
                new LinkedHashMap<>(upstream.joins());
        for (var r : renames) {
            propToCol.remove(r.from());
            joins.remove(r.from());
            propToCol.put(r.to(), r.to());
        }
        return new StoreResolution(
                upstream.tableName(), upstream.className(),
                java.util.Collections.unmodifiableMap(propToCol),
                java.util.Collections.unmodifiableMap(joins),
                upstream.extendNodeCols());
    }

    /**
     * Restrict an upstream store to a subset of columns ({@code select(...)}).
     * Properties / propToCol entries / joins not in the kept set are dropped.
     */
    private static StoreResolution restrictStore(StoreResolution upstream,
                                                 List<String> keptColumns) {
        Set<String> kept = Set.copyOf(keptColumns);
        Map<String, String> propToCol = new LinkedHashMap<>();
        Map<String, StoreResolution.JoinResolution> joins = new LinkedHashMap<>();
        for (var e : upstream.propertyToColumn().entrySet()) {
            if (kept.contains(e.getKey())) propToCol.put(e.getKey(), e.getValue());
        }
        for (var e : upstream.joins().entrySet()) {
            if (kept.contains(e.getKey())) joins.put(e.getKey(), e.getValue());
        }
        return new StoreResolution(
                upstream.tableName(), upstream.className(),
                java.util.Collections.unmodifiableMap(propToCol),
                java.util.Collections.unmodifiableMap(joins),
                upstream.extendNodeCols());
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
        String tableName = rm.table().dbName();
        Map<String, String> propToCol = new LinkedHashMap<>();

        // 1. Seed: simple column PMs → physical column;
        //    join-chain PMs → propName (traverse extend names them directly).
        for (PropertyMapping pm : rm.propertyMappings()) {
            String prop = pm.propertyName();
            String col = pm.hasJoinChain() ? pm.propertyName() : pm.columnName();
            propToCol.put(prop, col);
        }

        // 2. Inline struct-array properties (UNNEST candidates) from the model.
        Map<String, StoreResolution.JoinResolution> joins = buildStructArrayJoins(classFqn);

        // 3. Build the store now; 'propToCol' and 'joins' are the live
        //    mutable maps inside it — the single synth-body walk below
        //    layers extensions directly into them.
        var store = new StoreResolution(tableName, classFqn, propToCol, joins);

        // 4. One walk over the synth sourceSpec body: layers extend-derived
        //    overrides (scalar → DynaFunction, traverse → column, association
        //    → JoinResolution, embedded → embedded) AND stamps every inner
        //    node so PlanGenerator has a resolution lookup for each TypedSpec
        //    it walks while rendering this class fetch.
        TypedSpec body = compiledMappingBody(classFqn);
        if (body != null) {
            resolveMappingFunction(body, store, /* upstream */ null);
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

        // Inherit upstream's propToCol; typed-extend lowering below replaces
        // entries where the M2M declares a transform.
        Map<String, String> propToCol = new LinkedHashMap<>(upstream.propertyToColumn());
        Map<String, StoreResolution.JoinResolution> joins = new LinkedHashMap<>();

        var store = new StoreResolution(upstream.tableName(), targetClassFqn,
                propToCol, joins);

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
            resolveMappingFunction(body, store, upstream);
        }

        return store;
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
        String tableName = identity.table().dbName();
        Map<String, String> propToCol = new LinkedHashMap<>();
        for (PropertyMapping pm : identity.propertyMappings()) {
            String prop = pm.propertyName();
            String col = pm.columnName();
            propToCol.put(prop, col);
        }
        Map<String, StoreResolution.JoinResolution> joins = buildStructArrayJoins(classFqn);
        return new StoreResolution(tableName, classFqn, propToCol, joins);
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
     *       {@link #pruneUnusedExtendCols}.</li>
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
    private void resolveMappingFunction(TypedSpec node, StoreResolution store,
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
                    var existing = store.joins().get(aliasOf(col));
                    var contrib = resolveExtensionCol(col, className, tableName,
                            neededAssocs, upstream, existing);
                    applyContribution(contrib, store.propertyToColumn(), store.joins());
                }
                resolveMappingFunction(ext.source(), store, upstream);
                // Pruning runs in the same pass: inputs (declared aliases +
                // query's used-prop set for this class) are at hand, and
                // running here happens-after the class-store stamp above.
                // Re-stamp only when the active set is a strict subset of
                // declared. PlanGenerator detects the result via
                // StoreResolution.extendNodeCols() != null.
                StoreResolution.ExtendNodeCols marker = pruneUnusedExtendCols(ext, className);
                if (marker != null) {
                    resolutions.put(ext, new StoreResolution(
                            store.tableName(), store.className(),
                            store.propertyToColumn(), store.joins(), marker));
                }
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
                if (src != null) resolveMappingFunction(src, store, upstream);
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
     * What a single typed extend column contributes to the running
     * {@link StoreResolution} maps. Caller folds the contribution into
     * {@code propToCol} and {@code joins} via {@link #applyContribution}.
     */
    private sealed interface ExtensionContribution {
        /** No-op (e.g., association filtered out by needed-assoc set). */
        record Skip() implements ExtensionContribution {}
        /** Adds {@code alias → columnName} to {@code propertyToColumn}. */
        record Column(String alias, String columnName) implements ExtensionContribution {}
        /** Adds {@code alias → join} to {@code joins}. */
        record Join(String alias, StoreResolution.JoinResolution join) implements ExtensionContribution {}
        /** Adds both a column entry and a join entry under {@code alias} (M2M passthrough of a class-typed property). */
        record Both(String alias, String columnName, StoreResolution.JoinResolution join) implements ExtensionContribution {}
    }

    private static final ExtensionContribution SKIP = new ExtensionContribution.Skip();

    /**
     * Folds an {@link ExtensionContribution} into the running maps.
     */
    private static void applyContribution(
            ExtensionContribution c,
            Map<String, String> propToCol,
            Map<String, StoreResolution.JoinResolution> joins) {
        switch (c) {
            case ExtensionContribution.Skip ignored -> { /* no-op */ }
            case ExtensionContribution.Column col -> propToCol.put(col.alias(), col.columnName());
            case ExtensionContribution.Join j -> joins.put(j.alias(), j.join());
            case ExtensionContribution.Both both -> {
                propToCol.put(both.alias(), both.columnName());
                joins.put(both.alias(), both.join());
            }
        }
    }

    /** Output alias of a typed extend column. */
    private static String aliasOf(TypedExtendCol col) {
        return switch (col) {
            case TypedScalarExtendCol s     -> s.alias();
            case TypedWindowExtendCol w     -> w.alias();
            case TypedTraverseExtendCol t   -> t.alias();
            case TypedAssociationExtendCol a -> a.alias();
            case TypedEmbeddedExtendCol e   -> e.alias();
        };
    }

    /**
     * Resolves one {@link TypedExtendCol} into an {@link ExtensionContribution}.
     * Pure: no mutation. Caller folds the result into the running maps via
     * {@link #applyContribution}.
     *
     * <p>Cases:
     * <ul>
     *   <li>{@link TypedScalarExtendCol} / window / traverse-scalar →
     *       {@link ExtensionContribution.Column} (alias → physical column).
     *       Scalar extends in M2M passthrough position may instead return
     *       {@link ExtensionContribution.Both} (forwarded upstream join).</li>
     *   <li>{@link TypedAssociationExtendCol} →
     *       {@link ExtensionContribution.Join} for the associated class,
     *       or {@link ExtensionContribution.Skip} if not needed.</li>
     *   <li>{@link TypedEmbeddedExtendCol} →
     *       {@link ExtensionContribution.Join} (embedded — no physical JOIN).</li>
     * </ul>
     *
     * @param existingJoin the join currently registered for this alias, if
     *        any — used by association/embedded cols to detect the
     *        {@link StoreResolution.JoinResolution.Otherwise} pattern (an
     *        embedded extend already populated an Embedded under this alias,
     *        and now an association extend wants to layer an FK fallback,
     *        or vice versa).
     */
    private ExtensionContribution resolveExtensionCol(
            TypedExtendCol col, String className, String tableName,
            Set<String> neededAssocs,
            StoreResolution upstream,
            StoreResolution.JoinResolution existingJoin) {
        return switch (col) {
            case TypedScalarExtendCol s -> {
                TypedSpec bodyExpr = extractLambdaBody(s.expression());
                // M2M passthrough optimization: when the extend body is a bare
                // property access on the lambda's row parameter, forward the
                // upstream resolution verbatim — simple columns stay simple
                // columns, association JoinResolutions stay JoinResolutions.
                // Falling through to a plain alias entry for a trivial passthrough
                // would lose association multiplicity / target-resolution wiring.
                if (upstream != null) {
                    ExtensionContribution pass = forwardPassthrough(s, bodyExpr, className, upstream);
                    if (pass != null) yield pass;
                }
                // Relational rename extend: body is $row.<COL> on the lambda's
                // row var, no upstream — emitted by MappingNormalizer's
                // {@code addDynaFunctionExtends} for simple-column PMs so the
                // synth body's terminal Relation schema is fully PM-keyed
                // (logical-named). The contribution preserves the {@code
                // logicalProp → physicalCol} mapping seeded from the PM:
                // {@code Column(firstName, FIRST_NAME)}, NOT the alias-to-alias
                // form which would shadow the seed and break PlanGenerator
                // paths that don't apply the extend (e.g. usage analysis
                // pruning the entire extend node when the query reads only
                // a subset of properties).
                ExtensionContribution rename = forwardRelationalRename(s, bodyExpr);
                if (rename != null) yield rename;
                // Per-row computed column — alias entry; PlanGenerator
                // re-derives the expression from the typed extend node.
                yield new ExtensionContribution.Column(s.alias(), s.alias());
            }
            case TypedWindowExtendCol w ->
                // Window-computed column — alias entry. Downstream
                // PlanGenerator interprets window-ness from the original
                // TypedWindowExtendCol node.
                new ExtensionContribution.Column(w.alias(), w.alias());
            case TypedTraverseExtendCol t -> {
                // Per-column traverse (join-chain PM). The physical column
                // is recoverable from the lambda body when it's a simple
                // property access; otherwise the alias names itself.
                TypedSpec bodyExpr = extractLambdaBody(t.expression());
                if (bodyExpr instanceof TypedPropertyAccess tpa) {
                    yield new ExtensionContribution.Column(t.alias(), tpa.property());
                } else if (bodyExpr != null) {
                    yield new ExtensionContribution.Column(t.alias(), t.alias());
                } else {
                    yield SKIP;
                }
            }
            case TypedAssociationExtendCol a -> {
                if (!neededAssocs.contains(a.alias())) yield SKIP;
                var joinRes = buildAssociationJoin(a, className);
                if (joinRes == null) yield SKIP;
                // Otherwise mapping: an embedded extend already populated
                // an Embedded JoinResolution under the same alias. Wrap the
                // FK fallback with the embedded sub-cols so PropertyAccessLowering
                // can dispatch at access time.
                if (existingJoin instanceof StoreResolution.JoinResolution.Embedded emb
                        && joinRes instanceof StoreResolution.JoinResolution.FkJoin fk) {
                    yield new ExtensionContribution.Join(a.alias(),
                            new StoreResolution.JoinResolution.Otherwise(embeddedSubColMap(emb), fk));
                }
                yield new ExtensionContribution.Join(a.alias(), joinRes);
            }
            case TypedEmbeddedExtendCol e ->
                new ExtensionContribution.Join(e.alias(), buildEmbeddedJoin(e, tableName, existingJoin));
        };
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
     *   <li>Upstream {@code propertyToColumn.get(propName)} → target
     *       propertyToColumn under the new alias.</li>
     * </ul>
     *
     * @return the contribution to apply (Both for forwarded join, Column
     *         for forwarded simple column), or {@code null} if the body
     *         is not a passthrough pattern (caller falls back to a plain
     *         alias Column entry).
     */
    private ExtensionContribution forwardPassthrough(
            TypedScalarExtendCol col, TypedSpec bodyExpr, String targetClassFqn,
            StoreResolution upstream) {
        if (!(bodyExpr instanceof TypedPropertyAccess tpa)) return null;
        if (!(tpa.source() instanceof TypedVariable rowVar)) return null;
        // The lambda's row parameter name — passthrough requires the property
        // access to target that exact row variable.
        if (col.expression().parameters().isEmpty()) return null;
        String rowParam = col.expression().parameters().get(0).name();
        if (!rowParam.equals(rowVar.name())) return null;

        String srcProp = tpa.property();
        String alias = col.alias();

        var upstreamJoin = upstream.joins().get(srcProp);
        if (upstreamJoin != null) {
            // Forward the upstream JoinResolution structurally. Pure typecheck
            // requires the target property's multiplicity to match upstream's
            // (bare $row.<prop> can only bind to a property of equal
            // multiplicity), so the upstream's isToMany is correct here.
            return new ExtensionContribution.Both(alias, alias, upstreamJoin);
        }

        String upstreamCol = upstream.propertyToColumn().get(srcProp);
        return new ExtensionContribution.Column(alias,
                upstreamCol != null ? upstreamCol : srcProp);
    }

    /**
     * Detects a relational rename extend — body is {@code $row.<COL>} on the
     * lambda's row var — and emits {@code Column(alias, COL)} so the seeded
     * {@code logicalProp → physicalCol} mapping is preserved at the
     * logical-prop key.
     *
     * <p>Distinct from {@link #forwardPassthrough}: that one runs only when
     * an upstream {@link StoreResolution} exists (M2M chain) and forwards
     * upstream join / propertyToColumn entries by their source property name.
     * This one runs for relational ({@code upstream == null}) and uses the
     * accessed property name directly as the physical column — there's no
     * upstream propertyToColumn to consult.
     *
     * @return {@code Column(alias, accessedCol)} if the body is a bare row-var
     *         property access, {@code null} otherwise (caller falls back to
     *         alias-to-alias).
     */
    private ExtensionContribution forwardRelationalRename(
            TypedScalarExtendCol col, TypedSpec bodyExpr) {
        if (!(bodyExpr instanceof TypedPropertyAccess tpa)) return null;
        if (!(tpa.source() instanceof TypedVariable rowVar)) return null;
        if (col.expression().parameters().isEmpty()) return null;
        String rowParam = col.expression().parameters().get(0).name();
        if (!rowParam.equals(rowVar.name())) return null;
        return new ExtensionContribution.Column(col.alias(), tpa.property());
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

        String targetTable = targetMapping instanceof RelationalMapping trm ? trm.table().dbName() : null;
        if (col.hops().isEmpty()) return null;

        // Use the first hop's typed condition — for chained hops this is the
        // outermost predicate; the full chain is preserved in col.hops() for
        // PlanGenerator to render.
        var lastHop = col.hops().get(col.hops().size() - 1);
        TypedLambda cond = lastHop.condition();
        String sourceParam = cond.parameters().isEmpty() ? null : cond.parameters().get(0).name();
        String targetParam = cond.parameters().size() < 2 ? null : cond.parameters().get(1).name();

        TypedSpec condBody = extractLambdaBody(cond);

        return new StoreResolution.JoinResolution.FkJoin(
                targetTable != null ? targetTable : lastHop.tableName(),
                sourceParam, targetParam,
                nav.isToMany(),
                condBody,
                targetResolution);
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
    private StoreResolution.JoinResolution buildEmbeddedJoin(
            TypedEmbeddedExtendCol col, String parentTable,
            StoreResolution.JoinResolution existing) {
        Map<String, String> subPropToCol = new LinkedHashMap<>();
        for (var sub : col.subColumns()) {
            subPropToCol.put(sub.propertyName(), sub.columnName());
        }

        // Otherwise mapping: an FK join already exists for this alias. The
        // property has both an embedded sub-mapping (sub-cols on parent table)
        // AND an FK fallback. Wrap in an {@link Otherwise} so
        // {@link com.gs.legend.plan.lowering.scalar.PropertyAccessLowering}
        // dispatches at access time: embedded sub-cols → parent column directly
        // (no JOIN); other sub-properties → fall through to the FK join.
        if (existing instanceof StoreResolution.JoinResolution.FkJoin fk) {
            return new StoreResolution.JoinResolution.Otherwise(subPropToCol, fk);
        }

        StoreResolution embeddedTarget = new StoreResolution(
                parentTable, null, subPropToCol, Map.of());
        return new StoreResolution.JoinResolution.Embedded(false, embeddedTarget);
    }

    /**
     * Extract the parent-table column map from an Embedded JoinResolution
     * built by {@link #buildEmbeddedJoin}. Used when an Otherwise
     * mapping arrives in the order embedded-then-association.
     */
    private static Map<String, String> embeddedSubColMap(StoreResolution.JoinResolution.Embedded emb) {
        return emb.targetResolution().propertyToColumn();
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
        String tableName = rm.table().dbName();
        Map<String, String> propToCol = new LinkedHashMap<>();
        for (PropertyMapping pm : rm.propertyMappings()) {
            String prop = pm.propertyName();
            String col = pm.hasJoinChain() ? pm.propertyName() : pm.columnName();
            propToCol.put(prop, col);
        }
        return new StoreResolution(tableName, classFqn, propToCol, Map.of());
    }

    /**
     * Builds inline struct-array {@link StoreResolution.JoinResolution}s for
     * a class. For every class property whose type is a user class and
     * multiplicity is [*], emits an identity-mapped UNNEST join so
     * graphFetch can project sub-rows without requiring a mapping entry.
     *
     * @return alias → join entries to merge into the running joins map.
     *         Always a fresh, mutable map (caller may further fold into it).
     */
    private Map<String, StoreResolution.JoinResolution> buildStructArrayJoins(String className) {
        Map<String, StoreResolution.JoinResolution> joins = new LinkedHashMap<>();
        var pureClass = modelContext.findClass(className).orElse(null);
        if (pureClass == null) return joins;
        for (var prop : pureClass.properties()) {
            if (prop.multiplicity().isSingular()) continue;
            if (!(prop.type() instanceof Type.ClassType targetRef)) continue;
            String targetFqn = targetRef.qualifiedName();
            if (modelContext.findClass(targetFqn).isEmpty()) continue;
            StoreResolution targetResolution = resolveIdentity(targetFqn);
            if (targetResolution == null) continue;
            joins.put(prop.name(), new StoreResolution.JoinResolution.StructArrayUnnest(
                    prop.name(), true, targetResolution));
        }
        return joins;
    }

    // ==================== Extend Pruning ====================

    /**
     * Pure: computes which scalar extend columns this {@code ext} node is
     * actually read by the query, and returns an
     * {@link StoreResolution.ExtendNodeCols} marker if pruning is needed,
     * or {@code null} if every declared column is used (no marker required).
     *
     * <p>Inputs:
     * <ul>
     *   <li><b>Declared aliases</b>: every column on {@code ext.extensions()}.</li>
     *   <li><b>Used aliases</b>: {@code classPropertyAccesses[className]},
     *       populated by TypeChecker from each {@code $row.alias} read in
     *       the query body.</li>
     * </ul>
     *
     * <p>The active set = declared ∩ used. Caller re-stamps {@code ext}
     * with the marker layered onto the existing class-store resolution
     * (the underlying {@code propertyToColumn} / {@code joins} fields
     * must survive — {@code ExtendLowering}'s join-aware skip path reads
     * them, and an M2M passthrough class-typed property would mis-render
     * if its FK JoinResolution were dropped).
     *
     * <p>Association and embedded extend cols (synth-body output of
     * {@link com.gs.legend.compiler.MappingNormalizer}) are never counted
     * toward the active set: their physical traversal is installed
     * on-demand at access time (NavScope JOIN, scalar EXISTS), so the
     * synth-body's eager LEFT JOIN is always redundant. Marking them as
     * inactive here lets {@code ExtendLowering} drop the join entirely
     * when the query never navigates the association — critical for
     * to-many associations where the eager join would inflate rows
     * before {@code EXISTS} can de-duplicate.
     */
    private StoreResolution.ExtendNodeCols pruneUnusedExtendCols(TypedExtend ext, String className) {
        Set<String> declared = new HashSet<>();
        Set<String> scalarAliases = new HashSet<>();
        for (var col : ext.extensions()) {
            String alias = aliasOf(col);
            declared.add(alias);
            if (col instanceof TypedScalarExtendCol
                    || col instanceof TypedWindowExtendCol
                    || col instanceof TypedTraverseExtendCol) {
                scalarAliases.add(alias);
            }
        }
        if (declared.isEmpty()) return null;

        Set<String> usedProps = typeResult.dependencies().classPropertyAccesses()
                .getOrDefault(className, Set.of());
        Set<String> active = new HashSet<>();
        for (String a : scalarAliases) if (usedProps.contains(a)) active.add(a);
        if (active.size() == declared.size()) return null;     // every col used → no marker needed

        return new StoreResolution.ExtendNodeCols(active);
    }
}
