package com.gs.legend.compiler;

import com.gs.legend.compiled.CompiledExpression;
import com.gs.legend.compiled.CompiledFunction;
import com.gs.legend.compiler.typed.*;
import com.gs.legend.model.ModelContext;
import com.gs.legend.model.m3.PureClass;
import com.gs.legend.model.m3.Type;
import com.gs.legend.model.mapping.ClassMapping;
import com.gs.legend.model.mapping.PureClassMapping;
import com.gs.legend.model.mapping.RelationalMapping;
import com.gs.legend.model.store.PropertyMapping;

import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * <h1>MappingResolverV2 — logical→physical HIR rewrite (single switch).</h1>
 *
 * <p>End-state from {@code .windsurf/plans/mapping-resolver-as-rewrite.md}.
 * Replaces the sidecar-stamping {@link MappingResolver}: this class is a
 * pure {@code TypedSpec → TypedSpec} rewrite with no escaping state. After
 * {@link #resolve} returns, every {@link TypedPropertyAccess} carries a
 * physical column on its {@code property} field, every association
 * traversal has been turned into an explicit {@link TypedJoin} or
 * {@link TypedFlatten} (struct-array-unnest variant), and no
 * {@link TypedGetAll} survives.
 *
 * <h2>The four rules</h2>
 *
 * <p>The walk is a single sealed switch over {@code TypedSpec} that
 * applies these rules bottom-up with a substitution environment
 * ({@link Scope}):
 *
 * <ol>
 *   <li><b>Rule 1 — Class fetch inlining.</b> {@link TypedGetAll} → the
 *       inlined synth body of the class's mapping function. Memoized;
 *       cycle-guarded; M2M chains expand recursively. Synth-body
 *       scalar/window/traverse extend cols pruned if their alias is not
 *       in {@code classPropertyAccesses[class]}; <b>all</b>
 *       association/embedded extend cols are dropped from the inlined
 *       tree (their physical realization is added by Rule 3).</li>
 *
 *   <li><b>Rule 2 — Logical→physical column.</b>
 *       {@link TypedPropertyAccess}{@code (row, "logical")} where
 *       {@code row} is bound to a physical row alias rewrites
 *       {@code .property} to the physical column name via β-substitution
 *       against the inlined body's PM seed (threaded through
 *       {@link Scope#env}). {@code forwardPassthrough} and
 *       {@code forwardRelationalRename} collapse into this rule (they
 *       are β-substitution against synth-body extend bodies during
 *       Rule 1 inlining).</li>
 *
 *   <li><b>Rule 3 — Association → explicit join.</b>
 *       {@link TypedPropertyAccess} with a non-empty
 *       {@code associationPath} rewrites by walking the path and
 *       installing one {@link TypedJoin} (or {@link TypedFlatten} for
 *       struct-array hops) per non-embedded hop, immediately upstream
 *       of the smallest enclosing relational operator. The access
 *       itself becomes a column ref on the joined alias. {@code
 *       Otherwise} dispatch happens here: per-leaf-property check of
 *       {@code embeddedSubCols} → embedded path or FK path.</li>
 *
 *   <li><b>Rule 4 — Implicit-serialize wrap.</b> Class-typed roots get
 *       wrapped in {@link TypedSerializeImplicit} with a
 *       {@code ResolvedGraphTree} (TODO: phase-typed via sealed
 *       {@code TypedGraphTree}). Leaf list comes from
 *       {@code modelContext.findClass(rootClass).properties()}; the
 *       physical column for each leaf is resolved against the rewritten
 *       root body's column schema.</li>
 * </ol>
 *
 * <h2>Mirror, Don't Invent</h2>
 *
 * <p>Per-operator rules below cite the {@code bindVar(...)} site in the
 * corresponding lowering file they mirror. If MR's schema-env disagrees
 * with the lowering's binding semantics on any operator, that is a bug —
 * the parity fixture catches it as SQL drift.
 *
 * <h2>What this replaces</h2>
 *
 * <p>{@link StoreResolution} (entire file), {@code ResolvedMappings},
 * {@code ResolvedExpression}, {@code LoweringContext.storeFor},
 * {@code Scalar.store}, {@code NavScope}, {@code Relations.install*},
 * {@code Relations.joinTargetRelation}, {@code SourceLowering(TypedGetAll)},
 * {@code pruneUnusedExtendCols}, {@code ExtendNodeCols},
 * {@code forwardPassthrough}, {@code forwardRelationalRename},
 * {@code Walk A}, {@code Walk B}, {@code restampSubtree}, and the
 * separate {@link PropertyAccessPopulator}. All collapse into this single
 * rewrite.
 *
 * <p><b>Status:</b> spine. Most arms throw {@link
 * UnsupportedOperationException} with a {@code TODO} pointing at the
 * thing to fill in. Each arm's docstring cites the lowering file +
 * binding site whose semantics it mirrors.
 */
public final class MappingResolverV2 {

    // ==================== Public entry ====================

    private final CompiledExpression typeResult;
    private final ModelContext model;
    private final NormalizedMapping mappings;
    private final Map<String, Set<String>> classPropertyAccesses;
    private final Map<String, TypedSpec> classMemo = new HashMap<>();
    private final Set<String> resolving = new HashSet<>();
    /**
     * C+ row-shape cache. Identity-keyed by the rewritten AST node;
     * lazily populated by {@link #buildIndex}. The cache is inert —
     * deleting it preserves correctness, only costs cache misses. The
     * AST remains the single source of truth for row shape.
     */
    private final IdentityHashMap<TypedSpec, RowIndex> indexCache = new IdentityHashMap<>();

    public MappingResolverV2(CompiledExpression typeResult,
                             ModelContext model,
                             NormalizedMapping mappings,
                             Map<String, Set<String>> classPropertyAccesses) {
        this.typeResult = typeResult;
        this.model = model;
        this.mappings = mappings;
        this.classPropertyAccesses = classPropertyAccesses;
    }

    /**
     * Top-level resolve. Returns the rewritten HIR.
     *
     * <p>After return:
     * <ul>
     *   <li>No {@link TypedGetAll} anywhere in the tree.</li>
     *   <li>Every {@link TypedPropertyAccess#property} is a physical column.</li>
     *   <li>Every association traversal is an explicit {@link TypedJoin}
     *       or {@link TypedFlatten}.</li>
     *   <li>Class-typed roots wrapped in {@link TypedSerializeImplicit}
     *       with a resolved leaf tree.</li>
     * </ul>
     */
    public TypedSpec resolve(TypedSpec hir) {
        return wrapImplicitSerializeIfNeeded(rewrite(hir, Scope.empty()));
    }

    // ==================== Walk state ====================

    /**
     * Per-walk variable bindings. Each entry maps a lambda-bound
     * variable name to the AST node representing its row source. Rule 2
     * resolves property accesses by querying {@link #buildIndex} on the
     * bound node — no schema digest is carried alongside.
     *
     * <p>Rule 3 will reintroduce a pending-joins channel; for now Scope
     * is the minimum needed for Rules 1 + 2.
     */
    record Scope(Map<String, TypedSpec> env, Optional<String> mappingClass) {
        static Scope empty() {
            return new Scope(Map.of(), Optional.empty());
        }

        Scope bind(String name, TypedSpec node) {
            Map<String, TypedSpec> next = new HashMap<>(env);
            next.put(name, node);
            return new Scope(next, mappingClass);
        }

        /**
         * Mark that the subtree being rewritten is the inlined body of
         * {@code classFqn}'s mapping function. {@link #rewriteExtend}
         * reads this to know that the extend cols here are
         * compiler-generated PM materializations — candidates for
         * pruning by {@code classPropertyAccesses[classFqn]}. When this
         * is empty, the extend is user-authored and pruning is skipped.
         */
        Scope enterMapping(String classFqn) {
            return new Scope(env, Optional.of(classFqn));
        }
    }

    /**
     * Logical alias → physical column map for a relational node's row.
     * Built lazily by {@link #buildIndex} from the AST and cached by
     * node identity. The cache is a memoization, never an authoritative
     * record — clearing it preserves correctness.
     *
     * <p>Future: when Rule 3 lands, an entry may also reference a
     * {@link TypedAssociationExtendCol} so association traversal can
     * find its hop info from the same lookup. The plan is to fatten
     * the value type, not split the cache.
     */
    record RowIndex(Map<String, String> byAlias) {}

    // ==================== Rule 1 — class fetch inlining ====================

    /**
     * Inline a class fetch. Memoized per FQN; cycle-guarded.
     *
     * <p>The inlined body is the class's compiled mapping function,
     * with extend pruning applied:
     * <ul>
     *   <li>Scalar/window/traverse extend cols dropped if alias ∉
     *       {@link #classPropertyAccesses}{@code [class]}.</li>
     *   <li>All association/embedded extend cols dropped from the
     *       returned body — their join info populates {@code seedSchema.joins}.</li>
     *   <li>Inner {@link TypedGetAll}s (M2M chains) recursed via this
     *       same method — memo hit for already-resolved upstreams.</li>
     * </ul>
     *
     * <p>Cycle handling: a self-join's nested fetch hits the
     * {@link #resolving} set and returns a stub
     * {@code TypedTableReference} body + PM-seeded schema (no joins).
     * Mirrors today's {@code shallowResolution}.
     */
    private TypedSpec inlineClassFetch(String classFqn) {
        TypedSpec memo = classMemo.get(classFqn);
        if (memo != null) return memo;

        if (resolving.contains(classFqn)) {
            // Cycle: target class is in progress higher in the stack
            // (self-join / back-reference). Today's resolver returns a
            // shallowResolution stub here. C+ doesn't need a stub for the
            // resolver — buildIndex consumes whatever AST is bound — but a
            // cycle in inlineClassFetch itself still has to terminate.
            // No cycle test exists in the parity fixture yet; throw until
            // one does and we can design the right termination.
            throw new UnsupportedOperationException(
                    "TODO: cycle stub for " + classFqn
                            + " (no parity test exercises this yet)");
        }

        resolving.add(classFqn);
        try {
            // The synth body produced by MappingNormalizer is the inlined
            // representation of `Class.all()`. C+ binds this AST directly;
            // buildIndex walks it on demand to derive alias→physical-column
            // mappings. No precomputed seed schema is needed — the synth
            // body's structure (TypedTableReference + TypedExtend chain) is
            // self-describing.
            CompiledFunction cf = typeResult.dependencies().mappingFunctions().get(classFqn);
            if (cf == null) {
                throw new IllegalStateException(
                        "no compiled mapping function for class fetch: " + classFqn);
            }
            TypedSpec body = cf.body().hir();
            // Pruning is NOT done here — it's emergent from rewrite()'s
            // walk into the synth body. The TypedGetAll arm enters the
            // body with scope.synthBodyClass = classFqn, and rewriteExtend
            // filters extensions when that flag is set. See Rule 1 in the
            // class javadoc.
            classMemo.put(classFqn, body);
            return body;
        } finally {
            resolving.remove(classFqn);
        }
    }

    private String canonicalize(String name) {
        return model.findClass(name).map(PureClass::qualifiedName).orElse(name);
    }

    // ==================== Single-switch rewriter ====================

    /**
     * The rewriter. Applies all four rules in one walk. Returns the
     * rewritten subtree; updates {@code scope.pendingJoins} for any
     * association traversals encountered in scalar position.
     *
     * <p>Each arm cites the lowering file + binding site whose
     * semantics it mirrors. If you change an arm here, also check the
     * lowering counterpart.
     */
    private TypedSpec rewrite(TypedSpec node, Scope scope) {
        return switch (node) {

            // ---------- Rule 1: class fetch ----------
            // Mirrors SourceLowering.lower(TypedGetAll). Inline the
            // mapping function body and recurse with mappingClass set so
            // rewriteExtend can prune unused scalar/window/traverse cols.
            // The body is self-describing for buildIndex; no schema seed
            // is propagated.
            case TypedGetAll ga -> rewrite(inlineClassFetch(ga.className()),
                    scope.enterMapping(canonicalize(ga.className())));

            // ---------- Relation source terminals ----------
            // Pure data — no rewrite needed. Schema (when queried via
            // buildIndex) is read from the node's own type annotation.
            case TypedTableReference n -> n;
            case TypedTdsLiteral n -> n;
            case TypedSourceUrl n -> n;

            // ---------- Relational operators ----------
            case TypedFilter n -> rewriteFilter(n, scope);
            case TypedSort n -> rewriteSort(n, scope);
            case TypedSlice n -> rewriteSlice(n, scope);
            case TypedDistinct n -> rewriteDistinct(n, scope);
            case TypedFlatten n -> rewriteFlatten(n, scope);
            case TypedRename n -> rewriteRename(n, scope);
            case TypedConcatenate n -> rewriteConcatenate(n, scope);
            case TypedFold n -> rewriteFold(n, scope);
            case TypedMap n -> rewriteMap(n, scope);
            case TypedProject n -> rewriteProject(n, scope);
            case TypedExtend n -> rewriteExtend(n, scope);
            case TypedGroupBy n -> rewriteGroupBy(n, scope);
            case TypedAggregate n -> rewriteAggregate(n, scope);
            case TypedPivot n -> rewritePivot(n, scope);
            case TypedJoin n -> rewriteJoin(n, scope);
            case TypedAsOfJoin n -> rewriteAsOfJoin(n, scope);
            case TypedSelect n -> rewriteSelect(n, scope);
            case TypedZip n -> rewriteZip(n, scope);
            case TypedFrom n -> rewriteFrom(n, scope);

            // ---------- Rule 4: graph fetch / serialize ----------
            case TypedGraphFetch n -> rewriteGraphFetch(n, scope);
            case TypedSerialize n -> rewriteSerialize(n, scope);
            case TypedSerializeImplicit n -> rewriteSerializeImplicit(n, scope);
            case TypedWrite n -> rewriteWrite(n, scope);

            // ---------- Rule 2 + Rule 3: property access ----------
            case TypedPropertyAccess n -> rewritePropertyAccess(n, scope);

            // ---------- Bindings / scalar / control flow ----------
            case TypedVariable v -> v;
            case TypedLambda lam -> rewriteLambda(lam, scope);
            case TypedIf n -> rewriteIf(n, scope);
            case TypedLet n -> rewriteLet(n, scope);
            case TypedBlock n -> rewriteBlock(n, scope);
            case TypedMatch n -> rewriteMatch(n, scope);
            case TypedCast n -> rewriteCast(n, scope);
            case TypedCollection n -> rewriteCollection(n, scope);
            case TypedNewInstance n -> rewriteNewInstance(n, scope);
            case TypedStructExtract n -> rewriteStructExtract(n, scope);
            case TypedNativeCall n -> rewriteNativeCall(n, scope);
            case TypedEval n -> n;

            // ---------- Rule 0: user call (TODO: fold UserCallInliner) ----------
            case TypedUserCall uc -> throw new IllegalStateException(
                    "TODO: fold UserCallInliner into MR (plan §Unified Inliner). "
                            + "Currently UserCallInliner runs as a separate prologue. "
                            + "Got: " + uc.functionFqn());

            // ---------- Leaves ----------
            case TypedCInteger n -> n;
            case TypedCFloat n -> n;
            case TypedCDecimal n -> n;
            case TypedCString n -> n;
            case TypedCBoolean n -> n;
            case TypedCDateTime n -> n;
            case TypedCStrictDate n -> n;
            case TypedCStrictTime n -> n;
            case TypedCLatestDate n -> n;
            case TypedCByteArray n -> n;
            case TypedEnumValue n -> n;
            case TypedPackageableRef n -> n;
        };
    }

    // ==================== Per-op rewriters ====================
    //
    // Each method mirrors the corresponding lowering rule's bindVar
    // semantics. Each returns the rewritten subtree directly; row-shape
    // information is recovered via {@link #buildIndex} on the returned
    // node when needed.

    /**
     * Binds the first parameter of a single-param lambda to a source
     * AST node. No-op if the lambda has zero params or the source is
     * null. Rule 2 will query {@link #buildIndex} on the bound node
     * when resolving property accesses inside the lambda body.
     */
    private Scope bindFirst(Scope scope, TypedLambda lam, TypedSpec source) {
        if (source == null || lam.parameters().isEmpty()) return scope;
        return scope.bind(lam.parameters().get(0).name(), source);
    }

    private Scope bindJoinCondParams(Scope scope, TypedLambda cond, TypedSpec left, TypedSpec right) {
        Scope s = scope;
        if (left != null && cond.parameters().size() >= 1) {
            s = s.bind(cond.parameters().get(0).name(), left);
        }
        if (right != null && cond.parameters().size() >= 2) {
            s = s.bind(cond.parameters().get(1).name(), right);
        }
        return s;
    }

    /** Mirrors FilterLowering.java:89. */
    private TypedSpec rewriteFilter(TypedFilter n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);
        TypedLambda pred = rewriteLambda(n.predicate(), bindFirst(scope, n.predicate(), src));
        return new TypedFilter(src, pred, n.def(), n.info());
    }

    /** Mirrors SortLimitLowering.java:64. */
    private TypedSpec rewriteSort(TypedSort n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);
        List<TypedSortKey> keys = n.keys().stream()
                .map(k -> rewriteSortKey(k, scope, src))
                .toList();
        return new TypedSort(src, keys, n.def(), n.info());
    }

    private TypedSortKey rewriteSortKey(TypedSortKey k, Scope scope, TypedSpec src) {
        return switch (k) {
            case TypedColumnSortKey c -> c;
            case TypedExpressionSortKey e ->
                    new TypedExpressionSortKey(
                            rewriteLambda(e.keyFn(), bindFirst(scope, e.keyFn(), src)),
                            e.direction());
        };
    }

    private TypedSpec rewriteSlice(TypedSlice n, Scope scope) {
        return new TypedSlice(rewrite(n.source(), scope), n.offset(), n.limit(), n.def(), n.info());
    }

    private TypedSpec rewriteDistinct(TypedDistinct n, Scope scope) {
        return new TypedDistinct(rewrite(n.source(), scope), n.columns(), n.def(), n.info());
    }

    private TypedSpec rewriteFlatten(TypedFlatten n, Scope scope) {
        return new TypedFlatten(rewrite(n.source(), scope), n.column(), n.def(), n.info());
    }

    private TypedSpec rewriteRename(TypedRename n, Scope scope) {
        return new TypedRename(rewrite(n.source(), scope), n.renames(), n.def(), n.info());
    }

    private TypedSpec rewriteConcatenate(TypedConcatenate n, Scope scope) {
        return new TypedConcatenate(
                rewrite(n.left(), scope), rewrite(n.right(), scope),
                n.def(), n.info());
    }

    private TypedSpec rewriteFold(TypedFold n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);
        TypedLambda red = rewriteLambda(n.reducer(), bindFirst(scope, n.reducer(), src));
        TypedSpec init = rewrite(n.init(), scope);
        return new TypedFold(src, red, init, n.strategy(), n.def(), n.info());
    }

    private TypedSpec rewriteMap(TypedMap n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);
        TypedLambda m = rewriteLambda(n.mapper(), bindFirst(scope, n.mapper(), src));
        return new TypedMap(src, m, n.def(), n.info());
    }

    /** Mirrors ProjectLowering.java:59. */
    private TypedSpec rewriteProject(TypedProject n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);
        List<TypedProjectionCol> cols = n.projections().stream()
                .map(p -> new TypedProjectionCol(
                        p.alias(),
                        rewriteLambda(p.expression(), bindFirst(scope, p.expression(), src)),
                        p.associationPath()))
                .toList();
        return new TypedProject(src, cols, n.def(), n.info());
    }

    /**
     * Mirrors ExtendLowering.java:228, :295. Rule 1's structural pruning
     * is folded in here: when {@code scope.mappingClass()} is set, this
     * extend is inside an inlined mapping function body, and any
     * scalar/window/traverse col whose alias is not in
     * {@code classPropertyAccesses[mappingClass]} is dropped (no marker,
     * no flag — the col simply does not appear). Association/embedded
     * cols are always kept (Rule 3 territory). If every col is pruned
     * and there are no traversal specs, the entire {@link TypedExtend}
     * collapses to its source. When {@code mappingClass} is empty the
     * extend is user-authored and all cols are preserved.
     */
    private TypedSpec rewriteExtend(TypedExtend n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);
        List<TypedExtendCol> kept = scope.mappingClass()
                .map(c -> classPropertyAccesses.getOrDefault(c, Set.<String>of()))
                .map(used -> n.extensions().stream()
                        .filter(col -> isAccessedOrJoinDeclaring(col, used))
                        .toList())
                .orElse(n.extensions());
        if (kept.isEmpty() && n.traversalSpecs().isEmpty()) {
            return src;
        }
        List<TypedExtendCol> rewritten = kept.stream()
                .map(c -> rewriteExtendCol(c, scope, src))
                .toList();
        return new TypedExtend(src, n.traversalSpecs(), rewritten, n.def(), n.info());
    }

    /**
     * True if the extend col should survive mapping-body pruning.
     * Scalar/window/traverse cols survive iff their alias is in
     * {@code used} (the user query reads it). Association/embedded
     * cols always survive — they declare joins, which Rule 3 will
     * materialize into explicit {@link TypedJoin}s.
     */
    private static boolean isAccessedOrJoinDeclaring(TypedExtendCol col, Set<String> used) {
        return switch (col) {
            case TypedScalarExtendCol s    -> used.contains(s.alias());
            case TypedWindowExtendCol w    -> used.contains(w.alias());
            case TypedTraverseExtendCol t  -> used.contains(t.alias());
            case TypedAssociationExtendCol a -> true;
            case TypedEmbeddedExtendCol em   -> true;
        };
    }

    private TypedExtendCol rewriteExtendCol(TypedExtendCol c, Scope scope, TypedSpec src) {
        return switch (c) {
            case TypedScalarExtendCol s ->
                    new TypedScalarExtendCol(s.alias(),
                            rewriteLambda(s.expression(), bindFirst(scope, s.expression(), src)),
                            s.returnType());
            case TypedWindowExtendCol w -> w; // TODO: recurse on funcArgs / reducer / outerWrapper
            case TypedTraverseExtendCol t ->
                    new TypedTraverseExtendCol(t.alias(), t.hops(),
                            rewriteLambda(t.expression(), bindFirst(scope, t.expression(), src)));
            case TypedAssociationExtendCol a -> a;
            case TypedEmbeddedExtendCol e -> e;
        };
    }

    /** Mirrors GroupByAggregateLowering.java:297, :324. */
    private TypedSpec rewriteGroupBy(TypedGroupBy n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);
        List<TypedGroupKey> keys = n.keys().stream()
                .map(k -> rewriteGroupKey(k, scope, src))
                .toList();
        List<TypedAggCall> aggs = n.aggs().stream()
                .map(a -> rewriteAggCall(a, scope, src))
                .toList();
        return new TypedGroupBy(src, keys, aggs, n.def(), n.info());
    }

    private TypedGroupKey rewriteGroupKey(TypedGroupKey k, Scope scope, TypedSpec src) {
        return switch (k) {
            case TypedColumnGroupKey c -> c;
            case TypedExpressionGroupKey e ->
                    new TypedExpressionGroupKey(
                            rewriteLambda(e.keyFn(), bindFirst(scope, e.keyFn(), src)),
                            e.alias());
            case TypedAssociationGroupKey a -> a;
        };
    }

    private TypedAggCall rewriteAggCall(TypedAggCall a, Scope scope, TypedSpec src) {
        TypedLambda fn1 = a.fn1() == null ? null
                : rewriteLambda(a.fn1(), bindFirst(scope, a.fn1(), src));
        TypedLambda fn2 = a.fn2() == null ? null
                : rewriteLambda(a.fn2(), bindFirst(scope, a.fn2(), src));
        List<TypedSpec> extra = a.extraArgs().stream().map(x -> rewrite(x, scope)).toList();
        return new TypedAggCall(a.alias(), a.func(), fn1, fn2, extra, a.returnType(), a.castType());
    }

    private TypedSpec rewriteAggregate(TypedAggregate n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);
        List<TypedAggCall> aggs = n.aggs().stream()
                .map(a -> rewriteAggCall(a, scope, src))
                .toList();
        return new TypedAggregate(src, aggs, n.def(), n.info());
    }

    private TypedSpec rewritePivot(TypedPivot n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);
        List<TypedAggCall> aggs = n.aggs().stream()
                .map(a -> rewriteAggCall(a, scope, src))
                .toList();
        return new TypedPivot(src, n.pivotColumns(), aggs, n.def(), n.info());
    }

    /** Mirrors JoinLowering.java:59-60. */
    private TypedSpec rewriteJoin(TypedJoin n, Scope scope) {
        TypedSpec l = rewrite(n.left(), scope);
        TypedSpec r = rewrite(n.right(), scope);
        TypedLambda cond = rewriteLambda(n.condition(),
                bindJoinCondParams(scope, n.condition(), l, r));
        return new TypedJoin(l, r, cond, n.joinType(), n.renames(), n.def(), n.info());
    }

    /** Mirrors JoinLowering.java:131-132. */
    private TypedSpec rewriteAsOfJoin(TypedAsOfJoin n, Scope scope) {
        TypedSpec l = rewrite(n.left(), scope);
        TypedSpec r = rewrite(n.right(), scope);
        Scope inner = bindJoinCondParams(scope, n.matchCondition(), l, r);
        TypedLambda match = rewriteLambda(n.matchCondition(), inner);
        Optional<TypedLambda> key = n.keyCondition().map(lam -> rewriteLambda(lam, inner));
        return new TypedAsOfJoin(l, r, match, key, n.renames(), n.def(), n.info());
    }

    private TypedSpec rewriteSelect(TypedSelect n, Scope scope) {
        return new TypedSelect(rewrite(n.source(), scope), n.cols(), n.def(), n.info());
    }

    private TypedSpec rewriteZip(TypedZip n, Scope scope) {
        List<TypedSpec> srcs = n.sources().stream().map(s -> rewrite(s, scope)).toList();
        return new TypedZip(srcs, n.byKeys(), n.def(), n.info());
    }

    private TypedSpec rewriteFrom(TypedFrom n, Scope scope) {
        return new TypedFrom(rewrite(n.source(), scope), n.mapping(), n.runtime(), n.def(), n.info());
    }

    /** Rule 4 partial: graph fetch tree resolution. */
    private TypedSpec rewriteGraphFetch(TypedGraphFetch n, Scope scope) {
        // TODO: rewrite Parsed→Resolved graph tree leaves using buildIndex on src.
        return new TypedGraphFetch(rewrite(n.source(), scope), n.children(), n.def(), n.info());
    }

    private TypedSpec rewriteSerialize(TypedSerialize n, Scope scope) {
        return new TypedSerialize(rewrite(n.source(), scope), n.format(), n.children(), n.def(), n.info());
    }

    private TypedSpec rewriteSerializeImplicit(TypedSerializeImplicit n, Scope scope) {
        return new TypedSerializeImplicit(rewrite(n.source(), scope), n.children());
    }

    private TypedSpec rewriteWrite(TypedWrite n, Scope scope) {
        return new TypedWrite(
                rewrite(n.source(), scope), rewrite(n.destination(), scope),
                n.def(), n.info());
    }

    // ----- Rule 2 + Rule 3: property access -----

    /**
     * Mirrors PropertyAccessLowering.lower. The single most important
     * arm.
     *
     * <p>Empty associationPath → Rule 2: query {@link #buildIndex} on
     * the AST node bound to the property's source variable; the
     * physical column is {@code byAlias.get(pa.property)}. Rebuild
     * {@code TypedPropertyAccess} with the physical column populated.
     *
     * <p>Non-empty associationPath → Rule 3 (TODO): walk the hops via
     * {@code TypedAssociationExtendCol}s discovered by walking the
     * source node, build {@link TypedJoin}s, and rewrite the access to
     * a column ref on the joined alias.
     */
    private TypedSpec rewritePropertyAccess(TypedPropertyAccess n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);

        // Rule 2: leaf-only property access (no association hops),
        // source is a variable bound in scope.env. Resolve the property
        // to its physical column via buildIndex on the bound source.
        //
        // associationPath shape: TypeChecker stores the full access
        // chain. For `$p.age`, path = ["age"] (size 1). For
        // `$p.firm.legalName`, path = ["firm", "legalName"] (size 2).
        // Hops = path[0..n-1], leaf = path[n-1]. So "no hops" means
        // size <= 1 (or empty Optional).
        boolean leafOnly = n.associationPath().isEmpty()
                || n.associationPath().get().size() <= 1;
        if (leafOnly
                && n.physicalColumn().isEmpty()
                && src instanceof TypedVariable v
                && scope.env().get(v.name()) != null) {
            TypedSpec source = scope.env().get(v.name());
            String physical = buildIndex(source).byAlias().get(n.property());
            if (physical != null) {
                return new TypedPropertyAccess(
                        src, n.property(), n.associationPath(),
                        Optional.of(physical), n.info());
            }
            // Property not in the index: leave unresolved. Either the
            // synth body's structure doesn't expose this alias yet
            // (Rule 3 territory) or the property is genuinely
            // undefined. Don't throw — let downstream lowering fail
            // loudly if the column doesn't exist.
        }

        // TODO: Rule 3 — non-empty associationPath: walk hops, install
        // TypedJoin nodes, rewrite to column ref on joined alias.
        return new TypedPropertyAccess(
                src, n.property(), n.associationPath(), n.physicalColumn(), n.info());
    }

    // ----- Lambda binding -----

    /**
     * Rewrites a lambda's body. Param binding is the responsibility of
     * the relational op rewriting this lambda — Filter, Project, etc.,
     * which extend {@code scope.env} with (paramName → source AST node)
     * BEFORE calling here.
     *
     * <p>For free lambdas (e.g. callback args to native functions where
     * the param doesn't bind to a relational source), the param simply
     * has no entry in {@code env} and Rule 2 leaves accesses untouched.
     */
    private TypedLambda rewriteLambda(TypedLambda lam, Scope scope) {
        List<TypedSpec> body = lam.body().stream().map(s -> rewrite(s, scope)).toList();
        return new TypedLambda(lam.parameters(), body, lam.info());
    }

    // ----- Control flow -----

    private TypedSpec rewriteIf(TypedIf n, Scope scope) {
        return new TypedIf(
                rewrite(n.condition(), scope),
                rewrite(n.thenBranch(), scope),
                rewrite(n.elseBranch(), scope),
                n.info());
    }

    private TypedSpec rewriteLet(TypedLet n, Scope scope) {
        return new TypedLet(n.name(), rewrite(n.value(), scope), n.info());
    }

    private TypedSpec rewriteBlock(TypedBlock n, Scope scope) {
        List<TypedSpec> stmts = n.stmts().stream().map(s -> rewrite(s, scope)).toList();
        return new TypedBlock(stmts, n.info());
    }

    private TypedSpec rewriteMatch(TypedMatch n, Scope scope) {
        TypedSpec subject = rewrite(n.subject(), scope);
        List<TypedLambda> cases = n.cases().stream().map(c -> rewriteLambda(c, scope)).toList();
        return new TypedMatch(subject, cases, n.info());
    }

    private TypedSpec rewriteCast(TypedCast n, Scope scope) {
        return new TypedCast(rewrite(n.expr(), scope), n.targetType(), n.info());
    }

    private TypedSpec rewriteCollection(TypedCollection n, Scope scope) {
        // TODO: at relation root with class-typed values, rewrite to TypedClassValues.
        List<TypedSpec> vals = n.values().stream().map(v -> rewrite(v, scope)).toList();
        return new TypedCollection(vals, n.info());
    }

    private TypedSpec rewriteNewInstance(TypedNewInstance n, Scope scope) {
        // TODO: at relation root, rewrite to TypedClassValues.
        LinkedHashMap<String, TypedSpec> vals = new LinkedHashMap<>();
        for (var e : n.values().entrySet()) vals.put(e.getKey(), rewrite(e.getValue(), scope));
        return new TypedNewInstance(n.className(), vals, n.info());
    }

    private TypedSpec rewriteStructExtract(TypedStructExtract n, Scope scope) {
        return new TypedStructExtract(rewrite(n.source(), scope), n.field(), n.info());
    }

    private TypedSpec rewriteNativeCall(TypedNativeCall n, Scope scope) {
        List<TypedSpec> args = n.args().stream().map(a -> rewrite(a, scope)).toList();
        return new TypedNativeCall(n.func(), args, n.info());
    }

    // ==================== buildIndex (C+) ====================

    /**
     * Lazily compute the alias→physical-column index for a relational
     * AST node. The index is read on demand by Rule 2 (and later Rule 3)
     * to resolve property accesses against the row shape produced by
     * the bound node.
     *
     * <p>Caching is keyed by node identity ({@link IdentityHashMap}):
     * the same rewritten subtree, queried twice, walks once. The cache
     * is inert — clearing it preserves correctness, only costs cache
     * misses. The AST itself is the single source of truth.
     *
     * <p>Each case in {@link #computeIndex} layers this op's
     * contribution onto its source's index. A node type with no case
     * throws — adding a new relop without a buildIndex case fails at
     * the right place with a localized message.
     */
    private RowIndex buildIndex(TypedSpec node) {
        RowIndex memo = indexCache.get(node);
        if (memo != null) return memo;
        RowIndex computed = computeIndex(node);
        indexCache.put(node, computed);
        return computed;
    }

    private RowIndex computeIndex(TypedSpec node) {
        return switch (node) {
            case TypedTableReference t -> indexFromTableSchema(t);
            case TypedExtend e         -> overlayExtends(buildIndex(e.source()), e.extensions());
            case TypedProject p        -> aliasIdentity(p.projections().stream().map(TypedProjectionCol::alias).toList());
            case TypedFilter f         -> buildIndex(f.source());
            case TypedSort s           -> buildIndex(s.source());
            case TypedDistinct d       -> buildIndex(d.source());
            case TypedSlice s          -> buildIndex(s.source());
            case TypedFlatten f        -> buildIndex(f.source()); // TODO: collection→element row shape
            case TypedRename r         -> applyRenameIndex(buildIndex(r.source()), r.renames());
            case TypedSelect s         -> filterCols(buildIndex(s.source()), s.cols());
            case TypedGroupBy g        -> groupByOutput(g.keys(), g.aggs());
            case TypedAggregate a      -> aliasIdentity(a.aggs().stream().map(TypedAggCall::alias).toList());
            case TypedPivot p          -> pivotOutput(p.pivotColumns(), p.aggs());
            case TypedFrom f           -> buildIndex(f.source());
            case TypedConcatenate c    -> buildIndex(c.left());
            case TypedJoin j           -> buildIndex(j.left());      // TODO: merge L+R schemas with renames
            case TypedAsOfJoin j       -> buildIndex(j.left());      // TODO: merge L+R schemas with renames
            case TypedZip z            -> z.sources().isEmpty()
                                          ? new RowIndex(Map.of())
                                          : buildIndex(z.sources().get(0));
            case TypedTdsLiteral t     -> indexFromRelationSchema(t.info(),
                    "TypedTdsLiteral has no Relation schema");
            case TypedSourceUrl u      -> indexFromRelationSchema(u.info(),
                    "TypedSourceUrl has no Relation schema");
            // ^Class(...) literal at relation root — identity-mapped from
            // the model class's properties. V1's resolveIdentity equivalent.
            case TypedNewInstance ni   -> identityIndexFromClass(ni.className());
            // [^Class(...)] literal collection at relation root.
            case TypedCollection coll  -> coll.info().type() instanceof Type.ClassType ct
                    ? identityIndexFromClass(ct.qualifiedName())
                    : new RowIndex(Map.of());
            case TypedGetAll g -> throw new IllegalStateException(
                    "buildIndex: unrewritten TypedGetAll for " + g.className()
                            + " — Rule 1 must inline class fetches before binding.");
            default -> throw new UnsupportedOperationException(
                    "buildIndex: no row shape for " + node.getClass().getSimpleName()
                            + ". Add a case here to teach the resolver this node's logical→physical column mapping.");
        };
    }

    /**
     * Catalog-driven row shape for a {@link TypedTableReference}. The
     * Element Compiler embedded the table's column list in the typed
     * AST as a {@link com.gs.legend.model.m3.Type.Relation}; reading it
     * here mirrors Calcite's {@code RelNode.getRowType()}.
     */
    private RowIndex indexFromTableSchema(TypedTableReference t) {
        if (!(t.info().type() instanceof com.gs.legend.model.m3.Type.Relation rel)) {
            throw new IllegalStateException(
                    "TypedTableReference type is not Relation: " + t.info().type()
                            + " (table: " + t.storeName() + "." + t.tableName() + ")");
        }
        Map<String, String> ptc = new LinkedHashMap<>();
        for (String col : rel.schema().columnNames()) ptc.put(col, col);
        return new RowIndex(ptc);
    }

    /**
     * Layer extend columns onto a base index. For scalar/traverse
     * extends with bodies of shape {@code $param.COL}, the alias maps
     * to {@code COL}. For computed bodies, the alias names itself.
     * Window extends always name themselves. Association/embedded
     * extends contribute nothing to Rule 2 — they're consumed by
     * Rule 3 (when implemented).
     */
    private RowIndex overlayExtends(RowIndex base, List<TypedExtendCol> extensions) {
        Map<String, String> ptc = new LinkedHashMap<>(base.byAlias());
        for (TypedExtendCol c : extensions) {
            switch (c) {
                // Scalar rename optimization: a single-param body of shape
                // `$row.COL` is a relational rename PM (MappingNormalizer's
                // addRenameEnumExpressionExtends emits this for non-identity
                // simple PMs). The lowering produces `t0.COL AS alias` so
                // the output row has a physical column named COL accessible
                // under the logical alias. Computed bodies name themselves.
                case TypedScalarExtendCol s    -> ptc.put(s.alias(), scalarRenameTargetCol(s.expression(), s.alias()));
                case TypedWindowExtendCol w    -> ptc.put(w.alias(), w.alias());
                // Traverse extends always name themselves: the join-chain
                // lowering produces `t1.COL AS alias`, so within the synth
                // body's output relation the physical column for this
                // alias IS the alias itself, not the underlying column on
                // the target table.
                case TypedTraverseExtendCol t  -> ptc.put(t.alias(), t.alias());
                case TypedAssociationExtendCol a -> { /* Rule 3 territory */ }
                case TypedEmbeddedExtendCol e  -> { /* Rule 3 territory */ }
            }
        }
        return new RowIndex(ptc);
    }

    /**
     * For a scalar extend lambda of shape {@code row | $row.COL} (the
     * relational rename pattern), returns {@code COL}. Otherwise the
     * alias names itself.
     */
    private String scalarRenameTargetCol(TypedLambda lam, String fallback) {
        if (lam.body().size() == 1
                && lam.body().get(0) instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable v
                && lam.parameters().size() == 1
                && lam.parameters().get(0).name().equals(v.name())) {
            return pa.physicalColumn().orElse(pa.property());
        }
        return fallback;
    }

    private RowIndex aliasIdentity(List<String> aliases) {
        Map<String, String> ptc = new LinkedHashMap<>();
        for (String a : aliases) ptc.put(a, a);
        return new RowIndex(ptc);
    }

    /**
     * Apply column renames. The lowering emits {@code SELECT old AS new}
     * so downstream consumers see a column literally named {@code new} —
     * the renamed alias is its own physical column at the rename's output
     * level. Mirrors V1's {@code MappingResolver.renameStore}.
     */
    private RowIndex applyRenameIndex(RowIndex src, List<ColRename> renames) {
        Map<String, String> ptc = new LinkedHashMap<>(src.byAlias());
        for (ColRename r : renames) {
            ptc.remove(r.from());
            ptc.put(r.to(), r.to());
        }
        return new RowIndex(ptc);
    }

    private RowIndex filterCols(RowIndex src, List<String> cols) {
        Map<String, String> ptc = new LinkedHashMap<>();
        for (String c : cols) ptc.put(c, src.byAlias().getOrDefault(c, c));
        return new RowIndex(ptc);
    }

    private RowIndex groupByOutput(List<TypedGroupKey> keys, List<TypedAggCall> aggs) {
        Map<String, String> ptc = new LinkedHashMap<>();
        for (TypedGroupKey k : keys) ptc.put(k.alias(), k.alias());
        for (TypedAggCall a : aggs) ptc.put(a.alias(), a.alias());
        return new RowIndex(ptc);
    }

    /**
     * Static schema for a pivot: declared pivot columns + agg aliases. The
     * dynamic spread columns (one per distinct pivot value × agg) aren't
     * statically representable; static Pure code can't reference them by
     * name. Mirrors V1 MappingResolver.resolveQuery TypedPivot arm.
     */
    private RowIndex pivotOutput(List<String> pivotColumns, List<TypedAggCall> aggs) {
        Map<String, String> ptc = new LinkedHashMap<>();
        for (String c : pivotColumns) ptc.put(c, c);
        for (TypedAggCall a : aggs) ptc.put(a.alias(), a.alias());
        return new RowIndex(ptc);
    }

    /**
     * Build an identity index from a node's {@link Type.Relation} schema.
     * Used by relation-source terminals whose row shape is the catalog /
     * literal-declared column list rather than something derived from a
     * mapping.
     */
    private RowIndex indexFromRelationSchema(ExpressionType info, String errorContext) {
        if (!(info.type() instanceof Type.Relation rel)) {
            throw new IllegalStateException(errorContext + ": got " + info.type());
        }
        Map<String, String> ptc = new LinkedHashMap<>();
        for (String col : rel.schema().columnNames()) ptc.put(col, col);
        return new RowIndex(ptc);
    }

    /**
     * Identity index for a class-typed literal ({@code ^Class(...)} or a
     * class-typed collection). Each property is its own physical column
     * — the literal IS its own physical schema. Mirrors V1's
     * {@code resolveIdentity}.
     */
    private RowIndex identityIndexFromClass(String classFqn) {
        PureClass pc = model.findClass(classFqn).orElse(null);
        if (pc == null) return new RowIndex(Map.of());
        Map<String, String> ptc = new LinkedHashMap<>();
        for (var prop : pc.properties()) ptc.put(prop.name(), prop.name());
        return new RowIndex(ptc);
    }

    // ==================== Rule 4: implicit-serialize wrap ====================

    /**
     * If the resolved root is class-typed and not already wrapped,
     * synthesize a {@link TypedSerializeImplicit} with a leaf-only
     * resolved graph tree. Leaf list comes from
     * {@code modelContext.findClass(rootFqn).properties()}; physical
     * columns come from {@link #buildIndex} on the rewritten root.
     */
    private TypedSpec wrapImplicitSerializeIfNeeded(TypedSpec rewritten) {
        // Already wrapped, or already an explicit serialize/graph fetch:
        // those self-envelope via their own lowering rules.
        if (rewritten instanceof TypedGraphFetch
                || rewritten instanceof TypedSerialize
                || rewritten instanceof TypedSerializeImplicit) {
            return rewritten;
        }
        // Only class-typed roots get the implicit JSON envelope; relational
        // / TDS roots (Type.Relation) are emitted bare.
        if (!(rewritten.info().type() instanceof Type.ClassType)) {
            return rewritten;
        }
        // Leaf list: every alias the rewritten root exposes, in declaration
        // order. buildIndex's byAlias is a LinkedHashMap so iteration is
        // stable; the SQL emits columns in the same order.
        RowIndex idx = buildIndex(rewritten);
        List<TypedGraphTree> leaves = idx.byAlias().keySet().stream()
                .map(TypedGraphTree::leaf)
                .toList();
        return new TypedSerializeImplicit(rewritten, leaves);
    }
}
