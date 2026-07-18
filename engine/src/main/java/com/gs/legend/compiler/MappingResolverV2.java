package com.gs.legend.compiler;

import com.gs.legend.compiled.CompiledExpression;
import com.gs.legend.compiled.CompiledFunction;
import com.gs.legend.compiler.typed.*;
import com.gs.legend.model.ModelContext;
import com.gs.legend.model.m3.PureClass;
import com.gs.legend.model.m3.Type;

import java.util.ArrayList;
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
     * <p>{@link #navs} carries the per-relop navigation accumulator
     * when present; relops set it before walking their lambda body and
     * drain it after, turning discovered associations into TypedJoins.
     * Outside a relop's lambda walk it is empty.
     */
    record Scope(Map<String, TypedSpec> env,
                 Map<String, String> envClass,
                 Optional<String> mappingClass,
                 Optional<Navigations> navs) {
        static Scope empty() {
            return new Scope(Map.of(), Map.of(), Optional.empty(), Optional.empty());
        }

        Scope bind(String name, TypedSpec node) {
            Map<String, TypedSpec> next = new HashMap<>(env);
            next.put(name, node);
            return new Scope(next, envClass, mappingClass, navs);
        }

        /**
         * Bind a lambda param to its row source AND record the class
         * FQN this row represents. Used by Rule 3 to resolve assoc
         * cols on the row's owning class even when the bound spec's
         * static type is Relation (post-Rule-1 inlining).
         */
        Scope bindWithClass(String name, TypedSpec node, String classFqn) {
            Map<String, TypedSpec> nextEnv = new HashMap<>(env);
            nextEnv.put(name, node);
            Map<String, String> nextCls = new HashMap<>(envClass);
            if (classFqn != null) nextCls.put(name, classFqn); else nextCls.remove(name);
            return new Scope(nextEnv, nextCls, mappingClass, navs);
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
            return new Scope(env, envClass, Optional.of(classFqn), navs);
        }

        /** Attach a navigation accumulator for the upcoming lambda walk. */
        Scope withNavs(Navigations n) {
            return new Scope(env, envClass, mappingClass, Optional.of(n));
        }
    }

    /**
     * Per-relop bag of association navigations discovered while
     * rewriting the relop's lambda body. The relop creates one before
     * walking its lambda, exposes it via {@link Scope#navs()}, and
     * drains it after the walk to wrap its source in {@link TypedJoin}
     * nodes and add params to the lambda.
     *
     * <p>Mutation is stack-local: the bag is born and dies inside one
     * relop method. The resolver class itself remains immutable; this
     * is the same scoping V1's NavScope uses, just per-relop instead of
     * threaded through a class field.
     */
    static final class Navigations {
        private final LinkedHashMap<List<String>, Navigation> byPrefix = new LinkedHashMap<>();
        private int counter = 0;

        boolean isEmpty() { return byPrefix.isEmpty(); }

        java.util.Collection<Navigation> all() { return byPrefix.values(); }

        /** Lookup an existing navigation for a path prefix. */
        Navigation get(List<String> prefix) { return byPrefix.get(prefix); }

        /** Register a navigation for a path prefix. */
        void put(List<String> prefix, Navigation nav) { byPrefix.put(prefix, nav); }

        /** Allocate a fresh outer-lambda param name. */
        String freshParamName() { return "_jr" + counter++; }
    }

    /**
     * Logical alias → physical column map for a relational node's row,
     * plus alias → association-extend lookup. Both views are derived
     * by walking the AST once via {@link #buildIndex} and cached by
     * node identity. The cache is a memoization, never an authoritative
     * record — clearing it preserves correctness; the AST stays the
     * single source of truth.
     *
     * <p>{@link #assocCols} carries every {@link TypedAssociationExtendCol}
     * visible from the node's row. Rule 3 reads it directly when
     * rewriting path-bearing property accesses to join chains —
     * eliminates the body-walk that an earlier draft did via
     * {@code findAssocColInBody}.
     */
    record RowIndex(
            Map<String, String> byAlias,
            Map<String, TypedAssociationExtendCol> assocCols
    ) {
        /** Convenience: index with no association cols (the common case). */
        RowIndex(Map<String, String> byAlias) { this(byAlias, Map.of()); }

        /** Direct lookup; null if no association of this alias is in scope. */
        TypedAssociationExtendCol assocCol(String alias) { return assocCols.get(alias); }
    }

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
            // Self-join / back-reference: target is in progress higher
            // in the stack. Returning null lets callers (e.g. the nav
            // rewrite loop) abort and leave the access for later
            // resolution rather than infinite-recursing through
            // mapping-function bodies. Phase 3 of the V2 plan
            // introduces explicit cycle-aware reuse.
            return null;
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

    /**
     * Bind the first param of a lambda to a row source AND its owner
     * class FQN. {@code classFqn} may be null if the source is not a
     * class fetch (e.g. a TDS-backed relation); in that case Rule 3
     * navigation will fall through to other resolution paths.
     */
    private Scope bindFirstWithClass(Scope scope, TypedLambda lam, TypedSpec source, String classFqn) {
        if (source == null || lam.parameters().isEmpty()) return scope;
        return scope.bindWithClass(lam.parameters().get(0).name(), source, classFqn);
    }

    /**
     * Recover the owner class FQN of {@code n.source()} when it's a
     * class fetch (TypedGetAll), or when its static type is
     * {@link Type.ClassType}. Returns null for relational sources
     * (TDS literals, joins, projects, etc.) that don't carry a class
     * identity.
     */
    private String sourceOwnerClassFqn(TypedSpec preRewriteSource) {
        if (preRewriteSource instanceof TypedGetAll ga) {
            return canonicalize(ga.className());
        }
        Type t = preRewriteSource.info().type();
        if (t instanceof Type.ClassType ct) return ct.qualifiedName();
        return null;
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

    /**
     * Mirrors FilterLowering.java:89. Pre-Phase-2 shape: walks the
     * predicate body for leaf-PA {@code physicalColumn} stamping but
     * does <em>not</em> rewrite path-bearing accesses to TypedJoin —
     * those keep their {@code associationPath} and are handled by
     * Phase 2's {@link TypedExists} translation. Single-param lambda
     * is preserved (FilterLowering's invariant).
     */
    private TypedSpec rewriteFilter(TypedFilter n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);
        TypedLambda pred = rewriteLambda(n.predicate(), bindFirst(scope, n.predicate(), src));
        return new TypedFilter(src, pred, n.def(), n.info());
    }

    /**
     * Wrap {@code src} in a left-to-right chain of {@link TypedJoin}s,
     * one per registered navigation hop, in registration order.
     */
    private TypedSpec drainNavsToSource(TypedSpec src, Navigations navs) {
        TypedSpec joined = src;
        for (Navigation nav : navs.all()) {
            joined = new TypedJoin(
                    joined, nav.rightBody(), nav.condition(),
                    JoinType.LEFT_OUTER, Map.of(), null, joined.info());
        }
        return joined;
    }

    /**
     * Append one {@link TypedParam} per registered hop to the lambda's
     * parameter list, preserving body and info.
     */
    private TypedLambda expandLambdaParams(TypedLambda lam, Navigations navs) {
        List<TypedParam> params = new ArrayList<>(lam.parameters());
        for (Navigation nav : navs.all()) params.add(nav.param());
        return new TypedLambda(params, lam.body(), lam.info());
    }

    /** Mirrors SortLimitLowering.java:64. */
    private TypedSpec rewriteSort(TypedSort n, Scope scope) {
        String ownerFqn = sourceOwnerClassFqn(n.source());
        TypedSpec src = rewrite(n.source(), scope);
        Navigations navs = new Navigations();
        Scope withClassAndNavs = scope.withNavs(navs);
        // owner FQN is bound on each sort key's keyFn first param via rewriteSortKey
        List<TypedSortKey> keys = n.keys().stream()
                .map(k -> rewriteSortKey(k, withClassAndNavs, src, ownerFqn))
                .toList();
        if (navs.isEmpty()) {
            return new TypedSort(src, keys, n.def(), n.info());
        }
        TypedSpec joinedSrc = drainNavsToSource(src, navs);
        List<TypedSortKey> expandedKeys = keys.stream()
                .map(k -> expandSortKeyParams(k, navs))
                .toList();
        return new TypedSort(joinedSrc, expandedKeys, n.def(), n.info());
    }

    private TypedSortKey rewriteSortKey(TypedSortKey k, Scope scope, TypedSpec src, String ownerFqn) {
        return switch (k) {
            case TypedColumnSortKey c -> c;
            case TypedExpressionSortKey e ->
                    new TypedExpressionSortKey(
                            rewriteLambda(e.keyFn(), bindFirstWithClass(scope, e.keyFn(), src, ownerFqn)),
                            e.direction());
        };
    }

    private TypedSortKey expandSortKeyParams(TypedSortKey k, Navigations navs) {
        return switch (k) {
            case TypedColumnSortKey c -> c;
            case TypedExpressionSortKey e ->
                    new TypedExpressionSortKey(expandLambdaParams(e.keyFn(), navs), e.direction());
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
        String ownerFqn = sourceOwnerClassFqn(n.source());
        TypedSpec src = rewrite(n.source(), scope);
        Navigations navs = new Navigations();
        TypedLambda red = rewriteLambda(n.reducer(), bindFirstWithClass(scope, n.reducer(), src, ownerFqn).withNavs(navs));
        TypedSpec init = rewrite(n.init(), scope);
        if (navs.isEmpty()) {
            return new TypedFold(src, red, init, n.strategy(), n.def(), n.info());
        }
        return new TypedFold(
                drainNavsToSource(src, navs),
                expandLambdaParams(red, navs),
                init, n.strategy(), n.def(), n.info());
    }

    private TypedSpec rewriteMap(TypedMap n, Scope scope) {
        String ownerFqn = sourceOwnerClassFqn(n.source());
        TypedSpec src = rewrite(n.source(), scope);
        Navigations navs = new Navigations();
        TypedLambda m = rewriteLambda(n.mapper(), bindFirstWithClass(scope, n.mapper(), src, ownerFqn).withNavs(navs));
        if (navs.isEmpty()) {
            return new TypedMap(src, m, n.def(), n.info());
        }
        return new TypedMap(
                drainNavsToSource(src, navs),
                expandLambdaParams(m, navs),
                n.def(), n.info());
    }

    /** Mirrors ProjectLowering.java:59. */
    private TypedSpec rewriteProject(TypedProject n, Scope scope) {
        String ownerFqn = sourceOwnerClassFqn(n.source());
        TypedSpec src = rewrite(n.source(), scope);
        Navigations navs = new Navigations();
        List<TypedProjectionCol> cols = n.projections().stream()
                .map(p -> new TypedProjectionCol(
                        p.alias(),
                        rewriteLambda(p.expression(), bindFirstWithClass(scope, p.expression(), src, ownerFqn).withNavs(navs)),
                        p.associationPath()))
                .toList();
        if (navs.isEmpty()) {
            return new TypedProject(src, cols, n.def(), n.info());
        }
        TypedSpec joinedSrc = drainNavsToSource(src, navs);
        List<TypedProjectionCol> expandedCols = cols.stream()
                .map(p -> new TypedProjectionCol(
                        p.alias(),
                        expandLambdaParams(p.expression(), navs),
                        p.associationPath()))
                .toList();
        return new TypedProject(joinedSrc, expandedCols, n.def(), n.info());
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
        Navigations navs = new Navigations();
        List<TypedExtendCol> rewritten = kept.stream()
                .map(c -> rewriteExtendCol(c, scope.withNavs(navs), src))
                .toList();
        if (navs.isEmpty()) {
            return new TypedExtend(src, n.traversalSpecs(), rewritten, n.def(), n.info());
        }
        TypedSpec joinedSrc = drainNavsToSource(src, navs);
        List<TypedExtendCol> expandedCols = rewritten.stream()
                .map(c -> expandExtendColParams(c, navs))
                .toList();
        return new TypedExtend(joinedSrc, n.traversalSpecs(), expandedCols, n.def(), n.info());
    }

    private TypedExtendCol expandExtendColParams(TypedExtendCol c, Navigations navs) {
        return switch (c) {
            case TypedScalarExtendCol s ->
                    new TypedScalarExtendCol(s.alias(), expandLambdaParams(s.expression(), navs), s.returnType());
            case TypedWindowExtendCol w -> w;
            case TypedTraverseExtendCol t ->
                    new TypedTraverseExtendCol(t.alias(), t.hops(), expandLambdaParams(t.expression(), navs));
            case TypedAssociationExtendCol a -> a;
            case TypedEmbeddedExtendCol e -> e;
        };
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
        Navigations navs = new Navigations();
        Scope inner = scope.withNavs(navs);
        List<TypedGroupKey> keys = n.keys().stream()
                .map(k -> rewriteGroupKey(k, inner, src))
                .toList();
        List<TypedAggCall> aggs = n.aggs().stream()
                .map(a -> rewriteAggCall(a, inner, src))
                .toList();
        if (navs.isEmpty()) {
            return new TypedGroupBy(src, keys, aggs, n.def(), n.info());
        }
        TypedSpec joinedSrc = drainNavsToSource(src, navs);
        List<TypedGroupKey> expandedKeys = keys.stream()
                .map(k -> expandGroupKeyParams(k, navs))
                .toList();
        List<TypedAggCall> expandedAggs = aggs.stream()
                .map(a -> expandAggCallParams(a, navs))
                .toList();
        return new TypedGroupBy(joinedSrc, expandedKeys, expandedAggs, n.def(), n.info());
    }

    private TypedGroupKey expandGroupKeyParams(TypedGroupKey k, Navigations navs) {
        return switch (k) {
            case TypedColumnGroupKey c -> c;
            case TypedExpressionGroupKey e ->
                    new TypedExpressionGroupKey(expandLambdaParams(e.keyFn(), navs), e.alias());
            case TypedAssociationGroupKey a -> a;
        };
    }

    private TypedAggCall expandAggCallParams(TypedAggCall a, Navigations navs) {
        return new TypedAggCall(
                a.alias(), a.func(),
                a.fn1() == null ? null : expandLambdaParams(a.fn1(), navs),
                a.fn2() == null ? null : expandLambdaParams(a.fn2(), navs),
                a.extraArgs(), a.returnType(), a.castType());
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
        Navigations navs = new Navigations();
        List<TypedAggCall> aggs = n.aggs().stream()
                .map(a -> rewriteAggCall(a, scope.withNavs(navs), src))
                .toList();
        if (navs.isEmpty()) {
            return new TypedAggregate(src, aggs, n.def(), n.info());
        }
        return new TypedAggregate(
                drainNavsToSource(src, navs),
                aggs.stream().map(a -> expandAggCallParams(a, navs)).toList(),
                n.def(), n.info());
    }

    private TypedSpec rewritePivot(TypedPivot n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);
        Navigations navs = new Navigations();
        List<TypedAggCall> aggs = n.aggs().stream()
                .map(a -> rewriteAggCall(a, scope.withNavs(navs), src))
                .toList();
        if (navs.isEmpty()) {
            return new TypedPivot(src, n.pivotColumns(), aggs, n.def(), n.info());
        }
        return new TypedPivot(
                drainNavsToSource(src, navs),
                n.pivotColumns(),
                aggs.stream().map(a -> expandAggCallParams(a, navs)).toList(),
                n.def(), n.info());
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
     * Mirrors PropertyAccessLowering.lower. Two cases:
     *
     * <ul>
     *   <li><b>Leaf access</b> (no association hops): query
     *       {@link #buildIndex} on the AST node bound to the source
     *       variable; the physical column is
     *       {@code byAlias.get(pa.property)}. Emit a TPA with
     *       physicalColumn populated.</li>
     *   <li><b>Path-bearing access</b> (size ≥ 2 hops, e.g.
     *       {@code $p.firm.legalName}): if a {@link Navigations} sink
     *       is attached to the scope (the enclosing relop is
     *       collecting), get-or-register a single-hop FK navigation for
     *       {@code path[0]} and emit a direct access on the freshly
     *       allocated joined param. The relop drains the sink and
     *       wraps its source in {@link TypedJoin} after the walk.</li>
     * </ul>
     */
    private TypedSpec rewritePropertyAccess(TypedPropertyAccess n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);

        // Path-bearing access: defer to navigation accumulator if
        // present. N-hop paths register one Navigation per non-leaf
        // segment, dedup'd by cumulative dotted prefix so siblings
        // share common ancestors. The relop drains the bag after the
        // lambda walk, wrapping its source in TypedJoin per hop.
        if (n.associationPath().isPresent()
                && n.associationPath().get().size() >= 2
                && src instanceof TypedVariable v
                && scope.navs().isPresent()
                && scope.env().get(v.name()) != null) {
            TypedSpec rewritten = rewriteNavigation(n, v, scope);
            if (rewritten != null) return rewritten;
        }

        // Leaf access: resolve physical column via buildIndex on the
        // bound source. associationPath shape: TypeChecker stores the
        // full access chain — `$p.age` is path=["age"] (size 1),
        // `$p.firm.legalName` is path=["firm","legalName"] (size 2).
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
            // (multi-hop / embedded territory) or the property is
            // genuinely undefined. Don't throw — let downstream
            // lowering fail loudly if the column doesn't exist.
        }

        return new TypedPropertyAccess(
                src, n.property(), n.associationPath(), n.physicalColumn(), n.info());
    }

    /**
     * Rewrite an N-hop path-bearing TPA — e.g. {@code $x.firm.address.city}
     * with {@code path=[firm,address,city]} — into a leaf access on the
     * deepest hop's joined row, registering each hop in the active
     * {@link Navigations} sink as it goes. The relop drains the sink
     * after the lambda walk, wrapping its source in one {@link TypedJoin}
     * per registered hop.
     *
     * <p>Hops dedupe by cumulative {@code List<String>} prefix so siblings
     * (e.g. {@code $x.firm.legalName} and {@code $x.firm.city}) share one
     * registered Navigation; longer prefixes chain off shorter ones.
     *
     * <p>Returns {@code null} when the navigation cannot be rewritten
     * (no owner class FQN bound, owner body unavailable, assoc col
     * missing on owner, target class still resolving — self-cycle).
     * The caller leaves the access path-bearing and Phase 2 / 3 of the
     * V2 plan handles the residue.
     */
    private TypedSpec rewriteNavigation(TypedPropertyAccess pa, TypedVariable rowVar, Scope scope) {
        List<String> path = pa.associationPath().get();
        int hopCount = path.size() - 1;                  // path = [..hops.., leaf]
        Navigations navs = scope.navs().get();

        // Owner FQN priority: scope's class hint (relops thread it via
        // bindFirstWithClass), else the bound spec's static type.
        String ownerFqn = scope.envClass().get(rowVar.name());
        if (ownerFqn == null) {
            ownerFqn = classFqnOf(scope.env().get(rowVar.name()));
        }
        if (ownerFqn == null) return null;
        TypedSpec ownerBody = inlineClassFetch(ownerFqn);
        if (ownerBody == null) return null;

        Navigation nav = null;
        for (int i = 0; i < hopCount; i++) {
            String segment = path.get(i);
            List<String> prefix = List.copyOf(path.subList(0, i + 1));

            Navigation existing = navs.get(prefix);
            if (existing == null) {
                // Direct alias lookup on the owner body's RowIndex —
                // the assoc cols for this class are already indexed
                // there by {@link #overlayExtends} during {@link #buildIndex}.
                TypedAssociationExtendCol assocCol = buildIndex(ownerBody).assocCol(segment);
                if (assocCol == null) return null;

                String targetFqn = assocCol.targetClassFqn();
                TypedSpec rightBody = inlineClassFetch(targetFqn);
                if (rightBody == null) return null;     // memoed null = cycle

                TypedSpec rewrittenRight = rewrite(rightBody, scope.enterMapping(targetFqn));
                TypedLambda cond = assocCol.hops().get(assocCol.hops().size() - 1).condition();
                TypedParam param = new TypedParam(
                        navs.freshParamName(),
                        new Type.ClassType(targetFqn),
                        com.gs.legend.model.m3.Multiplicity.ZERO_OR_ONE);
                existing = new Navigation(prefix, param, rewrittenRight, cond, targetFqn);
                navs.put(prefix, existing);
            }
            nav = existing;
            // Walk into the next hop's owner: that's this hop's target
            // class. inlineClassFetch is memoised, so this is a map hit.
            ownerBody = inlineClassFetch(nav.targetClassFqn());
            if (ownerBody == null) return null;
        }
        if (nav == null) return null;

        // Emit a leaf access on the deepest joined param. Re-run
        // rewritePropertyAccess with the param bound so the leaf-PA
        // arm resolves physicalColumn via buildIndex on the right body.
        TypedVariable newSrc = new TypedVariable(nav.param().name(), rowVar.role(), rowVar.info());
        TypedPropertyAccess direct = new TypedPropertyAccess(
                newSrc, pa.property(),
                Optional.empty(),  // path consumed
                Optional.empty(),  // physicalColumn populated by leaf PA arm
                pa.info());
        Scope withRight = scope.bind(nav.param().name(), nav.rightBody());
        return rewritePropertyAccess(direct, withRight);
    }

    /**
     * Class FQN of a TypedSpec whose info type is a {@link Type.ClassType},
     * else null.
     */
    private String classFqnOf(TypedSpec spec) {
        if (spec == null) return null;
        Type t = spec.info().type();
        if (t instanceof Type.ClassType ct) return ct.qualifiedName();
        return null;
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

            // Cast preserves row shape (the cast operator narrows the
            // Pure type but doesn't restructure the relation's columns).
            case TypedCast c -> buildIndex(c.expr());

            // Both branches of an if-expression typecheck to the same
            // type, so they share row shape. Read either side.
            case TypedIf i -> buildIndex(i.thenBranch());

            // A relation-valued lambda's row shape is its body's last
            // expression's row shape (the result of the lambda).
            case TypedLambda l -> l.body().isEmpty()
                    ? new RowIndex(Map.of())
                    : buildIndex(l.body().get(l.body().size() - 1));

            // A block's value is its terminal statement (let-bindings
            // for side effects/locals; result is the last spec).
            case TypedBlock b -> b.stmts().isEmpty()
                    ? new RowIndex(Map.of())
                    : buildIndex(b.stmts().get(b.stmts().size() - 1));

            // Variables and native calls don't expose a sub-AST to
            // recurse into; their static {@link ExpressionType#type()}
            // is the schema source. If the type isn't a Relation,
            // there's nothing to index — return empty rather than
            // throwing, so a non-relation $var or scalar native call
            // sitting where buildIndex was speculatively called
            // (e.g. during a deeper resolution) doesn't crash.
            case TypedVariable v -> v.info().type() instanceof Type.Relation
                    ? indexFromRelationSchema(v.info(), "TypedVariable")
                    : new RowIndex(Map.of());
            case TypedNativeCall nc -> nc.info().type() instanceof Type.Relation
                    ? indexFromRelationSchema(nc.info(), "TypedNativeCall " + nc.func().name())
                    : new RowIndex(Map.of());

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
     * Layer extend columns onto a base index. Scalar/traverse extends
     * contribute alias→physical-column entries; window extends name
     * themselves; association extends populate {@code assocCols} for
     * Rule 3 to read directly without re-walking the body.
     */
    private RowIndex overlayExtends(RowIndex base, List<TypedExtendCol> extensions) {
        Map<String, String> ptc = new LinkedHashMap<>(base.byAlias());
        Map<String, TypedAssociationExtendCol> assocs = new LinkedHashMap<>(base.assocCols());
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
                // Association extends don't add scalar columns; surface
                // them via {@code assocCols} so Rule 3 can find the
                // navigation by alias in O(1).
                case TypedAssociationExtendCol a -> assocs.put(a.alias(), a);
                case TypedEmbeddedExtendCol e  -> { /* Rule 3 territory */ }
            }
        }
        return new RowIndex(ptc, assocs);
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

    // ==================== Rule 3: association → explicit join ====================

    /**
     * One association navigation discovered during a relop's lambda
     * walk. At drain time it becomes:
     *
     * <ul>
     *   <li>a {@link TypedJoin} wrapping the relop's source
     *       ({@code left = stacked-src, right = rightBody, on = condition}),
     *       and</li>
     *   <li>a {@link TypedParam} appended to the relop's lambda's
     *       parameter list, bound to the join's right side.</li>
     * </ul>
     *
     * The bag exists only because {@link TypedJoin}'s {@code left} is
     * not known until prior hops are stacked — eager construction
     * isn't possible. The fields are the AST nodes we'll splice in,
     * minus the {@code left} we don't know yet.
     *
     * @param prefix          The cumulative path prefix this hop covers
     *                        (e.g. {@code [firm, address]} for the
     *                        second hop of {@code $x.firm.address}).
     *                        Used as the {@link Navigations} dedup key.
     * @param param           Lambda param that will be appended at drain
     *                        time and bound to this hop's right side.
     * @param rightBody       Pre-rewrite right relation (this hop's
     *                        target class mapping body).
     * @param condition       The 2-param join condition from the assoc
     *                        col's last hop.
     * @param targetClassFqn  FQN of the destination class (read straight
     *                        from {@link TypedAssociationExtendCol#targetClassFqn}
     *                        — no archaeology).
     */
    private record Navigation(
            List<String> prefix,
            TypedParam param,
            TypedSpec rightBody,
            TypedLambda condition,
            String targetClassFqn
    ) {}

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
