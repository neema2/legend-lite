package com.gs.legend.compiler;

import com.gs.legend.compiled.CompiledExpression;
import com.gs.legend.compiled.CompiledFunction;
import com.gs.legend.compiler.typed.*;
import com.gs.legend.model.ModelContext;
import com.gs.legend.model.m3.PureClass;
import com.gs.legend.model.mapping.ClassMapping;
import com.gs.legend.model.mapping.PureClassMapping;
import com.gs.legend.model.mapping.RelationalMapping;
import com.gs.legend.model.store.PropertyMapping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
    private final Map<String, InlinedClass> classMemo = new HashMap<>();
    private final Set<String> resolving = new HashSet<>();

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
        Resolved rewritten = rewrite(hir, Scope.empty());
        return wrapImplicitSerializeIfNeeded(rewritten.node());
    }

    // ==================== Walk state ====================

    /**
     * Per-walk substitution environment.
     *
     * <ul>
     *   <li>{@code env} — lambda-bound variable name → row schema. Each
     *       row schema is a logical→physical column map for the alias's
     *       row shape. Rule 2 reads this to β-substitute property names.
     *       Multi-param lambdas (after Rule 3 installs joins) bind one
     *       entry per param.</li>
     *   <li>{@code pendingJoins} — joins collected from path-bearing
     *       property accesses inside the current relational scope. Drained
     *       upstream of the enclosing relational operator at scope exit.
     *       Mirrors {@code NavScope}'s per-scope behavior.</li>
     * </ul>
     */
    record Scope(Map<String, RowSchema> env, List<PendingJoin> pendingJoins) {
        static Scope empty() {
            return new Scope(Map.of(), List.of());
        }

        Scope bind(String name, RowSchema schema) {
            Map<String, RowSchema> next = new HashMap<>(env);
            next.put(name, schema);
            return new Scope(next, pendingJoins);
        }

        Scope withFreshJoinScope() {
            return new Scope(env, new ArrayList<>());
        }
    }

    /**
     * Logical→physical row schema for one alias / lambda parameter.
     *
     * <p>Lives only inside {@link Scope#env}; never persists past
     * {@link #resolve} return. Synth body's PM extends contribute their
     * {@code (alias → physicalCol)} entry into this map during Rule 1
     * inlining; user-query extends (Rule 2's
     * {@link #rewriteExtend}) add their alias entries as
     * {@code (alias → alias)}.
     */
    record RowSchema(LinkedHashMap<String, String> propToCol,
                     Map<String, JoinChain> joins) {
        static RowSchema identity(List<String> cols) {
            LinkedHashMap<String, String> m = new LinkedHashMap<>();
            for (String c : cols) m.put(c, c);
            return new RowSchema(m, Map.of());
        }
    }

    /**
     * One pending join collected during a scalar walk. Drained upstream
     * of the enclosing relational operator. The {@code prefix} key
     * deduplicates joins along the same path so two
     * {@code $p.firm.legalName} and {@code $p.firm.id} accesses share
     * one join.
     */
    record PendingJoin(String prefix, TypedSpec join, RowSchema targetSchema) {}

    /**
     * Pre-resolved join chain for an association on a row's class.
     * Computed once during Rule 1 inlining (from the synth body's
     * association/embedded extend cols) and consumed by Rule 3 to
     * produce {@link TypedJoin} nodes. Replaces today's
     * {@code StoreResolution.JoinResolution} sealed hierarchy.
     *
     * <p>TODO: variants for FK / embedded / structArrayUnnest /
     * otherwise. Likely a sealed interface mirroring
     * {@link StoreResolution.JoinResolution} but containing TypedSpec
     * fragments instead of resolution records.
     */
    sealed interface JoinChain
            permits JoinChain.FkJoin, JoinChain.Embedded, JoinChain.StructArrayUnnest, JoinChain.Otherwise {
        record FkJoin(String targetTable, TypedSpec joinCondition,
                      String sourceParam, String targetParam,
                      RowSchema targetSchema, boolean toMany) implements JoinChain {}
        record Embedded(Map<String, String> subCols) implements JoinChain {}
        record StructArrayUnnest(String arrayProperty, List<String> fields) implements JoinChain {}
        record Otherwise(Map<String, String> embeddedSubCols, FkJoin fallback) implements JoinChain {}
    }

    /** Result of {@link #inlineClassFetch}. */
    record InlinedClass(TypedSpec body, RowSchema seedSchema) {}

    /**
     * Result of a single {@link #rewrite} call: the rewritten node and
     * its row schema.
     *
     * <p>Relational expressions return a non-null schema; scalars
     * return {@code null}. Each per-op rewriter computes its output
     * schema from its inputs (source schemas, alias lists) and returns
     * it alongside the rebuilt node so callers can bind lambda params
     * without a separate read-only walk.
     *
     * <p>This is the single carrier of resolution information during
     * the rewrite. After {@link #resolve} returns, schemas are
     * discarded — the AST itself carries the resolved info forward
     * (physical column names on {@link TypedPropertyAccess}, explicit
     * {@link TypedJoin} nodes, inlined synth bodies).
     */
    record Resolved(TypedSpec node, RowSchema schema) {}

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
    private InlinedClass inlineClassFetch(String classFqn) {
        InlinedClass memo = classMemo.get(classFqn);
        if (memo != null) return memo;

        if (resolving.contains(classFqn)) {
            // Cycle: target class is in progress higher in the stack
            // (self-join / back-reference). Return a stub body of just the
            // target's TypedTableReference plus an empty seed schema.
            // Mirrors today's {@code shallowResolution}: the stub exposes
            // the class's PMs as identity-mapped row columns; further
            // navigations off the cycle target re-enter inlineClassFetch
            // and hit either the memo or the cycle guard again.
            // TODO: build the actual stub once we wire ModelContext.findClass
            // and RelationalMapping.identity. For now, throw — no cycle test
            // case exists yet in the parity fixture.
            throw new UnsupportedOperationException(
                    "TODO: cycle stub for " + classFqn
                            + " (port today's shallowResolution: TypedTableReference body + identity PM seed schema)");
        }

        resolving.add(classFqn);
        try {
            // Look up the synth body produced by MappingNormalizer. The
            // mapping function is keyed by class FQN in the compiled
            // dependencies; its body is a TypedSpec subtree representing
            // the physical materialization of {@code Class.all()}.
            CompiledFunction cf = typeResult.dependencies().mappingFunctions().get(classFqn);
            if (cf == null) {
                throw new IllegalStateException(
                        "no compiled mapping function for class fetch: " + classFqn);
            }
            TypedSpec body = cf.body().hir();

            // Phase A seed RowSchema: from the active mapping's PMs.
            // Mirrors today's MappingResolver.resolveRelational lines 668-678
            // / resolveM2M lines 716-732 — same data, packaged as RowSchema.
            //
            // TODO Phase B: walk the synth body's TypedExtend cols to layer
            // per-extend overrides:
            //   - scalar/window/traverse → alias \u2192 column extracted from $r.COL
            //   - association → JoinChain.FkJoin in seedSchema.joins
            //   - embedded → JoinChain.Embedded in seedSchema.joins
            //   - struct-array → JoinChain.StructArrayUnnest in seedSchema.joins
            // Plus prune scalar/window/traverse cols whose alias \u2209
            // classPropertyAccesses[classFqn]. Today's MR does this in
            // resolveMappingFunction; we'll port that walk in Phase B.
            ClassMapping mapping = mappings.findClassMapping(classFqn).orElse(null);
            RowSchema seed = switch (mapping) {
                case RelationalMapping rm -> seedFromRelational(rm);
                case PureClassMapping pcm -> seedFromM2M(pcm);
                case null -> new RowSchema(new LinkedHashMap<>(), Map.of());
            };

            InlinedClass result = new InlinedClass(body, seed);
            classMemo.put(classFqn, result);
            return result;
        } finally {
            resolving.remove(classFqn);
        }
    }

    /**
     * Phase A seed for a relational class: simple PMs → propToCol.
     *
     * <p>Mirrors today's MappingResolver.resolveRelational lines 668-678.
     * Join-chain PMs name themselves (the traverse extend in the synth
     * body provides the actual physical column for those during Phase B).
     */
    private RowSchema seedFromRelational(RelationalMapping rm) {
        LinkedHashMap<String, String> propToCol = new LinkedHashMap<>();
        for (PropertyMapping pm : rm.propertyMappings()) {
            String prop = pm.propertyName();
            String col = pm.hasJoinChain() ? pm.propertyName() : pm.columnName();
            propToCol.put(prop, col);
        }
        return new RowSchema(propToCol, Map.of());
    }

    /**
     * Phase A seed for an M2M class: inherit upstream's propToCol.
     *
     * <p>Mirrors today's MappingResolver.resolveM2M lines 716-732. The
     * synth body's TypedExtend cols (Phase B) layer per-property
     * overrides on top — bare {@code $src.prop} bodies passthrough
     * upstream entries, transforms replace them.
     */
    private RowSchema seedFromM2M(PureClassMapping pcm) {
        String upstreamFqn = canonicalize(pcm.sourceClassName());
        InlinedClass upstream = inlineClassFetch(upstreamFqn);
        LinkedHashMap<String, String> propToCol =
                new LinkedHashMap<>(upstream.seedSchema().propToCol());
        return new RowSchema(propToCol, Map.of());
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
    private Resolved rewrite(TypedSpec node, Scope scope) {
        return switch (node) {

            // ---------- Rule 1: class fetch ----------
            // Mirrors: SourceLowering.lower(TypedGetAll) — replaced by
            // splice-and-recurse here. The spliced body's schema is
            // the class's seed schema (computed by inlineClassFetch);
            // we recurse on the body, but pin the schema to the seed
            // since the body's own walk doesn't know what class it
            // came from.
            case TypedGetAll ga -> {
                InlinedClass inlined = inlineClassFetch(ga.className());
                Resolved inner = rewrite(inlined.body(), scope);
                yield new Resolved(inner.node(), inlined.seedSchema());
            }

            // ---------- Relation source terminals ----------
            // Without a back-pointer to the originating class, these
            // carry a null schema post-rewrite. They appear at the
            // bottom of an inlined synth body; their schema is the
            // class's seed schema, pinned by the TypedGetAll arm above.
            case TypedTableReference n -> new Resolved(n, null);
            case TypedTdsLiteral n -> new Resolved(n, null);
            case TypedSourceUrl n -> new Resolved(n, null);

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
            case TypedVariable v -> new Resolved(v, null);
            case TypedLambda lam -> new Resolved(rewriteLambda(lam, scope), null);
            case TypedIf n -> rewriteIf(n, scope);
            case TypedLet n -> rewriteLet(n, scope);
            case TypedBlock n -> rewriteBlock(n, scope);
            case TypedMatch n -> rewriteMatch(n, scope);
            case TypedCast n -> rewriteCast(n, scope);
            case TypedCollection n -> rewriteCollection(n, scope);
            case TypedNewInstance n -> rewriteNewInstance(n, scope);
            case TypedStructExtract n -> rewriteStructExtract(n, scope);
            case TypedNativeCall n -> rewriteNativeCall(n, scope);
            case TypedEval n -> new Resolved(n, null);

            // ---------- Rule 0: user call (TODO: fold UserCallInliner) ----------
            case TypedUserCall uc -> throw new IllegalStateException(
                    "TODO: fold UserCallInliner into MR (plan §Unified Inliner). "
                            + "Currently UserCallInliner runs as a separate prologue. "
                            + "Got: " + uc.functionFqn());

            // ---------- Leaves ----------
            case TypedCInteger n -> new Resolved(n, null);
            case TypedCFloat n -> new Resolved(n, null);
            case TypedCDecimal n -> new Resolved(n, null);
            case TypedCString n -> new Resolved(n, null);
            case TypedCBoolean n -> new Resolved(n, null);
            case TypedCDateTime n -> new Resolved(n, null);
            case TypedCStrictDate n -> new Resolved(n, null);
            case TypedCStrictTime n -> new Resolved(n, null);
            case TypedCLatestDate n -> new Resolved(n, null);
            case TypedCByteArray n -> new Resolved(n, null);
            case TypedEnumValue n -> new Resolved(n, null);
            case TypedPackageableRef n -> new Resolved(n, null);
        };
    }

    // ==================== Per-op rewriters ====================
    //
    // Each method mirrors the corresponding lowering rule's bindVar
    // semantics. Citations point at the lowering file + line number.
    // Most are TODO until we work through them in order.

    /**
     * Binds the first parameter of a single-param lambda to a row schema.
     * No-op if the lambda has zero params or schema is null.
     */
    private Scope bindFirst(Scope scope, TypedLambda lam, RowSchema schema) {
        if (schema == null || lam.parameters().isEmpty()) return scope;
        return scope.bind(lam.parameters().get(0).name(), schema);
    }

    private Scope bindJoinCondParams(Scope scope, TypedLambda cond, RowSchema leftSchema, RowSchema rightSchema) {
        Scope s = scope;
        if (leftSchema != null && cond.parameters().size() >= 1) {
            s = s.bind(cond.parameters().get(0).name(), leftSchema);
        }
        if (rightSchema != null && cond.parameters().size() >= 2) {
            s = s.bind(cond.parameters().get(1).name(), rightSchema);
        }
        return s;
    }

    /** Mirrors FilterLowering.java:89. Schema: pass-through. */
    private Resolved rewriteFilter(TypedFilter n, Scope scope) {
        Resolved srcR = rewrite(n.source(), scope);
        Scope inner = bindFirst(scope, n.predicate(), srcR.schema());
        TypedLambda pred = rewriteLambda(n.predicate(), inner);
        return new Resolved(new TypedFilter(srcR.node(), pred, n.def(), n.info()), srcR.schema());
    }

    /** Mirrors SortLimitLowering.java:64. Schema: pass-through. */
    private Resolved rewriteSort(TypedSort n, Scope scope) {
        Resolved srcR = rewrite(n.source(), scope);
        List<TypedSortKey> keys = n.keys().stream()
                .map(k -> rewriteSortKey(k, scope, srcR.schema()))
                .toList();
        return new Resolved(new TypedSort(srcR.node(), keys, n.def(), n.info()), srcR.schema());
    }

    private TypedSortKey rewriteSortKey(TypedSortKey k, Scope scope, RowSchema srcSchema) {
        return switch (k) {
            case TypedColumnSortKey c -> c;
            case TypedExpressionSortKey e ->
                    new TypedExpressionSortKey(
                            rewriteLambda(e.keyFn(), bindFirst(scope, e.keyFn(), srcSchema)),
                            e.direction());
        };
    }

    private Resolved rewriteSlice(TypedSlice n, Scope scope) {
        Resolved srcR = rewrite(n.source(), scope);
        return new Resolved(
                new TypedSlice(srcR.node(), n.offset(), n.limit(), n.def(), n.info()),
                srcR.schema());
    }

    private Resolved rewriteDistinct(TypedDistinct n, Scope scope) {
        Resolved srcR = rewrite(n.source(), scope);
        return new Resolved(
                new TypedDistinct(srcR.node(), n.columns(), n.def(), n.info()),
                srcR.schema());
    }

    private Resolved rewriteFlatten(TypedFlatten n, Scope scope) {
        Resolved srcR = rewrite(n.source(), scope);
        // TODO: Flatten changes the row shape (collection → element).
        // Output schema is element type's columns. For now, pass-through.
        return new Resolved(
                new TypedFlatten(srcR.node(), n.column(), n.def(), n.info()),
                srcR.schema());
    }

    private Resolved rewriteRename(TypedRename n, Scope scope) {
        Resolved srcR = rewrite(n.source(), scope);
        RowSchema outSchema = srcR.schema() == null ? null : applyRenames(srcR.schema(), n.renames());
        return new Resolved(
                new TypedRename(srcR.node(), n.renames(), n.def(), n.info()),
                outSchema);
    }

    private RowSchema applyRenames(RowSchema src, List<ColRename> renames) {
        LinkedHashMap<String, String> ptc = new LinkedHashMap<>(src.propToCol());
        for (ColRename r : renames) {
            String phys = ptc.remove(r.from());
            if (phys != null) ptc.put(r.to(), phys);
        }
        return new RowSchema(ptc, src.joins());
    }

    private Resolved rewriteConcatenate(TypedConcatenate n, Scope scope) {
        Resolved lR = rewrite(n.left(), scope);
        Resolved rR = rewrite(n.right(), scope);
        // Output schema: left's (assumed compatible).
        return new Resolved(
                new TypedConcatenate(lR.node(), rR.node(), n.def(), n.info()),
                lR.schema());
    }

    private Resolved rewriteFold(TypedFold n, Scope scope) {
        Resolved srcR = rewrite(n.source(), scope);
        TypedLambda red = rewriteLambda(n.reducer(), bindFirst(scope, n.reducer(), srcR.schema()));
        Resolved initR = rewrite(n.init(), scope);
        // Fold reduces a relation to a scalar; output schema is null.
        return new Resolved(
                new TypedFold(srcR.node(), red, initR.node(), n.strategy(), n.def(), n.info()),
                null);
    }

    private Resolved rewriteMap(TypedMap n, Scope scope) {
        Resolved srcR = rewrite(n.source(), scope);
        TypedLambda m = rewriteLambda(n.mapper(), bindFirst(scope, n.mapper(), srcR.schema()));
        // TODO: output schema depends on mapper's body shape.
        return new Resolved(
                new TypedMap(srcR.node(), m, n.def(), n.info()),
                null);
    }

    /** Mirrors ProjectLowering.java:59. Output: identity over aliases (TDS). */
    private Resolved rewriteProject(TypedProject n, Scope scope) {
        Resolved srcR = rewrite(n.source(), scope);
        Scope lambdaScope = scope; // each projection lambda binds independently
        LinkedHashMap<String, String> outPtc = new LinkedHashMap<>();
        List<TypedProjectionCol> cols = n.projections().stream()
                .map(p -> {
                    outPtc.put(p.alias(), p.alias());
                    return new TypedProjectionCol(
                            p.alias(),
                            rewriteLambda(p.expression(), bindFirst(lambdaScope, p.expression(), srcR.schema())),
                            p.associationPath());
                })
                .toList();
        return new Resolved(
                new TypedProject(srcR.node(), cols, n.def(), n.info()),
                new RowSchema(outPtc, Map.of()));
    }

    /** Mirrors ExtendLowering.java:228, :295. Output: source's + extends. */
    private Resolved rewriteExtend(TypedExtend n, Scope scope) {
        Resolved srcR = rewrite(n.source(), scope);
        List<TypedExtendCol> exts = n.extensions().stream()
                .map(c -> rewriteExtendCol(c, scope, srcR.schema()))
                .toList();
        LinkedHashMap<String, String> outPtc = srcR.schema() == null
                ? new LinkedHashMap<>()
                : new LinkedHashMap<>(srcR.schema().propToCol());
        for (TypedExtendCol c : exts) outPtc.put(c.alias(), c.alias());
        Map<String, JoinChain> joins = srcR.schema() == null ? Map.of() : srcR.schema().joins();
        return new Resolved(
                new TypedExtend(srcR.node(), n.traversalSpecs(), exts, n.def(), n.info()),
                new RowSchema(outPtc, joins));
    }

    private TypedExtendCol rewriteExtendCol(TypedExtendCol c, Scope scope, RowSchema srcSchema) {
        return switch (c) {
            case TypedScalarExtendCol s ->
                    new TypedScalarExtendCol(s.alias(),
                            rewriteLambda(s.expression(), bindFirst(scope, s.expression(), srcSchema)),
                            s.returnType());
            case TypedWindowExtendCol w -> w; // TODO: recurse on funcArgs / reducer / outerWrapper
            case TypedTraverseExtendCol t ->
                    new TypedTraverseExtendCol(t.alias(), t.hops(),
                            rewriteLambda(t.expression(), bindFirst(scope, t.expression(), srcSchema)));
            case TypedAssociationExtendCol a -> a;
            case TypedEmbeddedExtendCol e -> e;
        };
    }

    /** Mirrors GroupByAggregateLowering.java:297, :324. Output: identity over aliases. */
    private Resolved rewriteGroupBy(TypedGroupBy n, Scope scope) {
        Resolved srcR = rewrite(n.source(), scope);
        List<TypedGroupKey> keys = n.keys().stream()
                .map(k -> rewriteGroupKey(k, scope, srcR.schema()))
                .toList();
        List<TypedAggCall> aggs = n.aggs().stream()
                .map(a -> rewriteAggCall(a, scope, srcR.schema()))
                .toList();
        LinkedHashMap<String, String> outPtc = new LinkedHashMap<>();
        for (TypedGroupKey k : keys) outPtc.put(k.alias(), k.alias());
        for (TypedAggCall a : aggs) outPtc.put(a.alias(), a.alias());
        return new Resolved(
                new TypedGroupBy(srcR.node(), keys, aggs, n.def(), n.info()),
                new RowSchema(outPtc, Map.of()));
    }

    private TypedGroupKey rewriteGroupKey(TypedGroupKey k, Scope scope, RowSchema srcSchema) {
        return switch (k) {
            case TypedColumnGroupKey c -> c;
            case TypedExpressionGroupKey e ->
                    new TypedExpressionGroupKey(
                            rewriteLambda(e.keyFn(), bindFirst(scope, e.keyFn(), srcSchema)),
                            e.alias());
            case TypedAssociationGroupKey a -> a;
        };
    }

    private TypedAggCall rewriteAggCall(TypedAggCall a, Scope scope, RowSchema srcSchema) {
        TypedLambda fn1 = a.fn1() == null ? null
                : rewriteLambda(a.fn1(), bindFirst(scope, a.fn1(), srcSchema));
        TypedLambda fn2 = a.fn2() == null ? null
                : rewriteLambda(a.fn2(), bindFirst(scope, a.fn2(), srcSchema));
        List<TypedSpec> extra = a.extraArgs().stream().map(x -> rewrite(x, scope).node()).toList();
        return new TypedAggCall(a.alias(), a.func(), fn1, fn2, extra, a.returnType(), a.castType());
    }

    private Resolved rewriteAggregate(TypedAggregate n, Scope scope) {
        Resolved srcR = rewrite(n.source(), scope);
        List<TypedAggCall> aggs = n.aggs().stream()
                .map(a -> rewriteAggCall(a, scope, srcR.schema()))
                .toList();
        LinkedHashMap<String, String> outPtc = new LinkedHashMap<>();
        for (TypedAggCall a : aggs) outPtc.put(a.alias(), a.alias());
        return new Resolved(
                new TypedAggregate(srcR.node(), aggs, n.def(), n.info()),
                new RowSchema(outPtc, Map.of()));
    }

    private Resolved rewritePivot(TypedPivot n, Scope scope) {
        Resolved srcR = rewrite(n.source(), scope);
        List<TypedAggCall> aggs = n.aggs().stream()
                .map(a -> rewriteAggCall(a, scope, srcR.schema()))
                .toList();
        // Output schema for pivot is dynamic (depends on data values);
        // approximate as identity over agg aliases for now.
        LinkedHashMap<String, String> outPtc = new LinkedHashMap<>();
        for (TypedAggCall a : aggs) outPtc.put(a.alias(), a.alias());
        return new Resolved(
                new TypedPivot(srcR.node(), n.pivotColumns(), aggs, n.def(), n.info()),
                new RowSchema(outPtc, Map.of()));
    }

    /** Mirrors JoinLowering.java:59-60. Output: TODO multi-alias. */
    private Resolved rewriteJoin(TypedJoin n, Scope scope) {
        Resolved lR = rewrite(n.left(), scope);
        Resolved rR = rewrite(n.right(), scope);
        Scope inner = bindJoinCondParams(scope, n.condition(), lR.schema(), rR.schema());
        TypedLambda cond = rewriteLambda(n.condition(), inner);
        // TODO: output schema is the multi-alias merge of left + right.
        return new Resolved(
                new TypedJoin(lR.node(), rR.node(), cond, n.joinType(), n.renames(), n.def(), n.info()),
                null);
    }

    /** Mirrors JoinLowering.java:131-132. Output: TODO multi-alias. */
    private Resolved rewriteAsOfJoin(TypedAsOfJoin n, Scope scope) {
        Resolved lR = rewrite(n.left(), scope);
        Resolved rR = rewrite(n.right(), scope);
        final Scope inner = bindJoinCondParams(scope, n.matchCondition(), lR.schema(), rR.schema());
        TypedLambda match = rewriteLambda(n.matchCondition(), inner);
        Optional<TypedLambda> key = n.keyCondition().map(lam -> rewriteLambda(lam, inner));
        return new Resolved(
                new TypedAsOfJoin(lR.node(), rR.node(), match, key, n.renames(), n.def(), n.info()),
                null);
    }

    private Resolved rewriteSelect(TypedSelect n, Scope scope) {
        Resolved srcR = rewrite(n.source(), scope);
        RowSchema outSchema = null;
        if (srcR.schema() != null) {
            LinkedHashMap<String, String> ptc = new LinkedHashMap<>();
            for (String c : n.cols()) {
                ptc.put(c, srcR.schema().propToCol().getOrDefault(c, c));
            }
            outSchema = new RowSchema(ptc, Map.of());
        }
        return new Resolved(
                new TypedSelect(srcR.node(), n.cols(), n.def(), n.info()),
                outSchema);
    }

    private Resolved rewriteZip(TypedZip n, Scope scope) {
        List<TypedSpec> srcs = n.sources().stream()
                .map(s -> rewrite(s, scope).node())
                .toList();
        // TODO: zip output schema is union of inputs by key columns.
        return new Resolved(
                new TypedZip(srcs, n.byKeys(), n.def(), n.info()),
                null);
    }

    private Resolved rewriteFrom(TypedFrom n, Scope scope) {
        Resolved srcR = rewrite(n.source(), scope);
        return new Resolved(
                new TypedFrom(srcR.node(), n.mapping(), n.runtime(), n.def(), n.info()),
                srcR.schema());
    }

    /** Rule 4 partial: graph fetch tree resolution. */
    private Resolved rewriteGraphFetch(TypedGraphFetch n, Scope scope) {
        Resolved srcR = rewrite(n.source(), scope);
        // TODO: rewrite Parsed→Resolved graph tree leaves using srcR.schema().
        return new Resolved(
                new TypedGraphFetch(srcR.node(), n.children(), n.def(), n.info()),
                srcR.schema());
    }

    private Resolved rewriteSerialize(TypedSerialize n, Scope scope) {
        Resolved srcR = rewrite(n.source(), scope);
        return new Resolved(
                new TypedSerialize(srcR.node(), n.format(), n.children(), n.def(), n.info()),
                null);
    }

    private Resolved rewriteSerializeImplicit(TypedSerializeImplicit n, Scope scope) {
        Resolved srcR = rewrite(n.source(), scope);
        return new Resolved(
                new TypedSerializeImplicit(srcR.node(), n.children()),
                null);
    }

    private Resolved rewriteWrite(TypedWrite n, Scope scope) {
        Resolved srcR = rewrite(n.source(), scope);
        Resolved destR = rewrite(n.destination(), scope);
        return new Resolved(
                new TypedWrite(srcR.node(), destR.node(), n.def(), n.info()),
                null);
    }

    // ----- Rule 2 + Rule 3: property access -----

    /**
     * Mirrors PropertyAccessLowering.lower. The single most important
     * arm.
     *
     * <p>Empty associationPath → Rule 2: look up
     * {@code scope.env[var.name].propToCol[pa.property]}; rebuild
     * {@code TypedPropertyAccess(pa.source, physicalColumn, ...)}.
     *
     * <p>Non-empty associationPath → Rule 3: walk hops from
     * {@code scope.env[var.name].joins}. For each non-embedded hop,
     * append a {@link PendingJoin} to {@code scope.pendingJoins} (the
     * enclosing relop drains them). Rewrite this access to a column
     * ref on the leaf hop's joined alias.
     *
     * <p>{@code Otherwise} dispatch happens here on a per-leaf basis:
     * if {@code pa.property ∈ embeddedSubCols}, embedded path; else FK
     * fallback.
     */
    private Resolved rewritePropertyAccess(TypedPropertyAccess n, Scope scope) {
        Resolved srcR = rewrite(n.source(), scope);
        // TODO: Rule 2 — empty associationPath: β-substitute property name to
        // physical column via scope.env[var].propToCol when src is a
        // TypedVariable bound in scope.env.
        // TODO: Rule 3 — non-empty associationPath: walk hops, append PendingJoin
        // entries to scope, rewrite to column ref on joined alias.
        return new Resolved(
                new TypedPropertyAccess(srcR.node(), n.property(), n.associationPath(), n.physicalColumn(), n.info()),
                null);
    }

    // ----- Lambda binding -----

    /**
     * Rewrites a lambda by recursing on each statement in its body. Param
     * binding is the responsibility of the relational op rewriting this
     * lambda — Filter, Project, etc., which should extend {@code scope.env}
     * with (paramName → source's RowSchema) BEFORE calling here.
     *
     * <p>For free lambdas (e.g., callback args to native functions where
     * the param doesn't bind to a relational source), the param binding is a
     * no-op and recursion proceeds.
     */
    private TypedLambda rewriteLambda(TypedLambda lam, Scope scope) {
        List<TypedSpec> body = lam.body().stream().map(s -> rewrite(s, scope).node()).toList();
        return new TypedLambda(lam.parameters(), body, lam.info());
    }

    // ----- Control flow -----

    private Resolved rewriteIf(TypedIf n, Scope scope) {
        TypedSpec c = rewrite(n.condition(), scope).node();
        TypedSpec t = rewrite(n.thenBranch(), scope).node();
        TypedSpec e = rewrite(n.elseBranch(), scope).node();
        return new Resolved(new TypedIf(c, t, e, n.info()), null);
    }

    private Resolved rewriteLet(TypedLet n, Scope scope) {
        TypedSpec v = rewrite(n.value(), scope).node();
        return new Resolved(new TypedLet(n.name(), v, n.info()), null);
    }

    private Resolved rewriteBlock(TypedBlock n, Scope scope) {
        List<TypedSpec> stmts = n.stmts().stream().map(s -> rewrite(s, scope).node()).toList();
        return new Resolved(new TypedBlock(stmts, n.info()), null);
    }

    private Resolved rewriteMatch(TypedMatch n, Scope scope) {
        TypedSpec subject = rewrite(n.subject(), scope).node();
        List<TypedLambda> cases = n.cases().stream().map(c -> rewriteLambda(c, scope)).toList();
        return new Resolved(new TypedMatch(subject, cases, n.info()), null);
    }

    private Resolved rewriteCast(TypedCast n, Scope scope) {
        TypedSpec e = rewrite(n.expr(), scope).node();
        return new Resolved(new TypedCast(e, n.targetType(), n.info()), null);
    }

    private Resolved rewriteCollection(TypedCollection n, Scope scope) {
        // TODO: at relation root with class-typed values, rewrite to TypedClassValues
        // (Open Question 3). For now, recurse structurally on values.
        List<TypedSpec> vals = n.values().stream().map(v -> rewrite(v, scope).node()).toList();
        return new Resolved(new TypedCollection(vals, n.info()), null);
    }

    private Resolved rewriteNewInstance(TypedNewInstance n, Scope scope) {
        // TODO: at relation root, rewrite to TypedClassValues (Open Question 3).
        // In scalar position (struct literal), recurse on values.
        LinkedHashMap<String, TypedSpec> vals = new LinkedHashMap<>();
        for (var e : n.values().entrySet()) vals.put(e.getKey(), rewrite(e.getValue(), scope).node());
        return new Resolved(new TypedNewInstance(n.className(), vals, n.info()), null);
    }

    private Resolved rewriteStructExtract(TypedStructExtract n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope).node();
        return new Resolved(new TypedStructExtract(src, n.field(), n.info()), null);
    }

    private Resolved rewriteNativeCall(TypedNativeCall n, Scope scope) {
        List<TypedSpec> args = n.args().stream().map(a -> rewrite(a, scope).node()).toList();
        return new Resolved(new TypedNativeCall(n.func(), args, n.info()), null);
    }

    // ==================== Rule 4: implicit-serialize wrap ====================

    /**
     * If the resolved root is class-typed and not already wrapped,
     * synthesize a {@link TypedSerializeImplicit} with a leaf-only
     * resolved graph tree. Leaf list comes from
     * {@code modelContext.findClass(rootFqn).properties()}; physical
     * columns come from the rewritten root's row schema.
     */
    private TypedSpec wrapImplicitSerializeIfNeeded(TypedSpec rewritten) {
        // TODO: detect class-typed root, look up properties, build
        // ResolvedGraphTree leaves, wrap.
        return rewritten;
    }
}
