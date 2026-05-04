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
        TypedSpec rewritten = rewrite(hir, Scope.empty());
        return wrapImplicitSerializeIfNeeded(rewritten);
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

    // ==================== Schema computation (per-op output schemas) ====================

    /**
     * Returns the row schema of a rewritten relational expression.
     *
     * <p>Per-op output-schema rules. Mirrors the lowering's row-shape
     * semantics. Returns {@code null} for scalar nodes.
     *
     * <p>This is the read-side counterpart to per-op rewrite logic. Each
     * relop rewriter calls this on its rewritten source(s) to find the
     * schema to bind lambda params to. The implementation walks
     * structurally — schemas are not memoized; computation is cheap
     * because most arms read from the inlined seed at the leaf and
     * propagate upward.
     */
    private RowSchema schemaOf(TypedSpec node) {
        return switch (node) {
            // Class-fetch (PRE-rewrite). The relop calling schemaOf
            // passes the pre-rewrite source so this arm fires before
            // Rule 1 splices it away. Returns the inlined class's seed
            // RowSchema directly — this is the key plumbing that lets
            // Rule 2 resolve property accesses against the class's PMs.
            case TypedGetAll ga -> inlineClassFetch(ga.className()).seedSchema();

            // Inlined leaves (POST-rewrite). After Rule 1 splices a
            // class fetch, the body's leaf is one of these. We don't
            // currently have a back-pointer to the class FQN; relops
            // that need to chain schema queries across already-rewritten
            // sources will return null until we tag inlined roots.
            // TODO: tag the splice root, or thread schema via
            // Result(node, schema) refactor.
            case TypedTableReference t -> null;
            case TypedTdsLiteral t -> null; // TODO: TDS-shape
            case TypedSourceUrl s -> null;  // TODO: parsed-row shape

            // Pass-through: schema unchanged from source.
            case TypedFilter n -> schemaOf(n.source());
            case TypedSlice n -> schemaOf(n.source());
            case TypedDistinct n -> schemaOf(n.source());
            case TypedSort n -> schemaOf(n.source());
            case TypedRename n -> renameSchema(schemaOf(n.source()), n.renames());

            // Project: identity over projection aliases (TDS-shaped).
            case TypedProject n -> {
                LinkedHashMap<String, String> ptc = new LinkedHashMap<>();
                for (TypedProjectionCol p : n.projections()) ptc.put(p.alias(), p.alias());
                yield new RowSchema(ptc, Map.of());
            }

            // Extend: source's + each extend's alias → alias.
            case TypedExtend n -> {
                RowSchema s = schemaOf(n.source());
                LinkedHashMap<String, String> ptc = new LinkedHashMap<>(
                        s == null ? new LinkedHashMap<>() : s.propToCol());
                for (TypedExtendCol c : n.extensions()) ptc.put(c.alias(), c.alias());
                yield new RowSchema(ptc, s == null ? Map.of() : s.joins());
            }

            // GroupBy / Aggregate / Pivot: identity over output aliases.
            case TypedGroupBy n -> aggOutputSchema(n.keys(), n.aggs());
            case TypedAggregate n -> aggOutputSchema(List.of(), n.aggs());
            case TypedPivot n -> aggOutputSchema(List.of(), n.aggs());

            // TODO: Join / AsOfJoin produce multi-alias schemas that
            // multi-param lambdas read separately; punt for now.
            case TypedJoin n -> null;
            case TypedAsOfJoin n -> null;

            case TypedSelect n -> {
                RowSchema s = schemaOf(n.source());
                if (s == null) yield null;
                LinkedHashMap<String, String> ptc = new LinkedHashMap<>();
                for (String c : n.cols()) {
                    String phys = s.propToCol().getOrDefault(c, c);
                    ptc.put(c, phys);
                }
                yield new RowSchema(ptc, Map.of());
            }

            // Operators that don't produce a relational row in the
            // schema-propagation sense (or where it's not needed for
            // lambda binding): return null.
            default -> null;
        };
    }

    private RowSchema renameSchema(RowSchema src, List<ColRename> renames) {
        if (src == null) return null;
        LinkedHashMap<String, String> ptc = new LinkedHashMap<>(src.propToCol());
        for (ColRename r : renames) {
            String phys = ptc.remove(r.from());
            if (phys != null) ptc.put(r.to(), phys);
        }
        return new RowSchema(ptc, src.joins());
    }

    private RowSchema aggOutputSchema(List<TypedGroupKey> keys, List<TypedAggCall> aggs) {
        LinkedHashMap<String, String> ptc = new LinkedHashMap<>();
        for (TypedGroupKey k : keys) ptc.put(k.alias(), k.alias());
        for (TypedAggCall a : aggs) ptc.put(a.alias(), a.alias());
        return new RowSchema(ptc, Map.of());
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

            // Mirrors: SourceLowering.lower(TypedGetAll) — replaced by
            // splice-and-recurse here; SourceLowering(TypedGetAll) becomes
            // a defensive throw post-MR.
            case TypedGetAll ga -> {
                InlinedClass inlined = inlineClassFetch(ga.className());
                // The inlined body is itself a TypedSpec subtree. Recurse
                // on it under the SAME scope so its internal property
                // accesses get rewritten. The body already has its own
                // top-level row alias bound via Rule-1 pruning; we extend
                // scope's env with that binding.
                // TODO: scope.bind(rowAlias, inlined.seedSchema()) before recursing.
                yield rewrite(inlined.body(), scope);
            }

            // ---------- Relation source terminals ----------

            // TypedTableReference / TypedTdsLiteral / TypedSourceUrl pass
            // through unchanged. Their row schema is identity (column name
            // = column name); whatever lambda binds to them gets that
            // identity schema in scope.env.
            case TypedTableReference n -> n;
            case TypedTdsLiteral n -> n;
            case TypedSourceUrl n -> n;

            // ---------- Pass-through relation operators (rewrite source + lambda) ----------

            // Mirrors: FilterLowering.lower (line 89:
            //   ctx.bindVar(paramName, paramBinding, outerStore))
            // Schema env: lambda's row binds to source's schema.
            case TypedFilter n -> rewriteFilter(n, scope);

            // Mirrors: SortLimitLowering.lower (line 64:
            //   ctx.bindVar(p, new SqlExpr.Identifier(alias), store))
            case TypedSort n -> rewriteSort(n, scope);

            case TypedSlice n -> rewriteSlice(n, scope);
            case TypedDistinct n -> rewriteDistinct(n, scope);
            case TypedFlatten n -> rewriteFlatten(n, scope);
            case TypedRename n -> rewriteRename(n, scope);
            case TypedConcatenate n -> rewriteConcatenate(n, scope);
            case TypedFold n -> rewriteFold(n, scope);
            case TypedMap n -> rewriteMap(n, scope);

            // Mirrors: ProjectLowering.lower (line 59:
            //   ctx.bindVar(paramName, new SqlExpr.Identifier(alias), store))
            // Output schema: identity over projection aliases (TDS).
            case TypedProject n -> rewriteProject(n, scope);

            // Mirrors: ExtendLowering.lower across scalar/window/traverse
            // extend cols (line 228, 295). User-query extends keep their
            // cols (no synth-body pruning here — that runs only inside
            // inlineClassFetch). Output schema = source's + each extend
            // col's (alias → alias).
            case TypedExtend n -> rewriteExtend(n, scope);

            // Mirrors: GroupByAggregateLowering.lower (lines 297, 324).
            // Output schema = identity over output aliases.
            case TypedGroupBy n -> rewriteGroupBy(n, scope);
            case TypedAggregate n -> rewriteAggregate(n, scope);
            case TypedPivot n -> rewritePivot(n, scope);

            // Mirrors: JoinLowering.lower (lines 59-60). Multi-alias
            // schema for the join's output; multi-param lambda binds each
            // param to its side's schema.
            case TypedJoin n -> rewriteJoin(n, scope);
            case TypedAsOfJoin n -> rewriteAsOfJoin(n, scope);

            case TypedSelect n -> rewriteSelect(n, scope);
            case TypedZip n -> rewriteZip(n, scope);
            case TypedFrom n -> rewriteFrom(n, scope);

            // ---------- Rule 4: graph fetch / serialize ----------

            // TypedGraphFetch's children (TypedGraphTree) become resolved
            // graph trees post-MR. The source is rewritten; tree leaves
            // get physical columns from the rewritten body's row schema.
            case TypedGraphFetch n -> rewriteGraphFetch(n, scope);

            // Wraps the source's rewrite. The final implicit-serialize
            // wrapping happens in {@link #wrapImplicitSerializeIfNeeded}.
            case TypedSerialize n -> rewriteSerialize(n, scope);
            case TypedSerializeImplicit n -> rewriteSerializeImplicit(n, scope);

            case TypedWrite n -> rewriteWrite(n, scope);

            // ---------- Rule 2 + Rule 3: property access ----------

            // Mirrors: PropertyAccessLowering.lower. With empty
            // associationPath → Rule 2: β-substitute property name to
            // physical column via scope.env. With non-empty path → Rule 3:
            // install pending joins and rewrite to a column ref on the
            // joined alias.
            case TypedPropertyAccess n -> rewritePropertyAccess(n, scope);

            // ---------- Bindings / scalar / control flow ----------

            // Variable: look up in env. If env's value is a row schema, this
            // is a relational variable — return the variable as-is (Rule 2
            // resolves the actual property access against env at the access
            // site). If not in env, the variable is a scalar binding from
            // outside — return as-is.
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

            // ---------- Rule 0: user call inlining (folded in) ----------

            // {@link TypedUserCall} and {@link TypedGetAll} are both
            // abstraction-expansion: a call site over a body. Plan
            // §"Architecture: Unified Inliner" folds them into one pass —
            // here. Today {@link UserCallInliner} runs as a separate
            // prologue; once V2 is feature-complete this arm subsumes it
            // and that class deletes.
            //
            // Mechanics:
            //   1. Look up function body via
            //      typeResult.dependencies().userFunctions().get(uc.functionFqn()).
            //   2. Build formals → actuals env: bind each parameter name
            //      to its rewritten argument (recurse on each actual under
            //      current scope first).
            //   3. α-rename the body via the kernel from HirRewriter
            //      (still useful — the visitor pattern dies, the kernel
            //      stays). Per-occurrence renames keep multiple call sites
            //      capture-safe.
            //   4. Recurse on the renamed body under the extended env.
            //
            // Today (transitional): {@link UserCallInliner} still runs
            // before MR, so this arm should never fire. Once V2 owns
            // everything, swap the throw for the splice logic.
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
    // semantics. Citations point at the lowering file + line number.
    // Most are TODO until we work through them in order.

    /** Mirrors FilterLowering.java:89. */
    private TypedSpec rewriteFilter(TypedFilter n, Scope scope) {
        RowSchema srcSchema = schemaOf(n.source());
        TypedSpec src = rewrite(n.source(), scope);
        TypedLambda pred = rewriteLambda(n.predicate(), bindFirst(scope, n.predicate(), srcSchema));
        return (src == n.source() && pred == n.predicate())
                ? n
                : new TypedFilter(src, pred, n.def(), n.info());
    }

    /**
     * Binds the first parameter of a single-param lambda to a row schema.
     * No-op if the lambda has zero params or schema is null.
     */
    private Scope bindFirst(Scope scope, TypedLambda lam, RowSchema schema) {
        if (schema == null || lam.parameters().isEmpty()) return scope;
        return scope.bind(lam.parameters().get(0).name(), schema);
    }

    /** Mirrors SortLimitLowering.java:64. */
    private TypedSpec rewriteSort(TypedSort n, Scope scope) {
        RowSchema srcSchema = schemaOf(n.source());
        TypedSpec src = rewrite(n.source(), scope);
        List<TypedSortKey> keys = n.keys().stream().map(k -> rewriteSortKey(k, scope, srcSchema)).toList();
        return new TypedSort(src, keys, n.def(), n.info());
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

    private TypedSpec rewriteSlice(TypedSlice n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);
        return src == n.source() ? n : new TypedSlice(src, n.offset(), n.limit(), n.def(), n.info());
    }

    private TypedSpec rewriteDistinct(TypedDistinct n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);
        return src == n.source() ? n : new TypedDistinct(src, n.columns(), n.def(), n.info());
    }

    private TypedSpec rewriteFlatten(TypedFlatten n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);
        return src == n.source() ? n : new TypedFlatten(src, n.column(), n.def(), n.info());
    }

    private TypedSpec rewriteRename(TypedRename n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);
        return src == n.source() ? n : new TypedRename(src, n.renames(), n.def(), n.info());
    }

    private TypedSpec rewriteConcatenate(TypedConcatenate n, Scope scope) {
        TypedSpec l = rewrite(n.left(), scope);
        TypedSpec r = rewrite(n.right(), scope);
        return (l == n.left() && r == n.right()) ? n
                : new TypedConcatenate(l, r, n.def(), n.info());
    }

    private TypedSpec rewriteFold(TypedFold n, Scope scope) {
        RowSchema srcSchema = schemaOf(n.source());
        TypedSpec src = rewrite(n.source(), scope);
        TypedLambda red = rewriteLambda(n.reducer(), bindFirst(scope, n.reducer(), srcSchema));
        TypedSpec init = rewrite(n.init(), scope);
        return (src == n.source() && red == n.reducer() && init == n.init()) ? n
                : new TypedFold(src, red, init, n.strategy(), n.def(), n.info());
    }

    private TypedSpec rewriteMap(TypedMap n, Scope scope) {
        RowSchema srcSchema = schemaOf(n.source());
        TypedSpec src = rewrite(n.source(), scope);
        TypedLambda m = rewriteLambda(n.mapper(), bindFirst(scope, n.mapper(), srcSchema));
        return (src == n.source() && m == n.mapper()) ? n
                : new TypedMap(src, m, n.def(), n.info());
    }

    /** Mirrors ProjectLowering.java:59. */
    private TypedSpec rewriteProject(TypedProject n, Scope scope) {
        RowSchema srcSchema = schemaOf(n.source());
        TypedSpec src = rewrite(n.source(), scope);
        List<TypedProjectionCol> cols = n.projections().stream()
                .map(p -> new TypedProjectionCol(
                        p.alias(),
                        rewriteLambda(p.expression(), bindFirst(scope, p.expression(), srcSchema)),
                        p.associationPath()))
                .toList();
        return new TypedProject(src, cols, n.def(), n.info());
    }

    /** Mirrors ExtendLowering.java:228, :295. */
    private TypedSpec rewriteExtend(TypedExtend n, Scope scope) {
        RowSchema srcSchema = schemaOf(n.source());
        TypedSpec src = rewrite(n.source(), scope);
        List<TypedExtendCol> exts = n.extensions().stream()
                .map(c -> rewriteExtendCol(c, scope, srcSchema))
                .toList();
        return new TypedExtend(src, n.traversalSpecs(), exts, n.def(), n.info());
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

    /** Mirrors GroupByAggregateLowering.java:297, :324. */
    private TypedSpec rewriteGroupBy(TypedGroupBy n, Scope scope) {
        RowSchema srcSchema = schemaOf(n.source());
        TypedSpec src = rewrite(n.source(), scope);
        List<TypedGroupKey> keys = n.keys().stream().map(k -> rewriteGroupKey(k, scope, srcSchema)).toList();
        List<TypedAggCall> aggs = n.aggs().stream().map(a -> rewriteAggCall(a, scope, srcSchema)).toList();
        return new TypedGroupBy(src, keys, aggs, n.def(), n.info());
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
        List<TypedSpec> extra = a.extraArgs().stream().map(x -> rewrite(x, scope)).toList();
        return new TypedAggCall(a.alias(), a.func(), fn1, fn2, extra, a.returnType(), a.castType());
    }

    private TypedSpec rewriteAggregate(TypedAggregate n, Scope scope) {
        RowSchema srcSchema = schemaOf(n.source());
        TypedSpec src = rewrite(n.source(), scope);
        List<TypedAggCall> aggs = n.aggs().stream().map(a -> rewriteAggCall(a, scope, srcSchema)).toList();
        return new TypedAggregate(src, aggs, n.def(), n.info());
    }

    private TypedSpec rewritePivot(TypedPivot n, Scope scope) {
        RowSchema srcSchema = schemaOf(n.source());
        TypedSpec src = rewrite(n.source(), scope);
        List<TypedAggCall> aggs = n.aggs().stream().map(a -> rewriteAggCall(a, scope, srcSchema)).toList();
        return new TypedPivot(src, n.pivotColumns(), aggs, n.def(), n.info());
    }

    /** Mirrors JoinLowering.java:59-60 (multi-param lambda). */
    private TypedSpec rewriteJoin(TypedJoin n, Scope scope) {
        RowSchema leftSchema = schemaOf(n.left());
        RowSchema rightSchema = schemaOf(n.right());
        TypedSpec l = rewrite(n.left(), scope);
        TypedSpec r = rewrite(n.right(), scope);
        Scope inner = bindJoinCondParams(scope, n.condition(), leftSchema, rightSchema);
        TypedLambda cond = rewriteLambda(n.condition(), inner);
        return new TypedJoin(l, r, cond, n.joinType(), n.renames(), n.def(), n.info());
    }

    /** Mirrors JoinLowering.java:131-132. */
    private TypedSpec rewriteAsOfJoin(TypedAsOfJoin n, Scope scope) {
        RowSchema leftSchema = schemaOf(n.left());
        RowSchema rightSchema = schemaOf(n.right());
        TypedSpec l = rewrite(n.left(), scope);
        TypedSpec r = rewrite(n.right(), scope);
        final Scope inner = bindJoinCondParams(scope, n.matchCondition(), leftSchema, rightSchema);
        TypedLambda match = rewriteLambda(n.matchCondition(), inner);
        Optional<TypedLambda> key = n.keyCondition().map(lam -> rewriteLambda(lam, inner));
        return new TypedAsOfJoin(l, r, match, key, n.renames(), n.def(), n.info());
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

    private TypedSpec rewriteSelect(TypedSelect n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);
        return src == n.source() ? n : new TypedSelect(src, n.cols(), n.def(), n.info());
    }

    private TypedSpec rewriteZip(TypedZip n, Scope scope) {
        List<TypedSpec> srcs = n.sources().stream().map(s -> rewrite(s, scope)).toList();
        return new TypedZip(srcs, n.byKeys(), n.def(), n.info());
    }

    private TypedSpec rewriteFrom(TypedFrom n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);
        return src == n.source() ? n : new TypedFrom(src, n.mapping(), n.runtime(), n.def(), n.info());
    }

    /** Rule 4 partial: graph fetch tree resolution. */
    private TypedSpec rewriteGraphFetch(TypedGraphFetch n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);
        // TODO: rewrite Parsed→Resolved graph tree leaves using src's row schema.
        // For now, pass children through unchanged.
        return src == n.source() ? n
                : new TypedGraphFetch(src, n.children(), n.def(), n.info());
    }

    private TypedSpec rewriteSerialize(TypedSerialize n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);
        return src == n.source() ? n
                : new TypedSerialize(src, n.format(), n.children(), n.def(), n.info());
    }

    private TypedSpec rewriteSerializeImplicit(TypedSerializeImplicit n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);
        return src == n.source() ? n : new TypedSerializeImplicit(src, n.children());
    }

    private TypedSpec rewriteWrite(TypedWrite n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);
        TypedSpec dest = rewrite(n.destination(), scope);
        return (src == n.source() && dest == n.destination()) ? n
                : new TypedWrite(src, dest, n.def(), n.info());
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
    private TypedSpec rewritePropertyAccess(TypedPropertyAccess n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);
        // TODO: Rule 2 — empty associationPath: β-substitute property name to
        // physical column via scope.env[var].propToCol.
        // TODO: Rule 3 — non-empty associationPath: walk hops, append PendingJoin
        // entries to scope, rewrite to column ref on joined alias.
        return src == n.source() ? n
                : new TypedPropertyAccess(src, n.property(), n.associationPath(), n.physicalColumn(), n.info());
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
        List<TypedSpec> body = lam.body().stream().map(s -> rewrite(s, scope)).toList();
        return new TypedLambda(lam.parameters(), body, lam.info());
    }

    // ----- Control flow -----

    private TypedSpec rewriteIf(TypedIf n, Scope scope) {
        TypedSpec c = rewrite(n.condition(), scope);
        TypedSpec t = rewrite(n.thenBranch(), scope);
        TypedSpec e = rewrite(n.elseBranch(), scope);
        return (c == n.condition() && t == n.thenBranch() && e == n.elseBranch()) ? n
                : new TypedIf(c, t, e, n.info());
    }

    private TypedSpec rewriteLet(TypedLet n, Scope scope) {
        TypedSpec v = rewrite(n.value(), scope);
        return v == n.value() ? n : new TypedLet(n.name(), v, n.info());
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
        TypedSpec e = rewrite(n.expr(), scope);
        return e == n.expr() ? n : new TypedCast(e, n.targetType(), n.info());
    }

    private TypedSpec rewriteCollection(TypedCollection n, Scope scope) {
        // TODO: at relation root with class-typed values, rewrite to TypedClassValues
        // (Open Question 3). For now, recurse structurally on values.
        List<TypedSpec> vals = n.values().stream().map(v -> rewrite(v, scope)).toList();
        return new TypedCollection(vals, n.info());
    }

    private TypedSpec rewriteNewInstance(TypedNewInstance n, Scope scope) {
        // TODO: at relation root, rewrite to TypedClassValues (Open Question 3).
        // In scalar position (struct literal), recurse on values.
        java.util.LinkedHashMap<String, TypedSpec> vals = new java.util.LinkedHashMap<>();
        for (var e : n.values().entrySet()) vals.put(e.getKey(), rewrite(e.getValue(), scope));
        return new TypedNewInstance(n.className(), vals, n.info());
    }

    private TypedSpec rewriteStructExtract(TypedStructExtract n, Scope scope) {
        TypedSpec src = rewrite(n.source(), scope);
        return src == n.source() ? n : new TypedStructExtract(src, n.field(), n.info());
    }

    private TypedSpec rewriteNativeCall(TypedNativeCall n, Scope scope) {
        List<TypedSpec> args = n.args().stream().map(a -> rewrite(a, scope)).toList();
        return new TypedNativeCall(n.func(), args, n.info());
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
