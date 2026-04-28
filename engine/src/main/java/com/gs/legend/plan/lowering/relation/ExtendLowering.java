package com.gs.legend.plan.lowering.relation;

import com.gs.legend.compiler.StoreResolution;
import com.gs.legend.compiler.typed.TypedColumnSortKey;
import com.gs.legend.compiler.typed.TypedExpressionSortKey;
import com.gs.legend.compiler.typed.TypedExtend;
import com.gs.legend.compiler.typed.TypedExtendCol;
import com.gs.legend.compiler.typed.TypedLambda;
import com.gs.legend.compiler.typed.TypedOver;
import com.gs.legend.compiler.typed.TypedScalarExtendCol;
import com.gs.legend.compiler.typed.TypedSortKey;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.compiler.typed.TypedWindowExtendCol;
import com.gs.legend.plan.PlanGenNotPortedException;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.lowering.Relations;
import com.gs.legend.plan.sql.SqlRelation;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.ArrayList;
import java.util.List;

/**
 * Lowers {@link TypedExtend} (Pure {@code src->extend([~col:x|expr, ...])}) to
 * {@link SqlRelation.Extend} — a relation operator that appends scalar columns
 * to the source schema while preserving existing columns.
 *
 * <p><strong>Stage 4 scope</strong>: {@link TypedScalarExtendCol} only. The
 * other four {@link TypedExtendCol} variants (window / traverse / association /
 * embedded) require JOIN lifting or window-frame lowering and surface as
 * {@link PlanGenNotPortedException} tagged {@code extend:<variant>} until ported.
 *
 * <p>The {@code traversalHops} list on {@link TypedExtend} carries a resolved
 * join-chain that the extend body relies on when any of its columns navigate
 * associations. Stage 4 rejects non-empty hop lists so the gap is visible.
 */
public final class ExtendLowering {
    private ExtendLowering() {}

    public static SqlRelation lower(TypedExtend n, LoweringContext ctx) {
        // ExtendOverride: MappingResolver may stamp a cancellation marker on
        // this extend node (synth-body extends whose extension cols / hops
        // aren't used by the query). Two pruning levels:
        //   1. Fully-cancelled — skip the entire extend node. This also
        //      drops the {@code traversalHops}, eliminating spurious JOINs
        //      that would otherwise come from association extends.
        //   2. Partial — keep the extend, but filter individual cols by
        //      {@code isActive(alias)} below. (Hop-level filtering for
        //      partially-cancelled association extends isn't supported —
        //      synth-body produces one association per extend, so partial
        //      never applies to association cols today.)
        StoreResolution selfStore = ctx.storeFor(n);
        StoreResolution.ExtendOverride override =
                selfStore != null && selfStore.hasExtendOverride()
                        ? selfStore.extendOverride() : null;
        if (override != null && override.isFullyCancelled()) {
            return Lowerer.lowerRelation(n.source(), ctx);
        }
        SqlRelation src = Lowerer.lowerRelation(n.source(), ctx);
        SqlRelation aliased = Relations.ensureAliased(src, ctx);
        String srcAlias = aliased.alias();
        var store = ctx.storeFor(n.source());

        // Single NavScope holds (a) the resolved traversal specs pre-registered
        // as synthetic "[__hop_s_i]" entries and (b) any association navs
        // discovered while lowering scalar extend bodies below. One
        // Relations.install at rule exit weaves everything.
        //
        // Spec / hop semantics — mirrors legacy plangen
        // ({@code docs/reference/plangen-legacy-pre-port.java.txt:1981-1993}):
        //   for each spec:
        //     prevAlias = sourceAlias            // reset between specs
        //     for each hop in spec:
        //       allocate hopAlias
        //       JOIN target ON cond[prev->prevAlias, hop->hopAlias]
        //       prevAlias = hopAlias             // chain within spec
        //     terminalAliases.add(prevAlias)
        //
        // {@code aliasChain} is the lambda-param binding vector:
        // index 0 = source alias, index i+1 = terminal alias of spec i.
        // Multi-traverse lambdas like {@code {src, t1, t2 | …}} bind each
        // {@code t_i} to the corresponding spec's terminal.
        com.gs.legend.plan.lowering.NavScope scope = new com.gs.legend.plan.lowering.NavScope();
        List<String> aliasChain = new ArrayList<>(n.traversalSpecs().size() + 1);
        aliasChain.add(srcAlias);
        for (int s = 0; s < n.traversalSpecs().size(); s++) {
            var spec = n.traversalSpecs().get(s);
            String terminalAlias = srcAlias;
            for (int i = 0; i < spec.hops().size(); i++) {
                var hop = spec.hops().get(i);
                TypedLambda cond = hop.condition();
                if (cond.parameters().size() != 2) {
                    throw PlanGenNotPortedException.stage3(n, "extend:hop:non-binary-condition");
                }
                if (cond.body().isEmpty()) {
                    throw PlanGenNotPortedException.stage3(n, "extend:hop:empty-condition");
                }
                // Build a synthetic JoinResolution so we can reuse NavScope.navigate.
                StoreResolution.JoinResolution jr = new StoreResolution.JoinResolution(
                        hop.tableName(),
                        cond.parameters().get(0).name(),
                        cond.parameters().get(1).name(),
                        false,                                                // isToMany
                        cond.body().get(cond.body().size() - 1),               // joinCondition
                        java.util.Set.of(),                                    // sourceColumns (unused here)
                        null);                                                 // targetResolution
                // Per-spec scoped prefix keeps spec s's hop i distinct from
                // spec s'-1's hop i (otherwise NavScope dedups them).
                List<String> prefix = List.of("__hop_" + s + "_" + i);
                // Chain within spec: hop 0 parents to source (empty
                // parentPrefix); hop i > 0 parents to spec s's hop i-1.
                List<String> parentPrefix = i == 0
                        ? List.of()
                        : List.of("__hop_" + s + "_" + (i - 1));
                terminalAlias = scope.navigate(prefix, parentPrefix, jr, ctx.aliases());
            }
            aliasChain.add(terminalAlias);
        }

        List<SqlRelation.ExtendCol> cols = new ArrayList<>(n.extensions().size());
        for (TypedExtendCol col : n.extensions()) {
            // Per-col filter for partial overrides — matches legacy plangen
            // generateExtend lines 2005/2049 (`!extendOverride.isActive(cs.name()) continue`).
            if (override != null && !override.isActive(extendColAlias(col))) continue;
            SqlRelation.ExtendCol lowered = lowerExtendCol(col, aliasChain, store, ctx, n, scope);
            if (lowered != null) cols.add(lowered);
        }
        SqlRelation joined = Relations.install(aliased, srcAlias, store, scope, ctx);
        return new SqlRelation.Extend(joined, cols);
    }

    /** Output alias of any {@link TypedExtendCol} variant. */
    private static String extendColAlias(TypedExtendCol col) {
        return switch (col) {
            case TypedScalarExtendCol s         -> s.alias();
            case TypedWindowExtendCol w         -> w.alias();
            case com.gs.legend.compiler.typed.TypedTraverseExtendCol t -> t.alias();
            case com.gs.legend.compiler.typed.TypedAssociationExtendCol a -> a.alias();
            case com.gs.legend.compiler.typed.TypedEmbeddedExtendCol e -> e.alias();
        };
    }

    /**
     * Dispatches per {@link TypedExtendCol} variant.
     * <p>Returns {@code null} for variants that don't project a column —
     * {@link com.gs.legend.compiler.typed.TypedAssociationExtendCol} and
     * {@link com.gs.legend.compiler.typed.TypedEmbeddedExtendCol} are pure
     * join markers consumed by {@code MappingResolver}: the physical JOIN
     * (or same-alias embedded nav) is installed by MR into the mapping's
     * {@code sourceRelation} extends. PlanGen sees those as regular extends
     * below and never needs to emit anything from the marker col itself.
     */
    private static SqlRelation.ExtendCol lowerExtendCol(
            TypedExtendCol col, List<String> aliasChain, Object store,
            LoweringContext ctx, TypedExtend owner,
            com.gs.legend.plan.lowering.NavScope scope) {
        return switch (col) {
            case TypedScalarExtendCol sc -> new SqlRelation.ExtendCol(
                    sc.alias(),
                    lowerScalarLambda(sc.expression(), aliasChain, store, ctx, owner, "extend:scalar", scope));
            case TypedWindowExtendCol wc -> new SqlRelation.ExtendCol(
                    wc.alias(),
                    lowerWindowCol(wc, aliasChain, store, ctx, owner, scope));
            case com.gs.legend.compiler.typed.TypedAssociationExtendCol ignored -> null;
            case com.gs.legend.compiler.typed.TypedEmbeddedExtendCol ignored    -> null;
            case com.gs.legend.compiler.typed.TypedTraverseExtendCol t -> new SqlRelation.ExtendCol(
                    t.alias(),
                    lowerScalarLambda(t.expression(), aliasChain, store, ctx, owner, "extend:traverse", scope));
        };
    }

    /**
     * Structural lowering of a {@link TypedWindowExtendCol}:
     *
     * <ol>
     *   <li>Bind the HIR's {@code rowParamName} to the source alias as an
     *       {@link SqlExpr.Identifier} (carrying the extend's {@link StoreResolution}
     *       so nested property accesses resolve against the right store).</li>
     *   <li>Lower each {@code funcArg} as a scalar in that context.</li>
     *   <li>Lower the {@code over()} partition / order clauses against the
     *       source alias.</li>
     *   <li>Build the {@link SqlExpr.WindowCall}. The Pure native name passes
     *       through unchanged — {@link SqlExpr.WindowCall#toSql} delegates to
     *       {@link com.gs.legend.sqlgen.SQLDialect#renderFunction}, the sole
     *       source of truth for name translation (AGENTS.md invariant #3).</li>
     *   <li>If {@link TypedWindowExtendCol#outerWrapper()} is present, bind
     *       the gensymmed {@link TypedWindowExtendCol#holeName()} to the
     *       built window call in the {@link LoweringContext} and lower the
     *       wrapper. The {@link com.gs.legend.compiler.typed.TypedVariable}
     *       placed by the checker at the substitution point resolves to
     *       that SqlExpr via the standard variable-binding pathway.
     *       Otherwise return the window call directly.</li>
     * </ol>
     *
     * <p>No function-name classification lives here — the checker has already
     * decomposed the AST into {@code funcArgs} / {@code reducer} /
     * {@code outerWrapper}. This routine is purely structural.
     *
     * <p>The {@code over()} frame clause (ROWS/RANGE BETWEEN …), when present
     * on the HIR, is translated into a {@link SqlExpr.WindowFrame} via
     * {@link #lowerFrame(java.util.Optional)}.
     */
    private static SqlExpr lowerWindowCol(TypedWindowExtendCol wc, List<String> aliasChain,
                                          Object store, LoweringContext ctx,
                                          TypedExtend owner,
                                          com.gs.legend.plan.lowering.NavScope scope) {
        String baseAlias = aliasChain.get(0);
        StoreResolution storeRes = store instanceof StoreResolution sr ? sr : null;

        // Bind the row-param name (e.g. {@code "r"} from {@code {p,w,r|...}}) so
        // nested {@link TypedPropertyAccess} inside funcArgs resolves to columns
        // on the source alias. A bare identifier representing the row is a stand-in
        // — {@link com.gs.legend.plan.lowering.scalar.PropertyAccessLowering}
        // reads the bound store from the {@link LoweringContext.VarBinding}.
        LoweringContext bodyCtx = ctx.bindVar(wc.rowParamName(),
                new SqlExpr.Identifier(baseAlias), storeRes);

        // Lower the OVER clause first — purely structural, doesn't depend on
        // the window-call args.
        TypedOver over = wc.over();
        List<SqlExpr> partitionBy = new ArrayList<>(over.partitionBy().size());
        for (String col : over.partitionBy()) {
            partitionBy.add(new SqlExpr.Column(baseAlias, resolveColumnName(col, store)));
        }
        List<SqlExpr.OrderByTerm> orderBy = new ArrayList<>(over.orderBy().size());
        for (TypedSortKey key : over.orderBy()) {
            orderBy.add(lowerSortKey(key, aliasChain, store, ctx, owner, scope));
        }

        // Dispatch on resolved NativeFunctionDef identity via WindowBindings.
        // The binding receives the typed funcArgs + the row-bound context and
        // lowers each operand it consumes; this lets bindings whose overload
        // signature describes a structured arg (e.g. RowMapper<T,U>) inspect
        // the typed tree directly. See AGENTS.md invariant 2.
        SqlExpr windowCall = new SqlExpr.WindowCall(
                com.gs.legend.plan.lowering.natives.WindowBindings
                        .lookup(wc.func()).build(wc.funcArgs(), bodyCtx),
                partitionBy, orderBy, lowerFrame(over.frame()));

        // Substitute the window call into the outer wrapper (e.g.,
        // {@code round($$wh0, 2)}) by binding the gensymmed hole name to
        // the built window call. The TypedVariable the checker placed at
        // the substitution point resolves via VariableLowering.
        if (wc.outerWrapper().isPresent()) {
            var ow = wc.outerWrapper().get();
            LoweringContext wrapperCtx = bodyCtx.bindVar(ow.holeName(), windowCall, storeRes);
            return Lowerer.lowerScalar(ow.expr(), wrapperCtx);
        }
        return windowCall;
    }

    /**
     * Lowers a 1..N-param lambda against an alias chain. For single-hop extends
     * the chain is {@code [src]}; for traverse extends it's
     * {@code [src, hop1, hop2, …]} so each lambda parameter maps 1:1 to the
     * corresponding SQL alias (source first, then one per traversal hop).
     * Multi-traverse uses the same positional binding.
     */
    private static SqlExpr lowerScalarLambda(TypedLambda lam, List<String> aliasChain,
                                             Object store, LoweringContext ctx,
                                             TypedExtend owner, String hint,
                                             com.gs.legend.plan.lowering.NavScope scope) {
        if (lam.parameters().isEmpty()) {
            throw PlanGenNotPortedException.stage3(owner, hint + ":no-params");
        }
        if (lam.body().isEmpty()) {
            throw PlanGenNotPortedException.stage3(owner, hint + ":empty-body");
        }
        com.gs.legend.compiler.StoreResolution srcStore =
                (com.gs.legend.compiler.StoreResolution) store;
        LoweringContext inner = ctx.withNavScope(scope);
        for (int i = 0; i < lam.parameters().size(); i++) {
            String paramName = lam.parameters().get(i).name();
            String alias = i < aliasChain.size()
                    ? aliasChain.get(i)
                    : aliasChain.get(aliasChain.size() - 1);
            // Only param[0] is the source row; higher-indexed params are hop
            // table aliases without a StoreResolution (traverse hops carry
            // null targetResolution in this rule). Bind them without a store
            // so PropertyAccessLowering falls through to raw property names.
            com.gs.legend.compiler.StoreResolution bindStore = (i == 0) ? srcStore : null;
            inner = inner.bindVar(paramName, new SqlExpr.Identifier(alias), bindStore);
        }
        TypedSpec terminal = lam.body().get(lam.body().size() - 1);
        return Lowerer.lowerScalar(terminal, inner);
    }

    private static SqlExpr.OrderByTerm lowerSortKey(TypedSortKey k, List<String> aliasChain,
                                                    Object store, LoweringContext ctx,
                                                    TypedExtend owner,
                                                    com.gs.legend.plan.lowering.NavScope scope) {
        String baseAlias = aliasChain.get(0);
        return switch (k) {
            case TypedColumnSortKey c -> new SqlExpr.OrderByTerm(
                    new SqlExpr.Column(baseAlias, resolveColumnName(c.column(), store)),
                    c.direction().name(), nullOrderFor(c.direction().name()));
            case TypedExpressionSortKey e -> new SqlExpr.OrderByTerm(
                    lowerScalarLambda(e.keyFn(), aliasChain, store, ctx, owner, "extend:window:order-key", scope),
                    e.direction().name(), nullOrderFor(e.direction().name()));
        };
    }

    /**
     * Pure/Legend null-ordering convention for window ORDER BY:
     * DESC → {@code NULLS FIRST}, ASC → {@code NULLS LAST}. Matches legacy
     * PlanGenerator (line 2068) and is consistent across DuckDB / Postgres /
     * Snowflake default semantics made explicit in the emitted SQL.
     */
    private static String nullOrderFor(String direction) {
        return "DESC".equalsIgnoreCase(direction) ? "NULLS FIRST" : "NULLS LAST";
    }

    private static String resolveColumnName(String property, Object store) {
        if (!(store instanceof com.gs.legend.compiler.StoreResolution sr)) return property;
        String c = sr.columnFor(property);
        return c != null ? c : property;
    }

    /**
     * Translates a checker-side {@link com.gs.legend.compiler.typed.TypedFrame}
     * into a dialect-agnostic {@link SqlExpr.WindowFrame}. Empty when no frame
     * was declared on the {@code over()} clause.
     *
     * <p>Bound translation:
     * <ul>
     *   <li>{@link com.gs.legend.compiler.typed.Unbounded} →
     *       {@link SqlExpr.UnboundedFrameBound} (UNBOUNDED PRECEDING / FOLLOWING
     *       decided at render time by start/end position).</li>
     *   <li>{@link com.gs.legend.compiler.typed.CurrentRow} →
     *       {@link SqlExpr.CurrentRowFrameBound}.</li>
     *   <li>{@link com.gs.legend.compiler.typed.Offset} → signed
     *       {@link SqlExpr.OffsetFrameBound} (Pure's offset convention: negative
     *       = PRECEDING, positive = FOLLOWING, zero = CURRENT ROW).</li>
     * </ul>
     */
    private static java.util.Optional<SqlExpr.WindowFrame> lowerFrame(
            java.util.Optional<com.gs.legend.compiler.typed.TypedFrame> frame) {
        if (frame.isEmpty()) return java.util.Optional.empty();
        var f = frame.get();
        SqlExpr.FrameType type = switch (f.type()) {
            case ROWS -> SqlExpr.FrameType.ROWS;
            case RANGE -> SqlExpr.FrameType.RANGE;
        };
        return java.util.Optional.of(new SqlExpr.WindowFrame(
                type, lowerFrameBound(f.start()), lowerFrameBound(f.end())));
    }

    private static SqlExpr.FrameBound lowerFrameBound(
            com.gs.legend.compiler.typed.TypedFrameBound b) {
        return switch (b) {
            case com.gs.legend.compiler.typed.Unbounded u -> new SqlExpr.UnboundedFrameBound();
            case com.gs.legend.compiler.typed.CurrentRow c -> new SqlExpr.CurrentRowFrameBound();
            case com.gs.legend.compiler.typed.Offset o -> new SqlExpr.OffsetFrameBound(o.value());
        };
    }
}
