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
        SqlRelation src = Lowerer.lowerRelation(n.source(), ctx);
        SqlRelation aliased = Relations.ensureAliased(src, ctx);
        String srcAlias = aliased.alias();
        var store = ctx.storeFor(n.source());

        // Single NavScope holds (a) traversalHops pre-registered as synthetic
        // "[__hop_i]" entries with empty parentPrefix (so each hop's source
        // binds to the root srcAlias, matching legacy flat-install semantics),
        // and (b) any association navs discovered while lowering scalar extend
        // bodies below. One Relations.install at rule exit weaves everything.
        com.gs.legend.plan.lowering.NavScope scope = new com.gs.legend.plan.lowering.NavScope();
        List<String> aliasChain = new ArrayList<>(n.traversalHops().size() + 1);
        aliasChain.add(srcAlias);
        for (int i = 0; i < n.traversalHops().size(); i++) {
            var hop = n.traversalHops().get(i);
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
            List<String> prefix = List.of("__hop_" + i);
            String alias = scope.navigate(prefix, List.of(), jr, ctx.aliases());
            aliasChain.add(alias);
        }

        List<SqlRelation.ExtendCol> cols = new ArrayList<>(n.extensions().size());
        for (TypedExtendCol col : n.extensions()) {
            SqlRelation.ExtendCol lowered = lowerExtendCol(col, aliasChain, store, ctx, n, scope);
            if (lowered != null) cols.add(lowered);
        }
        SqlRelation joined = Relations.install(aliased, srcAlias, store, scope, ctx);
        return new SqlRelation.Extend(joined, cols);
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
     * <p>Frame clauses (ROWS/RANGE BETWEEN) remain deferred — dialects that
     * support them can subclass {@link SqlExpr.WindowCall} once the HIR
     * carries frame info.
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

        // Lower the window-call arguments.
        List<SqlExpr> args = new ArrayList<>(wc.funcArgs().size());
        for (TypedSpec a : wc.funcArgs()) {
            args.add(Lowerer.lowerScalar(a, bodyCtx));
        }

        // Lower the OVER clause.
        TypedOver over = wc.over();
        List<SqlExpr> partitionBy = new ArrayList<>(over.partitionBy().size());
        for (String col : over.partitionBy()) {
            partitionBy.add(new SqlExpr.Column(baseAlias, resolveColumnName(col, store)));
        }
        List<SqlExpr.OrderByTerm> orderBy = new ArrayList<>(over.orderBy().size());
        for (TypedSortKey key : over.orderBy()) {
            orderBy.add(lowerSortKey(key, aliasChain, store, ctx, owner, scope));
        }

        SqlExpr windowCall = new SqlExpr.WindowCall(wc.func().name(),
                args, partitionBy, orderBy);

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
                    c.direction().name(), "");
            case TypedExpressionSortKey e -> new SqlExpr.OrderByTerm(
                    lowerScalarLambda(e.keyFn(), aliasChain, store, ctx, owner, "extend:window:order-key", scope),
                    e.direction().name(), "");
        };
    }

    private static String resolveColumnName(String property, Object store) {
        if (!(store instanceof com.gs.legend.compiler.StoreResolution sr)) return property;
        String c = sr.columnFor(property);
        return c != null ? c : property;
    }
}
