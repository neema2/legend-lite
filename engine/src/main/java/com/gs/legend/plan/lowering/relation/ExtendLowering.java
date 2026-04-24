package com.gs.legend.plan.lowering.relation;

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

        // Install any top-level traversal hops as LEFT JOINs onto the source so
        // extend-column lambdas can reference columns from the joined tables.
        // Each hop's 2-param condition binds the source alias to its first
        // parameter and a fresh per-hop alias to its second. We also collect
        // the full alias chain [src, hop1, hop2, ...] so multi-param scalar
        // lambdas can bind their N+1 parameters positionally.
        SqlRelation joined = aliased;
        List<String> aliasChain = new ArrayList<>();
        aliasChain.add(srcAlias);
        for (com.gs.legend.compiler.typed.TraversalHop hop : n.traversalHops()) {
            String hopAlias = ctx.nextAlias();
            aliasChain.add(hopAlias);
            SqlRelation target = new SqlRelation.TableRef(
                    null, hop.tableName(), hopAlias, List.of());
            TypedLambda cond = hop.condition();
            if (cond.parameters().size() != 2) {
                throw PlanGenNotPortedException.stage3(n, "extend:hop:non-binary-condition");
            }
            if (cond.body().isEmpty()) {
                throw PlanGenNotPortedException.stage3(n, "extend:hop:empty-condition");
            }
            LoweringContext condCtx = ctx
                    .withVar(cond.parameters().get(0).name(), new SqlExpr.Identifier(srcAlias))
                    .withVar(cond.parameters().get(1).name(), new SqlExpr.Identifier(hopAlias))
                    .withStore(store);
            SqlExpr on = Lowerer.lowerScalar(cond.body().get(cond.body().size() - 1), condCtx);
            joined = new SqlRelation.Join(joined, target, SqlRelation.JoinType.LEFT, on);
        }

        List<SqlRelation.ExtendCol> cols = new ArrayList<>(n.extensions().size());
        for (TypedExtendCol col : n.extensions()) {
            SqlRelation.ExtendCol lowered = lowerExtendCol(col, aliasChain, store, ctx, n);
            if (lowered != null) cols.add(lowered);
        }
        return new SqlRelation.Extend(joined, cols);
    }

    /**
     * Dispatches per {@link TypedExtendCol} variant.
     * <p>Returns {@code null} for variants that don't project a column —
     * {@link com.gs.legend.compiler.typed.TypedAssociationExtendCol} and
     * {@link com.gs.legend.compiler.typed.TypedEmbeddedExtendCol} are pure
     * join markers: the physical JOIN is already installed by traversalHops
     * (for association) or handled as same-alias navigation (for embedded),
     * so no new column is added to the source schema.
     */
    private static SqlRelation.ExtendCol lowerExtendCol(
            TypedExtendCol col, List<String> aliasChain, Object store,
            LoweringContext ctx, TypedExtend owner) {
        return switch (col) {
            case TypedScalarExtendCol sc -> new SqlRelation.ExtendCol(
                    sc.alias(),
                    lowerScalarLambda(sc.expression(), aliasChain, store, ctx, owner, "extend:scalar"));
            case TypedWindowExtendCol wc -> new SqlRelation.ExtendCol(
                    wc.alias(),
                    lowerWindowCol(wc, aliasChain, store, ctx, owner));
            case com.gs.legend.compiler.typed.TypedAssociationExtendCol ignored -> null;
            case com.gs.legend.compiler.typed.TypedEmbeddedExtendCol ignored    -> null;
            case com.gs.legend.compiler.typed.TypedTraverseExtendCol t -> new SqlRelation.ExtendCol(
                    t.alias(),
                    lowerScalarLambda(t.expression(), aliasChain, store, ctx, owner, "extend:traverse"));
        };
    }

    /**
     * {@code FUNC(fn1($row)) OVER (PARTITION BY ... ORDER BY ...)}.
     *
     * <p>{@link TypedWindowExtendCol#func()} names the Pure native; {@code fn1}
     * selects the value; {@code over} carries partition/order keys resolved
     * against the same source alias. Two-arg window funcs ({@code lag(x, n)} /
     * {@code lead}) carry {@code fn2} as the offset argument. Frame clauses
     * (ROWS/RANGE BETWEEN) are deferred — dialects that support them can
     * subclass {@link SqlExpr.WindowCall} once the HIR carries frame info.
     */
    private static SqlExpr lowerWindowCol(TypedWindowExtendCol wc, List<String> aliasChain,
                                          Object store, LoweringContext ctx,
                                          TypedExtend owner) {
        List<SqlExpr> args = new ArrayList<>(2);
        args.add(lowerScalarLambda(wc.fn1(), aliasChain, store, ctx, owner, "extend:window:fn1"));
        wc.fn2().ifPresent(fn2 ->
                args.add(lowerScalarLambda(fn2, aliasChain, store, ctx, owner, "extend:window:fn2")));
        TypedOver over = wc.over();
        String baseAlias = aliasChain.get(0);
        List<SqlExpr> partitionBy = new ArrayList<>(over.partitionBy().size());
        for (String col : over.partitionBy()) {
            partitionBy.add(new SqlExpr.Column(baseAlias, resolveColumnName(col, store)));
        }
        List<SqlExpr.OrderByTerm> orderBy = new ArrayList<>(over.orderBy().size());
        for (TypedSortKey key : over.orderBy()) orderBy.add(lowerSortKey(key, aliasChain, store, ctx, owner));
        return new SqlExpr.WindowCall(wc.func().name(), args, partitionBy, orderBy);
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
                                             TypedExtend owner, String hint) {
        if (lam.parameters().isEmpty()) {
            throw PlanGenNotPortedException.stage3(owner, hint + ":no-params");
        }
        if (lam.body().isEmpty()) {
            throw PlanGenNotPortedException.stage3(owner, hint + ":empty-body");
        }
        LoweringContext inner = ctx.withStore((com.gs.legend.compiler.StoreResolution) store);
        for (int i = 0; i < lam.parameters().size(); i++) {
            String paramName = lam.parameters().get(i).name();
            String alias = i < aliasChain.size()
                    ? aliasChain.get(i)
                    : aliasChain.get(aliasChain.size() - 1);
            inner = inner.withVar(paramName, new SqlExpr.Identifier(alias));
        }
        TypedSpec terminal = lam.body().get(lam.body().size() - 1);
        return Lowerer.lowerScalar(terminal, inner);
    }

    private static SqlExpr.OrderByTerm lowerSortKey(TypedSortKey k, List<String> aliasChain,
                                                    Object store, LoweringContext ctx,
                                                    TypedExtend owner) {
        String baseAlias = aliasChain.get(0);
        return switch (k) {
            case TypedColumnSortKey c -> new SqlExpr.OrderByTerm(
                    new SqlExpr.Column(baseAlias, resolveColumnName(c.column(), store)),
                    c.direction().name(), "");
            case TypedExpressionSortKey e -> new SqlExpr.OrderByTerm(
                    lowerScalarLambda(e.keyFn(), aliasChain, store, ctx, owner, "extend:window:order-key"),
                    e.direction().name(), "");
        };
    }

    private static String resolveColumnName(String property, Object store) {
        if (!(store instanceof com.gs.legend.compiler.StoreResolution sr)) return property;
        String c = sr.columnFor(property);
        return c != null ? c : property;
    }
}
