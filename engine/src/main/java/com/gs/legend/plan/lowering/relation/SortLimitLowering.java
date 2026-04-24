package com.gs.legend.plan.lowering.relation;

import com.gs.legend.compiler.typed.TypedColumnSortKey;
import com.gs.legend.compiler.typed.TypedExpressionSortKey;
import com.gs.legend.compiler.typed.TypedSlice;
import com.gs.legend.compiler.typed.TypedSort;
import com.gs.legend.compiler.typed.TypedSortKey;
import com.gs.legend.plan.PlanGenNotPortedException;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.lowering.Relations;
import com.gs.legend.plan.sql.SqlRelation;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.ArrayList;
import java.util.List;

/**
 * Sort / slice (limit + offset) ops. Both are unary and preserve the source schema.
 *
 * <p>{@link TypedSort}: one {@link SqlRelation.Sort} wrapper with per-key
 * {@link SqlRelation.SortKey} translated from either a column-name key or an
 * expression-lambda key. Column-name keys are resolved against the current
 * {@link com.gs.legend.compiler.StoreResolution} when available so physical
 * column names win over logical property names.
 *
 * <p>{@link TypedSlice}: maps to a {@link SqlRelation.Limit} with the HIR's
 * raw offset/limit pair.
 */
public final class SortLimitLowering {
    private SortLimitLowering() {}

    public static SqlRelation lower(TypedSort n, LoweringContext ctx) {
        SqlRelation src = Lowerer.lowerRelation(n.source(), ctx);
        SqlRelation aliased = Relations.ensureAliased(src, ctx);
        String alias = aliased.alias();
        var store = ctx.storeFor(n.source());

        // Lift non-embedded association paths out of any expression-sort-key
        // lambdas so they render as LEFT JOINs on the sort source.
        java.util.Set<java.util.List<String>> allPaths = new java.util.LinkedHashSet<>();
        for (TypedSortKey k : n.keys()) {
            if (k instanceof TypedExpressionSortKey ek && !ek.keyFn().parameters().isEmpty()
                    && !ek.keyFn().body().isEmpty()) {
                String pn = ek.keyFn().parameters().get(0).name();
                var body = ek.keyFn().body().get(ek.keyFn().body().size() - 1);
                allPaths.addAll(AssocJoinLifter.collectPaths(body, pn));
            }
        }
        var lifted = AssocJoinLifter.liftPaths(aliased, alias, store, allPaths, ctx);
        SqlRelation joined = lifted.relation();
        var bindings = lifted.bindings();

        List<SqlRelation.SortKey> keys = new ArrayList<>(n.keys().size());
        for (TypedSortKey k : n.keys()) {
            SqlRelation.SortKey.Direction dir = switch (k.direction()) {
                case ASC -> SqlRelation.SortKey.Direction.ASC;
                case DESC -> SqlRelation.SortKey.Direction.DESC;
            };
            SqlExpr expr;
            if (k instanceof TypedColumnSortKey c) {
                String col = store != null && store.columnFor(c.column()) != null
                        ? store.columnFor(c.column()) : c.column();
                expr = new SqlExpr.Column(alias, col);
            } else if (k instanceof TypedExpressionSortKey ek) {
                if (ek.keyFn().parameters().size() != 1) {
                    throw PlanGenNotPortedException.stage3(n, "sort-multi-param-lambda");
                }
                var body = ek.keyFn().body();
                if (body.isEmpty()) {
                    throw PlanGenNotPortedException.stage3(n, "sort-empty-lambda-body");
                }
                String p = ek.keyFn().parameters().get(0).name();
                LoweringContext inner = ctx
                        .withVar(p, new SqlExpr.Identifier(alias))
                        .withStore(store)
                        .withAssocBindings(bindings);
                expr = Lowerer.lowerScalar(body.get(body.size() - 1), inner);
            } else {
                throw PlanGenNotPortedException.stage3(n, "sortkey:" + k.getClass().getSimpleName());
            }
            keys.add(new SqlRelation.SortKey(expr, dir, SqlRelation.SortKey.NullOrder.DEFAULT));
        }
        return new SqlRelation.Sort(joined, keys);
    }

    /**
     * {@code slice(offset, limit)} — limit is the number of rows after offset,
     * matching Pure semantics. The HIR normalises {@code first()}/{@code limit()}/
     * {@code drop()} to {@code TypedSlice} upstream, so this rule covers all three.
     */
    public static SqlRelation lower(TypedSlice n, LoweringContext ctx) {
        SqlRelation src = Lowerer.lowerRelation(n.source(), ctx);
        return new SqlRelation.Limit(src, n.limit(), n.offset());
    }
}
