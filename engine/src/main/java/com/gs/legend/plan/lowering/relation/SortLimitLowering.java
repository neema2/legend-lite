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

        // Right-architecture path: one NavScope shared across all
        // expression-sort-key lambdas; installed as LEFT JOINs at rule exit.
        com.gs.legend.plan.lowering.NavScope scope = new com.gs.legend.plan.lowering.NavScope();

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
                        .bindVar(p, new SqlExpr.Identifier(alias), store)
                        .withNavScope(scope);
                expr = Lowerer.lowerScalar(body.get(body.size() - 1), inner);
            } else {
                throw PlanGenNotPortedException.stage3(n, "sortkey:" + k.getClass().getSimpleName());
            }
            keys.add(new SqlRelation.SortKey(expr, dir, SqlRelation.SortKey.NullOrder.DEFAULT));
        }
        SqlRelation joined = Relations.install(aliased, alias, store, scope, ctx);
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

    /**
     * Lower {@link TypedSort} as a scalar list expression. Empty {@code keys}
     * → identity {@code list_sort(list)}; non-empty → keyed sort via
     * {@link SqlExpr.ListSort.SortKey} entries. Column-name sort keys are not
     * supported on scalar lists (no named columns); they would indicate a
     * type-checker bug.
     */
    public static SqlExpr lowerAsListExpr(TypedSort n, LoweringContext ctx) {
        SqlExpr list = Lowerer.lowerScalar(n.source(), ctx);
        List<SqlExpr.ListSort.SortKey> keys = new ArrayList<>(n.keys().size());
        for (TypedSortKey k : n.keys()) {
            boolean desc = k.direction() == com.gs.legend.compiler.typed.SortDirection.DESC;
            if (k instanceof TypedExpressionSortKey ek) {
                if (ek.keyFn().parameters().size() != 1) {
                    throw PlanGenNotPortedException.stage3(n, "sort-list-multi-param-lambda");
                }
                if (ek.keyFn().body().isEmpty()) {
                    throw PlanGenNotPortedException.stage3(n, "sort-list-empty-lambda-body");
                }
                String p = ek.keyFn().parameters().get(0).name();
                LoweringContext inner = ctx.bindVar(p, new SqlExpr.Identifier(p), null);
                SqlExpr body = Lowerer.lowerScalar(
                        ek.keyFn().body().get(ek.keyFn().body().size() - 1), inner);
                keys.add(new SqlExpr.ListSort.SortKey(
                        new SqlExpr.LambdaExpr(List.of(p), body), desc));
            } else {
                throw PlanGenNotPortedException.stage3(n, "sort-list-column-key");
            }
        }
        return new SqlExpr.ListSort(list, keys);
    }

    /**
     * Lower {@link TypedSlice} as a scalar list expression. Pure 0-based
     * {@code [offset, offset+limit)} → SQL 1-based inclusive
     * {@code [offset+1, offset+limit]}. Unbounded ({@code limit < 0}) is
     * currently rare enough to defer.
     */
    public static SqlExpr lowerAsListExpr(TypedSlice n, LoweringContext ctx) {
        SqlExpr list = Lowerer.lowerScalar(n.source(), ctx);
        if (n.limit() < 0) {
            throw PlanGenNotPortedException.stage3(n, "slice-list-unbounded");
        }
        return new SqlExpr.ListSlice(
                list,
                new SqlExpr.NumericLiteral(n.offset() + 1),
                new SqlExpr.NumericLiteral(n.offset() + n.limit()));
    }
}
