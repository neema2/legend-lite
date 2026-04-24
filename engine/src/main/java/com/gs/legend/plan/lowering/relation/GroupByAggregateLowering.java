package com.gs.legend.plan.lowering.relation;

import com.gs.legend.compiler.typed.TypedAggCall;
import com.gs.legend.compiler.typed.TypedAggregate;
import com.gs.legend.compiler.typed.TypedAssociationGroupKey;
import com.gs.legend.compiler.typed.TypedColumnGroupKey;
import com.gs.legend.compiler.typed.TypedExpressionGroupKey;
import com.gs.legend.compiler.typed.TypedGroupBy;
import com.gs.legend.compiler.typed.TypedGroupKey;
import com.gs.legend.compiler.typed.TypedLambda;
import com.gs.legend.compiler.typed.TypedPivot;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.plan.PlanGenNotPortedException;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.lowering.Relations;
import com.gs.legend.plan.sql.SqlRelation;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.ArrayList;
import java.util.List;

/**
 * Aggregation relational operators: {@link TypedGroupBy} (key-partitioned
 * aggregation), {@link TypedAggregate} (single-partition aggregate that collapses
 * to one row), and {@link TypedPivot} (still deferred).
 *
 * <p>Key / aggregate lambdas are lowered with the source's alias bound, so
 * {@code $row.col} inside them produces a {@code Column(alias, …)} reference
 * against the physical source.
 *
 * <h4>Aggregate function mapping</h4>
 * {@link TypedAggCall#func()} carries the Pure native function: Stage 5
 * recognises the common reducers ({@code sum}, {@code count}, {@code max},
 * {@code min}, {@code avg}, {@code distinct}). The mapped SQL function name is
 * dialect-delegated via {@link SqlRelation.Agg#function()} so that dialects
 * can rewrite (e.g., DuckDB {@code MEDIAN}, Snowflake {@code STDDEV_POP}).
 *
 * <h4>Sub-case deferrals (stage-5-<hint>)</h4>
 * <ul>
 *   <li>{@link TypedAssociationGroupKey} — requires JOIN-lifting, deferred.</li>
 *   <li>Two-arg aggregates (fn2 non-null on {@link TypedAggCall}) cover
 *       {@code aggregate(init, reducer)} / percentile-style calls and are
 *       not yet modelled on the MIR.</li>
 *   <li>{@link TypedPivot} — full pivot lowering deferred.</li>
 * </ul>
 */
public final class GroupByAggregateLowering {
    private GroupByAggregateLowering() {}

    public static SqlRelation lower(TypedGroupBy n, LoweringContext ctx) {
        SqlRelation src = Lowerer.lowerRelation(n.source(), ctx);
        SqlRelation aliased = Relations.ensureAliased(src, ctx);
        LoweringContext inner = ctx.withStore(ctx.storeFor(n.source()));
        List<SqlExpr> keys = new ArrayList<>(n.keys().size());
        for (TypedGroupKey k : n.keys()) keys.add(lowerKey(k, aliased, inner));
        List<SqlRelation.Agg> aggs = new ArrayList<>(n.aggs().size());
        for (TypedAggCall a : n.aggs()) aggs.add(lowerAgg(a, aliased, inner));
        return new SqlRelation.GroupBy(aliased, keys, aggs);
    }

    public static SqlRelation lower(TypedAggregate n, LoweringContext ctx) {
        SqlRelation src = Lowerer.lowerRelation(n.source(), ctx);
        SqlRelation aliased = Relations.ensureAliased(src, ctx);
        LoweringContext inner = ctx.withStore(ctx.storeFor(n.source()));
        List<SqlRelation.Agg> aggs = new ArrayList<>(n.aggs().size());
        for (TypedAggCall a : n.aggs()) aggs.add(lowerAgg(a, aliased, inner));
        return new SqlRelation.Aggregate(aliased, aggs);
    }

    public static SqlRelation lower(TypedPivot n, LoweringContext ctx) {
        throw PlanGenNotPortedException.stage3(n, "pivot");
    }

    // ==================== helpers ====================

    private static SqlExpr lowerKey(TypedGroupKey k, SqlRelation aliased, LoweringContext ctx) {
        String alias = aliased.alias();
        return switch (k) {
            case TypedColumnGroupKey c -> new SqlExpr.Column(
                    alias, resolveColumn(c.column(), ctx));
            case TypedExpressionGroupKey ek -> lowerSingleParamLambda(ek.keyFn(), alias, ctx);
            case TypedAssociationGroupKey a ->
                    throw PlanGenNotPortedException.stage3(null, "groupby:assoc-key:" + a.alias());
        };
    }

    private static SqlRelation.Agg lowerAgg(TypedAggCall a, SqlRelation aliased, LoweringContext ctx) {
        // Two-arg aggregates ({@code aggregate(x, {a,b|reducer})}) — {@code fn1} is
        // the element transform, {@code fn2} is the reducer lambda. The HIR names
        // the call by its canonical aggregate (e.g., {@code plus}→SUM,
        // {@code max}→MAX, {@code percentile}→PERCENTILE_CONT), so the reducer
        // body is redundant to the printer: mapping by {@link TypedAggCall#func()}
        // alone is sufficient. Dialects own specialised rendering (PERCENTILE_*,
        // STRING_AGG/joinStrings, weighted/wavg) via {@code renderFunction}.
        String sqlFunc = mapAggFunctionName(a.func().name());
        SqlExpr arg = lowerSingleParamLambda(a.fn1(), aliased.alias(), ctx);
        boolean distinct = "distinct".equalsIgnoreCase(a.func().name());
        // {@code distinct(x)} maps to {@code COUNT(DISTINCT x)} only if used in a
        // count position; otherwise the DISTINCT is hoisted onto whatever
        // aggregate wraps it. Stage 5 only emits the DISTINCT marker.
        return new SqlRelation.Agg(a.alias(), sqlFunc, List.of(arg), distinct);
    }

    private static SqlExpr lowerSingleParamLambda(TypedLambda lam, String srcAlias, LoweringContext ctx) {
        if (lam.parameters().size() != 1) {
            throw PlanGenNotPortedException.stage3(null, "agg:multi-param-lambda");
        }
        if (lam.body().isEmpty()) {
            throw PlanGenNotPortedException.stage3(null, "agg:empty-body");
        }
        String p = lam.parameters().get(0).name();
        TypedSpec terminal = lam.body().get(lam.body().size() - 1);
        return Lowerer.lowerScalar(terminal,
                ctx.withVar(p, new SqlExpr.Identifier(srcAlias)));
    }

    private static String resolveColumn(String property, LoweringContext ctx) {
        var s = ctx.currentStore();
        if (s == null) return property;
        String c = s.columnFor(property);
        return c != null ? c : property;
    }

    /**
     * Maps Pure native aggregate names to the canonical SQL aggregate name.
     * Unknown names pass through unchanged — the dialect's
     * {@code renderAggregate} receives the final say.
     */
    private static String mapAggFunctionName(String pureName) {
        return switch (pureName) {
            case "sum", "count", "max", "min", "avg", "stdDev", "variance" -> pureName.toLowerCase();
            case "stdev", "std"                 -> "stddev";
            case "average"                      -> "avg";
            case "distinct"                     -> "count"; // COUNT(DISTINCT x)
            default -> pureName;
        };
    }
}
