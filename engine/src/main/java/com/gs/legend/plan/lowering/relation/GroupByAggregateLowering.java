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
import com.gs.legend.plan.lowering.natives.AggregateBindings;
import com.gs.legend.plan.sql.SqlAggregate;
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
        var store = ctx.storeFor(n.source());
        com.gs.legend.plan.lowering.NavScope scope = new com.gs.legend.plan.lowering.NavScope();
        LoweringContext inner = ctx.withNavScope(scope);
        List<SqlRelation.Projection> keys = new ArrayList<>(n.keys().size());
        for (TypedGroupKey k : n.keys()) {
            keys.add(new SqlRelation.Projection(k.alias(), lowerKey(k, aliased, inner, store)));
        }
        List<SqlRelation.Agg> aggs = new ArrayList<>(n.aggs().size());
        for (TypedAggCall a : n.aggs()) aggs.add(lowerAgg(a, aliased, inner, store));
        SqlRelation joined = Relations.install(aliased, aliased.alias(), store, scope, ctx);
        return new SqlRelation.GroupBy(joined, keys, aggs);
    }

    public static SqlRelation lower(TypedAggregate n, LoweringContext ctx) {
        SqlRelation src = Lowerer.lowerRelation(n.source(), ctx);
        SqlRelation aliased = Relations.ensureAliased(src, ctx);
        var store = ctx.storeFor(n.source());
        com.gs.legend.plan.lowering.NavScope scope = new com.gs.legend.plan.lowering.NavScope();
        LoweringContext inner = ctx.withNavScope(scope);
        List<SqlRelation.Agg> aggs = new ArrayList<>(n.aggs().size());
        for (TypedAggCall a : n.aggs()) aggs.add(lowerAgg(a, aliased, inner, store));
        SqlRelation joined = Relations.install(aliased, aliased.alias(), store, scope, ctx);
        return new SqlRelation.Aggregate(joined, aggs);
    }

    /**
     * Ported from legacy {@code generatePivot} (plangen-legacy line 2442):
     * produces a DuckDB {@code PIVOT source ON pivotCol USING AGG(x) AS alias}.
     * Unlike classic SQL pivots, DuckDB auto-infers both the grouping keys
     * (all non-pivot / non-agg columns) and the distinct pivot values from
     * the source, so {@link SqlRelation.PivotSpec#groupingKeys()} and
     * {@link SqlRelation.PivotSpec#pivotValues()} stay empty — the printer
     * just emits {@code ON <col>} without a {@code IN (...)}.
     */
    public static SqlRelation lower(TypedPivot n, LoweringContext ctx) {
        SqlRelation src = Lowerer.lowerRelation(n.source(), ctx);
        SqlRelation aliased = Relations.ensureAliased(src, ctx);
        var store = ctx.storeFor(n.source());
        com.gs.legend.plan.lowering.NavScope scope = new com.gs.legend.plan.lowering.NavScope();
        LoweringContext inner = ctx.withNavScope(scope);
        if (n.pivotColumns().isEmpty()) {
            throw PlanGenNotPortedException.stage3(n, "pivot:no-pivot-column");
        }
        // Stage 5 scope: single pivot column. Multi-column pivots land with
        // DuckDB {@code PIVOT ... ON (colA, colB)} syntax later.
        String pivotCol = n.pivotColumns().get(0);
        List<SqlRelation.Agg> aggs = new ArrayList<>(n.aggs().size());
        for (TypedAggCall a : n.aggs()) {
            SqlRelation.Agg agg = lowerAgg(a, aliased, inner, store);
            // DuckDB PIVOT forbids qualified columns in the {@code USING}
            // clause ({@code PIVOT expression cannot contain qualified columns}
            // binder error). Rewrite {@code Column(alias, col)} → {@code ColumnRef(col)}
            // inside the aggregate's argument so the aggregates reference
            // pivot-source columns directly.
            aggs.add(new SqlRelation.Agg(agg.alias(), unqualifyAgg(agg.fn())));
        }
        SqlRelation joined = Relations.install(aliased, aliased.alias(), store, scope, ctx);
        SqlRelation.PivotSpec spec = new SqlRelation.PivotSpec(
                List.of(), pivotCol, List.of(), aggs);
        return new SqlRelation.Pivot(joined, spec, List.of());
    }

    /** Strip alias qualification: {@code t0.col} &rarr; {@code col}. */
    private static SqlExpr unqualify(SqlExpr e) {
        if (e instanceof SqlExpr.Column c) return new SqlExpr.ColumnRef(c.column());
        if (e instanceof SqlExpr.FunctionCall fc) {
            List<SqlExpr> newArgs = fc.args().stream().map(GroupByAggregateLowering::unqualify).toList();
            return new SqlExpr.FunctionCall(fc.name(), newArgs);
        }
        if (e instanceof SqlExpr.Binary b) {
            return new SqlExpr.Binary(unqualify(b.left()), b.op(), unqualify(b.right()));
        }
        return e;
    }

    /** Apply {@link #unqualify(SqlExpr)} to a typed reducer's argument(s). */
    private static SqlAggregate.Reducer unqualifyAgg(SqlAggregate.Reducer fn) {
        return switch (fn) {
            case SqlAggregate.Sum s                -> new SqlAggregate.Sum(unqualify(s.expr()));
            case SqlAggregate.Count c              -> new SqlAggregate.Count(unqualify(c.expr()));
            // CountStar has no operand to unqualify — it's structurally invariant.
            case SqlAggregate.CountStar cs         -> cs;
            case SqlAggregate.Max m                -> new SqlAggregate.Max(unqualify(m.expr()));
            case SqlAggregate.Min m                -> new SqlAggregate.Min(unqualify(m.expr()));
            case SqlAggregate.Avg a                -> new SqlAggregate.Avg(unqualify(a.expr()));
            case SqlAggregate.StdDev s             -> new SqlAggregate.StdDev(unqualify(s.expr()));
            case SqlAggregate.Variance v           -> new SqlAggregate.Variance(unqualify(v.expr()));
            case SqlAggregate.Product p            -> new SqlAggregate.Product(unqualify(p.expr()));
            case SqlAggregate.Median m             -> new SqlAggregate.Median(unqualify(m.expr()));
            case SqlAggregate.Mode m               -> new SqlAggregate.Mode(unqualify(m.expr()));
            case SqlAggregate.HashCode h           -> new SqlAggregate.HashCode(unqualify(h.expr()));
            case SqlAggregate.JoinStrings j        -> new SqlAggregate.JoinStrings(unqualify(j.expr()), unqualify(j.separator()));
            case SqlAggregate.StdDevPopulation s   -> new SqlAggregate.StdDevPopulation(unqualify(s.expr()));
            case SqlAggregate.StdDevSample s       -> new SqlAggregate.StdDevSample(unqualify(s.expr()));
            case SqlAggregate.VariancePopulation v -> new SqlAggregate.VariancePopulation(unqualify(v.expr()));
            case SqlAggregate.VarianceSample v     -> new SqlAggregate.VarianceSample(unqualify(v.expr()));
            case SqlAggregate.PercentileCont p     -> new SqlAggregate.PercentileCont(unqualify(p.expr()), unqualify(p.p()));
            case SqlAggregate.PercentileDisc p     -> new SqlAggregate.PercentileDisc(unqualify(p.expr()), unqualify(p.p()));
            case SqlAggregate.Corr c               -> new SqlAggregate.Corr(unqualify(c.x()), unqualify(c.y()));
            case SqlAggregate.CovarPopulation c    -> new SqlAggregate.CovarPopulation(unqualify(c.x()), unqualify(c.y()));
            case SqlAggregate.CovarSample c        -> new SqlAggregate.CovarSample(unqualify(c.x()), unqualify(c.y()));
            case SqlAggregate.MaxBy m              -> new SqlAggregate.MaxBy(unqualify(m.value()), unqualify(m.key()));
            case SqlAggregate.MinBy m              -> new SqlAggregate.MinBy(unqualify(m.value()), unqualify(m.key()));
            case SqlAggregate.WeightedAvg w        -> new SqlAggregate.WeightedAvg(unqualify(w.value()), unqualify(w.weight()));
        };
    }

    // ==================== helpers ====================

    private static SqlExpr lowerKey(TypedGroupKey k, SqlRelation aliased, LoweringContext ctx,
                                    com.gs.legend.compiler.StoreResolution store) {
        String alias = aliased.alias();
        return switch (k) {
            case TypedColumnGroupKey c -> new SqlExpr.Column(
                    alias, resolveColumn(c.column(), store));
            case TypedExpressionGroupKey ek -> lowerSingleParamLambda(ek.keyFn(), alias, ctx, store);
            case TypedAssociationGroupKey a -> lowerAssocKey(a, alias, store, ctx);
        };
    }

    /**
     * GroupBy key that navigates through associations: {@code groupBy(~[$p.firm.name])}.
     * Walks the path via the active {@link com.gs.legend.plan.lowering.NavScope}
     * (same mechanism {@link com.gs.legend.plan.lowering.scalar.PropertyAccessLowering}
     * uses), then emits a Column reference on the resolved terminal alias +
     * physical column.
     */
    private static SqlExpr lowerAssocKey(TypedAssociationGroupKey a, String srcAlias,
                                         com.gs.legend.compiler.StoreResolution store,
                                         LoweringContext ctx) {
        if (ctx.navScope() == null) {
            throw PlanGenNotPortedException.stage3(null,
                    "groupby:assoc-key:no-navscope:" + a.alias());
        }
        List<String> path = a.path();
        int hopCount = Math.max(0, path.size() - 1);
        String rowAlias = srcAlias;
        com.gs.legend.compiler.StoreResolution curStore = store;
        List<String> parentPrefix = List.of();
        for (int i = 0; i < hopCount; i++) {
            String assoc = path.get(i);
            if (curStore == null) {
                throw PlanGenNotPortedException.stage3(null,
                        "groupby:assoc-key:no-store:" + assoc);
            }
            var jr = curStore.joins() == null ? null : curStore.joins().get(assoc);
            if (jr == null) {
                throw PlanGenNotPortedException.stage3(null,
                        "groupby:assoc-key:unresolved:" + assoc);
            }
            List<String> prefix = List.copyOf(path.subList(0, i + 1));
            if (jr.embedded()) {
                curStore = jr.targetResolution();
                parentPrefix = prefix;
                continue;
            }
            rowAlias = ctx.navScope().navigate(prefix, parentPrefix, jr, ctx.aliases());
            curStore = jr.targetResolution();
            parentPrefix = prefix;
        }
        String terminal = path.get(path.size() - 1);
        return new SqlExpr.Column(rowAlias, resolveColumn(terminal, curStore));
    }

    private static SqlRelation.Agg lowerAgg(TypedAggCall a, SqlRelation aliased, LoweringContext ctx,
                                            com.gs.legend.compiler.StoreResolution store) {
        // Two-arg aggregates ({@code aggregate(x, {a,b|reducer})}) — {@code fn1} is
        // the element transform, {@code fn2} is the reducer lambda. The HIR names
        // the call by its canonical aggregate (e.g., {@code plus}→SUM,
        // {@code max}→MAX, {@code percentile}→PERCENTILE_CONT), so the reducer
        // body is redundant to the typed aggregate: dispatching on
        // {@link TypedAggCall#func()} alone is sufficient. Dialects own specialised
        // rendering of each {@link SqlAggregate} variant.
        //
        // fn1 produces the values being aggregated (first reducer operand);
        // {@link TypedAggCall#extraArgs()} carries the rest. Build a typed
        // arg list (fn1's terminal body + extras) and a row-bound context
        // for the binding. The binding owns its lowering — it lowers each
        // typed arg via {@code Lowerer.lowerScalar}, and bindings whose
        // overload signature carries a structured arg (rowMapper-overloads
        // of corr/covar/maxBy/minBy/wavg) destructure the typed tree
        // directly. See AGENTS.md invariant 2.
        TypedSpec fn1Body = fn1TerminalBody(a.fn1());
        List<TypedSpec> typedArgs = new ArrayList<>(1 + a.extraArgs().size());
        typedArgs.add(fn1Body);
        typedArgs.addAll(a.extraArgs());
        String fn1Param = a.fn1().parameters().isEmpty() ? null
                : a.fn1().parameters().get(0).name();
        LoweringContext bindingCtx = fn1Param == null ? ctx
                : ctx.bindVar(fn1Param, new SqlExpr.Identifier(aliased.alias()), store);
        SqlAggregate.Reducer agg = AggregateBindings.lookup(a.func()).build(typedArgs, bindingCtx);
        return new SqlRelation.Agg(a.alias(), agg);
    }

    /** Extract fn1's terminal body expression (the value being aggregated). */
    private static TypedSpec fn1TerminalBody(TypedLambda fn1) {
        if (fn1.parameters().size() != 1) {
            throw PlanGenNotPortedException.stage3(null, "agg:multi-param-lambda");
        }
        if (fn1.body().isEmpty()) {
            throw PlanGenNotPortedException.stage3(null, "agg:empty-body");
        }
        return fn1.body().get(fn1.body().size() - 1);
    }

    private static SqlExpr lowerSingleParamLambda(TypedLambda lam, String srcAlias, LoweringContext ctx,
                                                  com.gs.legend.compiler.StoreResolution store) {
        if (lam.parameters().size() != 1) {
            throw PlanGenNotPortedException.stage3(null, "agg:multi-param-lambda");
        }
        if (lam.body().isEmpty()) {
            throw PlanGenNotPortedException.stage3(null, "agg:empty-body");
        }
        String p = lam.parameters().get(0).name();
        TypedSpec terminal = lam.body().get(lam.body().size() - 1);
        return Lowerer.lowerScalar(terminal,
                ctx.bindVar(p, new SqlExpr.Identifier(srcAlias), store));
    }

    private static String resolveColumn(String property, com.gs.legend.compiler.StoreResolution store) {
        if (store == null) return property;
        String c = store.columnFor(property);
        return c != null ? c : property;
    }

}
