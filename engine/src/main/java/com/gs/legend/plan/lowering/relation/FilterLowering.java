package com.gs.legend.plan.lowering.relation;

import com.gs.legend.compiler.PureCompileException;
import com.gs.legend.compiler.typed.TypedFilter;
import com.gs.legend.compiler.typed.TypedLambda;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.plan.PlanGenNotPortedException;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.lowering.Relations;
import com.gs.legend.plan.sql.SqlRelation;
import com.gs.legend.sqlgen.SqlExpr;

/**
 * Lowers {@link TypedFilter} (Pure {@code src->filter(x | pred)}) into
 * {@link SqlRelation.Filter}. The lambda parameter is bound to the source's
 * alias so that {@code $x.col} inside the predicate resolves to
 * {@code "alias"."col"} during scalar lowering.
 *
 * <p>The source's own {@link com.gs.legend.compiler.StoreResolution} is
 * installed on the context via {@link LoweringContext#withStore} so that
 * property-to-column mapping is available to the predicate's body.
 *
 * <p>Stage 3: association navigation inside the predicate (lifting to JOINs /
 * EXISTS) is deferred — it's surfaced in {@code PropertyAccessLowering}'s
 * {@code associationPath} not-yet-ported signal.
 */
public final class FilterLowering {
    private FilterLowering() {}

    public static SqlRelation lower(TypedFilter n, LoweringContext ctx) {
        SqlRelation source = Lowerer.lowerRelation(n.source(), ctx);
        SqlRelation aliased = Relations.ensureAliased(source, ctx);

        TypedLambda pred = n.predicate();
        if (pred.parameters().size() != 1) {
            throw new PureCompileException(
                    "[plangen-c0954a] filter predicate must have exactly 1 parameter, got "
                            + pred.parameters().size());
        }
        if (pred.body().isEmpty()) {
            throw PlanGenNotPortedException.stage3(n, "filter-empty-body");
        }
        String paramName = pred.parameters().get(0).name();
        TypedSpec terminal = pred.body().get(pred.body().size() - 1);

        // Right-architecture path: bind the lambda param with its store via
        // bindVar, install a fresh NavScope for the predicate's scalar
        // recursion, then install navs as LEFT JOINs at rule exit.
        var store = ctx.storeFor(n.source());
        com.gs.legend.plan.lowering.NavScope scope = new com.gs.legend.plan.lowering.NavScope();
        LoweringContext predCtx = ctx
                .bindVar(paramName, new SqlExpr.Identifier(aliased.alias()), store)
                .withNavScope(scope);

        SqlExpr predicate = Lowerer.lowerScalar(terminal, predCtx);
        SqlRelation joined = Relations.install(aliased, aliased.alias(), store, scope, ctx);
        return new SqlRelation.Filter(joined, predicate);
    }
}
