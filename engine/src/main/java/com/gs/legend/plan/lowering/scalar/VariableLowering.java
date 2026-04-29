package com.gs.legend.plan.lowering.scalar;

import com.gs.legend.compiler.typed.TypedVariable;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.sql.SqlRelation;
import com.gs.legend.sqlgen.SqlExpr;

/**
 * Lambda-parameter / let-binding / function-parameter references ({@code $x}).
 *
 * <p>Resolution strategy: look up the name in {@link LoweringContext#lookupVar}.
 * When a parent lowering rule (filter, sort, extend, join) enters a lambda body,
 * it binds each parameter name to the {@link SqlExpr} representing that parameter's
 * value (e.g., a row alias like {@code t0} for the current row, or the pre-computed
 * {@link SqlExpr} value of a let-binding) and passes the extended context down.
 *
 * <p>When a parent hasn't yet been ported to install the binding, we fall back to
 * a raw {@link SqlExpr.Identifier} with the variable's Pure name. This yields
 * recognisably-wrong SQL rather than a crash, and matches the behaviour of the
 * legacy {@code SqlBuilder} variable path for un-bound names. Stage 3 rules will
 * install proper bindings and the fallback will become unreachable for
 * well-typed queries.
 */
public final class VariableLowering {
    private VariableLowering() {}

    public static SqlExpr lower(TypedVariable n, LoweringContext ctx) {
        SqlExpr bound = ctx.lookupVar(n.name());
        if (bound != null) return bound;
        return new SqlExpr.Identifier(n.name());
    }

    /**
     * Lower a {@link TypedVariable} at relational position.
     *
     * <p>When the variable is bound as a captured relational HIR
     * ({@code let r = SomeClass.all()->...}; later {@code $r->filter(...)}),
     * re-lower the captured node so the relational form materialises in
     * place. Otherwise route through the generic
     * {@link LoweringContext#toRelation}, which wraps the scalar form as
     * a one-row relation.
     */
    public static SqlRelation lowerVariable(TypedVariable n, LoweringContext ctx) {
        return ctx.lookupBinding(n.name()) instanceof LoweringContext.Rel rel
                ? Lowerer.lowerRelation(rel.node(), ctx)
                : ctx.toRelation(n);
    }
}
