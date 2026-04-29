package com.gs.legend.plan.lowering.scalar;

import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.compiler.typed.TypedUserCall;
import com.gs.legend.plan.PlanGenNotPortedException;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.sql.SqlRelation;
import com.gs.legend.sqlgen.SqlExpr;

/**
 * User-function call ({@link TypedUserCall}) — inlines the callee body
 * with formal-to-actual bindings installed.
 */
public final class UserCallLowering {
    private UserCallLowering() {}

    /**
     * Scalar form: bind actuals and re-lower the callee body via
     * {@link Lowerer#lowerScalar}.
     */
    public static SqlExpr lower(TypedUserCall n, LoweringContext ctx) {
        return Lowerer.lowerScalar(n.callee().body().hir(), bindArgs(n, ctx));
    }

    /**
     * Relational form: bind actuals and re-lower the callee body via
     * {@link Lowerer#lowerRelation}.
     */
    public static SqlRelation lowerCall(TypedUserCall n, LoweringContext ctx) {
        return Lowerer.lowerRelation(n.callee().body().hir(), bindArgs(n, ctx));
    }

    /**
     * Per-arg dispatch: relational args bind via {@link LoweringContext#bindRel}
     * (re-lowered on each {@code $r} reference); scalar args eagerly lower
     * via {@link Lowerer#lowerScalar} and bind via
     * {@link LoweringContext#bindVar}.
     */
    private static LoweringContext bindArgs(TypedUserCall n, LoweringContext ctx) {
        var formals = n.callee().parameters();
        var actuals = n.args();
        if (formals.size() != actuals.size()) {
            throw new PlanGenNotPortedException(n, "user-call:arity-mismatch",
                    "callee=" + n.functionFqn() + " formals=" + formals.size()
                            + " actuals=" + actuals.size());
        }
        LoweringContext cur = ctx;
        for (int i = 0; i < formals.size(); i++) {
            String name = formals.get(i).name();
            TypedSpec actual = actuals.get(i);
            if (cur.isRelationalSource(actual)) {
                cur = cur.bindRel(name, actual);
            } else {
                SqlExpr value = Lowerer.lowerScalar(actual, cur);
                cur = cur.bindVar(name, value, null);
            }
        }
        return cur;
    }
}
