package com.gs.legend.plan.lowering.natives;

import com.gs.legend.sqlgen.SqlExpr;

/**
 * Reusable {@link NativeBinding} factories for trivial cases. Bindings whose
 * argument shape varies per overload (e.g., the {@code init} family rewrites)
 * are written inline as lambdas — no factory captures their structure
 * cleanly.
 */
public final class Bindings {
    private Bindings() {}

    /**
     * Passthrough binding: emits {@code FunctionCall(sqlName, args)}. The
     * dialect's {@code renderFunction} renders the SQL syntax; for truly
     * generic functions where every dialect uses the same name, this is the
     * right answer (no typed IR variant needed).
     */
    public static NativeBinding fc(String sqlName) {
        return (call, args, ctx) -> new SqlExpr.FunctionCall(sqlName, args);
    }

    /**
     * Constant binding: emits a fixed {@link SqlExpr}, ignoring args. Used
     * for 0-arg ops like {@code today()} or constants like {@code pi()}.
     */
    public static NativeBinding constant(SqlExpr value) {
        return (call, args, ctx) -> value;
    }
}
