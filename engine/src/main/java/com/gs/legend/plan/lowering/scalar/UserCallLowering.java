package com.gs.legend.plan.lowering.scalar;

import com.gs.legend.compiler.typed.TypedUserCall;
import com.gs.legend.plan.PlanGenNotPortedException;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.sqlgen.SqlExpr;

/**
 * User-function call (inlines via callee.typedBody). Stage 1 skeleton: rule bodies throw {@code PlanGenNotPortedException.stage1}.
 */
public final class UserCallLowering {
    private UserCallLowering() {}

    public static SqlExpr lower(TypedUserCall n, LoweringContext ctx) {
        throw PlanGenNotPortedException.stage1(n);
    }
}
