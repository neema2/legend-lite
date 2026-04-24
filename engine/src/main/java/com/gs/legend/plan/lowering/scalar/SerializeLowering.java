package com.gs.legend.plan.lowering.scalar;

import com.gs.legend.compiler.typed.TypedSerialize;
import com.gs.legend.compiler.typed.TypedWrite;
import com.gs.legend.plan.PlanGenNotPortedException;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.sqlgen.SqlExpr;

/**
 * Pipeline terminators: serialize (JSON) and write (INSERT). Stage 1 skeleton: rule bodies throw {@code PlanGenNotPortedException.stage1}.
 */
public final class SerializeLowering {
    private SerializeLowering() {}

    public static SqlExpr lower(TypedSerialize n, LoweringContext ctx) {
        throw PlanGenNotPortedException.stage1(n);
    }

    public static SqlExpr lower(TypedWrite n, LoweringContext ctx) {
        throw PlanGenNotPortedException.stage1(n);
    }
}
