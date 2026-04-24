package com.gs.legend.plan.lowering.relation;

import com.gs.legend.compiler.typed.TypedFlatten;
import com.gs.legend.plan.PlanGenNotPortedException;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.sql.SqlRelation;

/**
 * Flatten (CROSS JOIN UNNEST). Stage 1 skeleton: rule bodies throw {@code PlanGenNotPortedException.stage1}.
 */
public final class FlattenLowering {
    private FlattenLowering() {}

    public static SqlRelation lower(TypedFlatten n, LoweringContext ctx) {
        throw PlanGenNotPortedException.stage1(n);
    }
}
