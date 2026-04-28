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

    /**
     * Lower {@link TypedFlatten} in scalar list position. Legacy
     * {@code generateFlatten} is relational-only; this stub exists so the
     * {@code lowerScalar} switch is exhaustive over the dual-form records.
     * If a real test surfaces this path, port the legacy logic then.
     */
    public static com.gs.legend.sqlgen.SqlExpr lowerAsListExpr(TypedFlatten n, LoweringContext ctx) {
        throw new PlanGenNotPortedException(n, "flatten-scalar-arm",
                "flatten in scalar position not yet ported; legacy doesn't use this path");
    }
}
