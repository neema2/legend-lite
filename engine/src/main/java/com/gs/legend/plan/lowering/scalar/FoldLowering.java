package com.gs.legend.plan.lowering.scalar;

import com.gs.legend.compiler.typed.TypedFold;
import com.gs.legend.plan.PlanGenNotPortedException;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.sqlgen.SqlExpr;

/**
 * {@link TypedFold} scalar reducer. Stage 5 keeps this on the backlog — a
 * faithful lowering needs per-{@link com.gs.legend.compiler.typed.FoldStrategy}
 * dispatch ({@code Concatenation}, {@code SameType}, {@code MapReduce},
 * {@code CollectionBuild}) and access to the element-transform / accumulator
 * lambdas with proper variable scoping.
 *
 * <p>Deferred with the {@code stage-4-scalar} label so fold-backed scalar
 * aggregation (custom {@code sum}-like reducers) stays visibly-unported in
 * the test scoreboard.
 */
public final class FoldLowering {
    private FoldLowering() {}

    public static SqlExpr lower(TypedFold n, LoweringContext ctx) {
        throw PlanGenNotPortedException.stage4(n);
    }
}
