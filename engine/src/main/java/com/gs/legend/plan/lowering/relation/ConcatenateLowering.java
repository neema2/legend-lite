package com.gs.legend.plan.lowering.relation;

import com.gs.legend.compiler.typed.TypedConcatenate;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.sql.SqlRelation;

/**
 * Lowers {@link TypedConcatenate} (Pure {@code left->concatenate(right)} /
 * {@code left->union(right)}) to a binary {@link SqlRelation.Union}.
 *
 * <p>Pure {@code concatenate} preserves duplicate rows — matches SQL
 * {@code UNION ALL}; Pure {@code union} has de-dup semantics in the AST but is
 * also compiled to the concatenate shape at this layer, so both land as
 * {@code UNION ALL} with an outer {@code SqlRelation.Distinct} inserted only
 * when the compiler marks the operation as de-duplicating. Current HIR lacks
 * that marker, so Stage 5 emits {@code UNION ALL} and leaves
 * dedup-as-needed to a follow-up when the HIR distinction is modelled.
 */
public final class ConcatenateLowering {
    private ConcatenateLowering() {}

    public static SqlRelation lower(TypedConcatenate n, LoweringContext ctx) {
        SqlRelation left  = Lowerer.lowerRelation(n.left(),  ctx);
        SqlRelation right = Lowerer.lowerRelation(n.right(), ctx);
        return new SqlRelation.Union(left, right, /* all = */ true);
    }
}
