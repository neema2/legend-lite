package com.gs.legend.plan.lowering.relation;

import com.gs.legend.compiler.typed.TypedGraphFetch;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.sql.SqlRelation;

/**
 * {@link TypedGraphFetch} — graph-shaped serialization of a relational source.
 *
 * <p>At the SQL-generation layer a graphFetch is a source-preserving wrapper:
 * the nested-object shape is a post-processing concern handled by the Execute
 * layer. The legacy PlanGen emitted the source's SQL directly for the
 * relational part of graphFetch; we replicate that here.
 *
 * <p>Genuine graph-fetch plan nodes (per-property sub-queries, root/sub-keys)
 * are an Execute-layer concern and live outside the {@code SqlRelation} MIR.
 */
public final class GraphFetchLowering {
    private GraphFetchLowering() {}

    public static SqlRelation lower(TypedGraphFetch n, LoweringContext ctx) {
        return Lowerer.lowerRelation(n.source(), ctx);
    }
}
