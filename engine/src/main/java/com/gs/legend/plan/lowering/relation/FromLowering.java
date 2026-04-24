package com.gs.legend.plan.lowering.relation;

import com.gs.legend.compiler.typed.TypedFrom;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.sql.SqlRelation;

/**
 * Lowers {@link TypedFrom} (Pure {@code src->from(mapping, runtime)}), which
 * overrides the active mapping/runtime for its source subtree.
 *
 * <p><strong>Stage 4 scope</strong>: routing is a whole-program concern; the
 * full per-subtree mapping re-binding is tracked in
 * {@code docs/mapping-alignment-with-legend-engine.md} and will land in a
 * dedicated mapping-routing pass. For now we lower the source under the
 * current context — correct whenever the active mapping in
 * {@link LoweringContext} already matches {@link TypedFrom#mapping()}, which is
 * true for all single-mapping tests.
 *
 * <p>Multi-mapping queries that genuinely need the override hit this rule and
 * are served by the base context's bindings; once the mapping-routing pass
 * lands, this rule will install an overlay and re-resolve the source's stores
 * instead of deferring to the ambient context.
 */
public final class FromLowering {
    private FromLowering() {}

    public static SqlRelation lower(TypedFrom n, LoweringContext ctx) {
        return Lowerer.lowerRelation(n.source(), ctx);
    }
}
