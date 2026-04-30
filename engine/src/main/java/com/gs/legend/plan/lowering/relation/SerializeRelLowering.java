package com.gs.legend.plan.lowering.relation;

import com.gs.legend.compiler.typed.TypedGraphFetch;
import com.gs.legend.compiler.typed.TypedGraphTree;
import com.gs.legend.compiler.typed.TypedSerialize;
import com.gs.legend.plan.lowering.JsonEnvelope;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.sql.SqlRelation;

import java.util.List;

/**
 * Lowers {@link TypedSerialize} as a relation. The source is lowered
 * normally; the serialize step's own job is the format conversion, so
 * we wrap the lowered source in a {@link JsonEnvelope} JSON projection
 * keyed by the serialize tree (or, when serialize was given an empty
 * tree, by the upstream graphFetch's tree).
 *
 * <p>Putting the envelope here, in the natural lowering rule, keeps
 * {@link com.gs.legend.plan.PlanGenerator#generate()} a uniform
 * lower-then-render pipeline with no result-format-specific branching.
 */
public final class SerializeRelLowering {
    private SerializeRelLowering() {}

    public static SqlRelation lower(TypedSerialize n, LoweringContext ctx) {
        SqlRelation source = Lowerer.lowerRelation(n.source(), ctx);
        List<TypedGraphTree> tree = !n.children().isEmpty()
                ? n.children()
                : (n.source() instanceof TypedGraphFetch g ? g.children() : List.of());
        // {@code MappingResolver} stamps the source's class store on every
        // relational op via the inherit/propagate walk — including
        // {@link TypedGraphFetch}, which transparently inherits its inner
        // class fetch's store. So the source's store is the right place to
        // look up association joins for nested-tree subquery building.
        return JsonEnvelope.wrap(source, tree, ctx.storeFor(n.source()), ctx.mode(), ctx);
    }
}
