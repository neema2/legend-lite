package com.gs.legend.plan.lowering.relation;

import com.gs.legend.compiler.typed.TypedGraphFetch;
import com.gs.legend.compiler.typed.TypedGraphTree;
import com.gs.legend.compiler.typed.TypedSerialize;
import com.gs.legend.compiler.typed.TypedSerializeImplicit;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.plan.lowering.JsonEnvelope;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.sql.SqlRelation;

import java.util.List;

/**
 * Lowers explicit {@link TypedSerialize} and implicit
 * {@link TypedSerializeImplicit} (the legend-lite bare-class-root marker)
 * as a relation. The source is lowered normally; the serialize step's own
 * job is the format conversion, so we wrap the lowered source in a
 * {@link JsonEnvelope} JSON projection keyed by the serialize tree
 * (or, when serialize was given an empty tree, by the upstream graphFetch's
 * tree, or — for the implicit form — by the leaf-only tree synthesized at
 * the tail of {@code MappingResolver}).
 *
 * <p>Putting the envelope here, in the natural lowering rule, keeps
 * {@link com.gs.legend.plan.PlanGenerator#generate()} a uniform
 * lower-then-render pipeline with no result-format-specific branching and
 * no root-level wrap. Both forms share the same wrap call below.
 */
public final class SerializeRelLowering {
    private SerializeRelLowering() {}

    public static SqlRelation lower(TypedSerialize n, LoweringContext ctx) {
        List<TypedGraphTree> tree = !n.children().isEmpty()
                ? n.children()
                : (n.source() instanceof TypedGraphFetch g ? g.children() : List.of());
        return wrap(n.source(), tree, ctx);
    }

    public static SqlRelation lower(TypedSerializeImplicit n, LoweringContext ctx) {
        // The implicit-serialize marker always carries a synthesized
        // leaf-only tree (one entry per mapped property of the resolved
        // store). Empty would mean MappingResolver elaborated a class with
        // no mapped properties, which is a compiler bug.
        return wrap(n.source(), n.children(), ctx);
    }

    private static SqlRelation wrap(TypedSpec source, List<TypedGraphTree> tree,
                                    LoweringContext ctx) {
        SqlRelation rel = Lowerer.lowerRelation(source, ctx);
        // {@code MappingResolver} stamps the source's class store on every
        // relational op via the inherit/propagate walk — including
        // {@link TypedGraphFetch}, which transparently inherits its inner
        // class fetch's store. So the source's store is the right place to
        // look up association joins for nested-tree subquery building.
        return JsonEnvelope.wrap(rel, tree, ctx.storeFor(source), ctx.mode(), ctx);
    }
}
