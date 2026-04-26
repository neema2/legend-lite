package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

import java.util.List;

/**
 * Serialize a relational result to a scalar format (e.g., JSON string).
 *
 * <p>{@code children} mirrors {@link TypedGraphFetch#children()} —
 * legend-engine's {@code serialize(source, #{Tree}#)} carries its own
 * graph-fetch tree which describes the desired output shape (often, but
 * not necessarily, identical to the upstream {@code graphFetch} tree).
 * {@link com.gs.legend.plan.lowering.JsonEnvelope} prefers this tree
 * when present.
 */
public record TypedSerialize(
        TypedSpec source,
        String format,
        List<TypedGraphTree> children,
        ExpressionType info
) implements TypedSpec {
    public TypedSerialize {
        children = children == null ? List.of() : List.copyOf(children);
    }
}
