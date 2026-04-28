package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;
import com.gs.legend.compiler.NativeFunctionDef;

import java.util.List;

/**
 * GraphFetch: {@code source->graphFetch(#{Tree}#)}.
 *
 * <p>The graph-fetch tree is reified into a list of {@link TypedGraphTree}
 * children (the root class is implicit in the source's class type).
 * Downstream passes (notably {@link com.gs.legend.plan.lowering.JsonEnvelope})
 * read the tree to shape the JSON envelope without re-walking the AST.
 *
 * <p>{@code info} preserves the source's {@link ExpressionType}
 * (typically {@code ClassType<T>[*]}) that graphFetch yields — graphFetch
 * is a projection, not a type change.
 */
public record TypedGraphFetch(
        TypedSpec source,
        List<TypedGraphTree> children,
        NativeFunctionDef def,
        ExpressionType info
) implements TypedNative {
    public TypedGraphFetch {
        children = children == null ? List.of() : List.copyOf(children);
    }
}
