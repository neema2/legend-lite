package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/**
 * GraphFetch: {@code source->graphFetch(#{Tree}#)}.
 *
 * <p>The graph-fetch tree is not yet reified as a typed structure; the tree's
 * shape and property paths are read off the original AST by downstream passes.
 * This node carries the source and the output {@link ExpressionType}
 * (typically {@code ClassType<T>[*]}) that graphFetch yields.
 */
public record TypedGraphFetch(
        TypedSpec source,
        ExpressionType info
) implements TypedSpec {}
