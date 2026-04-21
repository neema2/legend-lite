package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

import java.util.List;

/**
 * Relational extend: {@code source->extend(~[col1:expr, col2:window, col3:traverse, ...])}.
 *
 * <p>Each extension column is a sealed {@link TypedExtendCol} subvariant — scalar,
 * window, or traverse. PlanGenerator pattern-matches on the subvariant; no flags.
 *
 * <p>When a top-level {@code traverse()} clause appears as an extend parameter
 * (not wrapping a ColSpec), the resolved hops are recorded in
 * {@code traversalHops}. Those hops apply <em>before</em> the column extensions
 * are evaluated, exposing the terminal table's columns to every scalar/window
 * column in {@code extensions}. Empty when no top-level traverse is present.
 */
public record TypedExtend(
        TypedSpec source,
        List<TraversalHop> traversalHops,
        List<TypedExtendCol> extensions,
        ExpressionType info
) implements TypedSpec {
    public TypedExtend {
        traversalHops = List.copyOf(traversalHops);
        extensions = List.copyOf(extensions);
    }
}
