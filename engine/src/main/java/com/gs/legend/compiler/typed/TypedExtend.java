package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

import java.util.List;

/**
 * Relational extend: {@code source->extend(~[col1:expr, col2:window, col3:traverse, ...])}.
 *
 * <p>Each extension column is a sealed {@link TypedExtendCol} subvariant — scalar,
 * window, or traverse. PlanGenerator pattern-matches on the subvariant; no flags.
 */
public record TypedExtend(
        TypedSpec source,
        List<TypedExtendCol> extensions,
        ExpressionType info
) implements TypedSpec {
    public TypedExtend {
        extensions = List.copyOf(extensions);
    }
}
