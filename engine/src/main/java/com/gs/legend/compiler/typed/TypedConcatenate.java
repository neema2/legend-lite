package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/**
 * Union-all of two inputs: {@code left->concatenate(right)} or {@code left->union(right)}.
 *
 * <p>Both inputs may be relations (columns must align one-to-one) or scalar
 * collections. The single {@code info} on this node captures the resolved
 * output type, so downstream consumers don't need to re-derive the common
 * schema or element type.
 *
 * @param left  Left-hand side.
 * @param right Right-hand side.
 * @param info  Output {@link ExpressionType} — relation or {@code T[*]}.
 */
public record TypedConcatenate(
        TypedSpec left,
        TypedSpec right,
        ExpressionType info
) implements TypedSpec {}
