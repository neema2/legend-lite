package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/**
 * Relational slice: {@code source->slice(offset, offset + limit)} / {@code take}
 * / {@code drop} / {@code limit}. Offset is inclusive, limit is a count.
 * {@code limit < 0} means unbounded (for {@code drop(offset)}).
 */
public record TypedSlice(
        TypedSpec source,
        long offset,
        long limit,
        ExpressionType info
) implements TypedSpec {}
