package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

import java.util.List;

/** Collection literal: {@code [a, b, c]}. Element type and multiplicity via {@link #info()}. */
public record TypedCollection(
        List<TypedSpec> values,
        ExpressionType info
) implements TypedSpec {
    public TypedCollection {
        values = List.copyOf(values);
    }
}
