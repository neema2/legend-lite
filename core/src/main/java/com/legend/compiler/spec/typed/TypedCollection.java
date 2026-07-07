package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A type-checked collection literal {@code [a, b, c]} (engine {@code TypedCollection}).
 * Its {@link #info()} type is the least common supertype of the elements; its
 * multiplicity is the exact element count.
 */
public record TypedCollection(List<TypedSpec> elements, ExprType info) implements TypedSpec {
    public TypedCollection {
        elements = List.copyOf(elements);
    }

    @Override
    public List<TypedSpec> children() {
        return elements;
    }
}
