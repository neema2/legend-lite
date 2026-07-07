package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;
import java.util.Optional;

/**
 * A relation {@code write} (engine {@code TypedWrite}) &mdash;
 * {@code write<T>(Relation<T>[1] [, target:Any[1]]):Integer[1]}: persists the
 * relation, returning the row count; the optional destination reference rides
 * the node for the back-end.
 *
 * @param source      the relation being written
 * @param destination the write target reference, if present
 * @param info        {@code Integer[1]}
 */
public record TypedWrite(TypedSpec source, Optional<TypedSpec> destination, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return destination
                .map(d -> List.of(source, d))
                .orElseGet(() -> List.of(source));
    }
}
