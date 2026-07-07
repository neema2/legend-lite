package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A type-checked {@code limit}/{@code take} (aliases: both are SQL {@code LIMIT})
 * &mdash; {@code <T>(Relation<T>[1], Integer[1]):Relation<T>[1]} or the collection
 * {@code <T>(T[*], Integer[1]):T[*]}; lowering disambiguates by the source type.
 *
 * @param source the relation or collection being truncated
 * @param count  the row/element count expression
 * @param info   the source type unchanged
 */
public record TypedLimit(TypedSpec source, TypedSpec count, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of(source, count);
    }
}
