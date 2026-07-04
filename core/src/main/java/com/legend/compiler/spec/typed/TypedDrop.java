package com.legend.compiler.spec.typed;

import com.legend.compiler.spec.ExprType;

import java.util.List;

/**
 * A type-checked {@code drop} (SQL {@code OFFSET}) &mdash;
 * {@code <T>(Relation<T>[1], Integer[1]):Relation<T>[1]} or the collection form.
 *
 * @param source the relation or collection
 * @param count  how many leading rows/elements to skip
 * @param info   the source type unchanged
 */
public record TypedDrop(TypedSpec source, TypedSpec count, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of(source, count);
    }
}
