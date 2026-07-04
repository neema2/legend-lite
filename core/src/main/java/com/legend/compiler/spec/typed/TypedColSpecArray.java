package com.legend.compiler.spec.typed;

import com.legend.compiler.spec.ExprType;

import java.util.List;

/**
 * A type-checked bare column-specification array {@code ~[a, b, c]} &mdash; a
 * value of type {@code ColSpecArray<(a:?, b:?, c:?)>[1]} (see
 * {@link TypedColSpec} for the unknown-type convention).
 *
 * @param names the column names in source order
 * @param info  {@code ColSpecArray<(names…:?)>[1]}
 */
public record TypedColSpecArray(List<String> names, ExprType info) implements TypedSpec {
    public TypedColSpecArray {
        names = List.copyOf(names);
    }

    @Override
    public List<TypedSpec> children() {
        return List.of();
    }
}
