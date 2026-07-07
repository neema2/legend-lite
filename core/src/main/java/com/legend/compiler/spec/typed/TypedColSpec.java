package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A type-checked bare column specification {@code ~col} &mdash; a first-class
 * value of type {@code ColSpec<(col:?)>[1]}: the named column with the
 * <em>unknown</em> column type {@code ?}, solved by the enclosing call's
 * {@code ⊆}/{@code =} signature constraints (never leaking into an output
 * schema). Mirrors real Pure, where {@code ~col} is an ordinary value the
 * generic checker constrains &mdash; not special syntax each operator re-parses.
 *
 * @param name the column name as written in source
 * @param info {@code ColSpec<(name:?)>[1]}
 */
public record TypedColSpec(String name, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of();
    }
}
