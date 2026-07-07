package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A type-checked aggregate column specification {@code ~alias:x|map:y|reduce}
 * &mdash; a value of type {@code AggColSpec<F1, F2, (alias:ReduceType)>[1]}. The
 * signature's {@code K}/{@code V} solve <em>per column</em> (each aggregate's map
 * type is independent); {@code R} is the one-column schema bound for the
 * enclosing call's {@code Z+R} / {@code T+R} output.
 *
 * @param col  the checked aggregate column
 * @param info {@code AggColSpec<F1, F2, (alias:…)>[1]}
 */
public record TypedAggColSpec(TypedAggCol col, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of(col.map(), col.reduce());
    }
}
