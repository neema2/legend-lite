package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A type-checked aggregate column-specification array
 * {@code ~[a:x|…:y|…, b:x|…:y|…]} (see {@link TypedAggColSpec}; {@code K}/{@code V}
 * solve independently per element).
 *
 * @param cols the checked aggregate columns in source order
 * @param info {@code AggColSpecArray<F1, F2, (…)>[1]}
 */
public record TypedAggColSpecArray(List<TypedAggCol> cols, ExprType info) implements TypedSpec {
    public TypedAggColSpecArray {
        cols = List.copyOf(cols);
    }

    @Override
    public List<TypedSpec> children() {
        return cols.stream()
                .<TypedSpec>mapMulti((c, sink) -> {
            sink.accept(c.map());
            sink.accept(c.reduce());
        }).toList();
    }
}
