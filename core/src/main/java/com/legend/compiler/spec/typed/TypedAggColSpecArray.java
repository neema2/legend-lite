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

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 2 * cols.size(), "TypedAggColSpecArray");
        java.util.List<TypedAggCol> cs = new java.util.ArrayList<>(cols.size());
        for (int i = 0; i < cols.size(); i++) {
            TypedAggCol c = cols.get(i);
            cs.add(new TypedAggCol(c.name(), (TypedLambda) kids.get(2 * i),
                    (TypedLambda) kids.get(2 * i + 1), c.order()));
        }
        return new TypedAggColSpecArray(cs, info);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedAggColSpecArray(cols, info);
    }
}
