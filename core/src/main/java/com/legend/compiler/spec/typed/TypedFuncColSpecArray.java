package com.legend.compiler.spec.typed;

import com.legend.compiler.spec.ExprType;

import java.util.List;

/**
 * A type-checked mapped column-specification array {@code ~[a:x|…, b:x|…]} &mdash;
 * a value of type {@code FuncColSpecArray<F, (a:…, b:…)>[1]} (see
 * {@link TypedFuncColSpec}).
 *
 * @param cols the checked columns in source order
 * @param info {@code FuncColSpecArray<F, (…)>[1]}
 */
public record TypedFuncColSpecArray(List<TypedFuncCol> cols, ExprType info) implements TypedSpec {
    public TypedFuncColSpecArray {
        cols = List.copyOf(cols);
    }

    @Override
    public List<TypedSpec> children() {
        return cols.stream().<TypedSpec>map(TypedFuncCol::fn).toList();
    }
}
