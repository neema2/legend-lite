package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

import java.util.List;

/** Relational sort: {@code source->sort([ascending(~col), descending({r|expr}), ...])}. */
public record TypedSort(
        TypedSpec source,
        List<TypedSortKey> keys,
        ExpressionType info
) implements TypedSpec {
    public TypedSort {
        keys = List.copyOf(keys);
    }
}
