package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;
import com.gs.legend.compiler.NativeFunctionDef;

import java.util.List;

/** Relational sort: {@code source->sort([ascending(~col), descending({r|expr}), ...])}. */
public record TypedSort(
        TypedSpec source,
        List<TypedSortKey> keys,
        NativeFunctionDef def,
        ExpressionType info
) implements TypedNative {
    public TypedSort {
        keys = List.copyOf(keys);
    }
}
