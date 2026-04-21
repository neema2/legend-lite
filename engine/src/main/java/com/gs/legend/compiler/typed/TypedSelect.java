package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

import java.util.List;

/** Column selection: {@code source->select(~[col1, col2])}. */
public record TypedSelect(
        TypedSpec source,
        List<String> cols,
        ExpressionType info
) implements TypedSpec {
    public TypedSelect {
        cols = List.copyOf(cols);
    }
}
