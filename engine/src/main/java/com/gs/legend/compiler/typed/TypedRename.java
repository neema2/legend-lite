package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

import java.util.List;

/** Column rename: {@code source->rename(~from, ~to)} (possibly batched). */
public record TypedRename(
        TypedSpec source,
        List<ColRename> renames,
        ExpressionType info
) implements TypedSpec {
    public TypedRename {
        renames = List.copyOf(renames);
    }
}
