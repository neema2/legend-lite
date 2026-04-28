package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;
import com.gs.legend.compiler.NativeFunctionDef;

import java.util.List;

/** Column rename: {@code source->rename(~from, ~to)} (possibly batched). */
public record TypedRename(
        TypedSpec source,
        List<ColRename> renames,
        NativeFunctionDef def,
        ExpressionType info
) implements TypedNative {
    public TypedRename {
        renames = List.copyOf(renames);
    }
}
