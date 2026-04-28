package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;
import com.gs.legend.compiler.NativeFunctionDef;

import java.util.List;

/** Relational project: {@code source->project(~[alias:row|expr, ...])}. */
public record TypedProject(
        TypedSpec source,
        List<TypedProjectionCol> projections,
        NativeFunctionDef def,
        ExpressionType info
) implements TypedNative {
    public TypedProject {
        projections = List.copyOf(projections);
    }
}
