package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

import java.util.List;

/** Relational project: {@code source->project(~[alias:row|expr, ...])}. */
public record TypedProject(
        TypedSpec source,
        List<TypedProjectionCol> projections,
        ExpressionType info
) implements TypedSpec {
    public TypedProject {
        projections = List.copyOf(projections);
    }
}
