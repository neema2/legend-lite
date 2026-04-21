package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/** Relational filter: {@code source->filter(row | predicate)}. */
public record TypedFilter(
        TypedSpec source,
        TypedLambda predicate,
        ExpressionType info
) implements TypedSpec {}
