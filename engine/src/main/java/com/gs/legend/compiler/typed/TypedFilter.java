package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;
import com.gs.legend.compiler.NativeFunctionDef;

/** Relational filter: {@code source->filter(row | predicate)}. */
public record TypedFilter(
        TypedSpec source,
        TypedLambda predicate,
        NativeFunctionDef def,
        ExpressionType info
) implements TypedNative {}
