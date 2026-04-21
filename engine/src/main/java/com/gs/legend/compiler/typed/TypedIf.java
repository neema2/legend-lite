package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/** Conditional: {@code if(cond, |then, |else)}. */
public record TypedIf(
        TypedSpec condition,
        TypedSpec thenBranch,
        TypedSpec elseBranch,
        ExpressionType info
) implements TypedSpec {}
