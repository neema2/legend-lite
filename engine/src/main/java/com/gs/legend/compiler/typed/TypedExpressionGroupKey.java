package com.gs.legend.compiler.typed;

/** Group by a computed expression. */
public record TypedExpressionGroupKey(TypedLambda keyFn, String alias) implements TypedGroupKey {}
