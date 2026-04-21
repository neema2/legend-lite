package com.gs.legend.compiler.typed;

/** Sort by computed expression. */
public record TypedExpressionSortKey(TypedLambda keyFn, SortDirection direction) implements TypedSortKey {}
