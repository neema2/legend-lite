package com.gs.legend.compiler.typed;

/**
 * Group-by key for {@link TypedGroupBy}. Either a column, an expression lambda,
 * or an association navigation path — each produces a grouping alias.
 */
public sealed interface TypedGroupKey permits
        TypedColumnGroupKey, TypedExpressionGroupKey, TypedAssociationGroupKey {
    String alias();
}
