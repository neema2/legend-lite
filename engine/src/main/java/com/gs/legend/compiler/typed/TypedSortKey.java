package com.gs.legend.compiler.typed;

/**
 * Sort key for {@link TypedSort}. Either a column name or an expression lambda,
 * plus a direction — no null-fallback-to-AST.
 */
public sealed interface TypedSortKey permits TypedColumnSortKey, TypedExpressionSortKey {
    SortDirection direction();
}
