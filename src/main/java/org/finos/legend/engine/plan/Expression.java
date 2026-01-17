package org.finos.legend.engine.plan;

/**
 * Sealed interface representing expressions in the logical plan.
 * Expressions are used in filters, projections, and computed columns.
 */
public sealed interface Expression 
        permits ColumnReference, Literal, ComparisonExpression, LogicalExpression {
    
    /**
     * Accept method for the expression visitor pattern.
     * 
     * @param visitor The visitor to accept
     * @param <T> The return type of the visitor
     * @return The result of visiting this expression
     */
    <T> T accept(ExpressionVisitor<T> visitor);
}
