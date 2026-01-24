package org.finos.legend.engine.plan;

/**
 * Sealed interface representing expressions in the logical plan.
 * Expressions are used in filters, projections, and computed columns.
 * 
 * Includes:
 * - ColumnReference: reference to a column
 * - Literal: constant value
 * - ComparisonExpression: comparison operators (=, <, >, etc.)
 * - LogicalExpression: boolean operators (AND, OR, NOT)
 * - ExistsExpression: EXISTS (subquery) for to-many navigation
 */
public sealed interface Expression
                permits ColumnReference, Literal, ComparisonExpression, LogicalExpression, ExistsExpression,
                ConcatExpression, SqlFunctionCall, CaseExpression, ArithmeticExpression, AggregateExpression,
                JsonObjectExpression, SubqueryExpression, JsonArrayExpression, SqlCollectionCall {

        /**
         * Accept method for the expression visitor pattern.
         * 
         * @param visitor The visitor to accept
         * @param <T>     The return type of the visitor
         * @return The result of visiting this expression
         */
        <T> T accept(ExpressionVisitor<T> visitor);

        /**
         * Returns the SQL type of this expression.
         * Used for type-aware SQL generation (e.g., CAST insertion).
         * 
         * @return The SQL type of this expression
         */
        default SqlType type() {
                return SqlType.UNKNOWN;
        }
}
