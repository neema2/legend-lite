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
                ConcatExpression, FunctionExpression, CaseExpression, ArithmeticExpression, AggregateExpression,
                JsonObjectExpression, SubqueryExpression, JsonArrayExpression, CollectionExpression,
                DateFunctionExpression, CurrentDateExpression, DateDiffExpression, DateAdjustExpression,
                DateTruncExpression, EpochExpression, DateComparisonExpression, TimeBucketExpression,
                MinMaxExpression, InExpression, ListLiteral, CastExpression, ListFilterExpression,
                StructLiteralExpression {

        /**
         * Accept method for the expression visitor pattern.
         * 
         * @param visitor The visitor to accept
         * @param <T>     The return type of the visitor
         * @return The result of visiting this expression
         */
        <T> T accept(ExpressionVisitor<T> visitor);

        /**
         * Returns the Pure type of this expression.
         * Used for type-aware compilation and SQL generation.
         * Every Expression subtype must implement this — no default ANY fallback.
         * 
         * @return The Pure type of this expression
         */
        GenericType type();
}
