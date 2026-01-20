package org.finos.legend.pure.dsl;

/**
 * Sealed interface representing expressions in the Pure language AST.
 * 
 * Type hierarchy:
 * PureExpression
 * ├── ClassExpression (Class.all(), filter on Class)
 * ├── RelationExpression (project(), groupBy(), filter on Relation)
 * └── ValueExpression (literals, variables, comparisons, lambdas)
 */
public sealed interface PureExpression
        permits ClassExpression, RelationExpression,
        LambdaExpression, PropertyAccessExpression, ComparisonExpr,
        LiteralExpr, VariableExpr, LogicalExpr, FromExpression,
        SerializeExpression {
}
