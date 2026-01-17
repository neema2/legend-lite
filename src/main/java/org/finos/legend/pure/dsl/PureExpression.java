package org.finos.legend.pure.dsl;

/**
 * Sealed interface representing expressions in the Pure language AST.
 * 
 * Pure expressions follow a functional, fluent style:
 *   Person.all()->filter(p | $p.lastName == 'Smith')->project(...)
 */
public sealed interface PureExpression 
        permits ClassAllExpression, FilterExpression, ProjectExpression, 
                LambdaExpression, PropertyAccessExpression, ComparisonExpr, 
                LiteralExpr, VariableExpr, LogicalExpr {
}
