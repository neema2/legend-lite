package org.finos.legend.pure.dsl;

/**
 * Base interface representing expressions in the Pure language AST.
 * 
 * This is the root of all expression types parsed from Pure syntax.
 * 
 * Type hierarchy:
 * PureExpression
 * ├── ClassExpression (Class.all(), filter on Class)
 * ├── RelationExpression (project(), groupBy(), filter on Relation)
 * ├── ValueExpression (literals, variables, comparisons, lambdas)
 * └── Many more expression types for the complete Pure language
 */
public interface PureExpression {
        // Marker interface - implementations provide specific functionality
}
