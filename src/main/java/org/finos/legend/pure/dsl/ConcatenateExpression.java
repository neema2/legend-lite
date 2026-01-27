package org.finos.legend.pure.dsl;

/**
 * AST for concatenate operation: relation1->concatenate(relation2)
 * SQL: UNION ALL
 */
public record ConcatenateExpression(
        PureExpression left,
        PureExpression right) implements RelationExpression {
}
