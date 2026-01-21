package org.finos.legend.pure.dsl;

/**
 * Binary expression: left op right (e.g., a + b, x && y, i == j)
 */
public record BinaryExpression(
        PureExpression left,
        String operator,
        PureExpression right) implements PureExpression {
}
