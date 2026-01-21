package org.finos.legend.pure.dsl;

/**
 * Unary expression: op expr (e.g., !flag, -value)
 */
public record UnaryExpression(
        String operator,
        PureExpression operand) implements PureExpression {
}
