package org.finos.legend.pure.dsl;

/**
 * Let expression: let x = value
 */
public record LetExpression(
        String variableName,
        PureExpression value) implements PureExpression {
}
