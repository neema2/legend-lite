package org.finos.legend.pure.dsl;

/**
 * Filter expression: source->filter(lambda)
 */
public record FilterExpression(
                PureExpression source,
                LambdaExpression predicate) implements PureExpression {
}
