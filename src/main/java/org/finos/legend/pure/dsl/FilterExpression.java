package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * Filter expression: source->filter(lambda)
 */
public record FilterExpression(
        PureExpression source,
        LambdaExpression predicate) implements PureExpression {
}
