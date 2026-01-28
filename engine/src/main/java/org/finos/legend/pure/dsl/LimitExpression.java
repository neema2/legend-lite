package org.finos.legend.pure.dsl;

/**
 * Limit expression: source->limit(n) or source->take(n)
 */
public record LimitExpression(
                PureExpression source,
                int count) implements PureExpression {
}
