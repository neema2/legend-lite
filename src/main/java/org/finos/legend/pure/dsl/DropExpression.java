package org.finos.legend.pure.dsl;

/**
 * Drop expression: source->drop(n)
 */
public record DropExpression(
        PureExpression source,
        int count) implements PureExpression {
}
