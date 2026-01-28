package org.finos.legend.pure.dsl;

/**
 * Slice expression: source->slice(start, end)
 */
public record SliceExpression(
        PureExpression source,
        int start,
        int end) implements PureExpression {
}
