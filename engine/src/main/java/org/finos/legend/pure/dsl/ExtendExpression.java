package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * Extend expression: source->extend(cols)
 */
public record ExtendExpression(
        PureExpression source,
        List<PureExpression> columns) implements PureExpression {
}
