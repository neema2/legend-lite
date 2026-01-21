package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * Select expression: source->select(cols)
 */
public record SelectExpression(
        PureExpression source,
        List<PureExpression> columns) implements PureExpression {
}
