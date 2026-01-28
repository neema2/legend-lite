package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * Sort expression: source->sort(cols)
 */
public record SortExpression(
        PureExpression source,
        List<PureExpression> sortColumns) implements PureExpression {
}
