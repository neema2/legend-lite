package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * SortBy expression: source->sortBy(lambda)
 */
public record SortByExpression(
        PureExpression source,
        List<PureExpression> sortColumns) implements PureExpression {
}
