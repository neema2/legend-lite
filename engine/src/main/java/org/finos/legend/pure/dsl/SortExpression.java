package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * Sort expression: source->sort(cols)
 * Implements RelationExpression when used with relation sources.
 */
public record SortExpression(
                PureExpression source,
                List<PureExpression> sortColumns) implements RelationExpression {
}
