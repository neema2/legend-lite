package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * Limit expression: source->limit(n) or source->take(n)
 */
public record LimitExpression(
        PureExpression source,
        int count) implements PureExpression {
}
