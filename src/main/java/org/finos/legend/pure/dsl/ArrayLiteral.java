package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * Array literal: [expr1, expr2, ...]
 */
public record ArrayLiteral(List<PureExpression> elements) implements PureExpression {
}
