package org.finos.legend.pure.dsl;

import java.math.BigDecimal;

/**
 * Decimal literal value
 */
public record DecimalLiteral(BigDecimal value) implements PureExpression {
}
