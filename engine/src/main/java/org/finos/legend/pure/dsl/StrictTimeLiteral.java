package org.finos.legend.pure.dsl;

/**
 * StrictTime literal value (e.g., %12:30:00)
 */
public record StrictTimeLiteral(String value) implements PureExpression {
}
