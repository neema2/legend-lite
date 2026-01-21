package org.finos.legend.pure.dsl;

/**
 * Property access expression: source.propertyName
 */
public record PropertyAccess(
        PureExpression source,
        String propertyName) implements PureExpression {
}
