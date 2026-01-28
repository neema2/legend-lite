package org.finos.legend.pure.dsl;

/**
 * Property assignment in instance expressions: name = value
 */
public record PropertyAssignment(
        String propertyName,
        PureExpression value) {
}
