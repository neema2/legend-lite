package org.finos.legend.pure.dsl;

/**
 * Array/collection index access: source[index]
 */
public record IndexAccess(
        PureExpression source,
        int index) implements PureExpression {
}
