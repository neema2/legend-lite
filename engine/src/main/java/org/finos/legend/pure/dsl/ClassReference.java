package org.finos.legend.pure.dsl;

/**
 * Class reference without .all() - just the class name
 */
public record ClassReference(String className) implements PureExpression {
}
