package org.finos.legend.pure.dsl;

/**
 * Variable reference: $varName
 */
public record VariableReference(String name) implements PureExpression {
}
