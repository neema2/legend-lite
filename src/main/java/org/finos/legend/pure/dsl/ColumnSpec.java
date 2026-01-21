package org.finos.legend.pure.dsl;

/**
 * Column specification: ~colName:lambda or ~colName
 */
public record ColumnSpec(
        String name,
        PureExpression lambda,
        PureExpression extraFunction) implements PureExpression {
}
