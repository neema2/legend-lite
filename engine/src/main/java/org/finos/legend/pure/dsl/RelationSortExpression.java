package org.finos.legend.pure.dsl;

/**
 * AST node for sort() on a RelationExpression.
 * 
 * Pure syntax: ...->project([...])->sort('column', 'asc')
 * 
 * @param source    The RelationExpression to sort
 * @param column    The column name to sort by
 * @param ascending True for ascending, false for descending
 */
public record RelationSortExpression(
        RelationExpression source,
        String column,
        boolean ascending) implements RelationExpression {
}
