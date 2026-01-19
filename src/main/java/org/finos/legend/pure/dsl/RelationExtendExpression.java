package org.finos.legend.pure.dsl;

/**
 * Add a calculated column to a Relation.
 * 
 * Syntax: relation->extend(~newCol : x | expression)
 * 
 * Example:
 * 
 * <pre>
 * #>{store::DB.T_PERSON}
 *     ->select(~firstName, ~lastName)
 *     ->extend(~fullName : x | $x.firstName + ' ' + $x.lastName)
 * </pre>
 * 
 * @param source        The source Relation
 * @param newColumnName The name of the new column
 * @param expression    The lambda expression for calculating the column value
 */
public record RelationExtendExpression(
                RelationExpression source,
                String newColumnName,
                LambdaExpression expression) implements RelationExpression {
}
