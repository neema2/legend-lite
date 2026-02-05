package org.finos.legend.pure.dsl;

/**
 * AST node for relation write() function.
 * 
 * Pure syntax: relation->write(accessor)
 * 
 * The write() function writes the source relation data to the destination
 * specified by the accessor, and returns the count of rows written.
 * 
 * For PCT testing with TDS literals, this generates SELECT COUNT(*)
 * from the source relation.
 */
public record RelationWriteExpression(
        PureExpression source, // The relation to write
        PureExpression accessor // The RelationElementAccessor (destination)
) implements PureExpression {
}
