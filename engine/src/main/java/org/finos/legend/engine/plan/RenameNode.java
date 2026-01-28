package org.finos.legend.engine.plan;

/**
 * IR node for RENAME operation on a Relation column.
 * 
 * Maps to SQL: SELECT col AS newName FROM (source)
 * 
 * Pure: relation->rename(~oldCol, ~newCol)
 */
public record RenameNode(
        RelationNode source,
        String oldColumnName,
        String newColumnName) implements RelationNode {

    @Override
    public <T> T accept(RelationNodeVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
