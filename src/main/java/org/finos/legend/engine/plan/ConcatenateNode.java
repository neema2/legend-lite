package org.finos.legend.engine.plan;

/**
 * IR node for CONCATENATE operation (UNION ALL).
 * 
 * Maps to SQL: (source1) UNION ALL (source2)
 * 
 * Pure: relation1->concatenate(relation2)
 * 
 * Both relations must have the same column structure.
 */
public record ConcatenateNode(
        RelationNode left,
        RelationNode right) implements RelationNode {

    @Override
    public <T> T accept(RelationNodeVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
