package org.finos.legend.engine.plan;

/**
 * IR node for write operations.
 * 
 * The write operation takes data from a source relation and writes it
 * to a destination (specified by the accessor). For PCT testing, this
 * generates SELECT COUNT(*) FROM (source) to return the row count.
 */
public record WriteNode(
        RelationNode source // The relation being written
) implements RelationNode {

    @Override
    public <T> T accept(RelationNodeVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
