package org.finos.legend.engine.plan;

import java.util.List;

/**
 * IR node for DISTINCT operation on a Relation.
 * 
 * Maps to SQL: SELECT DISTINCT col1, col2, ... FROM (source)
 * 
 * Two forms:
 * - distinct() - all columns
 * - distinct(~[col1, col2]) - specific columns
 */
public record DistinctNode(
        RelationNode source,
        List<String> columns // empty = all columns
) implements RelationNode {

    public static DistinctNode all(RelationNode source) {
        return new DistinctNode(source, List.of());
    }

    public static DistinctNode columns(RelationNode source, List<String> columns) {
        return new DistinctNode(source, columns);
    }

    @Override
    public <T> T accept(RelationNodeVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
