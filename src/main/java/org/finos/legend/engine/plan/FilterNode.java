package org.finos.legend.engine.plan;

import java.util.Objects;

/**
 * Represents a filter operation in the relational algebra.
 * This corresponds to the WHERE clause in SQL.
 * 
 * @param source The source relation to filter
 * @param condition The filter condition expression
 */
public record FilterNode(
        RelationNode source,
        Expression condition
) implements RelationNode {
    
    public FilterNode {
        Objects.requireNonNull(source, "Source cannot be null");
        Objects.requireNonNull(condition, "Condition cannot be null");
    }
    
    @Override
    public <T> T accept(RelationNodeVisitor<T> visitor) {
        return visitor.visitFilter(this);
    }
    
    @Override
    public String toString() {
        return "FilterNode(" + condition + " <- " + source + ")";
    }
}
