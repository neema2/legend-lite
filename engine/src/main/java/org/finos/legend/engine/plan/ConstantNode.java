package org.finos.legend.engine.plan;

/**
 * Represents a constant expression that becomes SELECT &lt;expression&gt;
 * without a FROM clause.
 * 
 * This supports Pure expressions like |1+1 which translate to SELECT 1+1.
 * The database evaluates the expression and returns a single-row, single-column
 * result.
 *
 * Example:
 * Pure: |1 + 2
 * SQL: SELECT 1 + 2
 */
public record ConstantNode(Expression expression) implements RelationNode {

    @Override
    public <T> T accept(RelationNodeVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
