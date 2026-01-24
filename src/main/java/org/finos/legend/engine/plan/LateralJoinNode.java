package org.finos.legend.engine.plan;

import java.util.Objects;

/**
 * Represents a LATERAL JOIN (UNNEST) operation in the relational algebra tree.
 * 
 * This node unnests a JSON array column, creating one row per array element.
 * In DuckDB this generates: CROSS JOIN LATERAL unnest(array_col) AS col_name
 * 
 * @param source           The source relation containing the array column
 * @param arrayColumn      The column reference pointing to the JSON array
 * @param outputColumnName The name for the unnested elements
 */
public record LateralJoinNode(
        RelationNode source,
        ColumnReference arrayColumn,
        String outputColumnName) implements RelationNode {

    public LateralJoinNode {
        Objects.requireNonNull(source, "Source relation cannot be null");
        Objects.requireNonNull(arrayColumn, "Array column cannot be null");
        Objects.requireNonNull(outputColumnName, "Output column name cannot be null");
    }

    @Override
    public <T> T accept(RelationNodeVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
