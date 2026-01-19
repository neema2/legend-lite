package org.finos.legend.engine.plan;

import java.util.List;
import java.util.Objects;

/**
 * Represents an EXTEND operation that adds window function columns.
 * 
 * The extend operation adds calculated columns to the relation without
 * reducing the number of rows (unlike GROUP BY).
 * 
 * Example Pure:
 * ->extend(~rowNum : row_number()->over(~department))
 * ->extend(~rank : rank()->over(~department, ~salary->desc()))
 * ->extend(~runningSum : $x.salary->sum()->over(~department, ~salary))
 * 
 * SQL output:
 * SELECT *,
 * ROW_NUMBER() OVER (PARTITION BY "department") AS "rowNum",
 * RANK() OVER (PARTITION BY "department" ORDER BY "salary" DESC) AS "rank",
 * SUM("salary") OVER (PARTITION BY "department" ORDER BY "salary") AS
 * "runningSum"
 * FROM (source)
 */
public record ExtendNode(
        RelationNode source,
        List<WindowProjection> windowColumns) implements RelationNode {

    /**
     * A single window column projection.
     * 
     * @param alias      Output column name
     * @param expression The window expression defining the calculation
     */
    public record WindowProjection(
            String alias,
            WindowExpression expression) {
        public WindowProjection {
            Objects.requireNonNull(alias, "Alias cannot be null");
            Objects.requireNonNull(expression, "Window expression cannot be null");
        }
    }

    public ExtendNode {
        Objects.requireNonNull(source, "Source node cannot be null");
        Objects.requireNonNull(windowColumns, "Window columns cannot be null");
        if (windowColumns.isEmpty()) {
            throw new IllegalArgumentException("At least one window column is required");
        }
    }

    @Override
    public <T> T accept(RelationNodeVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
