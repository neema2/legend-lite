package org.finos.legend.engine.plan;

import java.util.List;
import java.util.Objects;

/**
 * IR node for ORDER BY operations.
 * 
 * SQL: SELECT ... FROM ... ORDER BY col1 ASC, col2 DESC
 * 
 * @param source  The source relation to sort
 * @param columns The columns to sort by with their directions
 */
public record SortNode(
        RelationNode source,
        List<SortColumn> columns) implements RelationNode {

    public SortNode {
        Objects.requireNonNull(source, "Source cannot be null");
        Objects.requireNonNull(columns, "Columns cannot be null");
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("At least one sort column is required");
        }
        columns = List.copyOf(columns);
    }

    /**
     * Represents a single column in the ORDER BY clause.
     */
    public record SortColumn(String column, SortDirection direction) {
        public SortColumn {
            Objects.requireNonNull(column, "Column cannot be null");
            Objects.requireNonNull(direction, "Direction cannot be null");
        }

        public static SortColumn asc(String column) {
            return new SortColumn(column, SortDirection.ASC);
        }

        public static SortColumn desc(String column) {
            return new SortColumn(column, SortDirection.DESC);
        }
    }

    /**
     * Sort direction for ORDER BY.
     */
    public enum SortDirection {
        ASC, DESC
    }

    @Override
    public <T> T accept(RelationNodeVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
