package org.finos.legend.engine.plan;

import java.util.List;
import java.util.Objects;

/**
 * Represents an EXTEND operation that adds calculated columns.
 * 
 * Supports two types of projections:
 * 1. Window projections: row_number(), rank(), sum()->over(), etc.
 * 2. Simple projections: $x.col1 + $x.col2, JSON get(), etc.
 * 
 * SQL output:
 * SELECT *, calculated_expr AS "alias" FROM (source)
 */
public record ExtendNode(
        RelationNode source,
        List<ExtendProjection> projections) implements RelationNode {

    /**
     * Sealed interface for extend projections.
     */
    public sealed interface ExtendProjection permits WindowProjection, SimpleProjection {
        String alias();
    }

    /**
     * A window function projection (e.g., row_number()->over(...)).
     */
    public record WindowProjection(
            String alias,
            WindowExpression expression) implements ExtendProjection {
        public WindowProjection {
            Objects.requireNonNull(alias, "Alias cannot be null");
            Objects.requireNonNull(expression, "Window expression cannot be null");
        }
    }

    /**
     * A simple expression projection (e.g., $x.PAYLOAD->get('page')).
     */
    public record SimpleProjection(
            String alias,
            Expression expression) implements ExtendProjection {
        public SimpleProjection {
            Objects.requireNonNull(alias, "Alias cannot be null");
            Objects.requireNonNull(expression, "Expression cannot be null");
        }
    }

    public ExtendNode {
        Objects.requireNonNull(source, "Source node cannot be null");
        Objects.requireNonNull(projections, "Projections cannot be null");
        if (projections.isEmpty()) {
            throw new IllegalArgumentException("At least one projection is required");
        }
    }

    /**
     * Convenience constructor for window-only columns (backwards compatible).
     */
    public ExtendNode(RelationNode source, List<WindowProjection> windowColumns, boolean _ignored) {
        this(source, windowColumns.stream().map(w -> (ExtendProjection) w).toList());
    }

    @Override
    public <T> T accept(RelationNodeVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
