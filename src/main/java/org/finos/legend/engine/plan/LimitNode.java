package org.finos.legend.engine.plan;

import java.util.Objects;

/**
 * IR node for LIMIT and OFFSET operations.
 * 
 * SQL: SELECT ... FROM ... LIMIT n OFFSET m
 * 
 * Supports:
 * - limit(n) → LIMIT n
 * - drop(m) → OFFSET m
 * - slice(start, stop) → LIMIT (stop-start) OFFSET start
 * 
 * @param source The source relation
 * @param limit  Maximum number of rows (null = no limit)
 * @param offset Number of rows to skip (0 = no offset)
 */
public record LimitNode(
        RelationNode source,
        Integer limit,
        int offset) implements RelationNode {

    public LimitNode {
        Objects.requireNonNull(source, "Source cannot be null");
        if (limit != null && limit < 0) {
            throw new IllegalArgumentException("Limit cannot be negative");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative");
        }
    }

    /**
     * Creates a LIMIT-only node (no offset).
     */
    public static LimitNode limit(RelationNode source, int limit) {
        return new LimitNode(source, limit, 0);
    }

    /**
     * Creates an OFFSET-only node (no limit).
     */
    public static LimitNode offset(RelationNode source, int offset) {
        return new LimitNode(source, null, offset);
    }

    /**
     * Creates a LIMIT+OFFSET node for slice(start, stop).
     */
    public static LimitNode slice(RelationNode source, int start, int stop) {
        return new LimitNode(source, stop - start, start);
    }

    @Override
    public <T> T accept(RelationNodeVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
