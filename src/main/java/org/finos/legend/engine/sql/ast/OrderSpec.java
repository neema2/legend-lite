package org.finos.legend.engine.sql.ast;

/**
 * ORDER BY specification: column [ASC|DESC] [NULLS FIRST|LAST]
 */
public record OrderSpec(
        Expression expression,
        Direction direction,
        NullsOrder nullsOrder) implements SQLNode {

    public enum Direction {
        ASC, DESC;

        public static Direction fromKeyword(String keyword) {
            if ("DESC".equalsIgnoreCase(keyword)) {
                return DESC;
            }
            return ASC; // default
        }
    }

    public enum NullsOrder {
        NULLS_FIRST, NULLS_LAST, DEFAULT;
    }

    /**
     * Creates an ascending order spec.
     */
    public static OrderSpec asc(Expression expr) {
        return new OrderSpec(expr, Direction.ASC, NullsOrder.DEFAULT);
    }

    /**
     * Creates a descending order spec.
     */
    public static OrderSpec desc(Expression expr) {
        return new OrderSpec(expr, Direction.DESC, NullsOrder.DEFAULT);
    }

    /**
     * Creates an order spec from a column name.
     */
    public static OrderSpec column(String name, Direction dir) {
        return new OrderSpec(Expression.column(name), dir, NullsOrder.DEFAULT);
    }
}
