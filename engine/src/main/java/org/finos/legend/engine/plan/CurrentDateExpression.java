package org.finos.legend.engine.plan;

/**
 * Represents zero-argument date functions that return the current date/time.
 * 
 * Pure syntax:
 * now() -> CURRENT_TIMESTAMP
 * today() -> CURRENT_DATE
 */
public record CurrentDateExpression(
        CurrentDateFunction function) implements Expression {

    public enum CurrentDateFunction {
        NOW("CURRENT_TIMESTAMP"),
        TODAY("CURRENT_DATE");

        private final String sql;

        CurrentDateFunction(String sql) {
            this.sql = sql;
        }

        public String sql() {
            return sql;
        }
    }

    @Override
    public GenericType type() {
        return function == CurrentDateFunction.NOW ? GenericType.Primitive.DATE_TIME : GenericType.Primitive.STRICT_DATE;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
