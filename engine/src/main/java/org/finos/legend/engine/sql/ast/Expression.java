package org.finos.legend.engine.sql.ast;

import java.util.List;

/**
 * Represents an SQL expression.
 * 
 * Expressions can be used in SELECT, WHERE, HAVING, ON, and other clauses.
 */
public sealed interface Expression extends SQLNode
        permits Expression.ColumnRef, Expression.Literal, Expression.BinaryOp,
        Expression.UnaryOp, Expression.FunctionCall, Expression.CaseExpr,
        Expression.ExistsExpr, Expression.InExpr, Expression.BetweenExpr,
        Expression.IsNullExpr, Expression.CastExpr, Expression.SubqueryExpr,
        Expression.WindowExpr {

    // ==================== Factory Methods ====================

    static Expression.ColumnRef column(String name) {
        return new ColumnRef(null, name);
    }

    static Expression.ColumnRef column(String table, String name) {
        return new ColumnRef(table, name);
    }

    static Expression.Literal stringLiteral(String value) {
        return new Literal(LiteralType.STRING, value);
    }

    static Expression.Literal intLiteral(long value) {
        return new Literal(LiteralType.INTEGER, value);
    }

    static Expression.Literal decimalLiteral(double value) {
        return new Literal(LiteralType.DECIMAL, value);
    }

    static Expression.Literal boolLiteral(boolean value) {
        return new Literal(LiteralType.BOOLEAN, value);
    }

    static Expression.Literal nullLiteral() {
        return new Literal(LiteralType.NULL, null);
    }

    static Expression.ColumnRef identifier(String name) {
        return new ColumnRef(null, name);
    }

    // ==================== AST Node Types ====================

    /**
     * Column reference: table.column or just column
     */
    record ColumnRef(String tableAlias, String columnName) implements Expression {
        public boolean isQualified() {
            return tableAlias != null;
        }
    }

    /**
     * Literal value: 'string', 123, 45.67, TRUE, NULL
     */
    record Literal(LiteralType type, Object value) implements Expression {
    }

    enum LiteralType {
        STRING, INTEGER, DECIMAL, BOOLEAN, NULL
    }

    /**
     * Binary operation: left op right
     * e.g., a = 1, x + y, name LIKE '%test%'
     */
    record BinaryOp(Expression left, BinaryOperator operator, Expression right) implements Expression {
    }

    enum BinaryOperator {
        // Comparison
        EQ("="), NE("<>"), LT("<"), LE("<="), GT(">"), GE(">="),
        LIKE("LIKE"), ILIKE("ILIKE"),

        // Logical
        AND("AND"), OR("OR"),

        // Arithmetic
        PLUS("+"), MINUS("-"), MULTIPLY("*"), DIVIDE("/"), MODULO("%"),

        // String
        CONCAT("||");

        private final String sql;

        BinaryOperator(String sql) {
            this.sql = sql;
        }

        public String toSql() {
            return sql;
        }
    }

    /**
     * Unary operation: op expr
     * e.g., NOT condition, -value
     */
    record UnaryOp(UnaryOperator operator, Expression operand) implements Expression {
    }

    enum UnaryOperator {
        NOT("NOT"), MINUS("-"), PLUS("+");

        private final String sql;

        UnaryOperator(String sql) {
            this.sql = sql;
        }

        public String toSql() {
            return sql;
        }
    }

    /**
     * Function call: functionName(arg1, arg2, ...)
     * e.g., UPPER(name), COUNT(*), SUM(salary)
     */
    record FunctionCall(String functionName, List<Expression> arguments, boolean distinct) implements Expression {
        public static FunctionCall of(String name, Expression... args) {
            return new FunctionCall(name, List.of(args), false);
        }

        public static FunctionCall distinct(String name, Expression arg) {
            return new FunctionCall(name, List.of(arg), true);
        }

        public boolean isAggregate() {
            return switch (functionName.toUpperCase()) {
                case "COUNT", "SUM", "AVG", "MIN", "MAX", "STDDEV", "VARIANCE" -> true;
                default -> false;
            };
        }
    }

    /**
     * CASE expression:
     * CASE WHEN cond1 THEN val1 WHEN cond2 THEN val2 ELSE default END
     */
    record CaseExpr(List<WhenClause> whenClauses, Expression elseExpr) implements Expression {
    }

    record WhenClause(Expression condition, Expression result) {
    }

    /**
     * EXISTS (subquery)
     */
    record ExistsExpr(SelectStatement subquery, boolean negated) implements Expression {
    }

    /**
     * expr IN (val1, val2, ...) or expr IN (subquery)
     */
    record InExpr(Expression expr, List<Expression> values, SelectStatement subquery, boolean negated)
            implements Expression {
        public boolean hasSubquery() {
            return subquery != null;
        }
    }

    /**
     * expr BETWEEN low AND high
     */
    record BetweenExpr(Expression expr, Expression low, Expression high, boolean negated) implements Expression {
    }

    /**
     * expr IS NULL or expr IS NOT NULL
     */
    record IsNullExpr(Expression expr, boolean negated) implements Expression {
    }

    /**
     * CAST(expr AS type) or expr::type (Postgres)
     */
    record CastExpr(Expression expr, String targetType) implements Expression {
    }

    /**
     * Subquery as expression: (SELECT ...)
     */
    record SubqueryExpr(SelectStatement subquery) implements Expression {
    }

    /**
     * Window function: func() OVER (PARTITION BY ... ORDER BY ...)
     */
    record WindowExpr(
            FunctionCall function,
            List<Expression> partitionBy,
            List<OrderSpec> orderBy,
            FrameSpec frame) implements Expression {

        public boolean hasPartition() {
            return partitionBy != null && !partitionBy.isEmpty();
        }

        public boolean hasOrderBy() {
            return orderBy != null && !orderBy.isEmpty();
        }

        public boolean hasFrame() {
            return frame != null;
        }
    }

    /**
     * Window frame specification: ROWS/RANGE BETWEEN start AND end
     */
    record FrameSpec(FrameType type, FrameBound start, FrameBound end) {
    }

    enum FrameType {
        ROWS, RANGE
    }

    record FrameBound(FrameBoundType type, Integer offset) {
        public static FrameBound unboundedPreceding() {
            return new FrameBound(FrameBoundType.UNBOUNDED_PRECEDING, null);
        }

        public static FrameBound unboundedFollowing() {
            return new FrameBound(FrameBoundType.UNBOUNDED_FOLLOWING, null);
        }

        public static FrameBound currentRow() {
            return new FrameBound(FrameBoundType.CURRENT_ROW, null);
        }

        public static FrameBound preceding(int offset) {
            return new FrameBound(FrameBoundType.PRECEDING, offset);
        }

        public static FrameBound following(int offset) {
            return new FrameBound(FrameBoundType.FOLLOWING, offset);
        }
    }

    enum FrameBoundType {
        UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING, CURRENT_ROW, PRECEDING, FOLLOWING
    }
}
