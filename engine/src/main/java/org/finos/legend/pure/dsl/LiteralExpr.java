package org.finos.legend.pure.dsl;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Represents a literal value in Pure.
 * 
 * Examples: 'Smith', 25, true, 3.14, %2024-01-01
 * 
 * @param value The literal value
 * @param type  The type of literal
 */
public record LiteralExpr(
        Object value,
        LiteralType type) implements PureExpression {

    public enum LiteralType {
        STRING,
        INTEGER,
        FLOAT,
        DECIMAL,
        BOOLEAN,
        DATE,
        STRICTTIME
    }

    public LiteralExpr {
        Objects.requireNonNull(type, "Type cannot be null");
    }

    public static LiteralExpr string(String value) {
        return new LiteralExpr(value, LiteralType.STRING);
    }

    public static LiteralExpr integer(long value) {
        return new LiteralExpr(value, LiteralType.INTEGER);
    }

    public static LiteralExpr integer(java.math.BigInteger value) {
        return new LiteralExpr(value, LiteralType.INTEGER);
    }

    public static LiteralExpr floatValue(double value) {
        return new LiteralExpr(value, LiteralType.FLOAT);
    }

    public static LiteralExpr decimalValue(double value) {
        return new LiteralExpr(value, LiteralType.DECIMAL);
    }

    public static LiteralExpr decimalValue(BigDecimal value) {
        return new LiteralExpr(value, LiteralType.DECIMAL);
    }

    public static LiteralExpr bool(boolean value) {
        return new LiteralExpr(value, LiteralType.BOOLEAN);
    }

    /**
     * Creates a date literal from %YYYY-MM-DD format.
     * Stores the raw string for SQL generation.
     */
    public static LiteralExpr date(String dateText) {
        // Store raw text (with %) for SQL generation - DuckDB uses DATE 'YYYY-MM-DD'
        return new LiteralExpr(dateText, LiteralType.DATE);
    }

    /**
     * Creates a strict time literal from %HH:MM:SS format.
     * Stores the raw string for SQL generation.
     */
    public static LiteralExpr strictTime(String timeText) {
        return new LiteralExpr(timeText, LiteralType.STRICTTIME);
    }
}
