package org.finos.legend.engine.plan;

import java.util.Objects;

/**
 * Represents a literal value in the logical plan.
 * Supports String, Integer, Boolean, and null values.
 * 
 * @param value       The literal value (can be null)
 * @param literalType The type of the literal
 */
public record Literal(
        Object value,
        LiteralType literalType) implements Expression {

    public enum LiteralType {
        STRING,
        INTEGER,
        BOOLEAN,
        DOUBLE,
        DECIMAL,
        NULL,
        DATE,
        TIMESTAMP,
        TIME
    }

    public Literal {
        Objects.requireNonNull(literalType, "Literal type cannot be null");

        // Validate type matches value
        if (value != null) {
            switch (literalType) {
                case STRING -> {
                    if (!(value instanceof String)) {
                        throw new IllegalArgumentException("STRING literal must have String value");
                    }
                }
                case INTEGER -> {
                    if (!(value instanceof Number)) {
                        throw new IllegalArgumentException("INTEGER literal must have Number value");
                    }
                }
                case BOOLEAN -> {
                    if (!(value instanceof Boolean)) {
                        throw new IllegalArgumentException("BOOLEAN literal must have Boolean value");
                    }
                }
                case DOUBLE, DECIMAL -> {
                    if (!(value instanceof Number)) {
                        throw new IllegalArgumentException("DOUBLE/DECIMAL literal must have Number value");
                    }
                }
                case NULL -> {
                    if (value != null) {
                        throw new IllegalArgumentException("NULL literal cannot have a value");
                    }
                }
                case DATE, TIMESTAMP, TIME -> {
                    if (!(value instanceof String)) {
                        throw new IllegalArgumentException("DATE/TIMESTAMP/TIME literal must have String value");
                    }
                }
            }
        }
    }

    public static Literal string(String value) {
        return new Literal(value, LiteralType.STRING);
    }

    public static Literal integer(int value) {
        return new Literal(value, LiteralType.INTEGER);
    }

    public static Literal integer(long value) {
        return new Literal(value, LiteralType.INTEGER);
    }

    public static Literal decimal(double value) {
        return new Literal(value, LiteralType.DECIMAL);
    }

    public static Literal bool(boolean value) {
        return new Literal(value, LiteralType.BOOLEAN);
    }

    public static Literal nullValue() {
        return new Literal(null, LiteralType.NULL);
    }

    /**
     * Factory for DATE literals. Value should be in 'YYYY-MM-DD' format.
     */
    public static Literal date(String value) {
        return new Literal(value, LiteralType.DATE);
    }

    /**
     * Factory for TIMESTAMP literals. Value should be in 'YYYY-MM-DD HH:MM:SS+ZZZZ' format.
     */
    public static Literal timestamp(String value) {
        return new Literal(value, LiteralType.TIMESTAMP);
    }

    /**
     * Factory for TIME literals. Value should be in 'HH:MM:SS' format.
     */
    public static Literal time(String value) {
        return new Literal(value, LiteralType.TIME);
    }

    /**
     * Convenience factory for integer literals.
     */
    public static Literal of(int value) {
        return integer(value);
    }

    /**
     * Convenience factory for string literals.
     */
    public static Literal of(String value) {
        return string(value);
    }

    /**
     * Convenience factory for null literals.
     */
    public static Literal ofNull() {
        return nullValue();
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitLiteral(this);
    }

    @Override
    public PureType type() {
        return switch (literalType) {
            case STRING -> PureType.STRING;
            case INTEGER -> PureType.INTEGER;
            case BOOLEAN -> PureType.BOOLEAN;
            case DOUBLE -> PureType.FLOAT;
            case DECIMAL -> PureType.DECIMAL;
            case NULL -> PureType.UNKNOWN;
            case DATE -> PureType.STRICT_DATE;
            case TIMESTAMP -> PureType.DATE_TIME;
            case TIME -> PureType.STRICT_TIME;
        };
    }

    @Override
    public String toString() {
        if (literalType == LiteralType.NULL) {
            return "NULL";
        }
        if (literalType == LiteralType.STRING) {
            return "'" + value + "'";
        }
        if (literalType == LiteralType.DATE) {
            return "DATE '" + value + "'";
        }
        if (literalType == LiteralType.TIMESTAMP) {
            return "TIMESTAMP '" + value + "'";
        }
        if (literalType == LiteralType.TIME) {
            return "TIME '" + value + "'";
        }
        return String.valueOf(value);
    }
}
