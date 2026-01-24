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
        NULL
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
                case DOUBLE -> {
                    if (!(value instanceof Number)) {
                        throw new IllegalArgumentException("DOUBLE literal must have Number value");
                    }
                }
                case NULL -> {
                    if (value != null) {
                        throw new IllegalArgumentException("NULL literal cannot have a value");
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

    public static Literal bool(boolean value) {
        return new Literal(value, LiteralType.BOOLEAN);
    }

    public static Literal nullValue() {
        return new Literal(null, LiteralType.NULL);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitLiteral(this);
    }

    @Override
    public SqlType type() {
        return switch (literalType) {
            case STRING -> SqlType.VARCHAR;
            case INTEGER -> SqlType.INTEGER;
            case BOOLEAN -> SqlType.BOOLEAN;
            case DOUBLE -> SqlType.DOUBLE;
            case NULL -> SqlType.UNKNOWN;
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
        return String.valueOf(value);
    }
}
