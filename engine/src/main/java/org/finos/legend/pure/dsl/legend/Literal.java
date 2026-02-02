package org.finos.legend.pure.dsl.legend;

/**
 * Literal values: strings, numbers, booleans, dates, null.
 */
public record Literal(Object value, Type type) implements Expression {

    public enum Type {
        STRING, INTEGER, DECIMAL, BOOLEAN, STRICT_DATE, DATETIME, STRICT_TIME, NULL
    }

    public static Literal string(String value) {
        return new Literal(value, Type.STRING);
    }

    public static Literal integer(long value) {
        return new Literal(value, Type.INTEGER);
    }

    public static Literal decimal(double value) {
        return new Literal(value, Type.DECIMAL);
    }

    public static Literal bool(boolean value) {
        return new Literal(value, Type.BOOLEAN);
    }

    public static Literal nil() {
        return new Literal(null, Type.NULL);
    }
}
