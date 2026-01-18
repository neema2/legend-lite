package org.finos.legend.pure.dsl.m2m;

import java.util.Objects;

/**
 * Represents a literal value in an M2M expression.
 * 
 * Examples:
 * - 'Hello'
 * - 42
 * - 3.14
 * - true
 * 
 * @param value The literal value
 * @param type The type of the literal
 */
public record M2MLiteral(Object value, LiteralType type) implements M2MExpression {
    
    public enum LiteralType {
        STRING,
        INTEGER,
        FLOAT,
        BOOLEAN
    }
    
    public M2MLiteral {
        Objects.requireNonNull(type, "Literal type cannot be null");
    }
    
    public static M2MLiteral string(String value) {
        return new M2MLiteral(value, LiteralType.STRING);
    }
    
    public static M2MLiteral integer(long value) {
        return new M2MLiteral(value, LiteralType.INTEGER);
    }
    
    public static M2MLiteral floatVal(double value) {
        return new M2MLiteral(value, LiteralType.FLOAT);
    }
    
    public static M2MLiteral bool(boolean value) {
        return new M2MLiteral(value, LiteralType.BOOLEAN);
    }
    
    @Override
    public <T> T accept(M2MExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
    
    @Override
    public String toString() {
        return switch (type) {
            case STRING -> "'" + value + "'";
            default -> String.valueOf(value);
        };
    }
}
