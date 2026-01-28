package org.finos.legend.engine.store;

import java.util.Objects;
import java.util.Optional;

/**
 * Maps a Pure property to a relational column or expression.
 * 
 * Supports two modes:
 * 1. Simple column mapping: propertyName -> columnName
 * 2. Expression mapping: propertyName -> expression (e.g.,
 * PAYLOAD->get('price', @Integer))
 * 
 * @param propertyName     The Pure property name (e.g., "price")
 * @param columnName       The relational column name (e.g., "PRICE" or
 *                         "PAYLOAD")
 * @param expressionString The full expression (null for simple column mappings)
 */
public record PropertyMapping(
        String propertyName,
        String columnName,
        String expressionString) {
    public PropertyMapping {
        Objects.requireNonNull(propertyName, "Property name cannot be null");
        Objects.requireNonNull(columnName, "Column name cannot be null");

        if (propertyName.isBlank()) {
            throw new IllegalArgumentException("Property name cannot be blank");
        }
        if (columnName.isBlank()) {
            throw new IllegalArgumentException("Column name cannot be blank");
        }
    }

    /**
     * Creates a simple column mapping.
     */
    public PropertyMapping(String propertyName, String columnName) {
        this(propertyName, columnName, null);
    }

    /**
     * Creates a simple column mapping.
     */
    public static PropertyMapping column(String propertyName, String columnName) {
        return new PropertyMapping(propertyName, columnName, null);
    }

    /**
     * Creates an expression-based mapping.
     */
    public static PropertyMapping expression(String propertyName, String columnName, String expression) {
        return new PropertyMapping(propertyName, columnName, expression);
    }

    /**
     * @return true if this property uses an expression mapping
     */
    public boolean hasExpression() {
        return expressionString != null;
    }

    /**
     * @return Optional containing the expression string, if present
     */
    public Optional<String> getExpressionString() {
        return Optional.ofNullable(expressionString);
    }

    @Override
    public String toString() {
        if (expressionString != null) {
            return propertyName + " -> " + expressionString;
        }
        return propertyName + " -> " + columnName;
    }
}
