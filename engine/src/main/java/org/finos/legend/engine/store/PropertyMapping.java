package org.finos.legend.engine.store;

import java.util.Map;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Maps a Pure property to a relational column or expression.
 * 
 * Supports three modes:
 * 1. Simple column mapping: propertyName -> columnName
 * 2. Expression mapping: propertyName -> expression (e.g.,
 * PAYLOAD->get('price', @Integer))
 * 3. Enum column mapping: propertyName -> CASE WHEN column=val1 THEN enum1 ...
 * 
 * @param propertyName     The Pure property name (e.g., "price")
 * @param columnName       The relational column name (e.g., "PRICE" or
 *                         "PAYLOAD")
 * @param expressionString The full expression (null for simple column mappings)
 * @param enumMapping      Map from enum value name to list of db values (null
 *                         if not enum)
 * @param enumType         The enum type name (null if not enum)
 */
public record PropertyMapping(
        String propertyName,
        String columnName,
        String expressionString,
        Map<String, List<Object>> enumMapping,
        String enumType) {
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
        this(propertyName, columnName, null, null, null);
    }

    /**
     * Creates a simple column mapping.
     */
    public static PropertyMapping column(String propertyName, String columnName) {
        return new PropertyMapping(propertyName, columnName, null, null, null);
    }

    /**
     * Creates an expression-based mapping.
     */
    public static PropertyMapping expression(String propertyName, String columnName, String expression) {
        return new PropertyMapping(propertyName, columnName, expression, null, null);
    }

    /**
     * Creates an enum column mapping with db-to-enum value translations.
     * 
     * @param propertyName The property name
     * @param columnName   The column containing database values
     * @param enumType     The target enum type name
     * @param enumMapping  Map from enum value name to list of source db values
     */
    public static PropertyMapping enumColumn(String propertyName, String columnName,
            String enumType, Map<String, List<Object>> enumMapping) {
        return new PropertyMapping(propertyName, columnName, null, Map.copyOf(enumMapping), enumType);
    }

    /**
     * @return true if this property uses an expression mapping
     */
    public boolean hasExpression() {
        return expressionString != null;
    }

    /**
     * @return true if this property uses an enumeration mapping
     */
    public boolean hasEnumMapping() {
        return enumMapping != null && !enumMapping.isEmpty();
    }

    /**
     * @return Optional containing the expression string, if present
     */
    public Optional<String> getExpressionString() {
        return Optional.ofNullable(expressionString);
    }

    @Override
    public String toString() {
        if (enumMapping != null) {
            return propertyName + " -> CASE " + columnName + " [enum]";
        }
        if (expressionString != null) {
            return propertyName + " -> " + expressionString;
        }
        return propertyName + " -> " + columnName;
    }
}
