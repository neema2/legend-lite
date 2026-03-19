package com.gs.legend.model.store;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    /**
     * Pre-parsed expression access (e.g., ->get('price', @Integer)).
     * Parsed at construction time so consumers don't need regex.
     *
     * @param jsonKey  The JSON key to access (e.g., "price")
     * @param castType The Pure type to cast to (nullable, e.g., "Integer")
     */
    public record ExpressionAccess(String jsonKey, String castType) {
    }

    private static final Pattern GET_PATTERN = Pattern.compile("->get\\('(\\w+)'(?:,\\s*@(\\w+))?\\)");

    /**
     * Parses the expression string into a structured ExpressionAccess.
     * Returns empty if no expression or if the pattern doesn't match.
     */
    public Optional<ExpressionAccess> expressionAccess() {
        if (expressionString == null)
            return Optional.empty();
        Matcher m = GET_PATTERN.matcher(expressionString);
        if (m.find()) {
            return Optional.of(new ExpressionAccess(m.group(1), m.group(2)));
        }
        return Optional.empty();
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
