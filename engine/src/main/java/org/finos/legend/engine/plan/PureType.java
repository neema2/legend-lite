package org.finos.legend.engine.plan;

/**
 * Represents Pure language types for expressions in the IR.
 * Mirrors Pure's type system, not SQL's.
 * The SQL generator is responsible for mapping PureType to dialect-specific SQL types.
 */
public enum PureType {
    // Numeric
    INTEGER,
    FLOAT,
    DECIMAL,
    NUMBER,

    // Text
    STRING,

    // Boolean
    BOOLEAN,

    // Temporal
    DATE,          // Pure Date (supertype of StrictDate and DateTime)
    STRICT_DATE,   // Pure StrictDate (date without time)
    DATE_TIME,     // Pure DateTime (date with time)
    STRICT_TIME,   // Pure StrictTime (time without date)

    // Collections
    LIST,

    // Special
    JSON,          // Variant/semi-structured data
    ENUM,          // Enumeration value

    UNKNOWN;

    /**
     * Maps a Pure type name (e.g., "Integer", "StrictDate") to the corresponding PureType.
     */
    public static PureType fromTypeName(String typeName) {
        return switch (typeName) {
            case "Integer" -> INTEGER;
            case "Float" -> FLOAT;
            case "Decimal" -> DECIMAL;
            case "Number" -> NUMBER;
            case "String" -> STRING;
            case "Boolean" -> BOOLEAN;
            case "Date" -> DATE;
            case "StrictDate" -> STRICT_DATE;
            case "DateTime" -> DATE_TIME;
            case "StrictTime" -> STRICT_TIME;
            default -> UNKNOWN;
        };
    }

    /**
     * @return true if this type is numeric (can be used in arithmetic)
     */
    public boolean isNumeric() {
        return this == INTEGER || this == FLOAT || this == DECIMAL || this == NUMBER;
    }

    /**
     * @return true if this type is temporal (date/time)
     */
    public boolean isTemporal() {
        return this == DATE || this == STRICT_DATE || this == DATE_TIME || this == STRICT_TIME;
    }

    /**
     * @return true if this type is JSON-related
     */
    public boolean isJson() {
        return this == JSON || this == LIST;
    }

    /**
     * @return true if this type needs CAST for arithmetic operations
     */
    public boolean needsNumericCast() {
        return this == STRING || this == JSON;
    }
}
