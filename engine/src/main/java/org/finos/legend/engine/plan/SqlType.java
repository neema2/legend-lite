package org.finos.legend.engine.plan;

/**
 * Represents SQL types for expressions in the IR.
 * Used for type-aware SQL generation, particularly for CAST insertion.
 */
public enum SqlType {
    INTEGER,
    BIGINT,
    DOUBLE,
    VARCHAR,
    BOOLEAN,
    JSON,
    LIST,
    DATE,
    TIME,
    TIMESTAMP,
    UNKNOWN;

    /**
     * @return true if this type is numeric (can be used in arithmetic)
     */
    public boolean isNumeric() {
        return this == INTEGER || this == DOUBLE;
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
        // Only cast VARCHAR and JSON to numeric - UNKNOWN type columns should pass
        // through
        // since they may be correctly typed integers that shouldn't be converted to
        // DOUBLE
        return this == VARCHAR || this == JSON;
    }
}
