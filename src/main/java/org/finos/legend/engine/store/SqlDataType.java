package org.finos.legend.engine.store;

import org.finos.legend.pure.m3.PrimitiveType;

/**
 * Represents SQL data types for relational columns.
 */
public enum SqlDataType {
    VARCHAR,
    INTEGER,
    BIGINT,
    BOOLEAN,
    DATE,
    TIMESTAMP,
    DOUBLE,
    DECIMAL;
    
    /**
     * Maps a Pure primitive type to its corresponding SQL type.
     * 
     * @param primitiveType The Pure primitive type
     * @return The corresponding SQL data type
     */
    public static SqlDataType fromPrimitiveType(PrimitiveType primitiveType) {
        return switch (primitiveType) {
            case STRING -> VARCHAR;
            case INTEGER -> INTEGER;
            case BOOLEAN -> BOOLEAN;
            case DATE -> DATE;
            case FLOAT -> DOUBLE;
            case DECIMAL -> DECIMAL;
        };
    }
}
