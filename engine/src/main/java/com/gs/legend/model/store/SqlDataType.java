package com.gs.legend.model.store;

import com.gs.legend.model.m3.PrimitiveType;

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
    DECIMAL,
    SEMISTRUCTURED;

    /**
     * Maps this SQL data type to its corresponding Pure GenericType.
     */
    public com.gs.legend.plan.GenericType toGenericType() {
        return switch (this) {
            case VARCHAR -> com.gs.legend.plan.GenericType.Primitive.STRING;
            case INTEGER -> com.gs.legend.plan.GenericType.Primitive.INTEGER;
            case BIGINT -> com.gs.legend.plan.GenericType.Primitive.INTEGER;
            case BOOLEAN -> com.gs.legend.plan.GenericType.Primitive.BOOLEAN;
            case DATE -> com.gs.legend.plan.GenericType.Primitive.STRICT_DATE;
            case TIMESTAMP -> com.gs.legend.plan.GenericType.Primitive.DATE_TIME;
            case DOUBLE -> com.gs.legend.plan.GenericType.Primitive.FLOAT;
            case DECIMAL -> com.gs.legend.plan.GenericType.Primitive.DECIMAL;
            case SEMISTRUCTURED -> com.gs.legend.plan.GenericType.Primitive.JSON;
        };
    }

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
            case STRICT_DATE -> DATE; // StrictDate maps to SQL DATE
            case DATE_TIME -> TIMESTAMP; // DateTime maps to SQL TIMESTAMP
            case FLOAT -> DOUBLE;
            case DECIMAL -> DECIMAL;
        };
    }
}
