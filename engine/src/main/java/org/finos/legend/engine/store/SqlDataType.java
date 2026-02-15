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
    DECIMAL,
    SEMISTRUCTURED;

    /**
     * Maps this SQL data type to its corresponding Pure GenericType.
     */
    public org.finos.legend.engine.plan.GenericType toGenericType() {
        return switch (this) {
            case VARCHAR -> org.finos.legend.engine.plan.GenericType.Primitive.STRING;
            case INTEGER -> org.finos.legend.engine.plan.GenericType.Primitive.INTEGER;
            case BIGINT -> org.finos.legend.engine.plan.GenericType.Primitive.INTEGER;
            case BOOLEAN -> org.finos.legend.engine.plan.GenericType.Primitive.BOOLEAN;
            case DATE -> org.finos.legend.engine.plan.GenericType.Primitive.STRICT_DATE;
            case TIMESTAMP -> org.finos.legend.engine.plan.GenericType.Primitive.DATE_TIME;
            case DOUBLE -> org.finos.legend.engine.plan.GenericType.Primitive.FLOAT;
            case DECIMAL -> org.finos.legend.engine.plan.GenericType.Primitive.DECIMAL;
            case SEMISTRUCTURED -> org.finos.legend.engine.plan.GenericType.Primitive.JSON;
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
