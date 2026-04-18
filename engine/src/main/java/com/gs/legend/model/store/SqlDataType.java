package com.gs.legend.model.store;

import com.gs.legend.model.m3.Type;

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
            case DECIMAL -> com.gs.legend.plan.GenericType.DEFAULT_DECIMAL;
            case SEMISTRUCTURED -> com.gs.legend.plan.GenericType.Primitive.JSON;
        };
    }

    /**
     * Maps a Pure primitive type ({@link Type.Primitive}) to its corresponding SQL type.
     *
     * <p>The mapping is intentionally coarse — this enum was designed for the eight
     * legacy {@code m3.PrimitiveType} values and does not preserve VARCHAR size,
     * DECIMAL precision/scale, or distinguish BIGINT / SMALLINT / TINYINT. See the
     * Phase B findings document for the planned post-Phase-B enhancement.
     *
     * <p>Throws for primitives that have no direct SQL column mapping
     * ({@code ANY}, {@code NIL}).
     */
    public static SqlDataType fromPrimitive(Type.Primitive primitive) {
        return switch (primitive) {
            case STRING -> VARCHAR;
            case INTEGER, INT64, INT128 -> INTEGER;
            case NUMBER, DECIMAL -> DECIMAL;
            case FLOAT -> DOUBLE;
            case BOOLEAN -> BOOLEAN;
            case DATE, STRICT_DATE -> DATE;
            case DATE_TIME, STRICT_TIME -> TIMESTAMP;
            case JSON -> SEMISTRUCTURED;
            case ANY, NIL -> throw new IllegalArgumentException(
                    "Pure primitive '" + primitive.pureName() + "' has no direct SQL column mapping");
        };
    }
}
