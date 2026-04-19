package com.gs.legend.model.store;

import com.gs.legend.model.m3.Primitive;

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
     * Maps this SQL data type to its corresponding Pure Type.
     */
    public com.gs.legend.model.m3.Type toGenericType() {
        return switch (this) {
            case VARCHAR -> com.gs.legend.model.m3.Primitive.STRING;
            case INTEGER -> com.gs.legend.model.m3.Primitive.INTEGER;
            case BIGINT -> com.gs.legend.model.m3.Primitive.INTEGER;
            case BOOLEAN -> com.gs.legend.model.m3.Primitive.BOOLEAN;
            case DATE -> com.gs.legend.model.m3.Primitive.STRICT_DATE;
            case TIMESTAMP -> com.gs.legend.model.m3.Primitive.DATE_TIME;
            case DOUBLE -> com.gs.legend.model.m3.Primitive.FLOAT;
            case DECIMAL -> com.gs.legend.model.m3.Type.DEFAULT_DECIMAL;
            case SEMISTRUCTURED -> com.gs.legend.model.m3.Primitive.VARIANT;
        };
    }

    /**
     * Maps a Pure primitive type ({@link Primitive}) to its corresponding SQL type.
     *
     * <p>The mapping is intentionally coarse — this enum was designed for the eight
     * legacy {@code m3.PrimitiveType} values and does not preserve VARCHAR size,
     * DECIMAL precision/scale, or distinguish BIGINT / SMALLINT / TINYINT. See the
     * Phase B findings document for the planned post-Phase-B enhancement.
     *
     * <p>Throws for primitives that have no direct SQL column mapping
     * ({@code ANY}, {@code NIL}).
     */
    public static SqlDataType fromPrimitive(Primitive primitive) {
        // Identity-compare against {@code Primitive} singletons. Java pattern-switch on
        // record values would require verbose {@code case Primitive p when p == X ->} guards
        // with mandatory default; the if-chain below is cleaner for a 15-case exhaustive map.
        if (primitive == Primitive.STRING) return VARCHAR;
        if (primitive == Primitive.INTEGER
                || primitive == Primitive.INT64
                || primitive == Primitive.INT128) return INTEGER;
        if (primitive == Primitive.NUMBER || primitive == Primitive.DECIMAL) return DECIMAL;
        if (primitive == Primitive.FLOAT) return DOUBLE;
        if (primitive == Primitive.BOOLEAN) return BOOLEAN;
        if (primitive == Primitive.DATE || primitive == Primitive.STRICT_DATE) return DATE;
        if (primitive == Primitive.DATE_TIME || primitive == Primitive.STRICT_TIME) return TIMESTAMP;
        if (primitive == Primitive.VARIANT) return SEMISTRUCTURED;
        // ANY / NIL have no direct SQL column mapping.
        throw new IllegalArgumentException(
                "Pure primitive '" + primitive.pureName() + "' has no direct SQL column mapping");
    }
}
