package com.legend.sql;

/**
 * The SQL layer's OWN type vocabulary (LEGEND_SQL_VISION.md prerequisite #1):
 * logical SQL types, independent of any frontend's type system. Frontends map
 * their types to these at the lowering boundary (Pure Integer → BIGINT is the
 * FRONTEND's 64-bit decision); dialects map these to spellings.
 */
public sealed interface SqlType {

    enum Scalar implements SqlType {
        BOOLEAN, BIGINT, DOUBLE, VARCHAR, DATE, TIMESTAMP, JSON
    }

    record Decimal(int precision, int scale) implements SqlType {
    }

    record Array(SqlType element) implements SqlType {
    }

    /**
     * A named-field composite (DuckDB/BigQuery {@code STRUCT}, Postgres
     * {@code ROW}). Field order is load-bearing — it is the layout the
     * emitting frontend declared, never inferred from data.
     */
    record Struct(java.util.List<Field> fields) implements SqlType {
        public Struct {
            fields = java.util.List.copyOf(fields);
        }

        public record Field(String name, SqlType type) {
        }
    }
}
