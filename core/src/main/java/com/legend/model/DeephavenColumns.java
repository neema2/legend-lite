// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.model;

/**
 * Which {@code ###Relational} column types a Deephaven column holds (docs/DEEPHAVEN_PLAN_2026_10_22.md §3): what the
 * model door checks when a runtime binds a store to a Deephaven connection, so a store Deephaven cannot hold is refused
 * before any query, naming its table, its column and the type. Data, kept in the model: the SQL layer never sees a
 * store's types, and the compiler, which checks the binding, reaches the model only. The table is proposed; the
 * Deephaven lanes measure it (L3).
 */
public final class DeephavenColumns {

    private DeephavenColumns() {
    }

    /** The Deephaven column type that holds every value of {@code type}, or null when none does yet. */
    public static @com.legend.base.Nullable String column(RelationalDataType type) {
        return switch (type) {
            case RelationalDataType.Bit t -> "Boolean";
            case RelationalDataType.TinyInt t -> "byte";
            case RelationalDataType.SmallInt t -> "short";
            case RelationalDataType.Integer_ t -> "int";
            case RelationalDataType.BigInt t -> "long";
            case RelationalDataType.Real t -> "float";
            case RelationalDataType.Float_ t -> "double";
            case RelationalDataType.Double_ t -> "double";
            case RelationalDataType.Decimal t -> "BigDecimal";
            case RelationalDataType.Numeric t -> "BigDecimal";
            // the size is not enforced, as on DuckDB
            case RelationalDataType.Varchar t -> "String";
            case RelationalDataType.Char_ t -> "String";
            case RelationalDataType.Date_ t -> "LocalDate";
            case RelationalDataType.Time t -> "LocalTime";
            case RelationalDataType.Timestamp t -> "Instant";
            // refused until a case asks for one
            case RelationalDataType.TimestampNs t -> null;
            case RelationalDataType.Binary t -> null;
            case RelationalDataType.Varbinary t -> null;
            case RelationalDataType.Distinct t -> null;
            case RelationalDataType.Other t -> null;
            case RelationalDataType.SemiStructured t -> null;
            case RelationalDataType.Array t -> null;
            case RelationalDataType.Object_ t -> null;
        };
    }

    /** {@code type} as a store spells it, for a refusal. */
    public static String spelled(RelationalDataType type) {
        return switch (type) {
            case RelationalDataType.Bit t -> "BIT";
            case RelationalDataType.TinyInt t -> "TINYINT";
            case RelationalDataType.SmallInt t -> "SMALLINT";
            case RelationalDataType.Integer_ t -> "INTEGER";
            case RelationalDataType.BigInt t -> "BIGINT";
            case RelationalDataType.Real t -> "REAL";
            case RelationalDataType.Float_ t -> "FLOAT";
            case RelationalDataType.Double_ t -> "DOUBLE";
            case RelationalDataType.Decimal t -> "DECIMAL(" + t.precision() + ", " + t.scale() + ")";
            case RelationalDataType.Numeric t -> "NUMERIC(" + t.precision() + ", " + t.scale() + ")";
            case RelationalDataType.Varchar t -> "VARCHAR(" + t.size() + ")";
            case RelationalDataType.Char_ t -> "CHAR(" + t.size() + ")";
            case RelationalDataType.Date_ t -> "DATE";
            case RelationalDataType.Time t -> "TIME";
            case RelationalDataType.Timestamp t -> "TIMESTAMP";
            case RelationalDataType.TimestampNs t -> "TIMESTAMP_NS";
            case RelationalDataType.Binary t -> "BINARY(" + t.size() + ")";
            case RelationalDataType.Varbinary t -> "VARBINARY(" + t.size() + ")";
            case RelationalDataType.Distinct t -> "DISTINCT";
            case RelationalDataType.Other t -> "OTHER";
            case RelationalDataType.SemiStructured t -> "SEMISTRUCTURED";
            case RelationalDataType.Array t -> "ARRAY";
            case RelationalDataType.Object_ t -> "OBJECT";
        };
    }
}
