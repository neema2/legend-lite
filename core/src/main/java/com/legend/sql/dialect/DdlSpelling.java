// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlDdl.ColumnType;

/**
 * What every dialect's DDL type table shares: a sized or scaled type as the store declared it, and the refusal of a
 * type no DDL creates. Each dialect's {@code ddlType} is its own whole table (2026-10-15, the dialect program's owner
 * guard; until then H2's spellings were the base every dialect overrode a few of), the same shape as its query
 * rendering. The engine text's table is here because the metamodel walk's {@code dataTypeToSqlText} native reads it
 * too ({@link #engineText}).
 */
public final class DdlSpelling {

    private DdlSpelling() {
    }

    /** A sized declared type as the store wrote it: {@code VARCHAR(20)}. */
    public static String sized(ColumnType.Sized z) {
        return z.kind() + "(" + z.size() + ")";
    }

    /** A scaled declared type as the store wrote it: {@code DECIMAL(10, 2)}. */
    public static String scaled(ColumnType.Scaled c) {
        return c.kind() + "(" + c.precision() + ", " + c.scale() + ")";
    }

    /** A declared column type no DDL can create (Other, Distinct, Array, Object where a database has none). */
    public static IllegalStateException unspelled(ColumnType.Kind kind) {
        return new IllegalStateException("no DDL spelling for declared column type " + kind);
    }

    /** THE engine {@code dataTypeToSqlText} spelling
     * (platform_store_relational/functions.pure:68-96), spelled ONCE —
     * the engine-text DDL and the metamodel walk's dataTypeToSqlText
     * native both read here (ratified E4 design: no type text is spelled
     * twice). Where it differs from H2's DDL: Integer spells INT; Other spells
     * OTHER (execution walls — a column of type Other cannot be created). */
    public static String engineText(ColumnType t) {
        return switch (t) {
            case ColumnType.Plain p -> switch (p.kind()) {
                case BIGINT -> "BIGINT";
                case SMALLINT -> "SMALLINT";
                case TINYINT -> "TINYINT";
                case INTEGER -> "INT";
                case FLOAT -> "FLOAT";
                case DOUBLE -> "DOUBLE";
                case REAL -> "REAL";
                case BIT -> "BIT";
                case TIMESTAMP -> "TIMESTAMP";
                case DATE -> "DATE";
                case TIME -> "TIME";
                case TIMESTAMP_NS -> "TIMESTAMP_NS";
                case JSON -> "JSON";
                case VARCHAR -> "VARCHAR";
                case OTHER -> "OTHER";
                case DISTINCT, ARRAY, OBJECT -> throw unspelled(p.kind());
            };
            case ColumnType.Sized z -> sized(z);
            case ColumnType.Scaled c -> scaled(c);
        };
    }
}
