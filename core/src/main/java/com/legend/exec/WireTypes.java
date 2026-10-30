// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.sql.SqlType;

import java.sql.Types;
import java.util.List;

/** THE WIRE TABLE: JDBC's type codes and a database's SQL type names to the SQL vocabulary and the Pure kind a
 * column delivers ({@link #sqlTypeOf}, {@link #pureTypeNameOf}), and a column as the database REPORTS it for a prepared
 * statement ({@link ReportedColumn}, what the session's state memoizes per statement text). The wire kind of a
 * wire-decided column is the database's (leg 3.3, docs/DATABASE_MODE_HOMEWORK_2026_09_18.md §4t): the reconciliation
 * that reads this table -- one prepare through the plan runner, a stamped kind cast to the reported one, the census of
 * what moved -- is the root's {@code WireProbe} since D71 (2026-10-30); this class decides nothing. */
public final class WireTypes {

    private WireTypes() {
    }

    /** A column as the database reports it for a prepared statement: its
     * label, its type in the SQL vocabulary (null outside it), nullability. */
    public record ReportedColumn(String name, @com.legend.base.Nullable SqlType type, boolean nullable) {
    }

    /** JDBC's type code to the SQL vocabulary (the kinds
     * {@code Type.kindOfSqlType} names). */
    static @com.legend.base.Nullable SqlType sqlTypeOf(int jdbcType, int precision, int scale) {
        return switch (jdbcType) {
            case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR,
                    Types.LONGNVARCHAR -> SqlType.Scalar.VARCHAR;
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> SqlType.Scalar.INTEGER;
            case Types.BIGINT -> SqlType.Scalar.BIGINT;
            case Types.FLOAT, Types.REAL, Types.DOUBLE -> SqlType.Scalar.DOUBLE;
            case Types.DECIMAL, Types.NUMERIC -> precision > 0
                    ? new SqlType.Decimal(precision, Math.max(scale, 0)) : null;
            case Types.BIT, Types.BOOLEAN -> SqlType.Scalar.BOOLEAN;
            case Types.DATE -> SqlType.Scalar.DATE;
            case Types.TIME -> SqlType.Scalar.TIME;
            case Types.TIMESTAMP -> SqlType.Scalar.TIMESTAMP;
            case Types.TIMESTAMP_WITH_TIMEZONE -> SqlType.Scalar.TIMESTAMPTZ;
            default -> null;
        };
    }

    /** The Pure primitive's NAME a column the database reports by its SQL type name carries -- a plan column's
     *  {@code type} ({@code ExecutionPlan.Column}: a pivot-generated column, a probed grid's); the compiler's own
     *  {@code Type} is the driver side's to rebuild from it (the root's wire probe; D72, 2026-10-30: the runtime
     *  names a type, never holds one). Every known name explicit -- an unrecognized one is a gap in this table,
     *  refused by name, never a String (audit 15: the silent String default corrupted result typing invisibly). */
    public static String pureTypeNameOf(String sqlType) {
        String t = pureTypeNameOrNull(sqlType);
        if (t == null) {
            throw new IllegalStateException("no Pure primitive mapped for SQL type '" + sqlType
                    + "' (a column the database reports) -- add it to WireTypes.pureTypeNameOrNull");
        }
        return t;
    }

    /** {@link #pureTypeNameOf}'s lookup: null for an unmapped SQL type (a late-bound grid's column then the trusted
     *  Any, GridProbe). The parameter suffix strips once ({@code DECIMAL(38,9)} is {@code DECIMAL}); the table is
     *  exact-match. */
    public static @com.legend.base.Nullable String pureTypeNameOrNull(String sqlType) {
        String t = sqlType.toUpperCase(java.util.Locale.ROOT);
        int paren = t.indexOf('(');
        if (paren > 0) {
            t = t.substring(0, paren).strip();
        }
        return switch (t) {
            case "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT" -> "Integer";
            case "FLOAT", "DOUBLE", "REAL" -> "Float";
            case "BOOLEAN" -> "Boolean";
            case "DATE" -> "StrictDate";
            case "TIMESTAMP" -> "DateTime";
            case "DECIMAL", "NUMERIC" -> "Decimal";
            case "VARCHAR", "CHAR", "TEXT", "STRING", "BPCHAR" -> "String";
            default -> null;
        };
    }
}
