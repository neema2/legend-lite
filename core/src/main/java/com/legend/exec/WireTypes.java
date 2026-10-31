// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.sql.SqlType;

import java.sql.Types;
import java.util.List;

/** THE WIRE TABLE: JDBC's type codes and a database's SQL type names to the SQL vocabulary and the Pure kind a
 * column delivers ({@link #sqlTypeOf}, {@link #pureTypeNameOf}), and a column as a result REPORTS it ({@link #column}):
 * a raw grid's columns, and a raw grid's column read by name ({@link #decodedAs}), are read as the database reports
 * them when the plan runs (the planner-driver seam program's S3c, 2026-10-30; until then the root's {@code WireProbe}
 * prepared statements at planning to reconcile them). This class decides nothing. */
public final class WireTypes {

    private WireTypes() {
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

    /** {@link #pureTypeNameOf}'s lookup: null for an unmapped SQL type (a raw grid's reported column of it refused
     *  by name: {@link #column}). The parameter suffix strips once ({@code DECIMAL(38,9)} is {@code DECIMAL}); the
     *  table is exact-match. */
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

    /** A column a result reports, as the plan's: its label; the Pure type this table names for its SQL type, and
     *  that type's slot; nullable as reported. The runtime NAMES the type, as the plan does (D72). A type JDBC's code
     *  or this table names no Pure type for is refused by name, never read as some kind (AGENTS.md invariant 4; the
     *  seam program's S3c audit, 2026-10-31: until then it was Any, its cell the driver's own kind). */
    public static ExecutionPlan.Column column(Statements.Reported r) {
        String slot = r.type() == null ? null : com.legend.executionplan.ColumnFacts.slot(r.type());
        String pure = slot == null ? null : pureTypeNameOrNull(slot);
        if (pure == null) {
            throw new com.legend.error.NotImplementedException("a raw grid's column '" + r.name() + "' of the"
                    + " database's type " + r.driverType() + ": no Pure type is named for it (WireTypes)");
        }
        return new ExecutionPlan.Column(r.name(), pure, new ExecutionPlan.Multiplicity(r.nullable() ? 0 : 1, 1), slot,
                null, null);
    }

    /** {@code declared} as the cells are read: a column whose kind is the wire's (no slot, Any: a raw grid's column
     *  read by name, {@code ColumnFacts.rawGridCell}) takes the type its result reports, {@link #column}; every other
     *  as declared. Its name and multiplicity stay the plan's. */
    static ExecutionPlan.Column decodedAs(ExecutionPlan.Column declared, Statements.Reported reported) {
        if (declared.slot() != null || !com.legend.result.RowDecode.ANY.equals(declared.type())) {
            return declared;
        }
        ExecutionPlan.Column wire = column(reported);
        return new ExecutionPlan.Column(declared.name(), wire.type(), declared.multiplicity(), wire.slot(),
                declared.store(), declared.precision());
    }
}
