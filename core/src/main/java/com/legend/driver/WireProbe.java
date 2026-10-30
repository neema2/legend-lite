// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.compiler.element.type.Type;
import com.legend.exec.WireTypes;

/**
 * The wire table's Pure types as the typer's (the probe half of {@code exec.WireTypes} until D71, 2026-10-30): the
 * plan NAMES a type, the driver side rebuilds it. The probes this class held are deleted by the planner-driver seam
 * program: the reconcile (a wire-decided bare store column re-typed to the kind the database reported) by S3b, the
 * canon asking the value in the statement ({@code CanonicalRenderSql.textOrStamp}); the staticizer (a raw grid framed
 * by a describe at planning) by S3c, a raw grid now a relation of the columns its result reports
 * ({@code Statements.reportedRows}). Nothing here reads a database.
 */
public final class WireProbe {
    private WireProbe() {
    }

    /** The compiler's type for a SQL type the database reports: the plan's NAME of it ({@link WireTypes#pureTypeNameOf},
     *  the wire table's) rebuilt as the typer's primitive on the driver side (D72, 2026-10-30: the runtime names a type,
     *  never holds one); an unmapped SQL type is refused by the table, by name. */
    public static Type pureOfSqlType(String sqlType) {
        return primitive(WireTypes.pureTypeNameOf(sqlType));
    }

    /** {@link #pureOfSqlType}'s lookup: null for an unmapped SQL type (a late-bound grid's column then the trusted Any). */
    public static @com.legend.base.Nullable Type pureOfSqlTypeOrNull(String sqlType) {
        String name = WireTypes.pureTypeNameOrNull(sqlType);
        return name == null ? null : primitive(name);
    }

    private static Type primitive(String name) {
        for (Type.Primitive p : Type.Primitive.values()) {
            if (p.typeName().equals(name)) {
                return p;
            }
        }
        throw new IllegalStateException("the wire table names '" + name + "', no primitive of the typer's");
    }

}
