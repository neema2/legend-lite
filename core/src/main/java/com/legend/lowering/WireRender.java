// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * E5 (JAVA_EVICTION_PLAN): the PRODUCT wire text is a PLAN PROJECTION —
 * the database renders the CSV/JSON bytes a product consumer observes
 * (the E1 PCT precedent, generalized to the product surface). Pure SQL
 * composition over an already-lowered plan: shape-agnostic wrapping,
 * never rewriting (F4.4).
 */
public final class WireRender {

    /** The product wire formats the plan can render. */
    public enum Format { CSV, JSON }

    private WireRender() {
    }

    /** The wire's typed relation: a tabular root's own schema; a scalar or
     * collection root is the one-column {@code value} relation (the
     * scalarRoot contract). */
    public static Type.RelationType schema(com.legend.compiler.element.type.ExprType info) {
        Type.RelationType rt = Type.schemaView(info.type());
        return rt != null ? rt
                : new Type.RelationType(List.of(new Type.Column("value", info.type(), info.multiplicity())));
    }

    /** The whole result as ONE text value ("wire" column): RFC 4180 CSV
     * or the {@code [row,…]} JSON array. {@code schema} is the root's
     * typed relation — CSV cells dispatch on the PURE column kind
     * (F5.4), JSON rides the database's own {@code json_object} typing. */
    public static SqlQuery wrap(SqlQuery plan, Type.RelationType schema,
            Format format) {
        SqlSelect inner = asSelect(plan);
        if (format == Format.JSON) {
            return Render.jsonWire(inner, "_wire1");
        }
        Map<String, Type.Column> byName = new LinkedHashMap<>();
        for (Type.Column c : schema.columns()) {
            byName.put(c.name(), c);
        }
        List<Type.Column> relCols = new ArrayList<>();
        for (var oc : inner.outputs()) {
            Type.Column t = byName.get(oc.name());
            if (t == null) {
                throw new com.legend.error.NotImplementedException(
                        "csv wire: output column '" + oc.name()
                        + "' has no typed relation column (dynamic-column"
                        + " plans are not wire-rendered yet)");
            }
            relCols.add(t);
        }
        return Render.csvWire(inner, relCols, "_wire1");
    }

    /** The STREAMING JSON form: one {@code _wire_row} object text per
     * JDBC row (order preserved on the wrapper) — the executor writes
     * only the array punctuation. */
    public static SqlQuery rows(SqlQuery plan) {
        return Render.jsonWireRows(asSelect(plan), "_wire1");
    }

    private static SqlSelect asSelect(SqlQuery plan) {
        return plan instanceof SqlSelect s ? s
                : SqlSelect.starOf(new SqlSource.Subselect(plan, "_wire0", null));
    }
}
