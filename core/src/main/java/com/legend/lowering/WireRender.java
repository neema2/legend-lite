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

    /** Whose text a JSON wire's values carry (docs/OUTPUT_LAYER_DESIGN_2026_10_10.md §4, decision 2, 2026-10-18):
     *  {@code PURE}, interpreted Pure's own print forms, on lite's own outputs; {@code ENGINE}, legend-engine's
     *  conventions -- a Float as Java prints a double, a DateTime with nine fraction digits -- on the engine's API
     *  ({@code pure/v1} execute, served and hosted). The VALUES are Pure's either way. */
    /** The JSON wire's text of a value: Pure's ({@code PURE}), or legend-engine's ({@code ENGINE}: the pure/v1 API).
     *  (The CARRIER spellings of the judges' typed rows, step 6, went with the JSON rows wire's typed use: since
     *  step 7, 2026-10-18, a session answers typed rows from the driver,
     *  docs/PLAN_AND_RESULT_MODEL_2026_10_18.md §3.) */
    public enum Spelling {
        PURE, ENGINE
    }

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
        return wrap(plan, schema, format, Spelling.PURE);
    }

    /** {@link #wrap(SqlQuery, Type.RelationType, Format)} with the JSON wire's values in {@code spelling}'s text;
     *  the CSV is lite's own and has one. */
    public static SqlQuery wrap(SqlQuery plan, Type.RelationType schema, Format format, Spelling spelling) {
        SqlSelect inner = asSelect(plan);
        if (dynamicPivotUnnamed(plan, schema)) {
            // a dynamic pivot's wire is its keyed plan's (KeyedPivot, 2026-10-20), made before any wrapper: a wire
            // over one would print its fixed columns alone
            throw new IllegalStateException("a wire over a dynamic pivot nothing names: its wire is the keyed plan's"
                    + " (KeyedPivot), not a wrapper's");
        }
        if (format == Format.JSON) {
            return Render.jsonWire(inner, typedOrNull(inner, schema), "_wire1", spelling);
        }
        if (spelling != Spelling.PURE) {
            throw new IllegalArgumentException("the CSV wire has one spelling, Pure's; the engine's is the JSON wire's");
        }
        Map<String, Type.Column> byName = new LinkedHashMap<>();
        for (Type.Column c : schema.columns()) {
            byName.put(c.name(), c);
        }
        List<Type.Column> relCols = new ArrayList<>();
        for (var oc : Render.presented(inner.outputs())) {
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
    public static SqlQuery rows(SqlQuery plan, Type.RelationType schema) {
        return rows(plan, schema, Spelling.PURE);
    }

    /** {@link #rows(SqlQuery, Type.RelationType)} with each cell's token in {@code spelling}. */
    public static SqlQuery rows(SqlQuery plan, Type.RelationType schema, Spelling spelling) {
        if (KeyedPivot.dynamic(plan) != null && !schema.dynamicColumns().isEmpty()) {
            // a dynamic pivot's rows are its keyed plan's (KeyedPivot, 2026-10-20): the rows over one printed its
            // fixed columns alone (DuckDB's streamed rows until then)
            throw new IllegalStateException("rows over a dynamic pivot nothing names: its rows are the keyed plan's"
                    + " (KeyedPivot), not a wrapper's");
        }
        // A statement's own CTEs stay at its head, the wire taken over the body (the plan path program's step 6,
        // 2026-10-18): H2 parses a derived table's text again for each plan it weighs, and a verdict statement's
        // WITH wrapped inside two of them exhausted its heap on the corpus's cross-database bodies, where the same
        // statement sent bare had run. The body itself keeps its projections (the dialects' rewrites find an UNNEST,
        // a list or a variant navigation at a projection's root, never inside the row's text) and is wrapped once.
        if (plan instanceof com.legend.sql.SqlWith w) {
            return w.with(w.ctes(), rows(w.body(), schema, spelling));
        }
        // a UNION ALL's branches each wrapped once (the fused verdict batch), where every branch is a select with no
        // order of its own (an ORDER BY inside a union branch is no SQL); else the union is wrapped whole
        if (plan instanceof com.legend.sql.SqlUnion u && u.all()
                && u.branches().stream().allMatch(b -> b instanceof SqlSelect bs && bs.orderBy().isEmpty())) {
            List<SqlQuery> branches = new ArrayList<>(u.branches().size());
            for (SqlQuery b : u.branches()) {
                SqlSelect bs = (SqlSelect) b;
                branches.add(Render.jsonWireRows(bs, typedOrNull(bs, schema), "_wire1", spelling));
            }
            return com.legend.sql.SqlUnion.ofBranches(branches, true);
        }
        SqlSelect inner = asSelect(plan);
        return Render.jsonWireRows(inner, typedOrNull(inner, schema), "_wire1", spelling);
    }

    /** Each output's typed column, or null where the schema names none (a pivot's generated column): the JSON wires
     *  spell a Float and a Decimal by their Pure type and take the database's token for the rest. */
    private static List<Type.@com.legend.base.Nullable Column> typedOrNull(SqlSelect inner, Type.RelationType schema) {
        Map<String, Type.Column> byName = new LinkedHashMap<>();
        for (Type.Column c : schema.columns()) {
            byName.put(c.name(), c);
        }
        List<Type.@com.legend.base.Nullable Column> out = new ArrayList<>();
        for (var oc : Render.presented(inner.outputs())) {
            out.add(byName.get(oc.name()));
        }
        return out;
    }

    /** A typed-rows plan's statement (the judges', the plan path program's step 7, 2026-10-18): the statement
     *  itself, its outputs unchanged, but for a sort key that is no output, carried as a hidden column of the inner
     *  select and ordered by outside ({@link Render#typedRows}); a WITH keeps its CTEs at the head. */
    public static SqlQuery typedRows(SqlQuery plan) {
        if (plan instanceof com.legend.sql.SqlWith w) {
            return w.with(w.ctes(), typedRows(w.body()));
        }
        if (plan instanceof SqlSelect s && !s.orderBy().isEmpty()) {
            return Render.typedRows(s, "_rows1");
        }
        return plan;
    }

    /** Does {@code plan} answer a dynamic pivot's columns UNNAMED: a pivot whose columns the database names (no
     *  {@code IN} list), and a typed schema that still carries the pivot's templates (its dynamic columns)? A cast
     *  over such a pivot names its columns statically and the lowering projects them (the plan path program's step
     *  7, 2026-10-18): the schema then has no templates, and the plan is an ordinary relation. */
    public static boolean dynamicPivotUnnamed(SqlQuery plan, Type.RelationType schema) {
        return !schema.dynamicColumns().isEmpty() && hasDynamicPivot(plan);
    }

    /** Does {@code plan} hold a pivot whose columns the database names (no {@code IN} list)? */
    public static boolean hasDynamicPivot(SqlQuery plan) {
        return TdsTextWrap.pivots(plan).stream().anyMatch(p -> p.in().isEmpty());
    }

    private static SqlSelect asSelect(SqlQuery plan) {
        return plan instanceof SqlSelect s ? s
                : SqlSelect.starOf(new SqlSource.Subselect(plan, "_wire0", null));
    }
}
