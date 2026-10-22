// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.SqlUnion;
import com.legend.sql.SqlWith;

import java.util.ArrayList;
import java.util.List;

/**
 * A DYNAMIC PIVOT's PROBE PLAN, the lowering's half (the plan path program, 2026-10-20, the user's decision after the
 * pivot discussion: docs/PLAN_PATH_PROGRAM_2026_10_17.md §9). A pivot no cast names has columns the data names, one per
 * key of its ON column and aggregate; SQL fixes a result's columns when it prepares the statement, so the keys are read
 * first and the statement is written for them:
 *
 * <ul>
 *   <li>the KEYS probe: the ON column's distinct non-null values over the pivot's source, in their order (the order a
 *       dynamic pivot names its columns in), each as its text (cell 0, which the body binds) and each key's name per
 *       aggregate as the output writes a name (cells 1..: the database writes them, as it wrote them in the
 *       row-as-JSON form this replaces);</li>
 *   <li>the BODY: the pivot as the grouped CASE aggregation every static emulation writes ({@code AGG(CASE WHEN on = k
 *       THEN arg END)}), its per-key aggregates a run of {@link OutputCol.Origin#PER_KEY} projections over a key's
 *       cell ({@link SqlExpr.KeyCell}), and the relation's own clauses over it; for a text output, each output column's
 *       cell in the format's text, the per-key ones a second per-key run. The runner writes each run once per key.</li>
 * </ul>
 *
 * The CASE form is the static emulation's own: exact for every aggregate that skips nulls (a sum, a count of a value,
 * an average, an extreme, a joined text), as DuckDB's FILTER-form PIVOT is; an aggregate that keeps nulls (a list)
 * differs there as in the static emulation (register S103). A null key names no column (the legacy probe's rule).
 */
public final class KeyedPivot {

    private KeyedPivot() {
    }

    /** What the body answers: typed rows, or a text format's cells. */
    public enum Out { ROWS, TDS_TEXT, CSV, JSON }

    /** The probe plan's statements and outputs: {@code keys} (one row per key: its text, then each per-key output's
     *  name), {@code body} (the fixed columns, then the per-key ones), {@code outputs} in the body's order. */
    public record Parts(SqlQuery keys, SqlQuery body, List<Output> outputs) {
    }

    /** One output of the body. */
    public sealed interface Output permits Fixed, PerKey {
    }

    /** A column the body answers once: the typed schema's column, and its name as the output writes one. */
    public record Fixed(Type.Column column, String name) implements Output {
    }

    /** A column the body answers once per key: the pivot's aggregate's template column, its slot, and the keys row's
     *  cell holding each key's name for it. */
    public record PerKey(Type.Column template, SqlType slot, int nameCell) implements Output {
    }

    /** The one pivot of {@code q} that no cast names (no IN, no named values), or null for none; two are refused. */
    public static SqlSource.@com.legend.base.Nullable Pivot dynamic(SqlQuery q) {
        List<SqlSource.Pivot> found = new ArrayList<>();
        collect(q, found);
        List<SqlSource.Pivot> dynamic = found.stream().filter(p -> p.in().isEmpty() && p.named().isEmpty()).toList();
        if (dynamic.size() > 1) {
            throw new com.legend.error.NotImplementedException(dynamic.size() + " dynamic pivots in one query: each"
                    + " names its columns from its own keys, and one probe plan reads one pivot's keys");
        }
        return dynamic.isEmpty() ? null : dynamic.get(0);
    }

    private static void collect(SqlQuery q, List<SqlSource.Pivot> out) {
        switch (q) {
            case SqlSelect s -> collect(s.from(), out);
            case SqlUnion u -> u.branches().forEach(b -> collect(b, out));
            case SqlWith w -> {
                w.ctes().forEach(c -> collect(c.query(), out));
                collect(w.body(), out);
            }
        }
    }

    private static void collect(SqlSource s, List<SqlSource.Pivot> out) {
        switch (s) {
            case SqlSource.Pivot p -> {
                out.add(p);
                collect(p.source(), out);
            }
            case SqlSource.Subselect sub -> collect(sub.inner(), out);
            case SqlSource.Join j -> {
                collect(j.left(), out);
                collect(j.right(), out);
            }
            case SqlSource.Table t -> {
            }
            case SqlSource.Cte c -> {
            }
            case SqlSource.Dual d -> {
            }
            case SqlSource.Values v -> {
            }
            case SqlSource.SourceUrl u -> {
            }
            case SqlSource.VarSetPlaceholder v -> {
            }
            case SqlSource.RawSql r -> {
            }
            case SqlSource.Catalog c -> {
            }
        }
    }

    /**
     * The probe plan of {@code lowered}, the relation whose one dynamic pivot is {@code pivot}, typed by {@code schema}
     * (its static columns, and the pivot's aggregates' templates as its dynamic ones), answered as {@code out}.
     */
    public static Parts of(SqlQuery lowered, SqlSource.Pivot pivot, Type.RelationType schema, Out out,
            boolean quoteStrings, LiteralSpelling.Layouts layouts, WireRender.Spelling spelling) {
        if (pivot.on().size() != 1) {
            throw new IllegalStateException("a dynamic pivot on " + pivot.on().size() + " columns: the lowering keys a"
                    + " pivot on one column (a composite key its columns' text joined)");
        }
        SqlExpr on = pivot.on().get(0);
        if (!(on.type() instanceof com.legend.sql.TypeFact.Typed keyTyped)) {
            throw new IllegalStateException("a dynamic pivot's key has no typed slot: " + on.type());
        }
        SqlType keySlot = keyTyped.type();
        List<Type.Column> templates = TdsTextWrap.templates(pivot, schema);
        List<OutputCol> statics = pivot.outputs().stream()
                .filter(oc -> !oc.name().contains(Type.RelationType.PIVOT_SEPARATOR)).toList();
        // the key as the body binds it: its text, read back by the key's own type (the database's text of its own
        // value is that value)
        SqlExpr keyText = new SqlExpr.KeyCell(0, new com.legend.sql.TypeFact.Typed(SqlType.Scalar.VARCHAR, false));
        SqlExpr keyBound = keySlot == SqlType.Scalar.VARCHAR ? keyText : new SqlExpr.Cast(keyText, keySlot);
        SqlQuery body0 = new SqlRewriter() {
            @Override
            protected SqlSource source(SqlSource s) {
                return s == pivot ? grouped(pivot, statics, on, keyBound) : s;
            }
        }.rewrite(lowered);
        if (body0 == lowered) {
            throw new IllegalStateException("the dynamic pivot is not in the relation its probe plan is made of");
        }
        SqlQuery keys = keys(pivot, on, templates, out);
        // the relation's own order lifted over its cells, as every text wrapper lifts it (TdsTextWrap.liftOrder)
        List<SqlSelect.SortKey> lifted = List.of();
        SqlQuery inner0 = body0;
        if (body0 instanceof SqlSelect ps && !ps.orderBy().isEmpty()) {
            lifted = ps.orderBy();
            inner0 = ps.withOrderBy(List.of());
        }
        TdsTextWrap.Lift lift = TdsTextWrap.liftOrder(lifted, lowered.outputs(), inner0, "_pct0");
        List<OutputCol> fixedOuts = lift.outs().stream().filter(oc -> !oc.name().startsWith("_pct_ord")).toList();
        List<Type.Column> fixedTyped = TdsTextWrap.staticColumns(fixedOuts, schema);
        List<SqlSelect.Projection> projections = new ArrayList<>();
        List<Output> outputs = new ArrayList<>();
        for (int i = 0; i < fixedOuts.size(); i++) {
            OutputCol oc = fixedOuts.get(i);
            Type.Column typed = fixedTyped.get(i);
            SqlExpr ref = SqlExpr.Column.of("_pct0", oc);
            projections.add(out == Out.ROWS ? new SqlSelect.Projection(ref, oc.name(), oc)
                    : new SqlSelect.Projection(cell(out, ref, typed, oc.type(), quoteStrings, layouts, spelling),
                            "_pct_c" + i, new OutputCol("_pct_c" + i, SqlType.Scalar.VARCHAR, false)));
            outputs.add(new Fixed(typed, fixedName(out, typed)));
        }
        for (int i = 0; i < pivot.usings().size(); i++) {
            SqlSource.Pivot.Using u = pivot.usings().get(i);
            SqlType slot = slotOf(u);
            String stem = stem(i);
            SqlExpr ref = SqlExpr.Column.of("_pct0", stem, slot, true, OutputCol.Origin.PER_KEY);
            projections.add(out == Out.ROWS
                    ? new SqlSelect.Projection(ref, stem, new OutputCol(stem, slot, true, OutputCol.Origin.PER_KEY))
                    : new SqlSelect.Projection(cell(out, ref, templates.get(i), slot, quoteStrings, layouts, spelling),
                            stem, new OutputCol(stem, SqlType.Scalar.VARCHAR, false, OutputCol.Origin.PER_KEY)));
            outputs.add(new PerKey(templates.get(i), slot, 1 + i));
        }
        SqlSelect body = SqlSelect.starOf(new SqlSource.Subselect(lift.inner0(), "_pct0", null))
                .withProjections(projections).withOrderBy(lift.remapped());
        return new Parts(keys, body, outputs);
    }

    /** The pivot as the grouped CASE aggregation: its static columns, grouped, then each aggregate over the rows of
     *  each key, a per-key run. */
    private static SqlSource grouped(SqlSource.Pivot pivot, List<OutputCol> statics, SqlExpr on, SqlExpr keyBound) {
        List<SqlSelect.Projection> ps = new ArrayList<>();
        List<SqlExpr> group = new ArrayList<>();
        for (OutputCol oc : statics) {
            SqlExpr g = SqlExpr.Column.of(pivot.source().alias(), oc);
            group.add(g);
            ps.add(new SqlSelect.Projection(g, oc.name(), oc));
        }
        SqlExpr isKey = SqlExpr.Call.of(SqlFn.EQUAL, on, keyBound);
        for (int i = 0; i < pivot.usings().size(); i++) {
            SqlSource.Pivot.Using u = pivot.usings().get(i);
            SqlAgg.Reducer agg = u.agg();
            List<SqlExpr> args = new ArrayList<>(agg.args());
            if (args.isEmpty()) {
                if (agg.fn() != SqlAgg.Fn.COUNT) {
                    throw new com.legend.error.NotImplementedException("a dynamic pivot's aggregate " + agg.fn()
                            + " takes no argument: its rows of a key have no CASE form");
                }
                args.add(new SqlExpr.Case(List.of(new SqlExpr.Case.When(isKey, new SqlExpr.IntLit(1))), null));
            } else {
                args.set(0, new SqlExpr.Case(List.of(new SqlExpr.Case.When(isKey, args.get(0))), null));
            }
            String stem = stem(i);
            ps.add(new SqlSelect.Projection(new SqlAgg.Reducer(agg.fn(), args, agg.distinct(), agg.orderBy()), stem,
                    new OutputCol(stem, slotOf(u), true, OutputCol.Origin.PER_KEY)));
        }
        SqlSelect sel = SqlSelect.starOf(pivot.source()).withProjections(ps);
        return new SqlSource.Subselect(group.isEmpty() ? sel : sel.withGroupBy(group), pivot.alias(), null);
    }

    /** The keys probe: each distinct non-null key in its order, its text, and each aggregate's name for it. */
    private static SqlQuery keys(SqlSource.Pivot pivot, SqlExpr on, List<Type.Column> templates, Out out) {
        if (!(on.type() instanceof com.legend.sql.TypeFact.Typed keyTyped)) {
            throw new IllegalStateException("a dynamic pivot's key has no typed slot: " + on.type());
        }
        SqlSelect distinct = SqlSelect.starOf(pivot.source())
                .withProjections(List.of(
                        new SqlSelect.Projection(on, "_pct_k", new OutputCol("_pct_k", keyTyped.type(), true)),
                        new SqlSelect.Projection(new SqlExpr.Cast(on, SqlType.Scalar.VARCHAR), "_pct_t",
                                new OutputCol("_pct_t", SqlType.Scalar.VARCHAR, true))))
                .withWhere(SqlExpr.Call.of(SqlFn.IS_NOT_NULL, on))
                .withDistinct();
        SqlExpr text = SqlExpr.Column.of("_pct_keys", "_pct_t", SqlType.Scalar.VARCHAR, true,
                OutputCol.Origin.DERIVED);
        List<SqlSelect.Projection> ps = new ArrayList<>();
        ps.add(new SqlSelect.Projection(text, "_pct_t", new OutputCol("_pct_t", SqlType.Scalar.VARCHAR, true)));
        for (int i = 0; i < pivot.usings().size(); i++) {
            String tail = Type.RelationType.PIVOT_SEPARATOR + pivot.usings().get(i).alias();
            Type.Column template = templates.get(i);
            SqlExpr name = switch (out) {
                case ROWS -> Render.generatedName(text, tail);
                // the TDS header's entry for the key's column: the name in literal quotes (tdsHeader's rule for a
                // pivot's name), its type and multiplicity the template's
                case TDS_TEXT -> Render.cat(new SqlExpr.StringLit("'\\'"), text,
                        new SqlExpr.StringLit(tail + "\\'':" + Render.tdsTypeName(template.type())
                                + (template.multiplicity() instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                                        && b.lower() == 0 ? "[0..1]" : "[1]")));
                case CSV -> Render.escapeCsv(Render.generatedName(text, tail));
                case JSON -> Render.jsonToken(Render.generatedName(text, tail));
            };
            String alias = "_pct_n" + i;
            ps.add(new SqlSelect.Projection(name, alias, new OutputCol(alias, SqlType.Scalar.VARCHAR, false)));
        }
        SqlExpr key = SqlExpr.Column.of("_pct_keys", "_pct_k", keyTyped.type(), true, OutputCol.Origin.DERIVED);
        return SqlSelect.starOf(new SqlSource.Subselect(distinct, "_pct_keys", null))
                .withProjections(ps)
                .withOrderBy(List.of(new SqlSelect.SortKey(key, true, null, null)));
    }

    /** Aggregate {@code i}'s per-key name in the SQL ({@code _u0}; written {@code "_pk3__u0"} for the fourth key): the
     *  plan reads a per-key column by its place, and a name of the aggregate's own could outrun a database's
     *  identifier length (Postgres keeps 63 bytes) and two of them meet. */
    private static String stem(int i) {
        return "_u" + i;
    }

    private static SqlType slotOf(SqlSource.Pivot.Using u) {
        SqlType t = u.type();
        if (t == null) {
            throw new IllegalStateException("a dynamic pivot's aggregate '" + u.alias() + "' carries no typed slot");
        }
        return t;
    }

    /** A column's cell in the output's text, as that output's wire writes one. */
    private static SqlExpr cell(Out out, SqlExpr ref, Type.Column typed, SqlType slot, boolean quoteStrings,
            LiteralSpelling.Layouts layouts, WireRender.Spelling spelling) {
        return switch (out) {
            case ROWS -> ref;
            case TDS_TEXT -> Render.tdsTextCell(ref, typed, slot, quoteStrings, layouts);
            case CSV -> Render.cell(ref, typed, slot, false);
            case JSON -> Render.jsonCell(ref, typed.type(), slot, spelling);
        };
    }

    /** A fixed column's name as the output writes one, a plan constant (the column's name is a plan fact). */
    private static String fixedName(Out out, Type.Column typed) {
        return switch (out) {
            case ROWS -> typed.name();
            case TDS_TEXT -> Render.tdsHeader(List.of(typed));
            case CSV -> Render.csvName(Type.RelationType.presentPivotName(typed.name()));
            case JSON -> Render.jsonName(Type.RelationType.presentPivotName(typed.name()));
        };
    }
}
