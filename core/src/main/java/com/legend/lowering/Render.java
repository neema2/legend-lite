// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.sql.DateFmt;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.List;

/**
 * A RELATION'S ROWS, THE PRODUCT'S HELPERS: the typed rows' shape ({@link #typedRows}), a select's order carried outside it
 * ({@link #liftOrder}), a write's count, a pivot's generated column name and the presented outputs. A relation's
 * {@code toCSV} and a TDS's {@code csv} are legend-pure's texts, the compatibility edge's ({@code edge.RelationTextLowering},
 * through the plan style's {@link RelationTexts} facet; the separation census's B17, 2026-11-17: they were composed here
 * until then); a relation's {@code toString} is lite's own text ({@code ProductRelationText}, B2b S5a, 2026-11-24).
 */
public final class Render {

    private Render() {
    }

    /** The csv property's receiver is a relation with a schema (the csv
     * text arm's guard). */
    static boolean csvOverRelation(com.legend.compiler.spec.typed.TypedPropertyAccess read) {
        return Type.relationSchema(read.source().info().type()) != null;
    }

    /** A select's order carried outside it: {@code source} the select with its hidden order columns, {@code keys}
     *  the sort keys over {@code rowAlias}, each addressing an output of the source. */
    record Lifted(SqlSelect source, List<SqlSelect.SortKey> keys) {
    }

    /** {@code inner}'s order lifted over the alias {@code rowAlias}: a sort key that is no output (a sort by an
     *  expression, or by a column the select drops; the plan path program's step 6, 2026-10-18) is carried as a
     *  hidden column of the inner select and ordered by outside, never written into the row. Null where a hidden
     *  column would change the rows (a star or a DISTINCT select) -- the caller says what it does then. */
    static @com.legend.base.Nullable Lifted liftOrder(SqlSelect inner, String rowAlias, String what) {
        List<OutputCol> cols = inner.outputs();
        List<SqlSelect.SortKey> lifted = new ArrayList<>();
        List<SqlSelect.Projection> hidden = new ArrayList<>();
        List<OutputCol> hiddenOut = new ArrayList<>();
        for (SqlSelect.SortKey k : inner.orderBy()) {
            String out = k.outputName() != null ? k.outputName()
                    : k.expr() instanceof SqlExpr.Column c ? c.name() : null;
            // the inner's own outputs, as the hoist's: a hidden sort column (withSortOutputs) is one, not one of cols
            OutputCol outCol = out == null ? null : inner.outputs().stream()
                    .filter(oc -> oc.name().equals(out)).findFirst()
                    .orElse(null);
            if (outCol == null) {
                if (!(k.expr().type() instanceof com.legend.sql.TypeFact.Typed typed)) {
                    throw new com.legend.error.NotImplementedException(what + " over an ordered relation whose sort"
                            + " key is not an output column and carries no typed slot");
                }
                if (inner.projections().isEmpty() || inner.distinct()) {
                    return null;
                }
                String oname = "_ord" + hidden.size();
                outCol = new OutputCol(oname, typed.type(), true);
                hidden.add(new SqlSelect.Projection(k.expr(), oname, outCol));
                hiddenOut.add(outCol);
            }
            lifted.add(new SqlSelect.SortKey(
                    SqlExpr.Column.of(rowAlias, outCol), k.ascending(),
                    k.nullOrder(), null));
        }
        SqlSelect source = inner;
        if (!hidden.isEmpty()) {
            List<SqlSelect.Projection> ps = new ArrayList<>(inner.projections());
            ps.addAll(hidden);
            List<OutputCol> outs = new ArrayList<>(inner.outputs());
            outs.addAll(hiddenOut);
            source = inner.withProjections(ps, outs);
        }
        return new Lifted(source, lifted);
    }

    /** {@code inner} as a typed-rows statement (the plan path program's step 7, 2026-10-18): itself, unless a sort
     *  key is no output -- then the select with that key hidden, wrapped once over {@code rowAlias} projecting the
     *  select's own outputs and ordered by the hidden key, as the JSON rows wire carried it (a dialect that emulates
     *  a join by a union, H2's FULL OUTER JOIN, addresses a sort key among the outputs only). Where a hidden column
     *  would change the rows the statement stays as it is. */
    static SqlSelect typedRows(SqlSelect inner, String rowAlias) {
        Lifted l = liftOrder(inner, rowAlias, "typed rows");
        if (l == null || l.source() == inner) {
            return inner;
        }
        List<OutputCol> cols = inner.outputs();
        List<SqlSelect.Projection> projs = new ArrayList<>(cols.size());
        for (OutputCol oc : cols) {
            projs.add(new SqlSelect.Projection(SqlExpr.Column.of(rowAlias, oc), oc.name(), oc));
        }
        return SqlSelect.starOf(new SqlSource.Subselect(l.source(), rowAlias, null))
                .withProjections(projs, cols)
                .withOrderBy(l.keys());
    }

    /** {@code cols} less the slot columns of its tagged columns: the columns a text wire presents (a tagged column's
     *  slots are read through it, {@link #perSlot}). */
    static List<OutputCol> presented(List<OutputCol> cols) {
        java.util.Set<String> slots = new java.util.HashSet<>();
        for (OutputCol c : cols) {
            if (c.type() instanceof SqlType.Tagged tg) {
                for (int i = 0; i < tg.slots().size(); i++) {
                    slots.add(SqlType.Tagged.slotColumn(c.name(), i));
                }
            }
        }
        return slots.isEmpty() ? cols : cols.stream().filter(c -> !slots.contains(c.name())).toList();
    }

    /** write(rel, accessor)'s observable: the COUNT of rows written
     *  (a TDS-relation accessor destination has no physical table — the
     *  write is vacuous, only the count is observable). */
    static SqlExpr writeCount(SqlSelect src, String alias) {
        SqlSelect count = SqlSelect.starOf(
                        new SqlSource.Subselect(src, alias, null))
                .withProjections(List.of(new SqlSelect.Projection(
                        new SqlAgg.Reducer(SqlAgg.Fn.COUNT,
                                List.of(), false, List.of()), null,
                        new OutputCol("count",
                                SqlType.Scalar.BIGINT, false))));
        return new SqlExpr.ScalarSubquery(count);
    }

    /** A generated column's name: the key's text, the separator and the USING's alias ({@code tail}), bare -- the
     *  physical column's, as a native PIVOT names it (the user's ruling of 2026-11-10, start-here §5.4: "Pivot names
     *  are bare"; phase D's D1.4, 2026-11-15). legend-engine labels it between literal single quotes (the pivot
     *  homework, docs/PIVOT_UPSTREAM_HOMEWORK_2026_10_18.md): its writers' quoting, the edge's. */
    static SqlExpr generatedName(SqlExpr keyText, String tail) {
        return cat(keyText, new SqlExpr.StringLit(tail));
    }

    static SqlExpr cat(SqlExpr... parts) {
        SqlExpr out = parts[0];
        for (int i = 1; i < parts.length; i++) {
            out = SqlExpr.Call.of(SqlFn.CONCAT, out, parts[i]);
        }
        return out;
    }
}
