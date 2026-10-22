// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.List;

/**
 * A grid side's row ordinal: its rows' order as one more output column, {@link #ORD}, computed where the plan defines
 * that order, so a judge that reads the side through other selects (the database judge's verdict statement, its sides
 * as CTEs) numbers the rows by it, never by their arrival there. A database need not keep a derived table's or a CTE's
 * order through the selects that read it (measured 2026-10-20: MariaDB and Trino return a sorted CTE, and a literal's
 * UNION ALL, in another order, so the verdict's positional zip of assertTdsEquivalent paired the wrong rows: channel
 * B's reduce and pivot tests).
 *
 * <ul>
 *   <li>A literal's rows (a VALUES source): its row index, a literal column of each row.</li>
 *   <li>A sorted select: {@code ROW_NUMBER() OVER (ORDER BY <its own sort expressions>)} among its own projections;
 *       a sorted DISTINCT select (the ordinal would count among its distinct columns) over its outputs, outside it,
 *       where its keys are outputs.</li>
 *   <li>A select that keeps its source's rows in order (no sort, no grouping, no DISTINCT, one source) over a source
 *       whose order is defined: the source's ordinal, passed through.</li>
 *   <li>Any other: its rows as the database returns them ({@code ROW_NUMBER() OVER ()}), the order the host judge
 *       reads it in: among a select's own projections, outside only a DISTINCT select or a union.</li>
 * </ul>
 *
 * <p>Numbered inside the side, never by a select wrapped around it where its own select can carry the ordinal: H2
 * re-parses a derived table's text for each plan it weighs, and a wrapper per grid side took the corpus's
 * columnValueDifferenceTest from 2,164 MB to 3,593 MB of heap in H2's database pass, the pass from under its 4 GB to
 * out of it (an OutOfMemoryError in H2's tokenizer, measured 2026-10-21).
 */
final class SideOrdinal {

    /** The ordinal's column. */
    static final String ORD = "__ord";

    private static final OutputCol ORD_OUT = new OutputCol(ORD, SqlType.Scalar.BIGINT, false);

    private SideOrdinal() {
    }

    /** {@code q} with its rows' ordinal as its last output. */
    static SqlQuery withOrdinal(SqlQuery q) {
        SqlQuery defined = defined(q);
        if (defined != null) {
            return defined;
        }
        // an unsorted select numbers its rows as it returns them in itself (a sorted one's window would number them
        // before its sort)
        SqlQuery inside = q instanceof SqlSelect s && !s.distinct() && s.orderBy().isEmpty() ? numberedInside(s) : null;
        return inside != null ? inside : numbered(q, List.of());
    }

    /** {@code q} with the ordinal its plan defines, or null where it defines none. */
    static @com.legend.base.Nullable SqlQuery defined(SqlQuery q) {
        if (!(q instanceof SqlSelect s)) {
            return null;
        }
        if (!s.orderBy().isEmpty()) {
            // a key that is a window's answer takes no place in another window's order: numbered outside, as a DISTINCT
            // select is (it would count the ordinal among its distinct columns), by its keys where they are outputs
            if (!s.distinct() && s.orderBy().stream().noneMatch(k -> windowed(k.expr()))) {
                return numberedInside(s);
            }
            List<SqlSelect.SortKey> keys = outputKeys(s);
            return keys == null ? null : numbered(s, keys);
        }
        if (s.distinct() || !s.groupBy().isEmpty() || s.having() != null || s.limit() != null
                || s.offset() != null) {
            return null;
        }
        String alias;
        SqlSource from;
        if (s.from() instanceof SqlSource.Values v) {
            alias = v.alias();
            from = indexed(v);
        } else if (s.from() instanceof SqlSource.Subselect sub && defined(sub.inner()) instanceof SqlQuery inner) {
            alias = sub.alias();
            from = new SqlSource.Subselect(inner, sub.alias(), sub.frameName());
        } else {
            return null;
        }
        SqlExpr ord = SqlExpr.Column.of(alias, ORD_OUT);
        if (s.projections().isEmpty()) {
            // a star frame reads its source's outputs, the ordinal among them (no clause here orders, groups or limits)
            return new SqlSelect(List.of(), false, from, s.where(), List.of(), null, s.qualify(), List.of(), null, null,
                    from.outputs());
        }
        SqlSelect over = s.withFrom(from);
        List<SqlSelect.Projection> ps = new ArrayList<>(s.projections());
        ps.add(new SqlSelect.Projection(ord, ORD, ORD_OUT));
        return over.withProjections(ps);
    }

    /** A literal's rows, each with its index. */
    private static SqlSource.Values indexed(SqlSource.Values v) {
        List<List<SqlExpr>> rows = new ArrayList<>(v.rows().size());
        for (int r = 0; r < v.rows().size(); r++) {
            List<SqlExpr> row = new ArrayList<>(v.rows().get(r));
            row.add(new SqlExpr.IntLit(r + 1));
            rows.add(row);
        }
        List<String> columns = new ArrayList<>(v.columns());
        columns.add(ORD);
        List<OutputCol> outs = new ArrayList<>(v.outputs());
        outs.add(ORD_OUT);
        return new SqlSource.Values(rows, columns, v.alias(), outs);
    }

    /** The sort keys of {@code s} as its output columns (read from outside it), or null where one is not an output. */
    private static @com.legend.base.Nullable List<SqlSelect.SortKey> outputKeys(SqlSelect s) {
        List<SqlSelect.SortKey> keys = new ArrayList<>(s.orderBy().size());
        for (SqlSelect.SortKey k : s.orderBy()) {
            String name = outputOf(s, k);
            if (name == null) {
                return null;
            }
            OutputCol out = s.outputs().stream().filter(o -> o.name().equals(name)).findFirst().orElseThrow();
            keys.add(new SqlSelect.SortKey(SqlExpr.Column.of("__o", out), k.ascending(), k.nullOrder(), null));
        }
        return keys;
    }

    /** The output of {@code s} a sort key orders by: the output it names, or the output whose expression it is (a
     *  star frame's outputs are its source's columns, by name). */
    private static @com.legend.base.Nullable String outputOf(SqlSelect s, SqlSelect.SortKey k) {
        if (k.outputName() != null && s.outputs().stream().anyMatch(o -> o.name().equals(k.outputName()))) {
            return k.outputName();
        }
        if (s.projections().isEmpty()) {
            return k.expr() instanceof SqlExpr.Column c
                    && s.outputs().stream().anyMatch(o -> o.name().equals(c.name())) ? c.name() : null;
        }
        for (SqlSelect.Projection p : s.projections()) {
            if (p.alias() != null && p.expr().equals(k.expr())) {
                return p.alias();
            }
        }
        return null;
    }

    /** {@code s} with {@code ROW_NUMBER() OVER (ORDER BY <its own sort keys>) AS __ord} among its projections (an
     *  unsorted select's {@code OVER ()}): a star frame's star qualified by each source it reads (MySQL and MariaDB take
     *  no bare star beside a column); null where a source has no alias to qualify it by. */
    private static @com.legend.base.Nullable SqlQuery numberedInside(SqlSelect s) {
        List<SqlSelect.SortKey> keys = new ArrayList<>(s.orderBy().size());
        for (SqlSelect.SortKey k : s.orderBy()) {
            keys.add(new SqlSelect.SortKey(k.expr(), k.ascending(), k.nullOrder(), null));
        }
        List<SqlSelect.Projection> ps = new ArrayList<>();
        if (s.projections().isEmpty()) {
            List<String> aliases = new ArrayList<>();
            if (!sourceAliases(s.from(), aliases)) {
                return null;
            }
            for (String a : aliases) {
                ps.add(new SqlSelect.Projection(new SqlExpr.Star(a), null, null));
            }
        } else {
            ps.addAll(s.projections());
        }
        ps.add(new SqlSelect.Projection(new SqlExpr.WindowCall(
                new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER, List.of()), List.of(), keys, null), ORD, ORD_OUT));
        return s.withProjections(ps);
    }

    /** Whether {@code e} holds a window function. */
    private static boolean windowed(SqlExpr e) {
        return e instanceof SqlExpr.WindowCall || e.children().stream().anyMatch(SideOrdinal::windowed);
    }

    /** The aliases a star over {@code from} reads, left to right; false where one has none. */
    private static boolean sourceAliases(SqlSource from, List<String> out) {
        return switch (from) {
            case SqlSource.Join j -> sourceAliases(j.left(), out) && sourceAliases(j.right(), out);
            case SqlSource.Table t -> t.alias() != null && out.add(t.alias());
            case SqlSource.Subselect sub -> sub.alias() != null && out.add(sub.alias());
            case SqlSource.Values v -> v.alias() != null && out.add(v.alias());
            default -> false;
        };
    }

    /** {@code SELECT __o.*, ROW_NUMBER() OVER (ORDER BY keys) AS __ord FROM (q) __o}. */
    private static SqlQuery numbered(SqlQuery q, List<SqlSelect.SortKey> keys) {
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (OutputCol o : q.outputs()) {
            ps.add(new SqlSelect.Projection(SqlExpr.Column.of("__o", o), o.name(), o));
        }
        ps.add(new SqlSelect.Projection(new SqlExpr.WindowCall(
                new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER, List.of()), List.of(), keys, null), ORD, ORD_OUT));
        List<OutputCol> outs = new ArrayList<>(q.outputs());
        outs.add(ORD_OUT);
        return new SqlSelect(ps, false, new SqlSource.Subselect(q, "__o", null), null, List.of(), null, null,
                List.of(), null, null, outs);
    }
}
