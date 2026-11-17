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
 * THE HOST JUDGE'S SIDES, BATCHED (the orchestrator's (1) of 2026-11-17: isolated BOTH's cost was the host's own side
 * reads, one statement each): consecutive asserts' sides read by ONE statement of the host plan alone -- each side
 * its own derived table, LEFT JOINed to the sides' indexes ({@link #batch}), so no column's type is unified with
 * another's. No verdict in it: the host judges the cells as it judges a side's own rows. A side that can be neither
 * written inline nor read as a derived table keeps its own statement.
 */
public final class HostSideBatch {

    /** The batch statement's index column: the row's assert, by its place in the batch. */
    public static final String INDEX = "__ix";

    private HostSideBatch() {
    }

    /** One column a batched side's row carries: {@code value}, the side's own expression (its select of no FROM),
     *  under {@code out}, named as the batch statement's column the host reads it from; {@code side} and
     *  {@code column} the side's own statement and the projection {@code value} is. */
    public record HostColumn(SqlExpr value, OutputCol out, @com.legend.base.Nullable SqlQuery side, int column) {
        public HostColumn {
            java.util.Objects.requireNonNull(value, "value");
            java.util.Objects.requireNonNull(out, "out");
        }

    }

    /**
     * A host side's planned statement {@code side} as the columns of a batch branch with no FROM (E-5c step 6; re-homed
     * in the host plan, 2026-11-17), each projection's expression under its name in {@code names} -- where, and only
     * where, the side is a SINGLE VALUE OF NO FROM, decided from the tree: a select of {@link SqlSource.Dual} with no
     * filter, grouping, window filter, distinct, order, limit or offset, one projection per name, each a value that is
     * its operands' alone ({@link SqlExpr#raisesNothing}: no UNNEST, no subquery, no raise) and reads no row, aggregates
     * or windows. Every alias inside it is prefixed by {@code aliasPrefix} ({@link com.legend.sql.AliasPrefix}). Null
     * otherwise: the side is read as a derived table, or keeps its own statement.
     */
    public static @com.legend.base.Nullable List<HostColumn> singleValueColumns(SqlQuery side, String aliasPrefix,
            List<String> names) {
        if (!(side instanceof SqlSelect s) || !(s.from() instanceof SqlSource.Dual) || s.distinct()
                || s.where() != null || !s.groupBy().isEmpty() || s.having() != null || s.qualify() != null
                || !s.orderBy().isEmpty() || s.limit() != null || s.offset() != null
                || s.projections().size() != names.size() || s.outputs().size() != names.size()) {
            return null;
        }
        if (!(com.legend.sql.AliasPrefix.apply(aliasPrefix, s) instanceof SqlSelect prefixed)) {
            throw new IllegalStateException("an alias prefix made a select no select");
        }
        List<HostColumn> out = new ArrayList<>(names.size());
        for (int p = 0; p < names.size(); p++) {
            SqlExpr value = prefixed.projections().get(p).expr();
            // inline only a value that is its operands' alone and keeps its select's one row: no UNNEST (a row per
            // element: removeDuplicates' list read as rows all numbered 1, 2026-11-17), no subquery, no raise -- the
            // rest are read as derived tables
            if (!SqlExpr.raisesNothing(value) || RowBound.of(value)) {
                return null;
            }
            OutputCol o = s.outputs().get(p);
            out.add(new HostColumn(value, new OutputCol(names.get(p), o.type(), o.nullable(), OutputCol.Origin.DERIVED),
                    side, p));
        }
        return out;
    }

    /** Whether an expression aggregates, windows, or reads a row of a source (a qualified column, a star, a row's
     *  order, a keyed region's cell, a spliced temp table) -- what would change meaning in another select (E-5c step
     *  6). Anything that still holds a query is walked whole, so a nested reader counts too: the answer errs toward
     *  the side's own statement. */
    private static final class RowBound extends com.legend.sql.SqlRewriter {
        /** The row-bound nodes the walk met. */
        private final List<SqlExpr> met = new ArrayList<>();

        static boolean of(SqlExpr e) {
            RowBound r = new RowBound();
            r.rewriteExpr(e);
            return !r.met.isEmpty();
        }

        @Override
        protected SqlExpr expr(SqlExpr e) {
            if (e instanceof SqlAgg.Reducer || e instanceof SqlExpr.OrderedListAgg || e instanceof SqlExpr.JsonArrayAgg
                    || e instanceof SqlExpr.WindowCall || e instanceof SqlExpr.RowOrder || e instanceof SqlExpr.KeyCell
                    || e instanceof SqlExpr.TempTableInSplice || e instanceof SqlExpr.Star
                    || e instanceof SqlExpr.StarExcept || e instanceof SqlExpr.Column c && c.table() != null) {
                met.add(e);
            }
            return e;
        }
    }


    /** The batch statement's row-order column: a side's row's place among its side's rows. */
    public static final String ORDER = "__rn";

    /** The batch's aliases: its indexes' literal table, each side's derived table (by its index), and a numbered
     *  side's own statement inside it. */
    private static final String INDEXES = "__i";
    private static final String SIDE = "__b";
    private static final String ROWS = "__r";

    /**
     * One side of a batch: {@code inline}, its single value's columns, written in its branch with no FROM; or
     * {@code numbered}, its statement read as a derived table -- with the ordinal its plan defines as its last output
     * ({@link SideOrdinal#defined}) where {@code ordinal}, else numbered by its rows' arrival from the derived table, as
     * the database judge numbers such a side; {@code outs}, the batch columns it fills, in its statement's column order.
     */
    public record BatchSide(@com.legend.base.Nullable List<HostColumn> inline, @com.legend.base.Nullable SqlQuery numbered,
            boolean ordinal, List<OutputCol> outs) {
        public BatchSide {
            outs = List.copyOf(outs);
            if ((inline == null) == (numbered == null)) {
                throw new IllegalArgumentException("a batched side is inline or numbered, one of them");
            }
        }
    }

    /**
     * {@code side}, a host side's planned statement, as a side of a batch whose columns are {@code names} (one per
     * output): inline where it is a single value of no FROM ({@link #singleValueColumns}); else numbered, where its
     * statement may be a derived table (no WITH: a CTE inside a derived table is not every database's); null
     * otherwise.
     */
    public static @com.legend.base.Nullable BatchSide side(SqlQuery side, String aliasPrefix, List<String> names) {
        if (side.outputs().size() != names.size()) {
            return null;
        }
        List<HostColumn> inline = singleValueColumns(side, aliasPrefix, names);
        if (inline != null) {
            List<OutputCol> outs = new ArrayList<>(names.size());
            for (int c = 0; c < names.size(); c++) {
                outs.add(new OutputCol(names.get(c), side.outputs().get(c).type(), true, OutputCol.Origin.DERIVED));
            }
            return new BatchSide(inline, null, false, outs);
        }
        if (side instanceof com.legend.sql.SqlWith || sorted(side) && SideOrdinal.defined(side) == null) {
            // a WITH is no derived table every database reads; a sort whose ordinal the plan cannot define would be
            // read by its arrival through the batch's selects, which a database need not keep (SideOrdinal)
            return null;
        }
        SqlQuery defined = SideOrdinal.defined(side);
        // every alias inside it prefixed, unique in the batch (a renderer's pass may lift a derived table's sources
        // into the select that reads it: H2's self-join sides each read "t3", 2026-11-17)
        SqlQuery numbered = com.legend.sql.AliasPrefix.apply(aliasPrefix, defined != null ? defined : side);
        List<OutputCol> sideOuts = numbered.outputs();
        if (defined != null ? sideOuts.size() != names.size() + 1
                || !SideOrdinal.ORD.equals(sideOuts.get(names.size()).name()) : sideOuts.size() != names.size()) {
            return null;
        }
        List<OutputCol> outs = new ArrayList<>(names.size());
        for (int c = 0; c < names.size(); c++) {
            outs.add(new OutputCol(names.get(c), sideOuts.get(c).type(), true, OutputCol.Origin.DERIVED));
        }
        return new BatchSide(null, numbered, defined != null, outs);
    }

    /** Whether {@code q} sorts its rows: its own ORDER BY, or one under the derived tables it reads through. */
    private static boolean sorted(SqlQuery q) {
        if (q instanceof com.legend.sql.SqlUnion u) {
            return u.branches().stream().anyMatch(HostSideBatch::sorted);
        }
        if (!(q instanceof SqlSelect s)) {
            return true;
        }
        return !s.orderBy().isEmpty() || s.from() instanceof SqlSource.Subselect sub && sorted(sub.inner());
    }

    /**
     * The batch statement over {@code sides} (at least two), with NO type its database must unify (H2 refused a UNION
     * ALL's typed slots where the model's type was not the column's: VARCHAR under DECFLOAT, an ID's text read as a
     * number, 2026-11-17): the sides' indexes as a literal table, each side its own derived table LEFT JOINed on its
     * index ({@code __i.__ix = k}), so every column is its own side's, typed by its own statement, and NULL on the
     * other indexes' rows. Its columns: the index ({@link #INDEX}), the row's order among its side's rows
     * ({@link #ORDER}: 1 for an inline side, its plan's ordinal, else its arrival from the derived table, as the
     * database judge numbers a side; NULL where the side has no row), then each side's columns; read in order of index
     * and row order.
     */
    public static SqlQuery batch(List<BatchSide> sides) {
        OutputCol ixOut = new OutputCol(INDEX, SqlType.Scalar.BIGINT, false);
        OutputCol rnOut = new OutputCol(ORDER, SqlType.Scalar.BIGINT, true);
        List<List<SqlExpr>> indexes = new ArrayList<>(sides.size());
        for (int i = 0; i < sides.size(); i++) {
            indexes.add(List.of(new SqlExpr.IntLit(i)));
        }
        SqlSource from = new SqlSource.Values(indexes, List.of(INDEX), INDEXES, List.of(ixOut));
        SqlExpr ix = SqlExpr.Column.of(INDEXES, ixOut);
        List<SqlExpr> orders = new ArrayList<>(sides.size());
        List<SqlSelect.Projection> columns = new ArrayList<>();
        List<OutputCol> outs = new ArrayList<>(List.of(ixOut, rnOut));
        for (int i = 0; i < sides.size(); i++) {
            BatchSide side = sides.get(i);
            String alias = SIDE + i;
            SqlQuery numbered = side.numbered();
            List<SqlSelect.Projection> ps = new ArrayList<>();
            SqlSource own;
            if (numbered == null) {
                ps.add(new SqlSelect.Projection(new SqlExpr.IntLit(1), ORDER, rnOut));
                List<HostColumn> inline = java.util.Objects.requireNonNull(side.inline());
                for (int c = 0; c < inline.size(); c++) {
                    ps.add(new SqlSelect.Projection(inline.get(c).value(), side.outs().get(c).name(), side.outs().get(c)));
                }
                own = new SqlSource.Dual();
            } else {
                List<OutputCol> sideOuts = numbered.outputs();
                SqlExpr order = side.ordinal() ? SqlExpr.Column.of(ROWS, sideOuts.get(sideOuts.size() - 1))
                        : new SqlExpr.WindowCall(new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER, List.of()), List.of(),
                                List.of(), null);
                ps.add(new SqlSelect.Projection(order, ORDER, rnOut));
                for (int c = 0; c < side.outs().size(); c++) {
                    ps.add(new SqlSelect.Projection(SqlExpr.Column.of(ROWS, sideOuts.get(c)), side.outs().get(c).name(),
                            side.outs().get(c)));
                }
                own = new SqlSource.Subselect(numbered, ROWS, null);
            }
            List<OutputCol> ownOuts = ps.stream().map(SqlSelect.Projection::out).toList();
            SqlSelect read = new SqlSelect(ps, false, own, null, List.of(), null, null, List.of(), null, null,
                    ownOuts);
            from = new SqlSource.Join(from, new SqlSource.Subselect(read, alias, null), SqlSource.Join.Kind.LEFT,
                    SqlExpr.Call.of(com.legend.sql.SqlFn.EQUAL, ix, new SqlExpr.IntLit(i)));
            orders.add(SqlExpr.Column.of(alias, rnOut));
            for (OutputCol o : side.outs()) {
                columns.add(new SqlSelect.Projection(SqlExpr.Column.of(alias, o), o.name(), o));
                outs.add(o);
            }
        }
        SqlExpr order = new SqlExpr.Call(com.legend.sql.SqlFn.COALESCE, orders);
        List<SqlSelect.Projection> read = new ArrayList<>(outs.size());
        read.add(new SqlSelect.Projection(ix, INDEX, ixOut));
        read.add(new SqlSelect.Projection(order, ORDER, rnOut));
        read.addAll(columns);
        List<SqlSelect.SortKey> by = List.of(new SqlSelect.SortKey(ix, true, null, null),
                new SqlSelect.SortKey(order, true, null, null));
        return new SqlSelect(read, false, from, null, List.of(), null, null, by, null, null, outs);
    }
}
