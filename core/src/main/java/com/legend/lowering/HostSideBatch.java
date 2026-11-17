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
import com.legend.sql.SqlUnion;

import java.util.ArrayList;
import java.util.List;

/**
 * THE HOST JUDGE'S SIDES, BATCHED (the orchestrator's (1) of 2026-11-17: isolated BOTH's cost was the host's own side
 * reads, one statement each): consecutive asserts' sides read by ONE statement of the host plan alone -- a
 * {@code UNION ALL} of one branch per side, its index and its rows' order first, then every batched side's columns,
 * each branch projecting its own and a typed slot ({@link SqlExpr.Reason#SLOT}) for the others'. No verdict in it: the
 * host judges the cells as it judges a side's own rows. A side that can be neither written inline nor read as a derived
 * table keeps its own statement.
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
     * A host side's planned statement {@code side} as the HOST columns of a fused verdict row (E-5c step 6; re-homed in the host plan, 2026-11-17), each projection's expression under its name in {@code names} -- where, and only where,
     * the side is a SINGLE VALUE OF NO FROM, decided from the tree: a select of {@link SqlSource.Dual} with no filter,
     * grouping, window filter, distinct, order, limit or offset (so it answers exactly one row, as the fused row does),
     * one projection per name, and no expression that aggregates, windows or reads a row of an enclosing source outside
     * a subquery (the expression moves into the verdict row's own select, which may read and group a source). Every
     * alias inside it is prefixed by {@code aliasPrefix} ({@link com.legend.sql.AliasPrefix}: a subquery's aliases unique
     * in the fused statement). Null otherwise: the side keeps its own statement.
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
            if (RowBound.of(SubqueriesOut.of(value))) {
                return null;
            }
            OutputCol o = s.outputs().get(p);
            out.add(new HostColumn(value, new OutputCol(names.get(p), o.type(), o.nullable(), OutputCol.Origin.DERIVED),
                    side, p));
        }
        return out;
    }

    /** An expression with each subquery taken out (E-5c step 6): a scalar subquery or an EXISTS a NULL, an IN or a
     *  quantified comparison its value -- what is left is what the expression computes in its own select. */
    private static final class SubqueriesOut extends com.legend.sql.SqlRewriter {
        static SqlExpr of(SqlExpr e) {
            return new SubqueriesOut().rewriteExpr(e);
        }

        @Override
        protected SqlExpr expr(SqlExpr e) {
            if (e instanceof SqlExpr.ScalarSubquery || e instanceof SqlExpr.Exists) {
                return new SqlExpr.NullLit();
            }
            if (e instanceof SqlExpr.InSubquery in) {
                return in.value();
            }
            if (e instanceof SqlExpr.Quantified q) {
                return q.value();
            }
            return e;
        }
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

    /** The batch's alias for a numbered side's derived table, and for the union under its ordering select. */
    private static final String SIDE = "__b";
    private static final String UNION = "__u";

    /**
     * One side of a batch: {@code inline}, its single value's columns, written in its branch with no FROM; or
     * {@code numbered}, its statement with its rows' ordinal ({@link SideOrdinal}) as its last output, read as a
     * derived table; {@code outs}, the batch columns it fills, in its statement's column order.
     */
    public record BatchSide(@com.legend.base.Nullable List<HostColumn> inline, @com.legend.base.Nullable SqlQuery numbered,
            List<OutputCol> outs) {
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
     * statement may be a derived table (no WITH: a CTE inside a derived table is not every database's); null otherwise.
     */
    public static @com.legend.base.Nullable BatchSide side(SqlQuery side, String aliasPrefix, List<String> names) {
        if (side.outputs().size() != names.size()) {
            return null;
        }
        List<HostColumn> inline = singleValueColumns(side, aliasPrefix, names);
        if (inline != null) {
            return new BatchSide(inline, null, inline.stream().map(h -> nullable(h.out())).toList());
        }
        if (side instanceof com.legend.sql.SqlWith) {
            return null;
        }
        SqlQuery numbered = SideOrdinal.withOrdinal(side);
        List<OutputCol> sideOuts = numbered.outputs();
        if (sideOuts.size() != names.size() + 1 || !SideOrdinal.ORD.equals(sideOuts.get(names.size()).name())) {
            return null;
        }
        List<OutputCol> outs = new ArrayList<>(names.size());
        for (int c = 0; c < names.size(); c++) {
            OutputCol o = sideOuts.get(c);
            outs.add(new OutputCol(names.get(c), o.type(), true, OutputCol.Origin.DERIVED));
        }
        return new BatchSide(null, numbered, outs);
    }

    private static OutputCol nullable(OutputCol o) {
        return new OutputCol(o.name(), o.type(), true, OutputCol.Origin.DERIVED);
    }

    /**
     * The batch statement over {@code sides} (at least one): one branch per side, its index ({@link #INDEX}) its place
     * in {@code sides}, its rows' order ({@link #ORDER}: 1 for an inline side, its statement's ordinal for a numbered
     * one), its own columns' values and a typed slot ({@link SqlExpr.Reason#SLOT}) for every other side's -- the
     * branches a UNION ALL, read in order of index and row order.
     */
    public static SqlQuery batch(List<BatchSide> sides) {
        List<OutputCol> outs = new ArrayList<>();
        outs.add(new OutputCol(INDEX, SqlType.Scalar.BIGINT, false));
        outs.add(new OutputCol(ORDER, SqlType.Scalar.BIGINT, false));
        for (BatchSide side : sides) {
            outs.addAll(side.outs());
        }
        List<SqlQuery> branches = new ArrayList<>(sides.size());
        int at = 2;
        for (int i = 0; i < sides.size(); i++) {
            BatchSide side = sides.get(i);
            SqlQuery numbered = side.numbered();
            List<SqlSelect.Projection> ps = new ArrayList<>(outs.size());
            ps.add(new SqlSelect.Projection(new SqlExpr.IntLit(i), INDEX, outs.get(0)));
            ps.add(new SqlSelect.Projection(numbered == null ? new SqlExpr.IntLit(1)
                    : SqlExpr.Column.of(SIDE, numbered.outputs().get(numbered.outputs().size() - 1)), ORDER, outs.get(1)));
            for (int c = 2; c < outs.size(); c++) {
                OutputCol o = outs.get(c);
                int k = c - at;
                SqlExpr v;
                if (k < 0 || k >= side.outs().size()) {
                    v = new SqlExpr.Cast(new SqlExpr.NullLit(), o.type(), SqlExpr.Reason.SLOT);
                } else if (numbered == null) {
                    v = java.util.Objects.requireNonNull(side.inline()).get(k).value();
                } else {
                    v = SqlExpr.Column.of(SIDE, numbered.outputs().get(k));
                }
                ps.add(new SqlSelect.Projection(v, o.name(), o));
            }
            at += side.outs().size();
            SqlSource from = numbered == null ? new SqlSource.Dual() : new SqlSource.Subselect(numbered, SIDE, null);
            branches.add(new SqlSelect(ps, false, from, null, List.of(), null, null, List.of(), null, null, outs));
        }
        SqlQuery union = branches.size() == 1 ? branches.get(0) : new SqlUnion(branches, true, outs);
        List<SqlSelect.Projection> read = new ArrayList<>(outs.size());
        for (OutputCol o : outs) {
            read.add(new SqlSelect.Projection(SqlExpr.Column.of(UNION, o), o.name(), o));
        }
        List<SqlSelect.SortKey> order = List.of(
                new SqlSelect.SortKey(SqlExpr.Column.of(UNION, outs.get(0)), true, null, null),
                new SqlSelect.SortKey(SqlExpr.Column.of(UNION, outs.get(1)), true, null, null));
        return new SqlSelect(read, false, new SqlSource.Subselect(union, UNION, null), null, List.of(), null, null,
                order, null, null, outs);
    }
}
