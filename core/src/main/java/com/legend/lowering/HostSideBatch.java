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
import com.legend.sql.SqlWith;

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
            // element: removeDuplicates' list read as rows all numbered 1, 2026-11-17), no subquery (what it
            // aggregates arrives in an order a UNION ALL's branches need not keep), no raise
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
                    || e instanceof SqlExpr.Star
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
     * output), each typed as the side's own output is -- a UNION ALL's column takes the type its branches share, so
     * the other branches' typed slots carry the type the side's statement answers, never a slot the plan reads it by
     * (H2's ID column is VARCHAR under a BIGINT slot: a BIGINT slot made the union answer 7 where the side answers
     * '7', 2026-11-17). Inline where it is a single value of no FROM ({@link #singleValueColumns}); else numbered,
     * where its statement may be a derived table (no WITH: a CTE inside a derived table is not every database's) and
     * {@code typedByItsTree}: the side reads no stored table, so the types its tree says are the types its cells
     * have (a stored table's column is the database's DDL, which the model's type need not be); null otherwise.
     */
    public static @com.legend.base.Nullable BatchSide side(SqlQuery side, String aliasPrefix, List<String> names,
            boolean typedByItsTree) {
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
        if (!typedByItsTree || side instanceof com.legend.sql.SqlWith
                || sorted(side) && SideOrdinal.defined(side) == null) {
            // a WITH is no derived table every database reads; a sort whose ordinal the plan cannot define would be
            // read by its arrival through the batch's selects, which a database need not keep (SideOrdinal)
            return null;
        }
        SqlQuery defined = SideOrdinal.defined(side);
        SqlQuery numbered = defined != null ? defined : side;
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
            SqlExpr order = numbered == null ? new SqlExpr.IntLit(1)
                    : side.ordinal() ? SqlExpr.Column.of(SIDE, numbered.outputs().get(numbered.outputs().size() - 1))
                    : new SqlExpr.WindowCall(new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER, List.of()), List.of(),
                            List.of(), null);
            ps.add(new SqlSelect.Projection(order, ORDER, outs.get(1)));
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
            if (i > 0) {
                // read after the earlier side (G10): a one-row count of it joined as the build side, which the database
                // finishes before it reads this side's rows
                from = numbered == null ? after(i - 1) : new SqlSource.Join(from, after(i - 1), SqlSource.Join.Kind.CROSS,
                        null);
            }
            branches.add(new SqlSelect(ps, false, from, null, List.of(), null, null, List.of(), null, null, outs));
        }
        // THE SIDES IN ORDER (G10, the orchestrator's MUST of 2026-11-17: a plan's answer never depends on timing): every
        // branch but the last is a MATERIALIZED expression, in the sides' order, so the database finishes each side
        // before it reads the next -- where two sides raise, the first side's error is the statement's, as upstream
        // evaluates an assert's arguments left to right (a UNION ALL's branches run in parallel on DuckDB: either
        // error, by timing)
        List<SqlWith.Cte> earlier = new ArrayList<>();
        for (int i = 0; i + 1 < branches.size(); i++) {
            String name = ORDERED + i;
            earlier.add(new SqlWith.Cte(name, branches.get(i), true));
            List<SqlSelect.Projection> all = new ArrayList<>(outs.size());
            for (OutputCol o : outs) {
                all.add(new SqlSelect.Projection(SqlExpr.Column.of(name, o), o.name(), o));
            }
            branches.set(i, new SqlSelect(all, false, new SqlSource.Cte(name, name, outs), null, List.of(), null, null,
                    List.of(), null, null, outs));
        }
        SqlQuery union = branches.size() == 1 ? branches.get(0) : new SqlUnion(branches, true, outs);
        List<SqlSelect.Projection> read = new ArrayList<>(outs.size());
        for (OutputCol o : outs) {
            read.add(new SqlSelect.Projection(SqlExpr.Column.of(UNION, o), o.name(), o));
        }
        List<SqlSelect.SortKey> order = List.of(
                new SqlSelect.SortKey(SqlExpr.Column.of(UNION, outs.get(0)), true, null, null),
                new SqlSelect.SortKey(SqlExpr.Column.of(UNION, outs.get(1)), true, null, null));
        return SqlWith.prepend(earlier, new SqlSelect(read, false, new SqlSource.Subselect(union, UNION, null), null,
                List.of(), null, null, order, null, null, outs));
    }

    /** The batch's name for an earlier side's branch, evaluated before the next (G10). */
    private static final String ORDERED = "__bs";

    /** The count of the earlier side {@code i}'s branch, one row: what the next side is read after (G10). */
    private static SqlSource after(int i) {
        OutputCol n = new OutputCol("__n", SqlType.Scalar.BIGINT, false);
        String name = ORDERED + i;
        SqlSelect count = new SqlSelect(List.of(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.COUNT,
                List.of(new SqlExpr.IntLit(1)), false, List.of()), n.name(), n)), false,
                new SqlSource.Cte(name, name, List.of()), null, List.of(), null, null, List.of(), null, null, List.of(n));
        return new SqlSource.Subselect(count, "__g" + i, null);
    }
}
