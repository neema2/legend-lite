// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.base.Nullable;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A SELECT OVER ONE SUBSELECT MERGED INTO IT (2026-11-04, the user's item 4 of the timeseries SQL): a select whose
 * one source is a subselect reads that subselect's columns; where the merge keeps every row, every value and every
 * raise, the outer select is written over the subselect's own source with the subselect's expressions in place of
 * its columns. Each Pure step lowers to its own level (a rename, an extend, a filter), and a database plans every
 * level: a timeseries answer of 300 levels spent 460 ms of DuckDB's 560 in planning (measured 2026-11-04). This is
 * {@link ProjectionWrapsFold} (H2's, a wrap of plain columns) widened to computed values, filters and windows.
 *
 * <p>The merge keeps the answer, by construction:
 * <ul>
 *   <li>the subselect's rows are its source's rows, filtered: no DISTINCT, ORDER BY, LIMIT or OFFSET in it;</li>
 *   <li>a subselect that groups, or has a window, or a QUALIFY, merges only under an outer select that projects,
 *       orders or limits (none of WHERE, GROUP BY, HAVING, QUALIFY, an aggregate or a window over its values: a
 *       window over a window is no SQL, and a filter over one is a later level's);</li>
 *   <li>a value the subselect computes is computed once: the outer reads it at most once, or it is a plain column;</li>
 *   <li>a value whose evaluation may raise ({@link SqlExpr#raisesNothing}) is evaluated on the same rows as
 *       before: the outer reads it once, as a projection of its own, and filters and limits nothing; a subselect's column the
 *       outer does not read is dropped only when it raises nothing;</li>
 *   <li>a filter of the outer joins the subselect's with AND only when it raises nothing (it may be evaluated on
 *       rows the subselect's own filter excludes);</li>
 *   <li>the outer reads the subselect through qualified columns alone: no star, no unqualified name, no subquery
 *       (whose reads this pass does not follow);</li>
 *   <li>no GROUP BY or ORDER BY key becomes a constant (a column's position on some databases).</li>
 * </ul>
 * Not on the engine-text printers: legend-engine's text keeps its levels.
 */
final class SelectMerge extends SqlRewriter {

    @Override
    protected SqlQuery select(SqlSelect s) {
        SqlSelect out = s;
        for (SqlSelect next = merged(out); next != null; next = merged(out)) {
            out = next;
        }
        return out;
    }

    /** {@code s} merged with its one subselect, or null where the merge could change the answer. */
    private static @Nullable SqlSelect merged(SqlSelect s) {
        if (!(s.from() instanceof SqlSource.Subselect sub) || !(sub.inner() instanceof SqlSelect in)
                || in.projections().isEmpty() || in.distinct() || !in.orderBy().isEmpty() || in.limit() != null
                || in.offset() != null) {
            return null;
        }
        Map<String, SqlExpr> byName = new HashMap<>();
        for (SqlSelect.Projection p : in.projections()) {
            String name = p.outputName();
            if (name == null || byName.put(name, p.expr()) != null) {
                return null;
            }
        }
        // SELECT * (no projections) reads each of the subselect's columns once, under its own name
        List<SqlSelect.Projection> outer = !s.projections().isEmpty() ? s.projections()
                : in.projections().stream().map(p -> new SqlSelect.Projection(
                        SqlExpr.Column.derived(sub.alias(), java.util.Objects.requireNonNull(p.outputName())), null, p.out()))
                        .toList();
        Reads reads = new Reads(sub.alias());
        reads.of(s, outer);
        if (reads.refused || !byName.keySet().containsAll(reads.count.keySet())) {
            return null;
        }
        boolean filters = s.where() != null || !s.groupBy().isEmpty() || s.having() != null || s.qualify() != null;
        boolean inLevels = !in.groupBy().isEmpty() || in.having() != null || in.qualify() != null
                || in.projections().stream().anyMatch(p -> levelled(p.expr()));
        if (inLevels && (filters || reads.levelled || reads.ordersBy(s, byName))) {
            return null;
        }
        for (SqlSelect.Projection p : in.projections()) {
            String name = p.outputName();
            int n = reads.count.getOrDefault(name, 0);
            if (n > 1 && !(p.expr() instanceof SqlExpr.Column)) {
                return null;
            }
            if (!SqlExpr.raisesNothing(p.expr())
                    && (n != 1 || !reads.bare.contains(name) || filters || s.limit() != null
                    || s.offset() != null)) {
                return null;
            }
        }
        if (s.where() != null && in.where() != null && !SqlExpr.raisesNothing(s.where())) {
            return null;
        }
        Substitute sub2 = new Substitute(sub.alias(), byName);
        List<SqlSelect.Projection> ps = new ArrayList<>(outer.size());
        for (SqlSelect.Projection p : outer) {
            String name = p.outputName();
            if (name == null) {
                return null;
            }
            SqlExpr e = sub2.apply(p.expr());
            boolean named = e instanceof SqlExpr.Column c && c.name().equals(name);
            ps.add(new SqlSelect.Projection(e, named ? null : name, p.out()));
        }
        SqlExpr outerWhere = s.where() == null ? null : sub2.apply(s.where());
        SqlExpr where = in.where() == null ? outerWhere
                : outerWhere == null ? in.where() : SqlExpr.Call.of(SqlFn.AND, in.where(), outerWhere);
        List<SqlSelect.SortKey> orderBy = new ArrayList<>(s.orderBy().size());
        for (SqlSelect.SortKey k : s.orderBy()) {
            orderBy.add(new SqlSelect.SortKey(sub2.apply(k.expr()), k.ascending(), k.nullOrder(), k.outputName()));
        }
        // a levelled subselect brings its own clauses (the outer has none); a plain one has none of its own but WHERE
        List<SqlExpr> groupBy = inLevels ? in.groupBy() : s.groupBy().stream().map(sub2::apply).toList();
        // a constant GROUP BY or ORDER BY key is a column's position on some databases (GROUP BY 1): never made by a merge
        if (!inLevels && groupBy.stream().anyMatch(SelectMerge::constant)
                || orderBy.stream().anyMatch(k -> constant(k.expr()))) {
            return null;
        }
        SqlExpr having = inLevels ? in.having() : s.having() == null ? null : sub2.apply(s.having());
        SqlExpr qualify = inLevels ? in.qualify() : s.qualify() == null ? null : sub2.apply(s.qualify());
        return new SqlSelect(ps, s.distinct(), in.from(), where, groupBy, having, qualify, orderBy, s.limit(),
                s.offset(), s.outputs());
    }

    /** Whether {@code e} reads no column: a constant, whatever its spelling. */
    private static boolean constant(SqlExpr e) {
        if (e instanceof SqlExpr.Column || e instanceof SqlExpr.WindowCall || SqlAgg.aggregatesSelect(e)) {
            return false;
        }
        for (SqlExpr c : e.children()) {
            if (!constant(c)) {
                return false;
            }
        }
        return true;
    }

    /** Whether {@code e} holds an aggregate, a window or an UNNEST: a value of its select's level, not of a row of its
     *  source. */
    private static boolean levelled(SqlExpr e) {
        if (e instanceof SqlExpr.WindowCall || SqlAgg.aggregatesSelect(e)
                || e instanceof SqlExpr.Call c && c.fn() == SqlFn.UNNEST) {
            return true;
        }
        for (SqlExpr c : e.children()) {
            if (levelled(c)) {
                return true;
            }
        }
        return false;
    }

    /** The outer select's reads of the subselect {@code alias}: each column's count, the ones read as a projection of
     *  their own, whether it aggregates or has a window, and whether it reads in a way this pass does not follow. */
    private static final class Reads extends SqlRewriter {
        private final String alias;
        final Map<String, Integer> count = new HashMap<>();
        final java.util.Set<String> bare = new java.util.HashSet<>();
        boolean refused;
        boolean levelled;

        Reads(String alias) {
            this.alias = alias;
        }

        void of(SqlSelect s, List<SqlSelect.Projection> projections) {
            for (SqlSelect.Projection p : projections) {
                if (p.expr() instanceof SqlExpr.Column c && alias.equals(c.table())) {
                    bare.add(c.name());
                }
                read(p.expr());
            }
            if (s.where() != null) {
                read(s.where());
            }
            s.groupBy().forEach(this::read);
            if (s.having() != null) {
                read(s.having());
            }
            if (s.qualify() != null) {
                read(s.qualify());
            }
            s.orderBy().forEach(k -> read(k.expr()));
        }

        /** Whether {@code s} orders by a value of the subselect's level (an aggregate, a window). */
        boolean ordersBy(SqlSelect s, Map<String, SqlExpr> byName) {
            for (SqlSelect.SortKey k : s.orderBy()) {
                Reads keyReads = new Reads(alias);
                keyReads.rewriteExpr(k.expr());
                for (String name : keyReads.count.keySet()) {
                    SqlExpr v = byName.get(name);
                    if (v == null || levelled(v)) {
                        return true;
                    }
                }
            }
            return false;
        }

        private void read(SqlExpr e) {
            levelled |= levelled(e);
            rewriteExpr(e);
        }

        @Override
        protected SqlExpr expr(SqlExpr e) {
            switch (e) {
                case SqlExpr.Column c -> {
                    if (c.table() == null) {
                        refused = true;
                    } else if (alias.equals(c.table())) {
                        count.merge(c.name(), 1, Integer::sum);
                    }
                }
                case SqlExpr.Star st -> refused = true;
                case SqlExpr.StarExcept st -> refused = true;
                case SqlExpr.ScalarSubquery q -> refused = true;
                case SqlExpr.Exists q -> refused = true;
                case SqlExpr.InSubquery q -> refused = true;
                case SqlExpr.Quantified q -> refused = true;
                case SqlExpr.DeferredTdsString d -> refused = true;
                case SqlExpr.Lambda l -> refused = true;
                default -> { }
            }
            return e;
        }
    }

    /** Each column of the subselect {@code alias} replaced by the subselect's expression for it. */
    private static final class Substitute extends SqlRewriter {
        private final String alias;
        private final Map<String, SqlExpr> byName;

        Substitute(String alias, Map<String, SqlExpr> byName) {
            this.alias = alias;
            this.byName = byName;
        }

        SqlExpr apply(SqlExpr e) {
            return rewriteExpr(e);
        }

        @Override
        protected SqlExpr expr(SqlExpr e) {
            if (e instanceof SqlExpr.Column c && alias.equals(c.table())) {
                // every read was checked to name one of the subselect's columns (merged)
                return java.util.Objects.requireNonNull(byName.get(c.name()), c.name());
            }
            return e;
        }
    }
}
