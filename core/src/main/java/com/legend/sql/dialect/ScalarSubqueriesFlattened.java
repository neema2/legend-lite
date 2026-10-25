// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.List;

/**
 * A scalar subquery whose select only reads one column of a derived table is that derived table's select, that one
 * column kept: {@code (SELECT x.v FROM (SELECT y.c AS v FROM (SELECT COUNT(*) AS c FROM u WHERE u.t_id = t.id) y) x)}
 * is {@code (SELECT COUNT(*) AS v FROM u WHERE u.t_id = t.id)}. Snowflake decorrelates a subquery whose correlation
 * is in its own WHERE, not one two derived tables down (the IQ scale program's Snowflake lane, 2026-10-23: there a
 * correlated COUNT over no row is NULL, and a correlated read of no aggregate is "Unsupported subquery type cannot be
 * evaluated"). A derived table that is DISTINCT over more than the column, that sorts, limits, filters its groups,
 * qualifies or explodes a list (each may read or multiply by the columns dropped), or whose one row is an aggregate's
 * beside the column read, stays as it is.
 */
final class ScalarSubqueriesFlattened extends SqlRewriter {

    @Override
    protected SqlExpr expr(SqlExpr e) {
        if (e instanceof SqlExpr.ScalarSubquery s) {
            SqlQuery flat = flattened(s.subquery());
            return flat == s.subquery() ? e : new SqlExpr.ScalarSubquery(flat);
        }
        return e;
    }

    static SqlQuery flattened(SqlQuery q) {
        q = collapsed(q);
        while (q instanceof SqlSelect s && s.from() instanceof SqlSource.Subselect sub
                && sub.inner() instanceof SqlSelect in && readsOneColumnOnly(s, sub.alias()) && keepsItsRows(in)) {
            SqlSelect.Projection p = s.projections().get(0);
            String name = ((SqlExpr.Column) p.expr()).name();
            SqlSelect.Projection read = projectionNamed(in, name);
            // a derived table that aggregates with no grouping has one row only while an aggregate is kept: its column
            // of no aggregate, read alone, would be one row per source row (an independent review, 2026-10-23)
            if (read == null || in.groupBy().isEmpty() && !SqlAgg.aggregatesSelect(read.expr())
                    && in.projections().stream().anyMatch(x -> SqlAgg.aggregatesSelect(x.expr()))) {
                break;
            }
            q = new SqlSelect(List.of(new SqlSelect.Projection(read.expr(), p.alias() != null ? p.alias() : name,
                    p.out())), in.distinct(), in.from(), in.where(), in.groupBy(), null, null, List.of(), null, null,
                    s.outputs());
        }
        return collapsed(q);
    }

    /**
     * {@code q}, where it computes one value from the columns of a derived table that aggregates with no grouping (one
     * row) and does nothing else, as that derived table's select of the value, each column read as the aggregate it
     * names: {@code (SELECT f(x.s, x.n) FROM (SELECT SUM(v) AS s, COUNT(v) AS n FROM u WHERE u.k = t.k) x)} is {@code
     * (SELECT f(SUM(v), COUNT(v)) FROM u WHERE u.k = t.k)}. The IQ scale program, 2026-10-26 (the Snowflake lane's
     * census2 scalar_correlated_in_where: IQ's exact AVG, a quotient of a SUM and a COUNT, correlated a derived table
     * down, which Snowflake evaluates no subquery of). Else {@code q} itself.
     */
    private static SqlQuery collapsed(SqlQuery q) {
        if (!(q instanceof SqlSelect s && s.from() instanceof SqlSource.Subselect sub
                && sub.inner() instanceof SqlSelect in && s.projections().size() == 1 && !s.distinct()
                && s.where() == null && s.groupBy().isEmpty() && s.having() == null && s.qualify() == null
                && s.orderBy().isEmpty() && s.limit() == null && s.offset() == null && in.groupBy().isEmpty()
                && !in.distinct() && keepsItsRows(in) && !in.projections().isEmpty()
                && in.projections().stream().allMatch(x -> SqlAgg.aggregatesSelect(x.expr()))
                && !SqlAgg.aggregatesSelect(s.projections().get(0).expr()))) {
            return q;
        }
        String alias = sub.alias();
        boolean[] unread = {false};
        SqlExpr value = new SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr e) {
                if (e instanceof SqlExpr.Column c && alias.equals(c.table())) {
                    SqlSelect.Projection read = projectionNamed(in, c.name());
                    if (read == null) {
                        unread[0] = true;
                        return e;
                    }
                    return read.expr();
                }
                return e;
            }

            SqlExpr run(SqlExpr root) {
                return rewriteExpr(root);
            }
        }.run(s.projections().get(0).expr());
        if (unread[0]) {
            return q;
        }
        SqlSelect.Projection p = s.projections().get(0);
        return new SqlSelect(List.of(new SqlSelect.Projection(value, p.alias(), p.out())), false, in.from(), in.where(),
                List.of(), null, null, List.of(), null, null, s.outputs());
    }

    /** {@code s} reads one column of its derived table {@code alias} and does nothing else. */
    private static boolean readsOneColumnOnly(SqlSelect s, String alias) {
        return s.projections().size() == 1 && s.projections().get(0).expr() instanceof SqlExpr.Column c
                && (c.table() == null || c.table().equals(alias)) && !s.distinct() && s.where() == null
                && s.groupBy().isEmpty() && s.having() == null && s.qualify() == null && s.orderBy().isEmpty()
                && s.limit() == null && s.offset() == null;
    }

    /** Keeping one of {@code in}'s columns keeps its rows: no clause reads the columns dropped. */
    private static boolean keepsItsRows(SqlSelect in) {
        return (!in.distinct() || in.projections().size() == 1) && in.having() == null && in.qualify() == null
                && in.orderBy().isEmpty() && in.limit() == null && in.offset() == null
                && !ExistsOverOneRow.explodes(in);
    }

    /** {@code in}'s projection of output name {@code name}, or null (none, or a star that may hold it). */
    private static SqlSelect.@com.legend.base.Nullable Projection projectionNamed(SqlSelect in, String name) {
        for (SqlSelect.Projection p : in.projections()) {
            if (p.expr() instanceof SqlExpr.Star || p.expr() instanceof SqlExpr.StarExcept) {
                return null;
            }
            String own = p.alias() != null ? p.alias() : p.expr() instanceof SqlExpr.Column c ? c.name() : null;
            if (name.equals(own)) {
                return p;
            }
        }
        return null;
    }
}
