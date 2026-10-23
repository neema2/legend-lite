// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

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
 * evaluated"). A derived table that is DISTINCT over more than the column, or that sorts, limits, filters its groups or
 * qualifies (each may read the columns dropped), stays as it is.
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
        while (q instanceof SqlSelect s && s.from() instanceof SqlSource.Subselect sub
                && sub.inner() instanceof SqlSelect in && readsOneColumnOnly(s, sub.alias()) && keepsItsRows(in)) {
            SqlSelect.Projection p = s.projections().get(0);
            String name = ((SqlExpr.Column) p.expr()).name();
            SqlSelect.Projection read = projectionNamed(in, name);
            if (read == null) {
                break;
            }
            q = new SqlSelect(List.of(new SqlSelect.Projection(read.expr(), p.alias() != null ? p.alias() : name,
                    p.out())), in.distinct(), in.from(), in.where(), in.groupBy(), null, null, List.of(), null, null,
                    s.outputs());
        }
        return q;
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
                && in.orderBy().isEmpty() && in.limit() == null && in.offset() == null;
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
