// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A ROW'S VALUES COMPUTED ONCE, WRITTEN BACK (the lean program's step 7, ruled 2026-11-16): the lowering binds what a
 * select writes more than once over its own row as a {@link SqlSource.RowValues}; a dialect that does not read it in a
 * form of its own writes each value back where it is read ({@link Inline}), the same query either way.
 */
public final class RowLets {

    private RowLets() {
    }

    /** A binding level's alias prefix: {@code __r<level>}. */
    public static final String ALIAS_PREFIX = "__r";

    /** A bound value's column prefix: {@code __rv<N>}. */
    public static final String COLUMN_PREFIX = "__rv";

    /**
     * Each {@link SqlSource.RowValues} written back: the select over it reads its rows again, and each reference to a
     * bound value is the value (a value reading a lower binding is that binding's value, in turn).
     */
    public static final class Inline extends SqlRewriter {

        @Override
        protected SqlQuery select(SqlSelect s) {
            SqlSource from = s.from();
            List<SqlSource.RowValues> levels = new ArrayList<>();
            while (from instanceof SqlSource.RowValues rv) {
                levels.add(0, rv);
                from = rv.rows();
            }
            if (levels.isEmpty()) {
                return s;
            }
            Map<String, SqlExpr> values = new HashMap<>();
            for (SqlSource.RowValues rv : levels) {
                for (SqlSelect.Projection p : rv.values()) {
                    values.put(rv.alias() + "." + p.alias(), swap(values).rewriteExprOf(p.expr()));
                }
            }
            Swap all = swap(values);
            // the select's own terms only: its FROM is the rows, which read no binding
            SqlSelect terms = (SqlSelect) all.rewrite(new SqlSelect(s.projections(), s.distinct(),
                    new SqlSource.Dual(), s.where(), s.groupBy(), s.having(), s.qualify(), s.orderBy(), s.limit(),
                    s.offset(), s.outputs()));
            return new SqlSelect(terms.projections(), terms.distinct(), from, terms.where(), terms.groupBy(),
                    terms.having(), terms.qualify(), terms.orderBy(), terms.limit(), terms.offset(), terms.outputs());
        }

        private static Swap swap(Map<String, SqlExpr> values) {
            return new Swap(java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(values)));
        }
    }

    /** A reference to a bound value replaced by the value. */
    private static final class Swap extends SqlRewriter {

        private final Map<String, SqlExpr> values;

        Swap(Map<String, SqlExpr> values) {
            this.values = values;
        }

        SqlExpr rewriteExprOf(SqlExpr e) {
            return values.isEmpty() ? e : rewriteExpr(e);
        }

        @Override
        protected SqlExpr expr(SqlExpr e) {
            if (e instanceof SqlExpr.Column c && c.table() != null) {
                SqlExpr v = values.get(c.table() + "." + c.name());
                if (v != null) {
                    return v;
                }
            }
            return e;
        }
    }
}
