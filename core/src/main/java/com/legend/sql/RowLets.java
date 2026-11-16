// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A ROW'S VALUES COMPUTED ONCE (step 7's second half, the lean program, 2026-11-16): an expression a select writes more
 * than once over its own row is bound once beside the row, {@code FROM <rows> CROSS JOIN LATERAL (SELECT <value> AS
 * __rv0) AS __r0}, and read by column. The lowering binds (LetBinding); this class names the shape and writes it back
 * inline for a database that reads no lateral ({@link Inline}), so every dialect either reads the binding in its own
 * form or writes the expression where it stood, the same query either way.
 *
 * <p>The shape: a {@link SqlSource.Join} of kind {@link SqlSource.Join.Kind#CROSS_LATERAL} whose right side is a
 * {@link SqlSource.Subselect} aliased {@code __r<N>} over a FROM-less select of plain projections.
 */
public final class RowLets {

    private RowLets() {
    }

    /** The binding's alias prefix: {@code __r<level>}. */
    public static final String ALIAS_PREFIX = "__r";

    /** A bound value's column prefix: {@code __rv<N>}. */
    public static final String COLUMN_PREFIX = "__rv";

    /** Whether {@code j} is a row binding: a cross lateral over a FROM-less select aliased {@code __r<N>}. */
    public static boolean is(SqlSource.Join j) {
        return j.kind() == SqlSource.Join.Kind.CROSS_LATERAL
                && j.right() instanceof SqlSource.Subselect sub
                && sub.alias().startsWith(ALIAS_PREFIX) && isLevel(sub.alias())
                && sub.inner() instanceof SqlSelect s && s.from() instanceof SqlSource.Dual;
    }

    private static boolean isLevel(String alias) {
        String n = alias.substring(ALIAS_PREFIX.length());
        return !n.isEmpty() && n.chars().allMatch(Character::isDigit);
    }

    /**
     * The row bindings written back where they stood, for a database that reads no lateral: each select whose FROM
     * ends in row bindings reads its rows again, and each reference to a bound value is the value (a value reading an
     * earlier binding is that binding's value, in turn).
     */
    public static final class Inline extends SqlRewriter {

        @Override
        protected SqlQuery select(SqlSelect s) {
            SqlSource from = s.from();
            List<SqlSource.Join> bindings = new ArrayList<>();
            while (from instanceof SqlSource.Join j && is(j)) {
                bindings.add(0, j);
                from = j.left();
            }
            if (bindings.isEmpty()) {
                return s;
            }
            Map<String, SqlExpr> values = new HashMap<>();
            for (SqlSource.Join j : bindings) {
                SqlSource.Subselect sub = (SqlSource.Subselect) j.right();
                SqlSelect inner = (SqlSelect) sub.inner();
                for (SqlSelect.Projection p : inner.projections()) {
                    values.put(sub.alias() + "." + p.alias(), substitute(p.expr(), values));
                }
            }
            SqlRewriter swap = new SqlRewriter() {
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
            };
            SqlSelect bare = new SqlSelect(s.projections(), s.distinct(), from, s.where(), s.groupBy(), s.having(),
                    s.qualify(), s.orderBy(), s.limit(), s.offset(), s.outputs());
            // the select's own terms only: its FROM is the rows, which read no binding
            SqlSelect swapped = (SqlSelect) swap.rewrite(new SqlSelect(bare.projections(), bare.distinct(),
                    new SqlSource.Dual(), bare.where(), bare.groupBy(), bare.having(), bare.qualify(), bare.orderBy(),
                    bare.limit(), bare.offset(), bare.outputs()));
            return new SqlSelect(swapped.projections(), swapped.distinct(), from, swapped.where(), swapped.groupBy(),
                    swapped.having(), swapped.qualify(), swapped.orderBy(), swapped.limit(), swapped.offset(),
                    swapped.outputs());
        }

        private static SqlExpr substitute(SqlExpr e, Map<String, SqlExpr> values) {
            if (values.isEmpty()) {
                return e;
            }
            SqlRewriter swap = new SqlRewriter() {
                @Override
                protected SqlExpr expr(SqlExpr x) {
                    if (x instanceof SqlExpr.Column c && c.table() != null) {
                        SqlExpr v = values.get(c.table() + "." + c.name());
                        if (v != null) {
                            return v;
                        }
                    }
                    return x;
                }
            };
            SqlSelect one = (SqlSelect) swap.rewrite(new SqlSelect(
                    List.of(new SqlSelect.Projection(e, "v", new OutputCol("v", SqlType.Scalar.VARCHAR, true))), false,
                    new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null,
                    List.of(new OutputCol("v", SqlType.Scalar.VARCHAR, true))));
            return one.projections().get(0).expr();
        }
    }
}
