// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Postgres's string functions take their counts and positions as a 32-bit {@code integer}: {@code substr}, {@code left},
 * {@code right}, {@code repeat}, {@code lpad}, {@code rpad} and {@code chr} have no {@code bigint} overload, and Postgres
 * casts a {@code bigint} to {@code integer} only on assignment, so {@code substr(s, 2, length(s) - 2)} computed in 64
 * bits (Pure's Integer) names no function there. This pass casts each such argument that is no literal to an
 * {@code integer}: a value past its range is Postgres's error, as a count past it is in other engines (IQ island,
 * 2026-10-17, the corpus lane: 23 of SEDE's and SQLStorm's queries, {@code SUBSTRING(s, 2, LEN(s) - 2)}; IQ
 * raises past the range too). It runs after {@link SubstringClamp}, whose start it casts too.
 */
public final class StringCountsAsInt extends SqlRewriter {

    /** The functions, by the positions of their 32-bit arguments. */
    private static final Map<SqlFn, List<Integer>> COUNTS = Map.of(
            SqlFn.SUBSTRING, List.of(1, 2),
            SqlFn.LEFT, List.of(1),
            SqlFn.RIGHT, List.of(1),
            SqlFn.REPEAT_STR, List.of(1),
            SqlFn.LPAD, List.of(1),
            SqlFn.RPAD, List.of(1),
            SqlFn.CHR, List.of(0));

    @Override
    protected SqlExpr expr(SqlExpr e) {
        SqlExpr out = super.expr(e);
        if (out instanceof SqlExpr.Call c && COUNTS.containsKey(c.fn())) {
            List<SqlExpr> args = new ArrayList<>(c.args());
            boolean changed = false;
            for (int i : COUNTS.get(c.fn())) {
                if (i < args.size() && !(args.get(i) instanceof SqlExpr.IntLit)
                        && !(args.get(i) instanceof SqlExpr.Cast cast && cast.target() == SqlType.Scalar.INTEGER)) {
                    args.set(i, narrowed(args.get(i)));
                    changed = true;
                }
            }
            if (changed) {
                return new SqlExpr.Call(c.fn(), args);
            }
        }
        return out;
    }

    /** {@code x} as an {@code integer}. */
    private static SqlExpr narrowed(SqlExpr x) {
        return new SqlExpr.Cast(x, SqlType.Scalar.INTEGER);
    }
}
