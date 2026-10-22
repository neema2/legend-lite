// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.TypeFact;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Each window call inside a larger expression of a select's projection computed in a select below, the expression
 * written over its column (2026-10-20, the parity phase). MySQL evaluates a subquery of an expression over a window
 * call for every row, whatever branch of a CASE it is in (measured on 8.4: {@code CASE WHEN LEAD(x) OVER w IS NULL
 * THEN NULL ELSE coalesce(LEAD(d) OVER w + INTERVAL 1 SECOND, (SELECT ... UNION ALL SELECT 0)) END} raised
 * "Subquery returns more than 1 row" on rows whose date was in range), so a guard's raise fired where it was not
 * reached; over the window's column, computed below, the CASE guards it again (measured). The select below reads the
 * FROM under its own alias, so every reference of the select still reads it. A select that groups, is distinct,
 * filters on windows or reads a join is left as it is.
 */
final class WindowsComputedBelow extends SqlRewriter {

    /** Numbers each rewritten select's window columns across the statement. */
    private final int[] next = {0};

    @Override
    public SqlQuery rewriteRoot(SqlQuery q) {
        next[0] = 0;
        return rewrite(q);
    }

    @Override
    protected SqlQuery select(SqlSelect s) {
        List<SqlExpr.WindowCall> calls = new ArrayList<>();
        for (SqlSelect.Projection p : s.projections()) {
            if (!(p.expr() instanceof SqlExpr.WindowCall)) {
                collect(p.expr(), calls);
            }
        }
        if (calls.isEmpty() || !s.groupBy().isEmpty() || s.having() != null || s.distinct()
                || s.qualify() != null || s.from() instanceof SqlSource.Join || s.from() instanceof SqlSource.Dual
                || s.from().alias() == null
                || s.from().outputs().isEmpty()) {
            return s;
        }
        String alias = s.from().alias();
        String stem = "__wb" + next[0]++;
        List<OutputCol> fromOutputs = s.from().outputs();
        List<SqlSelect.Projection> below = new ArrayList<>();
        for (OutputCol oc : fromOutputs) {
            below.add(new SqlSelect.Projection(SqlExpr.Column.of(alias, oc), oc.name(), oc));
        }
        Map<SqlExpr, SqlExpr> replaced = new LinkedHashMap<>();
        for (SqlExpr.WindowCall w : calls) {
            if (replaced.containsKey(w)) {
                continue;
            }
            String name = stem + "w" + replaced.size();
            OutputCol out = w.type() instanceof TypeFact.Typed t ? new OutputCol(name, t.type(), true) : null;
            below.add(new SqlSelect.Projection(w, name, out));
            replaced.put(w, out != null ? SqlExpr.Column.of(alias, out) : SqlExpr.Column.derived(alias, name));
        }
        SqlSelect rows = new SqlSelect(below, false, s.from(), s.where(), List.of(), null, null, List.of(), null,
                null, List.of());
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (SqlSelect.Projection p : s.projections()) {
            if (p.expr() instanceof SqlExpr.Star st && (st.table() == null || st.table().equals(alias))) {
                // the FROM's own columns, never the windows'
                for (OutputCol oc : fromOutputs) {
                    ps.add(new SqlSelect.Projection(SqlExpr.Column.of(alias, oc), oc.name(), oc));
                }
                continue;
            }
            SqlExpr x = substitute(p.expr(), replaced);
            ps.add(x == p.expr() ? p : new SqlSelect.Projection(x, p.alias(), p.out()));
        }
        List<SqlSelect.SortKey> order = new ArrayList<>();
        for (SqlSelect.SortKey k : s.orderBy()) {
            SqlExpr e = substitute(k.expr(), replaced);
            order.add(e == k.expr() ? k : new SqlSelect.SortKey(e, k.ascending(), k.nullOrder(), k.outputName()));
        }
        return new SqlSelect(ps, false, new SqlSource.Subselect(rows, alias, null), null, List.of(), null, null,
                order, s.limit(), s.offset(), s.outputs());
    }

    private static void collect(SqlExpr e, List<SqlExpr.WindowCall> out) {
        if (e instanceof SqlExpr.WindowCall w) {
            out.add(w);
            return;
        }
        for (SqlExpr c : e.children()) {
            collect(c, out);
        }
    }

    private static SqlExpr substitute(SqlExpr e, Map<SqlExpr, SqlExpr> replaced) {
        SqlExpr r = replaced.get(e);
        if (r != null) {
            return r;
        }
        return e.children().isEmpty() ? e : e.mapChildren(c -> substitute(c, replaced));
    }
}
