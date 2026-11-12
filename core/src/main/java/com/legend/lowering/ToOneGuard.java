// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * A relation's {@code toOne()} at a statement's root, checked by the DATABASE (decision D2 of
 * docs/GOALS_OF_RECORD_2026_11_07.md, the user, 2026-11-07: "sql guard is good"): the relation's rows, each counted by a
 * window over the whole relation, left-joined onto a one-row source, so an empty relation still yields the one row the
 * check reads; any count but one raises Pure's own {@code Cannot cast a collection of size N to multiplicity [1]}.
 * One read of the relation; proven on DuckDB and H2 for zero, one and three rows before it landed. Until then the
 * runner counted the rows in Java and the judge read the typed tree at run to know it should.
 *
 * <pre>
 * SELECT r.c1, ... FROM (SELECT 1 AS __d) d
 *   LEFT JOIN (SELECT q.c1, ..., COUNT(*) OVER () AS __rows FROM (q) q) r ON TRUE
 *  WHERE CASE WHEN COALESCE(r.__rows, 0) = 1 THEN TRUE ELSE error('Cannot cast ...') END
 * </pre>
 */
final class ToOneGuard {

    private ToOneGuard() {
    }

    /** {@code q}'s rows, exactly one, or the database raises Pure's size error. */
    static SqlQuery exactlyOne(SqlQuery q, Supplier<String> aliases) {
        List<OutputCol> cols = q.outputs();
        String qa = aliases.get();
        String ra = aliases.get();
        String da = aliases.get();
        // the count's column beside the relation's own: a name none of them has (the milestone audit, 2026-11-07)
        String countName = "__rows";
        while (named(cols, countName)) {
            countName = "_" + countName;
        }
        OutputCol rows = new OutputCol(countName, SqlType.Scalar.BIGINT, false);
        List<SqlSelect.Projection> counted = new ArrayList<>(cols.size() + 1);
        for (OutputCol c : cols) {
            counted.add(new SqlSelect.Projection(SqlExpr.Column.of(qa, c), c.name(), c));
        }
        counted.add(new SqlSelect.Projection(new SqlExpr.WindowCall(
                new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(), false, List.of()), List.of(), List.of(), null),
                rows.name(), rows));
        List<OutputCol> countedOut = new ArrayList<>(cols);
        countedOut.add(rows);
        SqlSelect withCount = new SqlSelect(counted, false, new SqlSource.Subselect(q, qa, null), null, List.of(),
                null, null, List.of(), null, null, countedOut);
        OutputCol d = new OutputCol("__d", SqlType.Scalar.INTEGER, false);
        SqlSelect one = new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.IntLit(1), "__d", d)), false,
                new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null, List.of(d));
        SqlSource joined = new SqlSource.Join(new SqlSource.Subselect(one, da, null),
                new SqlSource.Subselect(withCount, ra, null), SqlSource.Join.Kind.LEFT, new SqlExpr.BoolLit(true));
        SqlExpr n = SqlExpr.Call.of(SqlFn.COALESCE, SqlExpr.Column.of(ra, rows), new SqlExpr.IntLit(0));
        SqlExpr raise = SqlExpr.Call.of(SqlFn.ERROR, SqlExpr.Call.of(SqlFn.CONCAT,
                new SqlExpr.StringLit("Cannot cast a collection of size "),
                new SqlExpr.Cast(n, SqlType.Scalar.VARCHAR, SqlExpr.Reason.RAISE),
                new SqlExpr.StringLit(" to multiplicity [1]")));
        SqlExpr check = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.EQUAL, n, new SqlExpr.IntLit(1)), new SqlExpr.BoolLit(true))), raise, SqlExpr.Reason.RAISE);
        List<SqlSelect.Projection> out = new ArrayList<>(cols.size());
        for (OutputCol c : cols) {
            out.add(new SqlSelect.Projection(SqlExpr.Column.of(ra, c), c.name(), c));
        }
        return new SqlSelect(out, false, joined, check, List.of(), null, null, List.of(), null, null, cols);
    }

    private static boolean named(List<OutputCol> cols, String name) {
        for (OutputCol c : cols) {
            if (c.name().equalsIgnoreCase(name)) {
                return true;
            }
        }
        return false;
    }
}
