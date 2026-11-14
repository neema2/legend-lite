// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A PROJECTION-ONLY WRAP FOLDED INTO ITS SOURCE (H2): a select that only names or narrows the columns of its one
 * subselect -- each projection a column of it, no WHERE, GROUP BY, HAVING, QUALIFY, ORDER BY, LIMIT, OFFSET or
 * DISTINCT -- is that subselect with its columns under the outer names. The lowering frames a rename of a projected
 * source as legend-engine's text does (Lowerer.rename), and the engine-style printers keep that frame; H2's optimizer
 * plans every nested derived table again at each level above it (QueryExpressionIndex.getQuery, measured
 * 2026-11-01: a #TS answer of two lines whose periods a library function groups ran out of 512 MB planning), so H2
 * reads the same rows from one level fewer per rename. The subselect folds only where narrowing it cannot change its
 * rows or its order: it is not DISTINCT and has no ORDER BY, LIMIT or OFFSET.
 */
final class ProjectionWrapsFold extends SqlRewriter {

    /** How the database labels a table's column the dialect writes from its declared name: the name as written
     *  (DuckDB, SQLite, ClickHouse; Snowflake, which quotes every name), folded to lower case (Postgres) or to upper
     *  case (H2). A projection of a table's own column keeps its alias only where that label differs from the name
     *  (namedAs; PR #81, 2026-11-13). */
    private final java.util.function.UnaryOperator<String> bareLabel;

    ProjectionWrapsFold(java.util.function.UnaryOperator<String> bareLabel) {
        this.bareLabel = bareLabel;
    }

    @Override
    protected SqlQuery select(SqlSelect s) {
        if (!(s.from() instanceof SqlSource.Subselect sub) || !(sub.inner() instanceof SqlSelect inner)
                || !wrapOnly(s) || !foldable(inner)) {
            return s;
        }
        Map<String, SqlSelect.Projection> byName = new HashMap<>();
        for (SqlSelect.Projection p : inner.projections()) {
            String name = p.outputName();
            if (p.expr() instanceof SqlExpr.Star || name == null || byName.put(name, p) != null) {
                return s;
            }
        }
        List<SqlSelect.Projection> folded = new ArrayList<>(s.projections().size());
        Map<String, Integer> reads = new HashMap<>();
        for (SqlSelect.Projection p : s.projections()) {
            String name = p.outputName();
            if (!(p.expr() instanceof SqlExpr.Column c) || !sub.alias().equals(c.table()) || name == null) {
                return s;
            }
            SqlSelect.Projection source = byName.get(c.name());
            if (source == null) {
                return s;
            }
            // a value the subselect computes is computed once: read twice, it would be written twice (a random draw
            // drawn twice, a raise raised twice) -- SelectMerge's rule (the final review of PR #81, 2026-11-09)
            if (reads.merge(c.name(), 1, Integer::sum) > 1 && !(source.expr() instanceof SqlExpr.Column)) {
                return s;
            }
            boolean named = SelectMerge.namedAs(source.expr(), name, bareLabel);
            folded.add(new SqlSelect.Projection(source.expr(), named ? null : name, p.out()));
        }
        // a column the wrap does not read is dropped only where it keeps the rows and raises nothing: no UNNEST (a
        // row per element), aggregate (a row of all), raise or checked value (SqlExpr.raisesNothing)
        for (Map.Entry<String, SqlSelect.Projection> e : byName.entrySet()) {
            if (!reads.containsKey(e.getKey()) && !SqlExpr.raisesNothing(e.getValue().expr())) {
                return s;
            }
        }
        return new SqlSelect(folded, false, inner.from(), inner.where(), inner.groupBy(), inner.having(),
                inner.qualify(), inner.orderBy(), inner.limit(), inner.offset(), s.outputs());
    }

    /** Whether {@code s} does nothing but project its source's columns. */
    private static boolean wrapOnly(SqlSelect s) {
        return !s.projections().isEmpty() && !s.distinct() && s.where() == null && s.groupBy().isEmpty()
                && s.having() == null && s.qualify() == null && s.orderBy().isEmpty() && s.limit() == null
                && s.offset() == null;
    }

    /** Whether narrowing {@code inner}'s columns keeps its rows and their order. */
    private static boolean foldable(SqlSelect inner) {
        return !inner.projections().isEmpty() && !inner.distinct() && inner.orderBy().isEmpty()
                && inner.limit() == null && inner.offset() == null;
    }
}
