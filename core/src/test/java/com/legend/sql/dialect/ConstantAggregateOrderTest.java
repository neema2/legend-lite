// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlTyping;
import com.legend.testbackend.TestBackend;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A string aggregate ordered by a constant runs on every in-process database, its key dropped
 * ({@link ConstantAggregateOrder}: the IQ scale program's fuzz round 8, 2026-10-23, DuckDB's "ORDER BY non-integer
 * literal has no effect"); a percentile's constant key, its value, stays.
 */
class ConstantAggregateOrderTest {

    private static SqlSelect.SortKey key(SqlExpr e) {
        return new SqlSelect.SortKey(e, true, null, null);
    }

    private static SqlQuery select(SqlExpr e) {
        return SqlSelect.starOf(new SqlSource.Dual()).withProjections(List.of(new SqlSelect.Projection(e, "v", null)));
    }

    @ParameterizedTest(name = "{0}: a string aggregate ordered by a constant")
    @MethodSource("com.legend.testbackend.TestBackends#inProcess")
    void aConstantKeyOrdersNothing(TestBackend backend) throws Exception {
        SqlAgg.Reducer agg = new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG, List.of(new SqlExpr.StringLit("a"),
                new SqlExpr.StringLit(",")), false, List.of(key(new SqlExpr.StringLit(""))), SqlTyping.UNKNOWN);
        String sql = backend.dialect().render(select(agg));
        try (Connection conn = backend.open(); Statement st = conn.createStatement();
                ResultSet r = st.executeQuery(sql)) {
            assertTrue(r.next(), sql);
            assertEquals("a", r.getString(1), sql);
        }
        SqlAgg.Reducer percentile = new SqlAgg.Reducer(SqlAgg.Fn.QUANTILE_CONT, List.of(new SqlExpr.FloatLit(0.5)),
                false, List.of(key(new SqlExpr.IntLit(5))), SqlTyping.UNKNOWN);
        SqlQuery kept = new ConstantAggregateOrder().rewrite(select(percentile));
        assertEquals(select(percentile), kept, "a percentile's key is its value");
    }
}
