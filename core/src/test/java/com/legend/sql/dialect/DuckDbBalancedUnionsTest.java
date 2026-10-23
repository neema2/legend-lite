// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlUnion;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A union of thousands of branches runs on DuckDB, its rows the same ({@link BalancedUnions}: the IQ scale program's
 * Q10, 2026-10-23; a UNION ALL of 2,000 branches was past DuckDB's depth of 1,000).
 */
class DuckDbBalancedUnionsTest {

    private static SqlQuery branch(long v) {
        return SqlSelect.starOf(new SqlSource.Dual()).withProjections(List.of(
                new SqlSelect.Projection(new SqlExpr.IntLit(v), "x", null)));
    }

    private static List<Long> rows(Connection conn, String sql) throws Exception {
        List<Long> out = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet r = st.executeQuery(sql)) {
            while (r.next()) {
                out.add(r.getLong(1));
            }
        }
        return out;
    }

    @Test
    void aLongUnionKeepsItsRows() throws Exception {
        TestBackend backend = TestBackends.platform();
        int n = 5_000;
        List<SqlQuery> all = new ArrayList<>();
        List<SqlQuery> repeating = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            all.add(branch(i));
            repeating.add(branch(i % 100));
        }
        String unionAll = backend.dialect().render(SqlUnion.ofBranches(all, true));
        String union = backend.dialect().render(SqlUnion.ofBranches(repeating, false));
        assertTrue(unionAll.contains(" AS u1") || unionAll.contains(" AS \"u1\""), unionAll.substring(0, 400));
        try (Connection conn = backend.open()) {
            List<Long> kept = rows(conn, unionAll);
            assertEquals(n, kept.size(), backend + ": UNION ALL's rows");
            assertEquals((long) n * (n - 1) / 2, kept.stream().mapToLong(Long::longValue).sum(), backend + ": its sum");
            List<Long> deduplicated = rows(conn, union);
            Set<Long> distinct = new HashSet<>(deduplicated);
            assertEquals(100, deduplicated.size(), backend + ": UNION's rows");
            assertEquals(100, distinct.size(), backend + ": each once");
        }
        // the first branch's column names the union's, grouped or not
        assertTrue(SqlUnion.ofBranches(all, true).outputs().equals(new BalancedUnions().rewrite(
                SqlUnion.ofBranches(all, true)).outputs()));
    }
}
