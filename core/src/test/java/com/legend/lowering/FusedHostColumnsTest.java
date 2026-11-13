// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * E-5c (the user's ruling O5, 2026-11-13): a fused verdict statement carries each member's HOST columns -- a
 * single-value side's own expression -- and every other member's as a NULL of that column's type, so the union's
 * columns align on every database and the host reads its sides off its verdict's row. Without host columns the
 * statement is exactly the database judge's ({@link VerdictSql#batch}).
 */
class FusedHostColumnsTest {

    /** A verdict row of the batch's schema: verdict {@code holds}, expected and actual {@code text}. */
    private static SqlQuery verdictRow(boolean holds, String text) {
        List<SqlSelect.Projection> ps = new ArrayList<>();
        ps.add(projection(new SqlExpr.BoolLit(holds), VerdictSql.VERDICT, SqlType.Scalar.BOOLEAN));
        ps.add(projection(new SqlExpr.StringLit(text), VerdictSql.EXPECTED, SqlType.Scalar.VARCHAR));
        ps.add(projection(new SqlExpr.StringLit(text), VerdictSql.ACTUAL, SqlType.Scalar.VARCHAR));
        ps.add(projection(new SqlExpr.Cast(new SqlExpr.NullLit(), SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE),
                VerdictSql.UNJUDGED, SqlType.Scalar.VARCHAR));
        ps.add(projection(new SqlExpr.BoolLit(false), VerdictSql.LENIENT, SqlType.Scalar.BOOLEAN));
        ps.add(projection(new SqlExpr.BoolLit(holds), VerdictSql.HOLDS, SqlType.Scalar.BOOLEAN));
        ps.add(projection(new SqlExpr.Cast(new SqlExpr.NullLit(), SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE),
                VerdictSql.MESSAGE, SqlType.Scalar.VARCHAR));
        List<OutputCol> outs = ps.stream().map(SqlSelect.Projection::out).toList();
        return new SqlSelect(ps, false, new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null,
                outs);
    }

    private static SqlSelect.Projection projection(SqlExpr e, String name, SqlType type) {
        return new SqlSelect.Projection(e, name, new OutputCol(name, type, true));
    }

    private static VerdictSql.HostColumn host(SqlExpr value, String name, SqlType type) {
        return new VerdictSql.HostColumn(value, new OutputCol(name, type, true));
    }

    private static SqlQuery twoMembers() {
        return VerdictSql.batchOf(List.of(
                new VerdictSql.Member(verdictRow(true, "a"), List.of(host(
                        SqlExpr.Call.of(com.legend.sql.SqlFn.PLUS, new SqlExpr.IntLit(40), new SqlExpr.IntLit(2)),
                        "__h0_0", SqlType.Scalar.BIGINT))),
                new VerdictSql.Member(verdictRow(false, "b"), List.of(host(new SqlExpr.StringLit("it's"),
                        "__h1_0", SqlType.Scalar.VARCHAR)))), Map.of());
    }

    /** Each member's row holds its own host column and a typed NULL in the other's, on every in-process database. */
    @Test
    void eachRowCarriesItsOwnHostColumnsAndATypedNullForTheOthers() throws Exception {
        SqlQuery fused = twoMembers();
        for (com.legend.testbackend.TestBackend backend : com.legend.testbackend.TestBackends.inProcess()) {
            String sql = com.legend.database.Databases.dialect(backend.type()).render(fused);
            Map<Long, List<Object>> byIndex = new java.util.HashMap<>();
            try (java.sql.Connection c = backend.open(); java.sql.Statement st = c.createStatement();
                    java.sql.ResultSet rs = st.executeQuery(sql)) {
                assertEquals(10, rs.getMetaData().getColumnCount(), backend.type() + ": " + sql);
                while (rs.next()) {
                    List<Object> row = new ArrayList<>();
                    row.add(rs.getObject(9));
                    row.add(rs.getObject(10));
                    byIndex.put(rs.getLong(1), row);
                }
            }
            assertEquals(2, byIndex.size(), backend.type() + ": " + sql);
            assertEquals(42L, ((Number) byIndex.get(0L).get(0)).longValue(), backend.type() + ": " + sql);
            assertEquals(null, byIndex.get(0L).get(1), backend.type() + ": " + sql);
            assertEquals(null, byIndex.get(1L).get(0), backend.type() + ": " + sql);
            assertEquals("it's", byIndex.get(1L).get(1), backend.type() + ": " + sql);
        }
    }

    /** No host columns: the database judge's statement, unchanged. */
    @Test
    void withNoHostColumnsTheStatementIsTheDatabaseJudges() {
        List<SqlQuery> rows = List.of(verdictRow(true, "a"), verdictRow(false, "b"));
        assertEquals(VerdictSql.batch(rows, Map.of()), VerdictSql.batchOf(List.of(
                new VerdictSql.Member(rows.get(0), List.of()), new VerdictSql.Member(rows.get(1), List.of())),
                Map.of()));
    }

    /** Two members naming one host column is refused by name, never a silent collision. */
    @Test
    void aHostColumnNamedTwiceIsRefused() {
        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> VerdictSql.batchOf(
                List.of(new VerdictSql.Member(verdictRow(true, "a"), List.of(host(new SqlExpr.IntLit(1), "__h",
                                SqlType.Scalar.BIGINT))),
                        new VerdictSql.Member(verdictRow(true, "b"), List.of(host(new SqlExpr.IntLit(2), "__h",
                                SqlType.Scalar.BIGINT)))), Map.of()));
        assertTrue(refused.getMessage().contains("'__h'"), refused.getMessage());
    }
}
