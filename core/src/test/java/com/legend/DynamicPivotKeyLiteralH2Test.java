// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.driver.DynamicPivot;

import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;

import com.legend.executionplan.ExecutionPlan;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.dialect.H2;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Tier-1 regression pin for audit finding D (2026-08-18): the
 * staticization pre-pass splices DISCOVERED key values back into the
 * plan as literals, so each literal must ROUND-TRIP — equal (as a SQL
 * value) to the very cell it was read from, or the pivot column
 * silently vanishes.
 *
 * <p>The old code widened a JDBC {@code Float} straight to
 * {@code FloatLit(double)}: {@code 3.14f} became
 * {@code 3.140000104904175}, which matches no row. And timestamps went
 * through {@code LocalDateTime.toString()}, which DROPS {@code :00}
 * seconds — a JDK convenience spelling a repr-ruled value. On H2, whose driver
 * reports a REAL as a {@code Float} (DynamicPivotKeyLiteralTest until the dialect
 * program's Phase 6, 2026-10-16).
 */
class DynamicPivotKeyLiteralH2Test {

    private static Connection conn;

    @BeforeAll
    static void open() throws Exception {
        conn = TestBackends.of(DatabaseType.H2).open();
    }

    @AfterAll
    static void close() throws Exception {
        conn.close();
    }

    private static List<SqlExpr> discovered(String ddl, String col)
            throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute(ddl);
        }
        SqlSource.Pivot pivot = new SqlSource.Pivot(
                new SqlSource.Table("T", "t", List.of(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE),
                List.of(SqlExpr.Column.physical("t", col)),
                List.of(),          // empty IN = dynamic — triggers discovery
                List.of(), List.of(), "p", List.of());
        // the keys read as typed rows through the session seam, as the body driver reads them (track B's B1a)
        H2 dialect = new H2();
        ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(DatabaseType.H2),
                new ExecutionPlan.Servers.Every(), List.of(), List.of());
        DynamicPivot.KeyReader reader = (query, columns) -> {
            try {
                return PlanRunner.rows(new ExecutionPlan(List.of(), new ExecutionPlan.TdsResult(
                        ExecutionPlan.TdsResult.Shape.VALUE, columns,
                        new ExecutionPlan.Sql(dialect.render(query), List.of(), target, query, List.of()))),
                        java.util.Map.of(), PlanSessions.of(com.legend.testbackend.TestSessions.over(conn))).rows();
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        };
        SqlQuery out = DynamicPivot.staticize(SqlSelect.starOf(pivot), dialect,
                PlanSessions.of(com.legend.testbackend.TestSessions.over(conn)), target, reader);
        try (Statement st = conn.createStatement()) {
            st.execute("DROP TABLE T");
        }
        return ((SqlSource.Pivot) ((SqlSelect) out).from()).in();
    }

    @Test
    @DisplayName("REAL keys round-trip via the float's shortest repr, not the bare double widen")
    void floatKeyRoundTrips() throws Exception {
        List<SqlExpr> in = discovered(
                "CREATE TABLE T(K REAL); INSERT INTO T VALUES (3.14)", "K");
        assertEquals(1, in.size());
        SqlExpr.FloatLit lit = assertInstanceOf(SqlExpr.FloatLit.class,
                in.get(0));
        // the old widen produced 3.140000104904175
        assertEquals(3.14d, lit.value(),
                "a REAL key must splice as its printed value — the bare"
                + " (double) widen can never match the source row");
    }

    @Test
    @DisplayName("timestamp keys keep :00 seconds — no LocalDateTime.toString spelling")
    void timestampKeyKeepsSeconds() throws Exception {
        List<SqlExpr> in = discovered(
                "CREATE TABLE T(K TIMESTAMP); INSERT INTO T VALUES"
                + " (TIMESTAMP '2020-01-01 10:00:00')", "K");
        assertEquals(1, in.size());
        SqlExpr.TimestampLit lit = assertInstanceOf(
                SqlExpr.TimestampLit.class, in.get(0));
        // LocalDateTime.toString() spells this "2020-01-01T10:00" —
        // seconds gone
        assertEquals("2020-01-01 10:00:00", lit.iso());
    }

    @Test
    @DisplayName("subsecond keys carry their fraction exactly as stored")
    void subsecondKeyKeepsFraction() throws Exception {
        List<SqlExpr> in = discovered(
                "CREATE TABLE T(K TIMESTAMP(9)); INSERT INTO T VALUES"
                + " (TIMESTAMP '2020-01-01 10:00:00.123')", "K");
        SqlExpr.TimestampLit lit = assertInstanceOf(
                SqlExpr.TimestampLit.class, in.get(0));
        assertEquals("2020-01-01 10:00:00.123", lit.iso());
    }
}
