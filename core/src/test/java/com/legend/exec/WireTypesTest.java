// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.dialect.SqlDialect;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Leg 3.3: a wire-decided column takes the database's reported type
 * (§4t) — the store says INT, the fixture created VARCHAR. */
class WireTypesTest {

    /** {@code SELECT t.id AS id FROM t} with the column STAMPED {@code stamp}. */
    private static SqlSelect plan(SqlType stamp) {
        OutputCol id = new OutputCol("id", stamp, true);
        SqlSource.Table t = new SqlSource.Table("t", "t", List.of(id), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE);
        return new SqlSelect(List.of(new SqlSelect.Projection(
                        SqlExpr.Column.of("t", "id", stamp, true, OutputCol.Origin.DERIVED), "id", id)),
                false, t, null, List.of(), null, null, List.of(), null, null, List.of(id));
    }

    private static ExprType grid(Type declared) {
        return new ExprType(new Type.RelationType(List.of(
                new Type.Column("id", declared, Multiplicity.Bounded.ONE)), List.of()),
                Multiplicity.Bounded.ONE);
    }

    private static void battery(Connection conn, com.legend.model.ConnectionDefinition.DatabaseType type,
            SqlDialect dialect) throws Exception {
        conn.createStatement().execute("create table t(\"id\" varchar(200))");
        conn.createStatement().execute("insert into t values ('7')");
        Map<String, List<WireTypes.ReportedColumn>> memo = new HashMap<>();
        // the plan runner the reader goes through (the doors, 2026-10-29; the session seam since track B's B1a)
        PlanSessions.Source c = PlanSessions.given(conn);
        com.legend.executionplan.ExecutionPlan.Target target = new com.legend.executionplan.ExecutionPlan.Target(
                new com.legend.executionplan.ExecutionPlan.Database.Platform(type),
                new com.legend.executionplan.ExecutionPlan.Servers.Every(), List.of(), List.of());
        // a String-declared column stamped INTEGER (the store's declaration)
        // over a physical VARCHAR: reconciled to the wire
        SqlQuery r = WireTypes.reconcile(plan(SqlType.Scalar.INTEGER), grid(Type.Primitive.STRING),
                dialect, c, target, memo);
        assertEquals(SqlType.Scalar.VARCHAR, r.outputs().get(0).type());
        assertTrue(((SqlSelect) r).projections().get(0).expr() instanceof SqlExpr.Column ref
                && ref.type() instanceof com.legend.sql.TypeFact.Typed tf
                && tf.type() == SqlType.Scalar.VARCHAR, "the reference re-typed, never cast");
        assertEquals(1, memo.size(), "one prepare, memoized by statement text");
        // the same statement again: no second prepare
        WireTypes.reconcile(plan(SqlType.Scalar.INTEGER), grid(Type.Primitive.STRING), dialect, c, target, memo);
        assertEquals(1, memo.size());
        // a numeric declaration converts the wire cell — never reconciled
        SqlSelect p = plan(SqlType.Scalar.INTEGER);
        assertSame(p, WireTypes.reconcile(p, grid(Type.Primitive.INTEGER), dialect, c, target, memo));
        // a stamp the database agrees with: untouched
        SqlSelect q = plan(SqlType.Scalar.VARCHAR);
        assertSame(q, WireTypes.reconcile(q, grid(Type.Primitive.STRING), dialect, c, target, memo));
        // a COMPUTED projection keeps the compiler's type (never cast: H2 reports a
        // computed DECIMAL at scale 0 while the cell carries the real scale)
        OutputCol n = new OutputCol("n", SqlType.Scalar.DOUBLE, true);
        SqlSelect computed = new SqlSelect(List.of(new SqlSelect.Projection(
                        new SqlExpr.Cast(SqlExpr.Column.of("t", "id", SqlType.Scalar.VARCHAR, true,
                                OutputCol.Origin.DERIVED), SqlType.Scalar.DOUBLE), "n", n)),
                false, new SqlSource.Table("t", "t", List.of(n), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE), null, List.of(), null, null,
                List.of(), null, null, List.of(n));
        assertSame(computed, WireTypes.reconcile(computed, grid(Type.Primitive.NUMBER), dialect, c, target, memo));
        // a plan the compiler could not type, framed by the database's columns
        SqlQuery framed = WireTypes.staticized(plan(SqlType.Scalar.INTEGER), dialect, c, target, memo);
        assertEquals("id", framed.outputs().get(0).name());
        assertEquals(SqlType.Scalar.VARCHAR, framed.outputs().get(0).type());
        // the one-column value shape (no relation view) reconciles by the root kind
        SqlQuery v = WireTypes.reconcile(plan(SqlType.Scalar.INTEGER),
                new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ONE), dialect, c, target, memo);
        assertEquals(SqlType.Scalar.VARCHAR, v.outputs().get(0).type());
    }

    /** On each in-process database (the dialect program's Phase 6, 2026-10-16: until then DuckDB and H2, by name). */
    @ParameterizedTest(name = "{0}")
    @MethodSource("com.legend.testbackend.TestBackends#inProcess")
    void eachDatabaseReportsTheWire(com.legend.testbackend.TestBackend backend) throws Exception {
        try (Connection c = backend.open()) {
            battery(c, backend.type(), backend.dialect());
        }
    }

    @Test
    void jdbcVocabulary() {
        assertEquals(SqlType.Scalar.VARCHAR, WireTypes.sqlTypeOf(java.sql.Types.VARCHAR, 200, 0));
        assertEquals(new SqlType.Decimal(18, 3), WireTypes.sqlTypeOf(java.sql.Types.DECIMAL, 18, 3));
        assertEquals(SqlType.Scalar.INTEGER, WireTypes.sqlTypeOf(java.sql.Types.SMALLINT, 0, 0));
        assertEquals(null, WireTypes.sqlTypeOf(java.sql.Types.ARRAY, 0, 0));
    }
}
