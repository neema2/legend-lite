// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;


import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.dialect.SqlDialect;

import java.sql.Connection;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A wire-decided column (leg 3.3, §4t): the store says INT, the fixture created VARCHAR. Since the planner-driver seam
 * program's S3b (2026-10-30) no prepare at planning reconciles the slot: the grid canon asks the value in the
 * statement ({@code CanonicalRenderSql.textOrStamp}), so a String-declared cell stamped INTEGER spells as the text it
 * holds when the database stores text, and as the integer otherwise. (The raw grid's frame by a describe, the
 * staticizer, was deleted by S3c: a raw grid is a relation of the columns its result reports.)
 */
class WireProbeTest {

    /** {@code SELECT table.id AS id FROM table} with the column STAMPED {@code stamp}. */
    private static SqlSelect plan(SqlType stamp, String table) {
        OutputCol id = new OutputCol("id", stamp, true);
        SqlSource.Table t = new SqlSource.Table(table, table, List.of(id), false, java.util.Map.of(),
                com.legend.sql.NameQuoting.BARE);
        return new SqlSelect(List.of(new SqlSelect.Projection(
                        SqlExpr.Column.of(table, "id", stamp, true, OutputCol.Origin.DERIVED), "id", id)),
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
        conn.createStatement().execute("insert into t values ('a''b')");
        conn.createStatement().execute("insert into t values (null)");
        conn.createStatement().execute("create table n(\"id\" integer)");
        conn.createStatement().execute("insert into n values (7)");
        // a String-declared column stamped INTEGER (the store's declaration): the cell is the database's kind
        assertEquals(java.util.Set.of("'7'", "'a\\'b'", "TDSNull"), java.util.Set.copyOf(cells(conn, dialect, "t")),
                "the text it holds, quoted");
        assertEquals(List.of("7"), cells(conn, dialect, "n"), "the integer, bare");
    }

    /** The grid canon's cells of {@code SELECT id FROM table}, its column declared String and stamped INTEGER. */
    private static List<String> cells(Connection conn, SqlDialect dialect, String table) throws Exception {
        var w = com.legend.lowering.CanonicalRenderSql.wrapTdsCanon(plan(SqlType.Scalar.INTEGER, table),
                (Type.RelationType) grid(Type.Primitive.STRING).type());
        assertEquals(null, w.declineReason());
        List<String> out = new java.util.ArrayList<>();
        try (var s = conn.createStatement(); var r = s.executeQuery(dialect.render(w.plan()))) {
            while (r.next()) {
                out.add(r.getString(com.legend.lowering.CanonicalRenderSql.CELL_CANON + "0"));
            }
        }
        return out;
    }

    /** On each in-process database (the dialect program's Phase 6, 2026-10-16: until then DuckDB and H2, by name). */
    @ParameterizedTest(name = "{0}")
    @MethodSource("com.legend.testbackend.TestBackends#inProcess")
    void eachDatabaseTellsItsKind(com.legend.testbackend.TestBackend backend) throws Exception {
        try (Connection c = backend.open()) {
            battery(c, backend.type(), backend.dialect());
        }
    }
}
