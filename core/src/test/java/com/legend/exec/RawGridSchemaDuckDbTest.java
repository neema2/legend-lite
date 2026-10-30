// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.testing.Lean;
import com.legend.testing.Program;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE BOUNDARY RESOLVER's pins (Phase 1c grid endgame): late-bound
 * reads resolve against the FIRST-query stamped schema (the
 * dynamic-pivot model), and everything then rides the ordinary
 * pipeline — the database does the work.
 */
class RawGridSchemaDuckDbTest {

    /** The database each test's session runs on. */
    private static final DatabaseType SESSION = DatabaseType.DuckDB;

    private static final String CONN_LET =
            "{| let c = ^meta::external::store::relational::runtime::TestDatabaseConnection("
                    + "type=meta::relational::runtime::DatabaseType.DuckDB);\n";

    private static final String EXEC =
            "meta::relational::metamodel::execute::executeInDb(";

    private static Connection conn;

    @BeforeAll
    static void open() throws Exception {
        conn = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterAll
    static void close() throws Exception {
        conn.close();
    }

    /** A connection a helper builds, the way the corpus's {@code testRuntime()} does: the zero-parameter helper names
     *  an overload of one parameter, and every overload of that arity declares the same database. */
    private static final String HELPERS = """
            function test::conn():meta::external::store::relational::runtime::DatabaseConnection[1]
            { test::conn('x') }
            function test::conn(s:String[1]):meta::external::store::relational::runtime::DatabaseConnection[1]
            { ^meta::external::store::relational::runtime::TestDatabaseConnection(
                    type=meta::relational::runtime::DatabaseType.DuckDB) }
            function test::conn(i:Integer[1]):meta::external::store::relational::runtime::DatabaseConnection[1]
            { ^meta::external::store::relational::runtime::TestDatabaseConnection(
                    type=meta::relational::runtime::DatabaseType.%s) }
            """;

    @Test
    @DisplayName("a connection a helper builds declares its database through the helper's body (decision D2)")
    void helperConnectionDeclaresItsDatabase() throws Exception {
        Lean.Result r = Program.last(StorelessRuntime.with(HELPERS.formatted("DuckDB"), SESSION),
                "{| " + EXEC + "'select 1 as A union all select 2', test::conn(), 0, 1000).rows.value('A')->size();}",
                StorelessRuntime.RUNTIME, conn);
        assertEquals(2L, ((Number) r.one()).longValue());
    }

    @Test
    @DisplayName("overloads that declare different databases leave the text's language unknown: refused (decision D2)")
    void disagreeingOverloadsAreRefused() {
        // the raw body names test::conn, not which overload: H2 and DuckDB both answer, so neither is guessed
        Exception e = assertThrows(Exception.class, () -> Program.last(
                StorelessRuntime.with(HELPERS.formatted("H2"), SESSION),
                "{| " + EXEC + "'select 1 as A', test::conn(), 0, 1000).rows.value('A')->size();}",
                StorelessRuntime.RUNTIME, conn));
        assertTrue(String.valueOf(e.getMessage()).contains("decision D2"), e.toString());
    }

    private static final String T = "meta::external::store::relational::runtime::TestDatabaseConnection";

    private static final String DUCK = "meta::relational::runtime::DatabaseType.DuckDB";

    /** A database type that is not a literal: a function answering one, and a helper whose parameter is one. */
    private static final String TYPES = """
            function my::duck():meta::relational::runtime::DatabaseType[1] { meta::relational::runtime::DatabaseType.DuckDB }
            function my::conn(t:meta::relational::runtime::DatabaseType[1]):meta::external::store::relational::runtime::DatabaseConnection[1]
            { ^meta::external::store::relational::runtime::TestDatabaseConnection(type=$t) }
            """;

    /** The four ways a connection declares DuckDB: lets, then the connection argument. */
    private static final List<String[]> DUCK_CONNECTIONS = List.of(
            new String[] {"let c = ^" + T + "(type=" + DUCK + ");\n", "$c"},
            new String[] {"let t = " + DUCK + "; let c = ^" + T + "(type=$t);\n", "$c"},
            new String[] {"let c = ^" + T + "(type=my::duck());\n", "$c"},
            new String[] {"", "my::conn(" + DUCK + ")"});

    /** The name of the one column of {@code select count(*)} as the session ran it: DuckDB's {@code count_star()} for
     *  the text as written, {@code COUNT(*)} for the text translated as the engine's H2. */
    private static String countColumn(String lets, String connection) throws Exception {
        Lean.Result r = Program.last(StorelessRuntime.with(TYPES, SESSION), "{| " + lets + EXEC
                + "'select count(*) from (select 1 as x)', " + connection + ", 0, 1000).columnNames->at(0);}",
                StorelessRuntime.RUNTIME, conn);
        return String.valueOf(r.one());
    }

    @Test
    @DisplayName("a connection's type read through a let, a function or a helper's parameter is its database (decision D2)")
    void aTypeThatIsNotALiteralIsItsDatabase() throws Exception {
        // the PR #39 audit (2026-10-14): every type but a literal was read as H2's, and the text translated
        for (String[] c : DUCK_CONNECTIONS) {
            assertEquals("count_star()", countColumn(c[0], c[1]), c[0] + c[1]);
        }
        assertEquals("COUNT(*)", countColumn("let c = ^" + T + "(type=meta::relational::runtime::DatabaseType.H2);\n",
                "$c"));
    }

    @Test
    @DisplayName("the statement form reads the connection's database as the grid form does (decision D2)")
    void theStatementFormReadsTheSameDatabase() throws Exception {
        // DuckDB keeps a FLOAT as written; the engine's H2 text has its FLOAT translated to a DOUBLE
        for (int i = 0; i < DUCK_CONNECTIONS.size(); i++) {
            String[] c = DUCK_CONNECTIONS.get(i);
            Program.last(StorelessRuntime.with(TYPES, SESSION), "{| " + c[0] + EXEC + "'Create Table d2_float_" + i
                    + "(f FLOAT);', " + c[1] + ", 0, 1000);}", StorelessRuntime.RUNTIME, conn);
            try (var st = conn.createStatement(); var rs = st.executeQuery("select data_type from"
                    + " information_schema.columns where table_name = 'd2_float_" + i + "'")) {
                assertTrue(rs.next(), c[0] + c[1]);
                assertEquals("FLOAT", rs.getString(1), c[0] + c[1]);
            }
        }
    }

    @Test
    @DisplayName("a connection whose type cannot be read statically is refused, never read as H2's (decision D2)")
    void anUnreadableTypeIsRefused() {
        Exception e = assertThrows(Exception.class, () -> countColumn("let c = ^" + T + "(type=if(true, |" + DUCK
                + ", |meta::relational::runtime::DatabaseType.H2));\n", "$c"));
        assertTrue(String.valueOf(e.getMessage()).contains("decision D2"), e.toString());
    }

    @Test
    @DisplayName("columnNames resolves to the string collection and composes")
    void columnNamesComposes() throws Exception {
        Lean.Result r = Program.last(StorelessRuntime.with("", SESSION), CONN_LET
                + EXEC + "'select 1 as A, 2 as B', $c, 0, 1000)"
                + ".columnNames->at(1);}", StorelessRuntime.RUNTIME, conn);
        assertEquals("B", r.one());
    }

    @Test
    @DisplayName(".rows.value('N') AUTO-MAPS over many rows (pure's dot rule)")
    void rowsValueAutoMaps() throws Exception {
        Lean.Result r = Program.last(StorelessRuntime.with("", SESSION), CONN_LET
                + EXEC + "'select 1 as A union all select 2', $c, 0, 1000)"
                + ".rows.value('A')->size();}", StorelessRuntime.RUNTIME, conn);
        assertEquals(2L, ((Number) r.one())
                .longValue());
    }

    @Test
    @DisplayName("row-major .rows.values->at(k) resolves via the cells map")
    void rowMajorCellRead() throws Exception {
        Lean.Result r = Program.last(StorelessRuntime.with("", SESSION), CONN_LET
                + "let rs = " + EXEC + "'select 1 as A, \\'x\\' as B', $c);\n"
                + "$rs.rows.values->at(1);}", StorelessRuntime.RUNTIME, conn);
        assertEquals("x", r.one());
    }

    @Test
    @DisplayName("fold column-collect lowers as the per-row map")
    void foldColumnCollect() throws Exception {
        Lean.Result r = Program.last(StorelessRuntime.with("", SESSION), CONN_LET
                + EXEC + "'select 1 as A, \\'x\\' as B union all"
                + " select 2, \\'y\\'', $c, 0, 1000)"
                + ".rows->fold({a,b| concatenate($a.values->at(1), $b)}, [])"
                + "->size();}", StorelessRuntime.RUNTIME, conn);
        assertEquals(2L, ((Number) r.one())
                .longValue());
    }

    @Test
    @DisplayName("audit T1.1: a SHADOWING inner binder never sees the outer grid schema")
    void shadowedBinderIsNotResolved() throws Exception {
        // outer map binds `r` to the GRID row; the inner lambda over a
        // PLAIN collection re-binds `r` — its $r must stay the inner
        // value (2, 3), never the outer row's cells. A wrong resolver
        // would substitute the grid cells and change the sum.
        Lean.Result r = Program.last(StorelessRuntime.with("", SESSION), CONN_LET
                + EXEC + "'select 10 as A', $c, 0, 1000)"
                + ".rows->map(r | [2, 3]->map(r | $r)->sum());}", StorelessRuntime.RUNTIME, conn);
        // one grid row -> one mapped value: 2 + 3 = 5
        assertEquals(List.of(5L),
                r.values().stream()
                        .map(v -> ((Number) v).longValue()).toList());
    }
}
