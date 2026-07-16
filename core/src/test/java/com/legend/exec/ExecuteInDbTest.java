// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.Compiler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The K-native {@code executeInDb} dispatch ({@code Compiler}): raw SQL
 * executes over the AMBIENT connection — the connection ARGUMENT (the
 * corpus's {@code testRuntime()->connectionByElement(...)} chain) exists to
 * type-check and is never evaluated. The SQL argument is an ordinary Pure
 * expression evaluated through the pipeline; the resulting blob is
 * dialect-adapted (keyword column quoting, {@code CURRENT_TIMESTAMP()}) and
 * split on top-level {@code ;}.
 */
class ExecuteInDbTest {

    private static final String CONN_LET =
            "{| let c = meta::core::runtime::connectionByElement("
                    + "meta::external::store::relational::tests::testRuntime(), 1)"
                    + "->cast(@meta::external::store::relational::runtime::DatabaseConnection);\n";

    private static Connection conn;

    @BeforeAll
    static void open() throws Exception {
        conn = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterAll
    static void close() throws Exception {
        conn.close();
    }

    @Test
    @DisplayName("executeInDb: multi-statement blob with H2-flavored keyword columns executes ambiently")
    void multiStatementBlobWithKeywordColumns() throws Exception {
        // "default" is a keyword column name — legal unquoted on the
        // engine's H2, a syntax error on DuckDB without the dialect's
        // raw-SQL adaptation; the blob carries two statements
        ExecutionResult r = Compiler.execute("", CONN_LET
                + "meta::relational::metamodel::execute::executeInDb("
                + "'Create Table kTest(id INT, default VARCHAR(20));"
                + " Insert into kTest (id, default) values (7, \\'x\\');', $c, 0, 1000);}",
                conn);
        assertNull(((ExecutionResult.Scalar) r).value(), "opaque ResultSet handle");
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("select id, \"default\" from kTest")) {
            assertTrue(rs.next());
            assertEquals(7, rs.getInt(1));
            assertEquals("x", rs.getString(2));
        }
    }

    @Test
    @DisplayName("executeInDb: the sql argument is a Pure EXPRESSION, evaluated through the pipeline")
    void sqlArgumentEvaluatedThroughPipeline() throws Exception {
        Compiler.execute("", CONN_LET
                + "let tbl = 'kExpr';\n"
                + "meta::relational::metamodel::execute::executeInDb("
                + "'Create Table ' + $tbl + '(id INT); Insert into ' + $tbl"
                + " + ' (id) values (41 + 1);', $c, 0, 1000);}", conn);
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("select id from kExpr")) {
            assertTrue(rs.next());
            // "41 + 1" rides INSIDE the sql string: the database folds it
            assertEquals(42, rs.getInt(1));
        }
    }

    /** The corpus shape: a user WRAPPER over the native leaf, effectful
     * setup functions calling it statement after statement, nested. */
    private static final String SETUP_MODEL = """
            function my::w::executeInDb(sql: String[1],
                    conn: meta::external::store::relational::runtime::DatabaseConnection[1]): \
            meta::relational::metamodel::execute::ResultSet[1]
            {
               meta::relational::metamodel::execute::executeInDb($sql, $conn, 0, 1000);
            }
            function my::s::fill(tableName: String[1]): Boolean[1]
            {
               let c = meta::core::runtime::connectionByElement(
                  meta::external::store::relational::tests::testRuntime(), 1)
                  ->cast(@meta::external::store::relational::runtime::DatabaseConnection);
               my::w::executeInDb('Insert into ' + $tableName + ' (id) values (1);', $c);
               my::w::executeInDb('Insert into ' + $tableName + ' (id) values (2);', $c);
               true;
            }
            function my::s::createAndFill(): Boolean[1]
            {
               let c = meta::core::runtime::connectionByElement(
                  meta::external::store::relational::tests::testRuntime(), 1)
                  ->cast(@meta::external::store::relational::runtime::DatabaseConnection);
               my::w::executeInDb('Create Table kSeq(id INT);', $c);
               my::s::fill('kSeq');
               true;
            }
            """;

    @Test
    @DisplayName("setup functions: effectful statement SEQUENCES execute through call frames")
    void effectfulSetupFunctionSequences() throws Exception {
        // one statement-position call fans out: create + nested fill (a
        // parameterized callee — the arg travels through the frame)
        ExecutionResult r = Compiler.execute(SETUP_MODEL,
                "{| my::s::createAndFill();}", conn);
        assertEquals(true, ((ExecutionResult.Scalar) r).value(),
                "the sequence's value is its LAST statement");
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("select count(*), sum(id) from kSeq")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
        }
    }

    @Test
    @DisplayName("an effectful LET binding refuses loudly (β-substitution would drop the effect)")
    void effectfulLetIsLoud() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> Compiler.execute(SETUP_MODEL, CONN_LET
                        + "let rs = meta::relational::metamodel::execute::executeInDb("
                        + "'Create Table kDrop(id INT);', $c, 0, 1000);\ntrue;}", conn));
        assertTrue(ex.getMessage().contains("executeInDb"), ex.getMessage());
    }

    @Test
    @DisplayName("executeInDb: a broken statement fails loudly, never silently")
    void brokenStatementFailsLoudly() {
        assertThrows(java.sql.SQLException.class, () -> Compiler.execute("", CONN_LET
                + "meta::relational::metamodel::execute::executeInDb("
                + "'Insert into noSuchTable (id) values (1);', $c, 0, 1000);}", conn));
    }
}
