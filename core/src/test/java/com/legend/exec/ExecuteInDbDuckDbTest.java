// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.testing.Lean;
import com.legend.testing.Program;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;

import com.legend.Compiler;
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
 * corpus's {@code testRuntime()->connectionByElement(...)} chain) is never
 * evaluated; it is read statically for the database it declares, the
 * language of the SQL (decision D2). The SQL argument is an ordinary Pure
 * expression evaluated through the pipeline; the resulting blob is split on
 * top-level {@code ;}, and the engine's H2 text is translated for the session
 * (keyword column quoting, {@code CURRENT_TIMESTAMP()}), any other run as
 * written.
 */
class ExecuteInDbDuckDbTest {

    /** The database each test's session runs on. */
    private static final DatabaseType SESSION = DatabaseType.DuckDB;

    /** A connection declaring the engine's H2: its SQL is H2's, translated for the DuckDB session (decision D2). */
    private static final String CONN_LET =
            "{| let c = ^meta::external::store::relational::runtime::TestDatabaseConnection("
                    + "type=meta::relational::runtime::DatabaseType.H2);\n";

    /** A connection declaring DuckDB, the session's own database: its SQL runs as written. */
    private static final String OWN_CONN_LET =
            "{| let c = ^meta::external::store::relational::runtime::TestDatabaseConnection("
                    + "type=meta::relational::runtime::DatabaseType.DuckDB);\n";

    // one connection per method: nothing a test writes reaches the next (Bazel workplan P3-04)
    private Connection conn;

    @org.junit.jupiter.api.BeforeEach
    void open() throws Exception {
        conn = DriverManager.getConnection("jdbc:duckdb:");
    }

    @org.junit.jupiter.api.AfterEach
    void close() throws Exception {
        conn.close();
    }

    @Test
    @DisplayName("executeInDb: multi-statement blob with H2-flavored keyword columns executes ambiently")
    void multiStatementBlobWithKeywordColumns() throws Exception {
        // "default" is a keyword column name — legal unquoted on the
        // engine's H2, a syntax error on DuckDB without the dialect's
        // raw-SQL adaptation; the blob carries two statements
        Lean.Result r = Program.last(StorelessRuntime.with("", SESSION), CONN_LET
                + "meta::relational::metamodel::execute::executeInDb("
                + "'Create Table kTest(id INT, default VARCHAR(20));"
                + " Insert into kTest (id, default) values (7, \\'x\\');', $c, 0, 1000);}", StorelessRuntime.RUNTIME,
                conn);
        assertNull(r.value(), "opaque ResultSet handle");
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("select id, \"default\" from kTest")) {
            assertTrue(rs.next());
            assertEquals(7, rs.getInt(1));
            assertEquals("x", rs.getString(2));
        }
    }

    @Test
    @DisplayName("executeInDb: text written for the session's own database runs as written (decision D2)")
    void ownDatabaseTextRunsAsWritten() throws Exception {
        // H2's FLOAT is an 8-byte double, which the engine's H2 text maps to DOUBLE; on a connection declaring
        // DuckDB the text is DuckDB's, whose FLOAT (a 4-byte REAL) stays
        Program.last(StorelessRuntime.with("", SESSION), OWN_CONN_LET
                + "meta::relational::metamodel::execute::executeInDb('Create Table fOwn(f FLOAT);', $c, 0, 1000);}",
                StorelessRuntime.RUNTIME, conn);
        Program.last(StorelessRuntime.with("", SESSION), CONN_LET
                + "meta::relational::metamodel::execute::executeInDb('Create Table fH2(f FLOAT);', $c, 0, 1000);}",
                StorelessRuntime.RUNTIME, conn);
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("select table_name, data_type from information_schema.columns"
                        + " where table_name in ('fOwn', 'fH2') order by table_name")) {
            assertTrue(rs.next());
            assertEquals("fH2", rs.getString(1));
            assertEquals("DOUBLE", rs.getString(2), "the engine's H2 FLOAT, translated");
            assertTrue(rs.next());
            assertEquals("fOwn", rs.getString(1));
            assertEquals("FLOAT", rs.getString(2), "DuckDB's own FLOAT, as written");
        }
    }

    @Test
    @DisplayName("executeInDb: the sql argument is a Pure EXPRESSION, evaluated through the pipeline")
    void sqlArgumentEvaluatedThroughPipeline() throws Exception {
        Program.last(StorelessRuntime.with("", SESSION), CONN_LET
                + "let tbl = 'kExpr';\n"
                + "meta::relational::metamodel::execute::executeInDb("
                + "'Create Table ' + $tbl + '(id INT); Insert into ' + $tbl"
                + " + ' (id) values (41 + 1);', $c, 0, 1000);}", StorelessRuntime.RUNTIME, conn);
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
               let c = ^meta::external::store::relational::runtime::TestDatabaseConnection(
                  type=meta::relational::runtime::DatabaseType.DuckDB);
               my::w::executeInDb('Insert into ' + $tableName + ' (id) values (1);', $c);
               my::w::executeInDb('Insert into ' + $tableName + ' (id) values (2);', $c);
               true;
            }
            function my::s::createAndFill(): Boolean[1]
            {
               let c = ^meta::external::store::relational::runtime::TestDatabaseConnection(
                  type=meta::relational::runtime::DatabaseType.DuckDB);
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
        Lean.Result r = Program.last(StorelessRuntime.with(SETUP_MODEL, SESSION),
                "{| my::s::createAndFill();}", StorelessRuntime.RUNTIME, conn);
        assertEquals(true, r.one(),
                "the sequence's value is its LAST statement");
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("select count(*), sum(id) from kSeq")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
        }
    }

    @Test
    @DisplayName("let x = executeInDb(...): the effect runs exactly ONCE at the let (engine parity)")
    void effectfulLetExecutesOnce() throws Exception {
        // corpus shape (embedded createTimeStamKeysTableAndFill): the let
        // binds an opaque ResultSet handle as a smoke check, never read
        Lean.Result r = Program.last(StorelessRuntime.with(SETUP_MODEL, SESSION), CONN_LET
                + "let rs = meta::relational::metamodel::execute::executeInDb("
                + "'Create Table kDrop(id INT);', $c, 0, 1000);\ntrue;}", StorelessRuntime.RUNTIME, conn);
        assertEquals(true, r.one());
        try (Statement st = conn.createStatement()) {
            st.execute("insert into kDrop values (1)");   // table exists = ran
        }
    }

    @Test
    @DisplayName("READING a STATEMENT-shaped executeInDb binding refuses loudly (no host-side ResultSet)")
    void effectfulLetReadIsLoud() {
        // Phase 1c re-pin: the refusal is the EFFECT shape's (DDL/blob —
        // an opaque execute-once handle). A query shape is a VALUE now
        // (the typed-relation channel) — pinned green below.
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> Program.last(StorelessRuntime.with(SETUP_MODEL, SESSION), CONN_LET
                        + "let rs = meta::relational::metamodel::execute::executeInDb("
                        + "'create table EFFECT_LET_T(x int);', $c, 0, 1000);\n"
                        + "let n = $rs;\ntrue;}", StorelessRuntime.RUNTIME, conn));
        assertTrue(String.valueOf(ex.getMessage()).contains("executeInDb result binding"),
                ex.getMessage());
    }

    @Test
    @DisplayName("Phase 1c FLIP: a QUERY-shaped executeInDb binding is a VALUE — it reads back")
    void queryShapedLetReadsBack() throws Exception {
        Lean.Result r = Program.last(StorelessRuntime.with(SETUP_MODEL, SESSION), CONN_LET
                + "let rs = meta::relational::metamodel::execute::executeInDb("
                + "'select 1 as A;', $c, 0, 1000);\n"
                + "$rs.rows->size();}", StorelessRuntime.RUNTIME, conn);
        assertEquals(1L, ((Number) r.one())
                .longValue());
    }

    @Test
    @DisplayName("dropAndCreateTableInDb: DDL renders from the compiled store model")
    void dropAndCreateFromStoreModel() throws Exception {
        String model = """
                ###Relational
                Database my::store::DB
                (
                   Table kDdlTable(id INTEGER PRIMARY KEY, name VARCHAR(20))
                )
                """;
        String call = CONN_LET
                + "meta::relational::functions::toDDL::dropAndCreateTableInDb("
                + "my::store::DB, 'kDdlTable', $c);}";
        Lean.Result r = Program.last(StorelessRuntime.with(model, SESSION), call, StorelessRuntime.RUNTIME, conn);
        assertEquals(true, r.one());
        try (Statement st = conn.createStatement()) {
            st.execute("Insert into kDdlTable (id, name) values (1, 'a')");
        }
        // drop+create again: the table comes back EMPTY (no constraints —
        // engine-harness parity: milestoned seeds repeat ids)
        Program.last(StorelessRuntime.with(model, SESSION), call, StorelessRuntime.RUNTIME, conn);
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("select count(*) from kDdlTable")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test
    @DisplayName("executeInDb: a broken statement fails loudly, never silently")
    void brokenStatementFailsLoudly() {
        assertThrows(com.legend.error.DataError.class, () -> Program.last(StorelessRuntime.with("", SESSION), CONN_LET
                + "meta::relational::metamodel::execute::executeInDb("
                + "'Insert into noSuchTable (id) values (1);', $c, 0, 1000);}", StorelessRuntime.RUNTIME, conn));
    }

    @Test
    @DisplayName("T1.9: class-query-derived effect sql — Phase H runs; the scalar-root arm is the LOUD wall")
    void classQueryDerivedSqlResolves() throws Exception {
        String model = """
                Class t9::P { name: String[1]; }
                ###Relational
                Database t9::DB ( Table PT (NAME VARCHAR(20)) )
                ###Mapping
                Mapping t9::M (
                  *t9::P: Relational { ~mainTable [t9::DB] PT name: PT.NAME }
                )
                ###Connection
                RelationalDatabaseConnection t9::DBDuckDB { store: t9::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
                ###Runtime
                Runtime t9::RT { mappings: [t9::M]; connections: [ t9::DB: [ c0: t9::DBDuckDB ] ]; }
                """;
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE PT (NAME VARCHAR)");
            st.execute("INSERT INTO PT VALUES ('ann'), ('bob')");
        }
        // The effect-let path RUNS Phase H (T1.9). This pin was BISTABLE
        // (resolved-rows OR the loud TypedGetAll wall, flipping per JVM
        // run) until the T2.1/T3.1 arc: identity-preserving rebuilds
        // (mapChildren) + the memoized space/anchor classifier removed
        // every run-varying input the routing consumed — no identity-keyed
        // structure is ITERATED anywhere in core (T3.1 B3 census), and
        // 12 consecutive fresh-JVM runs resolve. Tightened to ROWS-ONLY:
        // any recurrence of the wall (or a third behavior) fails loudly.
        Program.last(model, CONN_LET
            + "let names = t9::P.all()->map(p|$p.name)->makeString('_');\n"
            + "let x = meta::relational::metamodel::execute::executeInDb("
            + "'Create Table T9OUT(v VARCHAR); Insert into T9OUT (v)"
            + " values (\\'' + $names + '\\');', $c, 0, 1000);\n"
            + "true;}", "t9::RT", conn);
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("select v from T9OUT")) {
            assertTrue(rs.next(), "resolved path must have inserted");
            assertEquals("ann_bob", rs.getString(1));
        }
    }
}
