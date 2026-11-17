package com.legend.exec;

import com.legend.testbackend.StorelessRuntime;
import com.legend.testing.Lean;
import com.legend.testing.Program;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ExecuteInDbDuckDbTest's cases on the compatibility edge's road (F's census line A5, 2026-11-17: split out so the
 * product's test names no edge class; Phase 6's B4 (ii)): the engine's H2 text translated for the DuckDB session
 * ({@code PureText.DuckDb.raw}: keyword column quoting, H2's FLOAT as DOUBLE), DuckDB's own text as written.
 */
class ExecuteInDbDuckDbEdgeTest {

    /** The compatibility edge's road. */
    private static final com.legend.ExecuteOptions EDGE =
            com.legend.ExecuteOptions.NONE.withStyle(com.legend.edge.PureText.UPSTREAM);

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
    @DisplayName("executeInDb: multi-statement blob with H2-flavored keyword columns executes ambiently on the edge")
    void multiStatementBlobWithKeywordColumns() throws Exception {
        String blob = ExecuteInDbDuckDbTest.CONN_LET + "meta::relational::metamodel::execute::executeInDb("
                + "'Create Table kTest(id INT, default VARCHAR(20));"
                + " Insert into kTest (id, default) values (7, \\'x\\');', $c, 0, 1000);}";
        Lean.Result r = Program.last(StorelessRuntime.with("", ExecuteInDbDuckDbTest.SESSION), blob,
                StorelessRuntime.RUNTIME, conn, EDGE);
        assertNull(r.value(), "opaque ResultSet handle");
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("select id, \"default\" from kTest")) {
            assertTrue(rs.next());
            assertEquals(7, rs.getInt(1));
            assertEquals("x", rs.getString(2));
        }
    }

    @Test
    @DisplayName("executeInDb: on the edge, the engine's H2 FLOAT translated, DuckDB's own text as written")
    void ownDatabaseTextRunsAsWritten() throws Exception {
        Program.last(StorelessRuntime.with("", ExecuteInDbDuckDbTest.SESSION), ExecuteInDbDuckDbTest.OWN_CONN_LET
                + "meta::relational::metamodel::execute::executeInDb('Create Table fOwn(f FLOAT);', $c, 0, 1000);}",
                StorelessRuntime.RUNTIME, conn, EDGE);
        Program.last(StorelessRuntime.with("", ExecuteInDbDuckDbTest.SESSION), ExecuteInDbDuckDbTest.CONN_LET
                + "meta::relational::metamodel::execute::executeInDb('Create Table fH2(f FLOAT);', $c, 0, 1000);}",
                StorelessRuntime.RUNTIME, conn, EDGE);
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("select table_name, data_type from information_schema.columns"
                        + " where table_name in ('fOwn', 'fH2') order by table_name")) {
            assertTrue(rs.next());
            assertEquals("fH2", rs.getString(1));
            assertEquals("DOUBLE", rs.getString(2), "the engine's H2 FLOAT, translated on the edge's road");
            assertTrue(rs.next());
            assertEquals("fOwn", rs.getString(1));
            assertEquals("FLOAT", rs.getString(2), "DuckDB's own FLOAT, as written");
        }
    }
}
