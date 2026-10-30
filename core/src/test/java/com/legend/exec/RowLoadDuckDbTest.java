package com.legend.exec;

import com.legend.testing.Program;

import com.legend.error.DataError;
import com.legend.setup.RowLoad;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A row load lands the SAME rows on DuckDB's bulk path (its Appender, cells
 * staged as text and cast by one INSERT ... SELECT) as on the text path (one
 * multi-row insert of quoted literals): the database types every cell on both. A database without a bulk loader is
 * {@code RowLoadH2Test}'s (both were {@code RowLoadTest} until the dialect program's Phase 6, 2026-10-16).
 */
// reads process-wide counters (CanonicalDivergence, Census) that every query bumps: runs alone if JUnit ever runs
// classes in parallel, so no other test's calls land between its before and after reads (Bazel workplan P3-04, A9)
@org.junit.jupiter.api.parallel.Isolated
class RowLoadDuckDbTest {

    private static final com.legend.sql.dialect.SqlDialect DUCK = new com.legend.sql.dialect.DuckDb();

    private static final String DDL = "CREATE TABLE s.t (i INTEGER, v VARCHAR, d DATE,"
            + " n DECIMAL(10,2), b BOOLEAN, ts TIMESTAMP, f DOUBLE)";

    private static List<List<String>> rows() {
        List<List<String>> rows = new ArrayList<>();
        rows.add(Arrays.asList("1", "O'Brien", "2024-02-29", "12.5", "true", "2024-01-01 10:00:00", "1.5e3"));
        rows.add(Arrays.asList("-2", "", "1999-12-31", "0.01", "false", "2024-06-30 23:59:59.123", "-0.25"));
        rows.add(Arrays.asList(null, null, null, null, null, null, null));
        rows.add(Arrays.asList("3", "semi;colon, comma", "2000-01-01", "-99.99", "1", "2000-01-01", "3"));
        return rows;
    }

    /** The DuckDB session's target: the one fact the runner checks a session against. */
    private static final com.legend.executionplan.ExecutionPlan.Target TARGET =
            new com.legend.executionplan.ExecutionPlan.Target(new com.legend.executionplan.ExecutionPlan.Database
                    .Platform(com.legend.model.ConnectionDefinition.DatabaseType.DuckDB),
                    new com.legend.executionplan.ExecutionPlan.Servers.Every(), List.of(), List.of());

    /** {@code load} on {@code c} through the plan runner: the bulk path (the doors, 2026-10-29; SetupRunner's load
     *  until then). */
    private static void bulk(Connection c, RowLoad load) {
        PlanRunner.setUp(List.of(load.staged(DUCK)), TARGET, PlanSessions.of(com.legend.testbackend.TestSessions.over(c)), null);
    }

    /** {@code load}'s one INSERT on {@code c} through the plan runner: the text path. */
    private static void text(Connection c, RowLoad load) {
        PlanRunner.setUp(List.of(new com.legend.executionplan.ExecutionPlan.SetupStep.Statement(
                DUCK.render(load.values()))), TARGET, PlanSessions.of(com.legend.testbackend.TestSessions.over(c)), null);
    }

    private static Connection duck() throws SQLException {
        Connection c = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = c.createStatement()) {
            st.execute("CREATE SCHEMA s");
            st.execute(DDL);
        }
        return c;
    }

    private static List<String> contents(Connection c) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT * FROM s.t ORDER BY i NULLS LAST, v")) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int k = 1; k <= n; k++) {
                    row.append(k == 1 ? "" : "|").append(rs.getString(k));
                }
                out.add(row.toString());
            }
        }
        return out;
    }

    @Test
    @DisplayName("DuckDB: the bulk path runs and lands exactly the text path's rows")
    void bulkMatchesText() throws Exception {
        RowLoad load = new RowLoad("s", "t", com.legend.sql.NameQuoting.BARE, List.of(), java.util.Set.of(), 7, rows());
        try (Connection bulk = duck(); Connection text = duck()) {
            long before = Census.count(Census.Key.BULK_LOADS);
            bulk(bulk, load);
            assertEquals(before + 1, Census.count(Census.Key.BULK_LOADS),
                    "DuckDB must load through its Appender, not the insert text");
            text(text, load);
            assertEquals(contents(text), contents(bulk));
            assertEquals(4, contents(bulk).size());
        }
    }

    @Test
    @DisplayName("a named column subset: the rest take their default")
    void columnSubset() throws Exception {
        RowLoad load = new RowLoad("s", "t", com.legend.sql.NameQuoting.BARE, List.of("v", "i"), java.util.Set.of(), 2,
                List.of(List.of("x", "7"), List.of("y", "8")));
        try (Connection bulk = duck(); Connection text = duck()) {
            bulk(bulk, load);
            text(text, load);
            assertEquals(List.of("7|x|null|null|null|null|null", "8|y|null|null|null|null|null"),
                    contents(bulk));
            assertEquals(contents(text), contents(bulk));
        }
    }

    @Test
    @DisplayName("a cell the column cannot take fails on the bulk path as on the text path")
    void badCastFails() throws Exception {
        RowLoad load = new RowLoad("s", "t", com.legend.sql.NameQuoting.BARE, List.of("i"), java.util.Set.of(), 1, List.of(List.of("not a number")));
        try (Connection bulk = duck(); Connection text = duck()) {
            assertThrows(DataError.class, () -> bulk(bulk, load));
            assertThrows(DataError.class, () -> text(text, load));
            assertEquals(List.of(), contents(bulk));
        }
    }

    @Test
    @DisplayName("the system metamodel seed loads through the bulk path on DuckDB")
    void systemSeedIsBulk() throws Exception {
        String model = """
                ###Relational
                Database ss::dbInc ( Table T (id INT PRIMARY KEY) )
                Database ss::db ( include ss::dbInc )
                ###Mapping
                Mapping ss::m ( )
                """;
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            long before = Census.count(Census.Key.BULK_LOADS);
            var r = Program.last(com.legend.test.StorelessRuntime.with(model, com.legend.model.ConnectionDefinition.DatabaseType.DuckDB),
                    "|ss::m->meta::pure::mapping::resolveStore(ss::dbInc).name", com.legend.test.StorelessRuntime.RUNTIME, c);
            assertEquals("dbInc", r.one());
            assertTrue(Census.count(Census.Key.BULK_LOADS) > before,
                    "the seed's rows must take DuckDB's Appender");
        }
    }
}
