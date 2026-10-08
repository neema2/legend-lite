package com.legend.exec;

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
 * multi-row insert of quoted literals): the database types every cell on both.
 */
// reads process-wide counters (CanonicalDivergence, Census) that every query bumps: runs alone if JUnit ever runs
// classes in parallel, so no other test's calls land between its before and after reads (Bazel workplan P3-04, A9)
@org.junit.jupiter.api.parallel.Isolated
class RowLoadTest {

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
        RowLoad load = new RowLoad("s", "t", List.of(), 7, rows());
        try (Connection bulk = duck(); Connection text = duck()) {
            long before = Census.count(Census.Key.BULK_LOADS);
            Executor.load(bulk, DUCK, load);
            assertEquals(before + 1, Census.count(Census.Key.BULK_LOADS),
                    "DuckDB must load through its Appender, not the insert text");
            Executor.executeRaw(text, DUCK.render(load.values()));
            assertEquals(contents(text), contents(bulk));
            assertEquals(4, contents(bulk).size());
        }
    }

    @Test
    @DisplayName("a named column subset: the rest take their default")
    void columnSubset() throws Exception {
        RowLoad load = new RowLoad("s", "t", List.of("v", "i"), 2,
                List.of(List.of("x", "7"), List.of("y", "8")));
        try (Connection bulk = duck(); Connection text = duck()) {
            Executor.load(bulk, DUCK, load);
            Executor.executeRaw(text, DUCK.render(load.values()));
            assertEquals(List.of("7|x|null|null|null|null|null", "8|y|null|null|null|null|null"),
                    contents(bulk));
            assertEquals(contents(text), contents(bulk));
        }
    }

    @Test
    @DisplayName("a cell the column cannot take fails on the bulk path as on the text path")
    void badCastFails() throws Exception {
        RowLoad load = new RowLoad("s", "t", List.of("i"), 1, List.of(List.of("not a number")));
        try (Connection bulk = duck(); Connection text = duck()) {
            assertThrows(DataError.class, () -> Executor.load(bulk, DUCK, load));
            assertThrows(DataError.class, () -> Executor.executeRaw(text, DUCK.render(load.values())));
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
            var r = com.legend.Execution.execute(com.legend.test.StorelessRuntime.with(model, com.legend.model.ConnectionDefinition.DatabaseType.DuckDB),
                    "|ss::m->meta::pure::mapping::resolveStore(ss::dbInc).name", com.legend.test.StorelessRuntime.RUNTIME, c);
            assertEquals("dbInc", ((ExecutionResult.Scalar) r).value());
            assertTrue(Census.count(Census.Key.BULK_LOADS) > before,
                    "the seed's rows must take DuckDB's Appender");
        }
    }

    @Test
    @DisplayName("a connection's declared CSV test data loads on its own database, rows exact")
    void declaredCsvTestDataLoadsOnItsDatabase() throws Exception {
        String model = """
                Class x::Firm { name: String[1]; since: StrictDate[0..1]; size: Integer[0..1]; }
                ###Relational
                Database x::DB ( Table FIRM (ID INTEGER PRIMARY KEY, NAME VARCHAR(32), SINCE DATE, SIZE INTEGER) )
                ###Mapping
                Mapping x::M ( *x::Firm: Relational { ~mainTable [x::DB] FIRM
                    name: [x::DB] FIRM.NAME, since: [x::DB] FIRM.SINCE, size: [x::DB] FIRM.SIZE } )
                ###Connection
                RelationalDatabaseConnection x::Conn
                {
                    store: x::DB;
                    type: H2;
                    specification: LocalH2 { testDataSetupCSV: 'default\\nFIRM\\nID,NAME,SINCE,SIZE\\n1,Acme,2020-01-02,10\\n2,O\\'Brien,,---null---\\n'; };
                    auth: DefaultH2;
                }
                ###Runtime
                Runtime x::RT { mappings: [x::M]; connections: [ x::DB: [ env: x::Conn ] ]; }
                """;
        // declared test data is a LocalH2 connection's (as legend-engine's): it loads on the H2 session its
        // runtime declares -- the server opens one for it (ConnectionResolver) -- which takes the insert
        // text; DuckDB's bulk path is systemSeedIsBulk's
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:declaredcsv" + com.legend.exec.H2Settings.SETTINGS)) {
            long before = Census.count(Census.Key.BULK_LOADS);
            var r = com.legend.Execution.execute(model, "|x::Firm.all()->project(~[name: f|$f.name,"
                    + " since: f|$f.since, size: f|$f.size])->sort(~name->ascending())", "x::RT", c);
            assertEquals(List.of("Acme|2020-01-02|10", "O'Brien|null|null"), r.rows().stream()
                    .map(row -> row.get(0) + "|" + row.get(1) + "|" + row.get(2)).toList());
            assertEquals(before, Census.count(Census.Key.BULK_LOADS), "H2 has no bulk loader");
        }
    }

    @Test
    @DisplayName("an engine without a bulk loader takes the insert text")
    void h2TakesText() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:rowload")) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE SCHEMA s");
                st.execute(DDL.replace("DOUBLE", "DOUBLE PRECISION"));
            }
            long before = Census.count(Census.Key.BULK_LOADS);
            Executor.load(c, new com.legend.sql.dialect.H2Modern(), new RowLoad("s", "t", List.of("i", "v"), 2, List.of(List.of("1", "a"))));
            assertEquals(before, Census.count(Census.Key.BULK_LOADS));
            assertEquals(List.of("1|a|null|null|null|null|null"), contents(c));
        }
    }
}
