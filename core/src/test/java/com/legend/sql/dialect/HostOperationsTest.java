// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.setup.ProjectData;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A project's own database (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C5): {@link ProjectData}'s statements, run on a
 * DuckDB, make a database of the project's own inside it, its schemas as the model declares them and its Data rows in
 * them, beside a person's table of the same name and never touching it; and every database without the operations
 * refuses by name. In the dialect package's tests: they name DuckDB by design.
 */
class HostOperationsTest {

    private static final String MODEL = """
            ###Relational
            Database demo::Store
            (
               Schema firm
               (
                  Table Person (id INTEGER PRIMARY KEY, name VARCHAR(50))
               )
               Table Code (code VARCHAR(10))
            )

            ###Data
            Data demo::Rows
            {
              Relational
              #{
                firm.Person:
                  'id,name\\n' +
                  '1,Ada\\n' +
                  '2,Grace\\n';
                default.Code:
                  'code\\n' +
                  'X1\\n';
              }#
            }
            """;

    @Test
    void duckDbsStatements() {
        HostOperations duck = HostOperations.of(new DuckDb());
        assertEquals(List.of("ATTACH ':memory:' AS \"p\"\"1\""), duck.openDatabase("p\"1"));
        assertEquals(List.of("USE \"p\"\"1\""), duck.useDatabase("p\"1"));
        assertEquals(List.of("DETACH DATABASE IF EXISTS \"p\"\"1\""), duck.closeDatabase("p\"1"));
    }

    @Test
    void aProjectsRowsInADatabaseOfTheirOwn() throws SQLException {
        ProjectData project = ProjectData.of(com.legend.Compiler.compileModel(MODEL), new DuckDb());
        assertTrue(project.name().matches("__project_[0-9a-f]{16}"), project.name());
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement st = c.createStatement()) {
            // a person's own table, of the very name the project declares, in the host's own database
            st.execute("CREATE SCHEMA firm");
            st.execute("CREATE TABLE firm.Person (mine VARCHAR)");
            st.execute("INSERT INTO firm.Person VALUES ('kept')");
            for (String sql : project.open()) {
                st.execute(sql);
            }
            for (String sql : project.use()) {
                st.execute(sql);
            }
            for (String sql : project.fill()) {
                st.execute(sql);
            }
            assertEquals(List.of("1 Ada", "2 Grace"), rows(st, "SELECT id || ' ' || name FROM firm.Person ORDER BY id"));
            assertEquals(List.of("X1"), rows(st, "SELECT code FROM Code"));
            // back on the host's own database: the person's table as it was
            st.execute("USE memory");
            assertEquals(List.of("kept"), rows(st, "SELECT mine FROM firm.Person"));
            for (String sql : project.close()) {
                st.execute(sql);
            }
            assertFalse(rows(st, "SELECT database_name FROM duckdb_databases()").contains(project.name()));
            // dropped twice: nothing refused
            for (String sql : project.close()) {
                st.execute(sql);
            }
        }
    }

    /** Named by what it holds (Plan Gen's review, 2026-10-13): never a caller's label, so a SNAPSHOT republished under
     *  the same coordinates with new rows is never answered with the old. */
    @Test
    void namedByWhatItHolds() {
        String name = ProjectData.of(com.legend.Compiler.compileModel(MODEL), new DuckDb()).name();
        // the same Databases and rows, another class beside them: the same database, no refill
        String withAClass = MODEL.replace("###Data", "###Pure\nClass demo::Other { x: Integer[1]; }\n\n###Data");
        assertEquals(name, ProjectData.of(com.legend.Compiler.compileModel(withAClass), new DuckDb()).name());
        // an edited row, or an edited table, is another
        assertNotEquals(name, ProjectData.of(com.legend.Compiler.compileModel(MODEL.replace("2,Grace", "3,Edsger")),
                new DuckDb()).name());
        assertNotEquals(name, ProjectData.of(com.legend.Compiler.compileModel(MODEL.replace("VARCHAR(50)", "VARCHAR(60)")),
                new DuckDb()).name());
    }

    @Test
    void everyOtherDatabaseRefusesByName() {
        for (SqlDialect d : List.of(new Postgres(), new H2(), new Snowflake(), new SQLite(), new ClickHouse())) {
            HostOperations host = HostOperations.of(d);
            String name = d.getClass().getSimpleName();
            for (Runnable op : List.<Runnable>of(() -> host.openDatabase("p"), () -> host.useDatabase("p"),
                    () -> host.closeDatabase("p"))) {
                DialectCapability refused = assertThrows(DialectCapability.class, op::run, name);
                assertTrue(refused.getMessage().contains(" on " + name + " is not built"), refused.getMessage());
            }
            assertThrows(DialectCapability.class,
                    () -> ProjectData.of(com.legend.Compiler.compileModel(MODEL), d), name);
        }
    }

    @Test
    void aDataElementFillingNoDeclaredTableIsRefused() {
        String model = MODEL.replace("default.Code:", "default.Missing:");
        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                () -> ProjectData.of(com.legend.Compiler.compileModel(model), new DuckDb()));
        assertEquals("the Data element demo::Rows fills default.Missing, which no Database of the model declares",
                refused.getMessage());
    }

    // ---- the named operations (D5): each statement as the tab and Python's host spelled it before, and run

    private static final HostOperations DUCK = HostOperations.of(new DuckDb());

    private static String one(HostOperation op) {
        List<String> out = DUCK.statements(op);
        assertEquals(1, out.size(), out.toString());
        return out.get(0);
    }

    @Test
    void duckDbsNamedOperations() {
        var file = HostOperation.Name.of("pivot");
        assertEquals("CREATE OR REPLACE TABLE \"pivot\" AS SELECT * FROM read_csv('upload-1.csv', AUTO_DETECT=TRUE, HEADER=TRUE)",
                one(new HostOperation.MakeRelation(HostOperation.Kind.TABLE, file, true, null, List.of(),
                        new HostOperation.Source.File("upload-1.csv", HostOperation.FileFormat.CSV))));
        assertEquals("CREATE OR REPLACE TABLE \"pivot\" AS SELECT \"a\", CAST(\"b\" AS VARCHAR) AS \"b\" FROM \"pivot\"",
                one(new HostOperation.MakeRelation(HostOperation.Kind.TABLE, file, true,
                        "\"a\", CAST(\"b\" AS VARCHAR) AS \"b\"", List.of(), new HostOperation.Source.Relation(file))));
        assertEquals("CREATE VIEW \"t\" AS SELECT * FROM \"__reg\"", one(new HostOperation.MakeRelation(HostOperation.Kind.VIEW,
                HostOperation.Name.of("t"), false, null, List.of(), new HostOperation.Source.Relation(HostOperation.Name.of("__reg")))));
        assertEquals("CREATE OR REPLACE VIEW \"r\" AS SELECT * FROM iceberg_scan('s3://b/t')", one(new HostOperation.MakeRelation(
                HostOperation.Kind.VIEW, HostOperation.Name.of("r"), true, null, List.of(),
                new HostOperation.Source.Remote("s3://b/t", HostOperation.RemoteFormat.ICEBERG))));
        assertEquals("CREATE OR REPLACE TABLE \"db\".\"s\".\"t\" AS SELECT * REPLACE (CAST(x AS VARCHAR) AS \"x\") FROM (SELECT 1 AS x)",
                one(new HostOperation.MakeRelation(HostOperation.Kind.TABLE, new HostOperation.Name("db", "s", "t", false), true,
                        null, List.of(new HostOperation.Conversion("x", "CAST(x AS VARCHAR)")),
                        new HostOperation.Source.Query("SELECT 1 AS x"))));
        assertEquals("INSERT INTO PARTY.PARTY (ID, NAME, \"order\") SELECT \"ID\", \"first name\", \"order\" FROM read_csv('f.csv', header = true, all_varchar = true)",
                one(new HostOperation.Insert(HostOperation.Name.declared("PARTY", "PARTY"),
                        List.of(new HostOperation.Column("ID", "ID"), new HostOperation.Column("NAME", "first name"),
                                new HostOperation.Column("order", "order")),
                        new HostOperation.Source.File("f.csv", HostOperation.FileFormat.CSV_TEXT))));
        assertEquals("SELECT count(*) AS n FROM T", one(new HostOperation.Count(
                new HostOperation.Source.Relation(HostOperation.Name.declared("default", "T")))));
        assertEquals("DROP VIEW IF EXISTS \"main\".\"x\"", one(new HostOperation.Drop(HostOperation.Kind.VIEW,
                new HostOperation.Name(null, "main", "x", false))));
        assertEquals(List.of("INSTALL httpfs", "LOAD httpfs", "INSTALL iceberg", "LOAD iceberg"),
                DUCK.statements(new HostOperation.Extensions(List.of("httpfs", "iceberg"))));
        assertEquals(List.of("SET custom_extension_repository = 'https://site/vendor/duckdb-extensions'",
                "SET autoinstall_extension_repository = 'https://site/vendor/duckdb-extensions'"),
                DUCK.statements(new HostOperation.ExtensionRepository("https://site/vendor/duckdb-extensions")));
        assertEquals("CREATE OR REPLACE SECRET \"datacube_s3_t\" (TYPE S3, KEY_ID 'k''1', USE_SSL false, SCOPE 's3://b/')",
                one(new HostOperation.Secret("datacube_s3_t", "s3://b/", credentials())));
        assertEquals(List.of(), DUCK.statements(new HostOperation.Secret("s", "s3://b/", java.util.Map.of())));
        assertThrows(IllegalArgumentException.class, () -> DUCK.statements(new HostOperation.Extensions(List.of("httpfs; DROP"))));
        assertThrows(IllegalArgumentException.class, () -> DUCK.statements(new HostOperation.Secret("s", "x", java.util.Map.of("PASSWORD", "p"))));
    }

    @Test
    void duckDbsOperationsRunAsTheyRead(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir) throws Exception {
        java.nio.file.Path csv = dir.resolve("people.csv");
        java.nio.file.Files.writeString(csv, "ID,first name\n1,Ada\n2,Grace\n");
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement st = c.createStatement()) {
            var t = HostOperation.Name.of("people");
            for (HostOperation op : List.of(
                    new HostOperation.MakeRelation(HostOperation.Kind.TABLE, t, true, null, List.of(),
                            new HostOperation.Source.File(csv.toString(), HostOperation.FileFormat.CSV)),
                    new HostOperation.MakeRelation(HostOperation.Kind.TABLE, t, true, null,
                            List.of(new HostOperation.Conversion("first name", "upper(\"first name\")")),
                            new HostOperation.Source.Relation(t)),
                    new HostOperation.MakeSchema(null, "firm"),
                    new HostOperation.MakeRelation(HostOperation.Kind.VIEW, new HostOperation.Name(null, "firm", "v", false), false,
                            null, List.of(), new HostOperation.Source.Query("SELECT ID FROM people")))) {
                for (String sql : DUCK.statements(op)) {
                    st.execute(sql);
                }
            }
            assertEquals(List.of("ADA", "GRACE"), rows(st, "SELECT \"first name\" FROM people ORDER BY ID"));
            assertEquals(List.of("2"), rows(st, one(new HostOperation.Count(new HostOperation.Source.Relation(t)))));
            assertEquals(List.of("2"), rows(st, one(new HostOperation.Count(new HostOperation.Source.Query("SELECT * FROM firm.v")))));
            assertEquals(List.of("1"), rows(st, one(new HostOperation.NameTaken("PEOPLE"))));
            assertEquals(List.of("1"), rows(st, one(new HostOperation.RelationExists(new HostOperation.Name(null, "FIRM", "V", false)))));
            assertEquals(List.of("1"), rows(st, one(new HostOperation.SchemaExists(null, "Firm"))));
            assertEquals(List.of("0"), rows(st, one(new HostOperation.TablesIn(null, "firm"))));
            assertEquals(List.of("memory"), rows(st, one(new HostOperation.CurrentDatabase())));
            for (String sql : DUCK.statements(new HostOperation.Drop(HostOperation.Kind.VIEW, new HostOperation.Name(null, "firm", "v", false)))) {
                st.execute(sql);
            }
            for (String sql : DUCK.statements(new HostOperation.DropSchema(null, "firm"))) {
                st.execute(sql);
            }
            assertEquals(List.of("0"), rows(st, one(new HostOperation.SchemaExists(null, "firm"))));
        }
    }

    /** A live table's writes (docs/DUCKDB_LIVE_2026_10_25.md): an upsert keeps the latest row a key -- a batch's last of
     *  a key, a later batch's over it -- and a stream's bound deletes what is past it, by rows or by a span. */
    @Test
    void liveTablesUpsertAndBound() throws Exception {
        var quote = HostOperation.Name.declared(null, "QUOTE");
        var staged = new HostOperation.Source.Relation(HostOperation.Name.of("__staged"));
        var upsert = new HostOperation.Upsert(quote, List.of(new HostOperation.Column("SYMBOL", "SYMBOL"),
                new HostOperation.Column("BID", "bid")), List.of("SYMBOL"), staged);
        assertEquals("INSERT OR REPLACE INTO QUOTE (SYMBOL, BID) SELECT \"SYMBOL\", \"bid\" FROM (SELECT *, row_number() OVER "
                + "(PARTITION BY \"SYMBOL\" ORDER BY rowid DESC) AS \"__latest\" FROM \"__staged\") WHERE \"__latest\" = 1", one(upsert));
        var tape = HostOperation.Name.declared(null, "TAPE");
        var byRows = new HostOperation.Bound(tape, "SEQ", new HostOperation.Bound.Keep.Rows(3));
        var bySpan = new HostOperation.Bound(tape, "AT", new HostOperation.Bound.Keep.Within(30_000));
        assertEquals("DELETE FROM TAPE WHERE SEQ < (SELECT SEQ FROM TAPE ORDER BY SEQ DESC LIMIT 1 OFFSET 2)", one(byRows));
        // AT a keyword of DuckDB's: the declared column quoted, as its seed made it
        assertEquals("DELETE FROM TAPE WHERE \"AT\" < (SELECT max(\"AT\") - to_milliseconds(30000) FROM TAPE)", one(bySpan));
        assertThrows(IllegalArgumentException.class, () -> new HostOperation.Upsert(quote,
                List.of(new HostOperation.Column("BID", "bid")), List.of("SYMBOL"), staged));
        assertThrows(IllegalArgumentException.class, () -> new HostOperation.Bound.Keep.Rows(0));

        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE QUOTE (SYMBOL VARCHAR PRIMARY KEY, BID DOUBLE)");
            st.execute("CREATE TABLE \"__staged\" AS SELECT * FROM (VALUES ('AAPL', 1.0), ('MSFT', 10.0), ('AAPL', 3.0)) t(SYMBOL, bid)");
            st.execute(one(upsert));
            st.execute("CREATE OR REPLACE TABLE \"__staged\" AS SELECT * FROM (VALUES ('MSFT', 12.0)) t(SYMBOL, bid)");
            st.execute(one(upsert));
            assertEquals(List.of("AAPL 3.0", "MSFT 12.0"), rows(st, "SELECT SYMBOL || ' ' || BID FROM QUOTE ORDER BY SYMBOL"));

            st.execute("CREATE TABLE TAPE AS SELECT i AS SEQ, TIMESTAMP '2026-10-25 14:30:00' + to_seconds(i) AS AT FROM range(61) t(i)");
            st.execute(one(bySpan));
            assertEquals(List.of("30 31"), rows(st, "SELECT min(SEQ) || ' ' || count(*) FROM TAPE"));
            st.execute(one(byRows));
            assertEquals(List.of("58 3"), rows(st, "SELECT min(SEQ) || ' ' || count(*) FROM TAPE"));
            st.execute(one(new HostOperation.Bound(tape, "SEQ", new HostOperation.Bound.Keep.Rows(10))));
            assertEquals(List.of("3"), rows(st, "SELECT count(*) FROM TAPE"), "a stream holding fewer than its bound loses none");
        }
    }

    @Test
    void postgresCountsWithTheAliasItAsksFor() {
        HostOperations pg = HostOperations.of(new Postgres());
        assertEquals(List.of("SELECT count(*) AS n FROM (SELECT 1) AS counted"),
                pg.statements(new HostOperation.Count(new HostOperation.Source.Query("SELECT 1"))));
        DialectCapability refused = assertThrows(DialectCapability.class,
                () -> pg.statements(new HostOperation.Extensions(List.of("httpfs"))));
        assertTrue(refused.getMessage().contains("Extensions on Postgres is not built"), refused.getMessage());
        assertThrows(DialectCapability.class, () -> HostOperations.of(new H2()).statements(new HostOperation.CurrentDatabase()));
    }

    /** Credentials in their order: a key with a quote in it, and SSL off. */
    private static java.util.Map<String, String> credentials() {
        java.util.Map<String, String> out = new java.util.LinkedHashMap<>();
        out.put("KEY_ID", "k'1");
        out.put("USE_SSL", "false");
        return out;
    }

    private static List<String> rows(Statement st, String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                out.add(rs.getString(1));
            }
        }
        return out;
    }
}
