// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.DialectCapability;

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
        var ctx = com.legend.Compiler.compileModel(MODEL);
        ProjectData project = ProjectData.of(ctx, new DuckDb());
        // its rows: the plan's ONCE load (B3, 2026-11-11: the load a server's project plan carries, which a host runs:
        // its statements, its rows a content blob of inserts)
        var load = (com.legend.executionplan.ExecutionPlan.Load) java.util.Objects.requireNonNull(
                com.legend.planner.LoadPlanner.projectData(ctx, new DuckDb(),
                        com.legend.executionplan.DatabaseType.DuckDB)).root();
        assertEquals(com.legend.executionplan.ExecutionPlan.Load.Cadence.ONCE, load.cadence());
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
            for (com.legend.executionplan.ExecutionPlan.LoadStep step : load.steps()) {
                switch (step) {
                    case com.legend.executionplan.ExecutionPlan.LoadStep.Statement one -> st.execute(one.sql());
                    case com.legend.executionplan.ExecutionPlan.LoadStep.Statements blob -> {
                        for (String sql : com.legend.executionplan.DataRef.Statements.decode(
                                load.content().get(blob.ref().digest()))) {
                            st.execute(sql);
                        }
                    }
                    case com.legend.executionplan.ExecutionPlan.LoadStep.File file ->
                            throw new AssertionError("a project's Data is statements: " + file);
                }
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
        // refused where its rows are planned: the plan's load (B3, 2026-11-11: until then where the fill's text was)
        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                () -> com.legend.planner.LoadPlanner.projectData(com.legend.Compiler.compileModel(model), new DuckDb(),
                        com.legend.executionplan.DatabaseType.DuckDB));
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
        // credentials by name (B3, 2026-11-11): every value bound, the statement holding none
        assertEquals("CREATE OR REPLACE TEMPORARY SECRET \"datacube_s3_t\" (TYPE S3, KEY_ID ?, USE_SSL ?, SCOPE ?)",
                one(new HostOperation.Secret("datacube_s3_t", "s3://b/", List.of("KEY_ID", "USE_SSL"))));
        assertEquals(List.of("KEY_ID", "USE_SSL", "SCOPE"),
                new HostOperation.Secret("datacube_s3_t", "s3://b/", List.of("KEY_ID", "USE_SSL")).binds());
        assertEquals(List.of(), DUCK.statements(new HostOperation.Secret("s", "s3://b/", List.of())));
        assertEquals("DROP TEMPORARY SECRET IF EXISTS \"datacube_s3_t\"", one(new HostOperation.DropSecret("datacube_s3_t")));
        assertThrows(IllegalArgumentException.class, () -> new HostOperation.Secret("s", "x", List.of("KEY_ID", "KEY_ID")));
        assertThrows(IllegalArgumentException.class, () -> DUCK.statements(new HostOperation.Extensions(List.of("httpfs; DROP"))));
        assertThrows(IllegalArgumentException.class, () -> DUCK.statements(new HostOperation.Secret("s", "x", List.of("PASSWORD"))));
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

    /**
     * CREDENTIALS BY NAME (B3, 2026-11-11, docs/TOTAL_WALL_START_HERE_2026_11_11.md §5.2): a secret's operation names its
     * fields, never their values, so no statement the operations answer can hold one -- each field's value and the scope
     * a bound {@code ?} in {@link HostOperation.Secret#binds()}'s order. DuckDB makes the secret from the bound values
     * (measured on DuckDB 1.5.6 JDBC, every field and the scope; docs/data-delivery-homework/probes/DuckDbBoundSecret.java.txt);
     * the run itself is DuckDB-WASM's test (datacube's remote.test.ts: an S3 secret needs httpfs, which this sandbox's
     * JDBC driver cannot load offline).
     */
    @Test
    void aSecretsStatementBindsEveryValue() {
        HostOperation.Secret secret = new HostOperation.Secret("datacube_s3_t", "s3://bucket/sales/",
                List.of("KEY_ID", "SECRET", "SESSION_TOKEN", "REGION", "ENDPOINT", "URL_STYLE", "USE_SSL"));
        String made = one(secret);
        assertEquals("CREATE OR REPLACE TEMPORARY SECRET \"datacube_s3_t\" (TYPE S3, KEY_ID ?, SECRET ?, SESSION_TOKEN ?,"
                + " REGION ?, ENDPOINT ?, URL_STYLE ?, USE_SSL ?, SCOPE ?)", made);
        assertEquals(secret.binds().size(), made.chars().filter(ch -> ch == '?').count(),
                "one bound value per ? (each field's, then the scope)");
        assertFalse(made.contains("s3://bucket"), "the scope is bound too: the statement holds no value");
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
