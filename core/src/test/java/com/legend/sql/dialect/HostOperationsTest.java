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
                    case com.legend.executionplan.ExecutionPlan.LoadStep.Bound bound ->
                            throw new AssertionError("a project's Data binds nothing from a host: " + bound);
                    case com.legend.executionplan.ExecutionPlan.LoadStep.Stream stream ->
                            throw new AssertionError("a project's Data streams no remote plane: " + stream);
                    case com.legend.executionplan.ExecutionPlan.LoadStep.Attach attach ->
                            throw new AssertionError("a project's Data attaches no host's value: " + attach);
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
        // a host's own file: its ? bound by the host's runner, never its name or path written (B3, 2026-11-11)
        assertEquals("CREATE OR REPLACE TABLE \"pivot\" AS SELECT * FROM read_csv(?, AUTO_DETECT=TRUE, HEADER=TRUE)",
                one(new HostOperation.MakeRelation(HostOperation.Kind.TABLE, file, true, List.of(),
                        new HostOperation.Source.HostFile(HostOperation.FileFormat.CSV))));
        // a copy names its columns' catalog, and the platform writes their conversions (the final audit's 1(d),
        // 2026-11-12: until then a host handed the select list in, as text)
        assertEquals("CREATE OR REPLACE TABLE \"pivot\" AS SELECT * REPLACE (CAST(timezone('UTC', \"at\") AS TIMESTAMP) AS"
                + " \"at\", CAST(\"big\" AS DECIMAL(20,0)) AS \"big\") FROM \"pivot\"",
                one(new HostOperation.MakeRelation(HostOperation.Kind.TABLE, file, true, CatalogModelTest.catalog(
                        "id BIGINT, \"at\" TIMESTAMP WITH TIME ZONE, big UBIGINT, u UUID"),
                        new HostOperation.Source.Relation(file))));
        assertEquals("CREATE VIEW \"t\" AS SELECT * FROM \"__reg\"", one(new HostOperation.MakeRelation(HostOperation.Kind.VIEW,
                HostOperation.Name.of("t"), false, List.of(), new HostOperation.Source.Relation(HostOperation.Name.of("__reg")))));
        assertEquals("CREATE OR REPLACE VIEW \"r\" AS SELECT * FROM iceberg_scan('s3://b/t')", one(new HostOperation.MakeRelation(
                HostOperation.Kind.VIEW, HostOperation.Name.of("r"), true, List.of(),
                new HostOperation.Source.Remote(new HostOperation.Location.Url("s3://b/t"), HostOperation.RemoteFormat.ICEBERG))));
        // a bearer URL (B3's Q3): read through the session variable the host binds it to, the view's text holding none
        // the variable the dialect names for the value the host holds under "datacube_url_r": the hex of the name's bytes
        assertEquals("lite_url_6461746163756265" + "5f75726c5f72", HostOperation.variable("datacube_url_r"));
        assertEquals("CREATE OR REPLACE VIEW \"r\" AS SELECT * FROM read_parquet(getvariable('" + HostOperation.variable("datacube_url_r") + "'))",
                one(new HostOperation.MakeRelation(HostOperation.Kind.VIEW, HostOperation.Name.of("r"), true, List.of(),
                        new HostOperation.Source.Remote(new HostOperation.Location.Variable("datacube_url_r"),
                                HostOperation.RemoteFormat.PARQUET))));
        assertEquals("SET VARIABLE " + HostOperation.variable("datacube_url_r") + " = ?", one(new HostOperation.SetVariable("datacube_url_r")));
        assertEquals("RESET VARIABLE " + HostOperation.variable("datacube_url_r"), one(new HostOperation.ResetVariable("datacube_url_r")));
        assertTrue(assertThrows(IllegalArgumentException.class, () -> new HostOperation.Location.Url(
                "https://b.s3.amazonaws.com/t.parquet?X-Amz-Signature=SENTINEL")).getMessage().contains("carries its token"));
        // a user in the address is a credential (the B checkpoint's tenets audit, LOW 5, 2026-11-11)
        var withUser = assertThrows(IllegalArgumentException.class, () -> new HostOperation.Location.Url(
                "https://u:SENTINEL@h/f.parquet"));
        assertTrue(withUser.getMessage().contains("names a user") && !withUser.getMessage().contains("SENTINEL"),
                withUser.getMessage());
        assertThrows(IllegalArgumentException.class, () -> new HostOperation.SetVariable("x = 1; DROP TABLE t"));
        assertEquals("CREATE OR REPLACE TABLE \"db\".\"s\".\"t\" AS SELECT * REPLACE (CAST(\"x\" AS DECIMAL(20,0)) AS \"x\") FROM \"u\"",
                one(new HostOperation.MakeRelation(HostOperation.Kind.TABLE, new HostOperation.Name("db", "s", "t", false), true,
                        CatalogModelTest.catalog("x UBIGINT"),
                        new HostOperation.Source.Relation(HostOperation.Name.of("u")))));
        assertEquals("INSERT INTO PARTY.PARTY (ID, NAME, \"order\") SELECT \"ID\", \"first name\", \"order\" FROM read_csv(?, header = true, all_varchar = true)",
                one(new HostOperation.Insert(HostOperation.Name.declared("PARTY", "PARTY"),
                        List.of(new HostOperation.Column("ID", "ID"), new HostOperation.Column("NAME", "first name"),
                                new HostOperation.Column("order", "order")),
                        new HostOperation.Source.HostFile(HostOperation.FileFormat.CSV_TEXT))));
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
            // the file read as a host's planned load reads it: the statement's ? bound to its path
            try (java.sql.PreparedStatement read = c.prepareStatement(one(new HostOperation.MakeRelation(
                    HostOperation.Kind.TABLE, t, true, List.of(), new HostOperation.Source.HostFile(
                            HostOperation.FileFormat.CSV))))) {
                read.setString(1, csv.toString());
                read.execute();
            }
            for (HostOperation op : List.of(
                    new HostOperation.MakeRelation(HostOperation.Kind.TABLE, t, true,
                            CatalogModelTest.catalog("ID BIGINT, \"first name\" VARCHAR"),
                            new HostOperation.Source.Relation(t)),
                    new HostOperation.MakeSchema(null, "firm"),
                    new HostOperation.MakeRelation(HostOperation.Kind.VIEW, new HostOperation.Name(null, "firm", "v", false), false,
                            List.of(), new HostOperation.Source.Relation(t)))) {
                for (String sql : DUCK.statements(op)) {
                    st.execute(sql);
                }
            }
            assertEquals(List.of("Ada", "Grace"), rows(st, "SELECT \"first name\" FROM people ORDER BY ID"));
            assertEquals(List.of("2"), rows(st, one(new HostOperation.Count(new HostOperation.Source.Relation(t)))));
            assertEquals(List.of("2"), rows(st, one(new HostOperation.Count(new HostOperation.Source.Relation(
                    new HostOperation.Name(null, "firm", "v", false))))));
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

    /** An ingest's landing on DuckDB (docs/DESK_DATA_ROADMAP_2026_10_29.md C0), as spelled: an upsert keeps the latest
     *  row a key -- a batch's last of a key -- and a retention keeps a table's latest rows by arrival, its row order. */
    @Test
    void anIngestsUpsertAndRetention() throws Exception {
        var quote = HostOperation.Name.declared(null, "QUOTE");
        var staged = new HostOperation.Source.Relation(HostOperation.Name.of("__staged"));
        var upsert = new HostOperation.Upsert(quote, List.of(new HostOperation.Column("SYMBOL", "SYMBOL"),
                new HostOperation.Column("BID", "bid")), List.of("SYMBOL"), staged);
        assertEquals("INSERT OR REPLACE INTO QUOTE (SYMBOL, BID) SELECT \"SYMBOL\", \"bid\" FROM (SELECT *, row_number() OVER "
                + "(PARTITION BY \"SYMBOL\" ORDER BY rowid DESC) AS \"__latest\" FROM \"__staged\") WHERE \"__latest\" = 1", one(upsert));
        var tape = HostOperation.Name.declared(null, "TAPE");
        var keep3 = new HostOperation.Bound(tape, 3);
        assertEquals("DELETE FROM TAPE WHERE rowid < (SELECT rowid FROM TAPE ORDER BY rowid DESC LIMIT 1 OFFSET 2)", one(keep3));
        assertThrows(IllegalArgumentException.class, () -> new HostOperation.Upsert(quote,
                List.of(new HostOperation.Column("BID", "bid")), List.of("SYMBOL"), staged));
        assertThrows(IllegalArgumentException.class, () -> new HostOperation.Bound(tape, 0));

        // run on DuckDB through an ingest's plans: planner/DuckDbIngestPlansTest
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

    /**
     * A BEARER URL IS ONE CONNECTION'S (B3's Q3, 2026-11-11, measured first on duckdb-python 1.5.5 and DuckDB-WASM): the
     * variable bound by the prepared {@code SET VARIABLE}, the view reading it through {@code getvariable} -- its text
     * holding the variable's name, never the address; a second connection to the same database reads the view only once
     * the setting is replayed on it (the host's replay), and a reset leaves the other connection's own.
     */
    @Test
    void aBearerUrlIsOneConnectionsVariable(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir) throws Exception {
        // the file written by Java, not by a JDBC write (the fixtures' writes are bounded: FixtureWriteCensusTest)
        java.nio.file.Path file = dir.resolve("t.csv");
        java.nio.file.Files.writeString(file, "id\n1\n2\n");
        String variable = HostOperation.variable("datacube_url_t");
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement st = c.createStatement();
                // another connection to the same database, opened before the variable is set (DuckDB JDBC's duplicate
                // copies the variables it has then -- measured, 1.5.6 -- so a later one would start with it); reached
                // without its type: no direct dependency here
                Connection other = (Connection) c.getClass().getMethod("duplicate").invoke(c);
                Statement os = other.createStatement()) {
            try (java.sql.PreparedStatement set = c.prepareStatement(one(new HostOperation.SetVariable("datacube_url_t")))) {
                set.setString(1, file.toString());
                set.execute();
            }
            st.execute(one(new HostOperation.MakeRelation(HostOperation.Kind.VIEW, HostOperation.Name.of("t"), true,
                    List.of(), new HostOperation.Source.Remote(new HostOperation.Location.Variable("datacube_url_t"),
                            HostOperation.RemoteFormat.CSV))));
            assertEquals(List.of("2"), rows(st, "SELECT count(*) FROM \"t\""));
            String text = rows(st, "SELECT sql FROM duckdb_views() WHERE view_name = 't'").get(0);
            assertFalse(text.contains(dir.toString()), "the view's text holds the variable's name, never the address: " + text);
            // the other connection holds no variable: the view reads nothing there until the setting is replayed on it
            assertEquals(java.util.Collections.singletonList(null), rows(os, "SELECT getvariable('" + variable + "')"));
            // (a failed query closes DuckDB JDBC's statement: each read after one on a statement of its own)
            assertThrows(SQLException.class, () -> rows(os, "SELECT count(*) FROM \"t\""));
            try (java.sql.PreparedStatement replay = other.prepareStatement(one(new HostOperation.SetVariable("datacube_url_t")))) {
                replay.setString(1, file.toString());
                replay.execute();
            }
            try (Statement again = other.createStatement()) {
                assertEquals(List.of("2"), rows(again, "SELECT count(*) FROM \"t\""));
            }
            st.execute(one(new HostOperation.ResetVariable("datacube_url_t")));
            assertThrows(SQLException.class, () -> rows(st, "SELECT count(*) FROM \"t\""));
            try (Statement again = other.createStatement()) {
                assertEquals(List.of("2"), rows(again, "SELECT count(*) FROM \"t\""), "a reset is its own connection's");
            }
        }
    }

    /** A copy names its source's catalog columns, and the platform writes the select list a host handed in until then
     *  (the final audit's 1(d), 2026-11-12): from TIMESTAMPTZ, UBIGINT and UUID columns, exactly the list
     *  {@code Boundary.tableModel} answered ({@code copySelectList}, from a convertible table's conversions), the UUID
     *  and the BIGINT copied as they are; no column that converts, a plain star. */
    @Test
    void aCopysCatalogColumnsWriteTheSelectListTableModelAnswered() {
        List<CatalogModel.Column> columns = CatalogModelTest.catalog(
                "id BIGINT, \"at\" TIMESTAMP WITH TIME ZONE, big UBIGINT, u UUID");
        List<CatalogModel.Conversion> today = CatalogModel.database("local::DB", null, "src", columns, new DuckDb(), true)
                .conversions();
        assertEquals(today, CatalogModel.copyConversions(columns, new DuckDb()));
        assertEquals("CREATE OR REPLACE TABLE \"copy\" AS SELECT * REPLACE (CAST(timezone('UTC', \"at\") AS TIMESTAMP)"
                + " AS \"at\", CAST(\"big\" AS DECIMAL(20,0)) AS \"big\") FROM \"src\"",
                one(new HostOperation.MakeRelation(HostOperation.Kind.TABLE, HostOperation.Name.of("copy"), true, columns,
                        new HostOperation.Source.Relation(HostOperation.Name.of("src")))));
        assertEquals("CREATE OR REPLACE TABLE \"copy\" AS SELECT " + new DuckDb().copySelectList(today) + " FROM \"src\"",
                one(new HostOperation.MakeRelation(HostOperation.Kind.TABLE, HostOperation.Name.of("copy"), true, columns,
                        new HostOperation.Source.Relation(HostOperation.Name.of("src")))));
        assertEquals("CREATE TABLE \"copy\" AS SELECT * FROM \"src\"", one(new HostOperation.MakeRelation(
                HostOperation.Kind.TABLE, HostOperation.Name.of("copy"), false, CatalogModelTest.catalog("id BIGINT, u UUID"),
                new HostOperation.Source.Relation(HostOperation.Name.of("src")))));
        IllegalArgumentException twice = assertThrows(IllegalArgumentException.class, () -> CatalogModel.copyConversions(
                List.of(columns.get(0), columns.get(0)), new DuckDb()));
        assertEquals("a copy's columns name 'id' twice", twice.getMessage());
    }

    /** Postgres writes no host operation (the final audit, 2026-11-12: its one, a planner's query counted, went with
     *  {@code Source.Query}): a count, an extension, each refused by name, and no transaction declared. */
    @Test
    void postgresRefusesEveryOperationByName() {
        HostOperations pg = HostOperations.of(new Postgres());
        DialectCapability count = assertThrows(DialectCapability.class, () -> pg.statements(
                new HostOperation.Count(new HostOperation.Source.Relation(HostOperation.Name.of("t")))));
        assertTrue(count.getMessage().contains("Count on Postgres is not built"), count.getMessage());
        DialectCapability refused = assertThrows(DialectCapability.class,
                () -> pg.statements(new HostOperation.Extensions(List.of("httpfs"))));
        assertTrue(refused.getMessage().contains("Extensions on Postgres is not built"), refused.getMessage());
        assertEquals(null, pg.transaction());
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
