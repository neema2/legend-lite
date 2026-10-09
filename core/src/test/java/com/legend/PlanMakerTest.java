// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.database.Databases;
import com.legend.executionplan.ExecutionPlan;
import com.legend.lowering.WireRender;
import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The planner's plans (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 2's landing 2): a plan answers exactly as
 * today's wire and streaming paths answer the same query. Each query runs twice, on two fresh databases: through
 * {@link Execution} (its connection's declared test data established first, as the server establishes it), and through
 * its plan — the target's session statements, its setup steps, its statement — run here as each step describes itself
 * (the runner is step 3). The texts must be equal, byte for byte. Postgres's case is {@code PostgresArmTest}'s (it needs
 * the embedded Postgres).
 */
public class PlanMakerTest {

    private static final AtomicInteger H2_NAMES = new AtomicInteger();

    /** A table of three rows, declared as the connection's test data; {@code type} is the connection's database. */
    private static String model(String type) {
        return """
                Class s::Item { id: Integer[1]; name: String[0..1]; price: Decimal[0..1]; }
                ###Relational
                Database s::DB ( Table T ( ID INTEGER PRIMARY KEY, NAME VARCHAR(20), PRICE DECIMAL(10,2) ) )
                ###Mapping
                Mapping s::M ( *s::Item: Relational { ~mainTable [s::DB] T
                    id: [s::DB] T.ID, name: [s::DB] T.NAME, price: [s::DB] T.PRICE } )
                ###Connection
                RelationalDatabaseConnection s::Conn
                {
                    store: s::DB;
                    type: %s;
                    specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID,NAME,PRICE\\n1,a,1.50\\n2,O\\'Brien,---null---\\n3,---null---,3.25\\n'; };
                    auth: DefaultH2;
                }
                ###Runtime
                Runtime s::RT { mappings: [s::M]; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """.formatted(type);
    }

    private static final String RELATION = "|#>{s::DB.T}#->select(~[ID, NAME, PRICE])->sort(~ID->ascending())";
    private static final String NONE = "|#>{s::DB.T}#->filter(r|$r.ID > 99)->select(~[ID, NAME])";
    private static final String PROJECT = "|s::Item.all()->project(~[name: i|$i.name, price: i|$i.price])"
            + "->sort(~name->ascending())";
    private static final String GRAPH = "|s::Item.all()->graphFetch(#{s::Item{id, name, price}}#)"
            + "->serialize(#{s::Item{id, name, price}}#)";

    @Test
    void aRelationsPlanAnswersAsTodaysPaths_everyOutput() throws Exception {
        for (DatabaseType type : List.of(DatabaseType.DuckDB, DatabaseType.H2)) {
            for (String query : List.of(RELATION, NONE, PROJECT)) {
                for (TypedQuery.Output output : TypedQuery.Output.values()) {
                    assertAnswersAsToday(type, query, output);
                }
            }
        }
    }

    @Test
    void aGraphFetchsPlanAnswersAsTodaysPaths_inJson() throws Exception {
        for (DatabaseType type : List.of(DatabaseType.DuckDB, DatabaseType.H2)) {
            assertAnswersAsToday(type, GRAPH, TypedQuery.Output.JSON);
            assertAnswersAsToday(type, GRAPH, TypedQuery.Output.STREAMED_JSON);
            var refused = assertThrows(com.legend.error.NotImplementedException.class,
                    () -> plan(type, GRAPH, TypedQuery.Output.CSV));
            assertTrue(refused.getMessage().contains("graph results have no CSV wire"), refused.getMessage());
        }
    }

    @Test
    void theTargetIsWhole_itsDatabaseVersionsSessionAndSetup() {
        ExecutionPlan.Target duck = target(plan(DatabaseType.DuckDB, RELATION, TypedQuery.Output.JSON));
        assertEquals("s::Conn", ((ExecutionPlan.Database.Declared) duck.database()).connection().qualifiedName());
        assertEquals(new ExecutionPlan.Servers.Every(), duck.servers());
        assertEquals(List.of("SET TimeZone='UTC'"), duck.session());
        // DuckDB's rows go to its bulk loader: the create, the rows, the copy and the drop
        ExecutionPlan.SetupStep.Rows rows = (ExecutionPlan.SetupStep.Rows) duck.setup().get(duck.setup().size() - 1);
        assertEquals(List.of(List.of("1", "a", "1.50"), java.util.Arrays.asList("2", "O'Brien", null),
                java.util.Arrays.asList("3", null, "3.25")), rows.rows());

        ExecutionPlan.Target h2 = target(plan(DatabaseType.H2, RELATION, TypedQuery.Output.JSON));
        assertEquals(new ExecutionPlan.Servers.Versions(List.of("2.1", "2.2")), h2.servers());
        assertEquals(List.of(), h2.session());
        // H2's rows are one INSERT
        assertInstanceOf(ExecutionPlan.SetupStep.Statement.class, h2.setup().get(h2.setup().size() - 1));
        assertTrue(h2.setup().stream().noneMatch(s -> s instanceof ExecutionPlan.SetupStep.Rows), h2.setup().toString());
    }

    @Test
    void aRuntimeOfModelDataRunsOnThePlatformsEngine() throws Exception {
        String model = """
                Class m::Raw { name: String[1]; }
                Class m::P { name: String[1]; }
                ###Mapping
                Mapping m::MM ( *m::P: Pure { ~src m::Raw name: $src.name } )
                ###Runtime
                Runtime m::RT { mappings: [m::MM]; connections: [ ModelStore: [ json: #{ JsonModelConnection {
                    class: m::Raw; url: 'data:application/json,[{"name":"b"},{"name":"a"}]'; } }# ] ]; }
                """;
        String query = "|m::P.all()->project(~[name: p|$p.name])->sort(~name->ascending())";
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(model), query)
                .executionPlan("m::RT", TypedQuery.Output.JSON);
        ExecutionPlan.Target t = target(plan);
        assertEquals(new ExecutionPlan.Database.Platform(Databases.PLATFORM), t.database());
        assertEquals(List.of(), t.setup());
        try (Connection today = DriverManager.getConnection("jdbc:duckdb:");
             Connection planned = DriverManager.getConnection("jdbc:duckdb:")) {
            StringWriter out = new StringWriter();
            Execution.executeWire(model, query, "m::RT", today, WireRender.Format.JSON, out);
            assertEquals(out.toString(), run(plan, planned));
        }
    }

    @Test
    void aRuntimeOfTwoDifferentConnectionsIsRefusedWhenThePlanIsMade() {
        // a second store with a connection of its own, another database of the same type: the query reads only the
        // first store, but the runtime binds both, and a query runs on one session (the server refuses it as it opens)
        String model = model("H2").replace("""
                ###Runtime
                Runtime s::RT { mappings: [s::M]; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """, """
                RelationalDatabaseConnection s::Other { store: s::DB2; type: H2; specification: LocalH2 { }; auth: DefaultH2; }
                ###Relational
                Database s::DB2 ( Table U ( ID INTEGER PRIMARY KEY ) )
                ###Runtime
                Runtime s::RT { mappings: [s::M]; connections: [ s::DB: [ c1: s::Conn ], s::DB2: [ c2: s::Other ] ]; }
                """);
        assertTrue(model.contains("c2: s::Other"), model);
        var refused = assertThrows(com.legend.error.NotImplementedException.class,
                () -> Compiler.query(Compiler.compileModel(model), RELATION).executionPlan("s::RT", TypedQuery.Output.JSON));
        assertTrue(refused.getMessage().contains("different connections: a query runs on one session"),
                refused.getMessage());
    }

    @Test
    void aQueryWithParametersIsRefusedByName_untilItsSlotsAreBound() {
        var refused = assertThrows(com.legend.error.NotImplementedException.class, () -> Compiler.query(
                Compiler.compileModel(model("DuckDB")), "{n: Integer[1]|#>{s::DB.T}#->filter(r|$r.ID > $n)}")
                .executionPlan("s::RT", TypedQuery.Output.JSON));
        assertTrue(refused.getMessage().contains("the plan of a query with parameters"), refused.getMessage());
    }

    // ---------------------------------------------------------------------------------------------------------------

    private static ExecutionPlan plan(DatabaseType type, String query, TypedQuery.Output output) {
        return Compiler.query(Compiler.compileModel(model(type.name())), query).executionPlan("s::RT", output);
    }

    private static ExecutionPlan.Target target(ExecutionPlan plan) {
        return ((ExecutionPlan.TextResult) plan.root()).sql().target();
    }

    private static void assertAnswersAsToday(DatabaseType type, String query, TypedQuery.Output output)
            throws Exception {
        String model = model(type.name());
        String what = type + " " + output + " " + query;
        ExecutionPlan plan = plan(type, query, output);
        try (Connection today = fresh(type); Connection planned = fresh(type)) {
            var ctx = Compiler.compileModel(model);
            var dialect = Databases.dialect(type);
            com.legend.exec.SetupRunner.run(com.legend.setup.CsvSeed.declaredSteps("s::RT", ctx, dialect), today,
                    dialect, null);
            StringWriter out = new StringWriter();
            switch (output) {
                case CSV -> Execution.executeWire(model, query, "s::RT", today, WireRender.Format.CSV, out);
                case JSON -> Execution.executeWire(model, query, "s::RT", today, WireRender.Format.JSON, out);
                case STREAMED_JSON -> Execution.executeStreaming(model, query, "s::RT", today, out);
            }
            assertEquals(out.toString(), run(plan, planned), what);
        }
    }

    private static Connection fresh(DatabaseType type) throws SQLException {
        return switch (type) {
            case DuckDB -> DriverManager.getConnection("jdbc:duckdb:");
            case H2 -> DriverManager.getConnection("jdbc:h2:mem:planmaker" + H2_NAMES.incrementAndGet()
                    + com.legend.exec.H2Settings.SETTINGS);
            default -> throw new IllegalArgumentException(type.name());
        };
    }

    /** {@code plan} run on {@code c} as its steps describe themselves: the session statements, the setup (a rows step:
     *  its staging table created, the cells inserted as text, copied, dropped), then the statement's text. */
    public static String run(ExecutionPlan plan, Connection c) throws SQLException {
        ExecutionPlan.TextResult text = (ExecutionPlan.TextResult) plan.root();
        ExecutionPlan.Target target = text.sql().target();
        try (Statement st = c.createStatement()) {
            for (String s : target.session()) {
                st.execute(s);
            }
            for (ExecutionPlan.SetupStep step : target.setup()) {
                switch (step) {
                    case ExecutionPlan.SetupStep.Statement s -> st.execute(s.sql());
                    case ExecutionPlan.SetupStep.Rows r -> {
                        st.execute(r.createStaging());
                        String marks = String.join(", ", java.util.Collections.nCopies(r.rows().get(0).size(), "?"));
                        try (PreparedStatement insert = c.prepareStatement(
                                "insert into " + r.stagingTable() + " values (" + marks + ")")) {
                            for (List<String> row : r.rows()) {
                                for (int i = 0; i < row.size(); i++) {
                                    insert.setString(i + 1, row.get(i));
                                }
                                insert.execute();
                            }
                        }
                        st.execute(r.copy());
                        st.execute(r.dropStaging());
                    }
                }
            }
            assertEquals(List.of(), text.sql().slots());
            try (ResultSet rs = st.executeQuery(text.sql().statement())) {
                return switch (text.format()) {
                    case CSV, JSON -> {
                        assertTrue(rs.next(), "the database writes the whole text as one row");
                        yield rs.getString(1);
                    }
                    case JSON_PER_ROW -> {
                        List<String> rows = new ArrayList<>();
                        while (rs.next()) {
                            rows.add(rs.getString(1));
                        }
                        yield "[" + String.join(",", rows) + "]";
                    }
                };
            }
        }
    }
}
