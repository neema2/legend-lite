package com.legend.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testing.EmbeddedPostgres;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The server's Postgres arm ({@link ConnectionResolver}: a {@code type: Postgres} connection with a static
 * datasource) opens a real Postgres through the product's own driver set ({@code //core:drivers}). Until
 * 2026-10-03 that set held no Postgres driver, so the arm failed with "No suitable driver" in the server
 * and in its deploy jar (Bazel workplan P0-08). Its own target ({@code //core:postgres_arm_test}): it
 * needs the embedded Postgres in its runfiles, which {@code //core:core_tests} does not carry.
 */
class PostgresArmTest {

    @Test
    @DisplayName("a Postgres connection resolves through the product path and runs a query")
    void thePostgresArmOpensARealPostgres() throws Exception {
        EmbeddedPostgres pg = EmbeddedPostgres.shared();
        // The arm sends no user (its connections authenticate as Test or NoAuth), so the driver logs in
        // as the operating-system account: give that account a login role on the test's own server.
        String account = System.getProperty("user.name");
        try (Connection admin = DriverManager.getConnection(pg.jdbcUrl("postgres"));
                PreparedStatement exists = admin.prepareStatement("SELECT 1 FROM pg_roles WHERE rolname = ?")) {
            exists.setString(1, account);
            try (ResultSet r = exists.executeQuery()) {
                if (!r.next()) {
                    try (Statement create = admin.createStatement()) {
                        create.execute("CREATE ROLE \"" + account.replace("\"", "\"\"") + "\" LOGIN");
                    }
                }
            }
        }

        String model = String.format(java.util.Locale.ROOT, """
                ###Relational
                Database store::PgDB ( Table T ( ID INTEGER PRIMARY KEY ) )

                ###Connection
                RelationalDatabaseConnection store::PgConn {
                    type: Postgres;
                    specification: Static { host: '127.0.0.1'; port: %d; name: 'postgres'; };
                    auth: Test;
                }

                ###Runtime
                Runtime test::PgRuntime {
                    mappings: [ ];
                    connections: [ store::PgDB: [ environment: store::PgConn ] ];
                }
                """, pg.port());

        try (ConnectionResolver.Lease lease = ConnectionResolver.lease(com.legend.Compiler.compileModel(model), "test::PgRuntime");
                Statement s = lease.connection().createStatement();
                ResultSet r = s.executeQuery("SELECT 41 + 1")) {
            assertTrue(lease.connection().getMetaData().getURL().startsWith("jdbc:postgresql://127.0.0.1:"),
                    lease.connection().getMetaData().getURL());
            assertTrue(r.next());
            assertEquals(42, r.getInt(1));
        }
    }

    @Test
    @DisplayName("a table and a schema named by reserved words seed and answer a query on Postgres (PARK-16)")
    void reservedNamesSeedAndAnswer() throws Exception {
        EmbeddedPostgres pg = EmbeddedPostgres.shared();
        try (Connection c = DriverManager.getConnection(pg.jdbcUrl("postgres"))) {
            com.legend.testcases.ReservedNames.seedsAndAnswers(com.legend.testcases.ReservedNames.model(
                    String.format(java.util.Locale.ROOT, "type: Postgres; specification: Static { host: '127.0.0.1';"
                            + " port: %d; name: 'postgres'; }; auth: Test;", pg.port())),
                    new com.legend.sql.dialect.Postgres(), c);
        }
    }

    /** A table of three rows on Postgres (com.legend.PlanMakerTest holds the DuckDB and H2 cases; the cases are
     *  com.legend.testcases.PlanCases). A Postgres
     *  connection declares no test data, so the test seeds the table and the plan's setup is empty. */
    private static final String PLANNED = """
            Class s::Item { id: Integer[1]; name: String[0..1]; price: Decimal[0..1]; }
            ###Relational
            Database s::DB ( Table PLAN_T ( ID INTEGER PRIMARY KEY, NAME VARCHAR(20), PRICE DECIMAL(10,2) ) )
            ###Mapping
            Mapping s::M ( *s::Item: Relational { ~mainTable [s::DB] PLAN_T
                id: [s::DB] PLAN_T.ID, name: [s::DB] PLAN_T.NAME, price: [s::DB] PLAN_T.PRICE } )
            ###Connection
            RelationalDatabaseConnection s::Conn {
                store: s::DB; type: Postgres;
                specification: Static { host: '127.0.0.1'; port: %d; name: 'postgres'; }; auth: Test; }
            ###Runtime
            Runtime s::RT { mappings: [s::M]; connections: [ s::DB: [ c1: s::Conn ] ]; }
            """;

    @Test
    @DisplayName("a query's plan answers on Postgres exactly as today's wire and streaming paths do")
    void aPlanAnswersAsTodaysPaths() throws Exception {
        EmbeddedPostgres pg = EmbeddedPostgres.shared();
        String model = String.format(java.util.Locale.ROOT, PLANNED, pg.port());
        var ctx = com.legend.Compiler.compileModel(model);
        try (Connection c = DriverManager.getConnection(pg.jdbcUrl("postgres"))) {
            try (Statement s = c.createStatement()) {
                for (String sql : com.legend.setup.CsvSeed.sqls("default\nPLAN_T\nID,NAME,PRICE\n1,a,1.50\n"
                        + "2,O'Brien,---null---\n3,---null---,3.25\n", "s::DB", ctx,
                        new com.legend.sql.dialect.Postgres())) {
                    s.execute(sql);
                }
            }
            for (String query : java.util.List.of("|#>{s::DB.PLAN_T}#->select(~[ID, NAME, PRICE])->sort(~ID->ascending())",
                    "|s::Item.all()->project(~[name: i|$i.name, price: i|$i.price])->sort(~name->ascending())",
                    "|s::Item.all()->graphFetch(#{s::Item{id, name, price}}#)->serialize(#{s::Item{id, name, price}}#)")) {
                for (com.legend.TypedQuery.Output output : com.legend.TypedQuery.Output.values()) {
                    boolean graph = query.contains("graphFetch");
                    if (graph && output == com.legend.TypedQuery.Output.CSV) {
                        continue;
                    }
                    com.legend.executionplan.ExecutionPlan plan = com.legend.Compiler.query(ctx, query)
                            .executionPlan("s::RT", output);
                    var target = ((com.legend.executionplan.ExecutionPlan.TextResult) plan.root()).sql().target();
                    assertEquals(new com.legend.executionplan.ExecutionPlan.Servers.Every(), target.servers());
                    assertEquals(java.util.List.of("SET TimeZone='UTC'"), target.session());
                    assertEquals(java.util.List.of(), target.setup());
                    java.io.StringWriter today = new java.io.StringWriter();
                    switch (output) {
                        case CSV -> com.legend.Execution.executeWire(model, query, "s::RT", c,
                                com.legend.lowering.WireRender.Format.CSV, today);
                        case JSON -> com.legend.Execution.executeWire(model, query, "s::RT", c,
                                com.legend.lowering.WireRender.Format.JSON, today);
                        case STREAMED_JSON -> com.legend.Execution.executeStreaming(model, query, "s::RT", c, today);
                    }
                    assertEquals(today.toString(), com.legend.testcases.PlanCases.run(plan, c), output + " " + query);
                }
            }
            // a query's scalar parameters: bound where they are written, answering as the query with their values does
            // (Postgres types a bare placeholder by the bound value, every type but a date-time, which is cast to the
            // type of its literal and passed as its text, cut to six digits as the literal is)
            for (com.legend.testcases.PlanCases.Parameterised q : com.legend.testcases.PlanCases.scalars("PLAN_T")) {
                assertPlanAnswersAsToday(model, ctx, q, c);
            }
            // an enumeration parameter, compared through a value table of the column's codes
            String enums = com.legend.testcases.PlanCases.enumModel(String.format(java.util.Locale.ROOT, "type: Postgres;"
                    + " specification: Static { host: '127.0.0.1'; port: %d; name: 'postgres'; }; auth: Test;",
                    pg.port()), "PLAN_E");
            var enumCtx = com.legend.Compiler.compileModel(enums);
            try (Statement s = c.createStatement()) {
                for (String sql : com.legend.setup.CsvSeed.sqls(com.legend.testcases.PlanCases.enumRows("PLAN_E"), "s::DB",
                        enumCtx, new com.legend.sql.dialect.Postgres())) {
                    s.execute(sql);
                }
            }
            for (com.legend.testcases.PlanCases.Parameterised q : com.legend.testcases.PlanCases.enumerations()) {
                assertPlanAnswersAsToday(enums, enumCtx, q, c);
            }
            // a list parameter, bound as one array: over the table, and an enumeration list through the value table
            for (com.legend.testcases.PlanCases.Parameterised q : com.legend.testcases.PlanCases.lists("PLAN_T")) {
                assertPlanAnswersAsToday(model, ctx, q, c);
            }
            assertPlanAnswersAsToday(enums, enumCtx, new com.legend.testcases.PlanCases.Parameterised("sts: s::Status[*]",
                    "let sts = [s::Status.ACTIVE];", "s::Acct.all()->filter(a|$a.status->in($sts))->project(~[id: a|$a.id])"
                            + "->sort(~id->ascending())", java.util.Map.of("sts", java.util.List.of("ACTIVE"))), c);
            // an optional parameter: with its value as the query with a let of it, with none as the query written []
            for (com.legend.testcases.PlanCases.OptionalCase q : com.legend.testcases.PlanCases.optionals("PLAN_T")) {
                for (com.legend.TypedQuery.Output output : com.legend.TypedQuery.Output.values()) {
                    com.legend.executionplan.ExecutionPlan plan = com.legend.Compiler.query(ctx, q.withParameter())
                            .executionPlan("s::RT", output);
                    for (boolean present : new boolean[]{true, false}) {
                        String query = present ? q.withValue() : q.withNone();
                        java.io.StringWriter today = new java.io.StringWriter();
                        switch (output) {
                            case CSV -> com.legend.Execution.executeWire(model, query, "s::RT", c,
                                    com.legend.lowering.WireRender.Format.CSV, today);
                            case JSON -> com.legend.Execution.executeWire(model, query, "s::RT", c,
                                    com.legend.lowering.WireRender.Format.JSON, today);
                            case STREAMED_JSON -> com.legend.Execution.executeStreaming(model, query, "s::RT", c,
                                    today);
                        }
                        assertEquals(today.toString(), com.legend.testcases.PlanCases.run(plan, c,
                                java.util.Collections.singletonMap("x", present ? q.value() : null)),
                                output + " " + query);
                    }
                }
            }
        }
    }

    /** {@code q}'s plan, run with its values, answers on {@code c} as today's path answers the query with a let of each,
     *  every output. */
    private static void assertPlanAnswersAsToday(String model, com.legend.compiler.element.ModelContext ctx,
            com.legend.testcases.PlanCases.Parameterised q, Connection c) throws Exception {
        for (com.legend.TypedQuery.Output output : com.legend.TypedQuery.Output.values()) {
            com.legend.executionplan.ExecutionPlan plan = com.legend.Compiler.query(ctx, q.withParameters())
                    .executionPlan("s::RT", output);
            java.io.StringWriter today = new java.io.StringWriter();
            switch (output) {
                case CSV -> com.legend.Execution.executeWire(model, q.withLets(), "s::RT", c,
                        com.legend.lowering.WireRender.Format.CSV, today);
                case JSON -> com.legend.Execution.executeWire(model, q.withLets(), "s::RT", c,
                        com.legend.lowering.WireRender.Format.JSON, today);
                case STREAMED_JSON -> com.legend.Execution.executeStreaming(model, q.withLets(), "s::RT", c, today);
            }
            assertEquals(today.toString(), com.legend.testcases.PlanCases.run(plan, c, q.values()),
                    output + " " + q.withParameters());
        }
    }
}
