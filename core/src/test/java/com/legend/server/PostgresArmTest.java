package com.legend.server;

import com.legend.testing.Lean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import java.sql.Connection;
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

    /** Postgres's test backend: this JVM's embedded server, its sessions, the connection a model declares for them. */
    private static final TestBackend POSTGRES = TestBackends.of(DatabaseType.Postgres);

    /** The arm's model: one table on Postgres's backend, its connection the backend's. */
    private static final String ARM_MODEL = """
            ###Relational
            Database store::PgDB ( Table T ( ID INTEGER PRIMARY KEY ) )

            ###Connection
            RelationalDatabaseConnection store::PgConn {
                %s
            }

            ###Runtime
            Runtime test::PgRuntime {
                mappings: [ ];
                connections: [ store::PgDB: [ environment: store::PgConn ] ];
            }
            """;

    /** The arm sends no user (its connections authenticate as Test or NoAuth), so the driver logs in as the
     *  operating-system account: give that account a login role on the test's own server. */
    private static void loginRole() throws Exception {
        String account = System.getProperty("user.name");
        try (Connection admin = POSTGRES.open();
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
    }

    @Test
    @DisplayName("a Postgres connection resolves through the product path and runs a query")
    void thePostgresArmOpensARealPostgres() throws Exception {
        loginRole();
        String model = ARM_MODEL.formatted(POSTGRES.connectionBody());

        // the server's session for the runtime (D70: a session, no raw connection): on Postgres, as the resolver's
        // check against the declared database proves, and on the arm's OWN server, as the port the session reports
        // through the seam's statements proves (the URL read from the raw connection until D70)
        try (com.legend.exec.Sessions.Session session =
                com.legend.exec.PlanSessions.PROJECT_DATABASES.open(com.legend.planner.PlanMaker.runtimeTarget(
                        com.legend.Compiler.compileModel(model), "test::PgRuntime"));
                var run = com.legend.base.RunScope.enter()) {
            // the session's check and its statements are a run's (THE WALL's rule 3, 2026-11-06)
            com.legend.exec.Sessions.check(session, com.legend.planner.ConnectionPlans.type(POSTGRES.type()));
            java.util.List<java.util.List<Object>> rows = new java.util.ArrayList<>();
            // the statements check the session against a plan target before any typed read (the driver's facts
            // are the database's)
            com.legend.exec.Statements statements = session.statements();
            statements.check(new com.legend.executionplan.ExecutionPlan.Target(
                    new com.legend.executionplan.ExecutionPlan.Database.Platform(com.legend.planner.ConnectionPlans.type(POSTGRES.type()), null),
                    java.util.List.of(),
                    java.util.List.of(), null));
            statements.rows("SELECT inet_server_port() AS port, 41 + 1 AS answer", java.util.List.of(),
                    java.util.List.of(), java.util.List.of(
                            new com.legend.executionplan.ExecutionPlan.Column("port", "Integer",
                                    new com.legend.executionplan.ExecutionPlan.Multiplicity(1, 1), null, null, null),
                            new com.legend.executionplan.ExecutionPlan.Column("answer", "Integer",
                                    new com.legend.executionplan.ExecutionPlan.Multiplicity(1, 1), null, null, null)),
                    rows::add);
            assertEquals(1, rows.size());
            assertEquals(java.util.List.of(String.valueOf(com.legend.testing.EmbeddedPostgres.shared().port()), "42"),
                    rows.get(0).stream().map(String::valueOf).toList());
        }
    }

    @Test
    @DisplayName("a date before Christ answers with its era")
    void aDateBeforeChristKeepsItsEra() throws Exception {
        // the executor read a DATE through java.sql.Date, which drops a BC date's era: 44 BC came back as 44 AD
        // (2026-10-17, the #SQL{} island's regression-suite corpus); it re-fetches the cell as java.time
        loginRole();
        var result = Lean.service(ARM_MODEL.formatted(POSTGRES.connectionBody()),
                "#SQL{select make_date(-44, 3, 15) as d}#", "test::PgRuntime");
        Object d = result.rows().get(0).get(0);
        assertTrue(d instanceof com.legend.values.PureDateLiteral.StrictDate date && date.year() == -43
                && date.month() == 3 && date.day() == 15, String.valueOf(d));
    }

    @Test
    @DisplayName("a table and a schema named by reserved words seed and answer a query on Postgres (PARK-16)")
    void reservedNamesSeedAndAnswer() throws Exception {
        try (Connection c = POSTGRES.open()) {
            com.legend.testcases.ReservedNames.seedsAndAnswers(
                    com.legend.testcases.ReservedNames.model(POSTGRES.connectionBody()), POSTGRES.dialect(), c);
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
                store: s::DB; %s }
            ###Runtime
            Runtime s::RT { mappings: [s::M]; connections: [ s::DB: [ c1: s::Conn ] ]; }
            """;

    @Test
    @DisplayName("a query's plan answers on Postgres exactly as today's wire and streaming paths do")
    void aPlanAnswersAsTodaysPaths() throws Exception {
        String model = PLANNED.formatted(POSTGRES.connectionBody());
        var ctx = com.legend.Compiler.compileModel(model);
        try (Connection c = POSTGRES.open()) {
            try (Statement s = c.createStatement()) {
                for (String sql : com.legend.setup.CsvSeed.sqls("default\nPLAN_T\nID,NAME,PRICE\n1,a,1.50\n"
                        + "2,O'Brien,---null---\n3,---null---,3.25\n", "s::DB", ctx,
                        POSTGRES.dialect())) {
                    s.execute(sql);
                }
            }
            for (String query : java.util.List.of("|#>{s::DB.PLAN_T}#->select(~[ID, NAME, PRICE])->sort(~ID->ascending())",
                    "|s::Item.all()->project(~[name: i|$i.name, price: i|$i.price])->sort(~name->ascending())",
                    "|s::Item.all()->graphFetch(#{s::Item{id, name, price}}#)->serialize(#{s::Item{id, name, price}}#)")) {
                for (com.legend.testcases.PlanCases.Written output : com.legend.testcases.PlanCases.Written.values()) {
                    boolean graph = query.contains("graphFetch");
                    // a graph result has no CSV (PlanMakerTest pins the refusal)
                    if (graph && output == com.legend.testcases.PlanCases.Written.CSV) {
                        continue;
                    }
                    com.legend.executionplan.ExecutionPlan plan = plan(ctx, query);
                    var target = plan.answerSql().target();
                    assertEquals(java.util.List.of("SET TimeZone='UTC'"), target.session());
                    assertEquals(java.util.List.of(), target.loads());
                    assertEquals(com.legend.testcases.PlanCases.today(model, query, "s::RT", c, output),
                            com.legend.testcases.PlanCases.write(plan, c, java.util.Map.of(), output::writer),
                            output + " " + query);
                }
            }
            // a query's scalar parameters: bound where they are written, answering as the query with their values does
            // (Postgres types a bare placeholder by the bound value, every type but a date or date-time, which is cast to
            // its declared type's TIMESTAMP and passed as its text, every digit, a value finer than the six it keeps
            // raised by the statement as the literal is refused at planning: S181, A7; D1, P4) -- but a
            // Date parameter given a StrictDate value, which answers a midnight date-time as upstream's TIMESTAMP'${d}'
            // does, where the StrictDate literal answers the date (both paths upstream's: the total wall's M2, R3)
            for (com.legend.testcases.PlanCases.Parameterised q : com.legend.testcases.PlanCases.scalars("PLAN_T")) {
                // (a Date parameter only compared, never answered, answers as its query does: the calendar's end)
                if (q.parameters().equals("d: Date[1]") && q.values().get("d") instanceof java.time.LocalDate
                        && q.body().contains("~d: r|$d")) {
                    for (com.legend.testcases.PlanCases.Written output : com.legend.testcases.PlanCases.Written.values()) {
                        String written = com.legend.testcases.PlanCases.write(plan(ctx, q.withParameters()), c, q.values(),
                                output::writer);
                        assertTrue(written.contains("2024-01-02T00:00:00"), output + ": " + written);
                    }
                    continue;
                }
                // a value finer than the microsecond: its literal refused at planning, the parameter by the statement's
                // own raise -- never cut, never rounded (S181, the user's ruling of 2026-11-10, A7)
                if (q.values().get("t") instanceof java.time.LocalDateTime t && t.getNano() % 1000 != 0) {
                    var literal = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                            () -> plan(ctx, q.withLets()));
                    assertTrue(String.valueOf(literal.getMessage()).contains("fraction digits; Postgres keeps 6"),
                            literal.getMessage());
                    var parameter = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                            () -> com.legend.testcases.PlanCases.write(plan(ctx, q.withParameters()), c, q.values(),
                                    com.legend.testcases.PlanCases.Written.JSON::writer));
                    assertTrue(String.valueOf(parameter.getMessage()).contains("the DateTime parameter 't' has more"
                            + " fraction digits than Postgres keeps (6): refused, never cut (S181)"),
                            parameter.getMessage());
                    continue;
                }
                assertPlanAnswersAsToday(model, ctx, q, c);
            }
            // the guard's ends: the last microsecond of a day answers on its own day; a half-microsecond past it, which
            // Postgres would round to the next day, is refused (S181, A7)
            String dt = "{t: DateTime[1]|#>{s::DB.PLAN_T}#->filter(r|$r.ID == 1)->extend(~t: r|$t)->select(~t)}";
            String last = com.legend.testcases.PlanCases.write(plan(ctx, dt), c, java.util.Map.of("t",
                    java.time.LocalDateTime.parse("2024-01-02T23:59:59.999999")),
                    com.legend.testcases.PlanCases.Written.JSON::writer);
            assertTrue(last.contains("2024-01-02T23:59:59.999999"), last);
            var rounded = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                    () -> com.legend.testcases.PlanCases.write(plan(ctx, dt), c, java.util.Map.of("t",
                            java.time.LocalDateTime.parse("2024-01-02T23:59:59.9999995")),
                            com.legend.testcases.PlanCases.Written.JSON::writer));
            assertTrue(String.valueOf(rounded.getMessage()).contains("refused, never cut (S181)"), rounded.getMessage());
            // an enumeration parameter, compared through a value table of the column's codes
            String enums = com.legend.testcases.PlanCases.enumModel(POSTGRES.connectionBody(), "PLAN_E");
            var enumCtx = com.legend.Compiler.compileModel(enums);
            try (Statement s = c.createStatement()) {
                for (String sql : com.legend.setup.CsvSeed.sqls(com.legend.testcases.PlanCases.enumRows("PLAN_E"), "s::DB",
                        enumCtx, POSTGRES.dialect())) {
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
                for (com.legend.testcases.PlanCases.Written output : com.legend.testcases.PlanCases.Written.values()) {
                    com.legend.executionplan.ExecutionPlan plan = plan(ctx, q.withParameter());
                    for (boolean present : new boolean[]{true, false}) {
                        String query = present ? q.withValue() : q.withNone();
                        assertEquals(com.legend.testcases.PlanCases.today(model, query, "s::RT", c, output),
                                com.legend.testcases.PlanCases.write(plan, c,
                                        java.util.Collections.singletonMap("x", present ? q.value() : null),
                                        output::writer), output + " " + query);
                    }
                }
            }
        }
    }

    /** {@code q}'s plan, run with its values, answers on {@code c} as today's path answers the query with a let of each,
     *  every output. */
    private static void assertPlanAnswersAsToday(String model, com.legend.compiler.element.ModelContext ctx,
            com.legend.testcases.PlanCases.Parameterised q, Connection c) throws Exception {
        for (com.legend.testcases.PlanCases.Written output : com.legend.testcases.PlanCases.Written.values()) {
            com.legend.executionplan.ExecutionPlan plan = plan(ctx, q.withParameters());
            assertEquals(com.legend.testcases.PlanCases.today(model, q.withLets(), "s::RT", c, output),
                    com.legend.testcases.PlanCases.write(plan, c, q.values(), output::writer),
                    output + " " + q.withParameters());
        }
    }

    /** {@code query}'s typed plan over {@code ctx}. */
    private static com.legend.executionplan.ExecutionPlan plan(com.legend.compiler.element.ModelContext ctx,
            String query) {
        return com.legend.Compiler.query(ctx, query).executionPlan("s::RT");
    }

    /** The test-data generator's seed over a text key, on Postgres (the B2d.3 audit's M4). */
    private static final String SEED = """
            Class seed::Code { n: Integer[0..1]; }
            ###Relational
            Database seed::DB ( Table SEED_CODES ( CODE VARCHAR(20) PRIMARY KEY, N INTEGER ) )
            ###Mapping
            Mapping seed::M ( seed::Code: Relational { ~mainTable [seed::DB] SEED_CODES n: [seed::DB] SEED_CODES.N } )
            ###Connection
            RelationalDatabaseConnection seed::Conn { store: seed::DB; %s }
            ###Runtime
            Runtime seed::RT { mappings: [seed::M]; connections: [ seed::DB: [ c1: seed::Conn ] ]; }
            """;

    /**
     * The seed's text keys in code-point order on Postgres whatever the column's collation (the B2d.3 audit's M4,
     * 2026-11-12; the design's §3.1, ruled with R4; register S198's note): the keys of a column collated by ICU's root
     * order ({@code "und-x-icu"}, which sorts {@code a b B Z}) are chosen, written in the seed string and answered by
     * the seed route as {@code B, Z, a, b} -- the dialect's {@code COLLATE "C"}. This server's initdb runs
     * {@code --no-locale}, so its default collation is binary already: only a column's own collation shows the
     * difference, which this table declares.
     */
    @Test
    @DisplayName("the test-data generator's seed orders a text key by its code points on Postgres (COLLATE \"C\")")
    void theSeedOrdersATextKeyByItsCodePoints() throws Exception {
        var ctx = com.legend.Compiler.compileModel(SEED.formatted(POSTGRES.connectionBody()));
        try (Connection c = POSTGRES.open()) {
            try (Statement s = c.createStatement()) {
                for (String sql : com.legend.setup.CsvSeed.sqls("default\nSEED_CODES\nCODE,N\nb,1\nB,2\na,3\nZ,4\n",
                        "seed::DB", ctx, POSTGRES.dialect())) {
                    s.execute(sql);
                }
                // the one statement no model or CSV can say: the column's own collation
                s.execute("ALTER TABLE SEED_CODES ALTER COLUMN CODE TYPE VARCHAR(20) COLLATE \"und-x-icu\"");
            }
            java.util.List<String> binary = java.util.List.of("B", "Z", "a", "b");
            // the premise: the column's own order is not the code points'
            org.junit.jupiter.api.Assertions.assertNotEquals(binary, column(c, "SELECT CODE FROM SEED_CODES ORDER BY CODE"));
            com.legend.protocol.spec.LambdaFunction query = (com.legend.protocol.spec.LambdaFunction)
                    com.legend.compiler.NameResolver.resolveQuery(com.legend.Compiler.parseQuery(
                            "|seed::Code.all()->project([x|$x.n], ['n'])"));
            com.legend.testdatagen.TestDataPlanner.Seed seed = com.legend.testdatagen.TestDataPlanner.seed(ctx, query,
                    "seed::M", false, q -> com.legend.Compiler.query(ctx, com.legend.testdatagen.TestDataPlanner
                            .underMapping(q, "seed::M", "seed::RT", com.legend.Compiler::parseQuery)).lower("seed::RT", false)
                    .plan());
            assertEquals(binary, column(c, POSTGRES.dialect().render(seed.rows())));
            String line = "       meta::relational::testDataGeneration::createRowIdentifier(['CODE'], ['%s'])";
            assertEquals(java.util.List.of("\nmeta::relational::testDataGeneration::createTableRowIdentifiers(seed::DB,"
                    + " 'default', 'SEED_CODES', [\n" + String.join(",\n", line.formatted("B"), line.formatted("Z"),
                            line.formatted("a"), line.formatted("b")) + "\n  ])\n"), column(c, POSTGRES.dialect().render(
                                    com.legend.testdatagen.TestDataPlanner.seedString(ctx, seed))));
            String json = column(c, POSTGRES.dialect().render(com.legend.testdatagen.TestDataPlanner.seedJson(ctx, seed)))
                    .get(0);
            assertTrue(json.replace(" ", "").endsWith("\"rowIdentifiers\":["
                    + "{\"columnValuePairs\":[{\"name\":\"CODE\",\"value\":\"B\"}]},"
                    + "{\"columnValuePairs\":[{\"name\":\"CODE\",\"value\":\"Z\"}]},"
                    + "{\"columnValuePairs\":[{\"name\":\"CODE\",\"value\":\"a\"}]},"
                    + "{\"columnValuePairs\":[{\"name\":\"CODE\",\"value\":\"b\"}]}]}"), json);
        }
    }

    /** {@code sql}'s first column, each row's text. */
    private static java.util.List<String> column(Connection c, String sql) throws Exception {
        java.util.List<String> out = new java.util.ArrayList<>();
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) {
            while (r.next()) {
                out.add(r.getString(1));
            }
        }
        return out;
    }
}
