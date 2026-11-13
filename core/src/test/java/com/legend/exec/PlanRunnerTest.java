// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.database.Databases;
import com.legend.executionplan.ExecutionPlan;
import com.legend.model.AuthenticationSpec;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.model.ConnectionSpecification;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The runner (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 3): it takes each value as its Pure type's Java value,
 * refuses what its parameter does not take in its own words, every problem at once; binds each slot as the plan says,
 * writing a value's type where the statement leaves it to the value; and gives out sessions by the plan's target
 * (decision A), on each in-process database. The plans' answers are {@code PlanMakerTest}'s and {@code PostgresArmTest}'s,
 * which run every case through it; what one database's sessions do (H2's type holes and versions, DuckDB's bulk
 * loader) is that database's test, {@code PlanRunner<Db>Test} (the dialect program's Phase 6, 2026-10-16).
 */
class PlanRunnerTest {

    static ExecutionPlan.Parameter declared(String name, String type, int lower, Integer upper,
            String... enumValues) {
        return new ExecutionPlan.Parameter(name, type, new ExecutionPlan.Multiplicity(lower, upper), List.of(enumValues));
    }

    private static Object one(String type, Object value) {
        var checked = PlanParameters.check(List.of(declared("p", type, 1, 1)), Map.of("p", value)).get("p");
        return ((PlanParameters.One) checked).value();
    }

    private static String refusal(ExecutionPlan.Parameter p, Object value) {
        return assertThrows(IllegalArgumentException.class,
                () -> PlanParameters.check(List.of(p), Map.of(p.name(), value))).getMessage();
    }

    private static String invalid(String... problems) {
        return "Invalid provided parameter(s): [" + String.join("; ", problems) + "]";
    }

    @Test
    void aValueOfItsTypesJavaType_isConvertedAsItsSlotBindsIt() {
        assertEquals(5L, one("Integer", 5L));
        assertEquals(true, one("Boolean", true));
        assertEquals("O'Brien", one("String", "O'Brien"));
        assertEquals(LocalDate.of(2020, 7, 14), one("StrictDate", LocalDate.of(2020, 7, 14)));
        assertEquals(LocalDateTime.of(2020, 7, 14, 15, 18, 23, 123_456_789),
                one("DateTime", LocalDateTime.of(2020, 7, 14, 15, 18, 23, 123_456_789)));
        assertEquals(LocalDate.of(2020, 7, 14), one("Date", LocalDate.of(2020, 7, 14)));
        assertEquals(LocalDateTime.of(2020, 7, 14, 15, 18), one("Date", LocalDateTime.of(2020, 7, 14, 15, 18)));
        assertEquals("FULL_TIME", one(declared("type", "test::EmployeeType", 1, 1, "CONTRACT", "FULL_TIME"),
                "FULL_TIME"));
        // a Decimal as its literal is written: its own digits, a negative scale's as plain digits
        assertEquals(new BigDecimal("2.50"), one("Decimal", new BigDecimal("2.50")));
        assertEquals(new BigDecimal("1000"), one("Decimal", new BigDecimal("1E+3")));
        // a Number: a Long, a Double (as a Float), a BigDecimal (as a Decimal)
        assertEquals(7L, one("Number", 7L));
        assertEquals(new BigDecimal("1.5"), one("Number", 1.5d));
        assertEquals(new BigDecimal("2.50"), one("Number", new BigDecimal("2.50")));
    }

    private static Object one(ExecutionPlan.Parameter p, Object value) {
        return ((PlanParameters.One) PlanParameters.check(List.of(p), Map.of(p.name(), value)).get(p.name())).value();
    }

    /** A Float binds as its literal is typed (the numeric charter's Rule 1): its plain digits as a decimal; at an
     *  extreme magnitude (at least 1e15, or below 1e-6) a double, as the literal is written in exponent form. */
    @Test
    void aFloatIsBoundAsItsLiteralIsTyped() {
        assertEquals(new BigDecimal("1.1"), one("Float", 1.1d));
        assertEquals(new BigDecimal("5.0"), one("Float", 5.0d));
        assertEquals(new BigDecimal("0.0"), one("Float", -0.0d));
        assertEquals(new BigDecimal("999999999999999.9"), one("Float", 999999999999999.9d));
        assertEquals(1e15d, one("Float", 1e15d));
        assertEquals(new BigDecimal("0.0000010"), one("Float", 1e-6d));
        assertEquals(9.99e-7d, one("Float", 9.99e-7d));
        assertEquals(Double.MAX_VALUE, one("Float", Double.MAX_VALUE));
    }

    @Test
    void aValueItsParameterDoesNotTake_isRefusedSayingWhatItTakes() {
        assertEquals(invalid("parameter 'p' (Integer[1]): given \"5\" (String): an Integer is a Long"),
                refusal(declared("p", "Integer", 1, 1), "5"));
        assertEquals(invalid("parameter 'p' (Integer[1]): given 5 (Integer): an Integer is a Long"),
                refusal(declared("p", "Integer", 1, 1), 5));
        assertEquals(invalid("parameter 'p' (Float[1]): given 1.5 (BigDecimal): a Float is a Double"),
                refusal(declared("p", "Float", 1, 1), new BigDecimal("1.5")));
        assertEquals(invalid("parameter 'p' (DateTime[1]): given 2020-07-14 (LocalDate): a DateTime is a"
                + " LocalDateTime (in UTC)"), refusal(declared("p", "DateTime", 1, 1), LocalDate.of(2020, 7, 14)));
        assertEquals(invalid("parameter 't' (test::EmployeeType[1]): \"CONTRCT\" is not a value of"
                + " test::EmployeeType [CONTRACT, FULL_TIME]"),
                refusal(declared("t", "test::EmployeeType", 1, 1, "CONTRACT", "FULL_TIME"), "CONTRCT"));
        // a Float that is not finite has no SQL literal
        for (double v : new double[] {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
            assertEquals(invalid("parameter 'p' (Float[1]): " + v + " is not finite: no SQL literal stands for it"),
                    refusal(declared("p", "Float", 1, 1), v));
        }
        // a date-time is passed as its text, which no database reads beyond the year 9999
        assertEquals(invalid("parameter 't' (DateTime[1]): +10000-01-01T00:00 is outside the years 1 to 9999: no"
                + " date-time text a database reads stands for it"),
                refusal(declared("t", "DateTime", 1, 1), LocalDateTime.of(10000, 1, 1, 0, 0)));
        // and a date alike, whether a StrictDate's or a Date's (the correctness re-audit's 4, 2026-11-09)
        assertEquals(invalid("parameter 'd' (StrictDate[1]): +10000-01-01 is outside the years 1 to 9999: no date text"
                + " a database reads stands for it"),
                refusal(declared("d", "StrictDate", 1, 1), java.time.LocalDate.of(10000, 1, 1)));
        assertEquals(invalid("parameter 'd' (Date[1]): -0001-01-01 is outside the years 1 to 9999: no date text a"
                + " database reads stands for it"),
                refusal(declared("d", "Date", 1, 1), java.time.LocalDate.of(-1, 1, 1)));
        // what no plan binds yet (the planner refuses it first; PARK-21)
        assertEquals(invalid("parameter 'b' (Byte[1]): a Byte value is not bound by a plan (PARK-21)"),
                refusal(declared("b", "Byte", 1, 1), 1L));
    }

    /** One value for a parameter of one, a list for a parameter of many, as many as it takes, none of them null; no
     *  value, or an empty list, is a required parameter's absence. */
    @Test
    void aValueMatchesItsParametersMultiplicity() {
        assertEquals(invalid("parameter 'p' (Integer[1]): takes one value, given a list of 2"),
                refusal(declared("p", "Integer", 1, 1), List.of(1L, 2L)));
        assertEquals(invalid("parameter 'p' (Integer[1]): takes one value, given a list of 1"),
                refusal(declared("p", "Integer", 1, 1), List.of(1L)));
        assertEquals(invalid("parameter 'ns' (Integer[*]): takes a list, given one value"),
                refusal(declared("ns", "Integer", 0, null), 1L));
        assertEquals(invalid("parameter 'ns' (Integer[1..2]): takes 1..2 values, given 3"),
                refusal(declared("ns", "Integer", 1, 2), List.of(1L, 2L, 3L)));
        assertEquals(invalid("parameter 'ns' (Integer[*]): a list holds no null"),
                refusal(declared("ns", "Integer", 0, null), Arrays.asList(1L, null)));
        assertEquals(invalid("parameter 'ns' (Integer[*]): given \"2\" (String): an Integer is a Long"),
                refusal(declared("ns", "Integer", 0, null), List.of(1L, "2")));
        Map<String, Object> none = new HashMap<>();
        none.put("p", null);
        assertEquals("Missing external parameter(s): p:Integer[1]", assertThrows(IllegalArgumentException.class,
                () -> PlanParameters.check(List.of(declared("p", "Integer", 1, 1)), none)).getMessage());
        assertEquals("Missing external parameter(s): ns:Integer[1..*]",
                refusal(declared("ns", "Integer", 1, null), List.of()));
    }

    /** Every problem is named at once: the missing values, then each value refused. */
    @Test
    void everyProblemIsNamedAtOnce() {
        String all = assertThrows(IllegalArgumentException.class, () -> PlanParameters.check(List.of(
                declared("a", "Integer", 1, 1), declared("b", "String", 1, null), declared("f", "Float", 1, 1),
                declared("e", "s::E", 1, 1, "A", "B")), Map.of("f", Double.NaN, "e", "X"))).getMessage();
        assertEquals("Missing external parameter(s): a:Integer[1], b:String[1..*]; " + invalid(
                "parameter 'f' (Float[1]): NaN is not finite: no SQL literal stands for it",
                "parameter 'e' (s::E[1]): \"X\" is not a value of s::E [A, B]"), all);
    }

    @Test
    void anOptionalValueMayBeAbsent_aListEmpty_andAValueForNoParameterIsIgnored() {
        var checked = PlanParameters.check(List.of(declared("o", "String", 0, 1), declared("ns", "Integer", 0, null)),
                Map.of("ns", List.of(1L, 2L, 3L), "stray", 9));
        assertEquals(new PlanParameters.None(), checked.get("o"));
        assertEquals(new PlanParameters.Many(List.of(1L, 2L, 3L)), checked.get("ns"));
        assertEquals(new PlanParameters.None(),
                PlanParameters.check(List.of(declared("ns", "Integer", 0, null)), Map.of("ns", List.of())).get("ns"));
    }

    /** A plan whose parameters fail is refused before any session is opened. */
    @Test
    void aPlanWhoseParametersFailOpensNoSession() {
        ExecutionPlan given = ExecutionPlan.reading(List.of(declared("n", "Integer", 1, 1)),
                counting(inMemory("s::C", Databases.PLATFORM, new ConnectionSpecification.InMemory()), "UNOPENED_T").root());
        var refused = assertThrows(IllegalArgumentException.class, () -> PlanRunner.rows(given, Map.of("n", "1"),
                com.legend.exec.PlanSessions.refusing("a session was opened"), java.util.UUID.randomUUID()));
        assertEquals(invalid("parameter 'n' (Integer[1]): given \"1\" (String): an Integer is a Long"),
                refused.getMessage());
    }

    /** A key cell's slot in each copy of a per-key region is the PLAN's own key slot, told apart by its type, its value
     *  the copy's key's text (the C final audit's J7, 2026-11-12: until then exec made a slot of its own for it, its
     *  type a VARCHAR exec wrote, told apart by an empty name). */
    @Test
    void anExpandedKeySlotIsThePlansOwn_itsValueTheKeysText() {
        String text = "SELECT X FROM T WHERE K IN (?)";
        int at = text.indexOf('?');
        ExecutionPlan.Slot key = new ExecutionPlan.Slot("", new ExecutionPlan.Binding.Key(0));
        ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(
                com.legend.planner.ConnectionPlans.type(Databases.PLATFORM), null), List.of(), List.of(), null,
                TestBackends.settings(Databases.PLATFORM), null);
        ExecutionPlan.Sql sql = new ExecutionPlan.Sql(text, List.of(key), target, null, List.of(
                new ExecutionPlan.Region(at, at + 1, "", ", ", null, 0, 1, List.of())));
        PlanRunner.Expanded expanded = PlanRunner.expand(sql, List.of(List.of("a"), List.of("b")), Map.of());
        assertEquals("SELECT X FROM T WHERE K IN (?, ?)", expanded.statement().text());
        assertEquals(List.of(key, key), expanded.slots());
        assertEquals(List.of(new PlanParameters.One("a"), new PlanParameters.One("b")), expanded.values());
    }

    /** A slot's null type is a JDBC type's name, or the plan is refused by name before any session is opened. */
    @Test
    void aSlotWhoseNullTypeIsNoJdbcTypeIsRefusedBeforeASessionIsOpened() {
        ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(com.legend.planner.ConnectionPlans.type(Databases.PLATFORM), null),
                List.of(), List.of(), null, null, null);
        ExecutionPlan bad = ExecutionPlan.reading(List.of(declared("n", "Integer", 0, 1)), cell(
                new ExecutionPlan.Sql("SELECT CAST(? AS VARCHAR)", List.of(new ExecutionPlan.Slot("n",
                        new ExecutionPlan.Binding.One("NOT_A_TYPE", null))), target, null)));
        var refused = assertThrows(IllegalStateException.class, () -> PlanRunner.rows(bad, Map.of(),
                com.legend.exec.PlanSessions.refusing("a session was opened"), java.util.UUID.randomUUID()));
        assertEquals("slot of 'n': its null type 'NOT_A_TYPE' is no JDBC type", refused.getMessage());
    }

    // ---- sessions -------------------------------------------------------------------------------------------------

    static ExecutionPlan counting(ConnectionDefinition connection, String table) {
        ExecutionPlan.Target target = target(new ExecutionPlan.Database.Declared(com.legend.planner.ConnectionPlans.of(connection), null),
                new ExecutionPlan.LoadStep.Statement("CREATE TABLE " + table + " (ID INTEGER)"),
                new ExecutionPlan.LoadStep.Statement("INSERT INTO " + table + " VALUES (1)"));
        return ExecutionPlan.reading(List.of(), cell(
                new ExecutionPlan.Sql("SELECT CAST(COUNT(*) AS VARCHAR) FROM " + table, List.of(), target, null)));
    }

    /** A typed answer of one text cell: {@code sql}'s one row's one column (the runner's sessions and setup are what
     *  these tests judge; a writer would spell the cell). */
    static ExecutionPlan.TdsResult cell(ExecutionPlan.Sql sql) {
        return new ExecutionPlan.TdsResult(ExecutionPlan.TdsResult.Shape.VALUE, List.of(new ExecutionPlan.Column("n",
                "String", new ExecutionPlan.Multiplicity(0, 1), "VARCHAR", null, null)), sql);
    }

    /** {@code plan}'s one cell, run on a session of {@code sessions}: its text, null for a null cell. */
    static @com.legend.base.Nullable String run(ExecutionPlan plan, PlanSessions.Source sessions) throws Exception {
        return run(plan, Map.of(), sessions);
    }

    /** {@link #run(ExecutionPlan, PlanSessions.Source)} with {@code values}. */
    static @com.legend.base.Nullable String run(ExecutionPlan plan, Map<String, ?> values, PlanSessions.Source sessions)
            throws Exception {
        List<List<Object>> rows = PlanRunner.rows(plan, values, sessions, java.util.UUID.randomUUID()).rows();
        assertEquals(1, rows.size(), "one row");
        return (String) rows.get(0).get(0);
    }

    static ConnectionDefinition inMemory(String name, DatabaseType type, ConnectionSpecification spec) {
        return new ConnectionDefinition(name, "s::DB", type, spec, new AuthenticationSpec.TestAuth());
    }

    /** The setup statements sent so far, in this JVM (the Census's count under the SEED mark). */
    static long setupStatements() {
        return StatementOrigin.snapshot()[StatementOrigin.SEED.ordinal()];
    }

    /** An in-memory connection on each in-process database: a private instance of its own. */
    private static List<ConnectionDefinition> inMemoryConnections() {
        return TestBackends.inProcess().stream().map(b -> inMemory("s::" + b.name(), b.type(),
                new ConnectionSpecification.InMemory())).toList();
    }

    /** Two runs of one target share its database, set up once -- its two setup statements sent once; a target with
     *  other setup is another database, set up in its turn (decision A). */
    @Test
    void runsOfOneTargetShareADatabase_setUpOnce_andAnotherTargetHasItsOwn() throws Exception {
        for (var connection : inMemoryConnections()) {
            ExecutionPlan plan = counting(connection, "SHARED_T");
            long before = setupStatements();
            assertEquals("1", run(plan, PlanSessions.shared()), connection.qualifiedName());
            assertEquals("1", run(plan, PlanSessions.shared()), connection.qualifiedName());
            assertEquals(2, setupStatements() - before, "set up once: " + connection.qualifiedName());
            assertEquals("1", run(counting(connection, "OTHER_T"), PlanSessions.shared()),
                    "another target, another database: " + connection.qualifiedName());
            assertEquals(4, setupStatements() - before, "another target, set up: " + connection.qualifiedName());
        }
    }

    static ExecutionPlan plan(String statement, ExecutionPlan.Target target) {
        return ExecutionPlan.reading(List.of(), cell(new ExecutionPlan.Sql(statement, List.of(), target, null)));
    }

    static ExecutionPlan.Target target(ExecutionPlan.Database database, ExecutionPlan.LoadStep... setup) {
        return com.legend.planner.LoadPlanner.withLoads(new ExecutionPlan.Target(database, List.of(), List.of(), null,
                TestBackends.settings(database.type()), null),
                List.of(setup));
    }

    static ExecutionPlan.LoadStep.Statement statement(String sql) {
        return new ExecutionPlan.LoadStep.Statement(sql);
    }

    /** A setup that fails leaves nothing behind: the next run of the target sets up a new database, and fails the same
     *  way (a reused half-set-up database -- an H2 one, kept by its name -- would say its table exists). */
    @Test
    void aFailedSetupLeavesNothing_theNextRunSetsUpAfresh() {
        for (var connection : inMemoryConnections()) {
            ExecutionPlan p = plan("SELECT 'never'",
                    target(new ExecutionPlan.Database.Declared(com.legend.planner.ConnectionPlans.of(connection), null), statement("CREATE TABLE FAILS_T (ID INTEGER)"), statement("INSERT INTO FAILS_T VALUES (1)"),
                            statement("SELECT * FROM NO_SUCH_TABLE")));
            for (int attempt = 1; attempt <= 2; attempt++) {
                var failed = assertThrows(com.legend.error.DataError.class, () -> run(p, PlanSessions.shared()));
                assertTrue(failed.getMessage().toUpperCase(java.util.Locale.ROOT).contains("NO_SUCH_TABLE"),
                        connection.qualifiedName() + " attempt " + attempt + ": " + failed.getMessage());
            }
        }
    }

    /** A load's model data is a content blob of statements (A2, 2026-11-10, docs/DATA_DELIVERY_DESIGN_2026_11_10.md
     *  §1.1): the runner sends each, as written, under the seeds' mark, on every in-process database. */
    @Test
    void aLoadsStatementsBlobSendsEachStatementAsWritten() throws Exception {
        for (var connection : inMemoryConnections()) {
            ExecutionPlan.Target target = com.legend.planner.LoadPlanner.withLoads(new ExecutionPlan.Target(
                    new ExecutionPlan.Database.Declared(com.legend.planner.ConnectionPlans.of(connection), null),
                    List.of(), List.of(), null, TestBackends.settings(connection.databaseType()), null), com.legend.setup.LoadSteps.of(List.of(statement(
                            "CREATE TABLE K (A VARCHAR(10), B VARCHAR(10))"))).then(com.legend.setup.LoadSteps.blob(
                            List.of("INSERT INTO K VALUES ('a', 'b')", "INSERT INTO K VALUES ('c', 'd'), ('e', 'f')"), "the data of the table K")));
            long before = setupStatements();
            assertEquals("3", run(ExecutionPlan.reading(List.of(), cell(new ExecutionPlan.Sql(
                    "SELECT CAST(COUNT(*) AS VARCHAR(10)) FROM K", List.of(), target, null))), PlanSessions.shared()),
                    connection.qualifiedName());
            assertEquals(3, setupStatements() - before, "the DDL and the blob's two inserts: "
                    + connection.qualifiedName());
        }
    }

    /** A load happens whole or not at all, and its refusal names its data (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §3
     *  rules 1 and 5, A4): a blob whose second insert the database refuses leaves none of the first's rows, on every
     *  in-process database, and the refusal names the table's data in the database's own words. */
    @Test
    void aLoadThatFailsPartWayLeavesNoneOfItsRows_andItsRefusalNamesItsData() throws Exception {
        for (com.legend.testbackend.TestBackend b : TestBackends.inProcess()) {
            try (java.sql.Connection c = b.open(); java.sql.Statement st = c.createStatement()) {
                st.execute("CREATE TABLE WHOLE_K (A VARCHAR(10))");
                ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(b.planType(),
                        null), List.of(), List.of(), null, TestBackends.settings(b.type()), null);
                com.legend.setup.LoadSteps steps = com.legend.setup.LoadSteps.blob(List.of(
                        "INSERT INTO WHOLE_K VALUES ('a')", "INSERT INTO NO_SUCH_TABLE VALUES ('b')"),
                        "the data of the table WHOLE_K");
                ExecutionPlan.Load load = com.legend.planner.LoadPlanner.load(ExecutionPlan.Load.Cadence.AT_STEP,
                        steps, target);
                Sessions.Session session = com.legend.testbackend.TestSessions.over(c);
                var refused = assertThrows(com.legend.error.DataError.class, () -> {
                    try (var run = com.legend.base.RunScope.enter()) {
                        PlanRunner.seed(load, session, PlanRunner.opened(session, load.target()),
                                com.legend.executionplan.DataStores.none(), ran -> { });
                    }
                }, b.name());
                assertTrue(refused.getMessage().startsWith("the data of the table WHOLE_K: "), refused.getMessage());
                try (java.sql.ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM WHOLE_K")) {
                    rs.next();
                    assertEquals(0, rs.getInt(1), b.name() + ": the load left a row of its");
                }
            }
        }
    }

    /** A database reached by a URL is the user's: a plan with setup for it is refused, and it is never opened (only an
     *  in-memory LocalH2 connection declares test data). */
    @Test
    void aDatabaseReachedByAUrlIsNeverSetUp() {
        java.nio.file.Path file = java.nio.file.Path.of(System.getenv("TEST_TMPDIR"), "never-opened.db");
        ExecutionPlan p = counting(inMemory("s::File", Databases.PLATFORM,
                new ConnectionSpecification.LocalFile(file.toString())), "URL_T");
        var refused = assertThrows(IllegalStateException.class, () -> run(p, PlanSessions.shared()));
        assertEquals("connection 's::File' is a database reached by a URL, and the plan loads data into it: never run on"
                + " it", refused.getMessage());
        assertTrue(java.nio.file.Files.notExists(file), "opened: " + file);
    }

    /** A target of the platform's own database (no declared connection) is shared and set up once too. */
    @Test
    void aPlatformTargetIsSharedAndSetUpOnce() throws Exception {
        ExecutionPlan p = plan("SELECT CAST(COUNT(*) AS VARCHAR) FROM PLATFORM_T",
                target(new ExecutionPlan.Database.Platform(com.legend.planner.ConnectionPlans.type(Databases.PLATFORM), null), statement("CREATE TABLE PLATFORM_T (ID INTEGER)"), statement("INSERT INTO PLATFORM_T VALUES (1)")));
        long before = setupStatements();
        assertEquals("1", run(p, PlanSessions.shared()));
        assertEquals("1", run(p, PlanSessions.shared()));
        assertEquals(2, setupStatements() - before, "set up once");
    }
}
