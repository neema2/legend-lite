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
        ExecutionPlan given = new ExecutionPlan(List.of(declared("n", "Integer", 1, 1)),
                counting(inMemory("s::C", Databases.PLATFORM, new ConnectionSpecification.InMemory()), "UNOPENED_T",
                        new ExecutionPlan.Servers.Every()).root());
        var refused = assertThrows(IllegalArgumentException.class, () -> PlanRunner.rows(given, Map.of("n", "1"),
                target -> {
                    throw new AssertionError("a session was opened");
                }));
        assertEquals(invalid("parameter 'n' (Integer[1]): given \"1\" (String): an Integer is a Long"),
                refused.getMessage());
    }

    /** A slot's null type is a JDBC type's name, or the plan is refused by name before any session is opened. */
    @Test
    void aSlotWhoseNullTypeIsNoJdbcTypeIsRefusedBeforeASessionIsOpened() {
        ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(Databases.PLATFORM),
                new ExecutionPlan.Servers.Every(), List.of(), List.of());
        ExecutionPlan bad = new ExecutionPlan(List.of(declared("n", "Integer", 0, 1)), cell(
                new ExecutionPlan.Sql("SELECT CAST(? AS VARCHAR)", List.of(new ExecutionPlan.Slot("n",
                        new ExecutionPlan.Binding.One("NOT_A_TYPE", null))), target, null)));
        var refused = assertThrows(IllegalStateException.class, () -> PlanRunner.rows(bad, Map.of(), t -> {
            throw new AssertionError("a session was opened");
        }));
        assertEquals("slot of 'n': its null type 'NOT_A_TYPE' is no JDBC type", refused.getMessage());
    }

    // ---- sessions -------------------------------------------------------------------------------------------------

    static ExecutionPlan counting(ConnectionDefinition connection, String table, ExecutionPlan.Servers servers) {
        ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Declared(connection), servers,
                List.of(), List.of(new ExecutionPlan.SetupStep.Statement("CREATE TABLE " + table + " (ID INTEGER)"),
                        new ExecutionPlan.SetupStep.Statement("INSERT INTO " + table + " VALUES (1)")));
        return new ExecutionPlan(List.of(), cell(
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
        List<List<Object>> rows = PlanRunner.rows(plan, values, sessions).rows();
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

    /** The servers {@code type}'s plans are written for, as the plan maker writes them (its dialect's versions). */
    private static ExecutionPlan.Servers servers(DatabaseType type) {
        List<String> versions = Databases.dialect(type).serverVersions();
        return versions == null ? new ExecutionPlan.Servers.Every() : new ExecutionPlan.Servers.Versions(versions);
    }

    /** Two runs of one target share its database, set up once -- its two setup statements sent once; a target with
     *  other setup is another database, set up in its turn (decision A). */
    @Test
    void runsOfOneTargetShareADatabase_setUpOnce_andAnotherTargetHasItsOwn() throws Exception {
        for (var connection : inMemoryConnections()) {
            var servers = servers(connection.databaseType());
            ExecutionPlan plan = counting(connection, "SHARED_T", servers);
            long before = setupStatements();
            assertEquals("1", run(plan, PlanSessions.shared()), connection.qualifiedName());
            assertEquals("1", run(plan, PlanSessions.shared()), connection.qualifiedName());
            assertEquals(2, setupStatements() - before, "set up once: " + connection.qualifiedName());
            assertEquals("1", run(counting(connection, "OTHER_T", servers), PlanSessions.shared()),
                    "another target, another database: " + connection.qualifiedName());
            assertEquals(4, setupStatements() - before, "another target, set up: " + connection.qualifiedName());
        }
    }

    static ExecutionPlan plan(String statement, ExecutionPlan.Target target) {
        return new ExecutionPlan(List.of(), cell(new ExecutionPlan.Sql(statement, List.of(), target, null)));
    }

    static ExecutionPlan.Target target(ExecutionPlan.Database database, ExecutionPlan.Servers servers,
            ExecutionPlan.SetupStep... setup) {
        return new ExecutionPlan.Target(database, servers, List.of(), List.of(setup));
    }

    static ExecutionPlan.SetupStep.Statement statement(String sql) {
        return new ExecutionPlan.SetupStep.Statement(sql);
    }

    /** A setup that fails leaves nothing behind: the next run of the target sets up a new database, and fails the same
     *  way (a reused half-set-up database -- an H2 one, kept by its name -- would say its table exists). */
    @Test
    void aFailedSetupLeavesNothing_theNextRunSetsUpAfresh() {
        for (var connection : inMemoryConnections()) {
            ExecutionPlan p = plan("SELECT 'never'",
                    target(new ExecutionPlan.Database.Declared(connection), new ExecutionPlan.Servers.Every(),
                            statement("CREATE TABLE FAILS_T (ID INTEGER)"), statement("INSERT INTO FAILS_T VALUES (1)"),
                            statement("SELECT * FROM NO_SUCH_TABLE")));
            for (int attempt = 1; attempt <= 2; attempt++) {
                var failed = assertThrows(com.legend.error.DataError.class, () -> run(p, PlanSessions.shared()));
                assertTrue(failed.getMessage().toUpperCase(java.util.Locale.ROOT).contains("NO_SUCH_TABLE"),
                        connection.qualifiedName() + " attempt " + attempt + ": " + failed.getMessage());
            }
        }
    }

    /** Rows for a bulk loader, into table K (A, B). */
    static ExecutionPlan.SetupStep.Rows kRows(List<String> row) {
        return new ExecutionPlan.SetupStep.Rows("K_STAGE", "CREATE TEMP TABLE K_STAGE (A VARCHAR, B VARCHAR)",
                "INSERT INTO K SELECT A, B FROM K_STAGE", "DROP TABLE K_STAGE", List.of(row));
    }

    /** A setup refused before any statement runs -- rows for a bulk loader, on a database with none -- fails the same
     *  way again: the refusal, a runtime error, closed what was opened, and nothing was kept. */
    @Test
    void aSetupRefusedForWantOfABulkLoaderFailsTheSameWayAgain() {
        int checked = 0;
        for (var connection : inMemoryConnections()) {
            if (Databases.dialect(connection.databaseType()).loadsRowsInBulk()) {
                continue;
            }
            checked++;
            ExecutionPlan p = plan("SELECT 'never'",
                    target(new ExecutionPlan.Database.Declared(connection), servers(connection.databaseType()),
                            statement("CREATE TABLE K (A VARCHAR, B VARCHAR)"), kRows(List.of("a", "b"))));
            for (int attempt = 1; attempt <= 2; attempt++) {
                var refused = assertThrows(IllegalStateException.class, () -> run(p, PlanSessions.shared()));
                assertEquals("the plan's setup has rows for a bulk loader, and the session's database has none ("
                        + connection.databaseType() + ")", refused.getMessage(), "attempt " + attempt);
            }
        }
        assertTrue(checked > 0, "no in-process database without a bulk loader: the refusal went unchecked");
    }

    /** A database reached by a URL is the user's: a plan with setup for it is refused, and it is never opened (only an
     *  in-memory LocalH2 connection declares test data). */
    @Test
    void aDatabaseReachedByAUrlIsNeverSetUp() {
        java.nio.file.Path file = java.nio.file.Path.of(System.getenv("TEST_TMPDIR"), "never-opened.db");
        ExecutionPlan p = counting(inMemory("s::File", Databases.PLATFORM,
                new ConnectionSpecification.LocalFile(file.toString())), "URL_T", new ExecutionPlan.Servers.Every());
        var refused = assertThrows(IllegalStateException.class, () -> run(p, PlanSessions.shared()));
        assertEquals("connection 's::File' is a database reached by a URL, and the plan has setup for it: never run on"
                + " it", refused.getMessage());
        assertTrue(java.nio.file.Files.notExists(file), "opened: " + file);
    }

    /** A target of the platform's own database (no declared connection) is shared and set up once too. */
    @Test
    void aPlatformTargetIsSharedAndSetUpOnce() throws Exception {
        ExecutionPlan p = plan("SELECT CAST(COUNT(*) AS VARCHAR) FROM PLATFORM_T",
                target(new ExecutionPlan.Database.Platform(Databases.PLATFORM), new ExecutionPlan.Servers.Every(),
                        statement("CREATE TABLE PLATFORM_T (ID INTEGER)"), statement("INSERT INTO PLATFORM_T VALUES (1)")));
        long before = setupStatements();
        assertEquals("1", run(p, PlanSessions.shared()));
        assertEquals("1", run(p, PlanSessions.shared()));
        assertEquals(2, setupStatements() - before, "set up once");
    }
}
