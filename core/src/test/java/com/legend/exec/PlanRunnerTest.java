// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.model.AuthenticationSpec;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.model.ConnectionSpecification;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
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
 * writing a value's type where the statement leaves it to the value (H2); and gives out sessions by the plan's target
 * (decision A). The plans' answers on DuckDB, H2 and Postgres are {@code PlanMakerTest}'s and {@code PostgresArmTest}'s,
 * which run every case through it.
 */
class PlanRunnerTest {

    private static ExecutionPlan.Parameter declared(String name, String type, int lower, Integer upper,
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
                counting(inMemory("s::Duck", DatabaseType.DuckDB, new ConnectionSpecification.InMemory()), "UNOPENED_T",
                        new ExecutionPlan.Servers.Every()).root());
        var refused = assertThrows(IllegalArgumentException.class, () -> PlanRunner.run(given, Map.of("n", "1"),
                target -> {
                    throw new AssertionError("a session was opened");
                }, new StringWriter()));
        assertEquals(invalid("parameter 'n' (Integer[1]): given \"1\" (String): an Integer is a Long"),
                refused.getMessage());
    }

    /** A slot's null type is a JDBC type's name, or the plan is refused by name before any session is opened. */
    @Test
    void aSlotWhoseNullTypeIsNoJdbcTypeIsRefusedBeforeASessionIsOpened() {
        ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(DatabaseType.DuckDB),
                new ExecutionPlan.Servers.Every(), List.of(), List.of());
        ExecutionPlan bad = new ExecutionPlan(List.of(declared("n", "Integer", 0, 1)), new ExecutionPlan.TextResult(
                ExecutionPlan.Format.JSON, new ExecutionPlan.Relation(List.of(new ExecutionPlan.Column("n", "Integer"))),
                new ExecutionPlan.Sql("SELECT CAST(? AS VARCHAR)", List.of(new ExecutionPlan.Slot("n",
                        new ExecutionPlan.Binding.One("NOT_A_TYPE", null))), target, null)));
        var refused = assertThrows(IllegalStateException.class, () -> PlanRunner.run(bad, Map.of(), t -> {
            throw new AssertionError("a session was opened");
        }, new StringWriter()));
        assertEquals("slot of 'n': its null type 'NOT_A_TYPE' is no JDBC type", refused.getMessage());
    }

    // ---- a type hole (H2, which types a placeholder when it prepares it) -----------------------------------------

    /** H2's spelling of each kind of value, as its dialect writes a type hole's (H2.holeType). */
    private static final Map<ExecutionPlan.ValueKind, ExecutionPlan.TypeSpelling> H2_TYPES = Map.of(
            ExecutionPlan.ValueKind.INTEGER, new ExecutionPlan.TypeSpelling("BIGINT", ExecutionPlan.Digits.NONE),
            ExecutionPlan.ValueKind.DECIMAL, new ExecutionPlan.TypeSpelling("NUMERIC",
                    ExecutionPlan.Digits.PRECISION_AND_SCALE),
            ExecutionPlan.ValueKind.FLOATING, new ExecutionPlan.TypeSpelling("DECFLOAT", ExecutionPlan.Digits.PRECISION),
            ExecutionPlan.ValueKind.DATE, new ExecutionPlan.TypeSpelling("DATE", ExecutionPlan.Digits.NONE),
            ExecutionPlan.ValueKind.DATE_TIME, new ExecutionPlan.TypeSpelling("TIMESTAMP(9)",
                    ExecutionPlan.Digits.NONE),
            ExecutionPlan.ValueKind.DATE_TIME_NANOS, new ExecutionPlan.TypeSpelling("TIMESTAMP(9)",
                    ExecutionPlan.Digits.NONE));

    /** A plan reading one value typed by its value, through a type hole, as text: {@code type} is the parameter's. */
    private static ExecutionPlan holed(String type) {
        String before = "SELECT CAST(CAST(? AS ";
        ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Declared(inMemory("s::H2",
                DatabaseType.H2, new ConnectionSpecification.LocalH2(null))), H2_SERVERS, List.of(), List.of());
        return new ExecutionPlan(List.of(declared("v", type, 0, 1)), new ExecutionPlan.TextResult(
                ExecutionPlan.Format.JSON, new ExecutionPlan.Relation(List.of(new ExecutionPlan.Column("v", "String"))),
                new ExecutionPlan.Sql(before + ") AS VARCHAR)", List.of(new ExecutionPlan.Slot("v",
                        new ExecutionPlan.Binding.One("DECIMAL", new ExecutionPlan.TypeHole(before.length(), H2_TYPES,
                                type.equals("Date") ? ExecutionPlan.ValueKind.DATE
                                        : ExecutionPlan.ValueKind.DECIMAL)))), target, null)));
    }

    /** The runner fills a type hole with the type of the value's own literal: each value answers on H2 exactly as its
     *  literal does, text included (the whole sweep, every position: probes/value-typed-cast-results.txt). */
    @Test
    void aTypeHoleIsFilledWithTheTypeOfTheValuesLiteral_andAnswersAsTheLiteralDoes() throws Exception {
        Map<Object, String> literals = new java.util.LinkedHashMap<>();
        literals.put(1.1d, "1.1");
        literals.put(new BigDecimal("2.50"), "2.50");
        literals.put(1e-6d, "0.0000010");
        literals.put(1.5e15d, "1.5E15");
        literals.put(2.5e-7d, "2.5E-7");
        literals.put(7L, "CAST(7 AS BIGINT)");
        try (Connection h2 = DriverManager.getConnection("jdbc:h2:mem:holes" + H2Settings.SETTINGS)) {
            for (var e : literals.entrySet()) {
                StringWriter out = new StringWriter();
                PlanRunner.run(holed("Number"), Map.of("v", e.getKey()), PlanSessions.given(h2), out);
                assertEquals(literalText(h2, e.getValue()), out.toString(), String.valueOf(e.getKey()));
            }
            Map<Object, String> dates = Map.of(LocalDate.of(2024, 1, 2), "DATE '2024-01-02'",
                    LocalDateTime.of(2024, 1, 2, 10, 30, 0, 123_456_789), "TIMESTAMP '2024-01-02 10:30:00.123456789'");
            for (var e : dates.entrySet()) {
                StringWriter out = new StringWriter();
                PlanRunner.run(holed("Date"), Map.of("v", e.getKey()), PlanSessions.given(h2), out);
                assertEquals(literalText(h2, e.getValue()), out.toString(), String.valueOf(e.getKey()));
            }
            // an absent value: a null of its absent kind's type, by name alone
            StringWriter absent = new StringWriter();
            PlanRunner.run(holed("Number"), Map.of(), PlanSessions.given(h2), absent);
            assertEquals("", absent.toString());
        }
    }

    private static String literalText(Connection c, String literal) throws Exception {
        try (var st = c.createStatement(); var rs = st.executeQuery("SELECT CAST(" + literal + " AS VARCHAR)")) {
            rs.next();
            return rs.getString(1);
        }
    }

    // ---- sessions -------------------------------------------------------------------------------------------------

    private static ExecutionPlan counting(ConnectionDefinition connection, String table, ExecutionPlan.Servers servers) {
        ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Declared(connection), servers,
                List.of(), List.of(new ExecutionPlan.SetupStep.Statement("CREATE TABLE " + table + " (ID INTEGER)"),
                        new ExecutionPlan.SetupStep.Statement("INSERT INTO " + table + " VALUES (1)")));
        return new ExecutionPlan(List.of(), new ExecutionPlan.TextResult(ExecutionPlan.Format.JSON,
                new ExecutionPlan.Relation(List.of(new ExecutionPlan.Column("n", "String"))),
                new ExecutionPlan.Sql("SELECT CAST(COUNT(*) AS VARCHAR) FROM " + table, List.of(), target, null)));
    }

    private static String run(ExecutionPlan plan, PlanSessions.Source sessions) throws Exception {
        StringWriter out = new StringWriter();
        PlanRunner.run(plan, Map.of(), sessions, out);
        return out.toString();
    }

    private static ConnectionDefinition inMemory(String name, DatabaseType type, ConnectionSpecification spec) {
        return new ConnectionDefinition(name, "s::DB", type, spec, new AuthenticationSpec.TestAuth());
    }

    /** The setup statements sent so far, in this JVM (the Census's count under the SEED mark). */
    private static long setupStatements() {
        return StatementOrigin.snapshot()[StatementOrigin.SEED.ordinal()];
    }

    /** Two runs of one target share its database, set up once -- its two setup statements sent once; a target with
     *  other setup is another database, set up in its turn (decision A). */
    @Test
    void runsOfOneTargetShareADatabase_setUpOnce_andAnotherTargetHasItsOwn() throws Exception {
        for (var connection : List.of(inMemory("s::Duck", DatabaseType.DuckDB, new ConnectionSpecification.InMemory()),
                inMemory("s::H2", DatabaseType.H2, new ConnectionSpecification.LocalH2(null)))) {
            var servers = connection.databaseType() == DatabaseType.H2 ? H2_SERVERS : new ExecutionPlan.Servers.Every();
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

    @Test
    void aSessionOfAnotherDatabaseOrServerVersionIsRefused_neverRun() throws Exception {
        ExecutionPlan duck = counting(inMemory("s::Duck", DatabaseType.DuckDB, new ConnectionSpecification.InMemory()),
                "T", new ExecutionPlan.Servers.Every());
        ExecutionPlan h2ForNine = counting(inMemory("s::H2", DatabaseType.H2, new ConnectionSpecification.LocalH2(null)),
                "T", new ExecutionPlan.Servers.Versions(List.of("9.")));
        try (Connection h2 = DriverManager.getConnection("jdbc:h2:mem:planrunner" + H2Settings.SETTINGS)) {
            var otherDatabase = assertThrows(RuntimeException.class, () -> run(duck, PlanSessions.given(h2)));
            assertTrue(otherDatabase.getMessage().contains("dialect/connection mismatch"), otherDatabase.getMessage());
            var otherVersion = assertThrows(com.legend.error.NotImplementedException.class,
                    () -> run(h2ForNine, PlanSessions.given(h2)));
            assertTrue(otherVersion.getMessage().contains("written for H2 [9.], and the session is H2 2.1"),
                    otherVersion.getMessage());
        }
    }

    private static final ExecutionPlan.Servers H2_SERVERS = new ExecutionPlan.Servers.Versions(List.of("2.1", "2.2"));

    private static ExecutionPlan plan(ExecutionPlan.Format format, String statement, ExecutionPlan.Target target) {
        return new ExecutionPlan(List.of(), new ExecutionPlan.TextResult(format,
                new ExecutionPlan.Relation(List.of(new ExecutionPlan.Column("n", "String"))),
                new ExecutionPlan.Sql(statement, List.of(), target, null)));
    }

    private static ExecutionPlan.Target target(ExecutionPlan.Database database, ExecutionPlan.Servers servers,
            ExecutionPlan.SetupStep... setup) {
        return new ExecutionPlan.Target(database, servers, List.of(), List.of(setup));
    }

    private static ExecutionPlan.SetupStep.Statement statement(String sql) {
        return new ExecutionPlan.SetupStep.Statement(sql);
    }

    /** Rows for DuckDB's Appender, into table K (A, B). */
    private static ExecutionPlan.SetupStep.Rows kRows(List<String> row) {
        return new ExecutionPlan.SetupStep.Rows("K_STAGE", "CREATE TEMP TABLE K_STAGE (A VARCHAR, B VARCHAR)",
                "INSERT INTO K SELECT A, B FROM K_STAGE", "DROP TABLE K_STAGE", List.of(row));
    }

    /** Two targets are one database only when their content is equal, cell for cell: a cell holding a comma and a
     *  null cell are spelled apart from their look-alikes (the audit's S4: a key of the targets' printed text gave
     *  {@code ["a, b", "c"]} and {@code ["a", "b, c"]} one database). */
    @Test
    void aTargetIsKeyedByItsWholeContent_soLookAlikeCellsNeverShareADatabase() throws Exception {
        var duck = new ExecutionPlan.Database.Declared(inMemory("s::Duck", DatabaseType.DuckDB,
                new ConnectionSpecification.InMemory()));
        String read = "SELECT string_agg(COALESCE(A, '<none>') || '/' || COALESCE(B, '<none>'), ';') FROM K";
        Map<List<String>, String> rows = new java.util.LinkedHashMap<>();
        rows.put(List.of("a, b", "c"), "a, b/c");
        rows.put(List.of("a", "b, c"), "a/b, c");
        rows.put(Arrays.asList(null, "x"), "<none>/x");
        rows.put(List.of("null", "x"), "null/x");
        for (var e : rows.entrySet()) {
            ExecutionPlan p = plan(ExecutionPlan.Format.JSON, read, target(duck, new ExecutionPlan.Servers.Every(),
                    statement("CREATE TABLE K (A VARCHAR, B VARCHAR)"), kRows(e.getKey())));
            assertEquals(e.getValue(), run(p, PlanSessions.shared()), String.valueOf(e.getKey()));
        }
    }

    /** A setup that fails leaves nothing behind: the next run of the target sets up a new database, and fails the same
     *  way (a reused half-set-up database -- an H2 one, kept by its name -- would say its table exists). */
    @Test
    void aFailedSetupLeavesNothing_theNextRunSetsUpAfresh() {
        for (var connection : List.of(inMemory("s::Duck", DatabaseType.DuckDB, new ConnectionSpecification.InMemory()),
                inMemory("s::H2", DatabaseType.H2, new ConnectionSpecification.LocalH2(null)))) {
            ExecutionPlan p = plan(ExecutionPlan.Format.JSON, "SELECT 'never'",
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

    /** A setup refused before any statement runs -- rows for a bulk loader, on a database with none -- fails the same
     *  way again: the refusal, a runtime error, closed what was opened, and nothing was kept. */
    @Test
    void aSetupRefusedForWantOfABulkLoaderFailsTheSameWayAgain() {
        ExecutionPlan p = plan(ExecutionPlan.Format.JSON, "SELECT 'never'",
                target(new ExecutionPlan.Database.Declared(inMemory("s::H2", DatabaseType.H2,
                                new ConnectionSpecification.LocalH2(null))), H2_SERVERS,
                        statement("CREATE TABLE K (A VARCHAR, B VARCHAR)"), kRows(List.of("a", "b"))));
        for (int attempt = 1; attempt <= 2; attempt++) {
            var refused = assertThrows(IllegalStateException.class, () -> run(p, PlanSessions.shared()));
            assertEquals("the plan's setup has rows for a bulk loader, and the session's database has none (H2)",
                    refused.getMessage(), "attempt " + attempt);
        }
    }

    /** A database reached by a URL is the user's: a plan with setup for it is refused, and it is never opened (only an
     *  in-memory LocalH2 connection declares test data). */
    @Test
    void aDatabaseReachedByAUrlIsNeverSetUp() {
        java.nio.file.Path file = java.nio.file.Path.of(System.getenv("TEST_TMPDIR"), "never-opened.duckdb");
        ExecutionPlan p = counting(inMemory("s::File", DatabaseType.DuckDB,
                new ConnectionSpecification.LocalFile(file.toString())), "URL_T", new ExecutionPlan.Servers.Every());
        var refused = assertThrows(IllegalStateException.class, () -> run(p, PlanSessions.shared()));
        assertEquals("connection 's::File' is a database reached by a URL, and the plan has setup for it: never run on"
                + " it", refused.getMessage());
        assertTrue(java.nio.file.Files.notExists(file), "opened: " + file);
    }

    /** So is an in-memory database the user named (an EmbeddedH2): the user's identity, shared by design. */
    @Test
    void aDatabaseTheUserNamedIsNeverSetUp() {
        ExecutionPlan p = counting(inMemory("s::Named", DatabaseType.H2,
                new ConnectionSpecification.EmbeddedH2("users_own", "/never", false)), "NAMED_T", H2_SERVERS);
        var refused = assertThrows(IllegalStateException.class, () -> run(p, PlanSessions.shared()));
        assertEquals("connection 's::Named' is an in-memory database the user named ('users_own'), and the plan has"
                + " setup for it: never run on it", refused.getMessage());
    }

    /** Runs of one target that start together set it up once: its two setup statements sent once, every run answered
     *  from that database (that a run waits for a setup in progress, and that another target's is not held up, is
     *  HandleStoreTest's, deterministically). */
    @Test
    void runsOfOneTargetThatStartTogetherSetItUpOnce() throws Exception {
        ExecutionPlan p = plan(ExecutionPlan.Format.JSON, "SELECT CAST(COUNT(*) AS VARCHAR) FROM TOGETHER_T",
                target(new ExecutionPlan.Database.Declared(inMemory("s::H2", DatabaseType.H2,
                                new ConnectionSpecification.LocalH2(null))), H2_SERVERS,
                        statement("CREATE TABLE TOGETHER_T (ID INTEGER)"),
                        statement("INSERT INTO TOGETHER_T SELECT X FROM SYSTEM_RANGE(1, 100000)")));
        int runs = 8;
        var start = new java.util.concurrent.CountDownLatch(1);
        var pool = java.util.concurrent.Executors.newFixedThreadPool(runs);
        long before = setupStatements();
        try {
            List<java.util.concurrent.Future<String>> answers = new java.util.ArrayList<>();
            for (int i = 0; i < runs; i++) {
                answers.add(pool.submit(() -> {
                    start.await();
                    return run(p, PlanSessions.shared());
                }));
            }
            start.countDown();
            for (var answer : answers) {
                assertEquals("100000", answer.get(60, java.util.concurrent.TimeUnit.SECONDS));
            }
        } finally {
            pool.shutdownNow();
        }
        assertEquals(2, setupStatements() - before, "set up once for " + runs + " runs");
    }

    /** A target of the platform's own database (no declared connection) is shared and set up once too. */
    @Test
    void aPlatformTargetIsSharedAndSetUpOnce() throws Exception {
        ExecutionPlan p = plan(ExecutionPlan.Format.JSON, "SELECT CAST(COUNT(*) AS VARCHAR) FROM PLATFORM_T",
                target(new ExecutionPlan.Database.Platform(DatabaseType.DuckDB), new ExecutionPlan.Servers.Every(),
                        statement("CREATE TABLE PLATFORM_T (ID INTEGER)"), statement("INSERT INTO PLATFORM_T VALUES (1)")));
        long before = setupStatements();
        assertEquals("1", run(p, PlanSessions.shared()));
        assertEquals("1", run(p, PlanSessions.shared()));
        assertEquals(2, setupStatements() - before, "set up once");
    }

    /** One JSON object per row, in an array: no row is an empty array. */
    @Test
    void jsonPerRowWritesAnArray_emptyForNoRows() throws Exception {
        var duck = new ExecutionPlan.Database.Platform(DatabaseType.DuckDB);
        assertEquals("[]", run(plan(ExecutionPlan.Format.JSON_PER_ROW, "SELECT '{}' WHERE 1 = 0",
                target(duck, new ExecutionPlan.Servers.Every())), PlanSessions.shared()));
        assertEquals("[{\"n\":1},{\"n\":2}]", run(plan(ExecutionPlan.Format.JSON_PER_ROW,
                "SELECT '{\"n\":' || i || '}' FROM range(1, 3) t(i) ORDER BY i",
                target(duck, new ExecutionPlan.Servers.Every())), PlanSessions.shared()));
    }
}
