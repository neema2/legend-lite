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
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The runner (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 3): its parameter checks are legend-engine's, case for
 * case and message for message (its {@code TestParametersValidation}, {@code TestServiceRunner}); its sessions are given
 * out by the plan's target (decision A). The plans' answers on DuckDB, H2 and Postgres are {@code PlanMakerTest}'s and
 * {@code PostgresArmTest}'s, which run every case through it.
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

    @Test
    void aValueOfTheRightJavaTypeOrAStringThatParsesIsChecked_andConvertedForItsSlot() {
        assertEquals(5L, one("Integer", "5"));
        assertEquals(5L, one("Integer", 5));
        assertEquals(new BigDecimal("5.0"), one("Float", 5));
        assertEquals(new BigDecimal("1.1"), one("Float", 1.1d));
        assertEquals(new BigDecimal("2.50"), one("Decimal", new BigDecimal("2.50")));
        assertEquals(new BigDecimal("1.5"), one("Decimal", "1.5"));
        assertEquals(false, one("Boolean", "false"));
        assertEquals(LocalDate.of(2020, 7, 14), one("StrictDate", "2020-07-14"));
        // an offset is converted to UTC (legend-engine's TestParametersValidation)
        assertEquals(LocalDateTime.of(2020, 7, 14, 18, 18, 23, 123_000_000), one("DateTime", "2020-07-14T15:18:23.123-0300"));
        assertEquals(LocalDateTime.of(2020, 7, 14, 15, 18, 23), one("DateTime", "2020-07-14 15:18:23"));
        assertEquals(LocalDate.of(2020, 7, 14), one("Date", "2020-07-14"));
        // a one-element list for a parameter of upper bound 1 is its element
        assertEquals(7L, one("Integer", List.of(7L)));
    }

    @Test
    void aValueThatFailsIsRefusedInLegendEnginesWords() {
        assertEquals("Invalid provided parameter(s): [Unable to process 'Integer' parameter, value: true.]",
                refusal(declared("p", "Integer", 1, 1), true));
        // a Decimal takes a BigDecimal only: a Long and a Double are refused
        assertEquals("Invalid provided parameter(s): [Unable to process 'Decimal' parameter, value: 5.]",
                refusal(declared("p", "Decimal", 1, 1), 5L));
        assertEquals("Invalid provided parameter(s): [Unable to process 'Decimal' parameter, value: 2.73.]",
                refusal(declared("p", "Decimal", 1, 1), 2.73d));
        assertEquals("Invalid provided parameter(s): [Unable to process 'String' parameter, value: 5.]",
                refusal(declared("p", "String", 1, 1), 5));
        assertEquals("Invalid provided parameter(s): [Unable to process 'Boolean' parameter, value: 'x' is not"
                + " parsable.]", refusal(declared("p", "Boolean", 1, 1), "x"));
        assertEquals("Invalid provided parameter(s): [Unable to process 'StrictDate' parameter, value:"
                + " '2020-07-14T15:18:23' is not parsable. Expected formats: [yyyy-MM-dd]]",
                refusal(declared("p", "StrictDate", 1, 1), "2020-07-14T15:18:23"));
        assertEquals("Invalid provided parameter(s): [Unable to process 'DateTime' parameter, value: '2020-07-14' is"
                + " not parsable. Expected formats: [yyyy-MM-dd'T'HH:mm:ss,yyyy-MM-dd'T'HH:mm:ss.SSS,"
                + "yyyy-MM-dd HH:mm:ss.SSS,yyyy-MM-dd HH:mm:ss,yyyy-MM-dd'T'HH:mm:ss.SSSZ,yyyy-MM-dd'T'HH:mm:ssZ]]",
                refusal(declared("p", "DateTime", 1, 1), "2020-07-14"));
        // legend-engine's TestServiceRunner
        assertEquals("Invalid provided parameter(s): [Invalid enum value CONTRCT for test::EmployeeType, valid enum"
                + " values: [CONTRACT, FULL_TIME]]",
                refusal(declared("type", "test::EmployeeType", 1, 1, "CONTRACT", "FULL_TIME"), "CONTRCT"));
        // a list's failure names the whole list
        assertEquals("Invalid provided parameter(s): [Unable to process 'Integer' parameter, value: [1, true].]",
                refusal(declared("ns", "Integer", 0, null), Arrays.asList(1L, true)));
    }

    @Test
    void everyFailureIsCollected_andEveryMissingValueNamed() {
        String both = assertThrows(IllegalArgumentException.class, () -> PlanParameters.check(
                List.of(declared("a", "Integer", 1, 1), declared("b", "Boolean", 1, 1)), Map.of("a", "x", "b", 3)))
                .getMessage();
        assertEquals("Invalid provided parameter(s): [Unable to process 'Integer' parameter, value: 'x' is not"
                + " parsable.,Unable to process 'Boolean' parameter, value: 3.]", both);
        String missing = assertThrows(IllegalArgumentException.class, () -> PlanParameters.check(
                List.of(declared("input", "String", 1, 1), declared("n", "Integer", 1, null),
                        declared("o", "String", 0, 1)), Map.of())).getMessage();
        assertEquals("Missing external parameter(s): input:String[1],n:Integer[1..*]", missing);
    }

    @Test
    void anOptionalValueMayBeAbsent_aListEmpty_andAValueForNoParameterIsIgnored() {
        var checked = PlanParameters.check(List.of(declared("o", "String", 0, 1), declared("ns", "Integer", 0, null)),
                Map.of("ns", List.of(1, "2", 3L), "stray", 9));
        assertEquals(new PlanParameters.None(), checked.get("o"));
        assertEquals(new PlanParameters.Many(List.of(1L, 2L, 3L)), checked.get("ns"));
        assertEquals(new PlanParameters.None(),
                PlanParameters.check(List.of(declared("ns", "Integer", 0, null)), Map.of("ns", List.of())).get("ns"));
    }

    @Test
    void whatLegendEngineDoesNotRunIsRefusedByName() {
        // legend-engine has no validator for a Number
        assertTrue(refusal(declared("n", "Number", 1, 1), 1L).startsWith("Invalid provided parameter(s): [Unknown"
                + " external parameter type: Number, valid external parameter types: ["));
        // a list for a parameter of upper bound 1: legend-engine's template writes no SQL a database runs
        assertEquals("parameter 'p' (Integer[1]) takes one value, given 2", refusal(declared("p", "Integer", 1, 1),
                List.of(1L, 2L)));
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

    /** Two runs of one target share its database, set up once (its CREATE TABLE would fail a second time); a target
     *  with other setup is another database (decision A). */
    @Test
    void runsOfOneTargetShareADatabase_setUpOnce_andAnotherTargetHasItsOwn() throws Exception {
        for (var connection : List.of(inMemory("s::Duck", DatabaseType.DuckDB, new ConnectionSpecification.InMemory()),
                inMemory("s::H2", DatabaseType.H2, new ConnectionSpecification.LocalH2(null)))) {
            var servers = connection.databaseType() == DatabaseType.H2
                    ? new ExecutionPlan.Servers.Versions(List.of("2.1", "2.2")) : new ExecutionPlan.Servers.Every();
            ExecutionPlan plan = counting(connection, "SHARED_T", servers);
            assertEquals("1", run(plan, PlanSessions.shared()), connection.qualifiedName());
            assertEquals("1", run(plan, PlanSessions.shared()), "set up once: " + connection.qualifiedName());
            assertEquals("1", run(counting(connection, "OTHER_T", servers), PlanSessions.shared()),
                    "another target, another database: " + connection.qualifiedName());
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
}
