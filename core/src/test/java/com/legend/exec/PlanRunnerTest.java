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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
                + " not parsable. Expected formats: [" + DATE_TIME_FORMATS + "]]",
                refusal(declared("p", "DateTime", 1, 1), "2020-07-14"));
        // legend-engine's TestServiceRunner
        assertEquals("Invalid provided parameter(s): [Invalid enum value CONTRCT for test::EmployeeType, valid enum"
                + " values: [CONTRACT, FULL_TIME]]",
                refusal(declared("type", "test::EmployeeType", 1, 1, "CONTRACT", "FULL_TIME"), "CONTRCT"));
        // a list's failure names its failing element (FunctionParametersParametersValidation validates each); an
        // enumeration's, the whole value (ParameterValidationContextExecutor)
        assertEquals("Invalid provided parameter(s): [Unable to process 'Integer' parameter, value: true.]",
                refusal(declared("ns", "Integer", 0, null), Arrays.asList(1L, true)));
        assertEquals("Invalid provided parameter(s): [Invalid enum value [CONTRACT, CONTRCT] for test::EmployeeType,"
                + " valid enum values: [CONTRACT, FULL_TIME]]", refusal(declared("types", "test::EmployeeType", 0,
                        null, "CONTRACT", "FULL_TIME"), List.of("CONTRACT", "CONTRCT")));
    }

    // ---- legend-engine's TestParametersValidation, case for case -------------------------------------------------
    // Its valid values, each with what the runner binds (legend-engine's own expected values are its EngineDate and
    // Double forms; the runner's are its slots' Java types); its invalid values, each with its exact message (to-many
    // too, where legend-engine's test asserts only the prefix). Its "now" values are fixed instants here.

    private static final String DATE_TIME_FORMATS = "yyyy-MM-dd'T'HH:mm:ss,yyyy-MM-dd'T'HH:mm:ss.SSS,"
            + "yyyy-MM-dd HH:mm:ss.SSS,yyyy-MM-dd HH:mm:ss,yyyy-MM-dd'T'HH:mm:ss.SSSZ,yyyy-MM-dd'T'HH:mm:ssZ";
    private static final Instant NOW = Instant.parse("2021-03-04T05:06:07.089Z");
    private static final LocalDateTime NOW_UTC = LocalDateTime.of(2021, 3, 4, 5, 6, 7, 89_000_000);
    private static final ZonedDateTime ZONED_NOW = ZonedDateTime.ofInstant(NOW, ZoneOffset.UTC);
    private static final LocalDate TODAY = LocalDate.of(2021, 3, 4);

    private static LocalDateTime at(int h, int m, int s, int millis) {
        return LocalDateTime.of(2020, 7, 14, h, m, s, millis * 1_000_000);
    }

    private static void toOne(String type, List<?> valid, List<?> expected, List<?> invalid, String suffix) {
        assertEquals(valid.size(), expected.size());
        for (int i = 0; i < valid.size(); i++) {
            Object v = valid.get(i);
            Object bound = one(type, v);
            assertEquals(expected.get(i), bound, type + " " + v);
            assertEquals(expected.get(i).getClass(), bound.getClass(), type + " " + v);
        }
        for (Object v : invalid) {
            assertEquals(engineMessage(type, v, suffix), refusal(declared("p", type, 1, 1), v), type + " " + v);
        }
    }

    /** {@code invalid}: each list, with its failing element. */
    private static void toMany(String type, List<List<?>> valid, List<List<?>> expected, Map<List<?>, Object> invalid,
            String suffix) {
        ExecutionPlan.Parameter p = declared("p", type, 0, null);
        Map<String, Object> absent = new HashMap<>();
        absent.put("p", null);
        assertEquals(new PlanParameters.None(), PlanParameters.check(List.of(p), absent).get("p"), type + " null");
        assertEquals(new PlanParameters.None(), PlanParameters.check(List.of(p), Map.of("p", List.of())).get("p"),
                type + " []");
        for (int i = 0; i < valid.size(); i++) {
            assertEquals(new PlanParameters.Many(new java.util.ArrayList<>(expected.get(i))),
                    PlanParameters.check(List.of(p), Map.of("p", valid.get(i))).get("p"), type + " " + valid.get(i));
        }
        invalid.forEach((list, failing) -> assertEquals(engineMessage(type, failing, suffix), refusal(p, list),
                type + " " + list));
    }

    /** legend-engine's test's expected message ({@code getExpectedExceptionMessage}). */
    private static String engineMessage(String type, Object value, String suffix) {
        StringBuilder b = new StringBuilder("Invalid provided parameter(s): [Unable to process '").append(type)
                .append("' parameter, value: ");
        if (value instanceof String) {
            b.append("'").append(value).append("'");
            if (!"String".equals(type)) {
                b.append(" is not parsable");
            }
        } else {
            b.append(value);
        }
        b.append(".");
        if (suffix != null) {
            b.append(" ").append(suffix);
        }
        return b.append("]").toString();
    }

    @Test
    void legendEnginesStringCases() {
        toOne("String", List.of("the quick brown fox", "ABCDE", "5", "6.0", "true"),
                List.of("the quick brown fox", "ABCDE", "5", "6.0", "true"),
                List.of(5, 6.0, true, false, NOW, TODAY), null);
        toMany("String", List.of(List.of("a", "b", "c"), List.of("the quick brown fox")),
                List.of(List.of("a", "b", "c"), List.of("the quick brown fox")),
                Map.of(List.of("a", "b", true), true, List.of(5, 6.0, "string", false), 5), null);
    }

    @Test
    void legendEnginesBooleanCases() {
        toOne("Boolean", List.of(true, false, "true", "false"), List.of(true, false, true, false),
                List.of(5, 6.0, "the quick brown fox", "jumped over the lazy dog", NOW, TODAY), null);
        toMany("Boolean", List.of(List.of(true, true), List.of(true, "false"), List.of(false)),
                List.of(List.of(true, true), List.of(true, false), List.of(false)),
                Map.of(List.of(true, "b"), "b", List.of("c", 5, "e"), "c"), null);
    }

    @Test
    void legendEnginesIntegerCases() {
        toOne("Integer", List.of(5, 6L, -1L, Long.MAX_VALUE, Long.MIN_VALUE, Integer.MAX_VALUE, "-1", "5", "6"),
                List.of(5L, 6L, -1L, Long.MAX_VALUE, Long.MIN_VALUE, (long) Integer.MAX_VALUE, -1L, 5L, 6L),
                List.of(true, false, "the quick brown fox", "jumped over the lazy dog", NOW, TODAY), null);
        toMany("Integer", List.of(List.of(1, "2", 3L), List.of(6L), List.of(Long.MAX_VALUE, Long.MIN_VALUE)),
                List.of(List.of(1L, 2L, 3L), List.of(6L), List.of(Long.MAX_VALUE, Long.MIN_VALUE)),
                Map.of(List.of(1, 2, "b"), "b", List.of("c", false), "c"), null);
    }

    @Test
    void legendEnginesDecimalCases() {
        toOne("Decimal", List.of(BigDecimal.valueOf(3.14d), BigDecimal.valueOf(5L), "-1.23", "2.73"),
                List.of(BigDecimal.valueOf(3.14d), BigDecimal.valueOf(5L), BigDecimal.valueOf(-1.23),
                        BigDecimal.valueOf(2.73)),
                List.of(5L, 2.73d, 1.23f, true, false, "the quick brown fox", "jumped over the lazy dog", NOW, TODAY),
                null);
        toMany("Decimal", List.of(List.of(BigDecimal.valueOf(3.14d)), List.of(BigDecimal.valueOf(5L), "-1.23")),
                List.of(List.of(BigDecimal.valueOf(3.14d)), List.of(BigDecimal.valueOf(5L), BigDecimal.valueOf(-1.23))),
                Map.of(List.of(1, 2, "b"), 1, List.of("c", false, 5L), "c"), null);
    }

    /** A Float binds as its literal is typed (the numeric charter's Rule 1): its plain digits as a decimal; at an
     *  extreme magnitude (at least 1e15, or below 1e-6) a double. */
    @Test
    void legendEnginesFloatCases() {
        toOne("Float", List.of(5.0, 6.12d, Double.MAX_VALUE, Double.MIN_VALUE, "-1.0", "5.234", "678978678", 5, 4,
                        Long.MAX_VALUE),
                List.of(new BigDecimal("5.0"), new BigDecimal("6.12"), Double.MAX_VALUE, Double.MIN_VALUE,
                        new BigDecimal("-1.0"), new BigDecimal("5.234"), new BigDecimal("678978678.0"),
                        new BigDecimal("5.0"), new BigDecimal("4.0"), (double) Long.MAX_VALUE),
                List.of(true, false, "the quick brown fox", "jumped over the lazy dog", NOW, TODAY), null);
        toMany("Float", List.of(List.of("5.0", 6.12d, -2.71), List.of(0.0), List.of(Double.MAX_VALUE, Double.MIN_VALUE)),
                List.of(List.of(new BigDecimal("5.0"), new BigDecimal("6.12"), new BigDecimal("-2.71")),
                        List.of(new BigDecimal("0.0")), List.of(Double.MAX_VALUE, Double.MIN_VALUE)),
                Map.of(List.of(5.0, 6.12d, "c"), "c", List.of(false, true, "a", "B"), false), null);
        // the edges of Rule 1's plain range
        assertEquals(new BigDecimal("999999999999999.9"), one("Float", 999999999999999.9d));
        assertEquals(1e15d, one("Float", 1e15d));
        assertEquals(new BigDecimal("0.0000010"), one("Float", 1e-6d));
        assertEquals(9.99e-7d, one("Float", 9.99e-7d));
    }

    @Test
    void legendEnginesDateCases() {
        String formats = "Expected formats: [yyyy-MM-dd," + DATE_TIME_FORMATS + "]";
        toOne("Date", List.of(NOW, TODAY, LocalDateTime.ofInstant(NOW, ZoneOffset.UTC), ZONED_NOW, "2020-07-14",
                        "2020-07-14 15:18:23", "2020-07-14T15:18:23", "2020-07-14 15:18:23.992", "2020-07-14T15:18:23.123",
                        "2020-07-14T15:18:23-0300"),
                List.of(NOW_UTC, TODAY, NOW_UTC, NOW_UTC, LocalDate.of(2020, 7, 14), at(15, 18, 23, 0),
                        at(15, 18, 23, 0), at(15, 18, 23, 992), at(15, 18, 23, 123), at(18, 18, 23, 0)),
                List.of(true, false, "the quick brown fox", "jumped over the lazy dog", 4.2, -3.14, 5, 4, 3), formats);
        toMany("Date", List.of(List.of(NOW), List.of(NOW, TODAY), List.of(ZONED_NOW, "2020-07-14")),
                List.of(List.of(NOW_UTC), List.of(NOW_UTC, TODAY), List.of(NOW_UTC, LocalDate.of(2020, 7, 14))),
                Map.of(List.of(5, 2, 3), 5, List.of(NOW, 2), 2), formats);
    }

    @Test
    void legendEnginesStrictDateCases() {
        String formats = "Expected formats: [yyyy-MM-dd]";
        toOne("StrictDate", List.of(TODAY, "2020-07-14"), List.of(TODAY, LocalDate.of(2020, 7, 14)),
                List.of(NOW, NOW_UTC, ZONED_NOW, true, false, "the quick brown fox", "jumped over the lazy dog", 4.2,
                        -3.14, 5, 4, 3, "2020-07-14 15:18:23", "2020-07-14T15:18:23", "2020-07-14 15:18:23.992",
                        "2020-07-14T15:18:23.123"), formats);
        toMany("StrictDate", List.of(List.of(TODAY), List.of(TODAY, "2020-07-14"), List.of("2020-07-14", "2020-08-06")),
                List.of(List.of(TODAY), List.of(TODAY, LocalDate.of(2020, 7, 14)),
                        List.of(LocalDate.of(2020, 7, 14), LocalDate.of(2020, 8, 6))),
                Map.of(List.of(5, 2, "c", false), 5, List.of(TODAY, ZONED_NOW), ZONED_NOW), formats);
    }

    @Test
    void legendEnginesDateTimeCases() {
        String formats = "Expected formats: [" + DATE_TIME_FORMATS + "]";
        // an offset is converted to UTC
        toOne("DateTime", List.of(NOW, LocalDateTime.ofInstant(NOW, ZoneOffset.UTC), ZONED_NOW, "2020-07-14 15:18:23",
                        "2020-07-14T15:18:23", "2020-07-14 15:18:23.992", "2020-07-14T15:18:23.123",
                        "2020-07-14T15:18:23.123-0300", "2020-07-14T15:18:23.123+0500"),
                List.of(NOW_UTC, NOW_UTC, NOW_UTC, at(15, 18, 23, 0), at(15, 18, 23, 0), at(15, 18, 23, 992),
                        at(15, 18, 23, 123), at(18, 18, 23, 123), at(10, 18, 23, 123)),
                List.of(TODAY, true, false, "the quick brown fox", "jumped over the lazy dog", 4.2, -3.14, 5, 4, 3,
                        "2020-07-14"), formats);
        toMany("DateTime", List.of(List.of(ZONED_NOW, NOW), List.of(ZONED_NOW), List.of("2020-07-14T15:18:23",
                        "2020-07-14 15:18:23.992", "2020-07-14T15:18:23.123", "2020-07-14T15:18:23.123-0300",
                        "2020-07-14T15:18:23.123+0500")),
                List.of(List.of(NOW_UTC, NOW_UTC), List.of(NOW_UTC), List.of(at(15, 18, 23, 0), at(15, 18, 23, 992),
                        at(15, 18, 23, 123), at(18, 18, 23, 123), at(10, 18, 23, 123))),
                Map.of(List.of(5, "b", 3), 5, List.of(ZONED_NOW, "b"), "b", List.of(TODAY, ZONED_NOW), TODAY), formats);
    }

    // ---- legend-engine's steps, in its order (measured on its own jars:
    //      docs/execution-plan-boundary-2026-10-05/probes/engine-validation-results.txt) -----------------------------

    private static final String NOT_BOUND = "Parameter value(s) the plan does not bind: [";

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

    /** legend-engine's checks come first: a value it fails is refused in its words, whatever the runner would refuse
     *  in the same values; the runner's own refusals, of values legend-engine passes on, come after, every one
     *  collected under their own heading. */
    @Test
    void legendEnginesFailuresComeFirst_thenTheRunnersOwnRefusals() {
        // a list for an upper bound of 1: legend-engine validates its elements
        assertEquals("Invalid provided parameter(s): [Unable to process 'Integer' parameter, value: 'x' is not"
                + " parsable.]", refusal(declared("p", "Integer", 1, 1), List.of(1L, "x")));
        assertEquals("Invalid provided parameter(s): [Unable to process 'Boolean' parameter, value: 3.]",
                assertThrows(IllegalArgumentException.class, () -> PlanParameters.check(
                        List.of(declared("a", "Integer", 1, 1), declared("b", "Boolean", 1, 1)),
                        Map.of("a", List.of(1L, 2L), "b", 3))).getMessage());
        // values legend-engine passes on, and the runner refuses
        assertEquals(NOT_BOUND + "parameter 'a' (Integer[1]) takes one value, given 2; parameter 'f' (Float) is NaN: a"
                + " value that is not finite has no SQL literal, and is not bound]",
                assertThrows(IllegalArgumentException.class, () -> PlanParameters.check(
                        List.of(declared("a", "Integer", 1, 1), declared("f", "Float", 1, 1)),
                        Map.of("a", List.of(1L, 2L), "f", "NaN"))).getMessage());
    }

    /** A required parameter given a null value or an empty list is missing: a deliberate difference (legend-engine
     *  counts either present, and its template then writes no statement a database runs, or an empty collection where
     *  one value or more is declared; docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9). An optional one's empty list is
     *  its absence. */
    @Test
    void aNullValueOrAnEmptyListForARequiredParameterIsMissing() {
        Map<String, Object> values = new HashMap<>();
        values.put("p", null);
        assertEquals("Missing external parameter(s): p:Integer[1]", assertThrows(IllegalArgumentException.class,
                () -> PlanParameters.check(List.of(declared("p", "Integer", 1, 1)), values)).getMessage());
        assertEquals("Missing external parameter(s): p:Integer[1]", refusal(declared("p", "Integer", 1, 1), List.of()));
        assertEquals("Missing external parameter(s): p:Integer[1..*]",
                refusal(declared("p", "Integer", 1, null), List.of()));
        assertEquals(new PlanParameters.None(),
                PlanParameters.check(List.of(declared("o", "Integer", 0, 1)), Map.of("o", List.of())).get("o"));
    }

    /** A null element: legend-engine's validation passes it, and its normalizer refuses it where it converts the type
     *  (in its own words, a Float's as a Double); a String's or an enumeration's it passes on, and the runner refuses
     *  (a Pure collection holds no null). */
    @Test
    void aNullElementIsRefused_inLegendEnginesWordsWhereItHasThem() {
        assertEquals("Invalid Integer value: null", refusal(declared("p", "Integer", 0, null), Arrays.asList(1L, null)));
        assertEquals("Invalid Double value: null", refusal(declared("p", "Float", 0, null), Arrays.asList(1.5, null)));
        // validation first: a later element that fails it is named
        assertEquals("Invalid provided parameter(s): [Unable to process 'Integer' parameter, value: 'x' is not"
                + " parsable.]", refusal(declared("p", "Integer", 0, null), Arrays.asList(null, "x")));
        assertEquals(NOT_BOUND + "parameter 'p' (String[*]) is given a null element]",
                refusal(declared("p", "String", 0, null), Arrays.asList("a", null)));
        assertEquals(NOT_BOUND + "parameter 'p' (test::E[*]) is given a null element]",
                refusal(declared("p", "test::E", 0, null, "A"), Arrays.asList("A", null)));
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
        // legend-engine has no validator for a Number: its own message, its types in the order it prints them
        assertEquals("Invalid provided parameter(s): [Unknown external parameter type: Number, valid external parameter"
                + " types: [Float, Byte, meta::pure::metamodel::variant::Variant, DateTime, Date, Decimal, String,"
                + " Integer, Boolean, StrictDate]]", refusal(declared("n", "Number", 1, 1), 1L));
        // a Byte and a Variant: checked as legend-engine checks them (a stream; JSON's text), and a value it passes is
        // not bound by a plan yet
        assertEquals("Invalid provided parameter(s): [Unable to process 'Byte' parameter, value: 1.]",
                refusal(declared("b", "Byte", 1, 1), 1L));
        assertEquals(NOT_BOUND + "parameter 'b' of type Byte is not bound by a plan (PARK-21)]",
                refusal(declared("b", "Byte", 1, 1), new java.io.ByteArrayInputStream(new byte[] {1})));
        String variant = "meta::pure::metamodel::variant::Variant";
        assertEquals("Invalid provided parameter(s): [Unable to process '" + variant + "' parameter, value: 5.]",
                refusal(declared("v", variant, 1, 1), 5L));
        assertEquals(NOT_BOUND + "parameter 'v' of type " + variant + " is not bound by a plan (PARK-21)]",
                refusal(declared("v", variant, 1, 1), "{}"));
        // a list for a parameter of upper bound 1: legend-engine's template writes no SQL a database runs
        assertEquals(NOT_BOUND + "parameter 'p' (Integer[1]) takes one value, given 2]",
                refusal(declared("p", "Integer", 1, 1), List.of(1L, 2L)));
        // a Float that is not finite has no SQL literal (legend-engine passes it, and writes NaN into its statement)
        for (Object v : List.<Object>of("NaN", Double.NaN, Double.POSITIVE_INFINITY, "-Infinity")) {
            String shown = String.valueOf(v instanceof String s ? Double.parseDouble(s) : v);
            assertEquals(NOT_BOUND + "parameter 'p' (Float) is " + shown + ": a value that is not finite has no SQL"
                    + " literal, and is not bound]", refusal(declared("p", "Float", 1, 1), v));
        }
    }

    /** A plan whose parameters fail is refused before any session is opened. */
    @Test
    void aPlanWhoseParametersFailOpensNoSession() {
        ExecutionPlan numbered = new ExecutionPlan(List.of(declared("n", "Number", 1, 1)),
                counting(inMemory("s::Duck", DatabaseType.DuckDB, new ConnectionSpecification.InMemory()), "NUMBER_T",
                        new ExecutionPlan.Servers.Every()).root());
        var refused = assertThrows(IllegalArgumentException.class, () -> PlanRunner.run(numbered, Map.of("n", 1L),
                target -> {
                    throw new AssertionError("a session was opened");
                }, new StringWriter()));
        assertTrue(refused.getMessage().startsWith("Invalid provided parameter(s): [Unknown external parameter type:"
                + " Number"), refused.getMessage());
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
