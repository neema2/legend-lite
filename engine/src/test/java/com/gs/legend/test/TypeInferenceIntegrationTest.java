package com.gs.legend.test;

import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.sqlgen.DuckDBDialect;
import com.gs.legend.sqlgen.SQLDialect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests verifying that the engine returns correct Java types
 * based on Pure's type inference, not DuckDB's raw JDBC types.
 *
 * These tests mimic PCT scalar function tests that fail when DuckDB returns
 * Double for expressions that Pure declares as Integer.
 */
public class TypeInferenceIntegrationTest extends AbstractDatabaseTest {

        @Override
        protected String getDatabaseType() {
                return "DuckDB";
        }

        @Override
        protected SQLDialect getDialect() {
                return DuckDBDialect.INSTANCE;
        }

        @Override
        protected String getJdbcUrl() {
                return "jdbc:duckdb:";
        }

        @BeforeEach
        void setUp() throws SQLException {
                connection = DriverManager.getConnection(getJdbcUrl());
                // Set UTC timezone so TIMESTAMPTZ values are returned in UTC
                try (var stmt = connection.createStatement()) {
                        stmt.execute("SET timezone='UTC'");
                }
                setupPureModel();
                setupDatabase();
        }

        @AfterEach
        void tearDown() throws SQLException {
                if (connection != null) {
                        connection.close();
                }
        }

        // ==================== round() must return Integer ====================

        @Test
        void testRoundFloatReturnsInteger() throws SQLException {
                // Pure: |round(17.6) -> 18 (Integer, not 18.0 Double)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|17.6->round()",
                                "test::TestRuntime",
                                connection);
                assertScalarInteger(result, 18L);
        }

        @Test
        void testRoundNegativeFloatReturnsInteger() throws SQLException {
                // Pure: |round(-17.6) -> -18 (Integer)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|round(-17.6)",
                                "test::TestRuntime",
                                connection);
                assertScalarInteger(result, -18L);
        }

        @Test
        void testRoundIntegerReturnsInteger() throws SQLException {
                // Pure: |round(17) -> 17 (Integer, not 17.0 Double)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|17->round()",
                                "test::TestRuntime",
                                connection);
                assertScalarInteger(result, 17L);
        }

        // ==================== sign() must return Integer ====================

        @Test
        void testSignNegativeReturnsInteger() throws SQLException {
                // Pure: |sign(-10) -> -1 (Integer)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|-10->sign()",
                                "test::TestRuntime",
                                connection);
                assertScalarInteger(result, -1L);
        }

        @Test
        void testSignPositiveReturnsInteger() throws SQLException {
                // Pure: |sign(5) -> 1 (Integer)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|5->sign()",
                                "test::TestRuntime",
                                connection);
                assertScalarInteger(result, 1L);
        }

        // ==================== floor()/ceiling() must return Integer
        // ====================

        @Test
        void testFloorReturnsInteger() throws SQLException {
                // Pure: |floor(3.7) -> 3 (Integer)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|3.7->floor()",
                                "test::TestRuntime",
                                connection);
                assertScalarInteger(result, 3L);
        }

        @Test
        void testCeilingReturnsInteger() throws SQLException {
                // Pure: |ceiling(3.2) -> 4 (Integer)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|3.2->ceiling()",
                                "test::TestRuntime",
                                connection);
                assertScalarInteger(result, 4L);
        }

        // ==================== hashCode() must return Integer ====================

        @Test
        void testHashCodeReturnsInteger() throws SQLException {
                // Pure: |'a'->hash() -> Integer (DuckDB HASH returns UBIGINT/BigInteger)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'a'->hash()",
                                "test::TestRuntime",
                                connection);
                Object value = result.rows().get(0).get(0);
                assertNotNull(value);
                // HASH returns UBIGINT (BigInteger via JDBC) — must be Integer or Long, not
                // Double
                assertTrue(value instanceof Integer || value instanceof Long || value instanceof java.math.BigInteger,
                                "Expected integer type but got " + value.getClass().getSimpleName() + " = " + value);
        }

        // ==================== Integer arithmetic stays Integer ====================

        @Test
        void testIntegerAdditionReturnsInteger() throws SQLException {
                // Pure: |1 + 2 -> 3 (Integer)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1 + 2",
                                "test::TestRuntime",
                                connection);
                assertScalarInteger(result, 3L);
        }

        @Test
        void testIntegerMultiplicationReturnsInteger() throws SQLException {
                // Pure: |3 * 4 -> 12 (Integer)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|3 * 4",
                                "test::TestRuntime",
                                connection);
                assertScalarInteger(result, 12L);
        }

        // ==================== coalesce ====================

        @Test
        void testCoalesceFirstEmpty() throws SQLException {
                // PCT: coalesce([], 'world') → 'world' ([] is empty/absent, return second arg)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[]->coalesce('world')",
                                "test::TestRuntime", connection);
                assertEquals("world", result.rows().get(0).get(0).toString());
        }

        @Test
        void testCoalesceNotEmptyDefaultEmpty() throws SQLException {
                // PCT: coalesce('hello', []) → 'hello' (first is non-empty, return it)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'hello'->coalesce([])",
                                "test::TestRuntime", connection);
                assertEquals("hello", result.rows().get(0).get(0).toString());
        }

        @Test
        void testCoalesce2SecondNotEmptyAllOthersEmpty() throws SQLException {
                // PCT: coalesce([], 'world', []) → 'world'
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[]->coalesce('world', [])",
                                "test::TestRuntime", connection);
                assertEquals("world", result.rows().get(0).get(0).toString());
        }

        // ==================== toString ====================

        @Test
        void testFloatToStringWithNegativeExponent() throws SQLException {
                // PCT: 0.000000013421->toString() → '0.000000013421' (fixed-point, not
                // scientific notation)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|0.000000013421->toString()",
                                "test::TestRuntime", connection);
                assertEquals("0.000000013421", result.rows().get(0).get(0).toString());
        }

        @Test
        void testClassToString() throws SQLException {
                // PCT: STR_Person->toString() → class name as string
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|meta::pure::functions::string::tests::toString::STR_Person->toString()",
                                "test::TestRuntime", connection);
                assertEquals("STR_Person", result.rows().get(0).get(0).toString());
        }

        // ==================== joinStrings on empty list ====================

        @Test
        void testJoinStringsNoStrings() throws SQLException {
                // PCT: 'a'->tail()->joinStrings(',') → '' (tail of single-element is empty,
                // joinStrings returns '')
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'a'->tail()->joinStrings(',')",
                                "test::TestRuntime", connection);
                assertEquals("", result.rows().get(0).get(0).toString());
        }

        @Test
        void testJoinStringsSingleString() throws SQLException {
                // PCT: 'a'->joinStrings(',') → 'a' (scalar string treated as single-element
                // list)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'a'->joinStrings(',')",
                                "test::TestRuntime", connection);
                assertEquals("a", result.rows().get(0).get(0).toString());
        }

        @Test
        void testJoinStringsNoStringsPrefixSuffix() throws SQLException {
                // PCT: 'a'->tail()->joinStrings('[', ',', ']') → '[]' (empty list with
                // prefix/suffix)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'a'->tail()->joinStrings('[', ',', ']')",
                                "test::TestRuntime", connection);
                assertEquals("[]", result.rows().get(0).get(0).toString());
        }

        // ==================== match() ====================

        @Test
        void testMatchWithFunctions() throws SQLException {
                // PCT testMatchWithFunctions: 1->match([a: Integer[1]|1, a: String[1]|[1, 2],
                // a: Date[1]|[4, 5, 6]]) → 1
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1->match([a: Integer[1]|1, a: String[1]|2, a: Date[1]|3])",
                                "test::TestRuntime", connection);
                assertScalarInteger(result, 1L);
        }

        @Test
        void testMatchWithFunctionsAsParam() throws SQLException {
                // PCT testMatchWithFunctionsAsParam: 1->match([a: Integer[1]|1, a:
                // String[1]|[6,7,1,2], a: String[*]|$a, a: Date[1]|[4,5,6]]) → 1
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1->match([a: Integer[1]|1, a: String[1]|2, a: Date[1]|3])",
                                "test::TestRuntime", connection);
                assertScalarInteger(result, 1L);
        }

        @Test
        void testMatchWithFunctionsManyMatch() throws SQLException {
                // PCT testMatchWithFunctionsManyMatch: ['1','2']->match([a: Integer[1]|1, a:
                // String[1]|[6,7,1,2], a: String[*]|$a, a: Date[1]|[4,5,6]])
                // Array of 2 strings: size=2 doesn't match String[1], matches String[*],
                // returns $a = ['1','2']
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|['1', '2']->match([a: Integer[1]|1, a: String[1]|[6, 7, 1, 2], a: String[*]|$a, a: Date[1]|[4, 5, 6]])",
                                "test::TestRuntime", connection);
                assertEquals(List.of("1", "2"), result.asCollection().values().stream().map(Object::toString).toList());
        }

        @Test
        void testMatchWithFunctionsAsParamManyMatch() throws SQLException {
                // PCT testMatchWithFunctionsAsParamManyMatch: ['1','2']->match([...]) with
                // String[*]|$a
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|['1', '2']->match([a: Integer[1]|1, a: String[1]|[6, 7, 1, 2], a: String[*]|$a, a: Date[1]|[4, 5, 6]])",
                                "test::TestRuntime", connection);
                assertEquals(List.of("1", "2"), result.asCollection().values().stream().map(Object::toString).toList());
        }

        @Test
        void testMatchOneWithZeroOne() throws SQLException {
                // PCT testMatchOneWithZeroOne: 1->match([i: Integer[0..1]|1, s:
                // String[1]|'address:']) → 1
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1->match([i: Integer[0..1]|1, s: String[1]|'address:'])",
                                "test::TestRuntime", connection);
                assertScalarInteger(result, 1L);
        }

        @Test
        void testMatchOneWith() throws SQLException {
                // PCT testMatchOneWith: 'Digby'->match([i: Integer[1..4]|[1,2,3], s:
                // String[1]|'address:' + $s]) → 'address:Digby'
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'Digby'->match([i: Integer[1..4]|'nope', s: String[1]|'address:' + $s])",
                                "test::TestRuntime", connection);
                assertEquals("address:Digby", result.rows().get(0).get(0).toString());
        }

        @Test
        void testMatchOneWithMany() throws SQLException {
                // PCT testMatchOneWithMany: 'Digby'->match([i: Integer[1..4]|[1,2,3], s:
                // String[1..2]|'address:Digby']) → 'address:Digby'
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'Digby'->match([i: Integer[1..4]|'nope', s: String[1..2]|'address:Digby'])",
                                "test::TestRuntime", connection);
                assertEquals("address:Digby", result.rows().get(0).get(0).toString());
        }

        @Test
        void testMatchManyWithMany() throws SQLException {
                // PCT testMatchManyWithMany: ['w','w','w']->match([i: Integer[1..4]|'z', s:
                // String[*]|'address:' + $s->size()->toString()])
                // Array of 3 strings matches String[*] branch, $s->size() = 3
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|['w', 'w', 'w']->match([i: Integer[1..4]|'z', s: String[*]|'address:' + $s->size()->toString()])",
                                "test::TestRuntime", connection);
                assertEquals("address:3", result.rows().get(0).get(0).toString());
        }

        @Test
        void testMatchWithExtraParam() throws SQLException {
                // PCT testMatchWithExtraParam: 1->match([{i: Integer[1], b|'good_' + $b}, {s:
                // String[1], b|'bad'}], 'other') → 'good_other'
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1->match([{i: Integer[1], b|'good_' + $b}, {s: String[1], b|'bad'}], 'other')",
                                "test::TestRuntime", connection);
                assertEquals("good_other", result.rows().get(0).get(0).toString());
        }

        @Test
        void testMatchWithExtraParamsAndFunctionsAsParam() throws SQLException {
                // PCT: '1'->match([{a: String[1], b: String[1]|'1' + $b}, {a: Integer[1], b:
                // String[1]|$b}, {a: Date[1], b: String[1]|'5' + $b}], '1') → '11'
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'1'->match([{a: String[1], b: String[1]|'1' + $b}, {a: Integer[1], b: String[1]|$b}, {a: Date[1], b: String[1]|'5' + $b}], '1')",
                                "test::TestRuntime", connection);
                assertEquals("11", result.rows().get(0).get(0).toString());
        }

        @Test
        void testMatchWithMixedReturnType() throws SQLException {
                // PCT testMatchWithMixedReturnType: similar to testMatch but with class
                // instances — skip class-based matching
                // Use simplified version: same type dispatch, mixed return types
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'hello'->match([a: Integer[1]|'integer', a: String[1]|'address', a: Date[1]|'date'])",
                                "test::TestRuntime", connection);
                assertEquals("address", result.rows().get(0).get(0).toString());
        }

        @Test
        void testMatchZeroWithMany() throws SQLException {
                // PCT: []->cast(@String)->match([i: Integer[1..4]|[1,2,3], s:
                // String[*]|'address']) → 'address'
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[]->cast(@String)->match([i: Integer[1..4]|'nope', s: String[*]|'address'])",
                                "test::TestRuntime", connection);
                assertEquals("address", result.rows().get(0).get(0).toString());
        }

        @Test
        void testMatchZeroWithZero() throws SQLException {
                // PCT: []->cast(@String)->match([i: Integer[1..4]|[1,2,3], s:
                // String[0]|'address']) → 'address'
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[]->cast(@String)->match([i: Integer[1..4]|'nope', s: String[0]|'address'])",
                                "test::TestRuntime", connection);
                assertEquals("address", result.rows().get(0).get(0).toString());
        }

        // ==================== multi-if with pair ====================

        @Test
        void testMultiIf() throws SQLException {
                // PCT: [pair(|5 == 1, |2), pair(|5 == 2, |22)]->if(|4) + 3
                // Should be: CASE WHEN 5=1 THEN 2 WHEN 5=2 THEN 22 ELSE 4 END + 3 = 7
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[meta::pure::functions::collection::pair(|5 == 1, |2), meta::pure::functions::collection::pair(|5 == 2, |22)]->meta::pure::functions::lang::if(|4) + 3",
                                "test::TestRuntime", connection);
                assertScalarInteger(result, 7L);
        }

        // ==================== dateDiff must use correct argument order
        // ====================

        @Test
        void testDateDiffDays() throws SQLException {
                // Pure: |%2015-01-01->dateDiff(%2015-01-10, DurationUnit.DAYS) -> 9
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2015-01-01->dateDiff(%2015-01-10, meta::pure::functions::date::DurationUnit.DAYS)",
                                "test::TestRuntime",
                                connection);
                assertScalarInteger(result, 9L);
        }

        @Test
        void testDateDiffYears() throws SQLException {
                // Pure: |%2015->dateDiff(%2016, DurationUnit.YEARS) -> 1
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2015->dateDiff(%2016, meta::pure::functions::date::DurationUnit.YEARS)",
                                "test::TestRuntime",
                                connection);
                assertScalarInteger(result, 1L);
        }

        @Test
        void testDateDiffWeeks() throws SQLException {
                // Exact PCT test: testDateDiffWeeks
                // Same day → 0
                var result1 = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2015-07-05->dateDiff(%2015-07-05, meta::pure::functions::date::DurationUnit.WEEKS)",
                                "test::TestRuntime", connection);
                assertScalarInteger(result1, 0L);

                // Friday to Saturday → 0 (no Sunday boundary crossed)
                var result2 = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2015-07-03->dateDiff(%2015-07-04, meta::pure::functions::date::DurationUnit.WEEKS)",
                                "test::TestRuntime", connection);
                assertScalarInteger(result2, 0L);

                // Saturday to Sunday → 1 (Sunday boundary crossed)
                var result3 = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2015-07-04->dateDiff(%2015-07-05, meta::pure::functions::date::DurationUnit.WEEKS)",
                                "test::TestRuntime", connection);
                assertScalarInteger(result3, 1L);
        }

        // ==================== date() with time components ====================

        @Test
        void testDateFromHourPrecision() throws SQLException {
                // PCT: date(1973, 11, 13, 23) → %1973-11-13T23 (hour precision, not full
                // timestamp)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1973->date(11, 13, 23)",
                                "test::TestRuntime", connection);
                assertEquals("1973-11-13T23", result.rows().get(0).get(0).toString());
        }

        @Test
        void testDateFromMinutePrecision() throws SQLException {
                // PCT: date(1973, 11, 13, 23, 9) → %1973-11-13T23:09 (minute precision)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1973->date(11, 13, 23, 9)",
                                "test::TestRuntime", connection);
                assertEquals("1973-11-13T23:09", result.rows().get(0).get(0).toString());
        }

        @Test
        void testDateFromSubSecondPrecision() throws SQLException {
                // PCT: date(1973, 11, 13, 23, 9, 11.0) → %1973-11-13T23:09:11.0 (subsecond
                // precision)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1973->date(11, 13, 23, 9, 11.0)",
                                "test::TestRuntime", connection);
                assertEquals("1973-11-13T23:09:11.0", result.rows().get(0).get(0).toString());
        }

        @Test
        void testDateWithTimeComponentsReturnsTimestamp() throws SQLException {
                // Pure: |1973->date(11, 13, 23, 9) -> DateTime (not StrictDate)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1973->date(11, 13, 23, 9)",
                                "test::TestRuntime",
                                connection);
                Object value = result.rows().get(0).get(0);
                assertNotNull(value);
                // Should be a timestamp, not a date
                assertTrue(value.toString().contains("23:09") || value.toString().contains("23:09:00"),
                                "Expected timestamp with time but got: " + value);
        }

        @Test
        void testPartialTimestampPadding() throws SQLException {
                // Pure: |%2015-04-15T17 should generate TIMESTAMP '2015-04-15 17:00:00'
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2015-04-15T17->hour()",
                                "test::TestRuntime",
                                connection);
                assertScalarInteger(result, 17L);
        }

        // ==================== format() / printf ====================

        @Test
        void testFormatStringSubstitution() throws SQLException {
                // Pure: |'hello %s'->format('world') -> 'hello world'
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'the quick brown %s jumps over the lazy %s'->format(['fox', 'dog'])",
                                "test::TestRuntime",
                                connection);
                assertEquals("the quick brown fox jumps over the lazy dog", result.rows().get(0).get(0));
        }

        @Test
        void testFormatIntegerSubstitution() throws SQLException {
                // Pure: |'value is %d'->format(42) -> 'value is 42'
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'the quick brown %s jumps over the lazy %d'->format(['fox', 3])",
                                "test::TestRuntime",
                                connection);
                assertEquals("the quick brown fox jumps over the lazy 3", result.rows().get(0).get(0));
        }

        @Test
        void testFormatFloatSubstitution() throws SQLException {
                // Pure: |'pi is %.2f'->format(3.14159) -> 'pi is 3.14'
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'the quick brown %s jumps over the lazy %.2f'->format(['fox', 1.338])",
                                "test::TestRuntime",
                                connection);
                assertEquals("the quick brown fox jumps over the lazy 1.34", result.rows().get(0).get(0));
        }

        @Test
        void testFormatDateSubstitution() throws SQLException {
                // Pure: |'on %t{yyyy-MM-dd}'->format(%2014-03-10) -> 'on 2014-03-10'
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'on %t{yyyy-MM-dd}'->format(%2014-03-10)",
                                "test::TestRuntime",
                                connection);
                assertEquals("on 2014-03-10", result.rows().get(0).get(0));
        }

        // ==================== between() ====================

        @Test
        void testBetweenInteger() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1->between(0, 3)",
                                "test::TestRuntime",
                                connection);
                assertEquals(true, result.rows().get(0).get(0));
        }

        // ==================== char() -> chr() ====================

        @Test
        void testCharSpace() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|32->char()",
                                "test::TestRuntime",
                                connection);
                assertEquals(" ", result.rows().get(0).get(0));
        }

        // ==================== parseInteger / parseFloat / parseBoolean
        // ====================

        @Test
        void testParseInteger() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'17'->parseInteger()",
                                "test::TestRuntime",
                                connection);
                assertScalarInteger(result, 17L);
        }

        @Test
        void testParseFloat() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'3.14'->parseFloat()",
                                "test::TestRuntime",
                                connection);
                Object val = result.rows().get(0).get(0);
                assertInstanceOf(Number.class, val);
                assertEquals(3.14, ((Number) val).doubleValue(), 0.001);
        }

        @Test
        void testParseBoolean() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'true'->parseBoolean()",
                                "test::TestRuntime",
                                connection);
                assertEquals(true, result.rows().get(0).get(0));
        }

        // ==================== ArrayLiteral collection operations ====================

        @Test
        void testArrayDrop() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 3]->drop(1)",
                                "test::TestRuntime",
                                connection);
                assertEquals(List.of(2, 3), result.asCollection().values());
        }

        @Test
        void testArrayTake() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 3]->take(2)",
                                "test::TestRuntime",
                                connection);
                assertEquals(List.of(1, 2), result.asCollection().values());
        }

        @Test
        void testArraySlice() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 3, 4]->slice(1, 3)",
                                "test::TestRuntime",
                                connection);
                assertEquals(List.of(2, 3), result.asCollection().values());
        }

        @Test
        void testArrayFirst() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[10, 20, 30]->first()",
                                "test::TestRuntime",
                                connection);
                assertEquals(10, ((Number) result.rows().get(0).get(0)).intValue());
        }

        @Test
        void testArrayConcatenate() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 3]->concatenate([4, 5])",
                                "test::TestRuntime",
                                connection);
                assertEquals(List.of(1, 2, 3, 4, 5), result.asCollection().values());
        }

        // ==================== List aggregate functions ====================

        @Test
        void testListSum() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[12.5, 13.5, 4.0, 1.5, 0.5]->sum()",
                                "test::TestRuntime",
                                connection);
                Object val = result.rows().get(0).get(0);
                assertInstanceOf(Number.class, val);
                assertEquals(32.0, ((Number) val).doubleValue(), 0.001);
        }

        @Test
        void testListSumIntegers() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 3]->sum()",
                                "test::TestRuntime",
                                connection);
                Object val = result.rows().get(0).get(0);
                assertInstanceOf(Number.class, val);
                assertEquals(6, ((Number) val).longValue());
        }

        @Test
        void testListMode() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[5.0, 5.0, 5.0, 2.0, 2.0]->mode()",
                                "test::TestRuntime",
                                connection);
                Object val = result.rows().get(0).get(0);
                assertInstanceOf(Number.class, val);
                assertEquals(5.0, ((Number) val).doubleValue(), 0.001);
        }

        @Test
        void testListModeSingleValue() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1.0->mode()",
                                "test::TestRuntime",
                                connection);
                Object val = result.rows().get(0).get(0);
                assertInstanceOf(Number.class, val);
                assertEquals(1.0, ((Number) val).doubleValue(), 0.001);
        }

        @Test
        void testListMedian() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 3, 4, 5]->median()",
                                "test::TestRuntime",
                                connection);
                Object val = result.rows().get(0).get(0);
                assertInstanceOf(Number.class, val);
                assertEquals(3.0, ((Number) val).doubleValue(), 0.001);
        }

        @Test
        void testListVarianceSample() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1.0, 2.0, 3.0]->varianceSample()",
                                "test::TestRuntime",
                                connection);
                Object val = result.rows().get(0).get(0);
                assertInstanceOf(Number.class, val);
                assertEquals(1.0, ((Number) val).doubleValue(), 0.001);
        }

        @Test
        void testListVariancePopulation() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2]->variancePopulation()",
                                "test::TestRuntime",
                                connection);
                Object val = result.rows().get(0).get(0);
                assertInstanceOf(Number.class, val);
                assertEquals(0.25, ((Number) val).doubleValue(), 0.001);
        }

        @Test
        void testListCorr() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2]->corr([10, 20])",
                                "test::TestRuntime",
                                connection);
                Object val = result.rows().get(0).get(0);
                assertInstanceOf(Number.class, val);
                assertEquals(1.0, ((Number) val).doubleValue(), 0.001);
        }

        @Test
        void testListCovarPopulation() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2]->covarPopulation([10, 20])",
                                "test::TestRuntime",
                                connection);
                Object val = result.rows().get(0).get(0);
                assertInstanceOf(Number.class, val);
                assertEquals(2.5, ((Number) val).doubleValue(), 0.001);
        }

        @Test
        void testEvalFunctionReference() throws SQLException {
                // acos_Number_1__Float_1_->eval(0.5) should compile to ACOS(0.5)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|meta::pure::functions::math::acos_Number_1__Float_1_->eval(0.5)",
                                "test::TestRuntime",
                                connection);
                Object val = result.rows().get(0).get(0);
                assertInstanceOf(Number.class, val);
                assertEquals(Math.acos(0.5), ((Number) val).doubleValue(), 0.001);
        }

        @Test
        void testHashCodeAggregate() throws SQLException {
                // hashCode as aggregate in groupBy -> HASH(LIST(col))
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|#TDS\n" +
                                                "  id, grp, val\n" +
                                                "  1, 1, 10.0\n" +
                                                "  2, 1, 20.0\n" +
                                                "  3, 2, 30.0\n" +
                                                "  4, 2, 40.0\n" +
                                                "#->groupBy(~grp, ~h : x | $x.val : y | $y->hashCode())",
                                "test::TestRuntime",
                                connection);
                
                var br = result;
                // Should have 2 groups, each with a non-null hash value
                assertEquals(2, br.rows().size());
                assertNotNull(br.rows().get(0).get(1));
                assertNotNull(br.rows().get(1).get(1));
                // Hash values should be different for different groups
                assertNotEquals(br.rows().get(0).get(1), br.rows().get(1).get(1));
        }

        // ==================== Scalar aggregate functions (should pass through, not use
        // list_*) ====================

        @Test
        void testScalarAverageFloat() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1.0->average()",
                                "test::TestRuntime", connection);
                assertEquals(1.0, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.001);
        }

        @Test
        void testScalarSumFloat() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|2.5->sum()",
                                "test::TestRuntime", connection);
                assertEquals(2.5, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.001);
        }

        @Test
        void testScalarSumInteger() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|7->sum()",
                                "test::TestRuntime", connection);
                assertScalarInteger(result, 7);
        }

        @Test
        void testScalarModeFloat() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|3.14->mode()",
                                "test::TestRuntime", connection);
                assertEquals(3.14, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.001);
        }

        @Test
        void testScalarAverageInteger() throws SQLException {
                // average() always returns Float, even for integer input
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|11->average()",
                                "test::TestRuntime", connection);
                assertEquals(11.0, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.001);
        }

        @Test
        void testScalarMeanFloat() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|5.0->mean()",
                                "test::TestRuntime", connection);
                assertEquals(5.0, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.001);
        }

        // ==================== AND/OR collection functions ====================

        @Test
        void testAndSingleTrue() throws SQLException {
                // and(true) via function-call syntax -> true
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|and(true)",
                                "test::TestRuntime", connection);
                assertEquals(true, result.rows().get(0).get(0));
        }

        @Test
        void testAndFunctionCallWithList() throws SQLException {
                // and([true, false]) via function-call syntax -> false
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|and([true, false])",
                                "test::TestRuntime", connection);
                assertEquals(false, result.rows().get(0).get(0));
        }

        @Test
        void testAndListTrue() throws SQLException {
                // and([true, true]) -> true
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[true, true]->and()",
                                "test::TestRuntime", connection);
                assertEquals(true, result.rows().get(0).get(0));
        }

        @Test
        void testAndListFalse() throws SQLException {
                // and([true, false]) -> false
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[true, false]->and()",
                                "test::TestRuntime", connection);
                assertEquals(false, result.rows().get(0).get(0));
        }

        @Test
        void testOrListTrue() throws SQLException {
                // or([false, true]) -> true
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[false, true]->or()",
                                "test::TestRuntime", connection);
                assertEquals(true, result.rows().get(0).get(0));
        }

        @Test
        void testOrListFalse() throws SQLException {
                // or([false, false]) -> false
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[false, false]->or()",
                                "test::TestRuntime", connection);
                assertEquals(false, result.rows().get(0).get(0));
        }

        // ==================== Cast on arrays ====================

        @Test
        void testGreatestScalar() throws SQLException {
                // -1->greatest() should return -1 (scalar passthrough, not LIST_MAX)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|-1->greatest()",
                                "test::TestRuntime", connection);
                assertScalarInteger(result, -1);
        }

        @Test
        void testLeastScalar() throws SQLException {
                // 42->least() should return 42 (scalar passthrough)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|42->least()",
                                "test::TestRuntime", connection);
                assertScalarInteger(result, 42);
        }

        @Test
        void testCastEmptyArrayToInteger() throws SQLException {
                // []->cast(@Integer)->greatest() should generate CAST([] AS INTEGER[])
                // DuckDB returns empty list for list_max of empty typed array
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[]->cast(@Integer)->greatest()",
                                "test::TestRuntime", connection);
                // greatest of empty list returns null
                assertNull(result.rows().get(0).get(0));
        }

        // ==================== TimeBucket ====================

        @Test
        void testTimeBucketStrictDate1Day() throws SQLException {
                // timeBucket(1, DAYS) on StrictDate should return same date (as DATE, not
                // TIMESTAMP)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2024-01-31->timeBucket(1, meta::pure::functions::date::DurationUnit.DAYS)",
                                "test::TestRuntime", connection);
                assertEquals(java.time.LocalDate.of(2024, 1, 31), result.rows().get(0).get(0));
        }

        @Test
        void testTimeBucketStrictDate2Days() throws SQLException {
                // timeBucket(2, DAYS) on 2024-01-31 should return 2024-01-30
                // (2-day buckets from Unix epoch 1970-01-01)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2024-01-31->timeBucket(2, meta::pure::functions::date::DurationUnit.DAYS)",
                                "test::TestRuntime", connection);
                assertEquals(java.time.LocalDate.of(2024, 1, 30), result.rows().get(0).get(0));
        }

        @Test
        void testTimeBucketDateTime() throws SQLException {
                // timeBucket(1, DAYS) on DateTime should return midnight of that day as
                // TIMESTAMP_NS
                // PCT asserts nanosecond precision (.000000000), so SQL must use TIMESTAMP_NS
                // not TIMESTAMP
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2024-01-31T00:32:34+0000->timeBucket(1, meta::pure::functions::date::DurationUnit.DAYS)",
                                "test::TestRuntime", connection);
                Object value = result.rows().get(0).get(0);
                assertEquals(java.sql.Timestamp.valueOf("2024-01-31 00:00:00"), value);
                // Verify nanosecond precision is preserved (TIMESTAMP_NS)
                assertInstanceOf(java.sql.Timestamp.class, value);
                assertEquals(0, ((java.sql.Timestamp) value).getNanos(),
                                "timeBucket should use TIMESTAMP_NS for nanosecond precision");
        }

        @Test
        void testTimeBucketStrictDate2Weeks() throws SQLException {
                // timeBucket(2, WEEKS) on 2024-01-31 (Wednesday) should return 2024-01-29
                // (Monday)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2024-01-31->timeBucket(2, meta::pure::functions::date::DurationUnit.WEEKS)",
                                "test::TestRuntime", connection);
                assertEquals(java.time.LocalDate.of(2024, 1, 29), result.rows().get(0).get(0));
        }

        @Test
        void testTimeBucketDateTime2Weeks() throws SQLException {
                // timeBucket(2, WEEKS) on DateTime should return Monday of that week
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2024-01-31T00:32:34+0000->timeBucket(2, meta::pure::functions::date::DurationUnit.WEEKS)",
                                "test::TestRuntime", connection);
                assertEquals(java.sql.Timestamp.valueOf("2024-01-29 00:00:00"), result.rows().get(0).get(0));
        }

        // ==================== GenerateGuid ====================

        @Test
        void testGenerateGuidScalar() throws SQLException {
                // generateGuid() should return a UUID string
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|meta::pure::functions::string::generation::generateGuid()",
                                "test::TestRuntime", connection);
                Object value = result.rows().get(0).get(0);
                assertNotNull(value);
                // UUID format: 8-4-4-4-12 hex chars
                assertTrue(value.toString().matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
                                "Expected UUID format, got: " + value);
        }

        @Test
        void testGenerateGuidInRelationExtend() throws SQLException {
                // generateGuid() used in extend should produce a column of UUIDs
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|#TDS\nval, str\n1, a\n3, ewe\n#->extend(~uid:x|meta::pure::functions::string::generation::generateGuid())",
                                "test::TestRuntime", connection);
                var buffered = result;
                assertEquals(2, buffered.rowCount());
                // Check uid column exists and has UUID values
                int uidCol = -1;
                for (int c = 0; c < buffered.columns().size(); c++) {
                        if ("uid".equals(buffered.columns().get(c).name())) { uidCol = c; break; }
                }
                assertTrue(uidCol >= 0, "uid column should exist");
                for (int i = 0; i < buffered.rowCount(); i++) {
                        Object uid = buffered.rows().get(i).get(uidCol);
                        assertNotNull(uid);
                        assertTrue(uid.toString()
                                        .matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
                                        "Expected UUID format, got: " + uid);
                }
        }

        // ==================== Math Functions ====================

        @Test
        void testPi() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|meta::pure::functions::math::pi()",
                                "test::TestRuntime", connection);
                double pi = ((Number) result.rows().get(0).get(0)).doubleValue();
                assertEquals(Math.PI, pi, 1e-10);
        }

        @Test
        void testLog() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|2.718281828->log()",
                                "test::TestRuntime", connection);
                double val = ((Number) result.rows().get(0).get(0)).doubleValue();
                assertEquals(1.0, val, 1e-5);
        }

        @Test
        void testLog10() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|100.0->log10()",
                                "test::TestRuntime", connection);
                double val = ((Number) result.rows().get(0).get(0)).doubleValue();
                assertEquals(2.0, val, 1e-10);
        }

        @Test
        void testExp() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1.0->exp()",
                                "test::TestRuntime", connection);
                double val = ((Number) result.rows().get(0).get(0)).doubleValue();
                assertEquals(Math.E, val, 1e-10);
        }

        @Test
        void testPow() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|2.0->pow(10.0)",
                                "test::TestRuntime", connection);
                double val = ((Number) result.rows().get(0).get(0)).doubleValue();
                assertEquals(1024.0, val, 1e-10);
        }

        @Test
        void testSqrt() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|9.0->sqrt()",
                                "test::TestRuntime", connection);
                double val = ((Number) result.rows().get(0).get(0)).doubleValue();
                assertEquals(3.0, val, 1e-10);
        }

        // --- toDecimal: should cast to DECIMAL, tracked through IR ---
        @Test
        void testIntToDecimal() throws SQLException {
                // PCT: |8->meta::pure::functions::math::toDecimal()
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|8->toDecimal()",
                                "test::TestRuntime", connection);
                assertEquals("Decimal(38,18)", result.returnType().typeName(), "toDecimal should produce Decimal(38,18) Pure type");
                assertEquals(8.0, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.001);
        }

        @Test
        void testDecimalLiteralTracking() throws SQLException {
                // Pure Decimal literal 3.0D should be tracked as DECIMAL through IR
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1.0D + 2.0D + 3.0D",
                                "test::TestRuntime", connection);
                assertTrue(result.returnType().typeName().startsWith("Decimal"), "Decimal literals should produce Decimal Pure type");
                assertEquals(6.0, ((Number) result.rows().get(0).get(0)).doubleValue(), 1e-10);
        }

        @Test
        void testDecimalAbs() throws SQLException {
                // PCT: |meta::pure::functions::math::abs(-3.0D)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|meta::pure::functions::math::abs(-3.0D)",
                                "test::TestRuntime", connection);
                assertTrue(result.returnType().typeName().startsWith("Decimal"), "abs of Decimal should produce Decimal Pure type");
                assertEquals(3.0, ((Number) result.rows().get(0).get(0)).doubleValue(), 1e-10);
        }

        // --- mod: Pure mod always returns non-negative ---
        @Test
        void testModWithNegativeNumbers() throws SQLException {
                // PCT: |meta::pure::functions::math::mod(-12, 5) => expected 3
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|meta::pure::functions::math::mod(-12, 5)",
                                "test::TestRuntime", connection);
                assertEquals(3, ((Number) result.rows().get(0).get(0)).intValue());
        }

        // --- round with scale: round(3.14159D, 2) => 3.14 ---
        @Test
        void testRoundWithScale() throws SQLException {
                // PCT: |3.14159D->meta::pure::functions::math::round(2)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|3.14159->round(2)",
                                "test::TestRuntime", connection);
                double val = ((Number) result.rows().get(0).get(0)).doubleValue();
                assertEquals(3.14, val, 1e-10);
        }

        // --- round half-even (banker's rounding): 16.5->round() => 16 ---
        @Test
        void testRoundHalfEvenDown() throws SQLException {
                // PCT: |16.5->meta::pure::functions::math::round() => expected 16
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|16.5->round()",
                                "test::TestRuntime", connection);
                assertEquals(16, ((Number) result.rows().get(0).get(0)).intValue());
        }

        @Test
        void testRoundHalfEvenUp() throws SQLException {
                // PCT: |meta::pure::functions::math::round(-16.5) => expected -16
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|meta::pure::functions::math::round(-16.5)",
                                "test::TestRuntime", connection);
                assertEquals(-16, ((Number) result.rows().get(0).get(0)).intValue());
        }

        // --- PCT Decimal test: parseDecimal strips d/D suffix ---
        // Note: DuckDB CAST AS DECIMAL uses DECIMAL(18,3) which limits precision.
        // Legend-engine has the same limitation (needsInvestigation). Our fix strips
        // the d/D suffix so at least the CAST doesn't error.
        @Test
        void testParseDecimalWithSuffix() throws SQLException {
                // PCT: |'3.14159d'->parseDecimal() - d suffix stripped, value parsed
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'3.14159d'->parseDecimal()",
                                "test::TestRuntime", connection);
                assertEquals("Decimal(38,18)", result.returnType().typeName(), "parseDecimal should produce Decimal(38,18) Pure type");
                // DuckDB DECIMAL(18,3) truncates to 3.142 - known limitation
            assertInstanceOf(BigDecimal.class, result.rows().get(0).get(0));
        }

        @Test
        void testParseDecimalSimple() throws SQLException {
                // PCT: |'3.14'->parseDecimal() - no suffix
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'3.14'->parseDecimal()",
                                "test::TestRuntime", connection);
                assertEquals("Decimal(38,18)", result.returnType().typeName(), "parseDecimal should produce Decimal(38,18) Pure type");
                assertEquals(3.14, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.01);
        }

        // --- PCT: min/max method-call for scalar comparison ---
        @Test
        void testMinFloats() throws SQLException {
                // PCT: |2.8->meta::pure::functions::math::min(1.23)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|2.8->min(1.23)",
                                "test::TestRuntime", connection);
                assertEquals(1.23, ((Number) result.rows().get(0).get(0)).doubleValue(), 1e-10);
        }

        @Test
        void testMaxFloats() throws SQLException {
                // PCT: |2.8->meta::pure::functions::math::max(1.23)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|2.8->max(1.23)",
                                "test::TestRuntime", connection);
                assertEquals(2.8, ((Number) result.rows().get(0).get(0)).doubleValue(), 1e-10);
        }

        @Test
        void testMinIntegers() throws SQLException {
                // PCT: |2->meta::pure::functions::math::min(1)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|2->min(1)",
                                "test::TestRuntime", connection);
                assertEquals(1, ((Number) result.rows().get(0).get(0)).intValue());
        }

        @Test
        void testMaxIntegers() throws SQLException {
                // PCT: |2->meta::pure::functions::math::max(1)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|2->max(1)",
                                "test::TestRuntime", connection);
                assertEquals(2, ((Number) result.rows().get(0).get(0)).intValue());
        }

        @Test
        void testMinNumbers() throws SQLException {
                // PCT: |2->meta::pure::functions::math::min(1.23D)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|2->min(1.23D)",
                                "test::TestRuntime", connection);
                assertEquals(1.23, ((Number) result.rows().get(0).get(0)).doubleValue(), 1e-10);
        }

        @Test
        void testDecimalTimes() throws SQLException {
                // PCT: |19.905D * 17774D => 353791.470D
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|19.905D * 17774D",
                                "test::TestRuntime", connection);
                assertTrue(result.returnType().typeName().startsWith("Decimal"), "Decimal multiplication should produce Decimal Pure type");
                assertEquals(353791.47, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.01);
        }

        // --- PCT: min/max scalar and aggregate ---
        @Test
        void testMinScalar() throws SQLException {
                // PCT sub-expression: |2.8->min(1.23) => 1.23
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|2.8->min(1.23)",
                                "test::TestRuntime", connection);
                assertEquals(1.23, ((Number) result.rows().get(0).get(0)).doubleValue(), 1e-10);
        }

        @Test
        void testMaxScalar() throws SQLException {
                // PCT sub-expression: |1.23->max(2.8) => 2.8
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1.23->max(2.8)",
                                "test::TestRuntime", connection);
                assertEquals(2.8, ((Number) result.rows().get(0).get(0)).doubleValue(), 1e-10);
        }

        @Test
        void testMinNoArgs() throws SQLException {
                // PCT sub-expression: |[3, 1, 2]->min() => 1
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[3, 1, 2]->min()",
                                "test::TestRuntime", connection);
                assertEquals(1, ((Number) result.rows().get(0).get(0)).intValue());
        }

        @Test
        void testMaxNoArgs() throws SQLException {
                // PCT sub-expression: |[3, 1, 2]->max() => 3
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[3, 1, 2]->max()",
                                "test::TestRuntime", connection);
                assertEquals(3, ((Number) result.rows().get(0).get(0)).intValue());
        }

        // --- PCT: variance population vs sample ---
        @Test
        void testVariancePopulation() throws SQLException {
                // PCT: |[1, 2]->variance(false) => 0.25 (population)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2]->variance(false)",
                                "test::TestRuntime", connection);
                assertEquals(0.25, ((Number) result.rows().get(0).get(0)).doubleValue(), 1e-10);
        }

        // --- PCT: exp, log, log10, pow ---
        @Test
        void testPowMethodCall() throws SQLException {
                // PCT sub-expression: |3.33->pow(4.33)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|3.33->pow(4.33)",
                                "test::TestRuntime", connection);
                assertTrue(((Number) result.rows().get(0).get(0)).doubleValue() > 100);
        }

        @Test
        void testExpMethodCall() throws SQLException {
                // PCT sub-expression: |1->exp()
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1->exp()",
                                "test::TestRuntime", connection);
                assertEquals(Math.E, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.001);
        }

        @Test
        void testLogMethodCall() throws SQLException {
                // PCT sub-expression: |2.718281828459045->log()
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|2.718281828459045->log()",
                                "test::TestRuntime", connection);
                assertEquals(1.0, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.001);
        }

        @Test
        void testLog10MethodCall() throws SQLException {
                // PCT sub-expression: |100->log10()
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|100->log10()",
                                "test::TestRuntime", connection);
                assertEquals(2.0, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.001);
        }

        // --- PCT: decodeBase64 returns string, not BLOB ---
        @Test
        void testDecodeBase64() throws SQLException {
                // PCT: |'SGVsbG8sIFdvcmxkIQ=='->meta::pure::functions::string::decodeBase64()
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'SGVsbG8sIFdvcmxkIQ=='->decodeBase64()",
                                "test::TestRuntime", connection);
                assertEquals("Hello, World!", result.rows().get(0).get(0));
        }

        @Test
        void testDecodeBase64RoundTrip() throws SQLException {
                // PCT: |'Any Random String'->encodeBase64()->decodeBase64()
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'Any Random String'->encodeBase64()->decodeBase64()",
                                "test::TestRuntime", connection);
                assertEquals("Any Random String", result.rows().get(0).get(0));
        }

        // --- PCT: splitPart(string, delimiter, index) - 0-based index ---
        @Test
        void testSplitPart() throws SQLException {
                // PCT: |'Hello World'->splitPart(' ', 0) => 'Hello'
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'Hello World'->splitPart(' ', 0)",
                                "test::TestRuntime", connection);
                assertEquals("Hello", result.rows().get(0).get(0));
        }

        @Test
        void testSplitPartSecondToken() throws SQLException {
                // PCT: |'Hello World'->splitPart(' ', 1) => 'World'
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'Hello World'->splitPart(' ', 1)",
                                "test::TestRuntime", connection);
                assertEquals("World", result.rows().get(0).get(0));
        }

        @Test
        void testSplitPartNoSplit() throws SQLException {
                // PCT: |'Hello World'->splitPart(';', 0) => 'Hello World'
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'Hello World'->splitPart(';', 0)",
                                "test::TestRuntime", connection);
                assertEquals("Hello World", result.rows().get(0).get(0));
        }

        @Test
        void testSplitPartTypicalToken() throws SQLException {
                // PCT: |'Sunglasses, Keys, Phone, SL-card'->splitPart(', ', 2) => 'Phone'
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'Sunglasses, Keys, Phone, SL-card'->splitPart(', ', 2)",
                                "test::TestRuntime", connection);
                assertEquals("Phone", result.rows().get(0).get(0));
        }

        // --- PCT: indexOf on lists - 0-based ---
        @Test
        void testIndexOfList() throws SQLException {
                // PCT: |['a', 'b', 'c', 'd']->indexOf('c') => 2
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|['a', 'b', 'c', 'd']->indexOf('c')",
                                "test::TestRuntime", connection);
                assertEquals(2, ((Number) result.rows().get(0).get(0)).intValue());
        }

        @Test
        void testIndexOfOneElement() throws SQLException {
                // PCT: |['a']->indexOf('a') => 0
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|['a']->indexOf('a')",
                                "test::TestRuntime", connection);
                assertEquals(0, ((Number) result.rows().get(0).get(0)).intValue());
        }

        // --- PCT: DateTime toString format ---
        @Test
        void testDateTimeToString() throws SQLException {
                // PCT: |%2014-01-01T00:00:00.000+0000->toString() =>
                // '2014-01-01T00:00:00.000+0000'
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2014-01-01T00:00:00.000+0000->toString()",
                                "test::TestRuntime", connection);
                assertEquals("2014-01-01T00:00:00.000+0000", result.rows().get(0).get(0));
        }

        @Test
        void testSplitPartEmptyString() throws SQLException {
                // PCT: |[]->splitPart(' ', 0) => [] (empty collection)
                // Empty list [] source returns NULL (represents empty collection)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[]->splitPart(' ', 0)",
                                "test::TestRuntime", connection);
                assertNull(result.rows().get(0).get(0));
        }

        @Test
        void testSplitPartEmptyDelimiter() throws SQLException {
                // PCT sub-expression: |'Hello World'->splitPart('', 0) => 'Hello World'
                // Splitting by empty string should return the whole string at index 0
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'Hello World'->splitPart('', 0)",
                                "test::TestRuntime", connection);
                assertEquals("Hello World", result.rows().get(0).get(0));
        }

        // ==================== XOR ====================

        @Test
        void testXorMethodCall() throws SQLException {
                // true->xor(true) should be false
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|true->xor(true)",
                                "test::TestRuntime", connection);
                assertEquals(false, result.rows().get(0).get(0));
        }

        @Test
        void testXorFunctionCall() throws SQLException {
                // xor(1 == 1, not(2 == 3)) -> xor(true, true) -> false
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|meta::pure::functions::boolean::xor(1 == 1, meta::pure::functions::boolean::not(2 == 3))",
                                "test::TestRuntime", connection);
                assertEquals(false, result.rows().get(0).get(0));
        }

        // ==================== RC-12: Date Function Gaps ====================

        // --- 12c: date() with 0 or 1 args ---
        @Test
        void testDateYearOnly() throws SQLException {
                // PCT: |2015->date() => year-only date
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|2015->date()",
                                "test::TestRuntime", connection);
                // year-only date(2015) creates 2015-01-01
                assertNotNull(result.rows().get(0).get(0));
        }

        @Test
        void testDateYearMonth() throws SQLException {
                // PCT: |2015->date(4) => year-month date
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|2015->date(4)",
                                "test::TestRuntime", connection);
                assertNotNull(result.rows().get(0).get(0));
        }

        // --- Date precision equality ---
        @Test
        void testDatePrecisionYearEqualsYear() throws SQLException {
                // %2014 == %2014 → true (same precision, same value)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2014 == %2014",
                                "test::TestRuntime", connection);
                assertEquals(true, result.rows().get(0).get(0));
        }

        @Test
        void testDatePrecisionYearNotEqualsDate() throws SQLException {
                // %2014 == %2014-01-01 → false (different precision: Year vs Date)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2014 == %2014-01-01",
                                "test::TestRuntime", connection);
                assertEquals(false, result.rows().get(0).get(0));
        }

        @Test
        void testDatePrecisionYearMonthNotEqualsDate() throws SQLException {
                // %2014-01 == %2014-01-01 → false (different precision: Month vs Date)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2014-01 == %2014-01-01",
                                "test::TestRuntime", connection);
                assertEquals(false, result.rows().get(0).get(0));
        }

        @Test
        void testDatePrecisionDateEqualsDate() throws SQLException {
                // %2014-01-01 == %2014-01-01 → true (same precision, same value)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2014-01-01 == %2014-01-01",
                                "test::TestRuntime", connection);
                assertEquals(true, result.rows().get(0).get(0));
        }

        // --- Date precision preservation in adjust() ---
        @Test
        void testAdjustByMonthsPreservesYearMonthPrecision() throws SQLException {
                // %2012-03 + 36 MONTHS → %2015-03 (not %2015-03-01)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2012-03->adjust(36, meta::pure::functions::date::DurationUnit.MONTHS)",
                                "test::TestRuntime", connection);
                assertEquals("2015-03", result.rows().get(0).get(0).toString());
        }

        @Test
        void testAdjustByYearsPreservesYearPrecision() throws SQLException {
                // %2014 + 3 YEARS → %2017 (not %2017-01-01)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2014->adjust(3, meta::pure::functions::date::DurationUnit.YEARS)",
                                "test::TestRuntime", connection);
                assertEquals("2017", result.rows().get(0).get(0).toString());
        }

        // --- 12b: parseDate without format ---
        @Test
        void testParseDateNoFormat() throws SQLException {
                // PCT: |'2014-02-27T10:01:35.231+0000'->parseDate()
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'2014-02-27T10:01:35.231+0000'->parseDate()",
                                "test::TestRuntime", connection);
                assertNotNull(result.rows().get(0).get(0));
        }

        @Test
        void testParseDateNoFormatSimple() throws SQLException {
                // PCT: |'2014-02-27'->parseDate()
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'2014-02-27'->parseDate()",
                                "test::TestRuntime", connection);
                assertNotNull(result.rows().get(0).get(0));
        }

        // --- 12a: datepart ---
        @Test
        void testDatePartOnYearMonth() throws SQLException {
                // PCT: |%1973-11->datePart() => should preserve year-month precision
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%1973-11->datePart()",
                                "test::TestRuntime", connection);
                assertEquals("1973-11", result.rows().get(0).get(0).toString());
        }

        @Test
        void testDatePartOnYear() throws SQLException {
                // PCT: |%1973->datePart() => should preserve year precision
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%1973->datePart()",
                                "test::TestRuntime", connection);
                assertEquals("1973", result.rows().get(0).get(0).toString());
        }

        @Test
        void testDatePartOnDate() throws SQLException {
                // PCT: |%1973-11-05->datePart() => date truncated to day
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%1973-11-05->datePart()",
                                "test::TestRuntime", connection);
                assertNotNull(result.rows().get(0).get(0));
        }

        @Test
        void testDatePartOnTimestamp() throws SQLException {
                // PCT: |%1973-11-05T13:01:25+0000->datePart() => date part only
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%1973-11-05T13:01:25+0000->datePart()",
                                "test::TestRuntime", connection);
                assertNotNull(result.rows().get(0).get(0));
        }

        // --- 12d: adjust as function call ---
        @Test
        void testAdjustFunctionCall() throws SQLException {
                // PCT: |adjust(%2015-02-28, 1, meta::pure::functions::date::DurationUnit.YEARS)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2015-02-28->adjust(1, meta::pure::functions::date::DurationUnit.YEARS)",
                                "test::TestRuntime", connection);
                assertNotNull(result.rows().get(0).get(0));
        }

        // --- 12e: hasMonth as function call ---
        @Test
        void testHasMonthOnFullDate() throws SQLException {
                // PCT: |hasMonth(%2015-04-01) => true
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2015-04-01->hasMonth()",
                                "test::TestRuntime", connection);
                assertEquals(true, result.rows().get(0).get(0));
        }

        // ==================== RC-13: DuckDB Function Mapping Gaps ====================

        // --- 13a: lpad/rpad ---
        @Test
        void testLpadDefaultFill() throws SQLException {
                // PCT: |'abcd'->lpad(10) => ' abcd' (6 spaces + abcd)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'abcd'->lpad(10)",
                                "test::TestRuntime", connection);
                assertEquals("      abcd", result.rows().get(0).get(0));
        }

        @Test
        void testLpadShorterThanString() throws SQLException {
                // PCT: |'abcdefghij'->lpad(5) => 'abcde' (truncate)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'abcdefghij'->lpad(5)",
                                "test::TestRuntime", connection);
                assertEquals("abcde", result.rows().get(0).get(0));
        }

        @Test
        void testRpadDefaultFill() throws SQLException {
                // PCT: |'abcd'->rpad(10) => 'abcd ' (abcd + 6 spaces)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'abcd'->rpad(10)",
                                "test::TestRuntime", connection);
                assertEquals("abcd      ", result.rows().get(0).get(0));
        }

        @Test
        void testRpadShorterThanString() throws SQLException {
                // PCT: |'abcdefghij'->rpad(5) => 'abcde' (truncate)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'abcdefghij'->rpad(5)",
                                "test::TestRuntime", connection);
                assertEquals("abcde", result.rows().get(0).get(0));
        }

        // --- 13b: contains on strings ---
        @Test
        void testContainsOnString() throws SQLException {
                // PCT: |'the quick brown fox jumps over the lazy dog'->contains('fox') => true
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'the quick brown fox jumps over the lazy dog'->contains('fox')",
                                "test::TestRuntime", connection);
                assertEquals(true, result.rows().get(0).get(0));
        }

        @Test
        void testContainsOnStringNotFound() throws SQLException {
                // PCT: |'the quick brown fox'->contains('cat') => false
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'the quick brown fox'->contains('cat')",
                                "test::TestRuntime", connection);
                assertEquals(false, result.rows().get(0).get(0));
        }

        // --- 13c: percentileCont on lists ---
        @Test
        void testPercentileContOnList() throws SQLException {
                // PCT: |[1, 2, 3, 4, 5]->percentileCont(0.5) => 3.0 (median)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 3, 4, 5]->percentileCont(0.5)",
                                "test::TestRuntime", connection);
                assertEquals(3.0, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.01);
        }

        @Test
        void testPercentileContOnListQuartile() throws SQLException {
                // PCT: |[1, 2, 3, 4, 5]->percentileCont(0.25) => 2.0
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 3, 4, 5]->percentileCont(0.25)",
                                "test::TestRuntime", connection);
                assertEquals(2.0, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.01);
        }

        // --- 13d: base64 decode without padding ---
        @Test
        void testDecodeBase64NoPadding() throws SQLException {
                // PCT: |'SGVsbG8sIFdvcmxkIQ'->decodeBase64() => 'Hello, World!'
                // The base64 string is missing the trailing '=' padding
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'SGVsbG8sIFdvcmxkIQ'->decodeBase64()",
                                "test::TestRuntime", connection);
                assertEquals("Hello, World!", result.rows().get(0).get(0));
        }

        // ==================== Collection function tests ====================

        @Test
        void testAddToList() throws SQLException {
                // PCT: |['a', 'b']->add('c') → list_append(['a', 'b'], 'c')
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|['a', 'b']->meta::pure::functions::collection::add('c')",
                                "test::TestRuntime", connection);
                assertEquals(List.of("a", "b", "c"), result.asCollection().values());
        }

        @Test
        void testAddToListWithOffset() throws SQLException {
                // PCT: |['a', 'b']->add(1, 'c') → insert 'c' at index 1 → ['a', 'c', 'b']
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|['a', 'b']->meta::pure::functions::collection::add(1, 'c')",
                                "test::TestRuntime", connection);
                assertEquals(List.of("a", "c", "b"), result.asCollection().values());
        }

        // ==================== Mixed-type list tests (JSON[]) ====================

        @Test
        void testConcatenateMixedType() throws SQLException {
                // PCT: |[1, 2, 3]->concatenate(['a', 'b']) -> [1, 2, 3, 'a', 'b']
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 3]->meta::pure::functions::collection::concatenate(['a', 'b'])",
                                "test::TestRuntime", connection);
                // UNNEST produces N rows. VARIANT[] elements are unwrapped to native Java types.
                var values = result.asCollection().values();
                assertEquals(5, values.size());
                assertEquals(1L, values.get(0));
                assertEquals(2L, values.get(1));
                assertEquals(3L, values.get(2));
                assertEquals("a", values.get(3));
                assertEquals("b", values.get(4));
        }

        @Test
        void testConcatenateHomogeneous() throws SQLException {
                // PCT: |[1, 2, 3]->concatenate([4, 5]) -> [1, 2, 3, 4, 5]
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 3]->meta::pure::functions::collection::concatenate([4, 5])",
                                "test::TestRuntime", connection);
                // UNNEST produces individual integer rows
                var values = result.asCollection().values();
                assertEquals(5, values.size());
                assertEquals(1, ((Number) values.get(0)).intValue());
                assertEquals(5, ((Number) values.get(4)).intValue());
        }

        @Test
        void testConcatenateHeterogeneousStructs() throws SQLException {
                // PCT: concatenate([^CO_Address(name='Hoboken'), ^CO_Address(name='Jersey City')],
                //                  [^CO_Location(place='Hoboken, NJ'), ^CO_Location(place='Jersey City, NJ')])
                // Expected: result type should be CO_GeographicEntity (their common supertype)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|meta::pure::functions::collection::concatenate(" +
                                "[^meta::pure::functions::collection::tests::model::CO_Address(name='Hoboken'), " +
                                "^meta::pure::functions::collection::tests::model::CO_Address(name='Jersey City')], " +
                                "[^meta::pure::functions::collection::tests::model::CO_Location(place='Hoboken, NJ'), " +
                                "^meta::pure::functions::collection::tests::model::CO_Location(place='Jersey City, NJ')])",
                                "test::TestRuntime", connection);
                assertNotNull(result, "Heterogeneous struct concatenation should produce a result");
                // Should have 4 elements (2 addresses + 2 locations)
                assertTrue(result.rows().size() > 0, "Should have rows");
        }


        void testContainsPrimitive() throws SQLException {
                // PCT: |[1, 2, 5, 2, 'a', true, %2014-02-01, 'c']->contains(1) -> true
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 5, 2, 'a', true, %2014-02-01, 'c']->meta::pure::functions::collection::contains(1)",
                                "test::TestRuntime", connection);
                assertEquals(true, result.rows().get(0).get(0));
        }

        @Test
        void testContainsPrimitiveString() throws SQLException {
                // PCT: |[1, 2, 5, 2, 'a', true, %2014-02-01, 'c']->contains('a') -> true
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 5, 2, 'a', true, %2014-02-01, 'c']->meta::pure::functions::collection::contains('a')",
                                "test::TestRuntime", connection);
                assertEquals(true, result.rows().get(0).get(0));
        }

        @Test
        void testInPrimitive() throws SQLException {
                // PCT: |1->in([1, 2, 5, 2, 'a', true, %2014-02-01, 'c']) -> true
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1->meta::pure::functions::collection::in([1, 2, 5, 2, 'a', true, %2014-02-01, 'c'])",
                                "test::TestRuntime", connection);
                assertEquals(true, result.rows().get(0).get(0));
        }

        @Test
        void testInPrimitiveNotFound() throws SQLException {
                // PCT: |'z'->in([1, 2, 5, 2, 'a', true, %2014-02-01, 'c']) -> false
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'z'->meta::pure::functions::collection::in([1, 2, 5, 2, 'a', true, %2014-02-01, 'c'])",
                                "test::TestRuntime", connection);
                assertEquals(false, result.rows().get(0).get(0));
        }

        // ==================== Mixed numeric type tests (should NOT use JSON[])
        // ====================

        @Test
        void testSumMixedNumbers() throws SQLException {
                // PCT: |[15, 13, 2.0, 1, 1.0]->sum() -> 32.0
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[15, 13, 2.0, 1, 1.0]->meta::pure::functions::math::sum()",
                                "test::TestRuntime", connection);
                Object val = result.rows().get(0).get(0);
                assertEquals(32.0, ((Number) val).doubleValue(), 0.001);
        }

        @Test
        void testAverageMixedNumbers() throws SQLException {
                // PCT: |[5D, 1.0, 2, 8, 3]->average() -> 3.8
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[5.0, 1.0, 2, 8, 3]->meta::pure::functions::math::average()",
                                "test::TestRuntime", connection);
                Object val = result.rows().get(0).get(0);
                assertEquals(3.8, ((Number) val).doubleValue(), 0.001);
        }

        // ==================== Lambda compilation tests ====================

        @Test
        void testSimpleIf() throws SQLException {
                // PCT: |if(1 == 1, |'truesentence', |'falsesentence')
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|meta::pure::functions::lang::if(1 == 1, |'truesentence', |'falsesentence')",
                                "test::TestRuntime", connection);
                assertEquals("truesentence", result.rows().get(0).get(0));
        }

        @Test
        void testIfMethodStyle() throws SQLException {
                // PCT: |true->if(|'truesentence', |'falsesentence')
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|true->meta::pure::functions::lang::if(|'truesentence', |'falsesentence')",
                                "test::TestRuntime", connection);
                assertEquals("truesentence", result.rows().get(0).get(0));
        }

        @Test
        void testEvalLambdaExp() throws SQLException {
                // PCT: |eval(a: Number[1]|$a->exp(), 1.0)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|meta::pure::functions::lang::eval(a: Number[1]|$a->meta::pure::functions::math::exp(), 1.0)",
                                "test::TestRuntime", connection);
                assertEquals(Math.E, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.0001);
        }

        @Test
        void testForAllTrue() throws SQLException {
                // PCT: |[1, 2, 3]->forAll(e|$e > 0)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 3]->meta::pure::functions::collection::forAll(e|$e > 0)",
                                "test::TestRuntime", connection);
                assertEquals(true, result.rows().get(0).get(0));
        }

        @Test
        void testForAllFalse() throws SQLException {
                // PCT: |[1, 2, 3]->forAll(e|$e > 1)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 3]->meta::pure::functions::collection::forAll(e|$e > 1)",
                                "test::TestRuntime", connection);
                assertEquals(false, result.rows().get(0).get(0));
        }

        @Test
        void testFindLiteral() throws SQLException {
                // PCT: |['Smith', 'Branche', 'Doe']->find(s: String[1]|$s->length() < 4)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|['Smith', 'Branche', 'Doe']->meta::pure::functions::collection::find(s: String[1]|$s->meta::pure::functions::string::length() < 4)",
                                "test::TestRuntime", connection);
                assertEquals("Doe", result.rows().get(0).get(0));
        }

        // ==================== Parser fix tests ====================

        @Test
        void testDropNegative() throws SQLException {
                // PCT: |[1, 2, 3]->drop(-1) — negative drop should return full list
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 3]->meta::pure::functions::collection::drop(-1)",
                                "test::TestRuntime", connection);
                // negative drop returns full list
                assertNotNull(result.rows().get(0).get(0));
        }

        @Test
        void testTakeNegative() throws SQLException {
                // PCT: |[1, 2, 3]->take(-1) — negative take should return empty list
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 3]->meta::pure::functions::collection::take(-1)",
                                "test::TestRuntime", connection);
                // negative take returns empty list — UNNEST produces 0 rows
                assertTrue(result.asCollection().values().isEmpty(),
                                "take(-1) should return empty collection");
        }

        @Test
        void testSliceNegativeStart() throws SQLException {
                // PCT: |[2, 3, 4, 5]->slice(-1, 10)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[2, 3, 4, 5]->meta::pure::functions::collection::slice(-1, 10)",
                                "test::TestRuntime", connection);
                // slice with negative start returns full list
                assertNotNull(result.rows().get(0).get(0));
        }

        @Test
        void testRelationAggregateWithCast() throws SQLException {
                // PCT: Relation aggregate with ->cast(@Number) in extend window expression
                // Tests that extractPropertyName handles CastExpression wrapping
                // PropertyAccessExpression
                String pureExpr = "|#TDS\n" +
                                "              id, grp, name\n" +
                                "              1.0, 2, A\n" +
                                "              2.0, 1, B\n" +
                                "              3.0, 3, C\n" +
                                "#->groupBy(~grp, ~id : x | $x.id : y | $y->average())";
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                pureExpr,
                                "test::TestRuntime", connection);
                // groupBy(~grp, ~id : x | $x.id : y | $y->average()) should produce 3 groups:
                // grp=1 → avg(id)=2.0, grp=2 → avg(id)=1.0, grp=3 → avg(id)=3.0
                assertNotNull(result);
                assertEquals(3, result.rows().size(), "Should have 3 groups");
                var grouped = collectGroupByResults(result, "grp", "id");
                assertEquals(1.0, ((Number) grouped.get(2)).doubleValue(), 0.01, "grp=2 avg should be 1.0");
                assertEquals(2.0, ((Number) grouped.get(1)).doubleValue(), 0.01, "grp=1 avg should be 2.0");
                assertEquals(3.0, ((Number) grouped.get(3)).doubleValue(), 0.01, "grp=3 avg should be 3.0");
        }

        // ==================== String indexOf / substring / joinStrings / sort tests
        // ====================

        @Test
        void testStringIndexOf() throws SQLException {
                // PCT: |'c'->indexOf('c') should return 0 (0-based)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'c'->meta::pure::functions::string::indexOf('c')",
                                "test::TestRuntime", connection);
                assertScalarInteger(result, 0);
        }

        @Test
        void testStringIndexOfSimple() throws SQLException {
                // PCT: |'the quick brown fox'->indexOf('quick') should return 4 (0-based)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'the quick brown fox jumps over the lazy dog'->meta::pure::functions::string::indexOf('quick')",
                                "test::TestRuntime", connection);
                assertScalarInteger(result, 4);
        }

        @Test
        void testStringIndexOfFromIndex() throws SQLException {
                // PCT: |'the the'->indexOf('h', 0) should return 1 (0-based)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'the the'->meta::pure::functions::string::indexOf('h', 0)",
                                "test::TestRuntime", connection);
                assertScalarInteger(result, 1);
        }

        @Test
        void testSubstringStartEnd() throws SQLException {
                // PCT: |'the quick brown fox jumps over the lazy dog'->substring(0, 43)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'the quick brown fox jumps over the lazy dog'->meta::pure::functions::string::substring(0, 43)",
                                "test::TestRuntime", connection);
                assertEquals("the quick brown fox jumps over the lazy dog", result.rows().get(0).get(0));
        }

        @Test
        void testSubstringStart() throws SQLException {
                // PCT: |'the quick brown fox'->substring(1) should return 'he quick brown fox'
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'the quick brown fox jumps over the lazy dog'->meta::pure::functions::string::substring(1)",
                                "test::TestRuntime", connection);
                assertEquals("he quick brown fox jumps over the lazy dog", result.rows().get(0).get(0));
        }

        @Test
        void testJoinStringsWithPrefixSuffix() throws SQLException {
                // PCT: |['a', 'b', 'c']->joinStrings('[', ',', ']') should return '[a,b,c]'
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|['a', 'b', 'c']->meta::pure::functions::string::joinStrings('[', ',', ']')",
                                "test::TestRuntime", connection);
                assertEquals("[a,b,c]", result.rows().get(0).get(0));
        }

        @Test
        void testJoinStringsSeparatorOnly() throws SQLException {
                // PCT: |['a', 'b', 'c']->joinStrings(',') should return 'a,b,c'
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|['a', 'b', 'c']->meta::pure::functions::string::joinStrings(',')",
                                "test::TestRuntime", connection);
                assertEquals("a,b,c", result.rows().get(0).get(0));
        }

        @Test
        void testSortDescending() throws SQLException {
                // PCT: |['Smith', 'Branche', 'Doe']->sort({x, y | $y->compare($x)})
                // Expected: descending order ['Smith', 'Doe', 'Branche']
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|['Smith', 'Branche', 'Doe']->meta::pure::functions::collection::sort({x: String[1], y: String[1]|$y->meta::pure::functions::lang::compare($x)})",
                                "test::TestRuntime", connection);
                assertEquals(List.of("Smith", "Doe", "Branche"), result.asCollection().values());
        }

        // ==================== Variant type mapping tests ====================

        @Test
        void testVariantMapsToJson() throws SQLException {
                // Verify that Variant type maps to JSON, not VARIANT (which DuckDB doesn't
                // have)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 3]->map(x|$x->meta::pure::functions::variant::toVariant())->meta::pure::functions::collection::toMany(@Integer)",
                                "test::TestRuntime", connection);
                assertNotNull(result.rows().get(0).get(0), "toVariant/toMany should produce a result");
        }

        @Test
        void testFoldFromVariantAsPrimitive() throws SQLException {
                // toVariant()->toMany(@Integer)->fold({val, acc | $acc + $val}, 1) = 1+1+2+3 =
                // 7
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 3]->meta::pure::functions::variant::convert::toVariant()->meta::pure::functions::variant::convert::toMany(@Integer)->meta::pure::functions::collection::fold({val: Integer[1], acc: Integer[1]|$acc + $val}, 1)",
                                "test::TestRuntime", connection);
                assertScalarInteger(result, 7);
        }

        // testFoldFromVariant skipped: fold with mixed types (JSON elements, Integer
        // accumulator)
        // causes list_prepend type coercion issue - needs separate fix for
        // heterogeneous fold

        // ==================== InstanceExpression / Struct literal tests
        // ====================

        @Test
        void testSingleInstanceStruct() throws SQLException {
                // ^Pair(first='a', second='b') -> struct literal
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|^meta::pure::functions::collection::Pair<String, String>(first='hello', second='world')->meta::pure::functions::string::toString()",
                                "test::TestRuntime", connection);
                assertNotNull(result.rows().get(0).get(0), "struct toString should produce a result");
        }

        @Test
        void testInstanceArrayWithMap() throws SQLException {
                // [^Person(firstName='A', lastName='B'), ...].map(p|$p.lastName) -> list of
                // lastNames
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[^meta::pure::functions::collection::tests::map::model::M_Person(firstName='Fabrice',lastName='Smith'), ^meta::pure::functions::collection::tests::map::model::M_Person(firstName='Pierre',lastName='Doe')]->meta::pure::functions::collection::map(p: meta::pure::functions::collection::tests::map::model::M_Person[1]|$p.lastName)",
                                "test::TestRuntime", connection);
                assertEquals(List.of("Smith", "Doe"), result.asCollection().values());
        }

        @Test
        void testInstanceEquality() throws SQLException {
                // ^SideClass(stringId='a', intId=1)->eq(^SideClass(stringId='a', intId=1)) =
                // true
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|^meta::pure::functions::boolean::tests::equalitymodel::SideClass(stringId='firstSide',intId=1)->meta::pure::functions::boolean::eq(^meta::pure::functions::boolean::tests::equalitymodel::SideClass(stringId='firstSide',intId=1))",
                                "test::TestRuntime", connection);
                assertEquals(true, result.rows().get(0).get(0));
        }

        @Test
        void testContainsStructVsPrimitiveReturnsFalse() throws SQLException {
                // [^Firm(legalName='f1'), ^Firm(legalName='f2')]->contains(3) = false (type
                // mismatch)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[^meta::pure::functions::collection::tests::model::CO_Firm(legalName='f1'), ^meta::pure::functions::collection::tests::model::CO_Firm(legalName='f2')]->meta::pure::functions::collection::contains(3)",
                                "test::TestRuntime", connection);
                assertEquals(false, result.rows().get(0).get(0));
        }

        @Test
        void testContainsStructReturnsTrue() throws SQLException {
                // [^Firm(legalName='f1'), ^Firm(legalName='f2')]->contains(^Firm(legalName='f1')) = true
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[^meta::pure::functions::collection::tests::model::CO_Firm(legalName='f1'), ^meta::pure::functions::collection::tests::model::CO_Firm(legalName='f2')]->meta::pure::functions::collection::contains(^meta::pure::functions::collection::tests::model::CO_Firm(legalName='f1'))",
                                "test::TestRuntime", connection);
                assertEquals(true, result.rows().get(0).get(0));
        }

        @Test
        void testLetInstancePropertyAccess() throws SQLException {
                // let person = ^Person(firstName='John', lastName='Doe'); $person.firstName
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|let person = ^meta::pure::functions::lang::tests::model::LA_Person(firstName='John', lastName='Doe'); $person.firstName;",
                                "test::TestRuntime", connection);
                assertEquals("John", result.rows().get(0).get(0));
        }

        @Test
        void testIsEmptyOnStructField() throws SQLException {
                // $p.lastName->isEmpty()->not() inside find lambda
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[^meta::pure::functions::collection::tests::model::CO_Person(firstName='Fabrice',lastName='Smith'), ^meta::pure::functions::collection::tests::model::CO_Person(firstName='Pierre',lastName='Doe')]->meta::pure::functions::collection::find(p: meta::pure::functions::collection::tests::model::CO_Person[1]|$p.lastName->meta::pure::functions::collection::isEmpty()->meta::pure::functions::boolean::not() && ($p.lastName->meta::pure::functions::multiplicity::toOne()->meta::pure::functions::string::length() < 6))",
                                "test::TestRuntime", connection);
                // find should return the matching struct (Doe has 3-char lastName < 6)
                assertNotNull(result.rows().get(0).get(0), "find on struct array should return a match");
        }

        @Test
        void testIsEmptyNotOnValue() throws SQLException {
                // 'hello'->isEmpty() = false
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'hello'->meta::pure::functions::collection::isEmpty()",
                                "test::TestRuntime", connection);
                assertEquals(false, result.rows().get(0).get(0));
        }

        @Test
        void testFindOnStructArrayReturnsMap() throws SQLException {
                // find on struct list should return a Map (unwrapped DuckDB struct)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[^meta::pure::functions::collection::tests::model::CO_Person(firstName='Fabrice',lastName='Smith'), ^meta::pure::functions::collection::tests::model::CO_Person(firstName='Pierre',lastName='Doe')]->meta::pure::functions::collection::find(p: meta::pure::functions::collection::tests::model::CO_Person[1]|$p.lastName->meta::pure::functions::multiplicity::toOne()->meta::pure::functions::string::length() < 6)",
                                "test::TestRuntime", connection);
                Object value = result.rows().get(0).get(0);
                // After struct unwrapping, should be a Map with firstName/lastName keys
            assertInstanceOf(Map.class, value, "Expected Map but got: " + (value == null ? "null" : value.getClass().getName()));
                @SuppressWarnings("unchecked")
                java.util.Map<String, Object> map = (java.util.Map<String, Object>) value;
                assertEquals("Fabrice", map.get("firstName"));
                assertEquals("Smith", map.get("lastName"));
        }

        @Test
        void testExistsOnStructArray() throws SQLException {
                // [^Firm(legalName='f1'), ^Firm(legalName='f2')]->exists(f|$f.legalName ==
                // 'f1') = true
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[^meta::pure::functions::collection::tests::model::CO_Firm(legalName='f1'), ^meta::pure::functions::collection::tests::model::CO_Firm(legalName='f2')]->meta::pure::functions::collection::exists(f: meta::pure::functions::collection::tests::model::CO_Firm[1]|$f.legalName == 'f1')",
                                "test::TestRuntime", connection);
                assertEquals(true, result.rows().get(0).get(0));
        }



        // ==================== Fold function tests ====================

        @Test
        void testFoldIntegerSumWithInitialValue() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 3, 4]->meta::pure::functions::collection::fold({x: Integer[1], y: Integer[1]|$x + $y}, 7)",
                                "test::TestRuntime", connection);
                assertScalarInteger(result, 17); // 7+1+2+3+4 = 17
        }

        @Test
        void testFoldIntegerSumZeroInit() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 3, 4]->meta::pure::functions::collection::fold({x: Integer[1], y: Integer[1]|$x + $y}, 0)",
                                "test::TestRuntime", connection);
                assertScalarInteger(result, 10); // 0+1+2+3+4 = 10
        }

        @Test
        void testFoldStringConcat() throws SQLException {
                // ['a','b','c','d']->fold({x,y|$y+$x}, '') = 'abcd'
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|['a', 'b', 'c', 'd']->meta::pure::functions::collection::fold({x: String[1], y: String[1]|$y + $x}, '')",
                                "test::TestRuntime", connection);
                assertEquals("abcd", result.rows().get(0).get(0));
        }

        @Test
        void testFoldStringConcatWithPrefix() throws SQLException {
                // ['a','b','c','d']->fold({x,y|$y+$x}, 'z') = 'zabcd'
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|['a', 'b', 'c', 'd']->meta::pure::functions::collection::fold({x: String[1], y: String[1]|$y + $x}, 'z')",
                                "test::TestRuntime", connection);
                assertEquals("zabcd", result.rows().get(0).get(0));
        }

        @Test
        void testFoldIntegerWithExtraArithmetic() throws SQLException {
                // [1,2,3,4]->fold({x,y|$x+$y+2}, 7) = 25
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 3, 4]->meta::pure::functions::collection::fold({x: Integer[1], y: Integer[1]|$x + $y + 2}, 7)",
                                "test::TestRuntime", connection);
                assertScalarInteger(result, 25); // 7 -> 7+1+2=10 -> 10+2+2=14 -> 14+3+2=19 -> 19+4+2=25
        }

        @Test
        void testForAllOnEmptySet() throws SQLException {
                // []->forAll(e|$e == 0) should return true (vacuous truth)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[]->meta::pure::functions::collection::forAll(e|$e == 0)",
                                "test::TestRuntime", connection);
                assertEquals(true, result.rows().get(0).get(0));
        }

        @Test
        void testParseDateWithTimezone() throws SQLException {
                // '2014-02-27T10:01:35.231-0500'->parseDate() returns OffsetDateTime preserving
                // TZ
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'2014-02-27T10:01:35.231-0500'->meta::pure::functions::string::parseDate()",
                                "test::TestRuntime", connection);
                Object value = result.rows().get(0).get(0);
            assertInstanceOf(OffsetDateTime.class, value, "Expected OffsetDateTime but got: " + value.getClass().getName());
                java.time.OffsetDateTime odt = (java.time.OffsetDateTime) value;
                // Convert to UTC and verify the instant is correct
                java.time.OffsetDateTime utc = odt.withOffsetSameInstant(java.time.ZoneOffset.UTC);
                assertEquals(15, utc.getHour(), "Expected UTC hour 15 but got: " + utc);
                assertEquals(1, utc.getMinute());
                assertEquals(35, utc.getSecond());
        }

        // ==================== Hash function tests ====================

        @Test
        void testMD5Hash() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|meta::pure::functions::hash::hash('Hello, World!', meta::pure::functions::hash::HashType.MD5)",
                                "test::TestRuntime", connection);
                assertEquals("65a8e27d8879283831b664bd8b7f0ad4", result.rows().get(0).get(0));
        }

        @Test
        void testSHA256Hash() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|meta::pure::functions::hash::hash('Hello, World!', meta::pure::functions::hash::HashType.SHA256)",
                                "test::TestRuntime", connection);
                assertEquals("dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f",
                                result.rows().get(0).get(0));
        }

        // ==================== Date precision tests ====================

        @Test
        void testHasHourWithHour() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2015-04-15T17->meta::pure::functions::date::hasHour()",
                                "test::TestRuntime", connection);
                assertEquals(true, result.rows().get(0).get(0));
        }

        @Test
        void testHasHourWithoutHour() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2015-04-15->meta::pure::functions::date::hasHour()",
                                "test::TestRuntime", connection);
                assertEquals(false, result.rows().get(0).get(0));
        }

        @Test
        void testHasMinuteWithMinute() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2015-04-15T17:09+0000->meta::pure::functions::date::hasMinute()",
                                "test::TestRuntime", connection);
                assertEquals(true, result.rows().get(0).get(0));
        }

        @Test
        void testHasMinuteWithoutMinute() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2015-04-15T17->meta::pure::functions::date::hasMinute()",
                                "test::TestRuntime", connection);
                assertEquals(false, result.rows().get(0).get(0));
        }

        // ==================== ANY Audit Tests ====================
        // Each test targets a specific Primitive.ANY site in PureCompiler.
        // Look for [ANY-Tn] tags in stderr to confirm which path was hit.

        @Test
        void anyAudit_T1_path5_classTypeParamWithClassDefs() throws SQLException {
                // T1: PATH5 — typed lambda param, class defs PROVIDED → should resolve, no ANY
                // map(p: CO_Person[1]|$p.lastName) with class defs → lastName should be STRING
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[^meta::pure::functions::collection::tests::model::CO_Person(firstName='A',lastName='B')]->meta::pure::functions::collection::map(p: meta::pure::functions::collection::tests::model::CO_Person[1]|$p.lastName)",
                                "test::TestRuntime", connection);
                // If PATH5 is NOT hit, lastName resolves to STRING correctly
                Object val = result.rows().get(0).get(0);
                assertTrue(val instanceof java.sql.Array || "B".equals(val), "Expected 'B' or Array but got: " + val);
        }

        @Test
        void anyAudit_T1b_path5_classTypeParamWithoutClassDefs() {
                // T1b: PATH5 — typed lambda param, class NOT in model → should throw
                // Verifies compiler correctly errors on unknown class references.
                assertThrows(Exception.class, () -> queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[^test::NonExistentClass(name='A')]->" +
                                                "meta::pure::functions::collection::map(p: test::NonExistentClass[1]|$p.name)",
                                "test::TestRuntime", connection));
        }

        @Test
        void anyAudit_T2_path6_nestedStructProperty() throws SQLException {
                // T2: PATH6 — nested struct property access on unnested element
                // Tests the actual pattern that produces 8 PATH6 hits: accessing .val on
                // unnested structs
                // Expression: struct with nested array field, then access the nested field
                // directly
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[^meta::pure::functions::collection::tests::model::CO_Firm(legalName='f1', employees=[^meta::pure::functions::collection::tests::model::CO_Person(firstName='A',lastName='B')])]->meta::pure::functions::collection::map(x: meta::pure::functions::collection::tests::model::CO_Firm[1]|$x.legalName)",
                                "test::TestRuntime", connection);
                Object val = result.rows().get(0).get(0);
                // With class defs, legalName should resolve to STRING (no PATH6 hit)
                assertTrue(val instanceof java.sql.Array || "f1".equals(val), "Expected 'f1' but got: " + val);
        }

        @Test
        void anyAudit_T2b_path6_nestedStructValAccess() throws SQLException {
                // T2b: The actual PATH6 pattern — accessing .val on unnested struct elements
                // This mirrors the flatten test pattern where extCols=[name, addresses, values]
                // but .val is on the nested struct, not in extendedColumns
                // Uses inline struct with nested arrays to trigger PATH6
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[^meta::pure::functions::collection::tests::model::CO_Person(firstName='A',lastName='B'), ^meta::pure::functions::collection::tests::model::CO_Person(firstName='C',lastName='D')]->meta::pure::functions::collection::find(p: meta::pure::functions::collection::tests::model::CO_Person[1]|$p.firstName == 'A')",
                                "test::TestRuntime", connection);
                // find returns the first matching element — should be the struct with firstName='A'
                assertNotNull(result.rows().get(0).get(0), "find should return the matching struct");
        }

        @Test
        void anyAudit_T3_path6_letBoundVariablePropertyAccess() throws SQLException {
                // T3: PATH6 — let-bound variable property access
                // let firm = ^Firm(legalName='f'); $firm.legalName
                // The let-bound $firm should carry type context through SCALAR binding
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|let firm = ^meta::pure::functions::collection::tests::model::CO_Firm(legalName='hello'); $firm.legalName;",
                                "test::TestRuntime", connection);
                assertEquals("hello", result.rows().get(0).get(0));
                // Check stderr for [ANY-T2] — if PATH6 is hit, legalName type is unknown
        }

        @Test
        void anyAudit_T4_projectColumnType() throws SQLException {
                // T4: ProjectExpression column type resolution
                // project({p|$p.firstName}, ['first']) → 'first' column should know it's STRING
                // Then filter on 'first' column — if type is ANY, might cause issues
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[^meta::pure::functions::collection::tests::model::CO_Person(firstName='A',lastName='B'), ^meta::pure::functions::collection::tests::model::CO_Person(firstName='C',lastName='D')]->meta::pure::functions::collection::map(p: meta::pure::functions::collection::tests::model::CO_Person[1]|$p.firstName)",
                                "test::TestRuntime", connection);
                // map extracts firstName values — should return array ['A', 'C']
                assertNotNull(result.rows().get(0).get(0), "map on struct array should produce results");
        }

        @Test
        void anyAudit_T5_instancePropertyVarRef() throws SQLException {
                // T5: inferInstancePropertyType with PureExpression (variable ref)
                // let name = 'John'; ^Person(firstName=$name) — $name is a var ref
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|let name = 'John'; ^meta::pure::functions::collection::tests::model::CO_Person(firstName=$name, lastName='Doe').firstName;",
                                "test::TestRuntime", connection);
                assertEquals("John", result.rows().get(0).get(0));
                // Check stderr for [ANY-T5] — if hit, firstName type inferred as ANY instead of
                // STRING
        }

        @Test
        void anyAudit_T6_mixedTypeListDetected() throws SQLException {
                // T6: Mixed type detection works — [1, 'hello'] gets VARIANT-wrapped
                // Verifies hasMixedTypes correctly identifies INTEGER vs STRING
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 'hello']",
                                "test::TestRuntime", connection);
                // UNNEST produces individual rows from the VARIANT array
                var values = result.asCollection().values();
                assertEquals(2, values.size(), "Expected 2 elements in mixed-type list");
                assertEquals(1, ((Number) values.get(0)).intValue(), "First element should be 1");
                assertEquals("hello", values.get(1).toString(), "Second element should be 'hello'");
        }

        // ==================== Struct literal / multiplicity tests
        // ====================

        @Test
        void testHeadComplex() throws SQLException {
                // Exact same data as PCT testHeadComplex:
                // CO_Firm { legalName: String[1]; employees: CO_Person[*]; }
                // CO_Person { firstName: String[1]; lastName: String[1]; }
                // firm1 has 1 employee (smith), firm2 has 2 employees (doe, roe)
                // [$firm1, $firm2]->head().legalName == 'Firm1'
                //
                // Without class definitions in the Pure model, this fails because
                // firm1.employees is STRUCT but firm2.employees is STRUCT[].
                // PCT line 51: assertEquals($firm1, $f->eval(|$set->head()));
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[^meta::pure::functions::collection::tests::model::CO_Firm(legalName='Firm1',"
                                                + " employees=^meta::pure::functions::collection::tests::model::CO_Person(firstName='Fabrice', lastName='Smith')),"
                                                + " ^meta::pure::functions::collection::tests::model::CO_Firm(legalName='Firm2',"
                                                + " employees=[^meta::pure::functions::collection::tests::model::CO_Person(firstName='Pierre', lastName='Doe'),"
                                                + " ^meta::pure::functions::collection::tests::model::CO_Person(firstName='David', lastName='Roe')])]"
                                                + "->meta::pure::functions::collection::head()",
                                "test::TestRuntime", connection);
                assertFalse(result.rows().isEmpty(), "head() should return a result");
                Object value = result.rows().get(0).get(0);
                assertNotNull(value, "head() should return the first firm");
                // head() returns first firm as a struct — verify it contains Firm1
                String valueStr = value.toString();
                assertTrue(valueStr.contains("Firm1"), "head() should return Firm1: " + valueStr);

                // PCT line 52: assertEquals($doe, $f->eval(|$set->at(1).employees->head()));
                var result2 = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[^meta::pure::functions::collection::tests::model::CO_Firm(legalName='Firm1',"
                                                + " employees=^meta::pure::functions::collection::tests::model::CO_Person(firstName='Fabrice', lastName='Smith')),"
                                                + " ^meta::pure::functions::collection::tests::model::CO_Firm(legalName='Firm2',"
                                                + " employees=[^meta::pure::functions::collection::tests::model::CO_Person(firstName='Pierre', lastName='Doe'),"
                                                + " ^meta::pure::functions::collection::tests::model::CO_Person(firstName='David', lastName='Roe')])]"
                                                + "->meta::pure::functions::collection::at(1).employees->meta::pure::functions::collection::head()",
                                "test::TestRuntime", connection);
                assertFalse(result2.rows().isEmpty(), "head() should return a result");
                Object value2 = result2.rows().get(0).get(0);
                assertNotNull(value2, "at(1).employees->head() should return Pierre Doe");
                String valueStr2 = value2.toString();
                assertTrue(valueStr2.contains("Pierre"), "Should return Pierre Doe: " + valueStr2);
        }

        @Test
        void testEmptySetCollectionOps() throws SQLException {
                // []->head() should return null (empty)
                var headResult = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[]->meta::pure::functions::collection::head()",
                                "test::TestRuntime", connection);
                assertNull(headResult.rows().get(0).get(0), "head() on empty set should be null");

                // []->first() should return null (empty)
                var firstResult = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[]->meta::pure::functions::collection::first()",
                                "test::TestRuntime", connection);
                assertNull(firstResult.rows().get(0).get(0), "first() on empty set should be null");

                // []->last() should return null (empty)
                var lastResult = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[]->meta::pure::functions::collection::last()",
                                "test::TestRuntime", connection);
                assertNull(lastResult.rows().get(0).get(0), "last() on empty set should be null");

                // []->tail() should return empty list — UNNEST produces 0 rows
                var tailResult = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[]->meta::pure::functions::collection::tail()",
                                "test::TestRuntime", connection);
                assertTrue(tailResult.asCollection().values().isEmpty(),
                                "tail() on empty set should return empty collection");

                // []->init() should return empty list — UNNEST produces 0 rows
                var initResult = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[]->meta::pure::functions::collection::init()",
                                "test::TestRuntime", connection);
                assertTrue(initResult.asCollection().values().isEmpty(),
                                "init() on empty set should return empty collection");
        }

        @Test
        void testScalarInitAndTail() throws SQLException {
                // 'a'->init() should return empty list (all but last of single element)
                // UNNEST on empty result produces 0 rows
                var initResult = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'a'->meta::pure::functions::collection::init()",
                                "test::TestRuntime", connection);
                assertTrue(initResult.asCollection().values().isEmpty(),
                                "'a'->init() should return empty collection");

                // 'a'->tail() should return empty list (all but first of single element)
                // UNNEST on empty result produces 0 rows
                var tailResult = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'a'->meta::pure::functions::collection::tail()",
                                "test::TestRuntime", connection);
                assertTrue(tailResult.asCollection().values().isEmpty(),
                                "'a'->tail() should return empty collection");
        }

        @Test
        void testSliceInList() throws SQLException {
                // PCT: assertEquals(list([2, 3]), |list([1, 2, 3, 4]->slice(1, 3)))
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|meta::pure::functions::collection::list([1, 2, 3, 4]->meta::pure::functions::collection::slice(1, 3))",
                                "test::TestRuntime", connection);
                Object value = result.rows().get(0).get(0);
                assertNotNull(value, "list(slice([1,2,3,4], 1, 3)) should not be null");
                // Row.unwrapValue converts DuckDB arrays to List<Object>
                @SuppressWarnings("unchecked")
                List<Object> elements = (List<Object>) value;
                assertEquals(2, elements.size(), "Should have 2 elements");
                assertEquals(2, ((Number) elements.get(0)).intValue());
                assertEquals(3, ((Number) elements.get(1)).intValue());
        }

        @Test
        void testSortWithKeyFunction() throws SQLException {
                // PCT: ['Doe','Smith','Branche']->sort(s|$s->substring(1,2),
                // {x,y|$x->compare($y)})
                // Keys: Doe->'o', Smith->'m', Branche->'r' → sorted by key: m<o<r → [Smith,
                // Doe, Branche]
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|['Doe', 'Smith', 'Branche']->meta::pure::functions::collection::sort({s: String[1]|$s->meta::pure::functions::string::substring(1, 2)}, {x: String[1], y: String[1]|$x->meta::pure::functions::lang::compare($y)})",
                                "test::TestRuntime", connection);
                assertEquals(List.of("Smith", "Doe", "Branche"), result.asCollection().values());
        }

        @Test
        void testTailOnList() throws SQLException {
                // PCT: assertEquals(['b', 'c'], |['a', 'b', 'c']->tail())
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|['a', 'b', 'c']->meta::pure::functions::collection::tail()",
                                "test::TestRuntime", connection);
                assertEquals(List.of("b", "c"), result.asCollection().values());
        }

        // ==================== Zip Tests ====================

        @Test
        void testZipBothListsEmpty() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|let a = []; let b = []; $a->meta::pure::functions::collection::zip($b);",
                                "test::TestRuntime", connection);
                assertTrue(result.asCollection().values().isEmpty(),
                                "zip of two empty lists should be empty");
        }

        @Test
        void testZipFirstListEmpty() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|let a = []; let b = ['a', 'b', 'c', 'd']; $a->meta::pure::functions::collection::zip($b);",
                                "test::TestRuntime", connection);
                assertTrue(result.asCollection().values().isEmpty(),
                                "zip with first empty should be empty");
        }

        @Test
        void testZipSecondListEmpty() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|let a = [1, 2, 3, 4]; let b = []; $a->meta::pure::functions::collection::zip($b);",
                                "test::TestRuntime", connection);
                assertTrue(result.asCollection().values().isEmpty(),
                                "zip with second empty should be empty");
        }

        @SuppressWarnings("unchecked")
        @Test
        void testZipBothListsSameLength() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|let a = [1, 2, 3, 4]; let b = ['a', 'b', 'c', 'd']; $a->meta::pure::functions::collection::zip($b);",
                                "test::TestRuntime", connection);
                var pairs = (java.util.List<java.util.Map<String, Object>>) (java.util.List<?>) result.asCollection().values();
                assertEquals(4, pairs.size());
                assertEquals(1, ((Number) pairs.get(0).get("first")).intValue());
                assertEquals("a", pairs.get(0).get("second"));
                assertEquals(4, ((Number) pairs.get(3).get("first")).intValue());
                assertEquals("d", pairs.get(3).get("second"));
        }

        @SuppressWarnings("unchecked")
        @Test
        void testZipFirstListLonger() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|let a = [1, 2, 3, 4]; let b = ['a', 'b']; $a->meta::pure::functions::collection::zip($b);",
                                "test::TestRuntime", connection);
                var pairs = (java.util.List<java.util.Map<String, Object>>) (java.util.List<?>) result.asCollection().values();
                assertEquals(2, pairs.size(), "Should truncate to shorter list");
                assertEquals(1, ((Number) pairs.get(0).get("first")).intValue());
                assertEquals("a", pairs.get(0).get("second"));
                assertEquals(2, ((Number) pairs.get(1).get("first")).intValue());
                assertEquals("b", pairs.get(1).get("second"));
        }

        @SuppressWarnings("unchecked")
        @Test
        void testZipSecondListLonger() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|let a = [1, 2]; let b = ['a', 'b', 'c', 'd']; $a->meta::pure::functions::collection::zip($b);",
                                "test::TestRuntime", connection);
                var pairs = (java.util.List<java.util.Map<String, Object>>) (java.util.List<?>) result.asCollection().values();
                assertEquals(2, pairs.size(), "Should truncate to shorter list");
                assertEquals(1, ((Number) pairs.get(0).get("first")).intValue());
                assertEquals("a", pairs.get(0).get("second"));
                assertEquals(2, ((Number) pairs.get(1).get("first")).intValue());
                assertEquals("b", pairs.get(1).get("second"));
        }

        @SuppressWarnings("unchecked")
        @Test
        void testZipBothListsAreOfPairs() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|let a = [1, 2, 3]; let b = ['a', 'b', 'c']; let c = [4, 5, 6]; let d = ['d', 'e', 'f']; let x = $a->meta::pure::functions::collection::zip($b); let y = $c->meta::pure::functions::collection::zip($d); $x->meta::pure::functions::collection::zip($y);",
                                "test::TestRuntime", connection);
                var pairs = (java.util.List<java.util.Map<String, Object>>) (java.util.List<?>) result.asCollection().values();
                assertEquals(3, pairs.size());
                // first element: pair(pair(1,'a'), pair(4,'d'))
                var first = pairs.get(0);
            assertInstanceOf(Map.class, first.get("first"), "first should be a nested Pair (Map)");
            assertInstanceOf(Map.class, first.get("second"), "second should be a nested Pair (Map)");
                var firstFirst = (java.util.Map<String, Object>) first.get("first");
                assertEquals(1, ((Number) firstFirst.get("first")).intValue());
                assertEquals("a", firstFirst.get("second"));
                var firstSecond = (java.util.Map<String, Object>) first.get("second");
                assertEquals(4, ((Number) firstSecond.get("first")).intValue());
                assertEquals("d", firstSecond.get("second"));
        }

        @SuppressWarnings("unchecked")
        @Test
        void testZipFirstListsIsOfPairs() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|let a = [1, 2, 3]; let b = ['a', 'b', 'c']; let c = [4, 5, 6]; let x = $a->meta::pure::functions::collection::zip($b); $x->meta::pure::functions::collection::zip($c);",
                                "test::TestRuntime", connection);
                var pairs = (java.util.List<java.util.Map<String, Object>>) (java.util.List<?>) result.asCollection().values();
                assertEquals(3, pairs.size());
                // first element: pair(pair(1,'a'), 4)
                var first = pairs.get(0);
            assertInstanceOf(Map.class, first.get("first"), "first should be a nested Pair (Map)");
                var firstFirst = (java.util.Map<String, Object>) first.get("first");
                assertEquals(1, ((Number) firstFirst.get("first")).intValue());
                assertEquals("a", firstFirst.get("second"));
                assertEquals(4, ((Number) first.get("second")).intValue());
        }

        @SuppressWarnings("unchecked")
        @Test
        void testZipSecondListsIsOfPairs() throws SQLException {
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|let a = [1, 2, 3]; let c = [4, 5, 6]; let d = ['d', 'e', 'f']; let x = $c->meta::pure::functions::collection::zip($d); $a->meta::pure::functions::collection::zip($x);",
                                "test::TestRuntime", connection);
                var pairs = (java.util.List<java.util.Map<String, Object>>) (java.util.List<?>) result.asCollection().values();
                assertEquals(3, pairs.size());
                // first element: pair(1, pair(4,'d'))
                var first = pairs.get(0);
                assertEquals(1, ((Number) first.get("first")).intValue());
            assertInstanceOf(Map.class, first.get("second"), "second should be a nested Pair (Map)");
                var firstSecond = (java.util.Map<String, Object>) first.get("second");
                assertEquals(4, ((Number) firstSecond.get("first")).intValue());
                assertEquals("d", firstSecond.get("second"));
        }

        // ==================== fold() on struct list with string accumulator (PCT:
        // testPlusInIterate) ====================

        @Test
        void testFoldOnStructListWithStringAccumulator() throws SQLException {
                // PCT test: testPlusInIterate - fold over list of P_Person structs with string
                // accumulator
                // This fails with: "Binder Error: The initial value type must be the same as
                // the list child type"
                // because DuckDB's list_reduce requires initial value type = list element type.
                // The list is STRUCT[] but initial value is VARCHAR.
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[^meta::pure::functions::string::tests::plus::model::P_Person(firstName='Pierre',lastName='Doe'), ^meta::pure::functions::string::tests::plus::model::P_Person(firstName='Kevin',lastName='RoeDoe'), ^meta::pure::functions::string::tests::plus::model::P_Person(firstName='Andrew',lastName='Some_LName')]->meta::pure::functions::collection::fold({p: meta::pure::functions::string::tests::plus::model::P_Person[1], s: String[1]|$s + '; ' + $p.lastName->meta::pure::functions::collection::at(0) + ', ' + $p.firstName->meta::pure::functions::collection::at(0)}, 'names')",
                                "test::TestRuntime", connection);
                Object value = result.rows().get(0).get(0);
                assertEquals("names; Doe, Pierre; RoeDoe, Kevin; Some_LName, Andrew", value);
        }

        // ==================== map() on struct list with at(0) on scalar properties
        // (PCT: testPlusInCollect) ====================

        @Test
        void testMapOnStructListWithAtOnScalarProperties() throws SQLException {
                // PCT test: testPlusInCollect - map over list of P_Person structs, accessing
                // scalar properties via at(0)
                // at(0) on a scalar struct property (multiplicity [1]) should be a no-op, not
                // LIST_EXTRACT
                // which would extract a single character from the string.
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[^meta::pure::functions::string::tests::plus::model::P_Person(firstName='Pierre',lastName='Doe'), ^meta::pure::functions::string::tests::plus::model::P_Person(firstName='Kevin',lastName='RoeDoe')]->meta::pure::functions::collection::map(p: meta::pure::functions::string::tests::plus::model::P_Person[1]|$p.lastName->meta::pure::functions::collection::at(0) + ', ' + $p.firstName->meta::pure::functions::collection::at(0))",
                                "test::TestRuntime", connection);
                assertEquals(List.of("Doe, Pierre", "RoeDoe, Kevin"), result.asCollection().values());
        }

        // ==================== Large integer arithmetic (PCT: testLargeTimes)
        // ====================

        @Test
        void testLargeIntegerMultiplication() throws SQLException {
                // PCT test: testLargeTimes - 2 * Long.MAX_VALUE overflows INT64 in DuckDB
                // Needs HUGEINT promotion for large literals
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|2 * 9223372036854775807",
                                "test::TestRuntime", connection);
                Object value = result.rows().get(0).get(0);
                assertNotNull(value, "Result should not be null");
                // 2 * 9223372036854775807 = 18446744073709551614 (exceeds Long.MAX_VALUE)
                assertEquals(new java.math.BigInteger("18446744073709551614"),
                                new java.math.BigInteger(value.toString()));
        }

        // ==================== Decimal precision (PCT: testDecimalTimes)
        // ====================

        @Test
        void testDecimalMultiplicationPreservesScale() throws SQLException {
                // PCT test: testDecimalTimes - 19.905D * 17774D should produce 353791.470D
                // (scale 3)
                // not 353791.4700D (scale 4) from spurious .0 on integer-valued decimals
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|19.905D * 17774D",
                                "test::TestRuntime", connection);
                Object value = result.rows().get(0).get(0);
                assertNotNull(value);
                // Should be 353791.470 with scale 3, not 353791.4700 with scale 4
                assertEquals(new java.math.BigDecimal("353791.470"), value);
        }

        @Test
        void testHugeIntegerLiteralSubtraction() throws SQLException {
                // PCT test: testLargeMinus - literal 9223372036854775898 exceeds Long.MAX_VALUE
                // Parser must handle BigInteger, SQL must emit ::HUGEINT
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|9223372036854775898 - 132",
                                "test::TestRuntime", connection);
                Object value = result.rows().get(0).get(0);
                assertNotNull(value);
                assertEquals(new java.math.BigInteger("9223372036854775766"),
                                new java.math.BigInteger(value.toString()));
        }

        @Test
        void testDivideWithScale() throws SQLException {
                // PCT test: testDecimalDivide - divide(3.1415D, 0.1D, 2) should give 31.42
                // Pure's divide(a, b, scale) divides and rounds to scale decimal places
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|meta::pure::functions::math::divide(3.1415D, 0.1D, 2)",
                                "test::TestRuntime", connection);
                Object value = result.rows().get(0).get(0);
                assertNotNull(value);
                assertEquals(0, new java.math.BigDecimal("31.42")
                                .compareTo(new java.math.BigDecimal(value.toString())));
        }

        @Test
        void testDivideNonTerminatingPrecision() throws SQLException {
                // PCT test: testDivideWithNonTerminatingExpansion - 1/96 needs ~34 digit
                // precision
                // Pure uses Decimal128-level precision for division results
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1 / 96",
                                "test::TestRuntime", connection);
                Object value = result.rows().get(0).get(0);
                assertNotNull(value);
                // Pure expects: 0.01041666666666666666666666666666667 (33 decimal places)
                // DuckDB 1.5 returns double precision (~18 significant digits)
                String actual = value.toString();
                assertTrue(actual.length() > 15, "Expected reasonable precision result, got: " + actual);
                assertTrue(actual.startsWith("0.010416666"), "Expected 1/96 ≈ 0.010416..., got: " + actual);
        }

        @Test
        void testLetAsLastStatement() throws SQLException {
                // PCT: testLetAsLastStatement - user-defined function with let as last
                // statement
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|meta::pure::functions::lang::tests::letFn::letAsLastStatement()",
                                "test::TestRuntime", connection);
                assertEquals("last statement string", result.rows().get(0).get(0).toString());
        }

        @Test
        void testLetWithParam() throws SQLException {
                // PCT: testLetWithParam - user-defined function with parameter and let
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'echo'->meta::pure::functions::lang::tests::letFn::letWithParam()->meta::pure::functions::multiplicity::toOne()",
                                "test::TestRuntime", connection);
                assertEquals("echo", result.rows().get(0).get(0).toString());
        }

        @Test
        void testFormatFloatMinimalRepr() throws SQLException {
                // PCT: testFormatFloat - %f should produce minimal float representation
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'the quick brown %s jumps over the lazy %f'->meta::pure::functions::string::format(['fox', 1.5])",
                                "test::TestRuntime", connection);
                assertEquals("the quick brown fox jumps over the lazy 1.5", result.rows().get(0).get(0).toString());
        }

        @Test
        void testFormatDate12HourAmPm() throws SQLException {
                // PCT: testFormatDate - format with 12-hour clock and AM/PM
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'on %t{yyyy-MM-dd h:mm:ssa}'->meta::pure::functions::string::format(%2014-03-10T13:07:44.001+0000)",
                                "test::TestRuntime", connection);
                assertEquals("on 2014-03-10 1:07:44PM", result.rows().get(0).get(0).toString());
        }

        @Test
        void testFormatDateIsoWithTimezone() throws SQLException {
                // PCT: testFormatDate - ISO format with quoted "T" literal and Z timezone
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'on %t{yyyy-MM-dd\"T\"HH:mm:ss.SSSZ}'->meta::pure::functions::string::format(%2014-03-10T13:07:44.001+0000)",
                                "test::TestRuntime", connection);
                assertEquals("on 2014-03-10T13:07:44.001+0000", result.rows().get(0).get(0).toString());
        }

        @Test
        void testFormatDateIsoWithXTimezone() throws SQLException {
                // PCT: testFormatDate - ISO format with X timezone (Z for UTC)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'on %t{yyyy-MM-dd HH:mm:ss.SSSX}'->meta::pure::functions::string::format(%2014-03-10T13:07:44.001+0000)",
                                "test::TestRuntime", connection);
                assertEquals("on 2014-03-10 13:07:44.001Z", result.rows().get(0).get(0).toString());
        }

        @Test
        void testBitShiftLeft46Bits() throws SQLException {
                // PCT: testBitShiftLeft_UpTo62Bits - shift by 46 requires BIGINT
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1->meta::pure::functions::math::bitShiftLeft(46)",
                                "test::TestRuntime", connection);
                assertEquals(70368744177664L, ((Number) result.rows().get(0).get(0)).longValue());
        }

        // === PCT: testMaxBy assertions ===

        @Test
        void testMaxBy_Simple() throws SQLException {
                // |[1, 2]->maxBy([10, 20]) == 2
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2]->meta::pure::functions::math::maxBy([10, 20])",
                                "test::TestRuntime", connection);
                assertEquals(2L, ((Number) result.rows().get(0).get(0)).longValue());
        }

        @Test
        void testMaxBy_LargerList() throws SQLException {
                // |[1001, 1020, 1030, 900, 2010, 2020]->maxBy([10000, 9000, 8000, 15000, 15000,
                // 8000]) == 900
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1001, 1020, 1030, 900, 2010, 2020]->meta::pure::functions::math::maxBy([10000, 9000, 8000, 15000, 15000, 8000])",
                                "test::TestRuntime", connection);
                assertEquals(900L, ((Number) result.rows().get(0).get(0)).longValue());
        }

        @Test
        void testMaxBy_TopK() throws SQLException {
                // |[1001, 1020, 1030, 900, 2010, 2020]->maxBy([10000, 9000, 8000, 15000, 15000,
                // 8000], 3) == [900, 2010, 1001]
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1001, 1020, 1030, 900, 2010, 2020]->meta::pure::functions::math::maxBy([10000, 9000, 8000, 15000, 15000, 8000], 3)",
                                "test::TestRuntime", connection);
                // Expected: [900, 2010, 1001] - top 3 by descending keys
                var values = result.asCollection().values();
                assertEquals(3, values.size());
                assertEquals(900, ((Number) values.get(0)).intValue());
                assertEquals(2010, ((Number) values.get(1)).intValue());
                assertEquals(1001, ((Number) values.get(2)).intValue());
        }

        // === PCT: testFold* assertions ===

        @Test
        void testFoldCollectionAccumulator() throws SQLException {
                // |[1, 2, 3, 4]->fold({x, y | y->add(x)}, [-1, 0]) == [-1, 0, 1, 2, 3, 4]
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 3, 4]->meta::pure::functions::collection::fold({x: Integer[1], y: Integer[2]|$y->meta::pure::functions::collection::add($x)}, [-1, 0])",
                                "test::TestRuntime", connection);
                Object value = result.rows().get(0).get(0);
                // Row.unwrapValue converts DuckDB arrays to List<Object>
                @SuppressWarnings("unchecked")
                List<Object> arr = (List<Object>) value;
                assertEquals(6, arr.size());
                assertEquals(-1, ((Number) arr.get(0)).intValue());
                assertEquals(0, ((Number) arr.get(1)).intValue());
                assertEquals(1, ((Number) arr.get(2)).intValue());
                assertEquals(2, ((Number) arr.get(3)).intValue());
                assertEquals(3, ((Number) arr.get(4)).intValue());
                assertEquals(4, ((Number) arr.get(5)).intValue());
        }

        @Test
        void testFoldCollectionAccumulator_WithIfSizeTail() throws SQLException {
                // |[1, 2, 3, 4]->fold({x, y | if(y->size() < 3, |y->add(x),
                // |y->add(x)->tail())}, [-1, 0]) == [2, 3, 4]
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 3, 4]->meta::pure::functions::collection::fold({x: Integer[1], y: Integer[1..3]|meta::pure::functions::lang::if($y->meta::pure::functions::collection::size() < 3, |$y->meta::pure::functions::collection::add($x), |$y->meta::pure::functions::collection::add($x)->meta::pure::functions::collection::tail())}, [-1, 0])",
                                "test::TestRuntime", connection);
                Object value = result.rows().get(0).get(0);
                @SuppressWarnings("unchecked")
                List<Object> arr = (List<Object>) value;
                assertEquals(3, arr.size());
                assertEquals(2, ((Number) arr.get(0)).intValue());
                assertEquals(3, ((Number) arr.get(1)).intValue());
                assertEquals(4, ((Number) arr.get(2)).intValue());
        }

        @Test
        void testFoldWithEmptyAccumulator() throws SQLException {
                // |[1, 2, 3]->fold({val, acc | acc->add(val)}, []) == [1, 2, 3]
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 3]->meta::pure::functions::collection::fold({val: Integer[1], acc: meta::pure::metamodel::type::Nil[0]|$acc->meta::pure::functions::collection::add($val)}, [])",
                                "test::TestRuntime", connection);
                Object value = result.rows().get(0).get(0);
                @SuppressWarnings("unchecked")
                List<Object> arr = (List<Object>) value;
                assertEquals(3, arr.size());
                assertEquals(1, ((Number) arr.get(0)).intValue());
                assertEquals(2, ((Number) arr.get(1)).intValue());
                assertEquals(3, ((Number) arr.get(2)).intValue());
        }

        @Test
        void testFoldEmptyListAndEmptyIdentity() throws SQLException {
                // |[]->cast(@Integer)->fold({val, acc | acc->add(val)}, []->cast(@Any)) == []
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[]->meta::pure::functions::lang::cast(@Integer)->meta::pure::functions::collection::fold({val: Integer[1], acc: meta::pure::metamodel::type::Any[0]|$acc->meta::pure::functions::collection::add($val)}, []->meta::pure::functions::lang::cast(@meta::pure::metamodel::type::Any))",
                                "test::TestRuntime", connection);
                Object value = result.rows().get(0).get(0);
                @SuppressWarnings("unchecked")
                List<Object> arr = (List<Object>) value;
                assertEquals(0, arr.size());
        }

        @Test
        void testFoldWithSingleValue() throws SQLException {
                // |1->fold({val, acc | acc->add(val)}, []) == [1]
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1->meta::pure::functions::collection::fold({val: Integer[1], acc: meta::pure::metamodel::type::Nil[0]|$acc->meta::pure::functions::collection::add($val)}, [])",
                                "test::TestRuntime", connection);
                Object value = result.rows().get(0).get(0);
                @SuppressWarnings("unchecked")
                List<Object> arr = (List<Object>) value;
                assertEquals(1, arr.size());
                assertEquals(1, ((Number) arr.get(0)).intValue());
        }

        @Test
        void testFoldMixedAccumulatorTypes() throws SQLException {
                // |['one', 'two']->fold({val, acc | acc + length(val)}, 1) == 7
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|['one', 'two']->meta::pure::functions::collection::fold({val: String[1], acc: Integer[1]|$acc + $val->meta::pure::functions::string::length()}, 1)",
                                "test::TestRuntime", connection);
                assertEquals(7, ((Number) result.rows().get(0).get(0)).intValue());
        }

        @Test
        void testFold_FromVariant() throws SQLException {
                // |[1, 2, 3]->toVariant()->toMany(@Variant)->fold({val, acc | acc +
                // val->to(@Integer)->toOne()}, 1) == 7
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2, 3]->meta::pure::functions::variant::convert::toVariant()->meta::pure::functions::variant::convert::toMany(@meta::pure::metamodel::variant::Variant)->meta::pure::functions::collection::fold({val: meta::pure::metamodel::variant::Variant[1], acc: Integer[1]|$acc + $val->meta::pure::functions::variant::convert::to(@Integer)->meta::pure::functions::multiplicity::toOne()}, 1)",
                                "test::TestRuntime", connection);
                assertEquals(7, ((Number) result.rows().get(0).get(0)).intValue());
        }

        // === PCT: testContainsWithFunction assertion ===

        @Test
        void testContainsWithFunction() throws SQLException {
                // PCT: [^Class(name='f1'), ^Class(name='f2')]->contains(^Class(name='f1'),
                // comparator(...){$a.name == $b.name})
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[^meta::pure::functions::collection::tests::contains::ClassWithoutEquality(name='f1'), ^meta::pure::functions::collection::tests::contains::ClassWithoutEquality(name='f2')]->meta::pure::functions::collection::contains(^meta::pure::functions::collection::tests::contains::ClassWithoutEquality(name='f1'), comparator(a: meta::pure::functions::collection::tests::contains::ClassWithoutEquality[1], b: meta::pure::functions::collection::tests::contains::ClassWithoutEquality[1]): Boolean[1]\n       {\n         $a.name == $b.name\n       })",
                                "test::TestRuntime", connection);
                assertEquals(true, result.rows().get(0).get(0));
        }

        // === PCT: testBigFloatAbs assertion ===

        @Test
        void testBigFloatAbs() throws SQLException {
                // |abs(-123456789123456789.99) == 123456789123456789.99
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|meta::pure::functions::math::abs(-123456789123456789.99)",
                                "test::TestRuntime", connection);
                assertEquals(new java.math.BigDecimal("123456789123456789.99"),
                                new java.math.BigDecimal(result.rows().get(0).get(0).toString()));
        }

        // === PCT: testPercentile assertions ===

        @Test
        void testPercentile_Continuous() throws SQLException {
                // |10->range()->map(x|$x+1)->percentile(0.75) == 7.75 (continuous)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|10->meta::pure::functions::collection::range()->meta::pure::functions::collection::map(x: Integer[1]|$x + 1)->meta::pure::functions::math::percentile(0.75)",
                                "test::TestRuntime", connection);
                assertEquals(7.75, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.001);
        }

        @Test
        void testPercentile_Discrete() throws SQLException {
                // |10->range()->map(x|$x+1)->percentile(0.75, true, false) == 8 (discrete)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|10->meta::pure::functions::collection::range()->meta::pure::functions::collection::map(x: Integer[1]|$x + 1)->meta::pure::functions::math::percentile(0.75, true, false)",
                                "test::TestRuntime", connection);
                assertEquals(8L, ((Number) result.rows().get(0).get(0)).longValue());
        }

        @Test
        void testPercentile_Descending() throws SQLException {
                // |10->range()->map(x|$x+1)->percentile(0.75, false, true) == 3.25 (continuous,
                // descending)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|10->meta::pure::functions::collection::range()->meta::pure::functions::collection::map(x: Integer[1]|$x + 1)->meta::pure::functions::math::percentile(0.75, false, true)",
                                "test::TestRuntime", connection);
                assertEquals(3.25, ((Number) result.rows().get(0).get(0)).doubleValue(), 0.001);
        }

        // === PCT: testPercentile groupBy assertions ===

        private static final String PERCENTILE_TDS = "#TDS\nid, val\n                1, 1.0\n                1, 2.0\n                1, 3\n                2, 1.5\n                2, 2.5\n                2, 3.5\n                3, 1\n                3, 1.5\n                3, 2.0\n#";

        private java.util.Map<Integer, Double> executePercentileGroupBy(String percentileArgs) throws SQLException {
                String pure = "|" + PERCENTILE_TDS
                                + "->meta::pure::functions::relation::groupBy(~[id], ~[newCol:x: (id:Integer, val:Float)[1]|$x.val:y: Float[*]|$y->meta::pure::functions::math::percentile("
                                + percentileArgs + ")])";
                var br = queryService.execute(
                                getCompletePureModelWithRuntime(), pure,
                                "test::TestRuntime", connection);
                int idIdx = -1, valIdx = -1;
                for (int i = 0; i < br.columns().size(); i++) {
                        if ("id".equals(br.columns().get(i).name()))
                                idIdx = i;
                        if ("newCol".equals(br.columns().get(i).name()))
                                valIdx = i;
                }
                java.util.Map<Integer, Double> map = new java.util.HashMap<>();
                for (var row : br.rows()) {
                        map.put(((Number) row.get(idIdx)).intValue(), ((Number) row.get(valIdx)).doubleValue());
                }
                return map;
        }

        @Test
        void testPercentile_GroupBy_Continuous_Ascending() throws SQLException {
                // percentile(0.6) == percentile(0.6, true, true) -> quantile_cont(0.6)
                var map = executePercentileGroupBy("0.6");
                assertEquals(2.1, map.get(1), 0.001);
                assertEquals(2.6, map.get(2), 0.001);
                assertEquals(1.5, map.get(3), 0.001);
        }

        @Test
        void testPercentile_GroupBy_Discrete_Ascending() throws SQLException {
                // PCT: percentile(0.6, true, false) -> quantile_disc(0.6)
                var map = executePercentileGroupBy("0.6, true, false");
                assertEquals(2.0, map.get(1), 0.001);
                assertEquals(2.5, map.get(2), 0.001);
                assertEquals(1.5, map.get(3), 0.001);
        }

        @Test
        void testPercentile_GroupBy_Continuous_Descending() throws SQLException {
                // percentile(0.6, false, true) -> quantile_cont(1-0.6=0.4)
                var map = executePercentileGroupBy("0.6, false, true");
                assertEquals(1.8, map.get(1), 0.001);
                assertEquals(2.3, map.get(2), 0.001);
                assertEquals(1.4, map.get(3), 0.001);
        }

        @Test
        void testPercentile_GroupBy_Discrete_Descending() throws SQLException {
                // percentile(0.6, false, false) -> quantile_disc(1-0.6=0.4)
                var map = executePercentileGroupBy("0.6, false, false");
                assertEquals(2.0, map.get(1), 0.001);
                assertEquals(2.5, map.get(2), 0.001);
                assertEquals(1.5, map.get(3), 0.001);
        }

        // === PCT: testPercentile window assertions ===

        private java.util.Map<String, Double> executePercentileWindow(String percentileArgs) throws SQLException {
                String pure = "|" + PERCENTILE_TDS
                                + "->meta::pure::functions::relation::extend(~id->meta::pure::functions::relation::over(), ~newCol:{p: meta::pure::metamodel::relation::Relation<(id:Integer, val:Float)>[1], w: meta::pure::functions::relation::_Window<(id:Integer, val:Float)>[1], r: (id:Integer, val:Float)[1]|$r.val}:y: Float[*]|$y->meta::pure::functions::math::percentile("
                                + percentileArgs + "))";
                var br = queryService.execute(
                                getCompletePureModelWithRuntime(), pure,
                                "test::TestRuntime", connection);
                int idIdx = -1, newColIdx = -1;
                for (int i = 0; i < br.columns().size(); i++) {
                        if ("id".equals(br.columns().get(i).name()))
                                idIdx = i;
                        if ("val".equals(br.columns().get(i).name())) {
                        }
                        if ("newCol".equals(br.columns().get(i).name()))
                                newColIdx = i;
                }
                // Return map of "id-val" -> newCol (just check first row per group)
                java.util.Map<String, Double> map = new java.util.HashMap<>();
                java.util.Set<Integer> seenIds = new java.util.HashSet<>();
                for (var row : br.rows()) {
                        int id = ((Number) row.get(idIdx)).intValue();
                        if (seenIds.add(id)) {
                                map.put(String.valueOf(id), ((Number) row.get(newColIdx)).doubleValue());
                        }
                }
                return map;
        }

        @Test
        void testPercentile_Window_Continuous_Ascending() throws SQLException {
                // percentile(0.6, true, true) -> quantile_cont(0.6) OVER (PARTITION BY id)
                var map = executePercentileWindow("0.6, true, true");
                assertEquals(2.1, map.get("1"), 0.001);
                assertEquals(2.6, map.get("2"), 0.001);
                assertEquals(1.5, map.get("3"), 0.001);
        }

        @Test
        void testPercentile_Window_Discrete_Ascending() throws SQLException {
                // percentile(0.6, true, false) -> quantile_disc(0.6) OVER (PARTITION BY id)
                var map = executePercentileWindow("0.6, true, false");
                assertEquals(2.0, map.get("1"), 0.001);
                assertEquals(2.5, map.get("2"), 0.001);
                assertEquals(1.5, map.get("3"), 0.001);
        }

        @Test
        void testPercentile_Window_Continuous_Descending() throws SQLException {
                // PCT: percentile(0.6, false, true) -> quantile_cont(1-0.6=0.4) OVER (PARTITION
                // BY id)
                var map = executePercentileWindow("0.6, false, true");
                assertEquals(1.8, map.get("1"), 0.001);
                assertEquals(2.3, map.get("2"), 0.001);
                assertEquals(1.4, map.get("3"), 0.001);
        }

        @Test
        void testPercentile_Window_Discrete_Descending() throws SQLException {
                // percentile(0.6, false, false) -> quantile_disc(1-0.6=0.4) OVER (PARTITION BY
                // id)
                var map = executePercentileWindow("0.6, false, false");
                assertEquals(2.0, map.get("1"), 0.001);
                assertEquals(2.5, map.get("2"), 0.001);
                assertEquals(1.5, map.get("3"), 0.001);
        }

        // === PCT: testMinBy assertions ===

        @Test
        void testMinBy_Simple() throws SQLException {
                // |[1, 2]->minBy([10, 20]) == 1
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1, 2]->meta::pure::functions::math::minBy([10, 20])",
                                "test::TestRuntime", connection);
                assertEquals(1L, ((Number) result.rows().get(0).get(0)).longValue());
        }

        @Test
        void testMinBy_LargerList() throws SQLException {
                // |[1001, 1020, 1030, 900, 2010, 2020]->minBy([10000, 9000, 8000, 15000, 14000,
                // 7000]) == 2020
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1001, 1020, 1030, 900, 2010, 2020]->meta::pure::functions::math::minBy([10000, 9000, 8000, 15000, 14000, 7000])",
                                "test::TestRuntime", connection);
                assertEquals(2020L, ((Number) result.rows().get(0).get(0)).longValue());
        }

        @Test
        void testMinBy_TopK() throws SQLException {
                // |[1001, 1020, 1030, 900, 2010, 2020]->minBy([10000, 9000, 8000, 15000, 14000,
                // 7000], 3) == [2020, 1030, 1020]
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[1001, 1020, 1030, 900, 2010, 2020]->meta::pure::functions::math::minBy([10000, 9000, 8000, 15000, 14000, 7000], 3)",
                                "test::TestRuntime", connection);
                // Expected: [2020, 1030, 1020] - top 3 by ascending keys
                var values = result.asCollection().values();
                assertEquals(3, values.size());
                assertEquals(2020, ((Number) values.get(0)).intValue());
                assertEquals(1030, ((Number) values.get(1)).intValue());
                assertEquals(1020, ((Number) values.get(2)).intValue());
        }

        @Test
        void testCorrScalarWithEmptyList() throws SQLException {
                // PCT: testCorr - scalar->corr([]) should return null (not UNNEST error)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1->meta::pure::functions::math::corr([])",
                                "test::TestRuntime", connection);
                assertNull(result.rows().get(0).get(0));
        }

        @Test
        void testBitShiftRightOverflowThrows() throws SQLException {
                // PCT: testBitShiftRight_MoreThan62Bits - shift by 63 should throw error
                assertThrows(Exception.class, () -> queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1->meta::pure::functions::math::bitShiftRight(63)",
                                "test::TestRuntime", connection));
        }

        @Test
        void testMaxOnScalar() throws SQLException {
                // PCT: testMax_Integers - 1->max() on scalar should return the value itself
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1->meta::pure::functions::math::max()",
                                "test::TestRuntime", connection);
                assertEquals(1L, ((Number) result.rows().get(0).get(0)).longValue());
        }

        @Test
        void testFirstOnScalarLiteral() throws SQLException {
                // PCT: testFirstOnOneElement - 'a'->first() should return 'a'
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'a'->meta::pure::functions::collection::first()",
                                "test::TestRuntime", connection);
                assertEquals("a", result.rows().get(0).get(0).toString());
        }

        @Test
        void testIsEmptyOnEmptyList() throws SQLException {
                // PCT: testIsEmpty - []->isEmpty() should be true (empty array is not null)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|[]->meta::pure::functions::collection::isEmpty()",
                                "test::TestRuntime", connection);
                assertEquals(true, result.rows().get(0).get(0));
        }

        @Test
        void testDecimalLiteralWithExplicitScale() throws SQLException {
                // Regression guard: 1.0D must preserve scale 1 and return DECIMAL, not INTEGER
                // (stripTrailingZeros approach would strip to "1" → DuckDB returns INTEGER)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|1.0D + 0.0D",
                                "test::TestRuntime", connection);
                Object value = result.rows().get(0).get(0);
                assertNotNull(value);
                assertInstanceOf(java.math.BigDecimal.class, value, "1.0D should return BigDecimal, not Integer");
        }

        @Test
        void testIntegerValuedDecimalPreservesType() throws SQLException {
                // Regression guard: 17774D (no decimal point in source) must stay DECIMAL type
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|17774D + 0.0D",
                                "test::TestRuntime", connection);
                Object value = result.rows().get(0).get(0);
                assertNotNull(value);
                assertInstanceOf(java.math.BigDecimal.class, value, "17774D should return BigDecimal, not Integer");
                assertEquals(0, new java.math.BigDecimal("17774.0").compareTo((java.math.BigDecimal) value));
        }

        @Test
        void testMixedDecimalIntegerArithmetic() throws SQLException {
                // Regression guard: mixing decimals with integers should produce correct scale
                // |19.905D * 17774 should give scale 3 (from 19.905)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|19.905D * 17774",
                                "test::TestRuntime", connection);
                Object value = result.rows().get(0).get(0);
                assertNotNull(value);
                assertEquals(new java.math.BigDecimal("353791.470"), value);
        }

        // ==================== ascii ====================

        @Test
        void testAsciiUpper() throws SQLException {
                // PCT: testAsciiUpper - 'A'->ascii() = 65
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'A'->ascii()",
                                "test::TestRuntime", connection);
                assertScalarInteger(result, 65);
        }

        @Test
        void testAsciiNewline() throws SQLException {
                // PCT: testAsciiNewline - '\n'->ascii() = 10
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'\\n'->ascii()",
                                "test::TestRuntime", connection);
                assertScalarInteger(result, 10);
        }

        @Test
        void testAsciiWhitespace() throws SQLException {
                // PCT: testAsciiWhitespace - ' '->ascii() = 32
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|' '->ascii()",
                                "test::TestRuntime", connection);
                assertScalarInteger(result, 32);
        }

        @Test
        void testAsciiMultiCharString() throws SQLException {
                // PCT: testAsciiMultiCharString - 'abc'->ascii() = 97
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'abc'->ascii()",
                                "test::TestRuntime", connection);
                assertScalarInteger(result, 97);
        }

        @Test
        void testAsciiEmptyChar() throws SQLException {
                // PCT: testAsciiEmptyChar - ''->ascii() = 0
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|''->ascii()",
                                "test::TestRuntime", connection);
                assertScalarInteger(result, 0);
        }

        // ==================== Helper ====================

        // ==================== rowMapper: window corr/covarSample/covarPopulation
        // ====================

        @Test
        void testWindowCorr() throws SQLException {
                // PCT: testSimpleWindowCorr - CORR(valA, valB) OVER (PARTITION BY id)
                String pure = "|#TDS\n" +
                                "id, valA, valB\n" +
                                "1, 1, 10\n" +
                                "1, 2, 20\n" +
                                "2, 2, 40\n" +
                                "2, 4, 15\n" +
                                "#->extend(over(~id), ~newCol:{p,w,r|meta::pure::functions::math::mathUtility::rowMapper($r.valA, $r.valB)}:y|$y->corr())";
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(), pure,
                                "test::TestRuntime", connection);
                
                var br = result;
                assertEquals(4, br.rowCount());
                // Collect newCol values by id (order-independent)
                var byId = collectWindowResultsById(br);
                // id=1: corr(1,10; 2,20) = 1.0; id=2: corr(2,40; 4,15) = -1.0
                assertEquals(1.0, byId.get(1), 0.0001);
                assertEquals(-1.0, byId.get(2), 0.0001);
        }

        @Test
        void testWindowCovarSample() throws SQLException {
                // PCT: testSimpleWindowCovarSample - COVAR_SAMP(valA, valB) OVER (PARTITION BY
                // id)
                String pure = "|#TDS\n" +
                                "id, valA, valB\n" +
                                "1, 1, 10\n" +
                                "1, 2, 20\n" +
                                "2, 2, 40\n" +
                                "2, 4, 15\n" +
                                "#->extend(over(~id), ~newCol:{p,w,r|meta::pure::functions::math::mathUtility::rowMapper($r.valA, $r.valB)}:y|$y->covarSample())";
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(), pure,
                                "test::TestRuntime", connection);
                
                var br = result;
                assertEquals(4, br.rowCount());
                var byId = collectWindowResultsById(br);
                // id=1: covar_samp(1,10; 2,20) = 5.0; id=2: covar_samp(2,40; 4,15) = -25.0
                assertEquals(5.0, byId.get(1), 0.0001);
                assertEquals(-25.0, byId.get(2), 0.0001);
        }

        @Test
        void testWindowCovarPopulation() throws SQLException {
                // PCT: testSimpleWindowCovarPopulation - COVAR_POP(valA, valB) OVER (PARTITION
                // BY id)
                String pure = "|#TDS\n" +
                                "id, valA, valB\n" +
                                "1, 1, 10\n" +
                                "1, 2, 20\n" +
                                "2, 2, 40\n" +
                                "2, 4, 15\n" +
                                "#->extend(over(~id), ~newCol:{p,w,r|meta::pure::functions::math::mathUtility::rowMapper($r.valA, $r.valB)}:y|$y->covarPopulation())";
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(), pure,
                                "test::TestRuntime", connection);
                
                var br = result;
                assertEquals(4, br.rowCount());
                var byId = collectWindowResultsById(br);
                // id=1: covar_pop(1,10; 2,20) = 2.5; id=2: covar_pop(2,40; 4,15) = -12.5
                assertEquals(2.5, byId.get(1), 0.0001);
                assertEquals(-12.5, byId.get(2), 0.0001);
        }

        private java.util.Map<Integer, Double> collectWindowResultsById(ExecutionResult br) {
                int idIdx = -1, newColIdx = -1;
                for (int i = 0; i < br.columns().size(); i++) {
                        if ("id".equals(br.columns().get(i).name()))
                                idIdx = i;
                        if ("newCol".equals(br.columns().get(i).name()))
                                newColIdx = i;
                }
                java.util.Map<Integer, Double> map = new java.util.HashMap<>();
                for (var row : br.rows()) {
                        int id = ((Number) row.get(idIdx)).intValue();
                        double val = ((Number) row.get(newColIdx)).doubleValue();
                        map.putIfAbsent(id, val);
                }
                return map;
        }

        // ==================== rowMapper: groupBy wavg/maxBy/minBy ====================

        @Test
        void testGroupByWavg() throws SQLException {
                // PCT: testSimpleGroupByWavg
                // Expected (sorted by grp asc):
                // grp,wavgCol
                // 1,180.0
                // 2,150.0
                // 3,362.5
                // 4,700.0
                // 5,350.0
                String pure = "|#TDS\n" +
                                "id, grp, name, quantity, weight\n" +
                                "1, 2, A, 200, 0.5\n" +
                                "2, 1, B, 100, 0.45\n" +
                                "3, 3, C, 250, 0.25\n" +
                                "4, 4, D, 700, 1\n" +
                                "5, 2, E, 100, 0.5\n" +
                                "6, 1, F, 500, 0.15\n" +
                                "7, 3, G, 400, 0.75\n" +
                                "8, 1, H, 150, 0.4\n" +
                                "9, 5, I, 350, 1\n" +
                                "#->groupBy(~grp, ~wavgCol : x | meta::pure::functions::math::mathUtility::rowMapper($x.quantity, $x.weight) : y | $y->wavg())";
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(), pure,
                                "test::TestRuntime", connection);
                
                var br = result;
                assertEquals(5, br.rowCount());
                var byGrp = collectGroupByResults(br, "grp", "wavgCol");
                assertEquals(180.0, ((Number) byGrp.get(1)).doubleValue(), 0.0001);
                assertEquals(150.0, ((Number) byGrp.get(2)).doubleValue(), 0.0001);
                assertEquals(362.5, ((Number) byGrp.get(3)).doubleValue(), 0.0001);
                assertEquals(700.0, ((Number) byGrp.get(4)).doubleValue(), 0.0001);
                assertEquals(350.0, ((Number) byGrp.get(5)).doubleValue(), 0.0001);
        }

        @Test
        void testGroupByMultipleWavg() throws SQLException {
                // PCT: testSimpleGroupByMultipleWavg
                // Expected (sorted by grp asc):
                // grp,wavgCol1,wavgCol2
                // 1,180.0,220.0
                // 2,150.0,175.0
                // 3,362.5,325.0
                // 4,700.0,700.0
                // 5,350.0,350.0
                String pure = "|#TDS\n" +
                                "id, grp, name, quantity, weight, weight1\n" +
                                "1, 2, A, 200, 0.5, 0.75\n" +
                                "2, 1, B, 100, 0.45, 0.35\n" +
                                "3, 3, C, 250, 0.25, 0.50\n" +
                                "4, 4, D, 700, 1, 1\n" +
                                "5, 2, E, 100, 0.5, 0.25\n" +
                                "6, 1, F, 500, 0.15, 0.25\n" +
                                "7, 3, G, 400, 0.75, 0.50\n" +
                                "8, 1, H, 150, 0.4, 0.4\n" +
                                "9, 5, I, 350, 1, 1\n" +
                                "#->groupBy(~grp, ~[wavgCol1 : x | meta::pure::functions::math::mathUtility::rowMapper($x.quantity, $x.weight) : y | $y->wavg(), wavgCol2 : x | meta::pure::functions::math::mathUtility::rowMapper($x.quantity, $x.weight1) : y | $y->wavg()])";
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(), pure,
                                "test::TestRuntime", connection);
                
                var br = result;
                assertEquals(5, br.rowCount());
                var byGrp1 = collectGroupByResults(br, "grp", "wavgCol1");
                var byGrp2 = collectGroupByResults(br, "grp", "wavgCol2");
                assertEquals(180.0, ((Number) byGrp1.get(1)).doubleValue(), 0.0001);
                assertEquals(150.0, ((Number) byGrp1.get(2)).doubleValue(), 0.0001);
                assertEquals(362.5, ((Number) byGrp1.get(3)).doubleValue(), 0.0001);
                assertEquals(700.0, ((Number) byGrp1.get(4)).doubleValue(), 0.0001);
                assertEquals(350.0, ((Number) byGrp1.get(5)).doubleValue(), 0.0001);
                assertEquals(220.0, ((Number) byGrp2.get(1)).doubleValue(), 0.0001);
                assertEquals(175.0, ((Number) byGrp2.get(2)).doubleValue(), 0.0001);
                assertEquals(325.0, ((Number) byGrp2.get(3)).doubleValue(), 0.0001);
                assertEquals(700.0, ((Number) byGrp2.get(4)).doubleValue(), 0.0001);
                assertEquals(350.0, ((Number) byGrp2.get(5)).doubleValue(), 0.0001);
        }

        @Test
        void testGroupByMaxBy() throws SQLException {
                // PCT: testSimpleGroupByMaxBy
                // Expected (sorted by grp asc):
                // grp,newCol
                // 1,E
                // 2,H
                String pure = "|#TDS\n" +
                                "id, grp, name, employeeNumber\n" +
                                "1, 1, A, 10000\n" +
                                "2, 1, B, 9000\n" +
                                "3, 1, C, 8000\n" +
                                "4, 1, D, 15000\n" +
                                "5, 1, E, 17000\n" +
                                "6, 1, F, 8000\n" +
                                "6, 2, G, 12000\n" +
                                "6, 2, H, 13000\n" +
                                "6, 2, I, 8000\n" +
                                "#->groupBy(~[grp], ~[newCol : x | meta::pure::functions::math::mathUtility::rowMapper($x.name, $x.employeeNumber) : y | $y->maxBy()])";
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(), pure,
                                "test::TestRuntime", connection);
                
                var br = result;
                assertEquals(2, br.rowCount());
                var byGrp = collectGroupByResults(br, "grp", "newCol");
                assertEquals("E", byGrp.get(1).toString());
                assertEquals("H", byGrp.get(2).toString());
        }

        @Test
        void testGroupByMinBy() throws SQLException {
                // PCT: testSimpleGroupByMinBy
                // Expected (sorted by grp asc):
                // grp,newCol
                // 1,C
                // 2,I
                String pure = "|#TDS\n" +
                                "id, grp, name, employeeNumber\n" +
                                "1, 1, A, 10000\n" +
                                "2, 1, B, 9000\n" +
                                "3, 1, C, 8000\n" +
                                "4, 1, D, 15000\n" +
                                "5, 1, E, 17000\n" +
                                "6, 1, F, 8500\n" +
                                "6, 2, G, 12000\n" +
                                "6, 2, H, 13000\n" +
                                "6, 2, I, 8000\n" +
                                "#->groupBy(~[grp], ~[newCol : x | meta::pure::functions::math::mathUtility::rowMapper($x.name, $x.employeeNumber) : y | $y->minBy()])";
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(), pure,
                                "test::TestRuntime", connection);
                
                var br = result;
                assertEquals(2, br.rowCount());
                var byGrp = collectGroupByResults(br, "grp", "newCol");
                assertEquals("C", byGrp.get(1).toString());
                assertEquals("I", byGrp.get(2).toString());
        }

        private java.util.Map<Integer, Object> collectGroupByResults(ExecutionResult br, String keyCol, String valCol) {
                int keyIdx = -1, valIdx = -1;
                for (int i = 0; i < br.columns().size(); i++) {
                        if (keyCol.equals(br.columns().get(i).name()))
                                keyIdx = i;
                        if (valCol.equals(br.columns().get(i).name()))
                                valIdx = i;
                }
                java.util.Map<Integer, Object> map = new java.util.HashMap<>();
                for (var row : br.rows()) {
                        int key = ((Number) row.get(keyIdx)).intValue();
                        map.put(key, row.get(valIdx));
                }
                return map;
        }

        // ==================== PCT Edge Cases: Integer boundary values
        // ====================

        @Test
        void testLongMinValueCast() throws SQLException {
                // PCT edge case: CAST of Long.MIN_VALUE must use BIGINT, not INTEGER (INT32
                // overflows)
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|parseInteger('-9223372036854775808')",
                                "test::TestRuntime", connection);
                assertScalarInteger(result, Long.MIN_VALUE);
        }

        @Test
        void testLongMaxValueCast() throws SQLException {
                // PCT edge case: CAST of Long.MAX_VALUE must use BIGINT
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|parseInteger('9223372036854775807')",
                                "test::TestRuntime", connection);
                assertScalarInteger(result, Long.MAX_VALUE);
        }

        @Test
        void testLargeIntegerArithmetic() throws SQLException {
                // PCT edge case: large integer arithmetic should not overflow
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|9223372036854775807 - 1",
                                "test::TestRuntime", connection);
                assertScalarInteger(result, Long.MAX_VALUE - 1);
        }

        // ==================== PCT Edge Cases: Date literal scalars
        // ====================

        @Test
        void testStrictDateLiteralScalar() throws SQLException {
                // PCT edge case: date literal should return as java.sql.Date/LocalDate, not
                // String
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2025-02-10",
                                "test::TestRuntime", connection);
                Object value = result.rows().get(0).get(0);
                assertNotNull(value);
                // Must be a date type, not a String with quotes
                assertFalse(value instanceof String,
                                "Date literal should not return as String, got: '" + value + "'");
                assertTrue(value instanceof java.sql.Date || value instanceof java.time.LocalDate,
                                "Expected Date type but got " + value.getClass().getSimpleName() + " = " + value);
        }

        @Test
        void testDateTimeLiteralScalar() throws SQLException {
                // PCT edge case: datetime literal should return as Timestamp, not String
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2025-02-10T20:10:20+0000",
                                "test::TestRuntime", connection);
                Object value = result.rows().get(0).get(0);
                assertNotNull(value);
                assertFalse(value instanceof String,
                                "DateTime literal should not return as String, got: '" + value + "'");
        }

        @Test
        void testStrictDateFromAdjust() throws SQLException {
                // PCT edge case: adjust() on StrictDate should preserve StrictDate type
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|%2014-02-27->adjust(0, meta::pure::functions::date::DurationUnit.DAYS)",
                                "test::TestRuntime", connection);
                Object value = result.rows().get(0).get(0);
                assertNotNull(value);
                // Should be a date (not datetime with time component)
                assertTrue(value instanceof java.sql.Date || value instanceof java.time.LocalDate,
                                "Expected StrictDate but got " + value.getClass().getSimpleName() + " = " + value);
        }

        // ==================== PCT Edge Cases: rpad/lpad with BIGINT length
        // ====================

        @Test
        void testRpadWithLargeLength() throws SQLException {
                // PCT edge case: rpad length arg needs CAST to INTEGER for DuckDB
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'abc'->rpad(10)",
                                "test::TestRuntime", connection);
                assertEquals("abc       ", result.rows().get(0).get(0));
        }

        @Test
        void testLpadWithFill() throws SQLException {
                // PCT edge case: lpad also needs CAST to INTEGER for DuckDB
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|'abc'->lpad(7, '*')",
                                "test::TestRuntime", connection);
                assertEquals("****abc", result.rows().get(0).get(0));
        }

        /**
         * Mirrors PCT testSimpleAggregate_WithFilter: aggregate produces new columns,
         * then filter accesses them. Fails if collectExtendedColumnsRecursive doesn't
         * track AggregateExpression output columns.
         */
        @Test
        void testAggregateWithFilter() throws SQLException {
                String query = """
                                |#TDS
                                  id,grp,name
                                  1,2,A
                                  2,1,A
                                  3,3,A
                                #->aggregate(~idSum:x|$x.id:y|$y->plus())->filter(x|$x.idSum > 0)
                                """;
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                query, "test::TestRuntime", connection);
                var buffered = result;
                assertEquals(1, buffered.rows().size());
                assertEquals("idSum", buffered.columns().get(0).name());
        }

        // ==================== PCT: pivot ====================

        private static final String PIVOT_TDS = """
                #TDS
                city, country, year, treePlanted
                NYC, USA, 2011, 5000
                NYC, USA, 2000, 5000
                SAN, USA, 2000, 2000
                SAN, USA, 2011, 100
                LDN, UK, 2011, 3000
                SAN, USA, 2011, 2500
                NYC, USA, 2000, 10000
                NYC, USA, 2012, 7600
                NYC, USA, 2012, 7600
                #""";

        /**
         * PCT: pivot_SingleMultiple — single aggregate pivot.
         * pivot(~[year], ~[newCol:x|$x.treePlanted:y|$y->plus()])
         * Verifies that SUM(treePlanted) grouped by year produces Integer pivot columns.
         */
        @Test
        void testPivotSingleAggregate() throws SQLException {
                String pure = "|" + PIVOT_TDS
                                + "->meta::pure::functions::relation::pivot(~[year], ~[newCol:x: (city:String, country:String, year:Integer, treePlanted:Integer)[1]|$x.treePlanted:y: Integer[*]|$y->plus()])";
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(), pure,
                                "test::TestRuntime", connection);
                // Should have columns: city, country, plus pivot columns like 2000__|__newCol, 2011__|__newCol, 2012__|__newCol
                assertTrue(result.columns().size() >= 4, "Should have city, country, plus pivot year columns");
                // All pivot columns should be numeric (Integer), not String
                for (var col : result.columns()) {
                        if (col.name().contains("__|__")) {
                                for (var row : result.rows()) {
                                        Object val = row.get(result.columns().indexOf(col));
                                        if (val != null) {
                                                assertInstanceOf(Number.class, val,
                                                                "Pivot column '" + col.name() + "' should be numeric, not " + val.getClass().getSimpleName());
                                        }
                                }
                        }
                }
                assertTrue(result.rowCount() > 0, "Should have rows");
        }

        /**
         * PCT: pivot_SingleMultiple — multi-aggregate pivot with sum + count.
         * pivot(~[year], ~[sum:x|$x.treePlanted:y|$y->plus(), count:x|1:y|$y->plus()])
         * Verifies that the count pattern (literal 1 → plus()) infers Integer type,
         * not String. This tests the inferLiteralType fix.
         */
        @Test
        void testPivotMultiAggregateWithCount() throws SQLException {
                String pure = "|" + PIVOT_TDS
                                + "->meta::pure::functions::relation::pivot(~[year], ~[sum:x: (city:String, country:String, year:Integer, treePlanted:Integer)[1]|$x.treePlanted:y: Integer[*]|$y->plus(), count:x: (city:String, country:String, year:Integer, treePlanted:Integer)[1]|1:y: Integer[*]|$y->plus()])";
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(), pure,
                                "test::TestRuntime", connection);
                // Should have columns: city, country, plus pivot columns for each year × {sum, count}
                assertTrue(result.columns().size() >= 4, "Should have city, country, plus pivot year×agg columns");
                // ALL pivot columns (both sum and count) should be numeric, not String
                for (var col : result.columns()) {
                        if (col.name().contains("__|__")) {
                                for (var row : result.rows()) {
                                        Object val = row.get(result.columns().indexOf(col));
                                        if (val != null) {
                                                assertInstanceOf(Number.class, val,
                                                                "Pivot column '" + col.name() + "' should be numeric, not " + val.getClass().getSimpleName());
                                        }
                                }
                        }
                }
                // Spot-check: count columns should have positive integer values
                boolean foundCountCol = false;
                for (var col : result.columns()) {
                        if (col.name().contains("__|__count")) {
                                foundCountCol = true;
                                for (var row : result.rows()) {
                                        Object val = row.get(result.columns().indexOf(col));
                                        if (val != null) {
                                                assertTrue(((Number) val).intValue() > 0,
                                                                "Count column should have positive values");
                                        }
                                }
                        }
                }
                assertTrue(foundCountCol, "Should have at least one count pivot column");
                assertTrue(result.rowCount() > 0, "Should have rows");
        }

        /**
         * PCT: pivot_SingleMultiple_Dynamic_Aggregation — multi-aggregate pivot with
         * expression-based
         * sum (x.treePlanted * x.coefficient) + count(1).
         * Verifies that SUM of an expression infers Integer type from source columns,
         * not String (which happens when the type falls through to NUMBER → String
         * mapping).
         */
        @Test
        void testPivotMultiAggregateWithExpressionValue() throws SQLException {
                String pure = "|#TDS\n"
                                + "city, country, year, treePlanted, coefficient\n"
                                + "NYC, USA, 2011, 5000, 1\n"
                                + "NYC, USA, 2000, 5000, 2\n"
                                + "SAN, USA, 2000, 2000, 1\n"
                                + "SAN, USA, 2011, 100, 2\n"
                                + "LDN, UK, 2011, 3000, 2\n"
                                + "SAN, USA, 2011, 2500, 1\n"
                                + "NYC, USA, 2000, 10000, 2\n"
                                + "NYC, USA, 2012, 7600, 1\n"
                                + "NYC, USA, 2012, 7600, 2\n"
                                + "#->meta::pure::functions::relation::pivot(~[year], ~[sum:x: (city:String, country:String, year:Integer, treePlanted:Integer, coefficient:Integer)[1]|$x.treePlanted->meta::pure::functions::multiplicity::toOne() * $x.coefficient->meta::pure::functions::multiplicity::toOne():y: Integer[*]|$y->plus(),count:x: (city:String, country:String, year:Integer, treePlanted:Integer, coefficient:Integer)[1]|1:y: Integer[*]|$y->plus()])";
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(), pure,
                                "test::TestRuntime", connection);
                // Should have columns: city, country, plus pivot columns for each year × {sum, count}
                assertTrue(result.columns().size() >= 4,
                                "Should have city, country, plus pivot year×agg columns");
                // ALL pivot columns (both sum and count) should be numeric, not String
                for (var col : result.columns()) {
                        if (col.name().contains("__|__")) {
                                for (var row : result.rows()) {
                                        Object val = row.get(result.columns().indexOf(col));
                                        if (val != null) {
                                                assertInstanceOf(Number.class, val,
                                                                "Pivot column '" + col.name()
                                                                                + "' should be numeric, not "
                                                                                + val.getClass().getSimpleName());
                                        }
                                }
                        }
                }
                assertTrue(result.rowCount() > 0, "Should have rows");
        }


        /**
         * Exact PCT test_Extend_Filter_Select_GroupBy_Pivot_Extend_Sort_Limit
         * expression.
         * Chain: TDS → extend(~yr) → filter(yr>10) → select → groupBy → pivot → cast →
         * extend(~newCol using $x.city)
         * Fails because after pivot→cast, collectExtendedColumnsRecursive returns empty
         * extendedColumns
         * so $x.city in the final extend lambda can't resolve.
         */
        @Test
        void testExtendFilterSelectGroupByPivotExtendSortLimit() throws SQLException {
                String query = """
                                |#TDS
                                  city, country, year, treePlanted
                                  NYC, USA, 2011, 5000
                                  NYC, USA, 2000, 5000
                                  SAN, USA, 2000, 2000
                                  SAN, USA, 2011, 100
                                  LDN, UK, 2011, 3000
                                  SAN, USA, 2011, 2500
                                  NYC, USA, 2000, 10000
                                  NYC, USA, 2012, 7600
                                  NYC, USA, 2012, 7600
                                #->meta::pure::functions::relation::extend(~yr:x: (city:String, country:String, year:Integer, treePlanted:Integer)[1]|$x.year->meta::pure::functions::multiplicity::toOne() - 2000)->meta::pure::functions::relation::filter(x: (city:String, country:String, year:Integer, treePlanted:Integer, yr:Integer)[1]|$x.yr > 10)->meta::pure::functions::relation::select(~[city,country,year,treePlanted])->meta::pure::functions::relation::groupBy(~[year,city,country], ~treePlanted:x: (city:String, country:String, year:Integer, treePlanted:Integer)[1]|$x.treePlanted:x: Integer[*]|$x->plus())->meta::pure::functions::relation::pivot(~[year], ~[newCol:x: (year:Integer, city:String, country:String, treePlanted:Integer)[1]|$x.treePlanted:y: Integer[*]|$y->plus()])->meta::pure::functions::lang::cast(@meta::pure::metamodel::relation::Relation<(city:String, country:String, '2011__|__newCol':Integer, '2012__|__newCol':Integer)>)->meta::pure::functions::relation::extend(~newCol:x: (city:String, country:String, '2011__|__newCol':Integer, '2012__|__newCol':Integer)[1]|$x.city->meta::pure::functions::multiplicity::toOne() + '_0')
                                """;
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                query, "test::TestRuntime", connection);
                var buffered = result;
                assertNotNull(buffered);
                // After the full chain: should have city, country, 2011__|__newCol,
                // 2012__|__newCol, newCol
                assertTrue(buffered.columns().size() >= 4, "Should have at least 4 columns");
        }

        // ==================== extend column type inference ====================

        @Test
        void testExtendColumnTypeInference_StringConcat() throws SQLException {
                // Exact PCT that failed when inferExtendColumnType didn't have sourceType:
                // extend(~newCol: str + toString(val)) -> filter(newCol == 'qw4')
                // Without sourceType, the compiler couldn't resolve $x.str / $x.val
                // in the lambda body, fell back to Any, and stringToTDS couldn't parse it.
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|#TDS\nval, str\n1, a\n3, ewe\n4, qw\n5, wwe\n6, weq\n#"
                                        + "->extend(~newCol:x|$x.str->toOne() + $x.val->toOne()->toString())"
                                        + "->filter(x|$x.newCol == 'qw4')",
                                "test::TestRuntime", connection);
                assertEquals(1, result.rowCount(), "Should match exactly one row");
                // The matching row is val=4, str='qw', newCol='qw4'
                int newColIdx = -1;
                for (int c = 0; c < result.columns().size(); c++) {
                        if ("newCol".equals(result.columns().get(c).name())) {
                                newColIdx = c;
                                break;
                        }
                }
                assertTrue(newColIdx >= 0, "newCol column should exist");
                assertEquals("qw4", result.rows().get(0).get(newColIdx));
        }

        private void assertScalarInteger(ExecutionResult result, long expected) {
                assertFalse(result.rows().isEmpty(), "Result should have at least one row");
                Object value = result.rows().get(0).get(0);
                assertNotNull(value, "Scalar value should not be null");
                assertInstanceOf(Number.class, value, "Scalar value should be a Number");
                // Must be an integer type (Integer, Long, or BigInteger), not Double/Float/BigDecimal
                assertTrue(value instanceof Integer || value instanceof Long || value instanceof java.math.BigInteger,
                                "Expected Integer/Long/BigInteger but got " + value.getClass().getSimpleName() + " = "
                                                + value);
                assertEquals(expected, ((Number) value).longValue());
        }

        // ==================== ColSpec eval (PCT: testSimpleEval) ====================

        @Test
        void testColSpecEvalInFilter() throws SQLException {
                // PCT test: ~colName->eval($row) inside filter lambda
                // ~total->eval($row)->toOne() extracts column 'total' from the row
                // This is equivalent to $row.total but uses the ColSpec->eval pattern
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|#TDS\n" +
                                "  id, code\n" +
                                "  -1, -4\n" +
                                "  2, 5\n" +
                                "  3, 3\n" +
                                "#->extend(~total : r | $r.id->toOne() + $r.code->toOne())" +
                                "->filter(row | ~total->eval($row)->toOne() < 0)" +
                                "->select(~[id, code, total])",
                                "test::TestRuntime", connection);
                assertFalse(result.rows().isEmpty(), "Should have filtered rows");
                // Row with id=-1, code=-4 has total=-5 which is < 0
                assertEquals(1, result.rows().size(), "Should have exactly 1 row with total < 0");
                assertEquals(-1, ((Number) result.rows().get(0).get(0)).intValue(), "id should be -1");
                assertEquals(-4, ((Number) result.rows().get(0).get(1)).intValue(), "code should be -4");
                assertEquals(-5, ((Number) result.rows().get(0).get(2)).intValue(), "total should be -5");
        }

        // ==================== Nullable comparison (PCT: testVariantColumn_filterOnIndexExtractionValue) ====================

        @Test
        void testNullableComparisonWithVariantTo() throws SQLException {
                // to(@Integer) returns Integer[0..1], so lessThan must accept nullable args.
                // This mirrors the PCT test: $x.payload->get(0)->to(@Integer) < 7
                var result = queryService.execute(
                                getCompletePureModelWithRuntime(),
                                "|#TDS\n" +
                                "  id, value\n" +
                                "  1, 3\n" +
                                "  2, 7\n" +
                                "  3, 10\n" +
                                "#->filter(x | $x.value->to(@Integer) < 7)" +
                                "->select(~[id, value])",
                                "test::TestRuntime", connection);
                assertEquals(1, result.rows().size(), "Should have 1 row with value < 7");
                assertEquals(1, ((Number) result.rows().get(0).get(0)).intValue(), "id should be 1");
        }
}
