package org.finos.legend.engine.test;

import org.finos.legend.engine.execution.BufferedResult;
import org.finos.legend.engine.execution.ScalarResult;
import org.finos.legend.engine.execution.Result;
import org.finos.legend.engine.server.QueryService;
import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLDialect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;

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
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|17.6->round()",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertScalarInteger(result, 18L);
    }

    @Test
    void testRoundNegativeFloatReturnsInteger() throws SQLException {
        // Pure: |round(-17.6) -> -18 (Integer)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|round(-17.6)",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertScalarInteger(result, -18L);
    }

    @Test
    void testRoundIntegerReturnsInteger() throws SQLException {
        // Pure: |round(17) -> 17 (Integer, not 17.0 Double)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|17->round()",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertScalarInteger(result, 17L);
    }

    // ==================== sign() must return Integer ====================

    @Test
    void testSignNegativeReturnsInteger() throws SQLException {
        // Pure: |sign(-10) -> -1 (Integer)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|-10->sign()",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertScalarInteger(result, -1L);
    }

    @Test
    void testSignPositiveReturnsInteger() throws SQLException {
        // Pure: |sign(5) -> 1 (Integer)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|5->sign()",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertScalarInteger(result, 1L);
    }

    // ==================== floor()/ceiling() must return Integer ====================

    @Test
    void testFloorReturnsInteger() throws SQLException {
        // Pure: |floor(3.7) -> 3 (Integer)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|3.7->floor()",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertScalarInteger(result, 3L);
    }

    @Test
    void testCeilingReturnsInteger() throws SQLException {
        // Pure: |ceiling(3.2) -> 4 (Integer)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|3.2->ceiling()",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertScalarInteger(result, 4L);
    }

    // ==================== hashCode() must return Integer ====================

    @Test
    void testHashCodeReturnsInteger() throws SQLException {
        // Pure: |'a'->hash() -> Integer (DuckDB HASH returns UBIGINT/BigInteger)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'a'->hash()",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult, "Expected ScalarResult");
        Object value = ((ScalarResult) result).value();
        assertNotNull(value);
        // HASH returns UBIGINT (BigInteger via JDBC) — must be Integer or Long, not Double
        assertTrue(value instanceof Integer || value instanceof Long || value instanceof java.math.BigInteger,
                "Expected integer type but got " + value.getClass().getSimpleName() + " = " + value);
    }

    // ==================== Integer arithmetic stays Integer ====================

    @Test
    void testIntegerAdditionReturnsInteger() throws SQLException {
        // Pure: |1 + 2 -> 3 (Integer)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|1 + 2",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertScalarInteger(result, 3L);
    }

    @Test
    void testIntegerMultiplicationReturnsInteger() throws SQLException {
        // Pure: |3 * 4 -> 12 (Integer)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|3 * 4",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertScalarInteger(result, 12L);
    }

    // ==================== dateDiff must use correct argument order ====================

    @Test
    void testDateDiffDays() throws SQLException {
        // Pure: |%2015-01-01->dateDiff(%2015-01-10, DurationUnit.DAYS) -> 9
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|%2015-01-01->dateDiff(%2015-01-10, meta::pure::functions::date::DurationUnit.DAYS)",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertScalarInteger(result, 9L);
    }

    @Test
    void testDateDiffYears() throws SQLException {
        // Pure: |%2015->dateDiff(%2016, DurationUnit.YEARS) -> 1
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|%2015->dateDiff(%2016, meta::pure::functions::date::DurationUnit.YEARS)",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertScalarInteger(result, 1L);
    }

    // ==================== date() with time components ====================

    @Test
    void testDateWithTimeComponentsReturnsTimestamp() throws SQLException {
        // Pure: |1973->date(11, 13, 23, 9) -> DateTime (not StrictDate)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|1973->date(11, 13, 23, 9)",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        Object value = ((ScalarResult) result).value();
        assertNotNull(value);
        // Should be a timestamp, not a date
        assertTrue(value.toString().contains("23:09") || value.toString().contains("23:09:00"),
                "Expected timestamp with time but got: " + value);
    }

    @Test
    void testPartialTimestampPadding() throws SQLException {
        // Pure: |%2015-04-15T17 should generate TIMESTAMP '2015-04-15 17:00:00'
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|%2015-04-15T17->hour()",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertScalarInteger(result, 17L);
    }

    // ==================== format() / printf ====================

    @Test
    void testFormatStringSubstitution() throws SQLException {
        // Pure: |'hello %s'->format('world') -> 'hello world'
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'the quick brown %s jumps over the lazy %s'->format(['fox', 'dog'])",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("the quick brown fox jumps over the lazy dog", ((ScalarResult) result).value());
    }

    @Test
    void testFormatIntegerSubstitution() throws SQLException {
        // Pure: |'value is %d'->format(42) -> 'value is 42'
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'the quick brown %s jumps over the lazy %d'->format(['fox', 3])",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("the quick brown fox jumps over the lazy 3", ((ScalarResult) result).value());
    }

    @Test
    void testFormatFloatSubstitution() throws SQLException {
        // Pure: |'pi is %.2f'->format(3.14159) -> 'pi is 3.14'
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'the quick brown %s jumps over the lazy %.2f'->format(['fox', 1.338])",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("the quick brown fox jumps over the lazy 1.34", ((ScalarResult) result).value());
    }

    @Test
    void testFormatDateSubstitution() throws SQLException {
        // Pure: |'on %t{yyyy-MM-dd}'->format(%2014-03-10) -> 'on 2014-03-10'
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'on %t{yyyy-MM-dd}'->format(%2014-03-10)",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("on 2014-03-10", ((ScalarResult) result).value());
    }

    // ==================== between() ====================

    @Test
    void testBetweenInteger() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|1->between(0, 3)",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(true, ((ScalarResult) result).value());
    }

    // ==================== char() -> chr() ====================

    @Test
    void testCharSpace() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|32->char()",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(" ", ((ScalarResult) result).value());
    }

    // ==================== parseInteger / parseFloat / parseBoolean ====================

    @Test
    void testParseInteger() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'17'->parseInteger()",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertScalarInteger(result, 17L);
    }

    @Test
    void testParseFloat() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'3.14'->parseFloat()",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        Object val = ((ScalarResult) result).value();
        assertInstanceOf(Number.class, val);
        assertEquals(3.14, ((Number) val).doubleValue(), 0.001);
    }

    @Test
    void testParseBoolean() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'true'->parseBoolean()",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(true, ((ScalarResult) result).value());
    }

    // ==================== ArrayLiteral collection operations ====================

    @Test
    void testArrayDrop() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[1, 2, 3]->drop(1)",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("[2, 3]", ((ScalarResult) result).value().toString());
    }

    @Test
    void testArrayTake() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[1, 2, 3]->take(2)",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("[1, 2]", ((ScalarResult) result).value().toString());
    }

    @Test
    void testArraySlice() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[1, 2, 3, 4]->slice(1, 3)",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("[2, 3]", ((ScalarResult) result).value().toString());
    }

    @Test
    void testArrayFirst() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[10, 20, 30]->first()",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(10, ((Number) ((ScalarResult) result).value()).intValue());
    }

    @Test
    void testArrayConcatenate() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[1, 2, 3]->concatenate([4, 5])",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("[1, 2, 3, 4, 5]", ((ScalarResult) result).value().toString());
    }

    // ==================== List aggregate functions ====================

    @Test
    void testListSum() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[12.5, 13.5, 4.0, 1.5, 0.5]->sum()",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        Object val = ((ScalarResult) result).value();
        assertInstanceOf(Number.class, val);
        assertEquals(32.0, ((Number) val).doubleValue(), 0.001);
    }

    @Test
    void testListSumIntegers() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[1, 2, 3]->sum()",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        Object val = ((ScalarResult) result).value();
        assertInstanceOf(Number.class, val);
        assertEquals(6, ((Number) val).longValue());
    }

    @Test
    void testListMode() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[5.0, 5.0, 5.0, 2.0, 2.0]->mode()",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        Object val = ((ScalarResult) result).value();
        assertInstanceOf(Number.class, val);
        assertEquals(5.0, ((Number) val).doubleValue(), 0.001);
    }

    @Test
    void testListModeSingleValue() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|1.0->mode()",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        Object val = ((ScalarResult) result).value();
        assertInstanceOf(Number.class, val);
        assertEquals(1.0, ((Number) val).doubleValue(), 0.001);
    }

    @Test
    void testListMedian() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[1, 2, 3, 4, 5]->median()",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        Object val = ((ScalarResult) result).value();
        assertInstanceOf(Number.class, val);
        assertEquals(3.0, ((Number) val).doubleValue(), 0.001);
    }

    @Test
    void testListVarianceSample() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[1.0, 2.0, 3.0]->varianceSample()",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        Object val = ((ScalarResult) result).value();
        assertInstanceOf(Number.class, val);
        assertEquals(1.0, ((Number) val).doubleValue(), 0.001);
    }

    @Test
    void testListVariancePopulation() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[1, 2]->variancePopulation()",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        Object val = ((ScalarResult) result).value();
        assertInstanceOf(Number.class, val);
        assertEquals(0.25, ((Number) val).doubleValue(), 0.001);
    }

    @Test
    void testListCorr() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[1, 2]->corr([10, 20])",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        Object val = ((ScalarResult) result).value();
        assertInstanceOf(Number.class, val);
        assertEquals(1.0, ((Number) val).doubleValue(), 0.001);
    }

    @Test
    void testListCovarPopulation() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[1, 2]->covarPopulation([10, 20])",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        Object val = ((ScalarResult) result).value();
        assertInstanceOf(Number.class, val);
        assertEquals(2.5, ((Number) val).doubleValue(), 0.001);
    }

    @Test
    void testEvalFunctionReference() throws SQLException {
        // acos_Number_1__Float_1_->eval(0.5) should compile to ACOS(0.5)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|meta::pure::functions::math::acos_Number_1__Float_1_->eval(0.5)",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        Object val = ((ScalarResult) result).value();
        assertInstanceOf(Number.class, val);
        assertEquals(Math.acos(0.5), ((Number) val).doubleValue(), 0.001);
    }

    @Test
    void testHashCodeAggregate() throws SQLException {
        // hashCode as aggregate in groupBy -> HASH(LIST(col))
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|#TDS\n" +
                "  id, grp, val\n" +
                "  1, 1, 10.0\n" +
                "  2, 1, 20.0\n" +
                "  3, 2, 30.0\n" +
                "  4, 2, 40.0\n" +
                "#->groupBy(~grp, ~h : x | $x.val : y | $y->hashCode())",
                "test::TestRuntime",
                connection,
                QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof BufferedResult);
        BufferedResult br = (BufferedResult) result;
        // Should have 2 groups, each with a non-null hash value
        assertEquals(2, br.rows().size());
        assertNotNull(br.rows().get(0).get(1));
        assertNotNull(br.rows().get(1).get(1));
        // Hash values should be different for different groups
        assertNotEquals(br.rows().get(0).get(1), br.rows().get(1).get(1));
    }

    // ==================== Scalar aggregate functions (should pass through, not use list_*) ====================

    @Test
    void testScalarAverageFloat() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|1.0->average()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(1.0, ((Number) ((ScalarResult) result).value()).doubleValue(), 0.001);
    }

    @Test
    void testScalarSumFloat() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|2.5->sum()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(2.5, ((Number) ((ScalarResult) result).value()).doubleValue(), 0.001);
    }

    @Test
    void testScalarSumInteger() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|7->sum()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertScalarInteger(result, 7);
    }

    @Test
    void testScalarModeFloat() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|3.14->mode()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(3.14, ((Number) ((ScalarResult) result).value()).doubleValue(), 0.001);
    }

    @Test
    void testScalarAverageInteger() throws SQLException {
        // average() always returns Float, even for integer input
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|11->average()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(11.0, ((Number) ((ScalarResult) result).value()).doubleValue(), 0.001);
    }

    @Test
    void testScalarMeanFloat() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|5.0->mean()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(5.0, ((Number) ((ScalarResult) result).value()).doubleValue(), 0.001);
    }

    // ==================== AND/OR collection functions ====================

    @Test
    void testAndSingleTrue() throws SQLException {
        // and(true) via function-call syntax -> true
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|and(true)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(true, ((ScalarResult) result).value());
    }

    @Test
    void testAndFunctionCallWithList() throws SQLException {
        // and([true, false]) via function-call syntax -> false
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|and([true, false])",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(false, ((ScalarResult) result).value());
    }

    @Test
    void testAndListTrue() throws SQLException {
        // and([true, true]) -> true
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[true, true]->and()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(true, ((ScalarResult) result).value());
    }

    @Test
    void testAndListFalse() throws SQLException {
        // and([true, false]) -> false
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[true, false]->and()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(false, ((ScalarResult) result).value());
    }

    @Test
    void testOrListTrue() throws SQLException {
        // or([false, true]) -> true
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[false, true]->or()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(true, ((ScalarResult) result).value());
    }

    @Test
    void testOrListFalse() throws SQLException {
        // or([false, false]) -> false
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[false, false]->or()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(false, ((ScalarResult) result).value());
    }

    // ==================== Cast on arrays ====================

    @Test
    void testGreatestScalar() throws SQLException {
        // -1->greatest() should return -1 (scalar passthrough, not LIST_MAX)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|-1->greatest()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertScalarInteger(result, -1);
    }

    @Test
    void testLeastScalar() throws SQLException {
        // 42->least() should return 42 (scalar passthrough)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|42->least()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertScalarInteger(result, 42);
    }

    @Test
    void testCastEmptyArrayToInteger() throws SQLException {
        // []->cast(@Integer)->greatest() should generate CAST([] AS INTEGER[])
        // DuckDB returns empty list for list_max of empty typed array
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[]->cast(@Integer)->greatest()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
    }

    // ==================== TimeBucket ====================

    @Test
    void testTimeBucketStrictDate1Day() throws SQLException {
        // timeBucket(1, DAYS) on StrictDate should return same date (as DATE, not TIMESTAMP)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|%2024-01-31->timeBucket(1, meta::pure::functions::date::DurationUnit.DAYS)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(java.time.LocalDate.of(2024, 1, 31), ((ScalarResult) result).value());
    }

    @Test
    void testTimeBucketStrictDate2Days() throws SQLException {
        // timeBucket(2, DAYS) on 2024-01-31 should return 2024-01-30
        // (2-day buckets from Unix epoch 1970-01-01)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|%2024-01-31->timeBucket(2, meta::pure::functions::date::DurationUnit.DAYS)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(java.time.LocalDate.of(2024, 1, 30), ((ScalarResult) result).value());
    }

    @Test
    void testTimeBucketDateTime() throws SQLException {
        // timeBucket(1, DAYS) on DateTime should return midnight of that day as TIMESTAMP_NS
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|%2024-01-31T00:32:34+0000->timeBucket(1, meta::pure::functions::date::DurationUnit.DAYS)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(java.sql.Timestamp.valueOf("2024-01-31 00:00:00"), ((ScalarResult) result).value());
    }

    @Test
    void testTimeBucketStrictDate2Weeks() throws SQLException {
        // timeBucket(2, WEEKS) on 2024-01-31 (Wednesday) should return 2024-01-29 (Monday)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|%2024-01-31->timeBucket(2, meta::pure::functions::date::DurationUnit.WEEKS)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(java.time.LocalDate.of(2024, 1, 29), ((ScalarResult) result).value());
    }

    @Test
    void testTimeBucketDateTime2Weeks() throws SQLException {
        // timeBucket(2, WEEKS) on DateTime should return Monday of that week
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|%2024-01-31T00:32:34+0000->timeBucket(2, meta::pure::functions::date::DurationUnit.WEEKS)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(java.sql.Timestamp.valueOf("2024-01-29 00:00:00"), ((ScalarResult) result).value());
    }

    // ==================== GenerateGuid ====================

    @Test
    void testGenerateGuidScalar() throws SQLException {
        // generateGuid() should return a UUID string
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|meta::pure::functions::string::generation::generateGuid()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        Object value = ((ScalarResult) result).value();
        assertNotNull(value);
        // UUID format: 8-4-4-4-12 hex chars
        assertTrue(value.toString().matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
                "Expected UUID format, got: " + value);
    }

    @Test
    void testGenerateGuidInRelationExtend() throws SQLException {
        // generateGuid() used in extend should produce a column of UUIDs
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|#TDS\nval, str\n1, a\n3, ewe\n#->extend(~uid:x|meta::pure::functions::string::generation::generateGuid())",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        BufferedResult buffered = result.toBuffered();
        assertEquals(2, buffered.rowCount());
        // Check uid column exists and has UUID values
        for (int i = 0; i < buffered.rowCount(); i++) {
            Object uid = buffered.getValue(i, "uid");
            assertNotNull(uid);
            assertTrue(uid.toString().matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
                    "Expected UUID format, got: " + uid);
        }
    }

    // ==================== Math Functions ====================

    @Test
    void testPi() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|meta::pure::functions::math::pi()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        double pi = ((Number) ((ScalarResult) result).value()).doubleValue();
        assertEquals(Math.PI, pi, 1e-10);
    }

    @Test
    void testLog() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|2.718281828->log()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        double val = ((Number) ((ScalarResult) result).value()).doubleValue();
        assertEquals(1.0, val, 1e-5);
    }

    @Test
    void testLog10() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|100.0->log10()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        double val = ((Number) ((ScalarResult) result).value()).doubleValue();
        assertEquals(2.0, val, 1e-10);
    }

    @Test
    void testExp() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|1.0->exp()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        double val = ((Number) ((ScalarResult) result).value()).doubleValue();
        assertEquals(Math.E, val, 1e-10);
    }

    @Test
    void testPow() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|2.0->pow(10.0)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        double val = ((Number) ((ScalarResult) result).value()).doubleValue();
        assertEquals(1024.0, val, 1e-10);
    }

    @Test
    void testSqrt() throws SQLException {
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|9.0->sqrt()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        double val = ((Number) ((ScalarResult) result).value()).doubleValue();
        assertEquals(3.0, val, 1e-10);
    }

    // --- toDecimal: should cast to DECIMAL, tracked through IR ---
    @Test
    void testIntToDecimal() throws SQLException {
        // PCT: |8->meta::pure::functions::math::toDecimal()
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|8->toDecimal()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        ScalarResult sr = (ScalarResult) result;
        assertEquals("DECIMAL_CAST", sr.sqlType(), "toDecimal should propagate DECIMAL_CAST");
    }

    @Test
    void testDecimalLiteralTracking() throws SQLException {
        // Pure Decimal literal 3.0D should be tracked as DECIMAL through IR
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|1.0D + 2.0D + 3.0D",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        ScalarResult sr = (ScalarResult) result;
        assertEquals("DECIMAL", sr.sqlType(), "Decimal literal arithmetic should track as DECIMAL");
        assertEquals(6.0, ((Number) sr.value()).doubleValue(), 1e-10);
    }

    @Test
    void testDecimalAbs() throws SQLException {
        // PCT: |meta::pure::functions::math::abs(-3.0D)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|meta::pure::functions::math::abs(-3.0D)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        ScalarResult sr = (ScalarResult) result;
        assertEquals("DECIMAL", sr.sqlType(), "abs of Decimal should be DECIMAL");
        assertEquals(3.0, ((Number) sr.value()).doubleValue(), 1e-10);
    }

    // --- mod: Pure mod always returns non-negative ---
    @Test
    void testModWithNegativeNumbers() throws SQLException {
        // PCT: |meta::pure::functions::math::mod(-12, 5) => expected 3
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|meta::pure::functions::math::mod(-12, 5)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(3, ((Number) ((ScalarResult) result).value()).intValue());
    }

    // --- round with scale: round(3.14159D, 2) => 3.14 ---
    @Test
    void testRoundWithScale() throws SQLException {
        // PCT: |3.14159D->meta::pure::functions::math::round(2)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|3.14159->round(2)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        double val = ((Number) ((ScalarResult) result).value()).doubleValue();
        assertEquals(3.14, val, 1e-10);
    }

    // --- round half-even (banker's rounding): 16.5->round() => 16 ---
    @Test
    void testRoundHalfEvenDown() throws SQLException {
        // PCT: |16.5->meta::pure::functions::math::round() => expected 16
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|16.5->round()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(16, ((Number) ((ScalarResult) result).value()).intValue());
    }

    @Test
    void testRoundHalfEvenUp() throws SQLException {
        // PCT: |meta::pure::functions::math::round(-16.5) => expected -16
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|meta::pure::functions::math::round(-16.5)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(-16, ((Number) ((ScalarResult) result).value()).intValue());
    }

    // --- PCT Decimal test: parseDecimal strips d/D suffix ---
    // Note: DuckDB CAST AS DECIMAL uses DECIMAL(18,3) which limits precision.
    // Legend-engine has the same limitation (needsInvestigation). Our fix strips
    // the d/D suffix so at least the CAST doesn't error.
    @Test
    void testParseDecimalWithSuffix() throws SQLException {
        // PCT: |'3.14159d'->parseDecimal() - d suffix stripped, value parsed
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'3.14159d'->parseDecimal()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        ScalarResult sr = (ScalarResult) result;
        assertEquals("DECIMAL_CAST", sr.sqlType());
        // DuckDB DECIMAL(18,3) truncates to 3.142 - known limitation
        assertTrue(sr.value() instanceof java.math.BigDecimal);
    }

    @Test
    void testParseDecimalSimple() throws SQLException {
        // PCT: |'3.14'->parseDecimal() - no suffix
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'3.14'->parseDecimal()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        ScalarResult sr = (ScalarResult) result;
        assertEquals("DECIMAL_CAST", sr.sqlType());
        assertEquals(3.14, ((Number) sr.value()).doubleValue(), 0.01);
    }

    // --- PCT: min/max method-call for scalar comparison ---
    @Test
    void testMinFloats() throws SQLException {
        // PCT: |2.8->meta::pure::functions::math::min(1.23)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|2.8->min(1.23)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(1.23, ((Number) ((ScalarResult) result).value()).doubleValue(), 1e-10);
    }

    @Test
    void testMaxFloats() throws SQLException {
        // PCT: |2.8->meta::pure::functions::math::max(1.23)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|2.8->max(1.23)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(2.8, ((Number) ((ScalarResult) result).value()).doubleValue(), 1e-10);
    }

    @Test
    void testMinIntegers() throws SQLException {
        // PCT: |2->meta::pure::functions::math::min(1)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|2->min(1)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(1, ((Number) ((ScalarResult) result).value()).intValue());
    }

    @Test
    void testMaxIntegers() throws SQLException {
        // PCT: |2->meta::pure::functions::math::max(1)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|2->max(1)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(2, ((Number) ((ScalarResult) result).value()).intValue());
    }

    @Test
    void testMinNumbers() throws SQLException {
        // PCT: |2->meta::pure::functions::math::min(1.23D)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|2->min(1.23D)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(1.23, ((Number) ((ScalarResult) result).value()).doubleValue(), 1e-10);
    }

    @Test
    void testDecimalTimes() throws SQLException {
        // PCT: |19.905D * 17774D => 353791.470D
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|19.905D * 17774D",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        ScalarResult sr = (ScalarResult) result;
        assertEquals("DECIMAL", sr.sqlType());
        assertEquals(353791.47, ((Number) sr.value()).doubleValue(), 0.01);
    }

    // --- PCT: min/max scalar and aggregate ---
    @Test
    void testMinScalar() throws SQLException {
        // PCT sub-expression: |2.8->min(1.23) => 1.23
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|2.8->min(1.23)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(1.23, ((Number) ((ScalarResult) result).value()).doubleValue(), 1e-10);
    }

    @Test
    void testMaxScalar() throws SQLException {
        // PCT sub-expression: |1.23->max(2.8) => 2.8
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|1.23->max(2.8)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(2.8, ((Number) ((ScalarResult) result).value()).doubleValue(), 1e-10);
    }

    @Test
    void testMinNoArgs() throws SQLException {
        // PCT sub-expression: |[3, 1, 2]->min() => 1
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[3, 1, 2]->min()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(1, ((Number) ((ScalarResult) result).value()).intValue());
    }

    @Test
    void testMaxNoArgs() throws SQLException {
        // PCT sub-expression: |[3, 1, 2]->max() => 3
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[3, 1, 2]->max()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(3, ((Number) ((ScalarResult) result).value()).intValue());
    }

    // --- PCT: variance population vs sample ---
    @Test
    void testVariancePopulation() throws SQLException {
        // PCT: |[1, 2]->variance(false) => 0.25 (population)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[1, 2]->variance(false)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(0.25, ((Number) ((ScalarResult) result).value()).doubleValue(), 1e-10);
    }

    // --- PCT: exp, log, log10, pow ---
    @Test
    void testPowMethodCall() throws SQLException {
        // PCT sub-expression: |3.33->pow(4.33)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|3.33->pow(4.33)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertTrue(((Number) ((ScalarResult) result).value()).doubleValue() > 100);
    }

    @Test
    void testExpMethodCall() throws SQLException {
        // PCT sub-expression: |1->exp()
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|1->exp()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(Math.E, ((Number) ((ScalarResult) result).value()).doubleValue(), 0.001);
    }

    @Test
    void testLogMethodCall() throws SQLException {
        // PCT sub-expression: |2.718281828459045->log()
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|2.718281828459045->log()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(1.0, ((Number) ((ScalarResult) result).value()).doubleValue(), 0.001);
    }

    @Test
    void testLog10MethodCall() throws SQLException {
        // PCT sub-expression: |100->log10()
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|100->log10()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(2.0, ((Number) ((ScalarResult) result).value()).doubleValue(), 0.001);
    }

    // --- PCT: decodeBase64 returns string, not BLOB ---
    @Test
    void testDecodeBase64() throws SQLException {
        // PCT: |'SGVsbG8sIFdvcmxkIQ=='->meta::pure::functions::string::decodeBase64()
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'SGVsbG8sIFdvcmxkIQ=='->decodeBase64()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("Hello, World!", ((ScalarResult) result).value());
    }

    @Test
    void testDecodeBase64RoundTrip() throws SQLException {
        // PCT: |'Any Random String'->encodeBase64()->decodeBase64()
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'Any Random String'->encodeBase64()->decodeBase64()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("Any Random String", ((ScalarResult) result).value());
    }

    // --- PCT: splitPart(string, delimiter, index) - 0-based index ---
    @Test
    void testSplitPart() throws SQLException {
        // PCT: |'Hello World'->splitPart(' ', 0) => 'Hello'
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'Hello World'->splitPart(' ', 0)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("Hello", ((ScalarResult) result).value());
    }

    @Test
    void testSplitPartSecondToken() throws SQLException {
        // PCT: |'Hello World'->splitPart(' ', 1) => 'World'
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'Hello World'->splitPart(' ', 1)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("World", ((ScalarResult) result).value());
    }

    @Test
    void testSplitPartNoSplit() throws SQLException {
        // PCT: |'Hello World'->splitPart(';', 0) => 'Hello World'
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'Hello World'->splitPart(';', 0)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("Hello World", ((ScalarResult) result).value());
    }

    @Test
    void testSplitPartTypicalToken() throws SQLException {
        // PCT: |'Sunglasses, Keys, Phone, SL-card'->splitPart(', ', 2) => 'Phone'
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'Sunglasses, Keys, Phone, SL-card'->splitPart(', ', 2)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("Phone", ((ScalarResult) result).value());
    }

    // --- PCT: indexOf on lists - 0-based ---
    @Test
    void testIndexOfList() throws SQLException {
        // PCT: |['a', 'b', 'c', 'd']->indexOf('c') => 2
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|['a', 'b', 'c', 'd']->indexOf('c')",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(2, ((Number) ((ScalarResult) result).value()).intValue());
    }

    @Test
    void testIndexOfOneElement() throws SQLException {
        // PCT: |['a']->indexOf('a') => 0
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|['a']->indexOf('a')",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(0, ((Number) ((ScalarResult) result).value()).intValue());
    }

    // --- PCT: DateTime toString format ---
    @Test
    void testDateTimeToString() throws SQLException {
        // PCT: |%2014-01-01T00:00:00.000+0000->toString() => '2014-01-01T00:00:00.000+0000'
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|%2014-01-01T00:00:00.000+0000->toString()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("2014-01-01T00:00:00.000+0000", ((ScalarResult) result).value());
    }

    @Test
    void testSplitPartEmptyString() throws SQLException {
        // PCT: |[]->splitPart(' ', 0) => ''
        // Empty list [] as String source means empty string
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[]->splitPart(' ', 0)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("", ((ScalarResult) result).value());
    }

    @Test
    void testSplitPartEmptyDelimiter() throws SQLException {
        // PCT sub-expression: |'Hello World'->splitPart('', 0) => 'Hello World'
        // Splitting by empty string should return the whole string at index 0
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'Hello World'->splitPart('', 0)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("Hello World", ((ScalarResult) result).value());
    }

    // ==================== XOR ====================

    @Test
    void testXorMethodCall() throws SQLException {
        // true->xor(true) should be false
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|true->xor(true)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(false, ((ScalarResult) result).value());
    }

    @Test
    void testXorFunctionCall() throws SQLException {
        // xor(1 == 1, not(2 == 3)) -> xor(true, true) -> false
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|meta::pure::functions::boolean::xor(1 == 1, meta::pure::functions::boolean::not(2 == 3))",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(false, ((ScalarResult) result).value());
    }

    // ==================== RC-12: Date Function Gaps ====================

    // --- 12c: date() with 0 or 1 args ---
    @Test
    void testDateYearOnly() throws SQLException {
        // PCT: |2015->date() => year-only date
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|2015->date()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        // year-only date(2015) creates 2015-01-01
        assertNotNull(((ScalarResult) result).value());
    }

    @Test
    void testDateYearMonth() throws SQLException {
        // PCT: |2015->date(4) => year-month date
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|2015->date(4)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertNotNull(((ScalarResult) result).value());
    }

    // --- 12b: parseDate without format ---
    @Test
    void testParseDateNoFormat() throws SQLException {
        // PCT: |'2014-02-27T10:01:35.231+0000'->parseDate()
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'2014-02-27T10:01:35.231+0000'->parseDate()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertNotNull(((ScalarResult) result).value());
    }

    @Test
    void testParseDateNoFormatSimple() throws SQLException {
        // PCT: |'2014-02-27'->parseDate()
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'2014-02-27'->parseDate()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertNotNull(((ScalarResult) result).value());
    }

    // --- 12a: datepart ---
    @Test
    void testDatePartOnDate() throws SQLException {
        // PCT: |%1973-11-05->datePart() => date truncated to day
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|%1973-11-05->datePart()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertNotNull(((ScalarResult) result).value());
    }

    @Test
    void testDatePartOnTimestamp() throws SQLException {
        // PCT: |%1973-11-05T13:01:25+0000->datePart() => date part only
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|%1973-11-05T13:01:25+0000->datePart()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertNotNull(((ScalarResult) result).value());
    }

    // --- 12d: adjust as function call ---
    @Test
    void testAdjustFunctionCall() throws SQLException {
        // PCT: |adjust(%2015-02-28, 1, meta::pure::functions::date::DurationUnit.YEARS)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|%2015-02-28->adjust(1, meta::pure::functions::date::DurationUnit.YEARS)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertNotNull(((ScalarResult) result).value());
    }

    // --- 12e: hasMonth as function call ---
    @Test
    void testHasMonthOnFullDate() throws SQLException {
        // PCT: |hasMonth(%2015-04-01) => true
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|%2015-04-01->hasMonth()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(true, ((ScalarResult) result).value());
    }

    // ==================== RC-13: DuckDB Function Mapping Gaps ====================

    // --- 13a: lpad/rpad ---
    @Test
    void testLpadDefaultFill() throws SQLException {
        // PCT: |'abcd'->lpad(10) => '      abcd' (6 spaces + abcd)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'abcd'->lpad(10)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("      abcd", ((ScalarResult) result).value());
    }

    @Test
    void testLpadShorterThanString() throws SQLException {
        // PCT: |'abcdefghij'->lpad(5) => 'abcde' (truncate)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'abcdefghij'->lpad(5)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("abcde", ((ScalarResult) result).value());
    }

    @Test
    void testRpadDefaultFill() throws SQLException {
        // PCT: |'abcd'->rpad(10) => 'abcd      ' (abcd + 6 spaces)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'abcd'->rpad(10)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("abcd      ", ((ScalarResult) result).value());
    }

    @Test
    void testRpadShorterThanString() throws SQLException {
        // PCT: |'abcdefghij'->rpad(5) => 'abcde' (truncate)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'abcdefghij'->rpad(5)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("abcde", ((ScalarResult) result).value());
    }

    // --- 13b: contains on strings ---
    @Test
    void testContainsOnString() throws SQLException {
        // PCT: |'the quick brown fox jumps over the lazy dog'->contains('fox') => true
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'the quick brown fox jumps over the lazy dog'->contains('fox')",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(true, ((ScalarResult) result).value());
    }

    @Test
    void testContainsOnStringNotFound() throws SQLException {
        // PCT: |'the quick brown fox'->contains('cat') => false
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'the quick brown fox'->contains('cat')",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(false, ((ScalarResult) result).value());
    }

    // --- 13c: percentileCont on lists ---
    @Test
    void testPercentileContOnList() throws SQLException {
        // PCT: |[1, 2, 3, 4, 5]->percentileCont(0.5) => 3.0 (median)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[1, 2, 3, 4, 5]->percentileCont(0.5)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(3.0, ((Number) ((ScalarResult) result).value()).doubleValue(), 0.01);
    }

    @Test
    void testPercentileContOnListQuartile() throws SQLException {
        // PCT: |[1, 2, 3, 4, 5]->percentileCont(0.25) => 2.0
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[1, 2, 3, 4, 5]->percentileCont(0.25)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(2.0, ((Number) ((ScalarResult) result).value()).doubleValue(), 0.01);
    }

    // --- 13d: base64 decode without padding ---
    @Test
    void testDecodeBase64NoPadding() throws SQLException {
        // PCT: |'SGVsbG8sIFdvcmxkIQ'->decodeBase64() => 'Hello, World!'
        // The base64 string is missing the trailing '=' padding
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'SGVsbG8sIFdvcmxkIQ'->decodeBase64()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("Hello, World!", ((ScalarResult) result).value());
    }

    // ==================== Collection function tests ====================

    @Test
    void testAddToList() throws SQLException {
        // PCT: |['a', 'b']->add('c') → list_append(['a', 'b'], 'c')
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|['a', 'b']->meta::pure::functions::collection::add('c')",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("[a, b, c]", String.valueOf(((ScalarResult) result).value()));
    }

    @Test
    void testAddToListWithOffset() throws SQLException {
        // PCT: |['a', 'b']->add(1, 'c') → insert 'c' at index 1 → ['a', 'c', 'b']
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|['a', 'b']->meta::pure::functions::collection::add(1, 'c')",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("[a, c, b]", String.valueOf(((ScalarResult) result).value()));
    }

    // ==================== Mixed-type list tests (JSON[]) ====================

    @Test
    void testConcatenateMixedType() throws SQLException {
        // PCT: |[1, 2, 3]->concatenate(['a', 'b']) -> [1, 2, 3, 'a', 'b']
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[1, 2, 3]->meta::pure::functions::collection::concatenate(['a', 'b'])",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        // Engine unwraps JSON[] to native Java List: [Long(1), Long(2), Long(3), String("a"), String("b")]
        Object val = ((ScalarResult) result).value();
        assertTrue(val instanceof java.util.List, "Expected List result, got: " + val.getClass());
        java.util.List<?> elements = (java.util.List<?>) val;
        assertEquals(5, elements.size());
        assertEquals(1L, elements.get(0));
        assertEquals(2L, elements.get(1));
        assertEquals(3L, elements.get(2));
        assertEquals("a", elements.get(3));
        assertEquals("b", elements.get(4));
    }

    @Test
    void testConcatenateHomogeneous() throws SQLException {
        // PCT: |[1, 2, 3]->concatenate([4, 5]) -> [1, 2, 3, 4, 5]
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[1, 2, 3]->meta::pure::functions::collection::concatenate([4, 5])",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        Object val = ((ScalarResult) result).value();
        // Homogeneous arrays stay as sql.Array (not unwrapped to List)
        assertTrue(val instanceof java.sql.Array, "Expected sql.Array result, got: " + val.getClass());
        Object[] elements = (Object[]) ((java.sql.Array) val).getArray();
        assertEquals(5, elements.length);
        assertEquals(1, elements[0]);
        assertEquals(5, elements[4]);
    }

    @Test
    void testContainsPrimitive() throws SQLException {
        // PCT: |[1, 2, 5, 2, 'a', true, %2014-02-01, 'c']->contains(1) -> true
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[1, 2, 5, 2, 'a', true, %2014-02-01, 'c']->meta::pure::functions::collection::contains(1)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(true, ((ScalarResult) result).value());
    }

    @Test
    void testContainsPrimitiveString() throws SQLException {
        // PCT: |[1, 2, 5, 2, 'a', true, %2014-02-01, 'c']->contains('a') -> true
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[1, 2, 5, 2, 'a', true, %2014-02-01, 'c']->meta::pure::functions::collection::contains('a')",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(true, ((ScalarResult) result).value());
    }

    @Test
    void testInPrimitive() throws SQLException {
        // PCT: |1->in([1, 2, 5, 2, 'a', true, %2014-02-01, 'c']) -> true
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|1->meta::pure::functions::collection::in([1, 2, 5, 2, 'a', true, %2014-02-01, 'c'])",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(true, ((ScalarResult) result).value());
    }

    @Test
    void testInPrimitiveNotFound() throws SQLException {
        // PCT: |'z'->in([1, 2, 5, 2, 'a', true, %2014-02-01, 'c']) -> false
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|'z'->meta::pure::functions::collection::in([1, 2, 5, 2, 'a', true, %2014-02-01, 'c'])",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(false, ((ScalarResult) result).value());
    }

    // ==================== Mixed numeric type tests (should NOT use JSON[]) ====================

    @Test
    void testSumMixedNumbers() throws SQLException {
        // PCT: |[15, 13, 2.0, 1, 1.0]->sum() -> 32.0
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[15, 13, 2.0, 1, 1.0]->meta::pure::functions::math::sum()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        Object val = ((ScalarResult) result).value();
        assertEquals(32.0, ((Number) val).doubleValue(), 0.001);
    }

    @Test
    void testAverageMixedNumbers() throws SQLException {
        // PCT: |[5D, 1.0, 2, 8, 3]->average() -> 3.8
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[5.0, 1.0, 2, 8, 3]->meta::pure::functions::math::average()",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        Object val = ((ScalarResult) result).value();
        assertEquals(3.8, ((Number) val).doubleValue(), 0.001);
    }

    // ==================== Lambda compilation tests ====================

    @Test
    void testSimpleIf() throws SQLException {
        // PCT: |if(1 == 1, |'truesentence', |'falsesentence')
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|meta::pure::functions::lang::if(1 == 1, |'truesentence', |'falsesentence')",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("truesentence", ((ScalarResult) result).value());
    }

    @Test
    void testIfMethodStyle() throws SQLException {
        // PCT: |true->if(|'truesentence', |'falsesentence')
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|true->meta::pure::functions::lang::if(|'truesentence', |'falsesentence')",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("truesentence", ((ScalarResult) result).value());
    }

    @Test
    void testEvalLambdaExp() throws SQLException {
        // PCT: |eval(a: Number[1]|$a->exp(), 1.0)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|meta::pure::functions::lang::eval(a: Number[1]|$a->meta::pure::functions::math::exp(), 1.0)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(Math.E, ((Number) ((ScalarResult) result).value()).doubleValue(), 0.0001);
    }

    @Test
    void testForAllTrue() throws SQLException {
        // PCT: |[1, 2, 3]->forAll(e|$e > 0)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[1, 2, 3]->meta::pure::functions::collection::forAll(e|$e > 0)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(true, ((ScalarResult) result).value());
    }

    @Test
    void testForAllFalse() throws SQLException {
        // PCT: |[1, 2, 3]->forAll(e|$e > 1)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[1, 2, 3]->meta::pure::functions::collection::forAll(e|$e > 1)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals(false, ((ScalarResult) result).value());
    }

    @Test
    void testFindLiteral() throws SQLException {
        // PCT: |['Smith', 'Branche', 'Doe']->find(s: String[1]|$s->length() < 4)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|['Smith', 'Branche', 'Doe']->meta::pure::functions::collection::find(s: String[1]|$s->meta::pure::functions::string::length() < 4)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
        assertEquals("Doe", ((ScalarResult) result).value());
    }

    // ==================== Parser fix tests ====================

    @Test
    void testDropNegative() throws SQLException {
        // PCT: |[1, 2, 3]->drop(-1) — negative drop should return full list
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[1, 2, 3]->meta::pure::functions::collection::drop(-1)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
    }

    @Test
    void testTakeNegative() throws SQLException {
        // PCT: |[1, 2, 3]->take(-1) — negative take should return empty list
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[1, 2, 3]->meta::pure::functions::collection::take(-1)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
    }

    @Test
    void testSliceNegativeStart() throws SQLException {
        // PCT: |[2, 3, 4, 5]->slice(-1, 10)
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                "|[2, 3, 4, 5]->meta::pure::functions::collection::slice(-1, 10)",
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof ScalarResult);
    }

    @Test
    void testRelationAggregateWithCast() throws SQLException {
        // PCT: Relation aggregate with ->cast(@Number) in extend window expression
        // Tests that extractPropertyName handles CastExpression wrapping PropertyAccessExpression
        String pureExpr = "|#TDS\n" +
                "              id, grp, name\n" +
                "              1.0, 2, A\n" +
                "              2.0, 1, B\n" +
                "              3.0, 3, C\n" +
                "#->groupBy(~grp, ~id : x | $x.id : y | $y->average())";
        Result result = queryService.execute(
                getCompletePureModelWithRuntime(),
                pureExpr,
                "test::TestRuntime", connection, QueryService.ResultMode.BUFFERED);
        assertTrue(result instanceof BufferedResult);
    }

    // ==================== Helper ====================

    private void assertScalarInteger(Result result, long expected) {
        assertTrue(result instanceof ScalarResult, "Expected ScalarResult but got " + result.getClass().getSimpleName());
        Object value = ((ScalarResult) result).value();
        assertNotNull(value, "Scalar value should not be null");
        assertInstanceOf(Number.class, value, "Scalar value should be a Number");
        // Must be an integer type (Integer or Long), not Double/Float
        assertTrue(value instanceof Integer || value instanceof Long,
                "Expected Integer/Long but got " + value.getClass().getSimpleName() + " = " + value);
        assertEquals(expected, ((Number) value).longValue());
    }
}
