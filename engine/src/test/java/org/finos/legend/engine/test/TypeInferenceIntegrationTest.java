package org.finos.legend.engine.test;

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
