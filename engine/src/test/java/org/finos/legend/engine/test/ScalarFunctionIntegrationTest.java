package org.finos.legend.engine.test;

import org.finos.legend.engine.execution.BufferedResult;
import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLDialect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for scalar functions that map Pure functions to DuckDB SQL.
 * Each test validates that a specific Pure function correctly compiles and
 * executes.
 * 
 * This class focuses on function mappings that were identified as missing from
 * PCT tests.
 */
public class ScalarFunctionIntegrationTest extends AbstractDatabaseTest {

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

    // ==================== Math Functions ====================

    @Test
    void testTypeOf() throws SQLException {
        BufferedResult result = executeRelation("|1->type()");
        assertEquals("INTEGER", result.rows().get(0).get(0));
    }

    @Test
    void testRem() throws SQLException {
        BufferedResult result = executeRelation("|10->rem(3)");
        assertEquals(1L, ((Number) result.rows().get(0).get(0)).longValue());
    }

    @Test
    void testToRadians() throws SQLException {
        BufferedResult result = executeRelation("|180->toRadians()");
        double value = ((Number) result.rows().get(0).get(0)).doubleValue();
        assertEquals(Math.PI, value, 0.0001);
    }

    @Test
    void testToDegrees() throws SQLException {
        BufferedResult result = executeRelation("|3.14159265359->toDegrees()");
        double value = ((Number) result.rows().get(0).get(0)).doubleValue();
        assertEquals(180.0, value, 0.1);
    }

    @Test
    void testSign() throws SQLException {
        BufferedResult result = executeRelation("|-5->sign()");
        assertEquals(-1L, ((Number) result.rows().get(0).get(0)).longValue());
    }

    // ==================== String Functions ====================

    @Test
    void testReverseString() throws SQLException {
        BufferedResult result = executeRelation("|'hello'->reverseString()");
        assertEquals("olleh", result.rows().get(0).get(0));
    }

    @Test
    void testContains() throws SQLException {
        // List contains - not string contains (string contains has different handling)
        BufferedResult result = executeRelation("|['hello', 'world']->contains('world')");
        assertEquals(true, result.rows().get(0).get(0));
    }

    @Test
    void testStartsWith() throws SQLException {
        BufferedResult result = executeRelation("|'hello'->startsWith('hel')");
        assertEquals(true, result.rows().get(0).get(0));
    }

    @Test
    void testEndsWith() throws SQLException {
        BufferedResult result = executeRelation("|'hello'->endsWith('lo')");
        assertEquals(true, result.rows().get(0).get(0));
    }

    @Test
    void testLevenshteinDistance() throws SQLException {
        BufferedResult result = executeRelation("|'hello'->levenshteinDistance('hallo')");
        assertEquals(1L, ((Number) result.rows().get(0).get(0)).longValue());
    }

    // ==================== Bit Operations ====================

    @Test
    void testBitAnd() throws SQLException {
        BufferedResult result = executeRelation("|12->bitAnd(10)");
        assertEquals(8L, ((Number) result.rows().get(0).get(0)).longValue());
    }

    @Test
    void testBitOr() throws SQLException {
        BufferedResult result = executeRelation("|12->bitOr(10)");
        assertEquals(14L, ((Number) result.rows().get(0).get(0)).longValue());
    }

    @Test
    void testBitXor() throws SQLException {
        BufferedResult result = executeRelation("|12->bitXor(10)");
        assertEquals(6L, ((Number) result.rows().get(0).get(0)).longValue());
    }

    @Test
    void testBitShiftLeft() throws SQLException {
        BufferedResult result = executeRelation("|1->bitShiftLeft(4)");
        assertEquals(16L, ((Number) result.rows().get(0).get(0)).longValue());
    }

    @Test
    void testBitShiftRight() throws SQLException {
        BufferedResult result = executeRelation("|16->bitShiftRight(2)");
        assertEquals(4L, ((Number) result.rows().get(0).get(0)).longValue());
    }

    // ==================== Date Functions ====================

    @Test
    void testMonthNumber() throws SQLException {
        BufferedResult result = executeRelation("|%2024-06-15->monthNumber()");
        assertEquals(6L, ((Number) result.rows().get(0).get(0)).longValue());
    }

    @Test
    void testDayNumber() throws SQLException {
        BufferedResult result = executeRelation("|%2024-06-15->dayOfMonth()");
        assertEquals(15L, ((Number) result.rows().get(0).get(0)).longValue());
    }

    @Test
    void testYear() throws SQLException {
        BufferedResult result = executeRelation("|%2024-06-15->year()");
        assertEquals(2024L, ((Number) result.rows().get(0).get(0)).longValue());
    }

    // ==================== Hash Functions ====================

    @Test
    void testHash() throws SQLException {
        BufferedResult result = executeRelation("|'test'->hash()");
        // Just verify it returns something - hash values are implementation specific
        assertTrue(result.rows().get(0).get(0) != null);
    }
}
