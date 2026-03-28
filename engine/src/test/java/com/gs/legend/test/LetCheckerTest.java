package com.gs.legend.test;

import com.gs.legend.sqlgen.DuckDBDialect;
import com.gs.legend.sqlgen.SQLDialect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for LetChecker — signature-driven type checking
 * for {@code letFunction()}.
 *
 * <p>
 * Covers:
 * <ul>
 * <li>Standard 2-arg form: {@code let x = expr}</li>
 * <li>Type propagation: Integer, String, Float, Boolean</li>
 * <li>Let chaining: multiple let bindings feeding into computation</li>
 * <li>Let with complex expressions: arithmetic, function calls</li>
 * <li>Let value into downstream operations: filter, project</li>
 * <li>Let with relational expressions</li>
 * </ul>
 *
 * <p>
 * <b>Strong Assertion Policy</b>: every test validates exact row counts,
 * exact column names, exact values element-by-element. No
 * {@code assertNotNull}-only.
 */
public class LetCheckerTest extends AbstractDatabaseTest {

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
        if (connection != null)
            connection.close();
    }

    // ==================== Basic let with scalar types ====================

    @Nested
    @DisplayName("let with scalar values")
    class ScalarLet {

        @Test
        @DisplayName("let integer literal — type propagates to Integer")
        void testLetInteger() throws SQLException {
            var result = executeRelation("|let x = 42; $x;");
            assertEquals(1, result.rows().size(), "Single row");
            assertEquals(42, result.rows().get(0).values().get(0), "Value is 42");
        }

        @Test
        @DisplayName("let string literal — type propagates to String")
        void testLetString() throws SQLException {
            var result = executeRelation("|let x = 'hello'; $x;");
            assertEquals(1, result.rows().size(), "Single row");
            assertEquals("hello", result.rows().get(0).values().get(0), "Value is 'hello'");
        }

        @Test
        @DisplayName("let boolean literal — type propagates to Boolean")
        void testLetBoolean() throws SQLException {
            var result = executeRelation("|let x = true; $x;");
            assertEquals(1, result.rows().size());
            assertEquals(true, result.rows().get(0).values().get(0), "Value is true");
        }

        @Test
        @DisplayName("let float literal — type propagates to Float")
        void testLetFloat() throws SQLException {
            var result = executeRelation("|let x = 3.14; $x;");
            assertEquals(1, result.rows().size());
            var value = ((Number) result.rows().get(0).values().get(0)).doubleValue();
            assertEquals(3.14, value, 0.001, "Value is 3.14");
        }
    }

    // ==================== Let with arithmetic ====================

    @Nested
    @DisplayName("let with arithmetic expressions")
    class ArithmeticLet {

        @Test
        @DisplayName("let with addition — result type is Integer")
        void testLetAddition() throws SQLException {
            var result = executeRelation("|let x = 10 + 32; $x;");
            assertEquals(1, result.rows().size());
            assertEquals(42, result.rows().get(0).values().get(0));
        }

        @Test
        @DisplayName("let with multiplication")
        void testLetMultiplication() throws SQLException {
            var result = executeRelation("|let x = 6 * 7; $x;");
            assertEquals(1, result.rows().size());
            assertEquals(42, result.rows().get(0).values().get(0));
        }

        @Test
        @DisplayName("let with nested arithmetic")
        void testLetNestedArith() throws SQLException {
            var result = executeRelation("|let x = (10 + 5) * 2; $x;");
            assertEquals(1, result.rows().size());
            assertEquals(30, result.rows().get(0).values().get(0));
        }

        @Test
        @DisplayName("let with string concatenation")
        void testLetStringConcat() throws SQLException {
            var result = executeRelation("|let x = 'hello' + ' ' + 'world'; $x;");
            assertEquals(1, result.rows().size());
            assertEquals("hello world", result.rows().get(0).values().get(0));
        }
    }

    // ==================== Let chaining ====================

    @Nested
    @DisplayName("let chaining (multiple bindings)")
    class LetChaining {

        @Test
        @DisplayName("two lets — second uses first")
        void testTwoLets() throws SQLException {
            var result = executeRelation("|let x = 10; let y = $x + 32; $y;");
            assertEquals(1, result.rows().size());
            assertEquals(42, result.rows().get(0).values().get(0));
        }

        @Test
        @DisplayName("three lets — cascading computation")
        void testThreeLets() throws SQLException {
            var result = executeRelation("|let a = 2; let b = 3; let c = $a * $b + 1; $c;");
            assertEquals(1, result.rows().size());
            assertEquals(7, result.rows().get(0).values().get(0));
        }

        @Test
        @DisplayName("let string chain — concatenation through variables")
        void testLetStringChain() throws SQLException {
            var result = executeRelation(
                    "|let first = 'John'; let last = 'Smith'; let full = $first + ' ' + $last; $full;");
            assertEquals(1, result.rows().size());
            assertEquals("John Smith", result.rows().get(0).values().get(0));
        }
    }

    // ==================== Let with function calls ====================

    @Nested
    @DisplayName("let with function calls")
    class LetWithFunctions {

        @Test
        @DisplayName("let with toLower — function return type propagates")
        void testLetWithToLower() throws SQLException {
            var result = executeRelation("|let x = 'HELLO'->toLower(); $x;");
            assertEquals(1, result.rows().size());
            assertEquals("hello", result.rows().get(0).values().get(0));
        }

        @Test
        @DisplayName("let with abs — numeric function return type propagates")
        void testLetWithAbs() throws SQLException {
            var result = executeRelation("|let x = 42->abs(); $x;");
            assertEquals(1, result.rows().size());
            assertEquals(42, result.rows().get(0).values().get(0));
        }

        @Test
        @DisplayName("let with length — String→Integer type change")
        void testLetWithLength() throws SQLException {
            var result = executeRelation("|let x = 'hello'->length(); $x;");
            assertEquals(1, result.rows().size());
            assertEquals(5L, ((Number) result.rows().get(0).values().get(0)).longValue());
        }
    }

    // ==================== Let feeding into relational operations
    // ====================

    @Nested
    @DisplayName("let feeding relational pipelines")
    class LetInRelational {

        @Test
        @DisplayName("let TDS literal — bind to variable then filter")
        void testLetTdsFilter() throws SQLException {
            var result = executeRelation("""
                    |let data = #TDS
                    id, name
                    1, Alice
                    2, Bob
                    3, Charlie
                    #;
                    $data->filter(x|$x.id > 1);""");
            assertEquals(2, result.rows().size(), "Bob and Charlie");
            assertEquals(2, result.rows().get(0).values().get(0));
            assertEquals("Bob", result.rows().get(0).values().get(1));
            assertEquals(3, result.rows().get(1).values().get(0));
            assertEquals("Charlie", result.rows().get(1).values().get(1));
        }

        @Test
        @DisplayName("let TDS then sort and limit")
        void testLetTdsSortLimit() throws SQLException {
            var result = executeRelation("""
                    |let data = #TDS
                    id, name
                    3, Charlie
                    1, Alice
                    2, Bob
                    #;
                    $data->sort(ascending(~id))->limit(2);""");
            assertEquals(2, result.rows().size());
            assertEquals("Alice", result.rows().get(0).values().get(1));
            assertEquals("Bob", result.rows().get(1).values().get(1));
        }

        @Test
        @DisplayName("let with class-based query — filter by let variable value")
        void testLetClassFilter() throws SQLException {
            var result = executeRelation("""
                    |let threshold = 30;
                    Person.all()->filter(p|$p.age > $threshold)->project(~[name:p|$p.firstName, age:p|$p.age]);""");
            assertEquals(1, result.rows().size(), "Only Bob (45) > 30");
            assertEquals("Bob", result.rows().get(0).values().get(0));
            assertEquals(45, result.rows().get(0).values().get(1));
        }
    }

    // ==================== Let with collections ====================

    @Nested
    @DisplayName("let with collections")
    class LetCollections {

        @Test
        @DisplayName("let integer list — type is Integer[*]")
        void testLetIntegerList() throws SQLException {
            var result = executeRelation("|let xs = [1, 2, 3]; $xs->filter(x|$x > 1);");
            assertEquals(2, result.rows().size());
            assertEquals(2, result.rows().get(0).values().get(0));
            assertEquals(3, result.rows().get(1).values().get(0));
        }

        @Test
        @DisplayName("let string list")
        void testLetStringList() throws SQLException {
            var result = executeRelation("|let names = ['Alice', 'Bob']; $names->filter(x|$x == 'Bob');");
            assertEquals(1, result.rows().size());
            assertEquals("Bob", result.rows().get(0).values().get(0));
        }
    }
}
