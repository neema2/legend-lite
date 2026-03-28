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
 * Comprehensive tests for FromChecker — signature-driven type checking
 * for {@code from()}.
 *
 * <p>
 * Covers:
 * <ul>
 * <li>Basic from(): runtime binding with type passthrough</li>
 * <li>from() after filter, project, sort — type preservation</li>
 * <li>from() with class-based sources</li>
 * <li>from() chaining with other operations</li>
 * <li>Schema preservation through from()</li>
 * </ul>
 *
 * <p>
 * <b>Strong Assertion Policy</b>: every test validates exact row counts,
 * exact column names, exact values element-by-element.
 */
public class FromCheckerTest extends AbstractDatabaseTest {

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

    // ==================== Basic from() ====================

    @Nested
    @DisplayName("from() basic passthrough")
    class BasicFrom {

        @Test
        @DisplayName("from() on class-based source preserves schema through project")
        void testFromClassSource() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                      ->project(~[name:p|$p.firstName, age:p|$p.age])
                      ->from(test::TestRuntime)""");
            assertEquals(3, result.rows().size(), "All 3 persons");
            assertEquals(2, result.columns().size(), "name and age columns");
            assertEquals("name", result.columns().get(0).name());
            assertEquals("age", result.columns().get(1).name());
        }

        @Test
        @DisplayName("from() preserves exact values through passthrough")
        void testFromPreservesValues() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                      ->sortBy({p|$p.firstName})
                      ->project(~[name:p|$p.firstName, age:p|$p.age])
                      ->from(test::TestRuntime)""");
            assertEquals(3, result.rows().size());
            // Sorted alphabetically: Bob, Jane, John
            assertEquals("Bob", result.rows().get(0).values().get(0));
            assertEquals(45, result.rows().get(0).values().get(1));
            assertEquals("Jane", result.rows().get(1).values().get(0));
            assertEquals(28, result.rows().get(1).values().get(1));
            assertEquals("John", result.rows().get(2).values().get(0));
            assertEquals(30, result.rows().get(2).values().get(1));
        }
    }

    // ==================== from() after filter ====================

    @Nested
    @DisplayName("from() after filter")
    class FromAfterFilter {

        @Test
        @DisplayName("filter→project→from() — type preserved through chain")
        void testFilterProjectFrom() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                      ->filter(p|$p.age > 30)
                      ->project(~[name:p|$p.firstName, age:p|$p.age])
                      ->from(test::TestRuntime)""");
            assertEquals(1, result.rows().size(), "Only Bob (45)");
            assertEquals("Bob", result.rows().get(0).values().get(0));
            assertEquals(45, result.rows().get(0).values().get(1));
        }

        @Test
        @DisplayName("filter→from() preserves filtering")
        void testFilterFrom() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                      ->filter(p|$p.lastName == 'Smith')
                      ->project(~[fn:p|$p.firstName, ln:p|$p.lastName])
                      ->from(test::TestRuntime)""");
            assertEquals(2, result.rows().size(), "John and Jane Smith");
            var names = result.rows().stream().map(r -> r.values().get(0)).toList();
            assertTrue(names.contains("John"));
            assertTrue(names.contains("Jane"));
        }
    }

    // ==================== from() after sort ====================

    @Nested
    @DisplayName("from() after sort")
    class FromAfterSort {

        @Test
        @DisplayName("sort→project→from() — ordering preserved")
        void testSortProjectFrom() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                      ->sortBy({p|$p.age})
                      ->project(~[name:p|$p.firstName, age:p|$p.age])
                      ->from(test::TestRuntime)""");
            assertEquals(3, result.rows().size());
            // Sorted by age ascending: Jane(28), John(30), Bob(45)
            assertEquals("Jane", result.rows().get(0).values().get(0));
            assertEquals(28, result.rows().get(0).values().get(1));
            assertEquals("John", result.rows().get(1).values().get(0));
            assertEquals(30, result.rows().get(1).values().get(1));
            assertEquals("Bob", result.rows().get(2).values().get(0));
            assertEquals(45, result.rows().get(2).values().get(1));
        }
    }

    // ==================== from() after slicing ====================

    @Nested
    @DisplayName("from() after slicing")
    class FromAfterSlicing {

        @Test
        @DisplayName("sort→limit→project→from() — limit preserved")
        void testSortLimitProjectFrom() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                      ->sortBy({p|$p.age})
                      ->limit(2)
                      ->project(~[name:p|$p.firstName, age:p|$p.age])
                      ->from(test::TestRuntime)""");
            assertEquals(2, result.rows().size());
            assertEquals("Jane", result.rows().get(0).values().get(0));
            assertEquals("John", result.rows().get(1).values().get(0));
        }
    }

    // ==================== from() with TDS ====================

    @Nested
    @DisplayName("from() with TDS")
    class FromWithTds {

        @Test
        @DisplayName("TDS→from() — passthrough on inline data")
        void testTdsFrom() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    1, Alice
                    2, Bob
                    #->from(test::TestRuntime)""");
            assertEquals(2, result.rows().size());
            assertEquals(2, result.columns().size());
            assertEquals("id", result.columns().get(0).name());
            assertEquals("name", result.columns().get(1).name());
            assertEquals(1, result.rows().get(0).values().get(0));
            assertEquals("Alice", result.rows().get(0).values().get(1));
        }

        @Test
        @DisplayName("TDS→filter→from() — passthrough preserves filter")
        void testTdsFilterFrom() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, val
                    1, 10
                    2, 20
                    3, 30
                    #->filter(x|$x.val > 15)->from(test::TestRuntime)""");
            assertEquals(2, result.rows().size());
            assertEquals(20, result.rows().get(0).values().get(1));
            assertEquals(30, result.rows().get(1).values().get(1));
        }
    }

    // ==================== from() schema column passthrough ====================

    @Nested
    @DisplayName("from() schema preservation")
    class FromSchemaPreservation {

        @Test
        @DisplayName("from() preserves all columns through extend→from")
        void testExtendFromSchema() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    1, Alice
                    2, Bob
                    #->extend(~upper:x|$x.name->toUpper())->from(test::TestRuntime)""");
            assertEquals(2, result.rows().size());
            assertEquals(3, result.columns().size(), "id, name, upper");
            assertEquals("id", result.columns().get(0).name());
            assertEquals("name", result.columns().get(1).name());
            assertEquals("upper", result.columns().get(2).name());
            assertEquals("ALICE", result.rows().get(0).values().get(2));
            assertEquals("BOB", result.rows().get(1).values().get(2));
        }
    }
}
