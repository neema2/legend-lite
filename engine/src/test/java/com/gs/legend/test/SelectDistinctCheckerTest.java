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
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SelectChecker and DistinctChecker.
 *
 * <p>Covers select (column subset) and distinct (dedup) operations on TDS and class-based sources.
 */
public class SelectDistinctCheckerTest extends AbstractDatabaseTest {

    @Override
    protected String getDatabaseType() { return "DuckDB"; }

    @Override
    protected SQLDialect getDialect() { return DuckDBDialect.INSTANCE; }

    @Override
    protected String getJdbcUrl() { return "jdbc:duckdb:"; }

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection(getJdbcUrl());
        setupPureModel();
        setupDatabase();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) connection.close();
    }

    // ==================== Select ====================

    @Nested
    @DisplayName("select — column subset")
    class SelectTests {

        @Test
        @DisplayName("select single column from TDS")
        void testSelectSingle() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name, score
                    1, Alice, 95
                    2, Bob, 87
                    #->select(~name)->sort(ascending(~name))""");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
            assertEquals("Alice", result.rows().get(0).values().get(0));
            assertEquals("Bob", result.rows().get(1).values().get(0));
        }

        @Test
        @DisplayName("select multiple columns from TDS")
        void testSelectMultiple() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name, score
                    1, Alice, 95
                    2, Bob, 87
                    #->select(~[id, name])->sort(ascending(~id))""");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
            assertEquals(1, result.rows().get(0).values().get(0));
            assertEquals("Alice", result.rows().get(0).values().get(1));
        }

        @Test
        @DisplayName("select after project on class-based source")
        void testSelectAfterProject() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                      ->project(~[name:p|$p.firstName, age:p|$p.age])
                      ->select(~name)
                      ->sort(ascending(~name))""");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            var names = result.rows().stream().map(r -> r.values().get(0)).toList();
            assertEquals(List.of("Bob", "Jane", "John"), names);
        }

        @Test
        @DisplayName("select non-existent column throws")
        void testSelectNonExistent() {
            assertThrows(Exception.class, () ->
                    executeRelation("""
                            |#TDS
                            id, name
                            1, Alice
                            #->select(~nonexistent)"""));
        }
    }

    // ==================== Distinct ====================

    @Nested
    @DisplayName("distinct — deduplication")
    class DistinctTests {

        @Test
        @DisplayName("distinct removes duplicates")
        void testDistinctSimple() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    city
                    New York
                    Chicago
                    New York
                    Chicago
                    Boston
                    #->distinct()->sort(ascending(~city))""");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            assertEquals("Boston", result.rows().get(0).values().get(0));
            assertEquals("Chicago", result.rows().get(1).values().get(0));
            assertEquals("New York", result.rows().get(2).values().get(0));
        }

        @Test
        @DisplayName("distinct on specific columns")
        void testDistinctColumns() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    dept, name
                    Eng, Alice
                    Eng, Bob
                    Sales, Alice
                    Sales, Charlie
                    #->distinct(~dept)->sort(ascending(~dept))""");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
            assertEquals("Eng", result.rows().get(0).values().get(0));
            assertEquals("Sales", result.rows().get(1).values().get(0));
        }

        @Test
        @DisplayName("distinct on class-based source after project")
        void testDistinctClassBased() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                      ->project(~[age:p|$p.age])
                      ->distinct()
                      ->sort(ascending(~age))""");
            assertNotNull(result);
            // 3 unique ages: 28, 30, 45
            assertEquals(3, result.rows().size());
            assertEquals(28, result.rows().get(0).values().get(0));
            assertEquals(30, result.rows().get(1).values().get(0));
            assertEquals(45, result.rows().get(2).values().get(0));
        }
    }

    // ==================== Select + Distinct chains ====================

    @Nested
    @DisplayName("select/distinct chains")
    class Chains {

        @Test
        @DisplayName("project→select→distinct→sort")
        void testProjectSelectDistinctSort() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                      ->project(~[name:p|$p.firstName, age:p|$p.age])
                      ->select(~age)
                      ->distinct()
                      ->sort(ascending(~age))""");
            assertNotNull(result);
            assertEquals(3, result.rows().size()); // 28, 30, 45
        }
    }
}
