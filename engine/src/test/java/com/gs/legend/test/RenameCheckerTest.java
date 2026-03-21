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
 * Comprehensive tests for RenameChecker — single and batch column rename.
 *
 * <p>Covers:
 * <ul>
 *   <li>Single column rename: ~old, ~new</li>
 *   <li>Batch rename: ~[old1, old2], ~[new1, new2]</li>
 *   <li>Chains: rename→filter, rename→sort, project→rename</li>
 *   <li>Error cases: non-existent column</li>
 * </ul>
 */
public class RenameCheckerTest extends AbstractDatabaseTest {

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

    // ==================== Single column rename ====================

    @Nested
    @DisplayName("single column rename (~old, ~new)")
    class SingleRename {

        @Test
        @DisplayName("rename single column on TDS")
        void testRenameSingle() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    1, Alice
                    2, Bob
                    #->rename(~name, ~fullName)""");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
            // Column should now be called fullName
            assertEquals("Alice", result.rows().get(0).values().get(1));
        }

        @Test
        @DisplayName("rename preserves data")
        void testRenamePreservesData() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name, score
                    1, Alice, 95
                    2, Bob, 87
                    #->rename(~score, ~points)->sort(ascending(~id))""");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
            assertEquals(95, result.rows().get(0).values().get(2));
            assertEquals(87, result.rows().get(1).values().get(2));
        }

        @Test
        @DisplayName("rename on class-based source after project")
        void testRenameAfterProject() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                      ->project(~[name:p|$p.firstName, age:p|$p.age])
                      ->rename(~name, ~fullName)
                      ->sort(ascending(~fullName))""");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            // EXCLUDE+rename reorders: age stays at 0, fullName at 1
            var names = result.rows().stream().map(r -> r.values().get(1)).toList();
            assertEquals(List.of("Bob", "Jane", "John"), names);
        }
    }

    // ==================== Batch rename ====================

    @Nested
    @DisplayName("batch rename (~[old1, old2], ~[new1, new2])")
    class BatchRename {

        @Test
        @DisplayName("rename multiple columns at once")
        void testRenameBatch() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name, score
                    1, Alice, 95
                    2, Bob, 87
                    #->rename(~[name, score], ~[fullName, points])->sort(ascending(~id))""");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
            // Data should be preserved
            assertEquals(1, result.rows().get(0).values().get(0));
            assertEquals("Alice", result.rows().get(0).values().get(1));
            assertEquals(95, result.rows().get(0).values().get(2));
        }

        @Test
        @DisplayName("batch rename on class-based source")
        void testRenameBatchClassBased() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                      ->project(~[name:p|$p.firstName, years:p|$p.age])
                      ->rename(~[name, years], ~[fullName, ageInYears])
                      ->sort(ascending(~fullName))""");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            var names = result.rows().stream().map(r -> r.values().get(0)).toList();
            assertEquals(List.of("Bob", "Jane", "John"), names);
        }
    }

    // ==================== Rename chains ====================

    @Nested
    @DisplayName("rename combined with other operations")
    class RenameChains {

        @Test
        @DisplayName("rename→filter uses new column name")
        void testRenameFilter() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    1, Alice
                    2, Bob
                    3, Charlie
                    #->rename(~id, ~userId)->filter(x|$x.userId > 1)->sort(ascending(~userId))""");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
            // EXCLUDE+rename: name at 0, userId at 1
            assertEquals(2, result.rows().get(0).values().get(1));
            assertEquals(3, result.rows().get(1).values().get(1));
        }

        @Test
        @DisplayName("rename→sort uses new column name")
        void testRenameSort() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    3, Charlie
                    1, Alice
                    2, Bob
                    #->rename(~name, ~fullName)->sort(ascending(~fullName))""");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            assertEquals("Alice", result.rows().get(0).values().get(1));
            assertEquals("Bob", result.rows().get(1).values().get(1));
            assertEquals("Charlie", result.rows().get(2).values().get(1));
        }
    }

    // ==================== Error cases ====================

    @Nested
    @DisplayName("rename error cases")
    class RenameErrors {

        @Test
        @DisplayName("rename non-existent column throws")
        void testRenameNonExistent() {
            assertThrows(Exception.class, () ->
                    executeRelation("""
                            |#TDS
                            id, name
                            1, Alice
                            #->rename(~nonexistent, ~newName)"""));
        }
    }
}
