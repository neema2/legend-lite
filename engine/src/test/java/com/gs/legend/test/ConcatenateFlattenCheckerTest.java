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
 * Comprehensive tests for ConcatenateChecker and FlattenChecker.
 *
 * <p>ConcatenateChecker covers:
 * <ul>
 *   <li>Relational: same-schema TDS concat</li>
 *   <li>Column mismatch errors: left-only, right-only, different names (symmetric diff)</li>
 *   <li>Class-based: same class concat</li>
 *   <li>Chains: concat→filter, concat→sort, concat→distinct</li>
 *   <li>Edge cases: empty TDS, single-row, chained 3-way</li>
 * </ul>
 *
 * <p>FlattenChecker covers:
 * <ul>
 *   <li>Non-existent column error</li>
 * </ul>
 */
public class ConcatenateFlattenCheckerTest extends AbstractDatabaseTest {

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

    // ==================== Concatenate: relational ====================

    @Nested
    @DisplayName("concatenate — relational TDS")
    class ConcatenateRelational {

        @Test
        @DisplayName("concat two TDS with same schema")
        void testConcatSameSchema() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    1, Alice
                    2, Bob
                    #->concatenate(
                      |#TDS
                      id, name
                      3, Charlie
                      #
                    )->sort(ascending(~id))""");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            assertEquals(1, result.rows().get(0).values().get(0));
            assertEquals("Charlie", result.rows().get(2).values().get(1));
        }

        @Test
        @DisplayName("concat then filter keeps matching rows from both sides")
        void testConcatFilter() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    1, Alice
                    2, Bob
                    #->concatenate(
                      |#TDS
                      id, name
                      3, Charlie
                      4, Dave
                      #
                    )->filter(x|$x.id > 2)->sort(ascending(~id))""");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
            assertEquals(3, result.rows().get(0).values().get(0));
            assertEquals(4, result.rows().get(1).values().get(0));
        }

        @Test
        @DisplayName("concat then sort by name")
        void testConcatSort() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    2, Bob
                    1, Alice
                    #->concatenate(
                      |#TDS
                      id, name
                      3, Charlie
                      #
                    )->sort(ascending(~id))""");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            // sorted by id: 1=Alice, 2=Bob, 3=Charlie
            assertEquals(1, result.rows().get(0).values().get(0));
            assertEquals(2, result.rows().get(1).values().get(0));
            assertEquals(3, result.rows().get(2).values().get(0));
        }
    }

    // ==================== Concatenate: errors ====================

    @Nested
    @DisplayName("concatenate — error cases")
    class ConcatenateErrors {

        @Test
        @DisplayName("column mismatch — left has extra column")
        void testConcatMismatchLeftExtra() {
            var ex = assertThrows(Exception.class, () ->
                    executeRelation("""
                            |#TDS
                            id, name, score
                            1, Alice, 95
                            #->concatenate(
                              |#TDS
                              id, name
                              2, Bob
                              #
                            )"""));
            assertTrue(ex.getMessage().contains("mismatch") || ex.getMessage().contains("column"),
                    "Expected column mismatch error, got: " + ex.getMessage());
        }

        @Test
        @DisplayName("column mismatch — right has extra column")
        void testConcatMismatchRightExtra() {
            var ex = assertThrows(Exception.class, () ->
                    executeRelation("""
                            |#TDS
                            id, name
                            1, Alice
                            #->concatenate(
                              |#TDS
                              id, name, age
                              2, Bob, 30
                              #
                            )"""));
            assertTrue(ex.getMessage().contains("mismatch") || ex.getMessage().contains("column"),
                    "Expected column mismatch error, got: " + ex.getMessage());
        }

        @Test
        @DisplayName("column mismatch — different column names (same count)")
        void testConcatDifferentColumns() {
            var ex = assertThrows(Exception.class, () ->
                    executeRelation("""
                            |#TDS
                            id, name
                            1, Alice
                            #->concatenate(
                              |#TDS
                              id, age
                              2, 30
                              #
                            )"""));
            assertTrue(ex.getMessage().contains("mismatch") || ex.getMessage().contains("column"),
                    "Expected column mismatch error, got: " + ex.getMessage());
        }
    }

    // ==================== Concatenate: class-based ====================

    @Nested
    @DisplayName("concatenate — class-based")
    class ConcatenateClass {

        @Test
        @DisplayName("concat two class queries with same type")
        void testConcatSameClass() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                      ->filter(p|$p.age > 30)
                      ->project(~[name:p|$p.firstName, age:p|$p.age])
                      ->concatenate(
                        Person.all()
                          ->filter(p|$p.age <= 30)
                          ->project(~[name:p|$p.firstName, age:p|$p.age])
                      )
                      ->sort(ascending(~age))""");
            assertNotNull(result);
            // All 3 people combined from both filter branches, sorted by age ascending
            assertEquals(3, result.rows().size());
            var ages = result.rows().stream().map(r -> r.values().get(1)).toList();
            assertEquals(java.util.List.of(28, 30, 45), ages);
        }
    }

    // ==================== Flatten ====================

    @Nested
    @DisplayName("flatten — UNNEST")
    class FlattenTests {

        @Test
        @DisplayName("flatten non-existent column throws")
        void testFlattenNonExistent() {
            assertThrows(Exception.class, () ->
                    executeRelation("""
                            |#TDS
                            id, name
                            1, Alice
                            #->flatten(~nonexistent)"""));
        }
    }

    // ==================== Chains ====================

    @Nested
    @DisplayName("concat + flatten chains")
    class Chains {

        @Test
        @DisplayName("concat→distinct on specific column")
        void testConcatDistinct() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    1, Alice
                    2, Bob
                    #->concatenate(
                      |#TDS
                      id, name
                      3, Alice
                      4, Charlie
                      #
                    )->distinct(~[name])->sort(ascending(~name))""");
            assertNotNull(result);
            // 'Alice' appears twice, distinct(~name) keeps 3 unique names
            assertEquals(3, result.rows().size());
        }

        @Test
        @DisplayName("concat→rename→filter chain")
        void testConcatRenameFilter() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    1, Alice
                    2, Bob
                    #->concatenate(
                      |#TDS
                      id, name
                      3, Charlie
                      #
                    )->rename(~name, ~fullName)->filter(x|$x.id > 1)->sort(ascending(~id))""");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
            assertEquals(2, result.rows().get(0).values().get(0));
        }
    }

    // ==================== Edge cases ====================

    @Nested
    @DisplayName("concatenate — edge cases")
    class ConcatenateEdgeCases {

        @Test
        @DisplayName("concat single-row TDS")
        void testConcatSingleRow() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    1, Alice
                    #->concatenate(
                      |#TDS
                      id, name
                      2, Bob
                      #
                    )->sort(ascending(~id))""");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
            assertEquals(1, result.rows().get(0).values().get(0));
            assertEquals(2, result.rows().get(1).values().get(0));
        }

        @Test
        @DisplayName("chained 3-way concat")
        void testConcatThreeWay() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    1, Alice
                    #->concatenate(
                      |#TDS
                      id, name
                      2, Bob
                      #
                    )->concatenate(
                      |#TDS
                      id, name
                      3, Charlie
                      #
                    )->sort(ascending(~id))""");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            assertEquals(1, result.rows().get(0).values().get(0));
            assertEquals(2, result.rows().get(1).values().get(0));
            assertEquals(3, result.rows().get(2).values().get(0));
        }

        @Test
        @DisplayName("concat preserves duplicates (UNION ALL, not UNION)")
        void testConcatPreservesDuplicates() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    1, Alice
                    2, Bob
                    #->concatenate(
                      |#TDS
                      id, name
                      1, Alice
                      2, Bob
                      #
                    )->sort(ascending(~id))""");
            assertNotNull(result);
            // UNION ALL: duplicates preserved — 4 rows, not 2
            assertEquals(4, result.rows().size());
        }
    }

    // ==================== Collection (scalar list) concatenation ====================

    @Nested
    @DisplayName("concatenate — collection (scalar lists)")
    class ConcatenateCollection {

        @Test
        @DisplayName("homogeneous integer list concat")
        void testConcatIntLists() throws SQLException {
            var result = executeRelation(
                    "|[1, 2, 3]->meta::pure::functions::collection::concatenate([4, 5])");
            var values = result.asCollection().values();
            assertEquals(5, values.size());
            assertEquals(1, ((Number) values.get(0)).intValue());
            assertEquals(5, ((Number) values.get(4)).intValue());
        }

        @Test
        @DisplayName("homogeneous string list concat")
        void testConcatStringLists() throws SQLException {
            var result = executeRelation(
                    "|['a', 'b']->meta::pure::functions::collection::concatenate(['c', 'd'])");
            var values = result.asCollection().values();
            assertEquals(java.util.List.of("a", "b", "c", "d"), values);
        }

        @Test
        @DisplayName("mixed-type list concat (int + string)")
        void testConcatMixedType() throws SQLException {
            var result = executeRelation(
                    "|[1, 2, 3]->meta::pure::functions::collection::concatenate(['a', 'b'])");
            var values = result.asCollection().values();
            assertEquals(5, values.size());
            // Mixed: ints become longs, strings stay strings
            assertEquals("a", values.get(3));
            assertEquals("b", values.get(4));
        }
    }
}
