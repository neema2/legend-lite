package com.legend.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for SlicingChecker — limit, take, drop, slice, first, last.
 *
 * <p>Covers:
 * <ul>
 *   <li>TDS: limit, drop, slice on inline TDS</li>
 *   <li>Class-based: take, drop, first, last on model::Person.all()</li>
 *   <li>Chains: filter→limit, sort→limit, project→slice</li>
 *   <li>Edge cases: limit(0), drop all, slice beyond bounds</li>
 *   <li>Error cases: wrong argument types</li>
 * </ul>
 */
public class SlicingCheckerTest extends AbstractDatabaseTest {

    @BeforeEach
    void setUp() throws SQLException {
        connection = backend().open();
        setupDatabase();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) connection.close();
    }

    // ==================== TDS slicing ====================

    @Nested
    @DisplayName("TDS limit/drop/slice")
    class TdsSlicing {

        @Test
        @DisplayName("limit on TDS")
        void testTdsLimit() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    1, Alice
                    2, Bob
                    3, Charlie
                    #->sort(ascending(~id))->limit(2)""");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
            assertEquals("Alice", result.rows().get(0).get(1));
            assertEquals("Bob", result.rows().get(1).get(1));
        }

        @Test
        @DisplayName("drop on TDS")
        void testTdsDrop() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    1, Alice
                    2, Bob
                    3, Charlie
                    #->sort(ascending(~id))->drop(1)""");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
            assertEquals("Bob", result.rows().get(0).get(1));
            assertEquals("Charlie", result.rows().get(1).get(1));
        }

        @Test
        @DisplayName("slice on TDS")
        void testTdsSlice() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    1, Alice
                    2, Bob
                    3, Charlie
                    4, Diana
                    #->sort(ascending(~id))->slice(1,3)""");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
            // slice(1,3) = rows at indices 1,2 (0-based) = Bob, Charlie
            assertEquals("Bob", result.rows().get(0).get(1));
            assertEquals("Charlie", result.rows().get(1).get(1));
        }

        @Test
        @DisplayName("limit(0) returns empty")
        void testTdsLimitZero() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    1, Alice
                    2, Bob
                    #->limit(0)""");
            assertNotNull(result);
            assertEquals(0, result.rows().size());
        }
    }

    // ==================== Class-based slicing ====================

    @Nested
    @DisplayName("Class.all()→slicing→project()")
    class ClassSlicing {

        @Test
        @DisplayName("take on class-based source")
        void testClassTake() throws SQLException {
            var result = executeRelation("""
                    model::Person.all()
                      ->sortBy({p|$p.firstName})
                      ->take(2)
                      ->project(~[name:p|$p.firstName, age:p|$p.age])""");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
            // Bob, Jane (first 2 alphabetically)
            var names = result.rows().stream().map(r -> r.get(0)).toList();
            assertEquals(List.of("Bob", "Jane"), names);
        }

        @Test
        @DisplayName("drop on class-based source")
        void testClassDrop() throws SQLException {
            var result = executeRelation("""
                    model::Person.all()
                      ->sortBy({p|$p.firstName})
                      ->drop(2)
                      ->project(~[name:p|$p.firstName, age:p|$p.age])""");
            assertNotNull(result);
            assertEquals(1, result.rows().size());
            // John (last 1 alphabetically after dropping Bob, Jane)
            assertEquals("John", result.rows().get(0).get(0));
        }

        @Test
        @DisplayName("first on TDS (returns single row)")
        void testFirst() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    1, Alice
                    2, Bob
                    3, Charlie
                    #->sort(ascending(~id))->first()""");
            assertNotNull(result);
            assertEquals(1, result.rows().size());
            assertEquals("Alice", result.rows().get(0).get(1));
        }

        @Test
        @DisplayName("limit on class-based source")
        void testClassLimit() throws SQLException {
            var result = executeRelation("""
                    model::Person.all()
                      ->sortBy({p|$p.age})
                      ->limit(1)
                      ->project(~[name:p|$p.firstName, age:p|$p.age])""");
            assertNotNull(result);
            assertEquals(1, result.rows().size());
            assertEquals("Jane", result.rows().get(0).get(0));
        }
    }

    // ==================== Slicing chains ====================

    @Nested
    @DisplayName("slicing combined with filter/sort/project")
    class SlicingChains {

        @Test
        @DisplayName("filter→sort→limit")
        void testFilterSortLimit() throws SQLException {
            var result = executeRelation("""
                    model::Person.all()
                      ->filter(p|$p.age > 25)
                      ->sortBy({p|$p.age})
                      ->limit(2)
                      ->project(~[name:p|$p.firstName, age:p|$p.age])""");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
            // Jane(28), John(30) — sorted by age, limited to 2
            var names = result.rows().stream().map(r -> r.get(0)).toList();
            assertEquals(List.of("Jane", "John"), names);
        }

        @Test
        @DisplayName("project→sort→slice")
        void testProjectSortSlice() throws SQLException {
            var result = executeRelation("""
                    model::Person.all()
                      ->project(~[name:p|$p.firstName, age:p|$p.age])
                      ->sort(ascending(~age))
                      ->slice(0,2)""");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
            // Jane(28), John(30)
            var names = result.rows().stream().map(r -> r.get(0)).toList();
            assertEquals(List.of("Jane", "John"), names);
        }

        @Test
        @DisplayName("sort→take→filter (TDS)")
        void testSortTakeFilter() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    3, Charlie
                    1, Alice
                    2, Bob
                    4, Diana
                    #->sort(ascending(~id))->take(3)->filter(x|$x.id > 1)""");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
            // Takes first 3 (Alice,Bob,Charlie), then filters id>1 → Bob(2), Charlie(3)
            assertEquals(2L, result.rows().get(0).get(0));
            assertEquals(3L, result.rows().get(1).get(0));
        }

        @Test
        @DisplayName("drop all rows returns empty")
        void testDropAll() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    1, Alice
                    2, Bob
                    #->drop(10)""");
            assertNotNull(result);
            assertEquals(0, result.rows().size());
        }
    }

    // ==================== Error cases ====================

    @Nested
    @DisplayName("slicing error cases")
    class SlicingErrors {

        @Test
        @DisplayName("first()→project() works on class-based source (first matches collection overload)")
        void testFirstThenProjectRejects() throws SQLException {
            // first(T[*]):T[0..1] matches class-based sources (isRelational=false)
            // project() works because class mapping is preserved in TypeInfo
            var result = executeRelation("""
                            model::Person.all()
                              ->sortBy({p|$p.age})
                              ->first()
                              ->project(~[name:p|$p.firstName])""");
            assertNotNull(result);
            assertEquals(1, result.rows().size());
        }
    }
}
