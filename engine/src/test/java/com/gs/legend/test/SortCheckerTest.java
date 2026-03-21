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
 * Comprehensive tests for SortChecker — canonical Pure sort syntax only.
 *
 * <p>Covers:
 * <ul>
 *   <li>Relation sort: ascending/descending with ColSpec</li>
 *   <li>TDS sort: single column, multi-column, descending</li>
 *   <li>Class-based sort: sort chained with project</li>
 *   <li>Sort→filter, filter→sort chains</li>
 *   <li>Edge cases: sort with no rows, single row, already sorted</li>
 *   <li>Error cases: invalid column, missing sort spec</li>
 * </ul>
 */
public class SortCheckerTest extends AbstractDatabaseTest {

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

    // ==================== TDS Relation sort — ascending ====================

    @Nested
    @DisplayName("#TDS→sort(ascending(~col))")
    class TdsSortAscending {

        @Test
        @DisplayName("sort single column ascending")
        void testSortSingleAsc() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    3, Charlie
                    1, Alice
                    2, Bob
                    #->sort(ascending(~id))""");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            // Verify order: 1, 2, 3
            assertEquals(1, result.rows().get(0).values().get(0));
            assertEquals(2, result.rows().get(1).values().get(0));
            assertEquals(3, result.rows().get(2).values().get(0));
        }

        @Test
        @DisplayName("sort string column ascending")
        void testSortStringAsc() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    3, Charlie
                    1, Alice
                    2, Bob
                    #->sort(ascending(~name))""");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            assertEquals("Alice", result.rows().get(0).values().get(1));
            assertEquals("Bob", result.rows().get(1).values().get(1));
            assertEquals("Charlie", result.rows().get(2).values().get(1));
        }
    }

    // ==================== TDS Relation sort — descending ====================

    @Nested
    @DisplayName("#TDS→sort(descending(~col))")
    class TdsSortDescending {

        @Test
        @DisplayName("sort single column descending")
        void testSortSingleDesc() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    1, Alice
                    3, Charlie
                    2, Bob
                    #->sort(descending(~id))""");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            assertEquals(3, result.rows().get(0).values().get(0));
            assertEquals(2, result.rows().get(1).values().get(0));
            assertEquals(1, result.rows().get(2).values().get(0));
        }
    }

    // ==================== TDS Relation sort — multi-column ====================

    @Nested
    @DisplayName("#TDS→sort([... , ...])")
    class TdsSortMultiColumn {

        @Test
        @DisplayName("sort by two columns ascending (PCT pattern)")
        void testSortTwoColumnsAsc() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    2, George
                    3, Pierre
                    1, Sachin
                    1, Neema
                    5, David
                    4, Alex
                    2, Thierry
                    #->sort([ascending(~id), ascending(~name)])""");
            assertNotNull(result);
            assertEquals(7, result.rows().size());
            // id=1: Neema before Sachin
            assertEquals("Neema", result.rows().get(0).values().get(1));
            assertEquals("Sachin", result.rows().get(1).values().get(1));
            // id=2: George before Thierry
            assertEquals("George", result.rows().get(2).values().get(1));
            assertEquals("Thierry", result.rows().get(3).values().get(1));
        }

        @Test
        @DisplayName("sort descending id, ascending name (PCT pattern)")
        void testSortDescAsc() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    2, George
                    3, Pierre
                    1, Sachin
                    1, Neema
                    5, David
                    4, Alex
                    2, Thierry
                    #->sort([descending(~id), ascending(~name)])""");
            assertNotNull(result);
            assertEquals(7, result.rows().size());
            // First row: id=5, David
            assertEquals(5, result.rows().get(0).values().get(0));
            assertEquals("David", result.rows().get(0).values().get(1));
            // Last rows: id=1, Neema then Sachin
            assertEquals(1, result.rows().get(5).values().get(0));
            assertEquals("Neema", result.rows().get(5).values().get(1));
            assertEquals("Sachin", result.rows().get(6).values().get(1));
        }
    }

    // ==================== Class-based sort (Collection sort — key lambdas) ====================

    @Nested
    @DisplayName("Class.all()→sort({p|$p.prop})→project()")
    class ClassSort {

        @Test
        @DisplayName("sort class by property ascending (default)")
        void testClassSortAsc() throws SQLException {
            var result = executeRelation(
                    "Person.all()->sort({p|$p.age})->project(~[name:p|$p.firstName, age:p|$p.age])");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            // Jane(28), John(30), Bob(45)
            var ages = result.rows().stream().map(r -> r.values().get(1)).toList();
            assertEquals(List.of(28, 30, 45), ages);
        }

        @Test
        @DisplayName("sort class by property descending (compare lambda)")
        void testClassSortDesc() throws SQLException {
            var result = executeRelation(
                    "Person.all()->sort({p|$p.age}, {x,y|$y->compare($x)})->project(~[name:p|$p.firstName, age:p|$p.age])");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            // Bob(45), John(30), Jane(28)
            var ages = result.rows().stream().map(r -> r.values().get(1)).toList();
            assertEquals(List.of(45, 30, 28), ages);
        }

        @Test
        @DisplayName("sortBy class shorthand")
        void testClassSortBy() throws SQLException {
            var result = executeRelation(
                    "Person.all()->sortBy({p|$p.firstName})->project(~[name:p|$p.firstName, age:p|$p.age])");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            // Bob, Jane, John (alphabetical)
            var names = result.rows().stream().map(r -> r.values().get(0)).toList();
            assertEquals(List.of("Bob", "Jane", "John"), names);
        }

        @Test
        @DisplayName("sort by to-one association property")
        void testClassSortByAssociation() throws SQLException {
            // sort by primaryAddress.city (to-one): Chicago(Jane), Detroit(Bob), New York(John)
            var result = executeRelation("""
                    Person.all()->sort({p|$p.primaryAddress.city})
                      ->project(~[name:p|$p.firstName, age:p|$p.age])""");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            var names = result.rows().stream().map(r -> r.values().get(0)).toList();
            assertEquals(List.of("Jane", "Bob", "John"), names);
        }
    }

    // ==================== Sort + Filter chains ====================

    @Nested
    @DisplayName("sort→filter / filter→sort chains")
    class SortFilterChains {

        @Test
        @DisplayName("filter then sort (class-based — key lambda)")
        void testFilterThenSort() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                      ->filter(p|$p.age > 25)
                      ->sort({p|$p.age})
                      ->project(~[name:p|$p.firstName, age:p|$p.age])""");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            // Jane(28), John(30), Bob(45)
            var ages = result.rows().stream().map(r -> r.values().get(1)).toList();
            assertEquals(List.of(28, 30, 45), ages);
        }

        @Test
        @DisplayName("sort then filter (on TDS)")
        void testSortThenFilter() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    3, Charlie
                    1, Alice
                    2, Bob
                    #->sort(ascending(~id))->filter(x|$x.id > 1)""");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
            // Should be sorted: 2, 3
            assertEquals(2, result.rows().get(0).values().get(0));
            assertEquals(3, result.rows().get(1).values().get(0));
        }


        @Test
        @DisplayName("project then sort by projected column")
        void testProjectThenSort() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                      ->project(~[name:p|$p.firstName, age:p|$p.age])
                      ->sort(ascending(~age))""");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            // Jane(28), John(30), Bob(45) — sorted by projected 'age' alias
            var ages = result.rows().stream().map(r -> r.values().get(1)).toList();
            assertEquals(List.of(28, 30, 45), ages);
        }

        @Test
        @DisplayName("project then sort descending")
        void testProjectThenSortDesc() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                      ->project(~[name:p|$p.firstName, age:p|$p.age])
                      ->sort(descending(~age))""");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            // Bob(45), John(30), Jane(28)
            var ages = result.rows().stream().map(r -> r.values().get(1)).toList();
            assertEquals(List.of(45, 30, 28), ages);
        }

        @Test
        @DisplayName("filter→project→sort (full pipeline)")
        void testFilterProjectSort() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                      ->filter(p|$p.age > 25)
                      ->project(~[name:p|$p.firstName, age:p|$p.age])
                      ->sort(ascending(~name))""");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            // Sorted by name: Bob, Jane, John
            var names = result.rows().stream().map(r -> r.values().get(0)).toList();
            assertEquals(List.of("Bob", "Jane", "John"), names);
        }

        @Test
        @DisplayName("project→sort→filter (sort then narrow)")
        void testProjectSortFilter() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                      ->project(~[name:p|$p.firstName, age:p|$p.age])
                      ->sort(ascending(~age))
                      ->filter(x|$x.age > 28)""");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
            // John(30), Bob(45) — sorted then filtered
            var ages = result.rows().stream().map(r -> r.values().get(1)).toList();
            assertEquals(List.of(30, 45), ages);
        }

        @Test
        @DisplayName("sort→project (class-based — key lambda desc)")
        void testSortThenProject() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                      ->sort({p|$p.age}, {x,y|$y->compare($x)})
                      ->project(~[name:p|$p.firstName, age:p|$p.age])""");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            // Bob(45), John(30), Jane(28)
            var ages = result.rows().stream().map(r -> r.values().get(1)).toList();
            assertEquals(List.of(45, 30, 28), ages);
        }

        @Test
        @DisplayName("filter→sort→filter (double filter with sort)")
        void testFilterSortFilter() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                      ->filter(p|$p.age > 25)
                      ->sort({p|$p.firstName})
                      ->filter(p|$p.lastName == 'Smith')
                      ->project(~[name:p|$p.firstName, age:p|$p.age])""");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
            // Jane(28) and John(30) — both Smiths with age > 25, sorted by firstName
            assertEquals("Jane", result.rows().get(0).values().get(0));
            assertEquals("John", result.rows().get(1).values().get(0));
        }

        @Test
        @DisplayName("project→sort multi-column")
        void testProjectThenSortMultiColumn() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                      ->project(~[ln:p|$p.lastName, fn:p|$p.firstName, age:p|$p.age])
                      ->sort([ascending(~ln), descending(~age)])""");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            // Jones(Bob,45) then Smith(John,30), Smith(Jane,28) — lastName asc, age desc within
            assertEquals("Jones", result.rows().get(0).values().get(0));
            assertEquals("Smith", result.rows().get(1).values().get(0));
            assertEquals(30, result.rows().get(1).values().get(2));  // John first (age 30 > 28)
            assertEquals(28, result.rows().get(2).values().get(2));  // Jane second
        }

        @Test
        @DisplayName("TDS sort→filter→sort (re-sort)")
        void testSortFilterSort() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id, name
                    5, Eve
                    1, Alice
                    3, Charlie
                    2, Bob
                    4, Dave
                    #->sort(ascending(~id))
                     ->filter(x|$x.id > 1)
                     ->sort(descending(~name))""");
            assertNotNull(result);
            assertEquals(4, result.rows().size());
            // Filtered out id=1, then re-sorted by name desc: Eve, Dave, Charlie, Bob
            assertEquals("Eve", result.rows().get(0).values().get(1));
            assertEquals("Dave", result.rows().get(1).values().get(1));
            assertEquals("Charlie", result.rows().get(2).values().get(1));
            assertEquals("Bob", result.rows().get(3).values().get(1));
        }
    }

    // ==================== Edge cases ====================

    @Nested
    @DisplayName("sort edge cases")
    class SortEdgeCases {

        @Test
        @DisplayName("sort single row (no-op)")
        void testSortSingleRow() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val
                    42
                    #->sort(ascending(~val))""");
            assertNotNull(result);
            assertEquals(1, result.rows().size());
            assertEquals(42, result.rows().get(0).values().get(0));
        }

        @Test
        @DisplayName("sort already sorted data")
        void testSortAlreadySorted() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    id
                    1
                    2
                    3
                    #->sort(ascending(~id))""");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            assertEquals(1, result.rows().get(0).values().get(0));
            assertEquals(2, result.rows().get(1).values().get(0));
            assertEquals(3, result.rows().get(2).values().get(0));
        }

        @Test
        @DisplayName("sort with all duplicate values")
        void testSortDuplicates() throws SQLException {
            var result = executeRelation("""
                    |#TDS
                    val, name
                    1, C
                    1, A
                    1, B
                    #->sort([ascending(~val), ascending(~name)])""");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            // All val=1, so sorted by name: A, B, C
            assertEquals("A", result.rows().get(0).values().get(1));
            assertEquals("B", result.rows().get(1).values().get(1));
            assertEquals("C", result.rows().get(2).values().get(1));
        }
    }

    // ==================== Error cases ====================

    @Nested
    @DisplayName("sort error cases")
    class SortErrors {

        @Test
        @DisplayName("sort on non-existent column throws")
        void testSortInvalidColumn() {
            assertThrows(Exception.class, () ->
                    executeRelation("""
                            |#TDS
                            id, name
                            1, Alice
                            #->sort(ascending(~nonexistent))"""));
        }

        @Test
        @DisplayName("sort with wrong syntax throws (string column name)")
        void testSortStringColumnRejects() {
            // Legacy sort('col') syntax is no longer supported
            assertThrows(Exception.class, () ->
                    executeRelation("""
                            |#TDS
                            id, name
                            1, Alice
                            #->sort('id')"""));
        }

        @Test
        @DisplayName("sort on to-many association rejects (which value to pick?)")
        void testSortToManyAssociationRejects() {
            // {p|$p.addresses.city} returns String[*], but sort key requires U[1]
            assertThrows(Exception.class, () ->
                    executeRelation("""
                            Person.all()->sort({p|$p.addresses.city})
                              ->project(~[name:p|$p.firstName])"""));
        }
    }
}
