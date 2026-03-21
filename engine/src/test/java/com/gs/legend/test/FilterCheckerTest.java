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
 * Integration tests for FilterChecker and ProjectChecker.
 * <ul>
 *   <li>Filter: Class-based, TDS, scalar collection, edge cases</li>
 *   <li>Project: Canonical ~[alias:x|$x.prop, ...] syntax</li>
 *   <li>Chained: filter→project→filter, project→filter</li>
 *   <li>Associations: filter via association navigation</li>
 * </ul>
 */
public class FilterCheckerTest extends AbstractDatabaseTest {

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

    // ==================== Class-based filter ====================

    @Nested
    @DisplayName("Class.all()->filter()")
    class ClassFilter {

        @Test
        @DisplayName("filter by integer property")
        void testFilterByAge() throws SQLException {
            var result = executeRelation(
                    "Person.all()->filter(p|$p.age > 30)->project(~[firstName:p|$p.firstName, age:p|$p.age])");
            assertNotNull(result);
            assertEquals(1, result.rows().size(), "Only Bob (45) has age > 30");
        }

        @Test
        @DisplayName("filter by string property")
        void testFilterByString() throws SQLException {
            var result = executeRelation(
                    "Person.all()->filter(p|$p.lastName == 'Smith')->project(~[firstName:p|$p.firstName, lastName:p|$p.lastName])");
            assertNotNull(result);
            assertEquals(2, result.rows().size(), "John and Jane Smith");
        }

        @Test
        @DisplayName("filter with AND")
        void testFilterWithAnd() throws SQLException {
            var result = executeRelation(
                    "Person.all()->filter(p|($p.lastName == 'Smith') && ($p.age > 28))->project(~[firstName:p|$p.firstName, age:p|$p.age])");
            assertNotNull(result);
            assertEquals(1, result.rows().size(), "Only John Smith (30)");
        }

        @Test
        @DisplayName("filter with OR")
        void testFilterWithOr() throws SQLException {
            var result = executeRelation(
                    "Person.all()->filter(p|($p.firstName == 'John') || ($p.firstName == 'Bob'))->project(~[firstName:p|$p.firstName])");
            assertNotNull(result);
            assertEquals(2, result.rows().size());
        }

        @Test
        @DisplayName("filter no match")
        void testFilterNoMatch() throws SQLException {
            var result = executeRelation(
                    "Person.all()->filter(p|$p.age > 100)->project(~[firstName:p|$p.firstName])");
            assertNotNull(result);
            assertTrue(result.rows().isEmpty());
        }

        @Test
        @DisplayName("chained filters")
        void testChainedFilters() throws SQLException {
            var result = executeRelation(
                    "Person.all()->filter(p|$p.age > 25)->filter(p|$p.lastName == 'Smith')->project(~[firstName:p|$p.firstName, age:p|$p.age])");
            assertNotNull(result);
            assertEquals(2, result.rows().size(), "John(30) and Jane(28) Smith");
        }

        @Test
        @DisplayName("filter with less-than")
        void testFilterLessThan() throws SQLException {
            var result = executeRelation(
                    "Person.all()->filter(p|$p.age < 30)->project(~[firstName:p|$p.firstName, age:p|$p.age])");
            assertNotNull(result);
            assertEquals(1, result.rows().size(), "Only Jane (28)");
        }

        @Test
        @DisplayName("filter with less-than-or-equal")
        void testFilterLte() throws SQLException {
            var result = executeRelation(
                    "Person.all()->filter(p|$p.age <= 28)->project(~[firstName:p|$p.firstName, age:p|$p.age])");
            assertNotNull(result);
            assertEquals(1, result.rows().size(), "Only Jane (28)");
        }

        @Test
        @DisplayName("filter with greater-than-or-equal")
        void testFilterGte() throws SQLException {
            var result = executeRelation(
                    "Person.all()->filter(p|$p.age >= 30)->project(~[firstName:p|$p.firstName, age:p|$p.age])");
            assertNotNull(result);
            assertEquals(2, result.rows().size(), "John (30) and Bob (45)");
        }
    }

    // ==================== Project (canonical ~[] syntax) ====================

    @Nested
    @DisplayName("project(~[...])")
    class ProjectSpec {

        @Test
        @DisplayName("project single column")
        void testProjectSingleColumn() throws SQLException {
            var result = executeRelation(
                    "Person.all()->project(~[name:p|$p.firstName])");
            assertNotNull(result);
            assertEquals(3, result.rows().size(), "All 3 persons");
            assertEquals(1, result.columns().size(), "Single column");
            assertEquals("name", result.columns().get(0).name());
        }

        @Test
        @DisplayName("project multiple columns")
        void testProjectMultipleColumns() throws SQLException {
            var result = executeRelation(
                    "Person.all()->project(~[fn:p|$p.firstName, ln:p|$p.lastName, age:p|$p.age])");
            assertNotNull(result);
            assertEquals(3, result.rows().size());
            assertEquals(3, result.columns().size());
            assertEquals("fn", result.columns().get(0).name());
            assertEquals("ln", result.columns().get(1).name());
            assertEquals("age", result.columns().get(2).name());
        }

        @Test
        @DisplayName("project with custom aliases")
        void testProjectAliases() throws SQLException {
            var result = executeRelation(
                    "Person.all()->project(~[givenName:p|$p.firstName, familyName:p|$p.lastName])");
            assertNotNull(result);
            assertEquals("givenName", result.columns().get(0).name());
            assertEquals("familyName", result.columns().get(1).name());
        }
    }

    // ==================== Filter-Project chains ====================

    @Nested
    @DisplayName("filter→project chains")
    class FilterProjectChains {

        @Test
        @DisplayName("filter then project (standard pattern)")
        void testFilterThenProject() throws SQLException {
            var result = executeRelation(
                    "Person.all()->filter(p|$p.age > 28)->project(~[name:p|$p.firstName, age:p|$p.age])");
            assertNotNull(result);
            assertEquals(2, result.rows().size(), "John(30) and Bob(45)");
        }

        @Test
        @DisplayName("filter-project-filter chain")
        void testFilterProjectFilter() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                      ->filter(p|$p.age > 25)
                      ->project(~[name:p|$p.firstName, age:p|$p.age])
                      ->filter(x|$x.age < 35)""");
            assertNotNull(result);
            assertEquals(2, result.rows().size(), "John(30) and Jane(28), not Bob(45)");
        }

        @Test
        @DisplayName("chained filter-filter-project")
        void testFilterFilterProject() throws SQLException {
            var result = executeRelation("""
                    Person.all()
                      ->filter(p|$p.lastName == 'Smith')
                      ->filter(p|$p.age >= 30)
                      ->project(~[fn:p|$p.firstName])""");
            assertNotNull(result);
            assertEquals(1, result.rows().size(), "Only John Smith");
        }
    }

    // ==================== Association filter ====================

    @Nested
    @DisplayName("filter on associations")
    class AssociationFilter {

        @Test
        @DisplayName("filter via association property (multi-hop → EXISTS)")
        void testFilterViaAssociation() throws SQLException {
            // $p.addresses.city navigates through association → EXISTS subquery
            var result = executeRelation(
                    "Person.all()->filter(p|$p.addresses.city == 'New York')->project(~[name:p|$p.firstName])");
            assertNotNull(result);
            assertEquals(1, result.rows().size(), "Only John has a New York address");
        }

        @Test
        @DisplayName("filter via association with contains")
        void testFilterViaAssociationContains() throws SQLException {
            // Navigate through association, apply string contains on target column
            var result = executeRelation(
                    "Person.all()->filter(p|$p.addresses.street->contains('Main'))->project(~[name:p|$p.firstName])");
            assertNotNull(result);
            assertEquals(2, result.rows().size(), "John (123 Main St) and Jane (789 Main Rd)");
        }
    }

    // ==================== TDS Relation filter ====================

    @Nested
    @DisplayName("#TDS->filter()")
    class TdsFilter {

        @Test
        @DisplayName("filter by column value")
        void testFilterByColumn() throws SQLException {
            var result = executeRelation(
                    "|#TDS\nval, name\n1, Alice\n3, Bob\n4, Charlie\n5, Dave\n#->filter(x|$x.val > 3)");
            assertNotNull(result);
            assertEquals(2, result.rows().size(), "val > 3: Charlie(4), Dave(5)");
        }

        @Test
        @DisplayName("filter by string column")
        void testFilterByStringColumn() throws SQLException {
            var result = executeRelation(
                    "|#TDS\nval, name\n1, Alice\n3, Bob\n4, Charlie\n#->filter(x|$x.name == 'Bob')");
            assertNotNull(result);
            assertEquals(1, result.rows().size());
        }

        @Test
        @DisplayName("filter no match on TDS")
        void testFilterNoMatch() throws SQLException {
            var result = executeRelation(
                    "|#TDS\nval\n1\n3\n4\n#->filter(x|$x.val > 100)");
            assertNotNull(result);
            assertTrue(result.rows().isEmpty());
        }

        @Test
        @DisplayName("extend then filter (PCT pattern)")
        void testExtendThenFilter() throws SQLException {
            var result = executeRelation("""
                    #TDS
                    val, str
                    1, a
                    3, ewe
                    4, qw
                    5, wwe
                    6, weq
                    #
                    ->extend(~newCol: x | $x.str + $x.val->toString())
                    ->filter(x | $x.newCol == 'qw4')
                    """);
            assertNotNull(result);
            assertEquals(1, result.rows().size());
        }
    }

    // ==================== Scalar collection filter ====================

    @Nested
    @DisplayName("[...]->filter()")
    class CollectionFilter {

        @Test
        @DisplayName("filter integer list")
        void testFilterIntegers() throws SQLException {
            var result = executeRelation("|[1, 2, 3, 4, 5]->filter(x|$x > 3)");
            assertNotNull(result);
            assertEquals(2, result.rows().size(), "4 and 5");
        }

        @Test
        @DisplayName("filter string list")
        void testFilterStrings() throws SQLException {
            var result = executeRelation("|['alice', 'bob', 'charlie']->filter(x|$x == 'bob')");
            assertNotNull(result);
            assertEquals(1, result.rows().size());
        }

        @Test
        @DisplayName("filter empty result")
        void testFilterNoMatch() throws SQLException {
            var result = executeRelation("|[1, 2, 3]->filter(x|$x > 100)");
            assertNotNull(result);
            assertTrue(result.rows().isEmpty());
        }

        @Test
        @DisplayName("filter single element list")
        void testFilterSingleElement() throws SQLException {
            var result = executeRelation("|[42]->filter(x|$x == 42)");
            assertNotNull(result);
            assertEquals(1, result.rows().size());
        }
    }
}
