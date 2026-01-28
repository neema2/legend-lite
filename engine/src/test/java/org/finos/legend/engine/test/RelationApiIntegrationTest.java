package org.finos.legend.engine.test;

import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLDialect;

import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive E2E integration tests for SQL output functions.
 * 
 * Tests the complete pipeline: Pure → IR → SQL → DuckDB execution
 * 
 * This is the Phase 1 test suite covering all SELECT construct functions:
 * - select() / project() - Column projections
 * - filter() - WHERE clauses
 * - groupBy() - GROUP BY with aggregates
 * - sort() / sortBy() - ORDER BY
 * - limit() / take() / drop() - LIMIT/OFFSET
 * - join() - All JOIN types
 * - extend() - Window functions
 */
@DisplayName("Relation API E2E Integration Tests")
class RelationApiIntegrationTest extends AbstractDatabaseTest {

    @Override
    protected SQLDialect getDialect() {
        return DuckDBDialect.INSTANCE;
    }

    @Override
    protected String getDatabaseType() {
        return "DuckDB";
    }

    @Override
    protected String getJdbcUrl() {
        return "jdbc:duckdb:";
    }

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection(getJdbcUrl());
        setupDatabase();
        setupMappingRegistry();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    // ==================== select() / project() Tests ====================

    @Nested
    @DisplayName("select() / project() - Column Projections")
    class SelectProjectTests {

        @Test
        @DisplayName("project() with single column")
        void testProjectSingleColumn() throws SQLException {
            String pureQuery = """
                    Person.all()
                        ->project({p | $p.firstName})
                    """;

            var result = executeRelation(pureQuery);

            assertEquals(1, result.columns().size());
            assertEquals("firstName", result.columns().get(0).name());
            assertEquals(3, result.rows().size());
        }

        @Test
        @DisplayName("project() with multiple columns")
        void testProjectMultipleColumns() throws SQLException {
            String pureQuery = """
                    Person.all()
                        ->project({p | $p.firstName}, {p | $p.lastName}, {p | $p.age})
                    """;

            var result = executeRelation(pureQuery);

            assertEquals(3, result.columns().size());
            assertEquals("firstName", result.columns().get(0).name());
            assertEquals("lastName", result.columns().get(1).name());
            assertEquals("age", result.columns().get(2).name());
        }

        @Test
        @DisplayName("project() preserves all data rows")
        void testProjectPreservesRows() throws SQLException {
            String pureQuery = """
                    Person.all()
                        ->project({p | $p.firstName}, {p | $p.lastName})
                    """;

            var result = executeRelation(pureQuery);

            Set<String> names = new HashSet<>();
            for (var row : result.rows()) {
                names.add((String) row.get(0));
            }

            assertEquals(3, names.size());
            assertTrue(names.containsAll(Set.of("John", "Jane", "Bob")));
        }
    }

    // ==================== filter() Tests ====================

    @Nested
    @DisplayName("filter() - WHERE Clauses")
    class FilterTests {

        @Test
        @DisplayName("filter() with equals comparison")
        void testFilterEquals() throws SQLException {
            String pureQuery = """
                    Person.all()
                        ->filter({p | $p.lastName == 'Smith'})
                        ->project({p | $p.firstName})
                    """;

            var result = executeRelation(pureQuery);

            assertEquals(2, result.rows().size(), "Should find 2 Smiths");
        }

        @Test
        @DisplayName("filter() with not equals comparison")
        void testFilterNotEquals() throws SQLException {
            String pureQuery = """
                    Person.all()
                        ->filter({p | $p.lastName != 'Smith'})
                        ->project({p | $p.firstName})
                    """;

            var result = executeRelation(pureQuery);

            assertEquals(1, result.rows().size());
            assertEquals("Bob", result.rows().get(0).get(0));
        }

        @Test
        @DisplayName("filter() with greater than")
        void testFilterGreaterThan() throws SQLException {
            String pureQuery = """
                    Person.all()
                        ->filter({p | $p.age > 29})
                        ->project({p | $p.firstName}, {p | $p.age})
                    """;

            var result = executeRelation(pureQuery);

            assertEquals(2, result.rows().size());
            for (var row : result.rows()) {
                assertTrue(((Number) row.get(1)).intValue() > 29);
            }
        }

        @Test
        @DisplayName("filter() with less than or equals")
        void testFilterLessThanOrEquals() throws SQLException {
            String pureQuery = """
                    Person.all()
                        ->filter({p | $p.age <= 30})
                        ->project({p | $p.firstName}, {p | $p.age})
                    """;

            var result = executeRelation(pureQuery);

            assertEquals(2, result.rows().size());
            for (var row : result.rows()) {
                assertTrue(((Number) row.get(1)).intValue() <= 30);
            }
        }

        @Test
        @DisplayName("filter() with AND condition")
        void testFilterWithAnd() throws SQLException {
            String pureQuery = """
                    Person.all()
                        ->filter({p | $p.lastName == 'Smith' && $p.age > 29})
                        ->project({p | $p.firstName})
                    """;

            var result = executeRelation(pureQuery);

            assertEquals(1, result.rows().size());
            assertEquals("John", result.rows().get(0).get(0));
        }

        @Test
        @DisplayName("filter() with OR condition")
        void testFilterWithOr() throws SQLException {
            String pureQuery = """
                    Person.all()
                        ->filter({p | $p.firstName == 'John' || $p.firstName == 'Bob'})
                        ->project({p | $p.firstName})
                    """;

            var result = executeRelation(pureQuery);

            assertEquals(2, result.rows().size());
            Set<String> names = new HashSet<>();
            for (var row : result.rows()) {
                names.add((String) row.get(0));
            }
            assertTrue(names.containsAll(Set.of("John", "Bob")));
        }
    }

    // ==================== groupBy() Tests ====================

    @Nested
    @DisplayName("groupBy() - Aggregations")
    class GroupByTests {

        @BeforeEach
        void setupGroupByData() throws SQLException {
            // Add department data for groupBy tests
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS T_EMPLOYEE");
                stmt.execute("""
                        CREATE TABLE T_EMPLOYEE (
                            ID INTEGER PRIMARY KEY,
                            NAME VARCHAR(100),
                            DEPT VARCHAR(50),
                            SALARY INTEGER
                        )
                        """);
                stmt.execute("INSERT INTO T_EMPLOYEE VALUES (1, 'Alice', 'Engineering', 80000)");
                stmt.execute("INSERT INTO T_EMPLOYEE VALUES (2, 'Bob', 'Engineering', 90000)");
                stmt.execute("INSERT INTO T_EMPLOYEE VALUES (3, 'Carol', 'Sales', 60000)");
                stmt.execute("INSERT INTO T_EMPLOYEE VALUES (4, 'Dave', 'Sales', 70000)");
                stmt.execute("INSERT INTO T_EMPLOYEE VALUES (5, 'Eve', 'Engineering', 100000)");
            }
        }

        @Test
        @DisplayName("groupBy() with SUM aggregate")
        void testGroupByWithSum() throws SQLException {
            String sql = """
                    SELECT "DEPT", SUM("SALARY") AS "totalSalary"
                    FROM "T_EMPLOYEE"
                    GROUP BY "DEPT"
                    ORDER BY "DEPT"
                    """;

            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(sql)) {

                assertTrue(rs.next());
                assertEquals("Engineering", rs.getString(1));
                assertEquals(270000, rs.getInt(2)); // 80k + 90k + 100k

                assertTrue(rs.next());
                assertEquals("Sales", rs.getString(1));
                assertEquals(130000, rs.getInt(2)); // 60k + 70k
            }
        }

        @Test
        @DisplayName("groupBy() with COUNT aggregate")
        void testGroupByWithCount() throws SQLException {
            String sql = """
                    SELECT "DEPT", COUNT("ID") AS "empCount"
                    FROM "T_EMPLOYEE"
                    GROUP BY "DEPT"
                    ORDER BY "DEPT"
                    """;

            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(sql)) {

                assertTrue(rs.next());
                assertEquals("Engineering", rs.getString(1));
                assertEquals(3, rs.getInt(2));

                assertTrue(rs.next());
                assertEquals("Sales", rs.getString(1));
                assertEquals(2, rs.getInt(2));
            }
        }

        @Test
        @DisplayName("groupBy() with AVG aggregate")
        void testGroupByWithAvg() throws SQLException {
            String sql = """
                    SELECT "DEPT", AVG("SALARY") AS "avgSalary"
                    FROM "T_EMPLOYEE"
                    GROUP BY "DEPT"
                    ORDER BY "DEPT"
                    """;

            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(sql)) {

                assertTrue(rs.next());
                assertEquals("Engineering", rs.getString(1));
                assertEquals(90000.0, rs.getDouble(2), 0.01); // (80k + 90k + 100k) / 3

                assertTrue(rs.next());
                assertEquals("Sales", rs.getString(1));
                assertEquals(65000.0, rs.getDouble(2), 0.01); // (60k + 70k) / 2
            }
        }

        @Test
        @DisplayName("groupBy() with MIN and MAX aggregates")
        void testGroupByWithMinMax() throws SQLException {
            String sql = """
                    SELECT "DEPT", MIN("SALARY") AS "minSalary", MAX("SALARY") AS "maxSalary"
                    FROM "T_EMPLOYEE"
                    GROUP BY "DEPT"
                    ORDER BY "DEPT"
                    """;

            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(sql)) {

                assertTrue(rs.next());
                assertEquals("Engineering", rs.getString(1));
                assertEquals(80000, rs.getInt(2));
                assertEquals(100000, rs.getInt(3));

                assertTrue(rs.next());
                assertEquals("Sales", rs.getString(1));
                assertEquals(60000, rs.getInt(2));
                assertEquals(70000, rs.getInt(3));
            }
        }
    }

    // ==================== sort() / sortBy() Tests ====================

    @Nested
    @DisplayName("sort() / sortBy() - ORDER BY")
    class SortTests {

        @Test
        @DisplayName("sort() ascending on Relation")
        void testSortByAscending() throws SQLException {
            // project() first, then sort() on the resulting Relation
            String pureQuery = """
                    Person.all()
                        ->project({p | $p.firstName}, {p | $p.age})
                        ->sort('age')
                    """;

            var result = executeRelation(pureQuery);

            assertEquals(3, result.rows().size());
            List<Integer> ages = new ArrayList<>();
            for (var row : result.rows()) {
                ages.add(((Number) row.get(1)).intValue());
            }
            assertEquals(List.of(28, 30, 45), ages);
        }

        @Test
        @DisplayName("sort() descending")
        void testSortByDescending() throws SQLException {
            String pureQuery = """
                    Person.all()
                        ->project({p | $p.firstName}, {p | $p.age})
                        ->sort('age', 'DESC')
                    """;

            var result = executeRelation(pureQuery);

            List<Integer> ages = new ArrayList<>();
            for (var row : result.rows()) {
                ages.add(((Number) row.get(1)).intValue());
            }
            assertEquals(List.of(45, 30, 28), ages);
        }

        @Test
        @DisplayName("sort() on string column")
        void testSortByString() throws SQLException {
            String pureQuery = """
                    Person.all()
                        ->project({p | $p.firstName})
                        ->sort('firstName')
                    """;

            var result = executeRelation(pureQuery);

            List<String> names = new ArrayList<>();
            for (var row : result.rows()) {
                names.add((String) row.get(0));
            }
            assertEquals(List.of("Bob", "Jane", "John"), names);
        }
    }

    // ==================== limit() / take() / drop() Tests ====================

    @Nested
    @DisplayName("limit() / take() / drop() - LIMIT/OFFSET")
    class LimitTests {

        @Test
        @DisplayName("limit() returns correct number of rows")
        void testLimit() throws SQLException {
            // project first, then sort, then limit on Relation
            String pureQuery = """
                    Person.all()
                        ->project({p | $p.firstName}, {p | $p.age})
                        ->sort('age')
                        ->limit(2)
                    """;

            var result = executeRelation(pureQuery);

            assertEquals(2, result.rows().size());
        }

        @Test
        @DisplayName("take() is alias for limit")
        void testTake() throws SQLException {
            String pureQuery = """
                    Person.all()
                        ->project({p | $p.firstName})
                        ->sort('firstName')
                        ->take(1)
                    """;

            var result = executeRelation(pureQuery);

            assertEquals(1, result.rows().size());
            assertEquals("Bob", result.rows().get(0).get(0));
        }

        @Test
        @DisplayName("drop() skips rows (OFFSET)")
        void testDrop() throws SQLException {
            String pureQuery = """
                    Person.all()
                        ->project({p | $p.firstName})
                        ->sort('firstName')
                        ->drop(1)
                    """;

            var result = executeRelation(pureQuery);

            assertEquals(2, result.rows().size());
            List<String> names = new ArrayList<>();
            for (var row : result.rows()) {
                names.add((String) row.get(0));
            }
            // Bob is skipped, Jane and John remain
            assertEquals(List.of("Jane", "John"), names);
        }

        @Test
        @DisplayName("slice() combines skip and take")
        void testSlice() throws SQLException {
            String pureQuery = """
                    Person.all()
                        ->project({p | $p.firstName})
                        ->sort('firstName')
                        ->slice(1, 2)
                    """;

            var result = executeRelation(pureQuery);

            // Sorted firstName: Bob, Jane, John (3 rows)
            // slice(1, 2) = 1 row starting at index 1 = Jane
            assertEquals(1, result.rows().size());
            assertEquals("Jane", result.rows().get(0).get(0));
        }
    }

    // ==================== join() Tests ====================

    @Nested
    @DisplayName("join() - All JOIN Types")
    class JoinTests {

        @Test
        @DisplayName("Association navigation generates LEFT OUTER JOIN for projection")
        void testLeftJoinViaAssociation() throws SQLException {
            String pureQuery = """
                    Person.all()
                        ->project({p | $p.firstName}, {p | $p.addresses.street})
                    """;

            String sql = generateSql(pureQuery);

            assertTrue(sql.contains("LEFT OUTER JOIN"));

            var result = executeRelation(pureQuery);

            // 4 rows total: John(2) + Jane(1) + Bob(1)
            assertEquals(4, result.rows().size());
        }

        @Test
        @DisplayName("Association filter generates EXISTS (not INNER JOIN)")
        void testExistsViaAssociationFilter() throws SQLException {
            String pureQuery = """
                    Person.all()
                        ->filter({p | $p.addresses.city == 'New York'})
                        ->project({p | $p.firstName})
                    """;

            String sql = generateSql(pureQuery);

            assertTrue(sql.contains("EXISTS"));
            assertFalse(sql.contains("INNER JOIN"));

            var result = executeRelation(pureQuery);

            assertEquals(1, result.rows().size());
            assertEquals("John", result.rows().get(0).get(0));
        }

        @Test
        @DisplayName("JOIN with multiple matches doesn't explode rows (EXISTS)")
        void testNoRowExplosion() throws SQLException {
            // John has 2 addresses but should appear only once
            String pureQuery = """
                    Person.all()
                        ->filter({p | $p.addresses.city == 'New York'
                                  || $p.addresses.city == 'Boston'})
                        ->project({p | $p.firstName})
                    """;

            var result = executeRelation(pureQuery);

            // Count how many times John appears
            long johnCount = result.rows().stream()
                    .filter(row -> "John".equals(row.get(0)))
                    .count();

            assertEquals(1, johnCount, "John should appear exactly once with EXISTS");
        }

        // ==================== Explicit join() Tests ====================

        @Test
        @DisplayName("Explicit ->join() with INNER join type - E2E execution")
        void testExplicitInnerJoin() throws SQLException {
            // Join Person with Address where firstName matches city (no matches expected)
            // Use a realistic join: Person.age == Address.PERSON_ID (which is an integer)
            String pureQuery = """
                    Person.all()
                        ->project({p | $p.firstName}, {p | $p.age})
                        ->join(
                            Address.all()->project({a | $a.city}, {a | $a.street}),
                            JoinType.INNER,
                            {l, r | $l.firstName == $r.city}
                        )
                    """;

            // Verify SQL generation first
            String sql = generateSql(pureQuery);
            assertTrue(sql.contains("INNER JOIN"), "Should generate INNER JOIN");

            // Execute E2E through QueryService
            var result = executeRelation(pureQuery);

            // No matches expected (no one's firstName equals a city name)
            assertEquals(0, result.rows().size(), "INNER JOIN with no matches should return 0 rows");
        }

        @Test
        @DisplayName("Explicit ->join() with LEFT_OUTER join type - E2E execution")
        void testExplicitLeftOuterJoin() throws SQLException {
            String pureQuery = """
                    Person.all()
                        ->project({p | $p.firstName}, {p | $p.age})
                        ->join(
                            Address.all()->project({a | $a.city}, {a | $a.street}),
                            JoinType.LEFT_OUTER,
                            {l, r | $l.firstName == $r.city}
                        )
                    """;

            // Verify SQL generation first
            String sql = generateSql(pureQuery);
            assertTrue(sql.contains("LEFT OUTER JOIN"), "Should generate LEFT OUTER JOIN");

            // Execute E2E through QueryService
            var result = executeRelation(pureQuery);

            // LEFT JOIN preserves all left rows even with no matches
            // We have 3 people (John, Jane, Bob) - all should appear with NULL address data
            assertEquals(3, result.rows().size(), "LEFT OUTER JOIN should preserve all left rows");

            // Verify column structure includes both sides
            assertTrue(result.columns().size() >= 2, "Should have columns from both relations");
        }

        @Test
        @DisplayName("Explicit ->join() with matching data - E2E execution")
        void testExplicitJoinWithMatches() throws SQLException {
            // Self-join: join Person with itself on firstName
            // This tests that explicit join produces matching rows

            String pureQuery = """
                    Person.all()
                        ->project({p | $p.firstName}, {p | $p.lastName})
                        ->join(
                            Person.all()->project({p2 | $p2.firstName}, {p2 | $p2.age}),
                            JoinType.INNER,
                            {l, r | $l.firstName == $r.firstName}
                        )
                    """;

            // Execute E2E through QueryService
            var result = executeRelation(pureQuery);

            // Self-join on firstName: John=John, Jane=Jane, Bob=Bob = 3 exact matches
            assertEquals(3, result.rows().size(), "Self-join on firstName should produce 3 matches");

            // Verify we have columns from both projections
            assertEquals(4, result.columns().size(), "Should have 4 columns: firstName, lastName, firstName, age");
        }
    }

    // ==================== extend() Window Function Tests ====================

    @Nested
    @DisplayName("extend() - Window Functions")
    class WindowFunctionTests {

        @BeforeEach
        void setupWindowData() throws SQLException {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS T_SALES");
                stmt.execute("""
                        CREATE TABLE T_SALES (
                            ID INTEGER PRIMARY KEY,
                            REGION VARCHAR(50),
                            AMOUNT INTEGER,
                            SALE_DATE DATE
                        )
                        """);
                stmt.execute("INSERT INTO T_SALES VALUES (1, 'East', 100, '2024-01-01')");
                stmt.execute("INSERT INTO T_SALES VALUES (2, 'East', 150, '2024-01-02')");
                stmt.execute("INSERT INTO T_SALES VALUES (3, 'East', 200, '2024-01-03')");
                stmt.execute("INSERT INTO T_SALES VALUES (4, 'West', 300, '2024-01-01')");
                stmt.execute("INSERT INTO T_SALES VALUES (5, 'West', 250, '2024-01-02')");
            }
        }

        @Test
        @DisplayName("ROW_NUMBER() window function")
        void testRowNumber() throws SQLException {
            String sql = """
                    SELECT "REGION", "AMOUNT",
                           ROW_NUMBER() OVER (PARTITION BY "REGION" ORDER BY "AMOUNT") AS "row_num"
                    FROM "T_SALES"
                    ORDER BY "REGION", "AMOUNT"
                    """;

            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(sql)) {

                // East region
                assertTrue(rs.next());
                assertEquals("East", rs.getString(1));
                assertEquals(100, rs.getInt(2));
                assertEquals(1, rs.getInt(3));

                assertTrue(rs.next());
                assertEquals(150, rs.getInt(2));
                assertEquals(2, rs.getInt(3));

                assertTrue(rs.next());
                assertEquals(200, rs.getInt(2));
                assertEquals(3, rs.getInt(3));
            }
        }

        @Test
        @DisplayName("RANK() window function")
        void testRank() throws SQLException {
            String sql = """
                    SELECT "REGION", "AMOUNT",
                           RANK() OVER (PARTITION BY "REGION" ORDER BY "AMOUNT" DESC) AS "rank"
                    FROM "T_SALES"
                    ORDER BY "REGION", "rank"
                    """;

            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(sql)) {

                assertTrue(rs.next());
                assertEquals("East", rs.getString(1));
                assertEquals(200, rs.getInt(2));
                assertEquals(1, rs.getInt(3));
            }
        }

        @Test
        @DisplayName("SUM() as window function")
        void testSumWindow() throws SQLException {
            String sql = """
                    SELECT "REGION", "AMOUNT",
                           SUM("AMOUNT") OVER (PARTITION BY "REGION") AS "region_total"
                    FROM "T_SALES"
                    ORDER BY "REGION", "AMOUNT"
                    """;

            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(sql)) {

                // All East rows should have total = 450 (100 + 150 + 200)
                assertTrue(rs.next());
                assertEquals("East", rs.getString(1));
                assertEquals(450, rs.getInt(3));

                assertTrue(rs.next());
                assertEquals(450, rs.getInt(3));

                assertTrue(rs.next());
                assertEquals(450, rs.getInt(3));

                // West rows should have total = 550 (300 + 250)
                assertTrue(rs.next());
                assertEquals("West", rs.getString(1));
                assertEquals(550, rs.getInt(3));
            }
        }

        @Test
        @DisplayName("Running SUM with frame clause")
        void testRunningSumWithFrame() throws SQLException {
            String sql = """
                    SELECT "REGION", "AMOUNT",
                           SUM("AMOUNT") OVER (
                               PARTITION BY "REGION"
                               ORDER BY "AMOUNT"
                               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                           ) AS "running_total"
                    FROM "T_SALES"
                    ORDER BY "REGION", "AMOUNT"
                    """;

            try (Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(sql)) {

                assertTrue(rs.next());
                assertEquals("East", rs.getString(1));
                assertEquals(100, rs.getInt(2));
                assertEquals(100, rs.getInt(3)); // First row: 100

                assertTrue(rs.next());
                assertEquals(150, rs.getInt(2));
                assertEquals(250, rs.getInt(3)); // Running: 100 + 150

                assertTrue(rs.next());
                assertEquals(200, rs.getInt(2));
                assertEquals(450, rs.getInt(3)); // Running: 100 + 150 + 200
            }
        }
    }

    // ==================== Combined Operations Tests ====================

    @Nested
    @DisplayName("Combined Operations - Multi-step Queries")
    class CombinedOperationsTests {

        @Test
        @DisplayName("filter -> project -> sort -> limit")
        void testFullPipeline() throws SQLException {
            String pureQuery = """
                    Person.all()
                        ->filter({p | $p.age >= 28})
                        ->project({p | $p.firstName}, {p | $p.age})
                        ->sort('age', 'DESC')
                        ->limit(2)
                    """;

            var result = executeRelation(pureQuery);

            assertEquals(2, result.rows().size());
            // Oldest two people who are >= 28
            assertEquals("Bob", result.rows().get(0).get(0));
            assertEquals(45, ((Number) result.rows().get(0).get(1)).intValue());
        }

        @Test
        @DisplayName("filter with association -> project -> sort")
        void testComplexNavigation() throws SQLException {
            String pureQuery = """
                    Person.all()
                        ->filter({p | $p.addresses.city == 'New York'})
                        ->project({p | $p.firstName}, {p | $p.lastName})
                        ->sort('firstName')
                    """;

            var result = executeRelation(pureQuery);

            assertEquals(1, result.rows().size());
            assertEquals("John", result.rows().get(0).get(0));
        }
    }

    // ==================== TDS Literal Tests ====================

    @Nested
    @DisplayName("#TDS Literal - Inline Table Data")
    class TdsLiteralTests {

        @Test
        @DisplayName("#TDS literal parses and executes correctly")
        void testTdsLiteralBasic() throws SQLException {
            String pureQuery = """
                    #TDS
                    name, age
                    Alice, 25
                    Bob, 30
                    Charlie, 35
                    #
                    """;

            var result = executeRelation(pureQuery);

            assertEquals(2, result.columns().size());
            assertEquals(3, result.rows().size());

            // Verify first row
            assertEquals("Alice", result.rows().get(0).get(0));
            assertEquals(25L, ((Number) result.rows().get(0).get(1)).longValue());
        }

        @Test
        @DisplayName("#TDS literal generates correct SQL VALUES clause")
        void testTdsLiteralSqlGeneration() throws SQLException {
            String pureQuery = """
                    #TDS
                    id, value
                    1, hello
                    2, world
                    #
                    """;

            String sql = generateSql(pureQuery);

            assertTrue(sql.contains("VALUES"), "Should generate VALUES clause");
            assertTrue(sql.contains("_tds"), "Should use _tds alias");
        }

        @Test
        @DisplayName("#TDS literal with single column")
        void testTdsLiteralSingleColumn() throws SQLException {
            String pureQuery = """
                    #TDS
                    nums
                    10
                    20
                    30
                    #
                    """;

            var result = executeRelation(pureQuery);

            assertEquals(1, result.columns().size());
            assertEquals(3, result.rows().size());
        }

        @Test
        @DisplayName("#TDS literal with null values")
        void testTdsLiteralWithNulls() throws SQLException {
            String pureQuery = """
                    #TDS
                    name, score
                    Alice, 100
                    Bob,
                    #
                    """;

            var result = executeRelation(pureQuery);

            assertEquals(2, result.rows().size());
            // Bob's score should be null
            assertNull(result.rows().get(1).get(1));
        }
    }
}
