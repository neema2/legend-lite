package org.finos.legend.engine.test;

import org.finos.legend.engine.execution.BufferedResult;
import org.finos.legend.engine.plan.*;
import org.finos.legend.engine.server.QueryService;
import org.finos.legend.engine.store.*;
import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLGenerator;
import org.finos.legend.pure.dsl.*;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for window function capability in the Relation API.
 * 
 * Tests cover:
 * - Parser: parsing window function syntax
 * - SQL Generator: generating OVER (PARTITION BY ... ORDER BY ...) SQL
 * - Integration: executing window functions through QueryService against DuckDB
 */
class WindowFunctionTest {

    // ==================== Parser Tests ====================

    @Nested
    @DisplayName("Parser Tests")
    class ParserTests {

        @Test
        @DisplayName("Parse row_number without partition")
        void testParseRowNumberNoPartition() {
            String pure = """
                    Person.all()
                        ->project({e | $e.name}, {e | $e.department})
                        ->extend(~rowNum : row_number()->over())
                    """;

            PureExpression ast = PureParser.parse(pure);

            // New parser returns ExtendExpression with MethodCall
            assertInstanceOf(ExtendExpression.class, ast);
            ExtendExpression extend = (ExtendExpression) ast;

            assertEquals(1, extend.columns().size());
            assertInstanceOf(MethodCall.class, extend.columns().get(0));
            MethodCall overCall = (MethodCall) extend.columns().get(0);
            assertEquals("over", overCall.methodName());
        }

        @Test
        @DisplayName("Parse rank with partition")
        void testParseRankWithPartition() {
            String pure = """
                    Person.all()
                        ->project({e | $e.name}, {e | $e.department})
                        ->extend(~rank : rank()->over(~department))
                    """;

            PureExpression ast = PureParser.parse(pure);

            assertInstanceOf(ExtendExpression.class, ast);
            ExtendExpression extend = (ExtendExpression) ast;

            assertFalse(extend.columns().isEmpty());
        }

        @Test
        @DisplayName("Parse rank with partition and order")
        void testParseRankWithPartitionAndOrder() {
            String pure = """
                    Person.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(~rank : rank()->over(~department, ~salary->desc()))
                    """;

            PureExpression ast = PureParser.parse(pure);

            assertInstanceOf(ExtendExpression.class, ast);
            ExtendExpression extend = (ExtendExpression) ast;

            // Check that over() call has arguments
            MethodCall overCall = (MethodCall) extend.columns().get(0);
            assertFalse(overCall.arguments().isEmpty());
        }

        @Test
        @DisplayName("Parse sum aggregate window function")
        void testParseSumAggregateWindow() {
            String pure = """
                    Person.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department, ~salary->ascending()), ~runningTotal:{p,w,r|$r.salary}:y|$y->plus())
                    """;

            PureExpression ast = PureParser.parse(pure);

            assertInstanceOf(ExtendExpression.class, ast);
            ExtendExpression extend = (ExtendExpression) ast;
            assertFalse(extend.columns().isEmpty());
        }

        @Test
        @DisplayName("Parse dense_rank window function")
        void testParseDenseRank() {
            String pure = """
                    Person.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(~denseRank : dense_rank()->over(~department, ~salary->desc()))
                    """;

            PureExpression ast = PureParser.parse(pure);

            assertInstanceOf(ExtendExpression.class, ast);
            ExtendExpression extend = (ExtendExpression) ast;
            assertEquals(1, extend.columns().size());
        }

        @Test
        @DisplayName("Parse window function with frame (rows)")
        void testParseWindowWithRowsFrame() {
            String pure = """
                    Person.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department, ~salary->ascending(), rows(unbounded(), 0)), ~runningSum:{p,w,r|$r.salary}:y|$y->plus())
                    """;

            PureExpression ast = PureParser.parse(pure);

            assertInstanceOf(ExtendExpression.class, ast);
        }

        @Test
        @DisplayName("Parse window function with frame (range)")
        void testParseWindowWithRangeFrame() {
            String pure = """
                    Person.all()
                        ->project({e | $e.name}, {e | $e.salary})
                        ->extend(over(~salary->ascending(), range(unbounded(), unbounded())), ~total:{p,w,r|$r.salary}:y|$y->plus())
                    """;

            PureExpression ast = PureParser.parse(pure);

            assertInstanceOf(ExtendExpression.class, ast);
        }

        @Test
        @DisplayName("Parse window function with numeric frame bounds")
        void testParseWindowWithNumericBounds() {
            String pure = """
                    Person.all()
                        ->project({e | $e.name}, {e | $e.salary})
                        ->extend(over(~salary->ascending(), rows(-3, 0)), ~movingAvg:{p,w,r|$r.salary}:y|$y->average())
                    """;

            PureExpression ast = PureParser.parse(pure);

            assertInstanceOf(ExtendExpression.class, ast);
        }
    }

    // ==================== SQL Generator Tests ====================

    @Nested
    @DisplayName("SQL Generator Tests")
    class SQLGeneratorTests {

        @Test
        @DisplayName("Generate ROW_NUMBER SQL")
        void testGenerateRowNumber() {
            TableNode table = new TableNode(
                    new Table("T_EMPLOYEE", List.of()), "t0");
            ProjectNode project = new ProjectNode(table, List.of(
                    Projection.column("t0", "NAME", "name"),
                    Projection.column("t0", "DEPARTMENT", "department")));

            WindowExpression window = WindowExpression.ranking(
                    WindowExpression.WindowFunction.ROW_NUMBER,
                    List.of("department"),
                    List.of());

            ExtendNode extend = new ExtendNode(project, List.of(
                    new ExtendNode.WindowProjection("rowNum", window)));

            SQLGenerator generator = new SQLGenerator(DuckDBDialect.INSTANCE);
            String sql = generator.generate(extend);

            assertTrue(sql.contains("ROW_NUMBER()"), "Should contain ROW_NUMBER()");
            assertTrue(sql.contains("OVER"), "Should contain OVER");
            assertTrue(sql.contains("PARTITION BY"), "Should contain PARTITION BY");
            assertTrue(sql.contains("\"rowNum\""), "Should alias as rowNum");
        }

        @Test
        @DisplayName("Generate RANK with ORDER BY SQL")
        void testGenerateRankWithOrder() {
            TableNode table = new TableNode(
                    new Table("T_EMPLOYEE", List.of()), "t0");
            ProjectNode project = new ProjectNode(table, List.of(
                    Projection.column("t0", "NAME", "name"),
                    Projection.column("t0", "DEPARTMENT", "department"),
                    Projection.column("t0", "SALARY", "salary")));

            WindowExpression window = WindowExpression.ranking(
                    WindowExpression.WindowFunction.RANK,
                    List.of("department"),
                    List.of(new WindowExpression.SortSpec("salary", WindowExpression.SortDirection.DESC)));

            ExtendNode extend = new ExtendNode(project, List.of(
                    new ExtendNode.WindowProjection("rank", window)));

            SQLGenerator generator = new SQLGenerator(DuckDBDialect.INSTANCE);
            String sql = generator.generate(extend);

            assertTrue(sql.contains("RANK()"), "Should contain RANK()");
            assertTrue(sql.contains("PARTITION BY"), "Should contain PARTITION BY");
            assertTrue(sql.contains("ORDER BY"), "Should contain ORDER BY");
            assertTrue(sql.contains("DESC"), "Should contain DESC");
        }

        @Test
        @DisplayName("Generate SUM aggregate window function")
        void testGenerateSumWindow() {
            TableNode table = new TableNode(
                    new Table("T_EMPLOYEE", List.of()), "t0");
            ProjectNode project = new ProjectNode(table, List.of(
                    Projection.column("t0", "NAME", "name"),
                    Projection.column("t0", "DEPARTMENT", "department"),
                    Projection.column("t0", "SALARY", "salary")));

            WindowExpression window = WindowExpression.aggregate(
                    WindowExpression.WindowFunction.SUM,
                    "salary",
                    List.of("department"),
                    List.of(new WindowExpression.SortSpec("salary", WindowExpression.SortDirection.ASC)));

            ExtendNode extend = new ExtendNode(project, List.of(
                    new ExtendNode.WindowProjection("runningTotal", window)));

            SQLGenerator generator = new SQLGenerator(DuckDBDialect.INSTANCE);
            String sql = generator.generate(extend);

            assertTrue(sql.contains("SUM(\"salary\")"), "Should contain SUM(\"salary\")");
            assertTrue(sql.contains("OVER"), "Should contain OVER");
            assertTrue(sql.contains("PARTITION BY"), "Should contain PARTITION BY");
            assertTrue(sql.contains("ORDER BY"), "Should contain ORDER BY");
        }

        @Test
        @DisplayName("Generate window function without partition")
        void testGenerateWindowWithoutPartition() {
            TableNode table = new TableNode(
                    new Table("T_EMPLOYEE", List.of()), "t0");

            WindowExpression window = WindowExpression.ranking(
                    WindowExpression.WindowFunction.ROW_NUMBER,
                    List.of(), // No partition
                    List.of(new WindowExpression.SortSpec("name", WindowExpression.SortDirection.ASC)));

            ExtendNode extend = new ExtendNode(table, List.of(
                    new ExtendNode.WindowProjection("rowNum", window)));

            SQLGenerator generator = new SQLGenerator(DuckDBDialect.INSTANCE);
            String sql = generator.generate(extend);

            assertTrue(sql.contains("ROW_NUMBER()"), "Should contain ROW_NUMBER()");
            assertTrue(sql.contains("OVER (ORDER BY"), "Should have ORDER BY without PARTITION BY");
            assertFalse(sql.contains("PARTITION BY"), "Should not have PARTITION BY");
        }

        @Test
        @DisplayName("Generate window function with ROWS frame")
        void testGenerateWindowWithRowsFrame() {
            TableNode table = new TableNode(
                    new Table("T_EMPLOYEE", List.of()), "t0");
            ProjectNode project = new ProjectNode(table, List.of(
                    Projection.column("t0", "DEPARTMENT", "department"),
                    Projection.column("t0", "SALARY", "salary")));

            WindowExpression.FrameSpec frame = WindowExpression.FrameSpec.rows(
                    WindowExpression.FrameBound.unbounded(),
                    WindowExpression.FrameBound.currentRow());

            WindowExpression window = WindowExpression.aggregate(
                    WindowExpression.WindowFunction.SUM,
                    "salary",
                    List.of("department"),
                    List.of(new WindowExpression.SortSpec("salary", WindowExpression.SortDirection.ASC)),
                    frame);

            ExtendNode extend = new ExtendNode(project, List.of(
                    new ExtendNode.WindowProjection("runningTotal", window)));

            SQLGenerator generator = new SQLGenerator(DuckDBDialect.INSTANCE);
            String sql = generator.generate(extend);

            assertTrue(sql.contains("ROWS BETWEEN"), "Should contain ROWS BETWEEN");
            assertTrue(sql.contains("UNBOUNDED PRECEDING"), "Should have UNBOUNDED PRECEDING");
            assertTrue(sql.contains("CURRENT ROW"), "Should have CURRENT ROW");
        }

        @Test
        @DisplayName("Generate window function with numeric frame bounds")
        void testGenerateWindowWithNumericBounds() {
            TableNode table = new TableNode(
                    new Table("T_DATA", List.of()), "t0");

            WindowExpression.FrameSpec frame = WindowExpression.FrameSpec.rows(
                    WindowExpression.FrameBound.preceding(3),
                    WindowExpression.FrameBound.following(1));

            WindowExpression window = WindowExpression.aggregate(
                    WindowExpression.WindowFunction.AVG,
                    "value",
                    List.of(),
                    List.of(new WindowExpression.SortSpec("date", WindowExpression.SortDirection.ASC)),
                    frame);

            ExtendNode extend = new ExtendNode(table, List.of(
                    new ExtendNode.WindowProjection("movingAvg", window)));

            SQLGenerator generator = new SQLGenerator(DuckDBDialect.INSTANCE);
            String sql = generator.generate(extend);

            assertTrue(sql.contains("ROWS BETWEEN"), "Should contain ROWS BETWEEN");
            assertTrue(sql.contains("3 PRECEDING"), "Should have 3 PRECEDING");
            assertTrue(sql.contains("1 FOLLOWING"), "Should have 1 FOLLOWING");
        }
    }

    // ==================== Integration Tests ====================

    @Nested
    @DisplayName("Integration Tests - Full QueryService Flow")
    class IntegrationTests {

        private Connection connection;
        private QueryService queryService;

        // Complete Pure model for Employee tests - following project patterns
        private static final String COMPLETE_MODEL = """
                Class model::Employee
                {
                    name: String[1];
                    department: String[1];
                    salary: Integer[1];
                }

                Database store::EmployeeDatabase
                (
                    Table T_EMPLOYEE
                    (
                        ID INTEGER PRIMARY KEY,
                        NAME VARCHAR(100) NOT NULL,
                        DEPARTMENT VARCHAR(50) NOT NULL,
                        SALARY INTEGER NOT NULL
                    )
                )

                Mapping model::EmployeeMapping
                (
                    Employee: Relational
                    {
                        ~mainTable [EmployeeDatabase] T_EMPLOYEE
                        name: [EmployeeDatabase] T_EMPLOYEE.NAME,
                        department: [EmployeeDatabase] T_EMPLOYEE.DEPARTMENT,
                        salary: [EmployeeDatabase] T_EMPLOYEE.SALARY
                    }
                )

                RelationalDatabaseConnection store::TestConnection
                {
                    type: DuckDB;
                    specification: InMemory { };
                    auth: NoAuth { };
                }

                Runtime test::TestRuntime
                {
                    mappings: [ model::EmployeeMapping ];
                    connections: [ store::EmployeeDatabase: store::TestConnection ];
                }
                """;

        @BeforeEach
        void setUp() throws Exception {
            connection = DriverManager.getConnection("jdbc:duckdb:");
            queryService = new QueryService();

            // Create table and insert test data
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("""
                            CREATE TABLE T_EMPLOYEE (
                                ID INTEGER PRIMARY KEY,
                                NAME VARCHAR(100),
                                DEPARTMENT VARCHAR(50),
                                SALARY INTEGER
                            )
                        """);

                // Insert test data with clear partition/ranking scenarios
                stmt.execute("INSERT INTO T_EMPLOYEE VALUES (1, 'Alice', 'Engineering', 100000)");
                stmt.execute("INSERT INTO T_EMPLOYEE VALUES (2, 'Bob', 'Engineering', 90000)");
                stmt.execute("INSERT INTO T_EMPLOYEE VALUES (3, 'Charlie', 'Engineering', 80000)");
                stmt.execute("INSERT INTO T_EMPLOYEE VALUES (4, 'Diana', 'Sales', 85000)");
                stmt.execute("INSERT INTO T_EMPLOYEE VALUES (5, 'Eve', 'Sales', 75000)");
                stmt.execute("INSERT INTO T_EMPLOYEE VALUES (6, 'Frank', 'Marketing', 70000)");
            }
        }

        @AfterEach
        void tearDown() throws Exception {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }

        private BufferedResult executeQuery(String pureQuery) throws SQLException {
            return queryService.execute(
                    COMPLETE_MODEL,
                    pureQuery,
                    "test::TestRuntime",
                    connection);
        }

        private String generateSql(String pureQuery) {
            var plan = queryService.compile(
                    COMPLETE_MODEL,
                    pureQuery,
                    "test::TestRuntime");
            return plan.sqlByStore().values().iterator().next().sql();
        }

        @Test
        @DisplayName("Execute ROW_NUMBER via Pure -> QueryService -> DuckDB")
        void testExecuteRowNumberViaPure() throws Exception {
            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(~rowNum : row_number()->over(~department, ~salary->desc()))
                    """;

            // Verify SQL generation
            String sql = generateSql(pureQuery);
            assertTrue(sql.contains("ROW_NUMBER()"), "SQL should contain ROW_NUMBER()");
            assertTrue(sql.contains("OVER"), "SQL should contain OVER");
            assertTrue(sql.contains("PARTITION BY"), "SQL should contain PARTITION BY");

            // Execute and verify results
            BufferedResult result = executeQuery(pureQuery);

            assertEquals(6, result.rowCount(), "Should have 6 employees");

            // Verify row number column exists
            assertTrue(result.columns().stream()
                    .anyMatch(c -> c.name().equals("rowNum")),
                    "Should have rowNum column");

            // Find Engineering employees and verify row numbers
            int engineeringRowNums = 0;
            for (var row : result.rows()) {
                int deptIdx = findColumnIndex(result, "department");
                int rowNumIdx = findColumnIndex(result, "rowNum");

                if ("Engineering".equals(row.get(deptIdx))) {
                    int rowNum = ((Number) row.get(rowNumIdx)).intValue();
                    assertTrue(rowNum >= 1 && rowNum <= 3, "Engineering rowNum should be 1-3");
                    engineeringRowNums++;
                }
            }
            assertEquals(3, engineeringRowNums, "Should have 3 Engineering employees");
        }

        @Test
        @DisplayName("Execute RANK via Pure -> QueryService -> DuckDB")
        void testExecuteRankViaPure() throws Exception {
            // First add an employee with same salary to test RANK ties
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO T_EMPLOYEE VALUES (7, 'Grace', 'Engineering', 90000)");
            }

            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(~salaryRank : rank()->over(~department, ~salary->desc()))
                    """;

            BufferedResult result = executeQuery(pureQuery);

            // Count ranks for Engineering (Alice=1, Bob=2, Grace=2, Charlie=4)
            int foundRankOf2 = 0;
            int foundRankOf4 = 0;

            for (var row : result.rows()) {
                int deptIdx = findColumnIndex(result, "department");
                int rankIdx = findColumnIndex(result, "salaryRank");
                int nameIdx = findColumnIndex(result, "name");

                if ("Engineering".equals(row.get(deptIdx))) {
                    int rank = ((Number) row.get(rankIdx)).intValue();
                    String name = (String) row.get(nameIdx);

                    if ("Alice".equals(name)) {
                        assertEquals(1, rank, "Alice should be rank 1 (highest salary)");
                    } else if ("Charlie".equals(name)) {
                        assertEquals(4, rank, "Charlie should be rank 4 (skipped 3 due to tie)");
                        foundRankOf4++;
                    } else if (rank == 2) {
                        foundRankOf2++;
                    }
                }
            }

            assertEquals(2, foundRankOf2, "Should have 2 employees with rank 2 (Bob and Grace tie)");
            assertEquals(1, foundRankOf4, "Should have 1 employee with rank 4");
        }

        @Test
        @DisplayName("Execute SUM running total via Pure -> QueryService -> DuckDB")
        void testExecuteRunningSumViaPure() throws Exception {
            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department, ~salary->ascending()), ~runningTotal:{p,w,r|$r.salary}:y|$y->plus())
                    """;

            BufferedResult result = executeQuery(pureQuery);

            // Verify running totals for Engineering (sorted by salary ASC):
            // Charlie(80k) -> 80k, Bob(90k) -> 170k, Alice(100k) -> 270k
            for (var row : result.rows()) {
                int deptIdx = findColumnIndex(result, "department");
                int nameIdx = findColumnIndex(result, "name");
                int runningIdx = findColumnIndex(result, "runningTotal");

                if ("Engineering".equals(row.get(deptIdx))) {
                    String name = (String) row.get(nameIdx);
                    long runningTotal = ((Number) row.get(runningIdx)).longValue();

                    switch (name) {
                        case "Charlie" -> assertEquals(80000, runningTotal,
                                "Charlie's running total (first in order)");
                        case "Bob" -> assertEquals(170000, runningTotal,
                                "Bob's running total (80k + 90k)");
                        case "Alice" -> assertEquals(270000, runningTotal,
                                "Alice's running total (80k + 90k + 100k)");
                    }
                }
            }
        }

        @Test
        @DisplayName("Execute DENSE_RANK via Pure -> QueryService -> DuckDB")
        void testExecuteDenseRankViaPure() throws Exception {
            // Add an employee with same salary
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO T_EMPLOYEE VALUES (7, 'Grace', 'Engineering', 90000)");
            }

            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(~denseRank : dense_rank()->over(~department, ~salary->desc()))
                    """;

            BufferedResult result = executeQuery(pureQuery);

            // Verify dense ranks for Engineering: Alice=1, Bob=2, Grace=2, Charlie=3 (no
            // gap!)
            for (var row : result.rows()) {
                int deptIdx = findColumnIndex(result, "department");
                int nameIdx = findColumnIndex(result, "name");
                int rankIdx = findColumnIndex(result, "denseRank");

                if ("Engineering".equals(row.get(deptIdx))) {
                    String name = (String) row.get(nameIdx);
                    int denseRank = ((Number) row.get(rankIdx)).intValue();

                    if ("Charlie".equals(name)) {
                        assertEquals(3, denseRank,
                                "Charlie should be dense_rank 3 (no gap unlike RANK)");
                    }
                }
            }
        }

        @Test
        @DisplayName("Execute SUM with ROWS frame via Pure -> QueryService -> DuckDB")
        void testExecuteSumWithRowsFrameViaPure() throws Exception {
            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department, ~salary->ascending(), rows(unbounded(), 0)), ~runningSum:{p,w,r|$r.salary}:y|$y->plus())
                    """;

            // Verify SQL generation includes frame
            String sql = generateSql(pureQuery);
            System.out.println("Generated SQL with frame: " + sql);
            assertTrue(sql.contains("ROWS BETWEEN"), "SQL should contain ROWS BETWEEN");
            assertTrue(sql.contains("UNBOUNDED PRECEDING"), "SQL should contain UNBOUNDED PRECEDING");

            // Execute and verify results
            BufferedResult result = executeQuery(pureQuery);

            assertEquals(6, result.rowCount(), "Should have 6 employees");

            // Verify running sum column exists
            assertTrue(result.columns().stream()
                    .anyMatch(c -> c.name().equals("runningSum")),
                    "Should have runningSum column");
        }

        private int findColumnIndex(BufferedResult result, String columnName) {
            for (int i = 0; i < result.columns().size(); i++) {
                if (result.columns().get(i).name().equals(columnName)) {
                    return i;
                }
            }
            throw new IllegalArgumentException("Column not found: " + columnName);
        }
    }
}
