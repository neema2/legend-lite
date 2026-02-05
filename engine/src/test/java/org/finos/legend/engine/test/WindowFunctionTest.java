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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        @DisplayName("Parse row_number with typed window spec")
        void testParseRowNumberNoPartition() {
            // Legend-engine syntax: extend(over(...), ~col:{p,w,r|...window func...})
            String pure = """
                    Person.all()
                        ->project({e | $e.name}, {e | $e.department})
                        ->extend(over(), ~rowNum:{p,w,r|$p->rowNumber($r)})
                    """;

            PureExpression ast = PureParser.parse(pure);

            // New parser returns RelationExtendExpression with TypedWindowSpec
            assertInstanceOf(RelationExtendExpression.class, ast);
            RelationExtendExpression extend = (RelationExtendExpression) ast;

            assertEquals("rowNum", extend.newColumnName());
            assertTrue(extend.isWindowFunction());
            assertNotNull(extend.windowSpec());
            assertInstanceOf(RankingFunctionSpec.class, extend.windowSpec().spec());
        }

        @Test
        @DisplayName("Parse rank with partition")
        void testParseRankWithPartition() {
            String pure = """
                    Person.all()
                        ->project({e | $e.name}, {e | $e.department})
                        ->extend(over(~department), ~rank:{p,w,r|$p->rank($w,$r)})
                    """;

            PureExpression ast = PureParser.parse(pure);

            assertInstanceOf(RelationExtendExpression.class, ast);
            RelationExtendExpression extend = (RelationExtendExpression) ast;

            assertTrue(extend.isWindowFunction());
            assertFalse(extend.windowSpec().partitionColumns().isEmpty());
        }

        @Test
        @DisplayName("Parse rank with partition and order")
        void testParseRankWithPartitionAndOrder() {
            String pure = """
                    Person.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department, ~salary->desc()), ~rank:{p,w,r|$p->rank($w,$r)})
                    """;

            PureExpression ast = PureParser.parse(pure);

            assertInstanceOf(RelationExtendExpression.class, ast);
            RelationExtendExpression extend = (RelationExtendExpression) ast;

            // Check that over() has partition and order specs
            assertFalse(extend.windowSpec().partitionColumns().isEmpty());
            assertFalse(extend.windowSpec().orderColumns().isEmpty());
        }

        @Test
        @DisplayName("Parse sum aggregate window function")
        void testParseSumAggregateWindow() {
            String pure = """
                    Person.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department, ~salary->ascending()), ~runningTotal:{p,w,r|$p->sum($w,$r).salary})
                    """;

            PureExpression ast = PureParser.parse(pure);

            assertInstanceOf(RelationExtendExpression.class, ast);
            RelationExtendExpression extend = (RelationExtendExpression) ast;
            assertTrue(extend.isWindowFunction());
        }

        @Test
        @DisplayName("Parse dense_rank window function")
        void testParseDenseRank() {
            String pure = """
                    Person.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department, ~salary->desc()), ~denseRank:{p,w,r|$p->denseRank($w,$r)})
                    """;

            PureExpression ast = PureParser.parse(pure);

            assertInstanceOf(RelationExtendExpression.class, ast);
            RelationExtendExpression extend = (RelationExtendExpression) ast;
            assertEquals("denseRank", extend.newColumnName());
        }

        @Test
        @DisplayName("Parse window function with frame (rows)")
        void testParseWindowWithRowsFrame() {
            String pure = """
                    Person.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department, ~salary->ascending(), rows(unbounded(), 0)), ~runningSum:{p,w,r|$p->sum($w,$r).salary})
                    """;

            PureExpression ast = PureParser.parse(pure);

            assertInstanceOf(RelationExtendExpression.class, ast);
            RelationExtendExpression extend = (RelationExtendExpression) ast;
            assertNotNull(extend.windowSpec().frame());
        }

        @Test
        @DisplayName("Parse window function with frame (range)")
        void testParseWindowWithRangeFrame() {
            String pure = """
                    Person.all()
                        ->project({e | $e.name}, {e | $e.salary})
                        ->extend(over(~salary->ascending(), range(unbounded(), unbounded())), ~total:{p,w,r|$p->sum($w,$r).salary})
                    """;

            PureExpression ast = PureParser.parse(pure);

            assertInstanceOf(RelationExtendExpression.class, ast);
        }

        @Test
        @DisplayName("Parse window function with numeric frame bounds")
        void testParseWindowWithNumericBounds() {
            String pure = """
                    Person.all()
                        ->project({e | $e.name}, {e | $e.salary})
                        ->extend(over(~salary->ascending(), rows(-3, 0)), ~movingAvg:{p,w,r|$p->avg($w,$r).salary})
                    """;

            PureExpression ast = PureParser.parse(pure);

            assertInstanceOf(RelationExtendExpression.class, ast);
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

        @Test
        @DisplayName("Invalid window frame boundary throws error when lower > upper bound")
        void testInvalidWindowFrameBoundaryThrowsError() {
            // This matches the PCT test case: 4->_range(2) which means
            // RANGE BETWEEN 4 FOLLOWING AND 2 FOLLOWING - invalid because 4 > 2

            Exception exception = assertThrows(IllegalArgumentException.class, () -> {
                WindowExpression.FrameSpec.range(
                        WindowExpression.FrameBound.following(4), // 4 FOLLOWING
                        WindowExpression.FrameBound.following(2)); // 2 FOLLOWING - invalid!
            });

            assertTrue(exception.getMessage().contains(
                    "Invalid window frame boundary - lower bound of window frame cannot be greater than the upper bound!"),
                    "Error message should indicate invalid frame boundary");
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
                    mappings:
                    [
                        model::EmployeeMapping
                    ];
                    connections:
                    [
                        store::EmployeeDatabase:
                        [
                            environment: store::TestConnection
                        ]
                    ];
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
                        ->extend(over(~department, ~salary->desc()), ~rowNum:{p,w,r|$p->rowNumber($r)})
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
                        ->extend(over(~department, ~salary->desc()), ~salaryRank:{p,w,r|$p->rank($w,$r)})
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
                        ->extend(over(~department, ~salary->desc()), ~denseRank:{p,w,r|$p->denseRank($w,$r)})
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

        @Test
        @DisplayName("Execute LAG via Pure -> QueryService -> DuckDB")
        void testExecuteLagViaPure() throws Exception {
            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department, ~salary->descending()), ~prevSalary:{p,w,r|$p->lag($r).salary})
                    """;

            BufferedResult result = executeQuery(pureQuery);

            // Build a map of name -> prevSalary for verification
            Map<String, Object> lagValues = new HashMap<>();
            for (var row : result.rows()) {
                String name = (String) row.get(findColumnIndex(result, "name"));
                Object prevSalary = row.get(findColumnIndex(result, "prevSalary"));
                lagValues.put(name, prevSalary);
            }

            // In Engineering (ordered by salary DESC): Alice(100k) -> Bob(90k) ->
            // Charlie(80k)
            // LAG(salary,1): Alice=null, Bob=100000, Charlie=90000
            assertNull(lagValues.get("Alice"), "Alice (first in partition) should have null LAG");
            assertEquals(100000, ((Number) lagValues.get("Bob")).intValue(), "Bob's LAG should be Alice's salary");
            assertEquals(90000, ((Number) lagValues.get("Charlie")).intValue(), "Charlie's LAG should be Bob's salary");

            // In Sales: Diana(85k) -> Eve(75k)
            assertNull(lagValues.get("Diana"), "Diana (first in Sales) should have null LAG");
            assertEquals(85000, ((Number) lagValues.get("Eve")).intValue(), "Eve's LAG should be Diana's salary");

            // In Marketing: Frank(70k) alone
            assertNull(lagValues.get("Frank"), "Frank (only in Marketing) should have null LAG");
        }

        @Test
        @DisplayName("Execute LEAD via Pure -> QueryService -> DuckDB")
        void testExecuteLeadViaPure() throws Exception {
            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department, ~salary->descending()), ~nextSalary:{p,w,r|$p->lead($r).salary})
                    """;

            BufferedResult result = executeQuery(pureQuery);

            // Build a map of name -> nextSalary for verification
            Map<String, Object> leadValues = new HashMap<>();
            for (var row : result.rows()) {
                String name = (String) row.get(findColumnIndex(result, "name"));
                Object nextSalary = row.get(findColumnIndex(result, "nextSalary"));
                leadValues.put(name, nextSalary);
            }

            // In Engineering (ordered by salary DESC): Alice(100k) -> Bob(90k) ->
            // Charlie(80k)
            // LEAD(salary,1): Alice=90000, Bob=80000, Charlie=null
            assertEquals(90000, ((Number) leadValues.get("Alice")).intValue(), "Alice's LEAD should be Bob's salary");
            assertEquals(80000, ((Number) leadValues.get("Bob")).intValue(), "Bob's LEAD should be Charlie's salary");
            assertNull(leadValues.get("Charlie"), "Charlie (last in partition) should have null LEAD");

            // In Sales: Diana(85k) -> Eve(75k)
            assertEquals(75000, ((Number) leadValues.get("Diana")).intValue(), "Diana's LEAD should be Eve's salary");
            assertNull(leadValues.get("Eve"), "Eve (last in Sales) should have null LEAD");

            // In Marketing: Frank(70k) alone
            assertNull(leadValues.get("Frank"), "Frank (only in Marketing) should have null LEAD");
        }

        @Test
        @DisplayName("Execute FIRST_VALUE via Pure -> QueryService -> DuckDB")
        void testExecuteFirstValueViaPure() throws Exception {
            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department, ~salary->descending()), ~topSalary:{p,w,r|$p->first($w,$r).salary})
                    """;

            BufferedResult result = executeQuery(pureQuery);

            // Every employee should see their department's top salary
            for (var row : result.rows()) {
                String dept = (String) row.get(findColumnIndex(result, "department"));
                int topSalary = ((Number) row.get(findColumnIndex(result, "topSalary"))).intValue();

                int expected = switch (dept) {
                    case "Engineering" -> 100000; // Alice's salary
                    case "Sales" -> 85000; // Diana's salary
                    case "Marketing" -> 70000; // Frank's salary
                    default -> throw new AssertionError("Unexpected department: " + dept);
                };
                assertEquals(expected, topSalary, "Top salary for " + dept + " should be " + expected);
            }
        }

        @Test
        @DisplayName("Execute LAST_VALUE via Pure -> QueryService -> DuckDB")
        void testExecuteLastValueViaPure() throws Exception {
            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department, ~salary->descending()), ~bottomSalary:{p,w,r|$p->last($w,$r).salary})
                    """;

            BufferedResult result = executeQuery(pureQuery);

            // Build map for verification
            Map<String, Integer> salaries = new HashMap<>();
            for (var row : result.rows()) {
                String name = (String) row.get(findColumnIndex(result, "name"));
                int bottomSalary = ((Number) row.get(findColumnIndex(result, "bottomSalary"))).intValue();
                salaries.put(name, bottomSalary);
            }

            // Without frame spec, LAST_VALUE uses default frame (UNBOUNDED PRECEDING to
            // CURRENT ROW)
            // With ORDER BY salary DESC: each row sees its own salary as the "last" value
            // so far
            // Engineering (sorted DESC): Alice(100k) -> Bob(90k) -> Charlie(80k)
            assertEquals(100000, salaries.get("Alice"), "Alice sees her own (first row, so last = first)");
            assertEquals(90000, salaries.get("Bob"), "Bob sees his own (last value up to row 2)");
            assertEquals(80000, salaries.get("Charlie"), "Charlie sees his own (last value up to row 3)");
            // Sales (sorted DESC): Diana(85k) -> Eve(75k)
            assertEquals(85000, salaries.get("Diana"), "Diana sees her own (first in Sales partition)");
            assertEquals(75000, salaries.get("Eve"), "Eve sees her own (last value up to row 2)");
            // Marketing: Frank(70k) alone
            assertEquals(70000, salaries.get("Frank"), "Frank sees his own salary (only in Marketing)");
        }

        @Test
        @DisplayName("Execute NTILE via Pure -> QueryService -> DuckDB")
        void testExecuteNtileViaPure() throws Exception {
            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department, ~salary->descending()), ~bucket:{p,w,r|$p->ntile($r,2)})
                    """;

            BufferedResult result = executeQuery(pureQuery);

            // Build map for verification
            Map<String, Integer> buckets = new HashMap<>();
            for (var row : result.rows()) {
                String name = (String) row.get(findColumnIndex(result, "name"));
                int bucket = ((Number) row.get(findColumnIndex(result, "bucket"))).intValue();
                buckets.put(name, bucket);
            }

            // Engineering has 3 employees split into 2 buckets: [Alice, Bob]=1, [Charlie]=2
            assertEquals(1, buckets.get("Alice"), "Alice should be in first bucket");
            assertEquals(1, buckets.get("Bob"), "Bob should be in first bucket");
            assertEquals(2, buckets.get("Charlie"), "Charlie should be in second bucket");
            // Sales has 2 employees: [Diana]=1, [Eve]=2
            assertEquals(1, buckets.get("Diana"), "Diana should be in first bucket");
            assertEquals(2, buckets.get("Eve"), "Eve should be in second bucket");
        }

        @Test
        @DisplayName("Execute FIRST_VALUE with unbounded frame via Pure -> QueryService -> DuckDB")
        void testExecuteFirstValueWithUnboundedFrameViaPure() throws Exception {
            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department, ~salary->descending(), rows(unbounded(), unbounded())), ~topSalary:{p,w,r|$p->first($w,$r).salary})
                    """;

            // Verify SQL generation includes unbounded frame
            String sql = generateSql(pureQuery);
            System.out.println("Generated SQL with unbounded frame: " + sql);
            assertTrue(sql.contains("FIRST_VALUE"), "SQL should contain FIRST_VALUE");
            assertTrue(sql.contains("ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"),
                    "SQL should contain unbounded frame");

            BufferedResult result = executeQuery(pureQuery);

            // Build map for verification
            Map<String, Integer> topSalaries = new HashMap<>();
            for (var row : result.rows()) {
                String name = (String) row.get(findColumnIndex(result, "name"));
                int topSalary = ((Number) row.get(findColumnIndex(result, "topSalary"))).intValue();
                topSalaries.put(name, topSalary);
            }

            // With unbounded frame and ORDER BY salary DESC, FIRST_VALUE is the highest
            // salary
            // Engineering (sorted DESC): Alice(100k) is first
            assertEquals(100000, topSalaries.get("Alice"), "Alice should see top salary (100k)");
            assertEquals(100000, topSalaries.get("Bob"), "Bob should see top salary (100k)");
            assertEquals(100000, topSalaries.get("Charlie"), "Charlie should see top salary (100k)");
            // Sales: Diana(85k) is first
            assertEquals(85000, topSalaries.get("Diana"), "Diana should see top salary (85k)");
            assertEquals(85000, topSalaries.get("Eve"), "Eve should see top salary (85k)");
            // Marketing: Frank(70k) alone
            assertEquals(70000, topSalaries.get("Frank"), "Frank should see his own salary");
        }

        @Test
        @DisplayName("Execute LAST_VALUE with unbounded frame via Pure -> QueryService -> DuckDB")
        void testExecuteLastValueWithUnboundedFrameViaPure() throws Exception {
            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department, ~salary->descending(), rows(unbounded(), unbounded())), ~bottomSalary:{p,w,r|$p->last($w,$r).salary})
                    """;

            // Verify SQL generation includes unbounded frame
            String sql = generateSql(pureQuery);
            System.out.println("Generated SQL with unbounded frame: " + sql);
            assertTrue(sql.contains("LAST_VALUE"), "SQL should contain LAST_VALUE");
            assertTrue(sql.contains("ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"),
                    "SQL should contain unbounded frame");

            BufferedResult result = executeQuery(pureQuery);

            // Build map for verification
            Map<String, Integer> bottomSalaries = new HashMap<>();
            for (var row : result.rows()) {
                String name = (String) row.get(findColumnIndex(result, "name"));
                int bottomSalary = ((Number) row.get(findColumnIndex(result, "bottomSalary"))).intValue();
                bottomSalaries.put(name, bottomSalary);
            }

            // With unbounded frame and ORDER BY salary DESC, LAST_VALUE is the lowest
            // salary
            // Engineering: Alice(100k) -> Bob(90k) -> Charlie(80k) - Charlie is last
            assertEquals(80000, bottomSalaries.get("Alice"), "Alice should see bottom salary (80k)");
            assertEquals(80000, bottomSalaries.get("Bob"), "Bob should see bottom salary (80k)");
            assertEquals(80000, bottomSalaries.get("Charlie"), "Charlie should see bottom salary (80k)");
            // Sales: Diana(85k) -> Eve(75k) - Eve is last
            assertEquals(75000, bottomSalaries.get("Diana"), "Diana should see bottom salary (75k)");
            assertEquals(75000, bottomSalaries.get("Eve"), "Eve should see bottom salary (75k)");
            // Marketing: Frank(70k) alone
            assertEquals(70000, bottomSalaries.get("Frank"), "Frank should see his own salary");
        }

        @Test
        @DisplayName("Execute SUM with running total frame (unbounded preceding to current row)")
        void testExecuteSumWithRunningTotalFrame() throws Exception {
            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department, ~salary->ascending(), rows(unbounded(), 0)), ~runningTotal:{p,w,r|$p->sum($w,$r).salary})
                    """;

            String sql = generateSql(pureQuery);
            System.out.println("Running total SQL: " + sql);
            assertTrue(sql.contains("SUM"), "SQL should contain SUM");
            assertTrue(sql.contains("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
                    "SQL should contain running total frame");

            BufferedResult result = executeQuery(pureQuery);
            assertEquals(6, result.rowCount(), "Should have 6 employees");

            // Verify running sum column exists and has values
            Map<String, Integer> runningTotals = new HashMap<>();
            for (var row : result.rows()) {
                String name = (String) row.get(findColumnIndex(result, "name"));
                int runningTotal = ((Number) row.get(findColumnIndex(result, "runningTotal"))).intValue();
                runningTotals.put(name, runningTotal);
            }

            // Engineering (sorted ASC by salary): Charlie(80k) -> Bob(90k) -> Alice(100k)
            // Running totals: 80k, 170k, 270k
            assertEquals(80000, runningTotals.get("Charlie"), "Charlie's running total (first)");
            assertEquals(170000, runningTotals.get("Bob"), "Bob's running total (80k + 90k)");
            assertEquals(270000, runningTotals.get("Alice"), "Alice's running total (80k + 90k + 100k)");
        }

        @Test
        @DisplayName("Execute AVG with sliding window frame (previous 1 row to next 1 row)")
        void testExecuteAvgWithSlidingWindowFrame() throws Exception {
            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department, ~salary->ascending(), rows(-1, 1)), ~movingAvg:{p,w,r|$p->avg($w,$r).salary})
                    """;

            String sql = generateSql(pureQuery);
            System.out.println("Sliding window SQL: " + sql);
            assertTrue(sql.contains("AVG"), "SQL should contain AVG");
            assertTrue(sql.contains("ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING"),
                    "SQL should contain sliding window frame");

            BufferedResult result = executeQuery(pureQuery);
            assertEquals(6, result.rowCount(), "Should have 6 employees");

            Map<String, Double> movingAvgs = new HashMap<>();
            for (var row : result.rows()) {
                String name = (String) row.get(findColumnIndex(result, "name"));
                double movingAvg = ((Number) row.get(findColumnIndex(result, "movingAvg"))).doubleValue();
                movingAvgs.put(name, movingAvg);
            }

            // Engineering (sorted ASC): Charlie(80k) -> Bob(90k) -> Alice(100k)
            // Charlie: avg(80k, 90k) = 85k (no previous row)
            // Bob: avg(80k, 90k, 100k) = 90k
            // Alice: avg(90k, 100k) = 95k (no following row)
            assertEquals(85000.0, movingAvgs.get("Charlie"), 0.01, "Charlie's moving avg");
            assertEquals(90000.0, movingAvgs.get("Bob"), 0.01, "Bob's moving avg");
            assertEquals(95000.0, movingAvgs.get("Alice"), 0.01, "Alice's moving avg");
        }

        @Test
        @DisplayName("Execute SUM with trailing window frame (previous 2 rows to current)")
        void testExecuteSumWithTrailingWindowFrame() throws Exception {
            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department, ~salary->ascending(), rows(-2, 0)), ~trailing3Sum:{p,w,r|$p->sum($w,$r).salary})
                    """;

            String sql = generateSql(pureQuery);
            System.out.println("Trailing window SQL: " + sql);
            assertTrue(sql.contains("SUM"), "SQL should contain SUM");
            assertTrue(sql.contains("ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"),
                    "SQL should contain trailing window frame");

            BufferedResult result = executeQuery(pureQuery);

            Map<String, Integer> trailingSums = new HashMap<>();
            for (var row : result.rows()) {
                String name = (String) row.get(findColumnIndex(result, "name"));
                int trailingSum = ((Number) row.get(findColumnIndex(result, "trailing3Sum"))).intValue();
                trailingSums.put(name, trailingSum);
            }

            // Engineering (sorted ASC): Charlie(80k) -> Bob(90k) -> Alice(100k)
            // Charlie: sum(80k) = 80k (no previous rows)
            // Bob: sum(80k, 90k) = 170k (only 1 previous row)
            // Alice: sum(80k, 90k, 100k) = 270k (2 previous rows)
            assertEquals(80000, trailingSums.get("Charlie"), "Charlie's trailing sum (only current)");
            assertEquals(170000, trailingSums.get("Bob"), "Bob's trailing sum (Charlie + Bob)");
            assertEquals(270000, trailingSums.get("Alice"), "Alice's trailing sum (all 3)");
        }

        @Test
        @DisplayName("Execute SUM with current row to end frame (0 to unbounded)")
        void testExecuteSumWithCurrentToEndFrame() throws Exception {
            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department, ~salary->ascending(), rows(0, unbounded())), ~remainingSum:{p,w,r|$p->sum($w,$r).salary})
                    """;

            String sql = generateSql(pureQuery);
            System.out.println("Current to end SQL: " + sql);
            assertTrue(sql.contains("SUM"), "SQL should contain SUM");
            assertTrue(sql.contains("ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING"),
                    "SQL should contain current to end frame");

            BufferedResult result = executeQuery(pureQuery);

            Map<String, Integer> remainingSums = new HashMap<>();
            for (var row : result.rows()) {
                String name = (String) row.get(findColumnIndex(result, "name"));
                int remainingSum = ((Number) row.get(findColumnIndex(result, "remainingSum"))).intValue();
                remainingSums.put(name, remainingSum);
            }

            // Engineering (sorted ASC): Charlie(80k) -> Bob(90k) -> Alice(100k)
            // Charlie: sum(80k, 90k, 100k) = 270k (all remaining)
            // Bob: sum(90k, 100k) = 190k
            // Alice: sum(100k) = 100k (only current)
            assertEquals(270000, remainingSums.get("Charlie"), "Charlie's remaining sum (all 3)");
            assertEquals(190000, remainingSums.get("Bob"), "Bob's remaining sum (Bob + Alice)");
            assertEquals(100000, remainingSums.get("Alice"), "Alice's remaining sum (only current)");
        }

        @Test
        @DisplayName("Execute COUNT with sliding window frame")
        void testExecuteCountWithSlidingWindowFrame() throws Exception {
            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department, ~salary->ascending(), rows(-1, 1)), ~neighborCount:{p,w,r|$p->count($w,$r)})
                    """;

            String sql = generateSql(pureQuery);
            System.out.println("Sliding COUNT SQL: " + sql);
            assertTrue(sql.contains("COUNT"), "SQL should contain COUNT");
            assertTrue(sql.contains("ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING"),
                    "SQL should contain sliding window frame");

            BufferedResult result = executeQuery(pureQuery);

            Map<String, Long> neighborCounts = new HashMap<>();
            for (var row : result.rows()) {
                String name = (String) row.get(findColumnIndex(result, "name"));
                long count = ((Number) row.get(findColumnIndex(result, "neighborCount"))).longValue();
                neighborCounts.put(name, count);
            }

            // Engineering (sorted ASC): Charlie(80k) -> Bob(90k) -> Alice(100k)
            // Charlie: count of (Charlie, Bob) = 2 (no previous row)
            // Bob: count of (Charlie, Bob, Alice) = 3 (full window)
            // Alice: count of (Bob, Alice) = 2 (no following row)
            assertEquals(2L, neighborCounts.get("Charlie"), "Charlie's neighbor count");
            assertEquals(3L, neighborCounts.get("Bob"), "Bob's neighbor count");
            assertEquals(2L, neighborCounts.get("Alice"), "Alice's neighbor count");
        }

        @Test
        @DisplayName("Window function with NULL values and DESC ordering - NULLS FIRST semantics")
        void testWindowFunctionWithNullValuesDescOrdering() throws Exception {
            // This test verifies that NULL values are ordered FIRST in DESC ordering
            // matching Pure semantics (opposite of SQL default NULLS LAST)

            // Use TDS literal to test window function with nulls
            String pureQuery = """
                    #TDS
                        p, o, i
                        0, 1, 10
                        0, 1, 10
                        0, null, 30
                        100, 1, 10
                        100, 2, 20
                        100, null, 20
                        100, 3, 30
                        100, null, 30
                    #->extend(~p->over(~o->descending(), unbounded()->_range(1)),
                              ~sumI:{p,w,r|$r.i}:y:Integer[*]|$y->plus())
                    """;

            String sql = generateSql(pureQuery);
            System.out.println("Window with NULLs SQL: " + sql);

            // Verify NULLS FIRST is in the SQL for DESC ordering
            assertTrue(sql.contains("NULLS FIRST"),
                    "SQL should contain NULLS FIRST for DESC ordering");
            assertTrue(sql.contains("DESC NULLS FIRST"),
                    "SQL should have DESC NULLS FIRST");
        }

        @Test
        @DisplayName("Decimal RANGE frame bounds - 0.5D->_range(2.5)")
        void testDecimalRangeFrameBounds() throws Exception {
            // This test verifies that decimal frame bounds work correctly
            // Example: 0.5D->_range(2.5) means RANGE BETWEEN 0.5 FOLLOWING AND 2.5
            // FOLLOWING

            String pureQuery = """
                    #TDS
                        menu_category, menu_cogs_usd
                        Beverage, 0.5
                        Beverage, 0.65
                        Beverage, 0.75
                        Dessert, 0.50
                        Dessert, 1.00
                        Dessert, 1.25
                    #->extend(~menu_cogs_usd->ascending()->over(0.5D->_range(2.5)),
                              ~sum_cogs:{p,w,r|$r.menu_cogs_usd}:y:Float[*]|$y->plus())
                    """;

            String sql = generateSql(pureQuery);
            System.out.println("Decimal RANGE frame SQL: " + sql);

            // Verify decimal bounds appear in the SQL
            assertTrue(sql.contains("RANGE BETWEEN"),
                    "SQL should contain RANGE BETWEEN");
            assertTrue(sql.contains("0.5 FOLLOWING"),
                    "SQL should contain 0.5 FOLLOWING for start bound");
            assertTrue(sql.contains("2.5 FOLLOWING"),
                    "SQL should contain 2.5 FOLLOWING for end bound");
        }

        @Test
        @DisplayName("Execute STDDEV window aggregate via Pure -> QueryService -> DuckDB")
        void testExecuteStdDevWindowViaPure() throws Exception {
            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department), ~deptStdDev:{p,w,r|$p->stdDev($w,$r).salary})
                    """;

            String sql = generateSql(pureQuery);
            System.out.println("STDDEV Window SQL: " + sql);
            assertTrue(sql.contains("STDDEV"), "SQL should contain STDDEV");
            assertTrue(sql.contains("OVER"), "SQL should contain OVER");
            assertTrue(sql.contains("PARTITION BY"), "SQL should contain PARTITION BY");

            BufferedResult result = executeQuery(pureQuery);
            assertEquals(6, result.rowCount(), "Should have 6 employees");

            // Verify stdDev column exists
            assertTrue(result.columns().stream()
                    .anyMatch(c -> c.name().equals("deptStdDev")),
                    "Should have deptStdDev column");

            // Print results for verification
            System.out.println("STDDEV Window Results:");
            for (var row : result.rows()) {
                String name = (String) row.get(findColumnIndex(result, "name"));
                String dept = (String) row.get(findColumnIndex(result, "department"));
                Object stddev = row.get(findColumnIndex(result, "deptStdDev"));
                System.out.printf("  %s (%s): stdDev = %s%n", name, dept, stddev);
            }
        }

        @Test
        @DisplayName("Execute VARIANCE window aggregate via Pure -> QueryService -> DuckDB")
        void testExecuteVarianceWindowViaPure() throws Exception {
            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department), ~deptVar:{p,w,r|$p->variance($w,$r).salary})
                    """;

            String sql = generateSql(pureQuery);
            System.out.println("VARIANCE Window SQL: " + sql);
            assertTrue(sql.contains("VARIANCE"), "SQL should contain VARIANCE");
            assertTrue(sql.contains("OVER"), "SQL should contain OVER");
            assertTrue(sql.contains("PARTITION BY"), "SQL should contain PARTITION BY");

            BufferedResult result = executeQuery(pureQuery);
            assertEquals(6, result.rowCount(), "Should have 6 employees");

            // Print results for verification
            System.out.println("VARIANCE Window Results:");
            for (var row : result.rows()) {
                String name = (String) row.get(findColumnIndex(result, "name"));
                String dept = (String) row.get(findColumnIndex(result, "department"));
                Object variance = row.get(findColumnIndex(result, "deptVar"));
                System.out.printf("  %s (%s): variance = %s%n", name, dept, variance);
            }
        }

        @Test
        @DisplayName("Execute PERCENT_RANK via Pure -> QueryService -> DuckDB")
        void testExecutePercentRankViaPure() throws Exception {
            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department, ~salary->desc()), ~pctRank:{p,w,r| $p->percentRank($w,$r)})
                    """;

            String sql = generateSql(pureQuery);
            System.out.println("PERCENT_RANK SQL: " + sql);
            assertTrue(sql.contains("PERCENT_RANK"), "SQL should contain PERCENT_RANK");
            assertTrue(sql.contains("OVER"), "SQL should contain OVER");

            BufferedResult result = executeQuery(pureQuery);
            assertEquals(6, result.rowCount(), "Should have 6 employees");

            // PERCENT_RANK is (rank - 1) / (total_rows - 1)
            // For Engineering sorted by salary desc: Alice(100k)=0.0, Bob(90k)=0.5,
            // Charlie(80k)=1.0
            System.out.println("PERCENT_RANK Results:");
            for (var row : result.rows()) {
                String name = (String) row.get(findColumnIndex(result, "name"));
                String dept = (String) row.get(findColumnIndex(result, "department"));
                Object pctRank = row.get(findColumnIndex(result, "pctRank"));
                System.out.printf("  %s (%s): pctRank = %s%n", name, dept, pctRank);

                if ("Engineering".equals(dept)) {
                    double pctVal = ((Number) pctRank).doubleValue();
                    switch (name) {
                        case "Alice" -> assertEquals(0.0, pctVal, 0.01, "Alice should have pctRank 0.0 (highest)");
                        case "Charlie" -> assertEquals(1.0, pctVal, 0.01, "Charlie should have pctRank 1.0 (lowest)");
                    }
                }
            }
        }

        @Test
        @DisplayName("Execute CUME_DIST via Pure -> QueryService -> DuckDB")
        void testExecuteCumeDistViaPure() throws Exception {
            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department, ~salary->desc()), ~cumeDist:{p,w,r| $p->cumulativeDistribution($w,$r)})
                    """;

            String sql = generateSql(pureQuery);
            System.out.println("CUME_DIST SQL: " + sql);
            assertTrue(sql.contains("CUME_DIST"), "SQL should contain CUME_DIST");
            assertTrue(sql.contains("OVER"), "SQL should contain OVER");

            BufferedResult result = executeQuery(pureQuery);
            assertEquals(6, result.rowCount(), "Should have 6 employees");

            // CUME_DIST is number_of_rows_with_value_<=_current / total_rows
            // For Engineering sorted by salary desc: Alice(100k)=0.333, Bob(90k)=0.667,
            // Charlie(80k)=1.0
            System.out.println("CUME_DIST Results:");
            for (var row : result.rows()) {
                String name = (String) row.get(findColumnIndex(result, "name"));
                String dept = (String) row.get(findColumnIndex(result, "department"));
                Object cumeDist = row.get(findColumnIndex(result, "cumeDist"));
                System.out.printf("  %s (%s): cumeDist = %s%n", name, dept, cumeDist);

                if ("Engineering".equals(dept)) {
                    double cdVal = ((Number) cumeDist).doubleValue();
                    switch (name) {
                        case "Alice" -> assertTrue(cdVal > 0.3 && cdVal < 0.4, "Alice should have cumeDist ~0.333");
                        case "Charlie" -> assertEquals(1.0, cdVal, 0.01, "Charlie should have cumeDist 1.0 (lowest)");
                    }
                }
            }
        }

        @Test
        @DisplayName("Execute CUME_DIST with chained round() post-processor")
        void testExecuteCumeDistWithRound() throws Exception {
            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                        ->extend(over(~department, ~salary->desc()), ~cumeDist:{p,w,r| $p->cumulativeDistribution($w,$r)->round(2)})
                    """;

            String sql = generateSql(pureQuery);
            System.out.println("CUME_DIST with round SQL: " + sql);
            assertTrue(sql.contains("ROUND"), "SQL should contain ROUND wrapping the window function");
            assertTrue(sql.contains("CUME_DIST"), "SQL should contain CUME_DIST");
            assertTrue(sql.contains("OVER"), "SQL should contain OVER");

            BufferedResult result = executeQuery(pureQuery);
            assertEquals(6, result.rowCount(), "Should have 6 employees");

            // Verify results are rounded to 2 decimal places
            System.out.println("CUME_DIST with round Results:");
            for (var row : result.rows()) {
                String name = (String) row.get(findColumnIndex(result, "name"));
                String dept = (String) row.get(findColumnIndex(result, "department"));
                Object cumeDist = row.get(findColumnIndex(result, "cumeDist"));
                System.out.printf("  %s (%s): cumeDist = %s%n", name, dept, cumeDist);

                if ("Engineering".equals(dept)) {
                    double cdVal = ((Number) cumeDist).doubleValue();
                    switch (name) {
                        case "Alice" -> assertEquals(0.33, cdVal, 0.01, "Alice should have rounded cumeDist ~0.33");
                        case "Bob" -> assertEquals(0.67, cdVal, 0.01, "Bob should have rounded cumeDist ~0.67");
                        case "Charlie" -> assertEquals(1.0, cdVal, 0.01, "Charlie should have cumeDist 1.0");
                    }
                }
            }
        }

        @Test
        @DisplayName("Execute aggregate extend without over() - SUM over entire relation")
        void testAggregateExtendNoOver() throws Exception {
            String pureQuery = """
                    Employee.all()
                        ->project({e | $e.name}, {e | $e.salary})
                        ->extend(~totalSalary:c|$c.salary:y|$y->plus())
                    """;

            String sql = generateSql(pureQuery);
            System.out.println("Aggregate extend (no over) SQL: " + sql);
            assertTrue(sql.contains("SUM"), "SQL should contain SUM");
            assertTrue(sql.contains("OVER"), "SQL should contain OVER (empty window = entire relation)");

            BufferedResult result = executeQuery(pureQuery);
            assertEquals(6, result.rowCount(), "Should have 6 employees");

            // Total salary should be sum of all: 100k + 90k + 80k + 85k + 75k + 70k = 500k
            int expectedTotal = 100000 + 90000 + 80000 + 85000 + 75000 + 70000;
            for (var row : result.rows()) {
                String name = (String) row.get(findColumnIndex(result, "name"));
                long totalSalary = ((Number) row.get(findColumnIndex(result, "totalSalary"))).longValue();
                System.out.printf("  %s: totalSalary = %d%n", name, totalSalary);
                assertEquals(expectedTotal, totalSalary,
                        "Total salary should be same for all rows (entire relation aggregate)");
            }
        }
    }
}
