package com.gs.legend.test;
import com.gs.legend.ast.*;
import com.gs.legend.antlr.*;
import com.gs.legend.parser.*;
import com.gs.legend.compiler.*;
import com.gs.legend.model.*;
import com.gs.legend.model.def.*;
import com.gs.legend.model.m3.*;
import com.gs.legend.model.store.*;
import com.gs.legend.model.mapping.*;
import com.gs.legend.plan.*;
import com.gs.legend.exec.*;
import com.gs.legend.serial.*;
import com.gs.legend.sqlgen.*;
import com.gs.legend.server.*;
import com.gs.legend.service.*;
import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.server.QueryService;
import com.gs.legend.plan.PlanGenerator;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for window function capability in the Relation API.
 *
 * Tests cover end-to-end: Pure query → PlanGenerator → SQL → DuckDB execution.
 *
 * Parser and SQL Generator unit tests removed — those tested old pipeline types
 * (PureExpression, SQLGenerator, ExtendNode, etc.) that no longer exist.
 */
class WindowFunctionTest {

    private Connection connection;
    private QueryService queryService;

    // Complete Pure model for Employee tests
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

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                        CREATE TABLE T_EMPLOYEE (
                            ID INTEGER PRIMARY KEY,
                            NAME VARCHAR(100),
                            DEPARTMENT VARCHAR(50),
                            SALARY INTEGER
                        )
                    """);

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

    private ExecutionResult executeQuery(String pureQuery) throws SQLException {
        return queryService.execute(
                COMPLETE_MODEL,
                pureQuery,
                "test::TestRuntime",
                connection);
    }

    private String generateSql(String pureQuery) {
        var plan = PlanGenerator.generate(
                COMPLETE_MODEL,
                pureQuery,
                "test::TestRuntime");
        return plan.sql();
    }

    private int findColumnIndex(ExecutionResult result, String columnName) {
        for (int i = 0; i < result.columns().size(); i++) {
            if (result.columns().get(i).name().equals(columnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Column not found: " + columnName);
    }

    @Test
    @DisplayName("Execute ROW_NUMBER via Pure → QueryService → DuckDB")
    void testExecuteRowNumberViaPure() throws Exception {
        String pureQuery = """
                Employee.all()
                    ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                    ->extend(over(~department, ~salary->desc()), ~rowNum:{p,w,r|$p->rowNumber($r)})
                """;

        String sql = generateSql(pureQuery);
        assertTrue(sql.contains("ROW_NUMBER()"), "SQL should contain ROW_NUMBER()");
        assertTrue(sql.contains("OVER"), "SQL should contain OVER");
        assertTrue(sql.contains("PARTITION BY"), "SQL should contain PARTITION BY");

        var result = executeQuery(pureQuery);
        assertEquals(6, result.rowCount(), "Should have 6 employees");

        assertTrue(result.columns().stream()
                .anyMatch(c -> c.name().equals("rowNum")),
                "Should have rowNum column");

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
    @DisplayName("Execute RANK via Pure → QueryService → DuckDB")
    void testExecuteRankViaPure() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO T_EMPLOYEE VALUES (7, 'Grace', 'Engineering', 90000)");
        }

        String pureQuery = """
                Employee.all()
                    ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                    ->extend(over(~department, ~salary->desc()), ~salaryRank:{p,w,r|$p->rank($w,$r)})
                """;

        var result = executeQuery(pureQuery);

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
    @DisplayName("Execute SUM running total via Pure → QueryService → DuckDB")
    void testExecuteRunningSumViaPure() throws Exception {
        String pureQuery = """
                Employee.all()
                    ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                    ->extend(over(~department, ~salary->ascending()), ~runningTotal:{p,w,r|$r.salary}:y|$y->plus())
                """;

        var result = executeQuery(pureQuery);

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
    @DisplayName("Execute DENSE_RANK via Pure → QueryService → DuckDB")
    void testExecuteDenseRankViaPure() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO T_EMPLOYEE VALUES (7, 'Grace', 'Engineering', 90000)");
        }

        String pureQuery = """
                Employee.all()
                    ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                    ->extend(over(~department, ~salary->desc()), ~denseRank:{p,w,r|$p->denseRank($w,$r)})
                """;

        var result = executeQuery(pureQuery);

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
    @DisplayName("Execute SUM with ROWS frame via Pure → QueryService → DuckDB")
    void testExecuteSumWithRowsFrameViaPure() throws Exception {
        String pureQuery = """
                Employee.all()
                    ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                    ->extend(over(~department, ~salary->ascending(), rows(unbounded(), 0)), ~runningSum:{p,w,r|$r.salary}:y|$y->plus())
                """;

        String sql = generateSql(pureQuery);
        assertTrue(sql.contains("ROWS BETWEEN"), "SQL should contain ROWS BETWEEN");
        assertTrue(sql.contains("UNBOUNDED PRECEDING"), "SQL should contain UNBOUNDED PRECEDING");

        var result = executeQuery(pureQuery);
        assertEquals(6, result.rowCount(), "Should have 6 employees");

        assertTrue(result.columns().stream()
                .anyMatch(c -> c.name().equals("runningSum")),
                "Should have runningSum column");
    }

    @Test
    @DisplayName("Execute LAG via Pure → QueryService → DuckDB")
    void testExecuteLagViaPure() throws Exception {
        String pureQuery = """
                Employee.all()
                    ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                    ->extend(over(~department, ~salary->descending()), ~prevSalary:{p,w,r|$p->lag($r).salary})
                """;

        var result = executeQuery(pureQuery);

        Map<String, Object> lagValues = new HashMap<>();
        for (var row : result.rows()) {
            String name = (String) row.get(findColumnIndex(result, "name"));
            Object prevSalary = row.get(findColumnIndex(result, "prevSalary"));
            lagValues.put(name, prevSalary);
        }

        assertNull(lagValues.get("Alice"), "Alice (first in partition) should have null LAG");
        assertEquals(100000, ((Number) lagValues.get("Bob")).intValue(), "Bob's LAG should be Alice's salary");
        assertEquals(90000, ((Number) lagValues.get("Charlie")).intValue(), "Charlie's LAG should be Bob's salary");

        assertNull(lagValues.get("Diana"), "Diana (first in Sales) should have null LAG");
        assertEquals(85000, ((Number) lagValues.get("Eve")).intValue(), "Eve's LAG should be Diana's salary");

        assertNull(lagValues.get("Frank"), "Frank (only in Marketing) should have null LAG");
    }

    @Test
    @DisplayName("Execute LEAD via Pure → QueryService → DuckDB")
    void testExecuteLeadViaPure() throws Exception {
        String pureQuery = """
                Employee.all()
                    ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                    ->extend(over(~department, ~salary->descending()), ~nextSalary:{p,w,r|$p->lead($r).salary})
                """;

        var result = executeQuery(pureQuery);

        Map<String, Object> leadValues = new HashMap<>();
        for (var row : result.rows()) {
            String name = (String) row.get(findColumnIndex(result, "name"));
            Object nextSalary = row.get(findColumnIndex(result, "nextSalary"));
            leadValues.put(name, nextSalary);
        }

        assertEquals(90000, ((Number) leadValues.get("Alice")).intValue(), "Alice's LEAD should be Bob's salary");
        assertEquals(80000, ((Number) leadValues.get("Bob")).intValue(), "Bob's LEAD should be Charlie's salary");
        assertNull(leadValues.get("Charlie"), "Charlie (last in partition) should have null LEAD");

        assertEquals(75000, ((Number) leadValues.get("Diana")).intValue(), "Diana's LEAD should be Eve's salary");
        assertNull(leadValues.get("Eve"), "Eve (last in Sales) should have null LEAD");

        assertNull(leadValues.get("Frank"), "Frank (only in Marketing) should have null LEAD");
    }

    @Test
    @DisplayName("Execute FIRST_VALUE via Pure → QueryService → DuckDB")
    void testExecuteFirstValueViaPure() throws Exception {
        String pureQuery = """
                Employee.all()
                    ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                    ->extend(over(~department, ~salary->descending()), ~topSalary:{p,w,r|$p->first($w,$r).salary})
                """;

        var result = executeQuery(pureQuery);

        for (var row : result.rows()) {
            String dept = (String) row.get(findColumnIndex(result, "department"));
            int topSalary = ((Number) row.get(findColumnIndex(result, "topSalary"))).intValue();

            int expected = switch (dept) {
                case "Engineering" -> 100000;
                case "Sales" -> 85000;
                case "Marketing" -> 70000;
                default -> throw new AssertionError("Unexpected department: " + dept);
            };
            assertEquals(expected, topSalary, "Top salary for " + dept + " should be " + expected);
        }
    }

    @Test
    @DisplayName("Execute LAST_VALUE via Pure → QueryService → DuckDB")
    void testExecuteLastValueViaPure() throws Exception {
        String pureQuery = """
                Employee.all()
                    ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                    ->extend(over(~department, ~salary->descending()), ~bottomSalary:{p,w,r|$p->last($w,$r).salary})
                """;

        var result = executeQuery(pureQuery);

        Map<String, Integer> salaries = new HashMap<>();
        for (var row : result.rows()) {
            String name = (String) row.get(findColumnIndex(result, "name"));
            int bottomSalary = ((Number) row.get(findColumnIndex(result, "bottomSalary"))).intValue();
            salaries.put(name, bottomSalary);
        }

        assertEquals(100000, salaries.get("Alice"), "Alice sees her own (first row, so last = first)");
        assertEquals(90000, salaries.get("Bob"), "Bob sees his own (last value up to row 2)");
        assertEquals(80000, salaries.get("Charlie"), "Charlie sees his own (last value up to row 3)");
        assertEquals(85000, salaries.get("Diana"), "Diana sees her own (first in Sales partition)");
        assertEquals(75000, salaries.get("Eve"), "Eve sees her own (last value up to row 2)");
        assertEquals(70000, salaries.get("Frank"), "Frank sees his own salary (only in Marketing)");
    }

    @Test
    @DisplayName("Execute NTILE via Pure → QueryService → DuckDB")
    void testExecuteNtileViaPure() throws Exception {
        String pureQuery = """
                Employee.all()
                    ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                    ->extend(over(~department, ~salary->descending()), ~bucket:{p,w,r|$p->ntile($r,2)})
                """;

        var result = executeQuery(pureQuery);

        Map<String, Integer> buckets = new HashMap<>();
        for (var row : result.rows()) {
            String name = (String) row.get(findColumnIndex(result, "name"));
            int bucket = ((Number) row.get(findColumnIndex(result, "bucket"))).intValue();
            buckets.put(name, bucket);
        }

        assertEquals(1, buckets.get("Alice"), "Alice should be in first bucket");
        assertEquals(1, buckets.get("Bob"), "Bob should be in first bucket");
        assertEquals(2, buckets.get("Charlie"), "Charlie should be in second bucket");
        assertEquals(1, buckets.get("Diana"), "Diana should be in first bucket");
        assertEquals(2, buckets.get("Eve"), "Eve should be in second bucket");
    }

    @Test
    @DisplayName("Execute FIRST_VALUE with unbounded frame via Pure → QueryService → DuckDB")
    void testExecuteFirstValueWithUnboundedFrameViaPure() throws Exception {
        String pureQuery = """
                Employee.all()
                    ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                    ->extend(over(~department, ~salary->descending(), rows(unbounded(), unbounded())), ~topSalary:{p,w,r|$p->first($w,$r).salary})
                """;

        String sql = generateSql(pureQuery);
        assertTrue(sql.contains("FIRST_VALUE"), "SQL should contain FIRST_VALUE");
        assertTrue(sql.contains("ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"),
                "SQL should contain unbounded frame");

        var result = executeQuery(pureQuery);

        Map<String, Integer> topSalaries = new HashMap<>();
        for (var row : result.rows()) {
            String name = (String) row.get(findColumnIndex(result, "name"));
            int topSalary = ((Number) row.get(findColumnIndex(result, "topSalary"))).intValue();
            topSalaries.put(name, topSalary);
        }

        assertEquals(100000, topSalaries.get("Alice"), "Alice should see top salary (100k)");
        assertEquals(100000, topSalaries.get("Bob"), "Bob should see top salary (100k)");
        assertEquals(100000, topSalaries.get("Charlie"), "Charlie should see top salary (100k)");
        assertEquals(85000, topSalaries.get("Diana"), "Diana should see top salary (85k)");
        assertEquals(85000, topSalaries.get("Eve"), "Eve should see top salary (85k)");
        assertEquals(70000, topSalaries.get("Frank"), "Frank should see his own salary");
    }

    @Test
    @DisplayName("Execute LAST_VALUE with unbounded frame via Pure → QueryService → DuckDB")
    void testExecuteLastValueWithUnboundedFrameViaPure() throws Exception {
        String pureQuery = """
                Employee.all()
                    ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                    ->extend(over(~department, ~salary->descending(), rows(unbounded(), unbounded())), ~bottomSalary:{p,w,r|$p->last($w,$r).salary})
                """;

        String sql = generateSql(pureQuery);
        assertTrue(sql.contains("LAST_VALUE"), "SQL should contain LAST_VALUE");
        assertTrue(sql.contains("ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"),
                "SQL should contain unbounded frame");

        var result = executeQuery(pureQuery);

        Map<String, Integer> bottomSalaries = new HashMap<>();
        for (var row : result.rows()) {
            String name = (String) row.get(findColumnIndex(result, "name"));
            int bottomSalary = ((Number) row.get(findColumnIndex(result, "bottomSalary"))).intValue();
            bottomSalaries.put(name, bottomSalary);
        }

        assertEquals(80000, bottomSalaries.get("Alice"), "Alice should see bottom salary (80k)");
        assertEquals(80000, bottomSalaries.get("Bob"), "Bob should see bottom salary (80k)");
        assertEquals(80000, bottomSalaries.get("Charlie"), "Charlie should see bottom salary (80k)");
        assertEquals(75000, bottomSalaries.get("Diana"), "Diana should see bottom salary (75k)");
        assertEquals(75000, bottomSalaries.get("Eve"), "Eve should see bottom salary (75k)");
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
        assertTrue(sql.contains("SUM"), "SQL should contain SUM");
        assertTrue(sql.contains("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"),
                "SQL should contain running total frame");

        var result = executeQuery(pureQuery);
        assertEquals(6, result.rowCount(), "Should have 6 employees");

        Map<String, Integer> runningTotals = new HashMap<>();
        for (var row : result.rows()) {
            String name = (String) row.get(findColumnIndex(result, "name"));
            int runningTotal = ((Number) row.get(findColumnIndex(result, "runningTotal"))).intValue();
            runningTotals.put(name, runningTotal);
        }

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
        assertTrue(sql.contains("AVG"), "SQL should contain AVG");
        assertTrue(sql.contains("ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING"),
                "SQL should contain sliding window frame");

        var result = executeQuery(pureQuery);
        assertEquals(6, result.rowCount(), "Should have 6 employees");

        Map<String, Double> movingAvgs = new HashMap<>();
        for (var row : result.rows()) {
            String name = (String) row.get(findColumnIndex(result, "name"));
            double movingAvg = ((Number) row.get(findColumnIndex(result, "movingAvg"))).doubleValue();
            movingAvgs.put(name, movingAvg);
        }

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
        assertTrue(sql.contains("SUM"), "SQL should contain SUM");
        assertTrue(sql.contains("ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"),
                "SQL should contain trailing window frame");

        var result = executeQuery(pureQuery);

        Map<String, Integer> trailingSums = new HashMap<>();
        for (var row : result.rows()) {
            String name = (String) row.get(findColumnIndex(result, "name"));
            int trailingSum = ((Number) row.get(findColumnIndex(result, "trailing3Sum"))).intValue();
            trailingSums.put(name, trailingSum);
        }

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
        assertTrue(sql.contains("SUM"), "SQL should contain SUM");
        assertTrue(sql.contains("ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING"),
                "SQL should contain current to end frame");

        var result = executeQuery(pureQuery);

        Map<String, Integer> remainingSums = new HashMap<>();
        for (var row : result.rows()) {
            String name = (String) row.get(findColumnIndex(result, "name"));
            int remainingSum = ((Number) row.get(findColumnIndex(result, "remainingSum"))).intValue();
            remainingSums.put(name, remainingSum);
        }

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
        assertTrue(sql.contains("COUNT"), "SQL should contain COUNT");
        assertTrue(sql.contains("ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING"),
                "SQL should contain sliding window frame");

        var result = executeQuery(pureQuery);

        Map<String, Long> neighborCounts = new HashMap<>();
        for (var row : result.rows()) {
            String name = (String) row.get(findColumnIndex(result, "name"));
            long count = ((Number) row.get(findColumnIndex(result, "neighborCount"))).longValue();
            neighborCounts.put(name, count);
        }

        assertEquals(2L, neighborCounts.get("Charlie"), "Charlie's neighbor count");
        assertEquals(3L, neighborCounts.get("Bob"), "Bob's neighbor count");
        assertEquals(2L, neighborCounts.get("Alice"), "Alice's neighbor count");
    }

    @Test
    @DisplayName("Window function with NULL values and DESC ordering - NULLS FIRST semantics")
    void testWindowFunctionWithNullValuesDescOrdering() throws Exception {
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
        assertTrue(sql.contains("NULLS FIRST"),
                "SQL should contain NULLS FIRST for DESC ordering");
        assertTrue(sql.contains("DESC NULLS FIRST"),
                "SQL should have DESC NULLS FIRST");
    }

    @Test
    @DisplayName("Decimal RANGE frame bounds - 0.5D->_range(2.5)")
    void testDecimalRangeFrameBounds() throws Exception {
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
        assertTrue(sql.contains("RANGE BETWEEN"),
                "SQL should contain RANGE BETWEEN");
        assertTrue(sql.contains("0.5 FOLLOWING"),
                "SQL should contain 0.5 FOLLOWING for start bound");
        assertTrue(sql.contains("2.5 FOLLOWING"),
                "SQL should contain 2.5 FOLLOWING for end bound");
    }

    @Test
    @DisplayName("Execute STDDEV window aggregate via Pure → QueryService → DuckDB")
    void testExecuteStdDevWindowViaPure() throws Exception {
        String pureQuery = """
                Employee.all()
                    ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                    ->extend(over(~department), ~deptStdDev:{p,w,r|$p->stdDev($w,$r).salary})
                """;

        String sql = generateSql(pureQuery);
        assertTrue(sql.contains("STDDEV"), "SQL should contain STDDEV");
        assertTrue(sql.contains("OVER"), "SQL should contain OVER");
        assertTrue(sql.contains("PARTITION BY"), "SQL should contain PARTITION BY");

        var result = executeQuery(pureQuery);
        assertEquals(6, result.rowCount(), "Should have 6 employees");

        assertTrue(result.columns().stream()
                .anyMatch(c -> c.name().equals("deptStdDev")),
                "Should have deptStdDev column");
    }

    @Test
    @DisplayName("Execute VARIANCE window aggregate via Pure → QueryService → DuckDB")
    void testExecuteVarianceWindowViaPure() throws Exception {
        String pureQuery = """
                Employee.all()
                    ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                    ->extend(over(~department), ~deptVar:{p,w,r|$p->variance($w,$r).salary})
                """;

        String sql = generateSql(pureQuery);
        assertTrue(sql.contains("VARIANCE"), "SQL should contain VARIANCE");
        assertTrue(sql.contains("OVER"), "SQL should contain OVER");
        assertTrue(sql.contains("PARTITION BY"), "SQL should contain PARTITION BY");

        var result = executeQuery(pureQuery);
        assertEquals(6, result.rowCount(), "Should have 6 employees");
    }

    @Test
    @DisplayName("Execute PERCENT_RANK via Pure → QueryService → DuckDB")
    void testExecutePercentRankViaPure() throws Exception {
        String pureQuery = """
                Employee.all()
                    ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                    ->extend(over(~department, ~salary->desc()), ~pctRank:{p,w,r| $p->percentRank($w,$r)})
                """;

        String sql = generateSql(pureQuery);
        assertTrue(sql.contains("PERCENT_RANK"), "SQL should contain PERCENT_RANK");

        var result = executeQuery(pureQuery);
        assertEquals(6, result.rowCount(), "Should have 6 employees");

        for (var row : result.rows()) {
            int deptIdx = findColumnIndex(result, "department");
            int nameIdx = findColumnIndex(result, "name");
            int pctIdx = findColumnIndex(result, "pctRank");

            if ("Engineering".equals(row.get(deptIdx))) {
                double pctVal = ((Number) row.get(pctIdx)).doubleValue();
                String name = (String) row.get(nameIdx);
                switch (name) {
                    case "Alice" -> assertEquals(0.0, pctVal, 0.01, "Alice should have pctRank 0.0 (highest)");
                    case "Charlie" -> assertEquals(1.0, pctVal, 0.01, "Charlie should have pctRank 1.0 (lowest)");
                }
            }
        }
    }

    @Test
    @DisplayName("Execute CUME_DIST via Pure → QueryService → DuckDB")
    void testExecuteCumeDistViaPure() throws Exception {
        String pureQuery = """
                Employee.all()
                    ->project({e | $e.name}, {e | $e.department}, {e | $e.salary})
                    ->extend(over(~department, ~salary->desc()), ~cumeDist:{p,w,r| $p->cumulativeDistribution($w,$r)})
                """;

        String sql = generateSql(pureQuery);
        assertTrue(sql.contains("CUME_DIST"), "SQL should contain CUME_DIST");

        var result = executeQuery(pureQuery);
        assertEquals(6, result.rowCount(), "Should have 6 employees");

        for (var row : result.rows()) {
            int deptIdx = findColumnIndex(result, "department");
            int nameIdx = findColumnIndex(result, "name");
            int cdIdx = findColumnIndex(result, "cumeDist");

            if ("Engineering".equals(row.get(deptIdx))) {
                double cdVal = ((Number) row.get(cdIdx)).doubleValue();
                String name = (String) row.get(nameIdx);
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
        assertTrue(sql.contains("ROUND"), "SQL should contain ROUND wrapping the window function");
        assertTrue(sql.contains("CUME_DIST"), "SQL should contain CUME_DIST");

        var result = executeQuery(pureQuery);
        assertEquals(6, result.rowCount(), "Should have 6 employees");

        for (var row : result.rows()) {
            int deptIdx = findColumnIndex(result, "department");
            int nameIdx = findColumnIndex(result, "name");
            int cdIdx = findColumnIndex(result, "cumeDist");

            if ("Engineering".equals(row.get(deptIdx))) {
                double cdVal = ((Number) row.get(cdIdx)).doubleValue();
                String name = (String) row.get(nameIdx);
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
        assertTrue(sql.contains("SUM"), "SQL should contain SUM");
        assertTrue(sql.contains("OVER"), "SQL should contain OVER (empty window = entire relation)");

        var result = executeQuery(pureQuery);
        assertEquals(6, result.rowCount(), "Should have 6 employees");

        int expectedTotal = 100000 + 90000 + 80000 + 85000 + 75000 + 70000;
        for (var row : result.rows()) {
            long totalSalary = ((Number) row.get(findColumnIndex(result, "totalSalary"))).longValue();
            assertEquals(expectedTotal, totalSalary,
                    "Total salary should be same for all rows (entire relation aggregate)");
        }
    }
}
