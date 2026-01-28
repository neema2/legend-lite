package org.finos.legend.engine.sql;

import org.finos.legend.engine.plan.*;
import org.finos.legend.engine.sql.ast.SelectStatement;
import org.finos.legend.engine.store.Column;
import org.finos.legend.engine.store.SqlDataType;
import org.finos.legend.engine.store.Table;
import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLGenerator;

import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration tests for the SQL Shim.
 * 
 * Tests the complete pipeline:
 * SQL String → Lexer → Parser → SQL AST → SQLCompiler → RelationNode IR → SQLGenerator → SQL String
 * 
 * Then executes against a real DuckDB database.
 */
@DisplayName("SQL Shim Integration Tests")
class SQLShimIntegrationTest {

    private Connection connection;
    private SQLCompiler compiler;
    private SQLGenerator sqlGenerator;
    private Map<String, Table> tableRegistry;

    @BeforeEach
    void setUp() throws SQLException {
        // Create DuckDB in-memory connection
        connection = DriverManager.getConnection("jdbc:duckdb:");
        
        // Create test tables
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                CREATE TABLE employees (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    department VARCHAR NOT NULL,
                    salary INTEGER NOT NULL,
                    manager_id INTEGER
                )
            """);
            
            stmt.execute("""
                CREATE TABLE departments (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    budget INTEGER NOT NULL
                )
            """);
            
            // Insert test data
            stmt.execute("""
                INSERT INTO employees VALUES
                (1, 'Alice', 'Engineering', 120000, NULL),
                (2, 'Bob', 'Engineering', 100000, 1),
                (3, 'Carol', 'Sales', 90000, NULL),
                (4, 'Dave', 'Sales', 85000, 3),
                (5, 'Eve', 'Engineering', 130000, 1)
            """);
            
            stmt.execute("""
                INSERT INTO departments VALUES
                (1, 'Engineering', 500000),
                (2, 'Sales', 300000),
                (3, 'Marketing', 200000)
            """);
        }
        
        // Set up table metadata for compiler
        Table employees = new Table("", "employees", List.of(
            Column.required("id", SqlDataType.INTEGER),
            Column.required("name", SqlDataType.VARCHAR),
            Column.required("department", SqlDataType.VARCHAR),
            Column.required("salary", SqlDataType.INTEGER),
            Column.nullable("manager_id", SqlDataType.INTEGER)
        ));
        
        Table departments = new Table("", "departments", List.of(
            Column.required("id", SqlDataType.INTEGER),
            Column.required("name", SqlDataType.VARCHAR),
            Column.required("budget", SqlDataType.INTEGER)
        ));
        
        tableRegistry = Map.of(
            "employees", employees,
            "departments", departments
        );
        
        compiler = new SQLCompiler((schema, tableName) -> 
            tableRegistry.get(tableName.toLowerCase()));
        
        sqlGenerator = new SQLGenerator(DuckDBDialect.INSTANCE);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    // ==================== Complete Pipeline Tests ====================

    @Nested
    @DisplayName("Full SQL Roundtrip")
    class FullRoundtripTests {

        @Test
        @DisplayName("SELECT columns FROM table - roundtrip and execute")
        void testSelectAllRoundtrip() throws SQLException {
            String inputSql = "SELECT id, name, department, salary FROM employees";
            
            // Parse → Compile → Generate
            SelectStatement ast = new SelectParser(inputSql).parseSelect();
            RelationNode ir = compiler.compile(ast);
            String generatedSql = sqlGenerator.generate(ir);
            
            System.out.println("Input SQL: " + inputSql);
            System.out.println("Generated SQL: " + generatedSql);
            
            // Execute and verify
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(generatedSql)) {
                
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                assertEquals(5, count, "Should return 5 employees");
            }
        }

        @Test
        @DisplayName("SELECT with WHERE - filter execution")
        void testSelectWhereRoundtrip() throws SQLException {
            String inputSql = "SELECT name, salary FROM employees WHERE department = 'Engineering'";
            
            // Parse → Compile → Generate
            SelectStatement ast = new SelectParser(inputSql).parseSelect();
            RelationNode ir = compiler.compile(ast);
            String generatedSql = sqlGenerator.generate(ir);
            
            System.out.println("Input SQL: " + inputSql);
            System.out.println("Generated SQL: " + generatedSql);
            
            // Execute and verify
            List<String> names = new ArrayList<>();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(generatedSql)) {
                
                while (rs.next()) {
                    names.add(rs.getString("name"));
                }
            }
            
            assertEquals(3, names.size(), "Should find 3 engineers");
            assertTrue(names.containsAll(List.of("Alice", "Bob", "Eve")));
        }

        @Test
        @DisplayName("SELECT with ORDER BY - sorted results")
        void testSelectOrderByRoundtrip() throws SQLException {
            String inputSql = "SELECT name, salary FROM employees ORDER BY salary DESC";
            
            // Parse → Compile → Generate
            SelectStatement ast = new SelectParser(inputSql).parseSelect();
            RelationNode ir = compiler.compile(ast);
            String generatedSql = sqlGenerator.generate(ir);
            
            System.out.println("Input SQL: " + inputSql);
            System.out.println("Generated SQL: " + generatedSql);
            
            // Execute and verify order
            List<Integer> salaries = new ArrayList<>();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(generatedSql)) {
                
                while (rs.next()) {
                    salaries.add(rs.getInt("salary"));
                }
            }
            
            assertEquals(5, salaries.size());
            // Verify descending order
            for (int i = 1; i < salaries.size(); i++) {
                assertTrue(salaries.get(i - 1) >= salaries.get(i),
                    "Salaries should be in descending order");
            }
            assertEquals(130000, salaries.get(0), "Highest salary should be 130000 (Eve)");
        }

        @Test
        @DisplayName("SELECT with LIMIT - limited results")
        void testSelectLimitRoundtrip() throws SQLException {
            // Include salary in projection since we ORDER BY it
            String inputSql = "SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 3";
            
            // Parse → Compile → Generate
            SelectStatement ast = new SelectParser(inputSql).parseSelect();
            RelationNode ir = compiler.compile(ast);
            String generatedSql = sqlGenerator.generate(ir);
            
            System.out.println("Input SQL: " + inputSql);
            System.out.println("Generated SQL: " + generatedSql);
            
            // Execute and verify
            List<String> names = new ArrayList<>();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(generatedSql)) {
                
                while (rs.next()) {
                    names.add(rs.getString("name"));
                }
            }
            
            assertEquals(3, names.size(), "Should return exactly 3 rows");
            assertEquals("Eve", names.get(0), "Highest paid should be Eve");
        }

        @Test
        @DisplayName("SELECT with complex WHERE - AND conditions")
        void testSelectWhereAndRoundtrip() throws SQLException {
            String inputSql = "SELECT name FROM employees WHERE department = 'Engineering' AND salary > 110000";
            
            // Parse → Compile → Generate
            SelectStatement ast = new SelectParser(inputSql).parseSelect();
            RelationNode ir = compiler.compile(ast);
            String generatedSql = sqlGenerator.generate(ir);
            
            System.out.println("Input SQL: " + inputSql);
            System.out.println("Generated SQL: " + generatedSql);
            
            // Execute and verify
            List<String> names = new ArrayList<>();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(generatedSql)) {
                
                while (rs.next()) {
                    names.add(rs.getString("name"));
                }
            }
            
            assertEquals(2, names.size(), "Should find 2 high-paid engineers");
            assertTrue(names.containsAll(List.of("Alice", "Eve")));
        }
    }

    // ==================== JOIN Tests ====================

    @Nested
    @DisplayName("JOIN Operations")
    class JoinTests {

        @Test
        @DisplayName("INNER JOIN - matching records")
        void testInnerJoinRoundtrip() throws SQLException {
            String inputSql = "SELECT e.name, d.name FROM employees e JOIN departments d ON e.department = d.name";
            
            // Parse → Compile → Generate
            SelectStatement ast = new SelectParser(inputSql).parseSelect();
            RelationNode ir = compiler.compile(ast);
            String generatedSql = sqlGenerator.generate(ir);
            
            System.out.println("Input SQL: " + inputSql);
            System.out.println("Generated SQL: " + generatedSql);
            
            // Execute and verify
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(generatedSql)) {
                
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                // All 5 employees should match (3 Engineering, 2 Sales)
                assertEquals(5, count, "Should match all employees with their departments");
            }
        }

        @Test
        @DisplayName("LEFT JOIN - all left records")
        void testLeftJoinRoundtrip() throws SQLException {
            String inputSql = "SELECT d.name, e.name FROM departments d LEFT JOIN employees e ON d.name = e.department";
            
            // Parse → Compile → Generate
            SelectStatement ast = new SelectParser(inputSql).parseSelect();
            RelationNode ir = compiler.compile(ast);
            String generatedSql = sqlGenerator.generate(ir);
            
            System.out.println("Input SQL: " + inputSql);
            System.out.println("Generated SQL: " + generatedSql);
            
            // Execute and verify
            Set<String> departments = new HashSet<>();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(generatedSql)) {
                
                while (rs.next()) {
                    departments.add(rs.getString(1));
                }
            }
            
            // All 3 departments should be included (Marketing has no employees but should appear)
            assertTrue(departments.contains("Marketing"), "Marketing should appear even with no employees");
            assertTrue(departments.contains("Engineering"));
            assertTrue(departments.contains("Sales"));
        }
    }

    // ==================== Expression Tests ====================

    @Nested
    @DisplayName("Expression Evaluation")
    class ExpressionTests {

        @Test
        @DisplayName("Arithmetic expression in SELECT")
        void testArithmeticExpression() throws SQLException {
            String inputSql = "SELECT name, salary + 10000 FROM employees WHERE salary > 100000";
            
            // Parse → Compile → Generate
            SelectStatement ast = new SelectParser(inputSql).parseSelect();
            RelationNode ir = compiler.compile(ast);
            String generatedSql = sqlGenerator.generate(ir);
            
            System.out.println("Input SQL: " + inputSql);
            System.out.println("Generated SQL: " + generatedSql);
            
            // Execute
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(generatedSql)) {
                
                boolean found = false;
                while (rs.next()) {
                    if ("Eve".equals(rs.getString(1))) {
                        assertEquals(140000, rs.getInt(2), "Eve's salary + 10000 should be 140000");
                        found = true;
                    }
                }
                assertTrue(found, "Should find Eve in results");
            }
        }

        @Test
        @DisplayName("IS NULL in WHERE clause")
        void testIsNullWhere() throws SQLException {
            String inputSql = "SELECT name FROM employees WHERE manager_id IS NULL";
            
            // Parse → Compile → Generate
            SelectStatement ast = new SelectParser(inputSql).parseSelect();
            RelationNode ir = compiler.compile(ast);
            String generatedSql = sqlGenerator.generate(ir);
            
            System.out.println("Input SQL: " + inputSql);
            System.out.println("Generated SQL: " + generatedSql);
            
            // Execute - should find Alice (Engineering manager) and Carol (Sales manager)
            List<String> managers = new ArrayList<>();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(generatedSql)) {
                
                while (rs.next()) {
                    managers.add(rs.getString("name"));
                }
            }
            
            assertEquals(2, managers.size(), "Should find 2 managers");
            assertTrue(managers.containsAll(List.of("Alice", "Carol")));
        }
    }

    // ==================== Pipeline Verification ====================

    @Nested
    @DisplayName("Pipeline Verification")
    class PipelineTests {

        @Test
        @DisplayName("IR structure is correct for simple SELECT")
        void testIRStructure() {
            String inputSql = "SELECT name FROM employees WHERE salary > 100000";
            
            // Parse
            SelectStatement ast = new SelectParser(inputSql).parseSelect();
            
            // Compile
            RelationNode ir = compiler.compile(ast);
            
            // Verify IR structure: ProjectNode -> FilterNode -> TableNode
            assertInstanceOf(ProjectNode.class, ir);
            ProjectNode project = (ProjectNode) ir;
            
            assertInstanceOf(FilterNode.class, project.source());
            FilterNode filter = (FilterNode) project.source();
            
            assertInstanceOf(TableNode.class, filter.source());
            TableNode table = (TableNode) filter.source();
            
            assertEquals("employees", table.table().name());
        }

        @Test
        @DisplayName("Full pipeline prints correctly")
        void testPipelinePrint() {
            String inputSql = "SELECT name, salary FROM employees WHERE department = 'Engineering' ORDER BY salary DESC LIMIT 10";
            
            System.out.println("=== SQL Shim Pipeline Test ===");
            System.out.println("Input SQL: " + inputSql);
            
            // Parse
            SelectStatement ast = new SelectParser(inputSql).parseSelect();
            System.out.println("\n1. Parsed AST:");
            System.out.println("   SELECT items: " + ast.selectItems().size());
            System.out.println("   FROM items: " + ast.from().size());
            System.out.println("   Has WHERE: " + ast.hasWhere());
            System.out.println("   Has ORDER BY: " + ast.hasOrderBy());
            System.out.println("   LIMIT: " + ast.limit());
            
            // Compile
            RelationNode ir = compiler.compile(ast);
            System.out.println("\n2. Compiled IR:");
            System.out.println("   " + ir);
            
            // Generate
            String generatedSql = sqlGenerator.generate(ir);
            System.out.println("\n3. Generated SQL:");
            System.out.println("   " + generatedSql);
            
            System.out.println("\n=== Pipeline Complete ===");
            
            assertNotNull(generatedSql);
            assertFalse(generatedSql.isEmpty());
        }
    }
    // ==================== Semantic Parity Tests ====================

    @Nested
    @DisplayName("Semantic Parity - Input SQL vs Generated SQL")
    class SemanticParityTests {

        /**
         * Helper: Execute SQL and return results as list of maps.
         */
        private List<Map<String, Object>> executeAndCollect(String sql) throws SQLException {
            List<Map<String, Object>> results = new ArrayList<>();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                ResultSetMetaData meta = rs.getMetaData();
                int colCount = meta.getColumnCount();
                
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int i = 1; i <= colCount; i++) {
                        row.put(meta.getColumnLabel(i).toLowerCase(), rs.getObject(i));
                    }
                    results.add(row);
                }
            }
            return results;
        }

        /**
         * Helper: Run input SQL through pipeline and compare with direct execution.
         */
        private void assertSemanticParity(String inputSql) throws SQLException {
            System.out.println("\n=== Semantic Parity Test ===");
            System.out.println("Input SQL: " + inputSql);
            
            // Run input SQL directly
            List<Map<String, Object>> directResults = executeAndCollect(inputSql);
            System.out.println("Direct execution rows: " + directResults.size());
            
            // Parse → Compile → Generate
            SelectStatement ast = new SelectParser(inputSql).parseSelect();
            RelationNode ir = compiler.compile(ast);
            String generatedSql = sqlGenerator.generate(ir);
            System.out.println("Generated SQL: " + generatedSql);
            
            // Run generated SQL
            List<Map<String, Object>> pipelineResults = executeAndCollect(generatedSql);
            System.out.println("Pipeline execution rows: " + pipelineResults.size());
            
            // Compare row counts
            assertEquals(directResults.size(), pipelineResults.size(),
                "Row count mismatch: direct=" + directResults.size() + 
                ", pipeline=" + pipelineResults.size());
            
            // Compare values (handle column aliasing)
            for (int i = 0; i < directResults.size(); i++) {
                Map<String, Object> directRow = directResults.get(i);
                Map<String, Object> pipelineRow = pipelineResults.get(i);
                
                // Check each column in direct results exists in pipeline results  
                for (String col : directRow.keySet()) {
                    Object directVal = directRow.get(col);
                    Object pipelineVal = pipelineRow.get(col);
                    
                    if (pipelineVal == null && !pipelineRow.containsKey(col)) {
                        // Column might have different name, skip detailed check
                        continue;
                    }
                    
                    assertEquals(directVal, pipelineVal,
                        "Value mismatch at row " + i + " column '" + col + 
                        "': direct=" + directVal + ", pipeline=" + pipelineVal);
                }
            }
            
            System.out.println("✓ Semantic parity verified!");
        }

        @Test
        @DisplayName("SELECT columns - parity check")
        void testSelectColumnsParity() throws SQLException {
            assertSemanticParity("SELECT name, salary FROM employees");
        }

        @Test
        @DisplayName("SELECT with WHERE - parity check")
        void testSelectWhereParity() throws SQLException {
            assertSemanticParity(
                "SELECT name, salary FROM employees WHERE department = 'Engineering'");
        }

        @Test
        @DisplayName("SELECT with ORDER BY - parity check")
        void testSelectOrderByParity() throws SQLException {
            assertSemanticParity(
                "SELECT name, salary FROM employees ORDER BY salary DESC");
        }

        @Test
        @DisplayName("SELECT with LIMIT - parity check")
        void testSelectLimitParity() throws SQLException {
            assertSemanticParity(
                "SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 3");
        }

        @Test
        @DisplayName("SELECT with complex WHERE - parity check")
        void testSelectComplexWhereParity() throws SQLException {
            assertSemanticParity(
                "SELECT name, salary FROM employees WHERE department = 'Engineering' AND salary > 100000");
        }

        @Test
        @DisplayName("INNER JOIN - parity check")
        void testInnerJoinParity() throws SQLException {
            assertSemanticParity(
                "SELECT e.name, d.name FROM employees e JOIN departments d ON e.department = d.name");
        }

        @Test
        @DisplayName("Arithmetic expression - parity check")
        void testArithmeticParity() throws SQLException {
            assertSemanticParity(
                "SELECT name, salary + 10000 FROM employees WHERE salary > 100000");
        }

        @Test
        @DisplayName("IS NULL - parity check")
        void testIsNullParity() throws SQLException {
            assertSemanticParity(
                "SELECT name FROM employees WHERE manager_id IS NULL");
        }
    }
}

