package org.finos.legend.engine.sql;

import org.finos.legend.engine.plan.*;
import org.finos.legend.engine.sql.ast.SelectStatement;
import org.finos.legend.engine.store.RelationalMapping;
import org.finos.legend.engine.test.AbstractDatabaseTest;
import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLDialect;
import org.finos.legend.engine.transpiler.SQLGenerator;
import org.finos.legend.pure.dsl.PureCompiler;
import org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder;
import org.finos.legend.pure.dsl.definition.ServiceDefinition;

import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Pure-integrated SQL Shim tests with TRUE E2E execution.
 * Input SQL → Parse → Compile → Generate → Execute on DuckDB → Verify Results
 */
@DisplayName("Pure-Integrated SQL Shim E2E Tests")
class PureIntegratedSQLShimTest extends AbstractDatabaseTest {

    private SQLCompiler fullCompiler;
    private SQLGenerator sqlGenerator;
    private PureCompiler pureCompiler;

    // Service definition for testing service() function
    private static final String ALL_PERSONS_SERVICE = """
            Service model::AllPersons
            {
                pattern: '/api/persons';
                function: |Person.all()->project({p | $p.firstName}, {p | $p.lastName}, {p | $p.age});
                documentation: 'Returns all people';
            }
            """;

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

        pureCompiler = new PureCompiler(mappingRegistry, modelBuilder);
        sqlGenerator = new SQLGenerator(DuckDBDialect.INSTANCE);

        // Add service to model builder
        ServiceDefinition serviceDef = PureDefinitionBuilder.parseServiceDefinition(ALL_PERSONS_SERVICE);
        modelBuilder.addService(serviceDef);

        fullCompiler = new SQLCompiler(
                // TableResolver
                (schema, tableName) -> modelBuilder != null ? modelBuilder.getTable(tableName) : null,
                // ClassResolver - uses MappingRegistry (registers by both qualified and simple
                // name)
                (className) -> {
                    Optional<RelationalMapping> mapping = mappingRegistry.findByClassName(className);
                    return mapping.map(RelationalMapping::table).orElse(null);
                },
                // ServiceResolver - looks up service by pattern and compiles its function body
                (servicePath) -> {
                    // Find service by pattern
                    ServiceDefinition service = findServiceByPattern(servicePath);
                    if (service == null)
                        return null;
                    // Compile the service's Pure function body
                    return pureCompiler.compile(service.functionBody());
                });
    }

    /** Find service by pattern (e.g., '/api/persons') */
    private ServiceDefinition findServiceByPattern(String pattern) {
        ServiceDefinition service = modelBuilder.getService("model::AllPersons");
        if (service != null && service.pattern().equals(pattern)) {
            return service;
        }
        return null;
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    /**
     * TRUE E2E: Input SQL → Parse → Compile → Generate → Execute → Results
     */
    private List<Map<String, Object>> executeSQL(String inputSql) throws SQLException {
        SelectStatement ast = new SelectParser(inputSql).parseSelect();
        RelationNode ir = fullCompiler.compile(ast);
        String generatedSql = sqlGenerator.generate(ir);

        System.out.println("Input SQL:     " + inputSql);
        System.out.println("Generated SQL: " + generatedSql);

        List<Map<String, Object>> results = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(generatedSql)) {
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();

            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(meta.getColumnLabel(i), rs.getObject(i));
                }
                results.add(row);
            }
        }
        return results;
    }

    // ==================== table() E2E Execution ====================

    @Nested
    @DisplayName("table() E2E Execution")
    class TableFunctionE2ETests {

        @Test
        @DisplayName("SELECT columns FROM table()")
        void testTableSelectColumns() throws SQLException {
            List<Map<String, Object>> results = executeSQL(
                    "SELECT FIRST_NAME, LAST_NAME, AGE_VAL FROM table('store::PersonDatabase.T_PERSON')");

            assertFalse(results.isEmpty(), "Should return rows");
            assertTrue(results.getFirst().containsKey("FIRST_NAME"));
        }

        @Test
        @DisplayName("SELECT with WHERE filter")
        void testTableWithWhere() throws SQLException {
            List<Map<String, Object>> results = executeSQL(
                    "SELECT FIRST_NAME, LAST_NAME FROM table('store::PersonDatabase.T_PERSON') WHERE FIRST_NAME = 'John'");

            assertFalse(results.isEmpty(), "Should find John");
            assertEquals("John", results.getFirst().get("FIRST_NAME"));
        }

        @Test
        @DisplayName("SELECT with ORDER BY and LIMIT")
        void testTableWithOrderAndLimit() throws SQLException {
            List<Map<String, Object>> results = executeSQL(
                    "SELECT FIRST_NAME FROM table('store::PersonDatabase.T_PERSON') ORDER BY FIRST_NAME LIMIT 2");

            assertEquals(2, results.size());
        }
    }

    // ==================== class() E2E Execution ====================

    @Nested
    @DisplayName("class() E2E Execution")
    class ClassFunctionE2ETests {

        @Test
        @DisplayName("SELECT columns FROM class('Person') - uses simple name lookup")
        void testClassSelectColumns() throws SQLException {
            // Works because MappingRegistry.register() stores by BOTH:
            // - qualifiedName: "model::Person"
            // - simpleName: "Person"
            List<Map<String, Object>> results = executeSQL(
                    "SELECT FIRST_NAME, LAST_NAME, AGE_VAL FROM class('Person')");

            assertFalse(results.isEmpty(), "Should return persons");
        }

        @Test
        @DisplayName("SELECT columns FROM class('model::Person') - qualified name")
        void testClassQualifiedName() throws SQLException {
            List<Map<String, Object>> results = executeSQL(
                    "SELECT FIRST_NAME, LAST_NAME FROM class('model::Person')");

            assertFalse(results.isEmpty(), "Should return persons with qualified name");
        }

        @Test
        @DisplayName("SELECT FROM class() with WHERE")
        void testClassWithFilter() throws SQLException {
            List<Map<String, Object>> results = executeSQL(
                    "SELECT FIRST_NAME, AGE_VAL FROM class('Person') WHERE AGE_VAL > 25");

            for (Map<String, Object> row : results) {
                assertTrue((Integer) row.get("AGE_VAL") > 25, "All ages should be > 25");
            }
        }
    }

    // ==================== service() E2E Execution ====================

    @Nested
    @DisplayName("service() E2E Execution")
    class ServiceFunctionE2ETests {

        @Test
        @DisplayName("SELECT FROM service('/api/persons') - executes service query")
        void testServiceExecution() throws SQLException {
            List<Map<String, Object>> results = executeSQL(
                    "SELECT firstName, lastName FROM service('/api/persons')");

            assertFalse(results.isEmpty(), "Should return persons from service");
            assertTrue(results.getFirst().containsKey("firstName"), "Should have firstName column");
            assertTrue(results.getFirst().containsKey("lastName"), "Should have lastName column");
            assertTrue(results.size() >= 3, "Should have at least 3 persons from test data");
        }

        @Test
        @DisplayName("SELECT with WHERE filter on service() results")
        void testServiceWithFilter() throws SQLException {
            List<Map<String, Object>> results = executeSQL(
                    "SELECT firstName FROM service('/api/persons') WHERE firstName = 'John'");

            assertFalse(results.isEmpty(), "Should find John");
            assertEquals("John", results.getFirst().get("firstName"));
        }
    }

    // ==================== Comparison: table() vs class() ====================

    @Nested
    @DisplayName("table() vs class() equivalence")
    class ComparisonTests {

        @Test
        @DisplayName("table() and class() return same data")
        void testTableAndClassEquivalent() throws SQLException {
            List<Map<String, Object>> tableResults = executeSQL(
                    "SELECT FIRST_NAME, LAST_NAME FROM table('store::PersonDatabase.T_PERSON') ORDER BY FIRST_NAME");

            List<Map<String, Object>> classResults = executeSQL(
                    "SELECT FIRST_NAME, LAST_NAME FROM class('Person') ORDER BY FIRST_NAME");

            assertEquals(tableResults.size(), classResults.size(), "Same row count");

            for (int i = 0; i < tableResults.size(); i++) {
                assertEquals(tableResults.get(i).get("FIRST_NAME"), classResults.get(i).get("FIRST_NAME"));
            }
        }
    }
}
