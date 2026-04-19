package com.gs.legend.test;

import com.gs.legend.model.def.ServiceDefinition;
import com.gs.legend.plan.GenericType;
import com.gs.legend.parser.PureParser;
import com.gs.legend.service.ServiceRegistry;
import com.gs.legend.service.ServiceServer;
import com.gs.legend.sqlgen.DuckDBDialect;
import com.gs.legend.sqlgen.SQLDialect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the hosted service infrastructure.
 * 
 * Uses real DuckDB database and full Pure language compilation pipeline:
 * - Pure query → PureCompiler → RelationNode → SQLGenerator → SQL → DuckDB →
 * JSON
 * 
 * Tests:
 * - Service definition parsing (Pure syntax)
 * - Real Pure query compilation to SQL
 * - HTTP server with service routing
 * - ResultSet JSON serialization
 * - Path parameter binding
 * - Association navigation (JOINs and EXISTS)
 */
@DisplayName("Hosted Service Integration Tests")
class HostedServiceIntegrationTest extends AbstractDatabaseTest {

    private ServiceServer server;
    private int serverPort;

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
        return "jdbc:duckdb:"; // In-memory DuckDB
    }

    // ==================== Pure Service Definitions ====================

    private static final String ALL_PERSONS_SERVICE = """
            import model::*;

            Service model::AllPersons
            {
                pattern: '/api/persons';
                documentation: 'Returns all people';
                execution: Single
                {
                    query: |Person.all()->project(~[firstName:p|$p.firstName, lastName:p|$p.lastName, age:p|$p.age]);
                }
            }
            """;

    private static final String PERSONS_BY_LASTNAME_SERVICE = """
            import model::*;

            Service model::PersonsByLastName
            {
                pattern: '/api/persons/{lastName}';
                documentation: 'Returns people by last name';
                execution: Single
                {
                    query: |Person.all()->filter({p | $p.lastName == $lastName})->project(~[firstName:p|$p.firstName, lastName:p|$p.lastName]);
                }
            }
            """;

    private static final String PERSONS_WITH_ADDRESSES_SERVICE = """
            import model::*;

            Service model::PersonsWithAddresses
            {
                pattern: '/api/persons-with-addresses';
                documentation: 'Returns all persons with their addresses (LEFT OUTER JOIN)';
                execution: Single
                {
                    query: |Person.all()->project(~[firstName:p|$p.firstName, street:p|$p.addresses.street, city:p|$p.addresses.city]);
                }
            }
            """;

    private static final String PERSONS_BY_CITY_SERVICE = """
            import model::*;

            Service model::PersonsByCity
            {
                pattern: '/api/persons-by-city/{city}';
                documentation: 'Returns persons who have an address in the specified city (EXISTS subquery)';
                execution: Single
                {
                    query: |Person.all()->filter({p | $p.addresses.city == $city})->project(~[firstName:p|$p.firstName]);
                }
            }
            """;

    @BeforeEach
    void setUp() throws Exception {
        // Set up DuckDB connection and database
        connection = DriverManager.getConnection(getJdbcUrl());
        setupDatabase(); // Creates T_PERSON and T_ADDRESS tables
        setupMappingRegistry(); // Sets up Pure model, mapping registry, and compiler

        // Create service registry with REAL Pure-compiled executors
        ServiceRegistry registry = new ServiceRegistry();

        // Service 1: Get all persons
        registerService(registry, ALL_PERSONS_SERVICE);

        // Service 2: Get persons by lastName (path parameter)
        registerServiceWithFilter(registry, PERSONS_BY_LASTNAME_SERVICE, "lastName");

        // Service 3: Get persons with addresses (LEFT OUTER JOIN)
        registerService(registry, PERSONS_WITH_ADDRESSES_SERVICE);

        // Service 4: Get persons by city (EXISTS subquery via association filter)
        registerServiceWithFilter(registry, PERSONS_BY_CITY_SERVICE, "city");

        // Start server on random available port
        server = new ServiceServer(0, registry, () -> connection);
        server.start();
        serverPort = server.getPort();
    }

    /**
     * Registers a service that executes its Pure function via QueryService.
     * No path parameters - just compile and run.
     */
    private void registerService(ServiceRegistry registry, String servicePure) {
        ServiceDefinition def = PureParser.parseSingle(servicePure, ServiceDefinition.class);
        String pureQuery = def.functionBody();
        String pureSource = getCompletePureModelWithRuntime();

        registry.register(def, (pathParams, queryParams, conn) -> {
            // Execute via QueryService (parse → compile → SQL → execute)
            var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", conn);
            return result.toJsonArray();
        });
    }

    /**
     * Registers a service with a path parameter filter via QueryService.
     * The filter value is substituted into the WHERE clause.
     */
    private void registerServiceWithFilter(ServiceRegistry registry, String servicePure, String paramName) {
        ServiceDefinition def = PureParser.parseSingle(servicePure, ServiceDefinition.class);
        String pureQueryTemplate = def.functionBody();
        String pureSource = getCompletePureModelWithRuntime();

        registry.register(def, (pathParams, queryParams, conn) -> {
            String paramValue = pathParams.get(paramName);

            // Replace $paramName with the actual value in the Pure query
            // This is a simplified parameter binding - in production you'd use prepared
            // statements
            String pureQuery = pureQueryTemplate.replace("$" + paramName, "'" + paramValue + "'");

            // Execute via QueryService (parse → compile → SQL → execute)
            var result = queryService.execute(pureSource, pureQuery, "test::TestRuntime", conn);
            return result.toJsonArray();
        });
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.stop(0);
        }
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    // ==================== Service Definition Parsing Tests ====================

    @Test
    @DisplayName("Parse service definition with path parameter")
    void testParseServiceDefinition() {
        ServiceDefinition def = PureParser.parseSingle(PERSONS_BY_LASTNAME_SERVICE, ServiceDefinition.class);

        assertEquals("model::PersonsByLastName", def.qualifiedName());
        assertEquals("PersonsByLastName", def.simpleName());
        assertEquals("/api/persons/{lastName}", def.pattern());
        assertEquals(1, def.pathParams().size());
        assertEquals("lastName", def.pathParams().getFirst());
        assertEquals("Returns people by last name", def.documentation());
        assertTrue(def.functionBody().contains("Person.all()"));
    }

    @Test
    @DisplayName("Parse service definition without path parameters")
    void testParseServiceWithoutParams() {
        ServiceDefinition def = PureParser.parseSingle(ALL_PERSONS_SERVICE, ServiceDefinition.class);

        assertEquals("model::AllPersons", def.qualifiedName());
        assertEquals("/api/persons", def.pattern());
        assertTrue(def.pathParams().isEmpty());
    }

    @Test
    @DisplayName("Service pattern converts to regex correctly")
    void testPatternToRegex() {
        ServiceDefinition def = PureParser.parseSingle(PERSONS_BY_LASTNAME_SERVICE, ServiceDefinition.class);
        var regex = def.toRegexPattern();

        assertTrue(regex.matcher("/api/persons/Smith").matches());
        assertTrue(regex.matcher("/api/persons/Jones").matches());
        assertFalse(regex.matcher("/api/persons").matches());
        assertFalse(regex.matcher("/api/persons/Smith/extra").matches());
    }

    // ==================== HTTP Server with Real Pure Compilation
    // ====================

    @Test
    @DisplayName("GET /api/persons uses real Pure compilation → SQL → JSON")
    void testGetAllPersons() throws Exception {
        String response = httpGet("/api/persons");

        System.out.println("Response: " + response);

        // Verify it's valid JSON array
        assertTrue(response.startsWith("["));
        assertTrue(response.endsWith("]"));

        // Verify contains expected data from DuckDB
        assertTrue(response.contains("\"firstName\":\"John\""));
        assertTrue(response.contains("\"firstName\":\"Jane\""));
        assertTrue(response.contains("\"firstName\":\"Bob\""));
        assertTrue(response.contains("\"lastName\":\"Smith\""));
        assertTrue(response.contains("\"lastName\":\"Jones\""));
    }

    @Test
    @DisplayName("GET /api/persons/{lastName} uses real Pure filter → SQL WHERE")
    void testGetPersonsByLastName() throws Exception {
        String response = httpGet("/api/persons/Smith");

        System.out.println("Response: " + response);

        // Should contain John and Jane Smith
        assertTrue(response.contains("\"firstName\":\"John\""));
        assertTrue(response.contains("\"firstName\":\"Jane\""));

        // Should NOT contain Bob Jones
        assertFalse(response.contains("\"firstName\":\"Bob\""));
        assertFalse(response.contains("\"lastName\":\"Jones\""));
    }

    @Test
    @DisplayName("GET /api/persons/{lastName} with no matches returns empty array")
    void testGetPersonsNoMatches() throws Exception {
        String response = httpGet("/api/persons/Unknown");

        System.out.println("Response: " + response);

        // Should be empty array
        assertEquals("[]", response);
    }

    @Test
    @DisplayName("GET unknown path returns 404")
    void testNotFound() throws Exception {
        HttpURLConnection conn = (HttpURLConnection) URI.create("http://localhost:" + serverPort + "/api/unknown")
                .toURL().openConnection();
        conn.setRequestMethod("GET");

        assertEquals(404, conn.getResponseCode());
    }

    // ==================== Association Navigation Tests ====================

    @Test
    @DisplayName("GET /api/persons-with-addresses uses real LEFT OUTER JOIN")
    void testGetPersonsWithAddresses() throws Exception {
        String response = httpGet("/api/persons-with-addresses");

        System.out.println("Persons with addresses: " + response);

        // John has 2 addresses: 123 Main St (New York), 456 Oak Ave (Boston)
        assertTrue(response.contains("\"firstName\":\"John\""));
        assertTrue(response.contains("\"street\":\"123 Main St\""));
        assertTrue(response.contains("\"street\":\"456 Oak Ave\""));

        // Jane has 1 address: 789 Main Rd (Chicago)
        assertTrue(response.contains("\"firstName\":\"Jane\""));
        assertTrue(response.contains("\"street\":\"789 Main Rd\""));

        // Bob has 1 address: 999 Pine Lane (Detroit)
        assertTrue(response.contains("\"firstName\":\"Bob\""));
        assertTrue(response.contains("\"street\":\"999 Pine Lane\""));
    }

    @Test
    @DisplayName("GET /api/persons-by-city/{city} uses real EXISTS subquery")
    void testGetPersonsByCity() throws Exception {
        // John lives in Boston (one of his addresses)
        String bostonResponse = httpGet("/api/persons-by-city/Boston");
        System.out.println("Persons in Boston: " + bostonResponse);
        assertTrue(bostonResponse.contains("\"firstName\":\"John\""));
        assertFalse(bostonResponse.contains("\"firstName\":\"Jane\""));
        assertFalse(bostonResponse.contains("\"firstName\":\"Bob\""));

        // Jane lives in Chicago
        String chicagoResponse = httpGet("/api/persons-by-city/Chicago");
        System.out.println("Persons in Chicago: " + chicagoResponse);
        assertTrue(chicagoResponse.contains("\"firstName\":\"Jane\""));
        assertFalse(chicagoResponse.contains("\"firstName\":\"John\""));

        // No one in Miami
        String miamiResponse = httpGet("/api/persons-by-city/Miami");
        System.out.println("Persons in Miami: " + miamiResponse);
        assertEquals("[]", miamiResponse);
    }

    // ==================== JSON Serialization Tests ====================

    @Test
    @DisplayName("ExecutionResult.toJsonArray produces valid JSON for various types")
    void testJsonSerializer() throws Exception {
        // Build a TabularResult directly with known types
        var columns = java.util.List.of(
                new com.gs.legend.exec.Column("id", "Integer", "Integer"),
                new com.gs.legend.exec.Column("name", "String", "String"),
                new com.gs.legend.exec.Column("active", "Boolean", "Boolean"),
                new com.gs.legend.exec.Column("value", "Float", "Float"));
        var rows = java.util.List.of(
                new com.gs.legend.exec.Row(java.util.List.of(1, "test", true, 42.5)));
        var schema = new GenericType.Relation.Schema(
                java.util.Map.of("id", com.gs.legend.plan.GenericType.Primitive.INTEGER,
                        "name", com.gs.legend.plan.GenericType.Primitive.STRING,
                        "active", com.gs.legend.plan.GenericType.Primitive.BOOLEAN,
                        "value", com.gs.legend.plan.GenericType.Primitive.FLOAT),
                java.util.List.of());
        var result = new com.gs.legend.exec.ExecutionResult.TabularResult(
                columns, rows, schema, new com.gs.legend.plan.GenericType.Relation(schema));

        String json = result.toJsonArray();
        System.out.println("JSON: " + json);

        assertTrue(json.contains("\"id\":1"));
        assertTrue(json.contains("\"name\":\"test\""));
        assertTrue(json.contains("\"active\":true"));
        assertTrue(json.contains("\"value\":42.5"));
    }

    @Test
    @DisplayName("ExecutionResult.toJsonArray handles null values correctly")
    void testJsonSerializerNulls() throws Exception {
        var columns = java.util.List.of(
                new com.gs.legend.exec.Column("firstName", "String", "String"),
                new com.gs.legend.exec.Column("street", "String", "String"));
        var rows = java.util.List.of(
                new com.gs.legend.exec.Row(java.util.Arrays.asList("NoAddress", null)));
        var schema = new GenericType.Relation.Schema(
                java.util.Map.of("firstName", com.gs.legend.plan.GenericType.Primitive.STRING,
                        "street", com.gs.legend.plan.GenericType.Primitive.STRING),
                java.util.List.of());
        var result = new com.gs.legend.exec.ExecutionResult.TabularResult(
                columns, rows, schema, new com.gs.legend.plan.GenericType.Relation(schema));

        String json = result.toJsonArray();
        System.out.println("JSON with nulls: " + json);

        assertTrue(json.contains("\"firstName\":\"NoAddress\""));
        assertTrue(json.contains("\"street\":null"));
    }

    @Test
    @DisplayName("ExecutionResult.toJsonArray escapes special characters in strings")
    void testJsonSerializerEscaping() throws Exception {
        var columns = java.util.List.of(
                new com.gs.legend.exec.Column("firstName", "String", "String"),
                new com.gs.legend.exec.Column("lastName", "String", "String"));
        var rows = java.util.List.of(
                new com.gs.legend.exec.Row(java.util.List.of("Quote\"Test", "Tab\tPerson")));
        var schema = new GenericType.Relation.Schema(
                java.util.Map.of("firstName", com.gs.legend.plan.GenericType.Primitive.STRING,
                        "lastName", com.gs.legend.plan.GenericType.Primitive.STRING),
                java.util.List.of());
        var result = new com.gs.legend.exec.ExecutionResult.TabularResult(
                columns, rows, schema, new com.gs.legend.plan.GenericType.Relation(schema));

        String json = result.toJsonArray();
        System.out.println("JSON with escaping: " + json);

        // Should contain escaped quote and tab
        assertTrue(json.contains("\\\"")); // Escaped quote
        assertTrue(json.contains("\\t")); // Escaped tab
    }

    // ==================== Helper Methods ====================

    private String httpGet(String path) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) URI.create("http://localhost:" + serverPort + path).toURL()
                .openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);

        assertEquals(200, conn.getResponseCode(), "Expected 200 OK for path: " + path);

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            return response.toString();
        }
    }
}
