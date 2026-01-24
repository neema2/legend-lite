package org.finos.legend.engine.test;

import org.finos.legend.engine.server.*;
import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLDialect;
import org.finos.legend.pure.dsl.definition.*;
import org.junit.jupiter.api.*;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.*;

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
            Service model::AllPersons
            {
                pattern: '/api/persons';
                function: |Person.all()->project({p | $p.firstName}, {p | $p.lastName}, {p | $p.age});
                documentation: 'Returns all people';
            }
            """;

    private static final String PERSONS_BY_LASTNAME_SERVICE = """
            Service model::PersonsByLastName
            {
                pattern: '/api/persons/{lastName}';
                function: |Person.all()->filter({p | $p.lastName == $lastName})->project({p | $p.firstName}, {p | $p.lastName});
                documentation: 'Returns people by last name';
            }
            """;

    private static final String PERSONS_WITH_ADDRESSES_SERVICE = """
            Service model::PersonsWithAddresses
            {
                pattern: '/api/persons-with-addresses';
                function: |Person.all()->project({p | $p.firstName}, {p | $p.addresses.street}, {p | $p.addresses.city});
                documentation: 'Returns all persons with their addresses (LEFT OUTER JOIN)';
            }
            """;

    private static final String PERSONS_BY_CITY_SERVICE = """
            Service model::PersonsByCity
            {
                pattern: '/api/persons-by-city/{city}';
                function: |Person.all()->filter({p | $p.addresses.city == $city})->project({p | $p.firstName});
                documentation: 'Returns persons who have an address in the specified city (EXISTS subquery)';
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
        ServiceDefinition def = PureDefinitionParser.parseServiceDefinition(servicePure);
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
        ServiceDefinition def = PureDefinitionParser.parseServiceDefinition(servicePure);
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
        ServiceDefinition def = PureDefinitionParser.parseServiceDefinition(PERSONS_BY_LASTNAME_SERVICE);

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
        ServiceDefinition def = PureDefinitionParser.parseServiceDefinition(ALL_PERSONS_SERVICE);

        assertEquals("model::AllPersons", def.qualifiedName());
        assertEquals("/api/persons", def.pattern());
        assertTrue(def.pathParams().isEmpty());
    }

    @Test
    @DisplayName("Service pattern converts to regex correctly")
    void testPatternToRegex() {
        ServiceDefinition def = PureDefinitionParser.parseServiceDefinition(PERSONS_BY_LASTNAME_SERVICE);
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
    @DisplayName("BufferedResult.toJsonArray produces valid JSON for various types")
    void testJsonSerializer() throws Exception {
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT 1 as \"id\", 'test' as \"name\", true as \"active\", 42.5 as \"value\"")) {

            var result = org.finos.legend.engine.execution.BufferedResult.fromResultSet(rs);
            String json = result.toJsonArray();
            System.out.println("JSON: " + json);

            assertEquals("[{\"id\":1,\"name\":\"test\",\"active\":true,\"value\":42.5}]", json);
        }
    }

    @Test
    @DisplayName("BufferedResult.toJsonArray handles null values correctly")
    void testJsonSerializerNulls() throws Exception {
        // Create a query that produces NULL values naturally via LEFT JOIN
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO T_PERSON VALUES (99, 'NoAddress', 'Person', 99)");
        }

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT p.FIRST_NAME as \"firstName\", a.STREET as \"street\" " +
                                "FROM T_PERSON p LEFT OUTER JOIN T_ADDRESS a ON p.ID = a.PERSON_ID " +
                                "WHERE p.ID = 99")) {

            var result = org.finos.legend.engine.execution.BufferedResult.fromResultSet(rs);
            String json = result.toJsonArray();
            System.out.println("JSON with nulls: " + json);

            assertTrue(json.contains("\"firstName\":\"NoAddress\""));
            assertTrue(json.contains("\"street\":null"));
        }
    }

    @Test
    @DisplayName("BufferedResult.toJsonArray escapes special characters in strings")
    void testJsonSerializerEscaping() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO T_PERSON VALUES (98, 'Quote\"Test', 'Tab\tPerson', 20)");
        }

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT FIRST_NAME as \"firstName\", LAST_NAME as \"lastName\" FROM T_PERSON WHERE ID = 98")) {

            var result = org.finos.legend.engine.execution.BufferedResult.fromResultSet(rs);
            String json = result.toJsonArray();
            System.out.println("JSON with escaping: " + json);

            // Should contain escaped quote and tab
            assertTrue(json.contains("\\\"")); // Escaped quote
            assertTrue(json.contains("\\t")); // Escaped tab
        }
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
