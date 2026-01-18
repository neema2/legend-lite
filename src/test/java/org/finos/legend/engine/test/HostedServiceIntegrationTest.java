package org.finos.legend.engine.test;

import org.finos.legend.engine.server.*;
import org.finos.legend.pure.dsl.definition.*;
import org.junit.jupiter.api.*;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the hosted service infrastructure.
 * 
 * Tests:
 * - Service definition parsing
 * - HTTP server with service routing
 * - ResultSet JSON serialization
 * - Path parameter extraction
 */
@DisplayName("Hosted Service Integration Tests")
class HostedServiceIntegrationTest {

    private Connection connection;
    private ServiceServer server;
    private int serverPort;

    // ==================== Pure Source Definitions ====================

    private static final String PERSON_SERVICE = """
            Service model::PersonByLastName
            {
                pattern: '/api/person/{lastName}';
                function: |Person.all()->filter({p | $p.lastName == $lastName});
                documentation: 'Returns people by last name';
            }
            """;

    private static final String ALL_PERSONS_SERVICE = """
            Service model::AllPersons
            {
                pattern: '/api/persons';
                function: |Person.all();
                documentation: 'Returns all people';
            }
            """;

    @BeforeEach
    void setUp() throws Exception {
        // Create in-memory DuckDB
        connection = DriverManager.getConnection("jdbc:duckdb:");

        // Create test table
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                    CREATE TABLE T_PERSON (
                        ID INTEGER PRIMARY KEY,
                        FIRST_NAME VARCHAR(100),
                        LAST_NAME VARCHAR(100),
                        AGE INTEGER
                    )
                    """);

            stmt.execute("INSERT INTO T_PERSON VALUES (1, 'John', 'Smith', 30)");
            stmt.execute("INSERT INTO T_PERSON VALUES (2, 'Jane', 'Smith', 28)");
            stmt.execute("INSERT INTO T_PERSON VALUES (3, 'Bob', 'Jones', 45)");
        }

        // Create service registry with mock executors
        ServiceRegistry registry = new ServiceRegistry();

        // Register a simple service that returns all persons
        ServiceDefinition allPersonsDef = PureDefinitionParser.parseServiceDefinition(ALL_PERSONS_SERVICE);
        registry.register(allPersonsDef, (pathParams, queryParams, conn) -> {
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT FIRST_NAME as \"firstName\", LAST_NAME as \"lastName\", AGE as \"age\" FROM T_PERSON")) {
                return ResultSetJsonSerializer.serializeAsArray(rs);
            }
        });

        // Register service with path parameter
        ServiceDefinition personByNameDef = PureDefinitionParser.parseServiceDefinition(PERSON_SERVICE);
        registry.register(personByNameDef, (pathParams, queryParams, conn) -> {
            String lastName = pathParams.get("lastName");
            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT FIRST_NAME as \"firstName\", LAST_NAME as \"lastName\", AGE as \"age\" " +
                            "FROM T_PERSON WHERE LAST_NAME = ?")) {
                stmt.setString(1, lastName);
                try (ResultSet rs = stmt.executeQuery()) {
                    return ResultSetJsonSerializer.serializeAsArray(rs);
                }
            }
        });

        // Start server on random available port
        server = new ServiceServer(0, registry, () -> connection);
        server.start();
        serverPort = server.getPort();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.stop(0);
        }
        if (connection != null) {
            connection.close();
        }
    }

    // ==================== Service Definition Parsing Tests ====================

    @Test
    @DisplayName("Parse service definition with path parameter")
    void testParseServiceDefinition() {
        ServiceDefinition def = PureDefinitionParser.parseServiceDefinition(PERSON_SERVICE);

        assertEquals("model::PersonByLastName", def.qualifiedName());
        assertEquals("PersonByLastName", def.simpleName());
        assertEquals("/api/person/{lastName}", def.pattern());
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
        ServiceDefinition def = PureDefinitionParser.parseServiceDefinition(PERSON_SERVICE);
        var regex = def.toRegexPattern();

        assertTrue(regex.matcher("/api/person/Smith").matches());
        assertTrue(regex.matcher("/api/person/Jones").matches());
        assertFalse(regex.matcher("/api/person").matches());
        assertFalse(regex.matcher("/api/person/Smith/extra").matches());
    }

    // ==================== HTTP Server Tests ====================

    @Test
    @DisplayName("GET /api/persons returns all people as JSON")
    void testGetAllPersons() throws Exception {
        String response = httpGet("/api/persons");

        System.out.println("Response: " + response);

        // Verify it's valid JSON array
        assertTrue(response.startsWith("["));
        assertTrue(response.endsWith("]"));

        // Verify contains expected data
        assertTrue(response.contains("\"firstName\":\"John\""));
        assertTrue(response.contains("\"firstName\":\"Jane\""));
        assertTrue(response.contains("\"firstName\":\"Bob\""));
    }

    @Test
    @DisplayName("GET /api/person/{lastName} returns filtered results")
    void testGetPersonByLastName() throws Exception {
        String response = httpGet("/api/person/Smith");

        System.out.println("Response: " + response);

        // Should contain John and Jane Smith
        assertTrue(response.contains("\"firstName\":\"John\""));
        assertTrue(response.contains("\"firstName\":\"Jane\""));

        // Should NOT contain Bob Jones
        assertFalse(response.contains("\"firstName\":\"Bob\""));
    }

    @Test
    @DisplayName("GET /api/person/{lastName} with no matches returns empty array")
    void testGetPersonNoMatches() throws Exception {
        String response = httpGet("/api/person/Unknown");

        System.out.println("Response: " + response);

        // Should be empty array
        assertEquals("[]", response);
    }

    @Test
    @DisplayName("GET unknown path returns 404")
    void testNotFound() throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL("http://localhost:" + serverPort + "/api/unknown")
                .openConnection();
        conn.setRequestMethod("GET");

        assertEquals(404, conn.getResponseCode());
    }

    // ==================== JSON Serialization Tests ====================

    @Test
    @DisplayName("ResultSetJsonSerializer produces valid JSON")
    void testJsonSerializer() throws Exception {
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT 1 as \"id\", 'test' as \"name\", true as \"active\", 42.5 as \"value\"")) {

            String json = ResultSetJsonSerializer.serializeAsArray(rs);
            System.out.println("JSON: " + json);

            assertEquals("[{\"id\":1,\"name\":\"test\",\"active\":true,\"value\":42.5}]", json);
        }
    }

    @Test
    @DisplayName("ResultSetJsonSerializer handles null values")
    void testJsonSerializerNulls() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO T_PERSON VALUES (99, NULL, 'NullFirst', NULL)");
        }

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT FIRST_NAME as \"firstName\", LAST_NAME as \"lastName\", AGE as \"age\" " +
                                "FROM T_PERSON WHERE ID = 99")) {

            String json = ResultSetJsonSerializer.serializeAsArray(rs);
            System.out.println("JSON with nulls: " + json);

            assertTrue(json.contains("null"));
            assertTrue(json.contains("\"lastName\":\"NullFirst\""));
        }
    }

    @Test
    @DisplayName("ResultSetJsonSerializer escapes special characters")
    void testJsonSerializerEscaping() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO T_PERSON VALUES (98, 'Quote\"Test', 'New\nLine', 20)");
        }

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT FIRST_NAME as \"firstName\", LAST_NAME as \"lastName\" " +
                                "FROM T_PERSON WHERE ID = 98")) {

            String json = ResultSetJsonSerializer.serializeAsArray(rs);
            System.out.println("JSON with escaping: " + json);

            // Should contain escaped quote and newline
            assertTrue(json.contains("\\\""));
            assertTrue(json.contains("\\n"));
        }
    }

    // ==================== Helper Methods ====================

    private String httpGet(String path) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL("http://localhost:" + serverPort + path).openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);

        assertEquals(200, conn.getResponseCode(), "Expected 200 OK");

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
