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

    // ==================== Association Navigation Tests ====================

    /**
     * Test class that sets up Person + Address with association for testing.
     */
    @Nested
    @DisplayName("Association Navigation via HTTP")
    class AssociationNavigationTests {

        private ServiceServer assocServer;
        private int assocServerPort;
        private Connection assocConnection;

        @BeforeEach
        void setUpAssociations() throws Exception {
            // Create separate in-memory DuckDB for association tests
            assocConnection = DriverManager.getConnection("jdbc:duckdb:");

            try (Statement stmt = assocConnection.createStatement()) {
                // Create Person table
                stmt.execute("""
                        CREATE TABLE T_PERSON (
                            ID INTEGER PRIMARY KEY,
                            FIRST_NAME VARCHAR(100),
                            LAST_NAME VARCHAR(100)
                        )
                        """);

                // Create Address table with FK to Person
                stmt.execute("""
                        CREATE TABLE T_ADDRESS (
                            ID INTEGER PRIMARY KEY,
                            PERSON_ID INTEGER,
                            STREET VARCHAR(200),
                            CITY VARCHAR(100)
                        )
                        """);

                // Insert Person data
                stmt.execute("INSERT INTO T_PERSON VALUES (1, 'John', 'Smith')");
                stmt.execute("INSERT INTO T_PERSON VALUES (2, 'Jane', 'Doe')");

                // Insert Address data (John has 2 addresses, Jane has 1)
                stmt.execute("INSERT INTO T_ADDRESS VALUES (1, 1, '123 Main St', 'New York')");
                stmt.execute("INSERT INTO T_ADDRESS VALUES (2, 1, '456 Oak Ave', 'Boston')");
                stmt.execute("INSERT INTO T_ADDRESS VALUES (3, 2, '789 Pine Rd', 'Chicago')");
            }

            // Create registry with association-aware services
            ServiceRegistry registry = new ServiceRegistry();

            // Service: Get persons with their addresses (LEFT OUTER JOIN)
            ServiceDefinition personWithAddressesDef = ServiceDefinition.of(
                    "model::PersonsWithAddresses",
                    "/api/persons-with-addresses",
                    "Person.all()->project({p | $p.firstName}, {p | $p.addresses.street}, {p | $p.addresses.city})",
                    "Returns all persons with their addresses");
            registry.register(personWithAddressesDef, (pathParams, queryParams, conn) -> {
                // Simulate the SQL that association navigation generates
                String sql = """
                        SELECT "t0"."FIRST_NAME" AS "firstName",
                               "j1"."STREET" AS "street",
                               "j1"."CITY" AS "city"
                        FROM "T_PERSON" AS "t0"
                        LEFT OUTER JOIN "T_ADDRESS" AS "j1" ON "t0"."ID" = "j1"."PERSON_ID"
                        """;
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery(sql)) {
                    return ResultSetJsonSerializer.serializeAsArray(rs);
                }
            });

            // Service: Get persons filtered by address city (EXISTS subquery)
            ServiceDefinition personByCityDef = ServiceDefinition.of(
                    "model::PersonsByCity",
                    "/api/persons-by-city/{city}",
                    "Person.all()->filter({p | $p.addresses.city == $city})->project({p | $p.firstName})",
                    "Returns persons who have an address in the specified city");
            registry.register(personByCityDef, (pathParams, queryParams, conn) -> {
                String city = pathParams.get("city");
                // Simulate the SQL that association filter generates (EXISTS)
                String sql = """
                        SELECT "t0"."FIRST_NAME" AS "firstName"
                        FROM "T_PERSON" AS "t0"
                        WHERE EXISTS (
                            SELECT 1 FROM "T_ADDRESS" AS "sub1"
                            WHERE ("sub1"."PERSON_ID" = "t0"."ID" AND "sub1"."CITY" = ?)
                        )
                        """;
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setString(1, city);
                    try (ResultSet rs = stmt.executeQuery()) {
                        return ResultSetJsonSerializer.serializeAsArray(rs);
                    }
                }
            });

            // Service: Get addresses for a specific person (path param + JOIN)
            ServiceDefinition addressesByPersonDef = ServiceDefinition.of(
                    "model::AddressesByPerson",
                    "/api/addresses/{personId}",
                    "Address.all()->filter({a | $a.person.id == $personId})",
                    "Returns addresses for a specific person");
            registry.register(addressesByPersonDef, (pathParams, queryParams, conn) -> {
                int personId = Integer.parseInt(pathParams.get("personId"));
                String sql = """
                        SELECT "t0"."STREET" AS "street", "t0"."CITY" AS "city"
                        FROM "T_ADDRESS" AS "t0"
                        WHERE "t0"."PERSON_ID" = ?
                        """;
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setInt(1, personId);
                    try (ResultSet rs = stmt.executeQuery()) {
                        return ResultSetJsonSerializer.serializeAsArray(rs);
                    }
                }
            });

            // Start server
            assocServer = new ServiceServer(0, registry, () -> assocConnection);
            assocServer.start();
            assocServerPort = assocServer.getPort();
        }

        @AfterEach
        void tearDownAssociations() throws Exception {
            if (assocServer != null) {
                assocServer.stop(0);
            }
            if (assocConnection != null) {
                assocConnection.close();
            }
        }

        @Test
        @DisplayName("GET /api/persons-with-addresses returns person data with JOINed addresses")
        void testGetPersonsWithAddresses() throws Exception {
            String response = httpGetAssoc("/api/persons-with-addresses");

            System.out.println("Persons with addresses: " + response);

            // John has 2 addresses, Jane has 1 = 3 rows total
            // Verify John appears with both addresses
            assertTrue(response.contains("\"firstName\":\"John\""));
            assertTrue(response.contains("\"street\":\"123 Main St\""));
            assertTrue(response.contains("\"street\":\"456 Oak Ave\""));

            // Verify Jane appears with her address
            assertTrue(response.contains("\"firstName\":\"Jane\""));
            assertTrue(response.contains("\"street\":\"789 Pine Rd\""));
        }

        @Test
        @DisplayName("GET /api/persons-by-city/{city} uses EXISTS for to-many filter")
        void testGetPersonsByCity() throws Exception {
            // John lives in Boston
            String bostonResponse = httpGetAssoc("/api/persons-by-city/Boston");
            System.out.println("Persons in Boston: " + bostonResponse);
            assertTrue(bostonResponse.contains("\"firstName\":\"John\""));
            assertFalse(bostonResponse.contains("\"firstName\":\"Jane\""));

            // Jane lives in Chicago
            String chicagoResponse = httpGetAssoc("/api/persons-by-city/Chicago");
            System.out.println("Persons in Chicago: " + chicagoResponse);
            assertTrue(chicagoResponse.contains("\"firstName\":\"Jane\""));
            assertFalse(chicagoResponse.contains("\"firstName\":\"John\""));

            // No one in Miami
            String miamiResponse = httpGetAssoc("/api/persons-by-city/Miami");
            System.out.println("Persons in Miami: " + miamiResponse);
            assertEquals("[]", miamiResponse);
        }

        @Test
        @DisplayName("GET /api/addresses/{personId} returns addresses for specific person")
        void testGetAddressesByPerson() throws Exception {
            // John (ID 1) has 2 addresses
            String johnAddresses = httpGetAssoc("/api/addresses/1");
            System.out.println("John's addresses: " + johnAddresses);
            assertTrue(johnAddresses.contains("\"street\":\"123 Main St\""));
            assertTrue(johnAddresses.contains("\"street\":\"456 Oak Ave\""));

            // Jane (ID 2) has 1 address
            String janeAddresses = httpGetAssoc("/api/addresses/2");
            System.out.println("Jane's addresses: " + janeAddresses);
            assertTrue(janeAddresses.contains("\"street\":\"789 Pine Rd\""));
            assertFalse(janeAddresses.contains("Main St"));
        }

        private String httpGetAssoc(String path) throws Exception {
            HttpURLConnection conn = (HttpURLConnection) new URL("http://localhost:" + assocServerPort + path)
                    .openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);

            assertEquals(200, conn.getResponseCode(), "Expected 200 OK for " + path);

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
}
