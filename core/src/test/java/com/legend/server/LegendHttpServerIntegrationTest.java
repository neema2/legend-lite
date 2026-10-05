package com.legend.server;
import com.legend.json.Json;


import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for LegendHttpServer endpoints.
 * 
 * Tests the HTTP layer with /lsp, legend-engine's pure/v1 execute, and the server's doors: no raw-SQL
 * route, and a page outside the origin allow-list is refused (execution plan W0.1). Tables are seeded
 * in-process through {@link Seed}, on the connection the server's queries then read.
 * Uses file-based DuckDB so data persists across HTTP requests.
 */
class LegendHttpServerIntegrationTest {

    private static LegendHttpServer server;
    private static int port;
    private static HttpClient httpClient;
    private static Path tempDbFile;

    @BeforeAll
    static void setup() throws IOException {
        // Create temp path for DuckDB (file-based for persistence)
        // DuckDB needs to create the file itself — just get a unique path
        tempDbFile = Files.createTempFile("legend-test-", ".duckdb");
        Files.delete(tempDbFile); // DuckDB will create it fresh

        // Start server on random available port
        server = new LegendHttpServer(0);
        server.start();
        port = server.getPort();
        httpClient = HttpClient.newHttpClient();
        System.out.println("Test server started on port " + port);
        System.out.println("Using temp DB file: " + tempDbFile);
    }

    @AfterAll
    static void teardown() {
        if (server != null) {
            server.stop();
        }
        // Clean up temp file. NOT swallowed: this catch used to printStackTrace
        // and pass, which is how a leaked connection hid here for the life of
        // the module — on Windows the delete fails outright ("the process
        // cannot access the file"), and a test that prints and goes green
        // reports nothing. The delete is part of the contract now.
        try {
            Files.deleteIfExists(tempDbFile);
            // DuckDB also creates .wal file
            Files.deleteIfExists(Path.of(tempDbFile.toString() + ".wal"));
        } catch (IOException e) {
            throw new IllegalStateException("the temp database could not be"
                    + " deleted — something still holds it open: " + tempDbFile, e);
        }
    }

    // Build sample model following AbstractDatabaseTest pattern - SIMPLE names in
    // mappings
    private static String buildSampleModel() {
        String dbPath = tempDbFile.toString().replace("\\", "/");
        String template = """
                ###Pure
                import model::*;

                Class model::Person {
                    firstName: String[1];
                    lastName: String[1];
                    age: Integer[1];
                }

                ###Relational
                Database TestDatabase (
                    Table T_PERSON (
                        ID INTEGER PRIMARY KEY,
                        FIRST_NAME VARCHAR(100),
                        LAST_NAME VARCHAR(100),
                        AGE_VAL INTEGER
                    )
                )

                ###Mapping
                import model::*;
                Mapping model::PersonMapping (
                    model::Person: Relational {
                        ~mainTable [TestDatabase] T_PERSON
                        firstName: [TestDatabase] T_PERSON.FIRST_NAME,
                        lastName: [TestDatabase] T_PERSON.LAST_NAME,
                        age: [TestDatabase] T_PERSON.AGE_VAL
                    }
                )

                ###Connection
                import model::*;
                RelationalDatabaseConnection store::TestConnection {
                    type: DuckDB;
                    specification: DuckDB { path: '{{DB_PATH}}'; };
                    auth: Test;
                }

                ###Runtime
                import model::*;
                Runtime test::TestRuntime {
                    mappings:
                    [
                        model::PersonMapping
                    ];
                    connections:
                    [
                        TestDatabase:
                        [
                            environment: store::TestConnection
                        ]
                    ];
                }
                """;
        return template.replace("{{DB_PATH}}", dbPath);
    }

    // Build JSON request body properly
    /**
     * legend-engine's own flow, as a client makes it: the query's lambda JSON
     * ({@code grammarToJson/lambda}), then {@code execution/execute} with the model as text
     * and the runtime as a pointer.
     */
    private HttpResponse<String> executeUpstream(String model, String query, String runtime)
            throws Exception {
        HttpResponse<String> lambda = httpClient.send(HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/pure/v1/grammar/grammarToJson/lambda"))
                .POST(HttpRequest.BodyPublishers.ofString(query)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, lambda.statusCode(), lambda.body());
        String input = "{\"clientVersion\":\"vX_X_X\",\"function\":" + lambda.body()
                + ",\"model\":{\"_type\":\"text\",\"code\":\"" + Json.escape(model) + "\"}"
                + ",\"runtime\":{\"_type\":\"runtimePointer\",\"runtime\":\"" + runtime + "\"}"
                + ",\"context\":{\"_type\":\"BaseExecutionContext\"}}";
        return httpClient.send(HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/pure/v1/execution/execute"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(input)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static String buildJsonRequest(String code, String sql, String runtime) {
        return "{" +
                "\"code\":\"" + Json.escape(code) + "\"," +
                "\"sql\":\"" + Json.escape(sql) + "\"," +
                "\"runtime\":\"" + Json.escape(runtime) + "\"" +
                "}";
    }

    @Test
    @DisplayName("GET /health returns status ok")
    void testHealthEndpoint() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/health"))
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"status\":\"ok\""));
    }

    @Test
    @DisplayName("POST /lsp initialize returns capabilities")
    void testLspInitialize() throws Exception {
        String body = """
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {}
                }
                """;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/lsp"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"capabilities\""));
        assertTrue(response.body().contains("legend-lite-lsp"));
    }

    /** The model's DuckDB file gets T_PERSON with John Smith: each test that reads it seeds it, so any one method
     *  runs alone (Bazel workplan P3-03; it was an @Order(3) test the @Order(7) query relied on). Idempotent. */
    private static void seedPersonTable() throws Exception {
        Seed.sql(buildSampleModel(), "DROP TABLE IF EXISTS T_PERSON", "test::TestRuntime");
        Seed.sql(buildSampleModel(), """
                CREATE TABLE T_PERSON (
                    ID INTEGER PRIMARY KEY,
                    FIRST_NAME VARCHAR(100),
                    LAST_NAME VARCHAR(100),
                    AGE_VAL INTEGER
                )
                """, "test::TestRuntime");
        Seed.sql(buildSampleModel(), "INSERT INTO T_PERSON VALUES (1, 'John', 'Smith', 30)",
                "test::TestRuntime");
    }

    @Test
    @DisplayName("POST /engine/sql is gone: raw SQL is not a product surface")
    void rawSqlRouteIsGone() throws Exception {
        HttpResponse<String> response = httpClient.send(HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/engine/sql"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(
                        buildJsonRequest(buildSampleModel(), "SELECT 1", "test::TestRuntime")))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(404, response.statusCode(), response.body());
    }

    @Test
    @DisplayName("a page outside the allow-list is refused, even with a preflight-free text/plain POST")
    void foreignOriginIsRefused() throws Exception {
        HttpResponse<String> response = httpClient.send(HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/pure/v1/grammar/grammarToJson/lambda"))
                .header("Content-Type", "text/plain")
                .header("Origin", "https://attacker.example")
                .POST(HttpRequest.BodyPublishers.ofString("|1")).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(403, response.statusCode(), response.body());
        assertTrue(response.headers().firstValue("Access-Control-Allow-Origin").isEmpty(),
                "a refused origin gets no CORS grant");
    }

    @Test
    @DisplayName("a loopback page is served, and the CORS answer names it rather than *")
    void loopbackOriginIsServedAndEchoed() throws Exception {
        HttpResponse<String> response = httpClient.send(HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/pure/v1/grammar/grammarToJson/lambda"))
                .header("Content-Type", "text/plain")
                .header("Origin", "http://localhost:5173")
                .POST(HttpRequest.BodyPublishers.ofString("|1")).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), response.body());
        assertEquals("http://localhost:5173",
                response.headers().firstValue("Access-Control-Allow-Origin").orElse(null));
    }

    @Test
    @DisplayName("the allow-list: loopback hosts on any port, listed origins, nothing that merely starts like them")
    void theAllowList() {
        LegendHttpServer.Origins loopback = LegendHttpServer.Origins.LOOPBACK;
        assertTrue(loopback.allows("http://localhost:3000"));
        assertTrue(loopback.allows("http://127.0.0.1:8080"));
        assertTrue(loopback.allows("http://[::1]:9000"));
        assertTrue(loopback.allows("https://localhost"));
        assertFalse(loopback.allows("http://localhost.attacker.example"));
        assertFalse(loopback.allows("http://attacker.example/?http://localhost"));
        assertFalse(loopback.allows("null"));
        LegendHttpServer.Origins listed = LegendHttpServer.Origins.fromEnv(" https://studio.example ,");
        assertTrue(listed.allows("https://studio.example"));
        assertTrue(listed.allows("http://localhost:3000"));
        assertFalse(listed.allows("https://studio.example.attacker.example"));
    }

    @Test
    @DisplayName("POST /lsp didOpen validates Pure model")
    void testLspDidOpenValidation() throws Exception {
        String validModel = """
                Class model::ValidPerson {
                    name: String[1];
                }
                """;

        String body = """
                {
                    "jsonrpc": "2.0",
                    "method": "textDocument/didOpen",
                    "params": {
                        "textDocument": {
                            "uri": "file:///test.pure",
                            "languageId": "pure",
                            "version": 1,
                            "text": "%s"
                        }
                    }
                }
                """.formatted(Json.escape(validModel));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/lsp"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println("didOpen response: " + response.body());

        assertEquals(200, response.statusCode());
        // Valid model should return empty diagnostics or no diagnostics array
        assertTrue(response.body().contains("\"diagnostics\":[]") ||
                !response.body().contains("\"severity\":1"),
                "Expected no errors for valid model: " + response.body());
    }

    @Test
    @DisplayName("pure/v1 execute runs a Pure query, in the engine's TDS result shape")
    void testEngineExecutePureQuery() throws Exception {
        seedPersonTable();
        HttpResponse<String> response = executeUpstream(buildSampleModel(),
                "|model::Person.all()->project(~[firstName:p|$p.firstName, lastName:p|$p.lastName])",
                "test::TestRuntime");
        System.out.println("Execute Pure response: " + response.body());

        assertEquals(200, response.statusCode(), response.body());
        Json.Obj result = Json.parseObject(response.body());
        assertEquals("tdsBuilder", result.getObj("builder").getString("_type"));
        assertEquals(List.of("firstName", "lastName"),
                result.getObj("result").getStringArray("columns"));
        // seeded above: John Smith
        assertTrue(response.body().contains("{\"values\": [\"John\",\"Smith\"]}"),
                "Expected query results: " + response.body());
    }

    @Test
    @DisplayName("E2E: Full workflow - Validate Model → Seed → Pure Query")
    void testFullE2EWorkflow() throws Exception {
        // Use InMemory DuckDB (no file) to test connection caching
        String pureModel = """
                ###Pure
                import model::*;

                Class model::Employee {
                    name: String[1];
                    department: String[1];
                    salary: Integer[1];
                }

                ###Relational
                Database EmployeeDB (
                    Table T_EMPLOYEE (
                        ID INTEGER PRIMARY KEY,
                        NAME VARCHAR(100),
                        DEPARTMENT VARCHAR(100),
                        SALARY INTEGER
                    )
                )

                ###Mapping
                import model::*;
                Mapping model::EmployeeMapping (
                    model::Employee: Relational {
                        ~mainTable [EmployeeDB] T_EMPLOYEE
                        name: [EmployeeDB] T_EMPLOYEE.NAME,
                        department: [EmployeeDB] T_EMPLOYEE.DEPARTMENT,
                        salary: [EmployeeDB] T_EMPLOYEE.SALARY
                    }
                )

                ###Connection
                import model::*;
                RelationalDatabaseConnection store::EmpConnection {
                    type: DuckDB;
                    specification: DuckDB { };
                    auth: Test;
                }

                ###Runtime
                import model::*;
                Runtime test::EmpRuntime {
                    mappings:
                    [
                        model::EmployeeMapping
                    ];
                    connections:
                    [
                        EmployeeDB:
                        [
                            environment: store::EmpConnection
                        ]
                    ];
                }
                """;

        // STEP 1: Validate Pure model via LSP
        System.out.println("\n=== E2E STEP 1: Validate Pure Model ===");
        String didOpenBody = """
                {
                    "jsonrpc": "2.0",
                    "method": "textDocument/didOpen",
                    "params": {
                        "textDocument": {
                            "uri": "file:///e2e-test.pure",
                            "languageId": "pure",
                            "version": 1,
                            "text": "%s"
                        }
                    }
                }
                """.formatted(Json.escape(pureModel));

        HttpRequest validateRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/lsp"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(didOpenBody))
                .build();

        HttpResponse<String> validateResponse = httpClient.send(validateRequest, HttpResponse.BodyHandlers.ofString());
        System.out.println("Validation response: " + validateResponse.body());
        assertEquals(200, validateResponse.statusCode());
        assertTrue(validateResponse.body().contains("\"diagnostics\":[]"),
                "Model validation should have no errors: " + validateResponse.body());

        // STEP 2 and 3: create and fill the table (seeded in-process; raw SQL has no route)
        Seed.sql(pureModel, """
                CREATE TABLE T_EMPLOYEE (
                    ID INTEGER PRIMARY KEY,
                    NAME VARCHAR(100),
                    DEPARTMENT VARCHAR(100),
                    SALARY INTEGER
                )
                """, "test::EmpRuntime");
        Seed.sql(pureModel, """
                INSERT INTO T_EMPLOYEE VALUES (1, 'Alice', 'Engineering', 120000);
                INSERT INTO T_EMPLOYEE VALUES (2, 'Bob', 'Engineering', 95000);
                INSERT INTO T_EMPLOYEE VALUES (3, 'Carol', 'Marketing', 85000);
                """, "test::EmpRuntime");

        // STEP 4: Run Pure Query
        System.out.println("\n=== E2E STEP 4: Execute Pure Query ===");
        HttpResponse<String> queryResponse = executeUpstream(pureModel, """
                |model::Employee.all()
                    ->filter(e | $e.department == 'Engineering')
                    ->project(~[name:e|$e.name, salary:e|$e.salary])
                """, "test::EmpRuntime");
        System.out.println("Pure Query response: " + queryResponse.body());

        assertEquals(200, queryResponse.statusCode(), queryResponse.body());
        // Should return Alice and Bob (Engineering), not Carol (Marketing)
        assertTrue(queryResponse.body().contains("Alice") && queryResponse.body().contains("Bob"),
                "Expected Alice and Bob in results: " + queryResponse.body());
        assertFalse(queryResponse.body().contains("Carol"),
                "Carol (Marketing) should be filtered out: " + queryResponse.body());

        System.out.println("\n=== E2E TEST PASSED ===\n");
    }
}