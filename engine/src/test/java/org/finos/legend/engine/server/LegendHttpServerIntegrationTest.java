package org.finos.legend.engine.server;

import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for LegendHttpServer endpoints.
 * 
 * Tests the HTTP layer with /lsp, /engine/execute, and /engine/sql.
 * Uses file-based DuckDB so data persists across HTTP requests.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LegendHttpServerIntegrationTest {

    private static LegendHttpServer server;
    private static int port;
    private static HttpClient httpClient;
    private static Path tempDbFile;

    @BeforeAll
    static void setup() throws IOException {
        // Create temp file for DuckDB (file-based for persistence)
        tempDbFile = Files.createTempFile("legend-test-", ".duckdb");

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
        // Clean up temp file
        try {
            Files.deleteIfExists(tempDbFile);
            // DuckDB also creates .wal file
            Files.deleteIfExists(Path.of(tempDbFile.toString() + ".wal"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Build sample model following AbstractDatabaseTest pattern - SIMPLE names in
    // mappings
    private static String buildSampleModel() {
        String dbPath = tempDbFile.toString().replace("\\", "/");
        String template = """
                Class model::Person {
                    firstName: String[1];
                    lastName: String[1];
                    age: Integer[1];
                }

                Database TestDatabase (
                    Table T_PERSON (
                        ID INTEGER PRIMARY KEY,
                        FIRST_NAME VARCHAR(100),
                        LAST_NAME VARCHAR(100),
                        AGE_VAL INTEGER
                    )
                )

                Mapping model::PersonMapping (
                    Person: Relational {
                        ~mainTable [TestDatabase] T_PERSON
                        firstName: [TestDatabase] T_PERSON.FIRST_NAME,
                        lastName: [TestDatabase] T_PERSON.LAST_NAME,
                        age: [TestDatabase] T_PERSON.AGE_VAL
                    }
                )

                RelationalDatabaseConnection store::TestConnection {
                    type: DuckDB;
                    specification: LocalFile { path: "{{DB_PATH}}"; };
                }

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
    private static String buildJsonRequest(String code, String sql, String runtime) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"code\":\"").append(escapeJson(code)).append("\",");
        sb.append("\"sql\":\"").append(escapeJson(sql)).append("\",");
        sb.append("\"runtime\":\"").append(escapeJson(runtime)).append("\"");
        sb.append("}");
        return sb.toString();
    }

    private static String escapeJson(String s) {
        if (s == null)
            return "";
        StringBuilder sb = new StringBuilder();
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    @Test
    @Order(1)
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
    @Order(2)
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

    @Test
    @Order(3)
    @DisplayName("POST /engine/sql CREATE TABLE succeeds")
    void testExecuteSqlCreateTable() throws Exception {
        // First drop if exists for idempotency
        String dropSql = "DROP TABLE IF EXISTS T_PERSON";
        String dropBody = buildJsonRequest(buildSampleModel(), dropSql, "test::TestRuntime");
        HttpRequest dropRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/engine/sql"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(dropBody))
                .build();
        httpClient.send(dropRequest, HttpResponse.BodyHandlers.ofString());

        String createTableSql = """
                CREATE TABLE T_PERSON (
                    ID INTEGER PRIMARY KEY,
                    FIRST_NAME VARCHAR(100),
                    LAST_NAME VARCHAR(100),
                    AGE_VAL INTEGER
                )
                """;

        String body = buildJsonRequest(buildSampleModel(), createTableSql, "test::TestRuntime");

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/engine/sql"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println("CREATE TABLE response: " + response.body());

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"success\":true"),
                "Expected success:true but got: " + response.body());
    }

    @Test
    @Order(4)
    @DisplayName("POST /engine/sql INSERT data")
    void testExecuteSqlInsert() throws Exception {
        String insertSql = "INSERT INTO T_PERSON VALUES (1, 'John', 'Smith', 30)";

        String body = buildJsonRequest(buildSampleModel(), insertSql, "test::TestRuntime");

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/engine/sql"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println("INSERT response: " + response.body());

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"success\":true"),
                "INSERT failed: " + response.body());
    }

    @Test
    @Order(5)
    @DisplayName("POST /engine/sql SELECT returns data")
    void testExecuteSqlSelect() throws Exception {
        String selectSql = "SELECT FIRST_NAME, LAST_NAME, AGE_VAL FROM T_PERSON WHERE ID = 1";

        String body = buildJsonRequest(buildSampleModel(), selectSql, "test::TestRuntime");

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/engine/sql"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println("SELECT response: " + response.body());

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"success\":true"),
                "SELECT failed: " + response.body());
        assertTrue(response.body().contains("John"), "Should contain John");
        assertTrue(response.body().contains("Smith"), "Should contain Smith");
    }

    @Test
    @Order(6)
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
                """.formatted(escapeJson(validModel));

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
    @Order(7)
    @DisplayName("POST /engine/execute runs Pure query")
    void testEngineExecutePureQuery() throws Exception {
        // Use the sample model with query appended
        String pureCodeWithQuery = buildSampleModel() + """

                Person.all()
                    ->project({p | $p.firstName}, {p | $p.lastName})
                """;

        String body = String.format("{\"code\":\"%s\"}", escapeJson(pureCodeWithQuery));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/engine/execute"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println("Execute Pure response: " + response.body());

        assertEquals(200, response.statusCode());
        // Should have success=true and return data from T_PERSON table
        assertTrue(response.body().contains("\"success\":true"),
                "Pure query failed: " + response.body());
        // Data was inserted in Order 4, so we should see John/Smith
        assertTrue(response.body().contains("John") || response.body().contains("firstName"),
                "Expected query results: " + response.body());
    }

    @Test
    @Order(8)
    @DisplayName("E2E: Full workflow - Validate Model → Create Table → Insert → Pure Query")
    void testFullE2EWorkflow() throws Exception {
        // Use InMemory DuckDB (no file) to test connection caching
        String pureModel = """
                Class model::Employee {
                    name: String[1];
                    department: String[1];
                    salary: Integer[1];
                }

                Database EmployeeDB (
                    Table T_EMPLOYEE (
                        ID INTEGER PRIMARY KEY,
                        NAME VARCHAR(100),
                        DEPARTMENT VARCHAR(100),
                        SALARY INTEGER
                    )
                )

                Mapping model::EmployeeMapping (
                    Employee: Relational {
                        ~mainTable [EmployeeDB] T_EMPLOYEE
                        name: [EmployeeDB] T_EMPLOYEE.NAME,
                        department: [EmployeeDB] T_EMPLOYEE.DEPARTMENT,
                        salary: [EmployeeDB] T_EMPLOYEE.SALARY
                    }
                )

                RelationalDatabaseConnection store::EmpConnection {
                    type: DuckDB;
                    specification: InMemory { };
                }

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
                """.formatted(escapeJson(pureModel));

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

        // STEP 2: Create Table via SQL
        System.out.println("\n=== E2E STEP 2: Create Table ===");
        String createSql = """
                CREATE TABLE T_EMPLOYEE (
                    ID INTEGER PRIMARY KEY,
                    NAME VARCHAR(100),
                    DEPARTMENT VARCHAR(100),
                    SALARY INTEGER
                )
                """;
        String createBody = buildJsonRequest(pureModel, createSql, "test::EmpRuntime");

        HttpRequest createRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/engine/sql"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(createBody))
                .build();

        HttpResponse<String> createResponse = httpClient.send(createRequest, HttpResponse.BodyHandlers.ofString());
        System.out.println("Create response: " + createResponse.body());
        assertEquals(200, createResponse.statusCode());
        assertTrue(createResponse.body().contains("\"success\":true"),
                "CREATE TABLE failed: " + createResponse.body());

        // STEP 3: Insert Data via SQL
        System.out.println("\n=== E2E STEP 3: Insert Data ===");
        String insertSql = """
                INSERT INTO T_EMPLOYEE VALUES (1, 'Alice', 'Engineering', 120000);
                INSERT INTO T_EMPLOYEE VALUES (2, 'Bob', 'Engineering', 95000);
                INSERT INTO T_EMPLOYEE VALUES (3, 'Carol', 'Marketing', 85000);
                """;
        String insertBody = buildJsonRequest(pureModel, insertSql, "test::EmpRuntime");

        HttpRequest insertRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/engine/sql"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(insertBody))
                .build();

        HttpResponse<String> insertResponse = httpClient.send(insertRequest, HttpResponse.BodyHandlers.ofString());
        System.out.println("Insert response: " + insertResponse.body());
        assertEquals(200, insertResponse.statusCode());
        assertTrue(insertResponse.body().contains("\"success\":true"),
                "INSERT failed: " + insertResponse.body());

        // STEP 4: Run Pure Query
        System.out.println("\n=== E2E STEP 4: Execute Pure Query ===");
        String pureQuery = pureModel + """

                Employee.all()
                    ->filter(e | $e.department == 'Engineering')
                    ->project({e | $e.name}, {e | $e.salary})
                """;

        String queryBody = String.format("{\"code\":\"%s\"}", escapeJson(pureQuery));

        HttpRequest queryRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/engine/execute"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(queryBody))
                .build();

        HttpResponse<String> queryResponse = httpClient.send(queryRequest, HttpResponse.BodyHandlers.ofString());
        System.out.println("Pure Query response: " + queryResponse.body());

        assertEquals(200, queryResponse.statusCode());
        assertTrue(queryResponse.body().contains("\"success\":true"),
                "Pure query failed: " + queryResponse.body());
        // Should return Alice and Bob (Engineering), not Carol (Marketing)
        assertTrue(queryResponse.body().contains("Alice") && queryResponse.body().contains("Bob"),
                "Expected Alice and Bob in results: " + queryResponse.body());
        assertFalse(queryResponse.body().contains("Carol"),
                "Carol (Marketing) should be filtered out: " + queryResponse.body());

        System.out.println("\n=== E2E TEST PASSED ===\n");
    }
}