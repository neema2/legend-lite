package com.gs.legend.test;

import com.gs.legend.serial.SerializerRegistry;
import com.gs.legend.server.QueryService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link QueryService#stream}.
 * Every test calls stream() and validates the OutputStream content.
 */
class StreamingIntegrationTest {

    private Connection connection;
    private QueryService queryService;

    private static final String PURE_MODEL = """
            import model::*;
            import store::*;
            import test::*;

            Class model::Employee
            {
                name: String[1];
                department: String[1];
                salary: Integer[1];
            }

            Database store::EmployeeDb
            (
                Table T_EMPLOYEE
                (
                    ID INTEGER PRIMARY KEY,
                    NAME VARCHAR(100) NOT NULL,
                    DEPARTMENT VARCHAR(100) NOT NULL,
                    SALARY INTEGER NOT NULL
                )
            )

            Mapping model::EmployeeMapping
            (
                Employee: Relational
                {
                    ~mainTable [EmployeeDb] T_EMPLOYEE
                    name: [EmployeeDb] T_EMPLOYEE.NAME,
                    department: [EmployeeDb] T_EMPLOYEE.DEPARTMENT,
                    salary: [EmployeeDb] T_EMPLOYEE.SALARY
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
                    store::EmployeeDb:
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
                        NAME VARCHAR(100) NOT NULL,
                        DEPARTMENT VARCHAR(100) NOT NULL,
                        SALARY INTEGER NOT NULL
                    )
                    """);

            for (int i = 1; i <= 100; i++) {
                String dept = switch (i % 3) {
                    case 0 -> "Engineering";
                    case 1 -> "Sales";
                    default -> "Marketing";
                };
                stmt.execute("INSERT INTO T_EMPLOYEE VALUES (%d, 'Employee_%d', '%s', %d)"
                        .formatted(i, i, dept, 50000 + (i * 1000)));
            }
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    // ==================== JSON Streaming ====================

    @Test
    @DisplayName("stream JSON: full result set")
    void testStreamJsonFull() throws Exception {
        String query = "Employee.all()->project(~[name:e|$e.name, salary:e|$e.salary])";

        var out = new ByteArrayOutputStream();
        queryService.stream(PURE_MODEL, query, "test::TestRuntime", connection, out, "json");

        String json = out.toString(StandardCharsets.UTF_8);
        assertTrue(json.startsWith("[{"), "Should be JSON array of objects");
        assertTrue(json.endsWith("}]"), "Should end with closing brace+bracket");
        // Count occurrences of "name" key — one per employee
        assertEquals(100, json.split("\"name\"").length - 1, "Should have all 100 employees");
    }

    @Test
    @DisplayName("stream JSON: filtered query")
    void testStreamJsonFiltered() throws Exception {
        String query = "Employee.all()->filter({e | $e.department == 'Sales'})->project(~[name:e|$e.name, salary:e|$e.salary])";

        var out = new ByteArrayOutputStream();
        queryService.stream(PURE_MODEL, query, "test::TestRuntime", connection, out, "json");

        String json = out.toString(StandardCharsets.UTF_8);
        assertTrue(json.startsWith("["), "Should start with array bracket");
        assertTrue(json.contains("\"name\":\"Employee_"), "Should contain employee names");
        assertFalse(json.contains("null"), "Sales employees should have no null fields");
    }

    @Test
    @DisplayName("stream JSON: with limit")
    void testStreamJsonLimit() throws Exception {
        String query = "Employee.all()->project(~[name:e|$e.name])->limit(3)";

        var out = new ByteArrayOutputStream();
        queryService.stream(PURE_MODEL, query, "test::TestRuntime", connection, out, "json");

        String json = out.toString(StandardCharsets.UTF_8);
        assertTrue(json.startsWith("[{"), "Should be JSON array of objects");
        assertTrue(json.contains("Employee_"), "Should contain employee data");
    }

    @Test
    @DisplayName("stream JSON: empty result set")
    void testStreamJsonEmpty() throws Exception {
        String query = "Employee.all()->filter({e | $e.salary > 999999})->project(~[name:e|$e.name])";

        var out = new ByteArrayOutputStream();
        queryService.stream(PURE_MODEL, query, "test::TestRuntime", connection, out, "json");

        String json = out.toString(StandardCharsets.UTF_8);
        assertEquals("[]", json, "Empty result should be empty JSON array");
    }

    // ==================== CSV Streaming ====================

    @Test
    @DisplayName("stream CSV: header + data rows")
    void testStreamCsvFull() throws Exception {
        String query = "Employee.all()->project(~[name:e|$e.name, department:e|$e.department, salary:e|$e.salary])->limit(5)";

        var out = new ByteArrayOutputStream();
        queryService.stream(PURE_MODEL, query, "test::TestRuntime", connection, out, "csv");

        String csv = out.toString(StandardCharsets.UTF_8);
        String[] lines = csv.split("\r\n");

        assertEquals(6, lines.length, "Should have header + 5 data rows");
        assertEquals("name,department,salary", lines[0], "First line should be header");
        assertTrue(lines[1].contains("Employee_"), "Data rows should contain employee names");
    }

    @Test
    @DisplayName("stream CSV: all 100 rows")
    void testStreamCsvAllRows() throws Exception {
        String query = "Employee.all()->project(~[name:e|$e.name])";

        var out = new ByteArrayOutputStream();
        queryService.stream(PURE_MODEL, query, "test::TestRuntime", connection, out, "csv");

        String csv = out.toString(StandardCharsets.UTF_8);
        String[] lines = csv.split("\r\n");
        assertEquals(101, lines.length, "Should have header + 100 data rows");
    }

    @Test
    @DisplayName("stream CSV: multiple columns with filter")
    void testStreamCsvFiltered() throws Exception {
        String query = "Employee.all()->filter({e | $e.department == 'Engineering'})->project(~[name:e|$e.name, salary:e|$e.salary])";

        var out = new ByteArrayOutputStream();
        queryService.stream(PURE_MODEL, query, "test::TestRuntime", connection, out, "csv");

        String csv = out.toString(StandardCharsets.UTF_8);
        String[] lines = csv.split("\r\n");

        assertTrue(lines.length > 1, "Should have header + data rows");
        assertEquals("name,salary", lines[0], "Header should match projected columns");
    }

    // ==================== Registry ====================

    @Test
    @DisplayName("SerializerRegistry: Returns correct serializers")
    void testSerializerRegistry() {
        assertTrue(SerializerRegistry.isSupported("json"), "JSON should be supported");
        assertTrue(SerializerRegistry.isSupported("csv"), "CSV should be supported");
        assertFalse(SerializerRegistry.isSupported("avro"), "Avro should not be supported yet");

        assertEquals("application/json", SerializerRegistry.get("json").contentType());
        assertEquals("text/csv", SerializerRegistry.get("csv").contentType());

        assertTrue(SerializerRegistry.get("json").supportsStreaming(), "JSON should support streaming");
        assertTrue(SerializerRegistry.get("csv").supportsStreaming(), "CSV should support streaming");
    }

    @Test
    @DisplayName("SerializerRegistry: Throws on unknown format")
    void testSerializerRegistryUnknownFormat() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> SerializerRegistry.get("unknown_format"));

        assertTrue(ex.getMessage().contains("Unknown serialization format"), "Should have helpful error message");
        assertTrue(ex.getMessage().contains("json"), "Should list available formats");
    }
}
