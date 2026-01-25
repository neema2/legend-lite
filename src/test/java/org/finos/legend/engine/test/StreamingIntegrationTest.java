package org.finos.legend.engine.test;

import org.finos.legend.engine.execution.BufferedResult;
import org.finos.legend.engine.execution.Result;
import org.finos.legend.engine.execution.Row;
import org.finos.legend.engine.execution.StreamingResult;
import org.finos.legend.engine.serialization.CsvSerializer;
import org.finos.legend.engine.serialization.JsonSerializer;
import org.finos.legend.engine.serialization.SerializerRegistry;
import org.finos.legend.engine.server.QueryService;
import org.finos.legend.engine.server.QueryService.ResultMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for streaming result functionality.
 * 
 * Tests verify:
 * - StreamingResult lazy iteration
 * - Proper resource cleanup
 * - Streaming serialization (JSON, CSV)
 * - Conversion between streaming and buffered modes
 */
class StreamingIntegrationTest {

    private Connection connection;
    private QueryService queryService;

    private static final String PURE_MODEL = """
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
                mappings: [ model::EmployeeMapping ];
                connections: [
                    store::EmployeeDb: [
                        conn1: store::TestConnection
                    ]
                ];
            }
            """;

    @BeforeEach
    void setUp() throws Exception {
        connection = DriverManager.getConnection("jdbc:duckdb:");
        queryService = new QueryService();

        // Create table with test data
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                    CREATE TABLE T_EMPLOYEE (
                        ID INTEGER PRIMARY KEY,
                        NAME VARCHAR(100) NOT NULL,
                        DEPARTMENT VARCHAR(100) NOT NULL,
                        SALARY INTEGER NOT NULL
                    )
                    """);

            // Insert 100 employees for streaming tests
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

    // ==================== Streaming Mode Tests ====================

    @Test
    @DisplayName("StreamingResult: Lazy iteration consumes rows on demand")
    void testStreamingResultLazyIteration() throws Exception {
        String query = "Employee.all()->project({e | $e.name}, {e | $e.salary})";

        try (Result result = queryService.execute(PURE_MODEL, query, "test::TestRuntime", connection,
                ResultMode.STREAMING)) {
            assertTrue(result instanceof StreamingResult, "Should return StreamingResult");
            assertEquals(-1, result.rowCount(), "Row count should be unknown for streaming");

            // Iterate and count
            AtomicInteger count = new AtomicInteger(0);
            result.iterator().forEachRemaining(row -> count.incrementAndGet());

            assertEquals(100, count.get(), "Should have iterated over all 100 rows");
        }
    }

    @Test
    @DisplayName("StreamingResult: Stream API with limit stops early")
    void testStreamingResultWithLimit() throws Exception {
        String query = "Employee.all()->project({e | $e.name}, {e | $e.salary})";

        List<Row> collected = new ArrayList<>();

        try (Result result = queryService.execute(PURE_MODEL, query, "test::TestRuntime", connection,
                ResultMode.STREAMING)) {
            // Only take first 10 rows
            result.stream()
                    .limit(10)
                    .forEach(collected::add);
        }

        assertEquals(10, collected.size(), "Should have collected exactly 10 rows");
        assertTrue(collected.get(0).get(0).toString().contains("Employee_"), "Should have employee names");
    }

    @Test
    @DisplayName("StreamingResult: toBuffered() materializes all rows")
    void testStreamingToBuffered() throws Exception {
        String query = "Employee.all()->filter({e | $e.department == 'Engineering'})->project({e | $e.name})";

        try (Result streaming = queryService.execute(PURE_MODEL, query, "test::TestRuntime", connection,
                ResultMode.STREAMING)) {
            BufferedResult buffered = streaming.toBuffered();

            // Engineering is every 3rd employee (i % 3 == 0), so ~33 employees
            assertTrue(buffered.rowCount() > 0, "Should have some rows");
            assertTrue(buffered.rowCount() <= 34, "Should have around 33-34 Engineering employees");

            // Can iterate multiple times on buffered
            long firstCount = buffered.stream().count();
            long secondCount = buffered.stream().count();
            assertEquals(firstCount, secondCount, "Buffered result should be re-iterable");
        }
    }

    @Test
    @DisplayName("StreamingResult: Resources are released on close")
    void testStreamingResultResourceCleanup() throws Exception {
        String query = "Employee.all()->project({e | $e.name})";

        Result result = queryService.execute(PURE_MODEL, query, "test::TestRuntime", connection, ResultMode.STREAMING);

        // Consume a few rows
        var iterator = result.iterator();
        iterator.next();
        iterator.next();

        // Close without consuming all
        result.close();

        // Attempting to iterate after close should fail gracefully
        assertFalse(iterator.hasNext(), "Iterator should report no more rows after close");
    }

    // ==================== Buffered Mode Tests ====================

    @Test
    @DisplayName("BufferedResult: All rows materialized immediately")
    void testBufferedResultMaterialization() throws Exception {
        String query = "Employee.all()->project({e | $e.name}, {e | $e.department})";

        BufferedResult result = queryService.execute(PURE_MODEL, query, "test::TestRuntime", connection);

        assertEquals(100, result.rowCount(), "Should have 100 rows materialized");
        assertEquals(2, result.columnCount(), "Should have 2 columns");

        // Random access works
        Object name = result.getValue(0, "name");
        assertNotNull(name, "Should be able to access by column name");
    }

    @Test
    @DisplayName("BufferedResult: toJsonArray produces valid JSON")
    void testBufferedResultToJson() throws Exception {
        String query = "Employee.all()->filter({e | $e.salary > 140000})->project({e | $e.name}, {e | $e.salary})";

        BufferedResult result = queryService.execute(PURE_MODEL, query, "test::TestRuntime", connection);

        String json = result.toJsonArray();

        assertTrue(json.startsWith("["), "JSON should start with array");
        assertTrue(json.endsWith("]"), "JSON should end with array");
        assertTrue(json.contains("\"name\":"), "JSON should have name field");
        assertTrue(json.contains("\"salary\":"), "JSON should have salary field");
    }

    // ==================== Serializer Tests ====================

    @Test
    @DisplayName("JsonSerializer: Streaming serialization produces valid JSON")
    void testJsonSerializerStreaming() throws Exception {
        String query = "Employee.all()->filter({e | $e.department == 'Sales'})->project({e | $e.name}, {e | $e.salary})";

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try (Result result = queryService.execute(PURE_MODEL, query, "test::TestRuntime", connection,
                ResultMode.STREAMING)) {
            JsonSerializer.INSTANCE.serializeStreaming((StreamingResult) result, out);
        }

        String json = out.toString(StandardCharsets.UTF_8);

        assertTrue(json.startsWith("["), "Should start with array bracket");
        assertTrue(json.endsWith("]"), "Should end with array bracket");
        assertTrue(json.contains("\"name\":\"Employee_"), "Should contain employee names");
        assertFalse(json.contains("null"), "Should not have null values in this query");

        System.out
                .println("Streaming JSON output (first 500 chars): " + json.substring(0, Math.min(500, json.length())));
    }

    @Test
    @DisplayName("CsvSerializer: Streaming serialization produces valid CSV")
    void testCsvSerializerStreaming() throws Exception {
        String query = "Employee.all()->project({e | $e.name}, {e | $e.department}, {e | $e.salary})->limit(5)";

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try (Result result = queryService.execute(PURE_MODEL, query, "test::TestRuntime", connection,
                ResultMode.STREAMING)) {
            CsvSerializer.INSTANCE.serializeStreaming((StreamingResult) result, out);
        }

        String csv = out.toString(StandardCharsets.UTF_8);
        String[] lines = csv.split("\r\n");

        assertEquals(6, lines.length, "Should have header + 5 data rows");
        assertEquals("name,department,salary", lines[0], "First line should be header");
        assertTrue(lines[1].contains("Employee_"), "Data rows should contain employee names");

        System.out.println("Streaming CSV output:\n" + csv);
    }

    @Test
    @DisplayName("executeAndSerialize: Convenience method streams to JSON")
    void testExecuteAndSerializeJson() throws Exception {
        String query = "Employee.all()->project({e | $e.name})->limit(3)";

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        queryService.executeAndSerialize(PURE_MODEL, query, "test::TestRuntime", connection, out, "json");

        String json = out.toString(StandardCharsets.UTF_8);

        assertTrue(json.startsWith("[{"), "Should be JSON array of objects");
        assertTrue(json.contains("Employee_"), "Should contain employee data");
    }

    @Test
    @DisplayName("executeAndSerialize: Convenience method streams to CSV")
    void testExecuteAndSerializeCsv() throws Exception {
        String query = "Employee.all()->project({e | $e.name}, {e | $e.salary})->limit(3)";

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        queryService.executeAndSerialize(PURE_MODEL, query, "test::TestRuntime", connection, out, "csv");

        String csv = out.toString(StandardCharsets.UTF_8);
        String[] lines = csv.split("\r\n");

        assertEquals(4, lines.length, "Should have header + 3 data rows");
        assertTrue(lines[0].contains("name"), "Header should contain column names");
    }

    // ==================== Registry Tests ====================

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
