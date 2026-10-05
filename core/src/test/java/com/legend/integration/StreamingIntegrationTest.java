package com.legend.integration;

import com.legend.server.serial.SerializerRegistry;
import com.legend.server.OutputFormat;
import com.legend.server.QueryService;
import com.legend.json.Json;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link QueryService#execute} and {@link QueryService#stream}.
 *
 * <p>Coverage:
 * <ul>
 *   <li>{@code testExecuteJson*} / {@code testExecuteCsv*} — materialized
 *       snapshot path that serializes an {@link com.legend.exec.ExecutionResult}
 *       to an OutputStream in the given {@link OutputFormat}.</li>
 *   <li>{@code testStream*} — true streaming path (no materialization).
 *       Two of these tests are mutation-verified proofs that streaming is
 *       actually happening: {@code testStreamMultipleWrites} and
 *       {@code testStreamOutputVisibleIncrementallyDuringExecution}.</li>
 * </ul>
 */
class StreamingIntegrationTest {

    private Connection connection;
    private QueryService queryService;

    private static final String PURE_MODEL = """
            ###Pure
            import model::*;
            import store::*;
            import test::*;

            Class model::Employee
            {
                name: String[1];
                department: String[1];
                salary: Integer[1];
            }

            ###Relational
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

            ###Mapping
            import model::*;
            import store::*;
            import test::*;
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

            ###Connection
            import model::*;
            import store::*;
            import test::*;
            RelationalDatabaseConnection store::TestConnection
            {
                type: DuckDB;
                specification: DuckDB { };
                auth: Test;
            }

            ###Runtime
            import model::*;
            import store::*;
            import test::*;
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

    // ==================== execute() snapshot-to-OutputStream (materialized) ====================

    @Test
    @DisplayName("execute JSON: full result set")
    void testExecuteJsonFull() throws Exception {
        String query = "model::Employee.all()->project(~[name:e|$e.name, salary:e|$e.salary])";

        var out = new ByteArrayOutputStream();
        queryService.execute(PURE_MODEL, query, "test::TestRuntime", connection, out, OutputFormat.JSON);

        String json = out.toString(StandardCharsets.UTF_8);
        assertTrue(json.startsWith("[{"), "Should be JSON array of objects");
        assertTrue(json.endsWith("}]"), "Should end with closing brace+bracket");
        // Count occurrences of "name" key — one per employee
        assertEquals(100, json.split("\"name\"").length - 1, "Should have all 100 employees");
    }

    @Test
    @DisplayName("execute JSON: filtered query")
    void testExecuteJsonFiltered() throws Exception {
        String query = "model::Employee.all()->filter({e | $e.department == 'Sales'})->project(~[name:e|$e.name, salary:e|$e.salary])";

        var out = new ByteArrayOutputStream();
        queryService.execute(PURE_MODEL, query, "test::TestRuntime", connection, out, OutputFormat.JSON);

        String json = out.toString(StandardCharsets.UTF_8);
        assertTrue(json.startsWith("["), "Should start with array bracket");
        assertTrue(json.contains("\"name\":\"Employee_"), "Should contain employee names");
        assertFalse(json.contains("null"), "Sales employees should have no null fields");
    }

    @Test
    @DisplayName("execute JSON: with limit")
    void testExecuteJsonLimit() throws Exception {
        String query = "model::Employee.all()->project(~[name:e|$e.name])->limit(3)";

        var out = new ByteArrayOutputStream();
        queryService.execute(PURE_MODEL, query, "test::TestRuntime", connection, out, OutputFormat.JSON);

        String json = out.toString(StandardCharsets.UTF_8);
        assertTrue(json.startsWith("[{"), "Should be JSON array of objects");
        assertTrue(json.contains("Employee_"), "Should contain employee data");
    }

    @Test
    @DisplayName("execute JSON: empty result set")
    void testExecuteJsonEmpty() throws Exception {
        String query = "model::Employee.all()->filter({e | $e.salary > 999999})->project(~[name:e|$e.name])";

        var out = new ByteArrayOutputStream();
        queryService.execute(PURE_MODEL, query, "test::TestRuntime", connection, out, OutputFormat.JSON);

        String json = out.toString(StandardCharsets.UTF_8);
        assertEquals("[]", json, "Empty result should be empty JSON array");
    }

    @Test
    @DisplayName("execute CSV: header + data rows")
    void testExecuteCsvFull() throws Exception {
        String query = "model::Employee.all()->project(~[name:e|$e.name, department:e|$e.department, salary:e|$e.salary])->limit(5)";

        var out = new ByteArrayOutputStream();
        queryService.execute(PURE_MODEL, query, "test::TestRuntime", connection, out, OutputFormat.CSV);

        String csv = out.toString(StandardCharsets.UTF_8);
        String[] lines = csv.split("\r\n");

        assertEquals(6, lines.length, "Should have header + 5 data rows");
        assertEquals("name,department,salary", lines[0], "First line should be header");
        assertTrue(lines[1].contains("Employee_"), "Data rows should contain employee names");
    }

    @Test
    @DisplayName("execute CSV: all 100 rows")
    void testExecuteCsvAllRows() throws Exception {
        String query = "model::Employee.all()->project(~[name:e|$e.name])";

        var out = new ByteArrayOutputStream();
        queryService.execute(PURE_MODEL, query, "test::TestRuntime", connection, out, OutputFormat.CSV);

        String csv = out.toString(StandardCharsets.UTF_8);
        String[] lines = csv.split("\r\n");
        assertEquals(101, lines.length, "Should have header + 100 data rows");
    }

    @Test
    @DisplayName("execute CSV: multiple columns with filter")
    void testExecuteCsvFiltered() throws Exception {
        String query = "model::Employee.all()->filter({e | $e.department == 'Engineering'})->project(~[name:e|$e.name, salary:e|$e.salary])";

        var out = new ByteArrayOutputStream();
        queryService.execute(PURE_MODEL, query, "test::TestRuntime", connection, out, OutputFormat.CSV);

        String csv = out.toString(StandardCharsets.UTF_8);
        String[] lines = csv.split("\r\n");

        assertTrue(lines.length > 1, "Should have header + data rows");
        assertEquals("name,salary", lines[0], "Header should match projected columns");
    }

    // ==================== stream() true streaming (no materialization) ====================
    //
    // These tests verify that stream() actually streams — bytes flow to the
    // OutputStream as rows are fetched from JDBC, rather than all being
    // buffered before a single final write. Two techniques:
    //
    //   1. CountingOutputStream records every write() call. QueryService.stream
    //      wraps in OutputStreamWriter (8KB buffer) and calls flush() after
    //      each row, so the OutputStream sees ~100 write calls for a 100-row
    //      query under streaming; a materialized impl produces ~1.
    //
    //   2. A recording connection counts ResultSet.next(), and every write
    //      notes how many rows had been fetched: streaming writes at many
    //      distinct points of the fetch, a materialized impl at one or two
    //      (no clock, no observer thread: Bazel workplan P3-15).
    //
    // Both tests are mutation-verified: temporarily replacing stream() with a
    // materialized impl causes them to fail with clear messages.

    /** OutputStream that tracks the number and sizes of write calls. */
    static final class CountingOutputStream extends OutputStream {
        final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        final AtomicInteger writeCallCount = new AtomicInteger(0);
        final List<Integer> writeCallSizeHistory = new ArrayList<>();

        @Override public void write(int b) {
            recordWrite(1);
            buffer.write(b);
        }
        @Override public void write(byte[] b, int off, int len) {
            recordWrite(len);
            buffer.write(b, off, len);
        }
        @Override public void flush() {}
        @Override public void close() {}

        String asString() {
            return buffer.toString(StandardCharsets.UTF_8);
        }

        private void recordWrite(int size) {
            writeCallCount.incrementAndGet();
            writeCallSizeHistory.add(size);
        }
    }

    @Test
    @DisplayName("stream: produces valid JSON matching the materialized path")
    void testStreamValidityAndParity() throws Exception {
        String query = "model::Employee.all()->project(~[name:e|$e.name, salary:e|$e.salary])->limit(5)";

        var baos = new ByteArrayOutputStream();
        queryService.stream(PURE_MODEL, query, "test::TestRuntime", connection, baos);

        String streamed = baos.toString(StandardCharsets.UTF_8);
        assertTrue(streamed.startsWith("[{"), "Streamed output should start with [{");
        assertTrue(streamed.endsWith("}]"), "Streamed output should end with }]");

        // Parse: if the streamed bytes are malformed JSON, Json.parse will throw.
        Json.Node parsed = Json.parse(streamed);
        assertInstanceOf(Json.Arr.class, parsed, "Streamed root must be an array");
        assertEquals(5, ((Json.Arr) parsed).items().size(), "All 5 rows must be present");
    }

    @Test
    @DisplayName("stream: emits many small writes (proves streaming, not buffered) [MUTATION-VERIFIED]")
    void testStreamMultipleWrites() throws Exception {
        // Streaming flushes after each row. For 100 rows, the underlying
        // OutputStream should see roughly 100 write() calls. A materialized
        // impl would have OutputStreamWriter buffer everything and see 1
        // write at close() time.
        String query = "model::Employee.all()->project(~[name:e|$e.name, department:e|$e.department, salary:e|$e.salary])";

        CountingOutputStream cos = new CountingOutputStream();
        queryService.stream(PURE_MODEL, query, "test::TestRuntime", connection, cos);

        int writes = cos.writeCallCount.get();
        String result = cos.asString();

        // Sanity: all 100 rows present
        Json.Node parsed = Json.parse(result);
        assertEquals(100, ((Json.Arr) parsed).items().size());

        // The key streaming assertion: one flush per row → ~100 OutputStream
        // writes. A materialized impl would write once at the end.
        assertTrue(writes >= 50,
                "Expected >=50 OutputStream.write calls (roughly one per row flush) for "
                        + "streaming emission; got " + writes + ". A low count strongly suggests "
                        + "the result was materialized before writing.");
    }

    @Test
    @DisplayName("stream: bytes reach the output while rows are still being fetched [MUTATION-VERIFIED]")
    void testStreamOutputVisibleIncrementallyDuringExecution() throws Exception {
        // The streaming proof without a clock (Bazel workplan P3-15; it sampled the buffer from a thread every 1 ms
        // around a busy-wait): a recording connection counts ResultSet.next() calls, and the output stream notes how
        // many rows had been fetched at each write. Streaming writes as it fetches, at many fetched-row counts; a
        // materialized implementation fetches all 100 rows first and writes at one or two.
        String query = "model::Employee.all()->project(~[name:e|$e.name, dept:e|$e.department, salary:e|$e.salary])";
        assertWrittenThroughoutFetch(query);
    }

    @Test
    @DisplayName("stream: empty result emits valid '[]'")
    void testStreamEmptyResultSet() throws Exception {
        String query = "model::Employee.all()->filter({e | $e.salary > 9999999})->project(~[name:e|$e.name])";

        var baos = new ByteArrayOutputStream();
        queryService.stream(PURE_MODEL, query, "test::TestRuntime", connection, baos);

        assertEquals("[]", baos.toString(StandardCharsets.UTF_8),
                "Empty result should stream as '[]'");
    }

    // ==================== stream() graphFetch (byte-passthrough per row) ====================
    //
    // Graph-shaped results are emitted by PlanGenerator in two shapes depending
    // on Mode:
    //   SNAPSHOT  (execute): SELECT json_group_array(json_object(...)) FROM ...
    //                        → 1 JDBC row, whole JSON array pre-built by DB.
    //   STREAMING (stream):  SELECT json_object(...) FROM ...
    //                        → N JDBC rows, one json_object each.
    //
    // PlanExecutor.streamGraph reads rows from the STREAMING SQL and writes
    // each row's JSON verbatim with ',' separators inside '[' ... ']'. No
    // StringBuilder, no Java-side concat — bytes pass through.

    @Test
    @DisplayName("stream graphFetch: produces valid JSON matching the materialized path")
    void testStreamGraphFetchValidityAndParity() throws Exception {
        String query = """
                model::Employee.all()
                    ->graphFetch(#{ Employee { name, department, salary } }#)
                    ->serialize(#{ Employee { name, department, salary } }#)
                """;

        var baos = new ByteArrayOutputStream();
        queryService.stream(PURE_MODEL, query, "test::TestRuntime", connection, baos);

        String streamed = baos.toString(StandardCharsets.UTF_8);
        assertTrue(streamed.startsWith("[") && streamed.endsWith("]"),
                "Streamed graphFetch must be a JSON array: " + streamed);

        Json.Node parsed = Json.parse(streamed);
        assertInstanceOf(Json.Arr.class, parsed, "Streamed root must be an array");
        assertEquals(100, ((Json.Arr) parsed).items().size(),
                "All 100 employees must be present in streamed output");

        // Every item must be an object with the graphFetch-projected properties.
        Json.Obj first = (Json.Obj) ((Json.Arr) parsed).items().get(0);
        assertNotNull(first.getString("name"), "Row must have 'name'");
        assertNotNull(first.getString("department"), "Row must have 'department'");
    }

    @Test
    @DisplayName("stream graphFetch: emits many small writes (proves byte-passthrough, not materialized) [MUTATION-VERIFIED]")
    void testStreamGraphFetchMultipleWrites() throws Exception {
        // streamGraph writes per-row JSON with a flush after each row. For
        // 100 rows, the underlying OutputStream should see ~100 writes. A
        // materialized fallback (toJsonArray() then one write()) produces 1.
        String query = """
                model::Employee.all()
                    ->graphFetch(#{ Employee { name, department, salary } }#)
                    ->serialize(#{ Employee { name, department, salary } }#)
                """;

        CountingOutputStream cos = new CountingOutputStream();
        queryService.stream(PURE_MODEL, query, "test::TestRuntime", connection, cos);

        int writes = cos.writeCallCount.get();
        String result = cos.asString();

        // Sanity: all 100 rows present
        Json.Node parsed = Json.parse(result);
        assertEquals(100, ((Json.Arr) parsed).items().size());

        assertTrue(writes >= 50,
                "Expected >=50 OutputStream.write calls (roughly one per row flush) for "
                        + "streaming graphFetch; got " + writes + ". A low count strongly suggests "
                        + "the Graph result was materialized before writing (snapshot fallback).");
    }

    @Test
    @DisplayName("stream graphFetch: bytes reach the output while rows are still being fetched [MUTATION-VERIFIED]")
    void testStreamGraphFetchOutputVisibleIncrementallyDuringExecution() throws Exception {
        // the graphFetch mirror of testStreamOutputVisibleIncrementallyDuringExecution, without a clock (P3-15)
        String query = """
                model::Employee.all()
                    ->graphFetch(#{ Employee { name, department, salary } }#)
                    ->serialize(#{ Employee { name, department, salary } }#)
                """;
        assertWrittenThroughoutFetch(query);
    }

    /** Streams {@code query} over a connection that counts every ResultSet.next(), and asserts the output was
     *  written at many different points of the fetch: streaming writes as it fetches, so its writes land at many
     *  distinct fetched-row counts; a materialized result (even one that writes an opening "[" first) writes at one
     *  or two. */
    private void assertWrittenThroughoutFetch(String query) throws Exception {
        AtomicInteger fetched = new AtomicInteger();
        java.util.Set<Integer> fetchedAtWrites = java.util.Collections.synchronizedSet(new java.util.TreeSet<>());
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        OutputStream out = new OutputStream() {
            @Override public void write(int b) {
                fetchedAtWrites.add(fetched.get());
                buffer.write(b);
            }
            @Override public void write(byte[] b, int off, int len) {
                fetchedAtWrites.add(fetched.get());
                buffer.write(b, off, len);
            }
            @Override public void flush() {}
            @Override public void close() {}
        };
        queryService.stream(PURE_MODEL, query, "test::TestRuntime", countingRows(connection, fetched), out);
        assertTrue(buffer.size() > 0, "Should have produced some output");
        assertTrue(fetched.get() >= 100, "the recording connection saw " + fetched.get() + " rows fetched, not the"
                + " 100 the query reads: the stream bypassed it, and this test proves nothing");
        long midFetch = fetchedAtWrites.stream().filter(n -> n > 0 && n < fetched.get()).count();
        assertTrue(midFetch >= 50, "the output was written at only " + midFetch + " distinct points while rows were"
                + " still being fetched (" + fetchedAtWrites + "): the result was materialized before writing");
    }

    /** {@code inner}, with every ResultSet its statements return counting its next() calls into {@code fetched}. */
    private static Connection countingRows(Connection inner, AtomicInteger fetched) {
        return (Connection) java.lang.reflect.Proxy.newProxyInstance(StreamingIntegrationTest.class.getClassLoader(),
                new Class<?>[] {Connection.class}, (proxy, m, args) -> {
                    Object r = invoke(inner, m, args);
                    if (r instanceof java.sql.PreparedStatement ps) {
                        return wrapStatement(ps, java.sql.PreparedStatement.class, fetched);
                    }
                    if (r instanceof Statement st) {
                        return wrapStatement(st, Statement.class, fetched);
                    }
                    return r;
                });
    }

    private static <S extends Statement> S wrapStatement(S inner, Class<S> type, AtomicInteger fetched) {
        return type.cast(java.lang.reflect.Proxy.newProxyInstance(StreamingIntegrationTest.class.getClassLoader(),
                new Class<?>[] {type}, (proxy, m, args) -> {
                    Object r = invoke(inner, m, args);
                    if (r instanceof java.sql.ResultSet rs) {
                        return java.lang.reflect.Proxy.newProxyInstance(StreamingIntegrationTest.class.getClassLoader(),
                                new Class<?>[] {java.sql.ResultSet.class}, (p2, m2, a2) -> {
                                    Object v = invoke(rs, m2, a2);
                                    if (m2.getName().equals("next") && Boolean.TRUE.equals(v)) {
                                        fetched.incrementAndGet();
                                    }
                                    return v;
                                });
                    }
                    return r;
                }));
    }

    private static Object invoke(Object target, java.lang.reflect.Method m, Object[] args) throws Throwable {
        try {
            return m.invoke(target, args);
        } catch (java.lang.reflect.InvocationTargetException e) {
            throw e.getCause();
        }
    }

    @Test
    @DisplayName("stream graphFetch: empty result emits valid '[]'")
    void testStreamGraphFetchEmpty() throws Exception {
        String query = """
                model::Employee.all()
                    ->filter({e | $e.salary > 9999999})
                    ->graphFetch(#{ Employee { name } }#)
                    ->serialize(#{ Employee { name } }#)
                """;

        var baos = new ByteArrayOutputStream();
        queryService.stream(PURE_MODEL, query, "test::TestRuntime", connection, baos);

        assertEquals("[]", baos.toString(StandardCharsets.UTF_8),
                "Empty graphFetch should stream as '[]'");
    }

    @Test
    @DisplayName("stream: escape coverage at streaming boundary (\\n, \\t, \\\", control chars)")
    void testStreamEscapesControlCharsThroughStream() throws Exception {
        // Insert a row whose value contains characters that previously
        // broke the hand-rolled row serializers: tab, quote, backslash,
        // newline, and a raw control character (0x01 — BEL-range).
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("INSERT INTO T_EMPLOYEE VALUES (9001, 'tab\there\t\"quote\\bk\nline\u0001ctl', 'Engineering', 42)");
        }

        String query = "model::Employee.all()->filter({e | $e.name->startsWith('tab')})->project(~[name:e|$e.name])";

        var baos = new ByteArrayOutputStream();
        queryService.stream(PURE_MODEL, query, "test::TestRuntime", connection, baos);

        String streamed = baos.toString(StandardCharsets.UTF_8);

        // Must be parseable — a broken escape would produce invalid JSON.
        Json.Node parsed = Json.parse(streamed);
        Json.Arr arr = (Json.Arr) parsed;
        assertEquals(1, arr.items().size());

        // Round-trip: the decoded name must equal the inserted value exactly.
        Json.Obj row = (Json.Obj) arr.items().get(0);
        assertEquals("tab\there\t\"quote\\bk\nline\u0001ctl", row.getString("name"));
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
