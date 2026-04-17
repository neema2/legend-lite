package com.gs.legend.server;

import com.gs.legend.util.Json;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for PureLspServer — LSP protocol implementation.
 *
 * Uses {@link LspTestClient} for typed access to JSON-RPC messages
 * and strong assertions on URIs, diagnostics, error messages, and line numbers.
 */
class PureLspServerTest {

    private PureLspServer server;
    private LspTestClient lsp;

    @BeforeEach
    void setUp() {
        server = new PureLspServer();
        lsp = new LspTestClient(server);
    }

    // ==================== Protocol Tests ====================

    @Test
    void testInitialize() {
        var result = lsp.initialize();
        assertNotNull(result.get("capabilities"));
        var serverInfo = obj(result, "serverInfo");
        assertEquals("legend-lite-lsp", serverInfo.get("name"));
        assertEquals("1.0.0", serverInfo.get("version"));
    }

    @Test
    void testInitializedIsNoOp() {
        List<String> responses = server.handleMessage("""
                {"jsonrpc":"2.0","method":"initialized","params":{}}""");
        assertTrue(responses.isEmpty());
    }

    @Test
    void testShutdown() {
        var result = lsp.shutdown(42);
        assertEquals(42, ((Number) result.get("id")).intValue());
        assertNull(result.get("error"));
    }

    @Test
    void testUnknownMethodReturnsError() {
        List<String> responses = server.handleMessage("""
                {"jsonrpc":"2.0","id":7,"method":"textDocument/hover","params":{}}""");
        assertEquals(1, responses.size());
        var resp = parseAsMap(responses.get(0));
        assertEquals(7, ((Number) resp.get("id")).intValue());
        var error = obj(resp, "error");
        assertEquals(-32601, ((Number) error.get("code")).intValue());
        assertTrue(((String) error.get("message")).contains("hover"));
    }

    // ==================== Single-File Tests ====================

    @Test
    void testDidOpenValidClass_zeroDiagnostics() {
        var diags = lsp.openAndGetDiagnostics(
                "file:///model.pure",
                "Class test::Person { name: String[1]; age: Integer[1]; }");
        var d = diags.get("file:///model.pure");
        assertNotNull(d, "Should have diagnostics for opened URI");
        assertTrue(d.isEmpty(), "Valid class should produce zero diagnostics");
    }

    @Test
    void testDidOpenSyntaxError_reportsLineAndMessage() {
        // Missing colon between property name and type
        var diags = lsp.openAndGetDiagnostics(
                "file:///bad.pure",
                "Class test::Person { name String[1]; }");
        var d = diags.get("file:///bad.pure");
        assertNotNull(d);
        assertEquals(1, d.size(), "Should have exactly one diagnostic");

        var diag = d.get(0);
        assertEquals(0, diag.line(), "Error should be on line 0 (0-indexed from line 1)");
        assertEquals(26, diag.character(), "Error should point to column of 'String'");
        assertEquals(1, diag.severity(), "Should be severity 1 (Error)");
        assertEquals("legend-lite", diag.source());
        assertTrue(diag.message().contains("missing COLON"),
                "Message should mention missing colon, got: " + diag.message());
    }

    @Test
    void testDidChange_updatesDocument() {
        lsp.open("file:///test.pure", "Class test::A { x: String[1]; }");
        var diags = lsp.changeAndGetDiagnostics(
                "file:///test.pure",
                "Class test::A { x: String[1]; y: Integer[1]; }");
        assertTrue(diags.get("file:///test.pure").isEmpty());
        assertEquals("Class test::A { x: String[1]; y: Integer[1]; }",
                server.getDocument("file:///test.pure"));
    }

    @Test
    void testDidClose_clearsDocument() {
        lsp.open("file:///test.pure", "Class test::A { x: String[1]; }");
        assertTrue(server.hasDocument("file:///test.pure"));

        var diags = lsp.closeAndGetDiagnostics("file:///test.pure");
        assertFalse(server.hasDocument("file:///test.pure"));
        // Closed file gets empty diagnostics to clear editor squiggles
        assertTrue(diags.containsKey("file:///test.pure"));
        assertTrue(diags.get("file:///test.pure").isEmpty());
    }

    // ==================== Multi-File Tests ====================

    @Test
    void testTwoValidFiles_bothGetEmptyDiagnostics() {
        lsp.open("file:///a.pure", "Class test::Person { name: String[1]; }");
        var diags = lsp.openAndGetDiagnostics(
                "file:///b.pure",
                "Class test::Trade { ticker: String[1]; qty: Integer[1]; }");
        assertEquals(2, diags.size());
        assertTrue(diags.get("file:///a.pure").isEmpty(), "a.pure should be clean");
        assertTrue(diags.get("file:///b.pure").isEmpty(), "b.pure should be clean");
    }

    @Test
    void testErrorInOneFile_otherStaysClean() {
        lsp.open("file:///model.pure", "Class test::Person { name: String[1]; }");
        var diags = lsp.openAndGetDiagnostics(
                "file:///bad.pure",
                "Class test::Trade { ticker String[1]; }");

        assertEquals(2, diags.size());
        assertTrue(diags.get("file:///model.pure").isEmpty(), "model.pure should be clean");
        assertFalse(diags.get("file:///bad.pure").isEmpty(), "bad.pure should have error");
    }

    @Test
    void testFixingBrokenFile_clearsBothDiagnostics() {
        lsp.open("file:///model.pure", "Class test::Person { name: String[1]; }");
        lsp.open("file:///store.pure", "Class test::Trade { ticker String[1]; }");

        // Verify error exists
        var before = lsp.changeAndGetDiagnostics("file:///store.pure",
                "Class test::Trade { ticker String[1]; }");
        assertFalse(before.get("file:///store.pure").isEmpty());

        // Fix the file
        var after = lsp.changeAndGetDiagnostics("file:///store.pure",
                "Class test::Trade { ticker: String[1]; qty: Integer[1]; }");
        assertEquals(2, after.size());
        assertTrue(after.get("file:///model.pure").isEmpty());
        assertTrue(after.get("file:///store.pure").isEmpty());
    }

    @Test
    void testCloseFile_remainingFileStillDiagnosed() {
        lsp.open("file:///a.pure", "Class test::A { x: String[1]; }");
        lsp.open("file:///b.pure", "Class test::B { y: Integer[1]; }");

        var diags = lsp.closeAndGetDiagnostics("file:///a.pure");
        // Closed file: empty diagnostics to clear squiggles
        assertTrue(diags.get("file:///a.pure").isEmpty());
        // Remaining file: still diagnosed
        assertTrue(diags.containsKey("file:///b.pure"));
        assertFalse(server.hasDocument("file:///a.pure"));
        assertTrue(server.hasDocument("file:///b.pure"));
    }

    @Test
    void testThreeFiles_associationAcrossFiles() {
        lsp.open("file:///person.pure", "Class test::Person { name: String[1]; }");
        lsp.open("file:///trade.pure", "Class test::Trade { ticker: String[1]; }");
        var diags = lsp.openAndGetDiagnostics("file:///assoc.pure",
                "Association test::PersonTrades { person: test::Person[1]; trades: test::Trade[*]; }");
        assertEquals(3, diags.size());
        assertTrue(diags.get("file:///person.pure").isEmpty());
        assertTrue(diags.get("file:///trade.pure").isEmpty());
        assertTrue(diags.get("file:///assoc.pure").isEmpty());
    }

    // ==================== HTTP Integration Test ====================

    @Test
    void testHttpRoundTrip() throws Exception {
        int port = 19876 + new Random().nextInt(100);
        LegendHttpServer httpServer = new LegendHttpServer(port);
        httpServer.start();
        try {
            // Initialize
            String initResp = httpPost(port, "/lsp", """
                    {"jsonrpc":"2.0","id":1,"method":"initialize","params":{"processId":null,"rootUri":null,"capabilities":{}}}""");
            var initResult = parseAsMap(initResp);
            assertEquals(1, ((Number) initResult.get("id")).intValue());
            assertNotNull(obj(initResult, "result").get("capabilities"));

            // didOpen with valid code — single file → single notification (not array)
            String openResp = httpPost(port, "/lsp", """
                    {"jsonrpc":"2.0","method":"textDocument/didOpen","params":{"textDocument":{"uri":"file:///http-test.pure","languageId":"pure","version":1,"text":"Class test::Foo { x: String[1]; }"}}}""");
            var openResult = parseAsMap(openResp);
            assertEquals("textDocument/publishDiagnostics", openResult.get("method"));
            var openParams = obj(openResult, "params");
            assertEquals("file:///http-test.pure", openParams.get("uri"));
            assertTrue(((List<?>) openParams.get("diagnostics")).isEmpty());

            // didOpen second file — returns JSON array of 2 notifications
            String open2Resp = httpPost(port, "/lsp", """
                    {"jsonrpc":"2.0","method":"textDocument/didOpen","params":{"textDocument":{"uri":"file:///http-test2.pure","languageId":"pure","version":1,"text":"Class test::Bar { y: Integer[1]; }"}}}""");
            // HTTP layer returns JSON array when >1 notification
            assertTrue(open2Resp.trim().startsWith("["), "Multi-file response should be JSON array");

            // Health check
            String healthResp = httpGet(port, "/health");
            assertTrue(healthResp.contains("\"ok\""));
        } finally {
            httpServer.stop();
        }
    }

    // ==================== Helpers ====================

    @SuppressWarnings("unchecked")
    private static Map<String, Object> obj(Map<String, Object> parent, String key) {
        return (Map<String, Object>) parent.get(key);
    }

    private static String httpPost(int port, String path, String body) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) URI.create(
                "http://localhost:" + port + path).toURL().openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);
        conn.getOutputStream().write(body.getBytes(StandardCharsets.UTF_8));
        return new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }

    private static String httpGet(int port, String path) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) URI.create(
                "http://localhost:" + port + path).toURL().openConnection();
        return new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }

    // ==================== LspTestClient ====================

    /**
     * Typed client wrapper for PureLspServer — eliminates JSON ceremony,
     * parses responses into structured records, indexes diagnostics by URI.
     */
    static class LspTestClient {
        private final PureLspServer server;
        private int version = 1;

        LspTestClient(PureLspServer server) { this.server = server; }

        record Diagnostic(int line, int character, int severity, String source, String message) {}

        Map<String, Object> initialize() {
            var responses = server.handleMessage("""
                    {"jsonrpc":"2.0","id":1,"method":"initialize","params":{"processId":null,"rootUri":null,"capabilities":{}}}""");
            assertEquals(1, responses.size());
            var resp = parseAsMap(responses.get(0));
            return obj(resp, "result");
        }

        Map<String, Object> shutdown(int id) {
            var responses = server.handleMessage(
                    "{\"jsonrpc\":\"2.0\",\"id\":" + id + ",\"method\":\"shutdown\"}");
            assertEquals(1, responses.size());
            return parseAsMap(responses.get(0));
        }

        void open(String uri, String text) {
            server.handleMessage("{\"jsonrpc\":\"2.0\",\"method\":\"textDocument/didOpen\","
                    + "\"params\":{\"textDocument\":{\"uri\":\"" + uri + "\","
                    + "\"languageId\":\"pure\",\"version\":" + (version++) + ","
                    + "\"text\":\"" + Json.escape(text) + "\"}}}");
        }

        Map<String, List<Diagnostic>> openAndGetDiagnostics(String uri, String text) {
            var responses = server.handleMessage(
                    "{\"jsonrpc\":\"2.0\",\"method\":\"textDocument/didOpen\","
                    + "\"params\":{\"textDocument\":{\"uri\":\"" + uri + "\","
                    + "\"languageId\":\"pure\",\"version\":" + (version++) + ","
                    + "\"text\":\"" + Json.escape(text) + "\"}}}");
            return parseDiagnosticsMap(responses);
        }

        Map<String, List<Diagnostic>> changeAndGetDiagnostics(String uri, String text) {
            var responses = server.handleMessage(
                    "{\"jsonrpc\":\"2.0\",\"method\":\"textDocument/didChange\","
                    + "\"params\":{\"textDocument\":{\"uri\":\"" + uri + "\","
                    + "\"version\":" + (version++) + "},"
                    + "\"contentChanges\":[{\"text\":\"" + Json.escape(text) + "\"}]}}");
            return parseDiagnosticsMap(responses);
        }

        Map<String, List<Diagnostic>> closeAndGetDiagnostics(String uri) {
            var responses = server.handleMessage(
                    "{\"jsonrpc\":\"2.0\",\"method\":\"textDocument/didClose\","
                    + "\"params\":{\"textDocument\":{\"uri\":\"" + uri + "\"}}}");
            return parseDiagnosticsMap(responses);
        }

        @SuppressWarnings("unchecked")
        private Map<String, List<Diagnostic>> parseDiagnosticsMap(List<String> responses) {
            Map<String, List<Diagnostic>> result = new LinkedHashMap<>();
            for (String json : responses) {
                var notification = parseAsMap(json);
                assertEquals("textDocument/publishDiagnostics", notification.get("method"),
                        "Expected publishDiagnostics notification");
                var params = (Map<String, Object>) notification.get("params");
                String uri = (String) params.get("uri");
                assertNotNull(uri, "Diagnostic notification must have URI");
                List<Object> rawDiags = (List<Object>) params.get("diagnostics");
                List<Diagnostic> diags = new ArrayList<>();
                if (rawDiags != null) {
                    for (Object rd : rawDiags) {
                        var d = (Map<String, Object>) rd;
                        var range = (Map<String, Object>) d.get("range");
                        var start = (Map<String, Object>) range.get("start");
                        diags.add(new Diagnostic(
                                ((Number) start.get("line")).intValue(),
                                ((Number) start.get("character")).intValue(),
                                ((Number) d.get("severity")).intValue(),
                                (String) d.get("source"),
                                (String) d.get("message")));
                    }
                }
                result.put(uri, diags);
            }
            return result;
        }

        @SuppressWarnings("unchecked")
        private static Map<String, Object> obj(Map<String, Object> parent, String key) {
            return (Map<String, Object>) parent.get(key);
        }

    }

    /**
     * Test-only bridge: parse JSON and convert the Node tree back to Java-native
     * Map/List/String/Number/Boolean/null so the existing Map-based assertions
     * keep working after the LegendHttpJson → util.Json migration. Production
     * code should use the typed Node accessors directly (getString, getInt, …).
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> parseAsMap(String json) {
        return (Map<String, Object>) nodeToJava(Json.parse(json));
    }

    private static Object nodeToJava(Json.Node n) {
        if (n == null || n instanceof Json.Null) return null;
        if (n instanceof Json.Str s) return s.value();
        if (n instanceof Json.Num num) {
            return num.isInteger() ? (Object) num.longValue() : (Object) num.doubleValue();
        }
        if (n instanceof Json.Bool b) return b.value();
        if (n instanceof Json.Obj o) {
            Map<String, Object> m = new LinkedHashMap<>();
            for (var e : o.fields().entrySet()) m.put(e.getKey(), nodeToJava(e.getValue()));
            return m;
        }
        if (n instanceof Json.Arr a) {
            List<Object> l = new ArrayList<>(a.items().size());
            for (var item : a.items()) l.add(nodeToJava(item));
            return l;
        }
        throw new IllegalStateException("Unknown Node kind: " + n.getClass());
    }
}
