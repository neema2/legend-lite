package org.finos.legend.engine.server;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for PureLspServer - LSP protocol implementation.
 */
class PureLspServerTest {

    private PureLspServer server;

    @BeforeEach
    void setUp() {
        server = new PureLspServer();
    }

    // ========== Initialize Tests ==========

    @Test
    void testInitialize() {
        String request = """
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "processId": 12345,
                        "rootUri": "file:///workspace"
                    }
                }
                """;

        String response = server.handleMessage(request);
        assertNotNull(response);

        Map<String, Object> result = LegendHttpJson.parseObject(response);
        assertEquals(1, ((Number) result.get("id")).intValue());
        assertNotNull(result.get("result"));

        Map<String, Object> resultObj = LegendHttpJson.getObject(result, "result");
        assertNotNull(resultObj.get("capabilities"));

        Map<String, Object> serverInfo = LegendHttpJson.getObject(resultObj, "serverInfo");
        assertEquals("legend-lite-lsp", serverInfo.get("name"));
    }

    @Test
    void testInitializedNotification() {
        String notification = """
                {
                    "jsonrpc": "2.0",
                    "method": "initialized",
                    "params": {}
                }
                """;

        // Notifications return null (no response)
        String response = server.handleMessage(notification);
        assertNull(response);
    }

    // ========== Document Sync Tests ==========

    @Test
    void testDidOpenValidCode() {
        String didOpen = """
                {
                    "jsonrpc": "2.0",
                    "method": "textDocument/didOpen",
                    "params": {
                        "textDocument": {
                            "uri": "file:///test.pure",
                            "languageId": "pure",
                            "version": 1,
                            "text": "Person.all()"
                        }
                    }
                }
                """;

        String response = server.handleMessage(didOpen);

        // Should return diagnostics notification
        assertNotNull(response);
        Map<String, Object> result = LegendHttpJson.parseObject(response);
        assertEquals("textDocument/publishDiagnostics", result.get("method"));
    }

    @Test
    void testDidOpenInvalidCode() {
        String didOpen = """
                {
                    "jsonrpc": "2.0",
                    "method": "textDocument/didOpen",
                    "params": {
                        "textDocument": {
                            "uri": "file:///test.pure",
                            "languageId": "pure",
                            "version": 1,
                            "text": "Class Person { name String[1]; }"
                        }
                    }
                }
                """;

        String response = server.handleMessage(didOpen);
        assertNotNull(response);

        Map<String, Object> result = LegendHttpJson.parseObject(response);
        assertEquals("textDocument/publishDiagnostics", result.get("method"));

        Map<String, Object> params = LegendHttpJson.getObject(result, "params");
        List<Object> diagnostics = LegendHttpJson.getList(params, "diagnostics");

        // Should have at least one error
        assertFalse(diagnostics.isEmpty());
    }

    @Test
    void testDidChange() {
        // First open a document
        String didOpen = """
                {
                    "jsonrpc": "2.0",
                    "method": "textDocument/didOpen",
                    "params": {
                        "textDocument": {
                            "uri": "file:///test.pure",
                            "languageId": "pure",
                            "version": 1,
                            "text": "Person.all()"
                        }
                    }
                }
                """;
        server.handleMessage(didOpen);

        // Then change it
        String didChange = """
                {
                    "jsonrpc": "2.0",
                    "method": "textDocument/didChange",
                    "params": {
                        "textDocument": {
                            "uri": "file:///test.pure",
                            "version": 2
                        },
                        "contentChanges": [
                            {"text": "Person.all()->filter(p | $p.name == 'test')"}
                        ]
                    }
                }
                """;

        String response = server.handleMessage(didChange);
        assertNotNull(response);

        Map<String, Object> result = LegendHttpJson.parseObject(response);
        assertEquals("textDocument/publishDiagnostics", result.get("method"));
    }

    @Test
    void testDidClose() {
        // First open a document
        String didOpen = """
                {
                    "jsonrpc": "2.0",
                    "method": "textDocument/didOpen",
                    "params": {
                        "textDocument": {
                            "uri": "file:///test.pure",
                            "languageId": "pure",
                            "version": 1,
                            "text": "Person.all()"
                        }
                    }
                }
                """;
        server.handleMessage(didOpen);
        assertTrue(server.hasDocument("file:///test.pure"));

        // Then close it
        String didClose = """
                {
                    "jsonrpc": "2.0",
                    "method": "textDocument/didClose",
                    "params": {
                        "textDocument": {
                            "uri": "file:///test.pure"
                        }
                    }
                }
                """;

        String response = server.handleMessage(didClose);
        assertNotNull(response); // Clear diagnostics notification
        assertFalse(server.hasDocument("file:///test.pure"));
    }

    // ========== Error Handling Tests ==========

    @Test
    void testUnknownMethod() {
        String request = """
                {
                    "jsonrpc": "2.0",
                    "id": 99,
                    "method": "unknownMethod",
                    "params": {}
                }
                """;

        String response = server.handleMessage(request);
        assertNotNull(response);

        Map<String, Object> result = LegendHttpJson.parseObject(response);
        assertEquals(99, ((Number) result.get("id")).intValue());

        Map<String, Object> error = LegendHttpJson.getObject(result, "error");
        assertNotNull(error);
        assertEquals(-32601, ((Number) error.get("code")).intValue()); // Method not found
    }

    @Test
    void testShutdown() {
        String request = """
                {
                    "jsonrpc": "2.0",
                    "id": 100,
                    "method": "shutdown"
                }
                """;

        String response = server.handleMessage(request);
        assertNotNull(response);

        Map<String, Object> result = LegendHttpJson.parseObject(response);
        assertEquals(100, ((Number) result.get("id")).intValue());
        assertNull(result.get("error"));
    }
}
