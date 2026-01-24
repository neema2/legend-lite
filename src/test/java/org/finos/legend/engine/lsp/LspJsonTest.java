package org.finos.legend.engine.lsp;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for LspJson - zero-dependency JSON parser.
 */
class LspJsonTest {

    // ========== Parsing Tests ==========

    @Test
    void testParseEmptyObject() {
        Map<String, Object> result = LspJson.parseObject("{}");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void testParseSimpleObject() {
        Map<String, Object> result = LspJson.parseObject("{\"name\":\"test\",\"value\":42}");
        assertEquals("test", result.get("name"));
        assertEquals(42, ((Number) result.get("value")).intValue());
    }

    @Test
    void testParseNestedObject() {
        Map<String, Object> result = LspJson.parseObject(
                "{\"outer\":{\"inner\":\"value\"}}");

        Map<String, Object> outer = LspJson.getObject(result, "outer");
        assertNotNull(outer);
        assertEquals("value", outer.get("inner"));
    }

    @Test
    void testParseArray() {
        String json = "{\"items\":[1,2,3]}";
        Map<String, Object> result = LspJson.parseObject(json);

        List<Object> items = LspJson.getList(result, "items");
        assertNotNull(items);
        assertEquals(3, items.size());
        assertEquals(1, ((Number) items.get(0)).intValue());
        assertEquals(2, ((Number) items.get(1)).intValue());
        assertEquals(3, ((Number) items.get(2)).intValue());
    }

    @Test
    void testParseStringWithEscapes() {
        String json = "{\"text\":\"hello\\nworld\\t!\"}";
        Map<String, Object> result = LspJson.parseObject(json);
        assertEquals("hello\nworld\t!", result.get("text"));
    }

    @Test
    void testParseBooleans() {
        Map<String, Object> result = LspJson.parseObject("{\"yes\":true,\"no\":false}");
        assertEquals(true, result.get("yes"));
        assertEquals(false, result.get("no"));
    }

    @Test
    void testParseNull() {
        Map<String, Object> result = LspJson.parseObject("{\"nothing\":null}");
        assertNull(result.get("nothing"));
    }

    @Test
    void testParseFloats() {
        Map<String, Object> result = LspJson.parseObject("{\"pi\":3.14159}");
        assertEquals(3.14159, (Double) result.get("pi"), 0.00001);
    }

    // ========== Serialization Tests ==========

    @Test
    void testSerializeSimpleObject() {
        Map<String, Object> data = Map.of("name", "test", "value", 42);
        String json = LspJson.toJson(data);

        assertTrue(json.contains("\"name\":\"test\""));
        assertTrue(json.contains("\"value\":42"));
    }

    @Test
    void testSerializeStringEscaping() {
        Map<String, Object> data = Map.of("text", "line1\nline2\ttab");
        String json = LspJson.toJson(data);

        assertTrue(json.contains("\\n"));
        assertTrue(json.contains("\\t"));
    }

    @Test
    void testSerializeNull() {
        Map<String, Object> data = new java.util.HashMap<>();
        data.put("nothing", null);
        String json = LspJson.toJson(data);

        assertTrue(json.contains("\"nothing\":null"));
    }

    @Test
    void testRoundTrip() {
        String original = "{\"id\":1,\"name\":\"test\",\"active\":true}";
        Map<String, Object> parsed = LspJson.parseObject(original);
        String serialized = LspJson.toJson(parsed);
        Map<String, Object> reparsed = LspJson.parseObject(serialized);

        assertEquals(parsed.get("id"), reparsed.get("id"));
        assertEquals(parsed.get("name"), reparsed.get("name"));
        assertEquals(parsed.get("active"), reparsed.get("active"));
    }

    // ========== LSP-Specific JSON-RPC Tests ==========

    @Test
    void testParseJsonRpcRequest() {
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

        Map<String, Object> result = LspJson.parseObject(request);

        assertEquals("2.0", result.get("jsonrpc"));
        assertEquals(1, ((Number) result.get("id")).intValue());
        assertEquals("initialize", result.get("method"));

        Map<String, Object> params = LspJson.getObject(result, "params");
        assertNotNull(params);
        assertEquals(12345, ((Number) params.get("processId")).intValue());
        assertEquals("file:///workspace", params.get("rootUri"));
    }

    @Test
    void testParseTextDocumentDidChange() {
        String notification = """
                {
                    "jsonrpc": "2.0",
                    "method": "textDocument/didChange",
                    "params": {
                        "textDocument": {
                            "uri": "file:///test.pure",
                            "version": 2
                        },
                        "contentChanges": [
                            {"text": "Class Person { name: String[1]; }"}
                        ]
                    }
                }
                """;

        Map<String, Object> result = LspJson.parseObject(notification);

        assertEquals("textDocument/didChange", result.get("method"));

        Map<String, Object> params = LspJson.getObject(result, "params");
        Map<String, Object> textDocument = LspJson.getObject(params, "textDocument");
        assertEquals("file:///test.pure", textDocument.get("uri"));

        List<Object> changes = LspJson.getList(params, "contentChanges");
        assertEquals(1, changes.size());
    }
}
