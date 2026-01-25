package org.finos.legend.engine.server;

import org.finos.legend.engine.store.MappingRegistry;
import org.finos.legend.pure.dsl.PureCompiler;

import java.util.*;

/**
 * Minimal LSP (Language Server Protocol) implementation for Pure language.
 * 
 * Zero dependencies - pure Java. GraalVM native-image compatible.
 * 
 * Implements only the subset needed for MVP:
 * - initialize / initialized
 * - textDocument/didOpen
 * - textDocument/didChange
 * - textDocument/didClose
 * 
 * Sends:
 * - textDocument/publishDiagnostics
 */
public class PureLspServer {

    // Document storage: URI -> content
    private final Map<String, String> documents = new HashMap<>();

    // Reusable compiler infrastructure (avoid re-creating per diagnostics call)
    private final MappingRegistry mappingRegistry = new MappingRegistry();
    private final PureCompiler compiler = new PureCompiler(mappingRegistry);

    /**
     * Handle an incoming JSON-RPC message and return a response (if any).
     * 
     * @param messageJson The JSON-RPC message
     * @return Response JSON, or null for notifications
     */
    public String handleMessage(String messageJson) {
        Map<String, Object> message = LegendHttpJson.parseObject(messageJson);
        if (message == null) {
            return errorResponse(null, -32700, "Parse error");
        }

        String method = LegendHttpJson.getString(message, "method");
        Object id = message.get("id");
        Map<String, Object> params = LegendHttpJson.getObject(message, "params");

        if (method == null) {
            return errorResponse(id, -32600, "Invalid Request: missing method");
        }

        try {
            return switch (method) {
                case "initialize" -> handleInitialize(id, params);
                case "initialized" -> null; // notification, no response
                case "shutdown" -> handleShutdown(id);
                case "exit" -> null; // notification
                case "textDocument/didOpen" -> handleDidOpen(params);
                case "textDocument/didChange" -> handleDidChange(params);
                case "textDocument/didClose" -> handleDidClose(params);
                default -> {
                    // Unknown method - return method not found for requests
                    if (id != null) {
                        yield errorResponse(id, -32601, "Method not found: " + method);
                    }
                    yield null;
                }
            };
        } catch (Exception e) {
            return errorResponse(id, -32603, "Internal error: " + e.getMessage());
        }
    }

    /**
     * Handle initialize request - return server capabilities.
     */
    private String handleInitialize(Object id, Map<String, Object> params) {
        Map<String, Object> capabilities = new LinkedHashMap<>();

        // Text document sync: full sync (send full document on change)
        Map<String, Object> textDocumentSync = new LinkedHashMap<>();
        textDocumentSync.put("openClose", true);
        textDocumentSync.put("change", 1); // 1 = Full sync
        capabilities.put("textDocumentSync", textDocumentSync);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("capabilities", capabilities);

        Map<String, Object> serverInfo = new LinkedHashMap<>();
        serverInfo.put("name", "legend-lite-lsp");
        serverInfo.put("version", "1.0.0");
        result.put("serverInfo", serverInfo);

        return successResponse(id, result);
    }

    /**
     * Handle shutdown request.
     */
    private String handleShutdown(Object id) {
        return successResponse(id, null);
    }

    /**
     * Handle textDocument/didOpen notification.
     */
    private String handleDidOpen(Map<String, Object> params) {
        Map<String, Object> textDocument = LegendHttpJson.getObject(params, "textDocument");
        if (textDocument == null)
            return null;

        String uri = LegendHttpJson.getString(textDocument, "uri");
        String text = LegendHttpJson.getString(textDocument, "text");

        if (uri != null && text != null) {
            documents.put(uri, text);
            // Return diagnostics as a notification
            return publishDiagnostics(uri, text);
        }
        return null;
    }

    /**
     * Handle textDocument/didChange notification.
     */
    private String handleDidChange(Map<String, Object> params) {
        Map<String, Object> textDocument = LegendHttpJson.getObject(params, "textDocument");
        if (textDocument == null)
            return null;

        String uri = LegendHttpJson.getString(textDocument, "uri");
        List<Object> contentChanges = LegendHttpJson.getList(params, "contentChanges");

        if (uri != null && contentChanges != null && !contentChanges.isEmpty()) {
            // With full sync, first change contains full text
            @SuppressWarnings("unchecked")
            Map<String, Object> change = (Map<String, Object>) contentChanges.get(0);
            String text = LegendHttpJson.getString(change, "text");

            if (text != null) {
                documents.put(uri, text);
                return publishDiagnostics(uri, text);
            }
        }
        return null;
    }

    /**
     * Handle textDocument/didClose notification.
     */
    private String handleDidClose(Map<String, Object> params) {
        Map<String, Object> textDocument = LegendHttpJson.getObject(params, "textDocument");
        if (textDocument == null)
            return null;

        String uri = LegendHttpJson.getString(textDocument, "uri");
        if (uri != null) {
            documents.remove(uri);
            // Clear diagnostics
            return publishDiagnosticsEmpty(uri);
        }
        return null;
    }

    /**
     * Compile the document and publish diagnostics.
     */
    private String publishDiagnostics(String uri, String text) {
        List<Map<String, Object>> diagnostics = new ArrayList<>();

        try {
            // Validate the Pure model definitions (not query execution)
            compiler.validate(text);
            // Success - no diagnostics
        } catch (Exception e) {
            // Parse the error to extract location if possible
            String message = e.getMessage();
            int line = 0;
            int character = 0;

            // Try to extract line number from common error format: "line X:Y"
            if (message != null) {
                int lineIdx = message.indexOf("line ");
                if (lineIdx >= 0) {
                    try {
                        int start = lineIdx + 5;
                        int end = start;
                        while (end < message.length() && Character.isDigit(message.charAt(end))) {
                            end++;
                        }
                        if (end > start) {
                            line = Integer.parseInt(message.substring(start, end)) - 1; // LSP is 0-indexed
                            // Try to get column
                            if (end < message.length() && message.charAt(end) == ':') {
                                int colStart = end + 1;
                                int colEnd = colStart;
                                while (colEnd < message.length() && Character.isDigit(message.charAt(colEnd))) {
                                    colEnd++;
                                }
                                if (colEnd > colStart) {
                                    character = Integer.parseInt(message.substring(colStart, colEnd));
                                }
                            }
                        }
                    } catch (NumberFormatException ignored) {
                    }
                }
            }

            Map<String, Object> diagnostic = new LinkedHashMap<>();

            // Range
            Map<String, Object> range = new LinkedHashMap<>();
            Map<String, Object> startPos = new LinkedHashMap<>();
            startPos.put("line", line);
            startPos.put("character", character);
            Map<String, Object> endPos = new LinkedHashMap<>();
            endPos.put("line", line);
            endPos.put("character", character + 10); // Approximate
            range.put("start", startPos);
            range.put("end", endPos);

            diagnostic.put("range", range);
            diagnostic.put("severity", 1); // 1 = Error
            diagnostic.put("source", "legend-lite");
            diagnostic.put("message", message != null ? message : "Compilation error");

            diagnostics.add(diagnostic);
        }

        return createDiagnosticsNotification(uri, diagnostics);
    }

    private String publishDiagnosticsEmpty(String uri) {
        return createDiagnosticsNotification(uri, List.of());
    }

    private String createDiagnosticsNotification(String uri, List<Map<String, Object>> diagnostics) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("uri", uri);
        params.put("diagnostics", diagnostics);

        Map<String, Object> notification = new LinkedHashMap<>();
        notification.put("jsonrpc", "2.0");
        notification.put("method", "textDocument/publishDiagnostics");
        notification.put("params", params);

        return LegendHttpJson.toJson(notification);
    }

    // ========== JSON-RPC Response Helpers ==========

    private String successResponse(Object id, Object result) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("jsonrpc", "2.0");
        response.put("id", id);
        response.put("result", result);
        return LegendHttpJson.toJson(response);
    }

    private String errorResponse(Object id, int code, String message) {
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("code", code);
        error.put("message", message);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("jsonrpc", "2.0");
        response.put("id", id);
        response.put("error", error);
        return LegendHttpJson.toJson(response);
    }

    // ========== Document Access ==========

    /**
     * Get the current content of a document.
     */
    public String getDocument(String uri) {
        return documents.get(uri);
    }

    /**
     * Check if a document is open.
     */
    public boolean hasDocument(String uri) {
        return documents.containsKey(uri);
    }

    /**
     * Get the shared compiler instance.
     * Used by LspHttpAdapter for execute operations.
     */
    public PureCompiler getCompiler() {
        return compiler;
    }

    /**
     * Execute Pure code and return result as JSON.
     * 
     * @param code The Pure code to compile and execute
     * @return JSON string with sql and plan
     * @throws Exception if compilation fails
     */
    public String executeCode(String code) throws Exception {
        // Compile using reusable compiler
        org.finos.legend.engine.plan.RelationNode plan = compiler.compile(code);

        // Generate SQL
        org.finos.legend.engine.transpiler.SQLGenerator generator = new org.finos.legend.engine.transpiler.SQLGenerator(
                org.finos.legend.engine.transpiler.DuckDBDialect.INSTANCE);
        String sql = generator.generate(plan);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("sql", sql);
        result.put("plan", plan.toString());

        return LegendHttpJson.toJson(result);
    }
}
