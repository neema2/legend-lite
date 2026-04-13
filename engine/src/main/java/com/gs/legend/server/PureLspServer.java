package com.gs.legend.server;

import com.gs.legend.model.PureModelBuilder;

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

    private final Map<String, String> documents = new HashMap<>();

    public List<String> handleMessage(String messageJson) {
        Map<String, Object> message = LegendHttpJson.parseObject(messageJson);
        if (message == null) {
            return List.of(errorResponse(null, -32700, "Parse error"));
        }

        String method = LegendHttpJson.getString(message, "method");
        Object id = message.get("id");
        Map<String, Object> params = LegendHttpJson.getObject(message, "params");

        if (method == null) {
            return List.of(errorResponse(id, -32600, "Invalid Request: missing method"));
        }

        try {
            return switch (method) {
                case "initialize" -> List.of(handleInitialize(id, params));
                case "initialized" -> List.of();
                case "shutdown" -> List.of(handleShutdown(id));
                case "exit" -> List.of();
                case "textDocument/didOpen" -> handleDidOpen(params);
                case "textDocument/didChange" -> handleDidChange(params);
                case "textDocument/didClose" -> handleDidClose(params);
                default -> {
                    if (id != null) {
                        yield List.of(errorResponse(id, -32601, "Method not found: " + method));
                    }
                    yield List.of();
                }
            };
        } catch (Exception e) {
            return List.of(errorResponse(id, -32603, "Internal error: " + e.getMessage()));
        }
    }

    private String handleInitialize(Object id, Map<String, Object> params) {
        Map<String, Object> capabilities = new LinkedHashMap<>();
        Map<String, Object> textDocumentSync = new LinkedHashMap<>();
        textDocumentSync.put("openClose", true);
        textDocumentSync.put("change", 1);
        capabilities.put("textDocumentSync", textDocumentSync);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("capabilities", capabilities);

        Map<String, Object> serverInfo = new LinkedHashMap<>();
        serverInfo.put("name", "legend-lite-lsp");
        serverInfo.put("version", "1.0.0");
        result.put("serverInfo", serverInfo);

        return successResponse(id, result);
    }

    private String handleShutdown(Object id) {
        return successResponse(id, null);
    }

    private List<String> handleDidOpen(Map<String, Object> params) {
        Map<String, Object> textDocument = LegendHttpJson.getObject(params, "textDocument");
        if (textDocument == null) return List.of();

        String uri = LegendHttpJson.getString(textDocument, "uri");
        String text = LegendHttpJson.getString(textDocument, "text");

        if (uri != null && text != null) {
            documents.put(uri, text);
            return rebuildAndPublishAll();
        }
        return List.of();
    }

    private List<String> handleDidChange(Map<String, Object> params) {
        Map<String, Object> textDocument = LegendHttpJson.getObject(params, "textDocument");
        if (textDocument == null) return List.of();

        String uri = LegendHttpJson.getString(textDocument, "uri");
        List<Object> contentChanges = LegendHttpJson.getList(params, "contentChanges");

        if (uri != null && contentChanges != null && !contentChanges.isEmpty()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> change = (Map<String, Object>) contentChanges.get(0);
            String text = LegendHttpJson.getString(change, "text");
            if (text != null) {
                documents.put(uri, text);
                return rebuildAndPublishAll();
            }
        }
        return List.of();
    }

    private List<String> handleDidClose(Map<String, Object> params) {
        Map<String, Object> textDocument = LegendHttpJson.getObject(params, "textDocument");
        if (textDocument == null) return List.of();

        String uri = LegendHttpJson.getString(textDocument, "uri");
        if (uri != null) {
            documents.remove(uri);
            List<String> notifications = new ArrayList<>(rebuildAndPublishAll());
            notifications.add(publishDiagnosticsEmpty(uri));
            return notifications;
        }
        return List.of();
    }

    /**
     * Rebuild model from ALL open documents and publish diagnostics for each.
     * ParseCache ensures only changed files are re-parsed.
     */
    private List<String> rebuildAndPublishAll() {
        if (documents.isEmpty()) return List.of();

        PureModelBuilder model = new PureModelBuilder();
        Map<String, Exception> fileErrors = new LinkedHashMap<>();

        for (var entry : documents.entrySet()) {
            try {
                model.addSource(entry.getValue());
            } catch (Exception e) {
                fileErrors.put(entry.getKey(), e);
            }
        }

        List<String> notifications = new ArrayList<>();
        for (var entry : documents.entrySet()) {
            String uri = entry.getKey();
            String text = entry.getValue();
            Exception error = fileErrors.get(uri);

            if (error != null) {
                notifications.add(createDiagnosticsForError(uri, text, error));
            } else {
                notifications.add(createDiagnosticsNotification(uri, List.of()));
            }
        }
        return notifications;
    }

    /**
     * Create a diagnostics notification for a single error in a file.
     */
    private String createDiagnosticsForError(String uri, String text, Exception e) {
        String message = e.getMessage();
        int line = 0;
        int character = 0;

        if (message != null) {
            int lineIdx = message.indexOf("line ");
            if (lineIdx >= 0) {
                try {
                    int start = lineIdx + 5;
                    int end = start;
                    while (end < message.length() && Character.isDigit(message.charAt(end))) end++;
                    if (end > start) {
                        line = Integer.parseInt(message.substring(start, end)) - 1;
                        if (end < message.length() && message.charAt(end) == ':') {
                            int colStart = end + 1;
                            int colEnd = colStart;
                            while (colEnd < message.length() && Character.isDigit(message.charAt(colEnd))) colEnd++;
                            if (colEnd > colStart) character = Integer.parseInt(message.substring(colStart, colEnd));
                        }
                    }
                } catch (NumberFormatException ignored) { }
            }

            if (line == 0) {
                int quoteStart = message.indexOf("'");
                int quoteEnd = message.indexOf("'", quoteStart + 1);
                if (quoteStart >= 0 && quoteEnd > quoteStart) {
                    String searchTerm = message.substring(quoteStart + 1, quoteEnd).split("\\s+")[0];
                    int[] location = findInSource(text, searchTerm);
                    if (location[0] > 0) {
                        line = location[0] - 1;
                        character = location[1];
                    }
                }
            }
        }

        Map<String, Object> diagnostic = new LinkedHashMap<>();
        Map<String, Object> range = new LinkedHashMap<>();
        Map<String, Object> startPos = new LinkedHashMap<>();
        startPos.put("line", line);
        startPos.put("character", character);
        Map<String, Object> endPos = new LinkedHashMap<>();
        endPos.put("line", line);
        endPos.put("character", character + 20);
        range.put("start", startPos);
        range.put("end", endPos);
        diagnostic.put("range", range);
        diagnostic.put("severity", 1);
        diagnostic.put("source", "legend-lite");
        diagnostic.put("message", message != null ? message : "Compilation error");

        return createDiagnosticsNotification(uri, List.of(diagnostic));
    }

    private int[] findInSource(String source, String searchTerm) {
        String[] lines = source.split("\n");
        for (int i = 0; i < lines.length; i++) {
            int col = lines[i].indexOf(searchTerm);
            if (col >= 0) return new int[]{ i + 1, col };
        }
        return new int[]{ 0, 0 };
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

    public String getDocument(String uri) { return documents.get(uri); }
    public boolean hasDocument(String uri) { return documents.containsKey(uri); }
}
