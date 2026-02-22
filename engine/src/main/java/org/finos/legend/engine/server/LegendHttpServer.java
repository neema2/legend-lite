package org.finos.legend.engine.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.finos.legend.engine.execution.BufferedResult;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Legend Studio Lite HTTP Server.
 * 
 * Uses Java's built-in com.sun.net.httpserver - no external dependencies.
 * 
 * Endpoints:
 * - POST /lsp - Handle LSP JSON-RPC messages (diagnostics, completions, etc.)
 * - POST /engine/execute - Execute Pure query (compile + generate SQL + run)
 * - POST /engine/sql - Execute raw SQL against Connection from Runtime
 * - GET /health - Health check
 */
public class LegendHttpServer {

    private final HttpServer server;
    private final PureLspServer lspServer;
    private final QueryService queryService = new QueryService();

    // Pattern to find the Runtime definition
    private static final Pattern RUNTIME_PATTERN = Pattern.compile(
            "Runtime\\s+([\\w:]+)\\s*\\{",
            Pattern.MULTILINE);

    public LegendHttpServer(int port) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.lspServer = new PureLspServer();
        setupRoutes();
    }

    private void setupRoutes() {
        // LSP Protocol - diagnostics, completions, etc.
        server.createContext("/lsp", new LspHandler());

        // Engine - query and SQL execution
        server.createContext("/engine/execute", new ExecuteHandler());
        server.createContext("/engine/sql", new ExecuteSqlHandler());
        server.createContext("/engine/diagram", new DiagramHandler());

        // Health check
        server.createContext("/health", exchange -> {
            addCorsHeaders(exchange);
            sendResponse(exchange, 200, "{\"status\":\"ok\"}");
        });

        // CORS preflight for all routes
        server.createContext("/", exchange -> {
            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                addCorsHeaders(exchange);
                exchange.sendResponseHeaders(204, -1);
            } else {
                exchange.sendResponseHeaders(404, -1);
            }
            exchange.close();
        });
    }

    private class LspHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);
            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
                return;
            }
            if (!"POST".equals(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
                return;
            }
            try {
                String body = readBody(exchange);
                String response = lspServer.handleMessage(body);
                if (response != null) {
                    sendResponse(exchange, 200, response);
                } else {
                    sendResponse(exchange, 204, "");
                }
            } catch (Exception e) {
                sendResponse(exchange, 500, "{\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
            }
        }
    }

    /**
     * Execute Pure code from the frontend.
     * 
     * The frontend sends the COMPLETE Pure source (model + mapping + connection +
     * runtime + query).
     * This handler separates the model (definitions) from the query (expression)
     * and executes.
     */
    private class ExecuteHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);
            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
                return;
            }
            if (!"POST".equals(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
                return;
            }

            try {
                String body = readBody(exchange);
                Map<String, Object> request = LegendHttpJson.parseObject(body);
                String fullSource = LegendHttpJson.getString(request, "code");

                if (fullSource == null || fullSource.isBlank()) {
                    sendResponse(exchange, 400, "{\"error\":\"Missing 'code' field\"}");
                    return;
                }

                // Extract the runtime name from the source
                String runtimeName = extractRuntimeName(fullSource);
                if (runtimeName == null) {
                    sendResponse(exchange, 400, "{\"error\":\"No Runtime definition found in source\"}");
                    return;
                }

                // Separate the model (definitions) from the query (expression at the end)
                String[] parts = separateModelAndQuery(fullSource);
                String modelSource = parts[0];
                String query = parts[1];

                if (query == null || query.isBlank()) {
                    sendResponse(exchange, 400, "{\"error\":\"No query expression found after Runtime definition\"}");
                    return;
                }

                System.out.println("Model source length: " + modelSource.length());
                System.out.println("Query: " + query);
                System.out.println("Runtime: " + runtimeName);

                // Execute using QueryService - uses Connection from Runtime
                BufferedResult result = queryService.execute(
                        modelSource,
                        query,
                        runtimeName);

                Map<String, Object> response = new LinkedHashMap<>();
                response.put("success", true);
                response.put("data", result.toJsonArray());
                response.put("columns", result.columns().stream().map(c -> c.name()).toList());
                response.put("rowCount", result.rows().size());

                sendResponse(exchange, 200, LegendHttpJson.toJson(response));

            } catch (Exception e) {
                e.printStackTrace();
                Map<String, Object> response = new LinkedHashMap<>();
                response.put("success", false);
                response.put("error", e.getMessage());
                sendResponse(exchange, 200, LegendHttpJson.toJson(response));
            }
        }
    }

    /**
     * Execute raw SQL against the Connection from the user's Runtime.
     * 
     * Request format:
     * {
     * "code": "full Pure model with Runtime definition",
     * "sql": "CREATE TABLE T_PERSON (...) or INSERT INTO ... or SELECT ...",
     * "runtime": "test::TestRuntime"
     * }
     */
    private class ExecuteSqlHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);
            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
                return;
            }
            if (!"POST".equals(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
                return;
            }

            try {
                String body = readBody(exchange);
                System.out.println("Raw body length: " + body.length());
                Map<String, Object> request = LegendHttpJson.parseObject(body);
                String pureSource = LegendHttpJson.getString(request, "code");
                String sql = LegendHttpJson.getString(request, "sql");
                String runtimeName = LegendHttpJson.getString(request, "runtime");

                if (pureSource == null || pureSource.isBlank()) {
                    sendResponse(exchange, 400, "{\"error\":\"Missing 'code' field\"}");
                    return;
                }
                if (sql == null || sql.isBlank()) {
                    sendResponse(exchange, 400, "{\"error\":\"Missing 'sql' field\"}");
                    return;
                }
                if (runtimeName == null || runtimeName.isBlank()) {
                    // Try to extract from source
                    runtimeName = extractRuntimeName(pureSource);
                    if (runtimeName == null) {
                        sendResponse(exchange, 400,
                                "{\"error\":\"Missing 'runtime' field and no Runtime found in source\"}");
                        return;
                    }
                }

                // Execute using QueryService
                BufferedResult result = queryService.executeSql(pureSource, sql, runtimeName);

                Map<String, Object> response = new LinkedHashMap<>();
                response.put("success", true);
                if (result.columns().isEmpty()) {
                    // DDL/DML statement - no result set
                    response.put("message", "SQL executed successfully");
                } else {
                    // SELECT - return results
                    response.put("data", result.toJsonArray());
                    response.put("columns", result.columns().stream().map(c -> c.name()).toList());
                    response.put("rowCount", result.rows().size());
                }

                sendResponse(exchange, 200, LegendHttpJson.toJson(response));

            } catch (Exception e) {
                e.printStackTrace();
                Map<String, Object> response = new LinkedHashMap<>();
                response.put("success", false);
                response.put("error", e.getMessage());
                sendResponse(exchange, 200, LegendHttpJson.toJson(response));
            }
        }
    }


    /**
     * Extract the Runtime name from the Pure source.
     */
    private String extractRuntimeName(String source) {
        Matcher matcher = RUNTIME_PATTERN.matcher(source);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    /**
     * Separate the model definitions from the query expression.
     * 
     * The model includes all definitions (Class, Association, Database, Mapping,
     * Connection, Runtime).
     * The query is everything after the Runtime definition's closing brace
     * (non-comment code).
     * 
     * @return String[2] where [0] = model source (definitions only), [1] = query
     *         expression
     */
    private String[] separateModelAndQuery(String source) {
        // Find the Runtime block and locate its end
        Matcher matcher = RUNTIME_PATTERN.matcher(source);
        if (!matcher.find()) {
            return new String[] { source, "" };
        }

        int braceCount = 0;
        int endOfRuntime = -1;

        for (int i = matcher.end() - 1; i < source.length(); i++) {
            char c = source.charAt(i);
            if (c == '{')
                braceCount++;
            else if (c == '}') {
                braceCount--;
                if (braceCount == 0) {
                    endOfRuntime = i + 1;
                    break;
                }
            }
        }

        if (endOfRuntime == -1) {
            return new String[] { source, "" };
        }

        // Model = everything up to and including the Runtime closing brace
        String modelSource = source.substring(0, endOfRuntime);

        // Query = everything after, skipping comments
        String afterRuntime = source.substring(endOfRuntime).trim();
        StringBuilder query = new StringBuilder();
        for (String line : afterRuntime.split("\n")) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty() && !trimmed.startsWith("//")) {
                query.append(line).append("\n");
            }
        }

        return new String[] { modelSource, query.toString().trim() };
    }

    public static void addCorsHeaders(HttpExchange exchange) {
        var headers = exchange.getResponseHeaders();
        headers.add("Access-Control-Allow-Origin", "*");
        headers.add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        headers.add("Access-Control-Allow-Headers", "Content-Type");
    }

    public static String readBody(HttpExchange exchange) throws IOException {
        try (InputStream is = exchange.getRequestBody();
                BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
            return sb.toString();
        }
    }

    public static void sendResponse(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private String escapeJson(String s) {
        if (s == null)
            return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }

    /**
     * HTTP handler for /engine/diagram — delegates to DiagramService.
     */
    private class DiagramHandler implements HttpHandler {
        private final DiagramService diagramService = new DiagramService();

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);
            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
                return;
            }
            if (!"POST".equals(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
                return;
            }

            try {
                String body = readBody(exchange);
                Map<String, Object> request = LegendHttpJson.parseObject(body);
                String pureSource = LegendHttpJson.getString(request, "code");

                if (pureSource == null || pureSource.isBlank()) {
                    sendResponse(exchange, 400, "{\"error\":\"Missing 'code' field\"}");
                    return;
                }

                DiagramService.DiagramData data = diagramService.extract(pureSource);
                String json = diagramService.toJson(data);
                sendResponse(exchange, 200, json);

            } catch (Throwable e) {
                System.err.println("DiagramHandler error: " + e);
                e.printStackTrace(System.err);
                System.err.flush();
                try {
                    sendResponse(exchange, 500, "{\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
                } catch (Throwable ignore) {
                    // response already committed
                }
            }
        }
    }

    /**
     * Register an additional HTTP context (route) on this server.
     * Used by extension modules (e.g. nlq) to add endpoints.
     */
    public void addContext(String path, HttpHandler handler) {
        server.createContext(path, handler);
    }

    public void start() {
        server.setExecutor(null);
        server.start();
        System.out.println("Legend HTTP server started on port " + server.getAddress().getPort());
    }

    public void stop() {
        server.stop(0);
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    public static void main(String[] args) throws IOException {
        int port = 8080;
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port: " + args[0]);
            }
        }

        LegendHttpServer server = new LegendHttpServer(port);
        server.start();

        System.out.println();
        System.out.println("======================================");
        System.out.println("  Legend Studio Lite - Backend Ready");
        System.out.println("======================================");
        System.out.println();
        System.out.println("Endpoints:");
        System.out.println("  POST http://localhost:" + port + "/lsp         - LSP Protocol");
        System.out.println("  POST http://localhost:" + port + "/engine/execute - Execute Pure query");
        System.out.println("  POST http://localhost:" + port + "/engine/sql     - Execute raw SQL");
        System.out.println("  GET  http://localhost:" + port + "/health         - Health check");
        System.out.println();
        System.out.println("Press Ctrl+C to stop");

        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
    }
}
