package org.finos.legend.engine.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.finos.legend.engine.execution.BufferedResult;
import org.finos.legend.engine.server.QueryService;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * HTTP adapter for the Pure LSP server.
 * 
 * Uses Java's built-in com.sun.net.httpserver - no external dependencies.
 * 
 * Endpoints:
 * - POST /lsp - Handle LSP JSON-RPC messages
 * - POST /lsp/execute - Execute Pure code (receives full model from frontend)
 * - GET /health - Health check
 */
public class LegendHttpServer {

    private final HttpServer server;
    private final PureLspServer lspServer;
    private final QueryService queryService = new QueryService();
    private Connection duckDbConnection;

    // Pattern to find the Runtime definition
    private static final Pattern RUNTIME_PATTERN = Pattern.compile(
            "Runtime\\s+([\\w:]+)\\s*\\{",
            Pattern.MULTILINE);

    public LegendHttpServer(int port) throws IOException, SQLException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.lspServer = new PureLspServer();
        initializeDuckDb();
        setupRoutes();
    }

    /**
     * Initialize in-memory DuckDB with sample data.
     * The database provides real data to execute queries against.
     */
    private void initializeDuckDb() throws SQLException {
        duckDbConnection = DriverManager.getConnection("jdbc:duckdb:");

        try (Statement stmt = duckDbConnection.createStatement()) {
            // Create T_PERSON table
            stmt.execute("""
                    CREATE TABLE T_PERSON (
                        ID INTEGER PRIMARY KEY,
                        FIRST_NAME VARCHAR(100) NOT NULL,
                        LAST_NAME VARCHAR(100) NOT NULL,
                        AGE_VAL INTEGER NOT NULL
                    )
                    """);

            // Create T_ADDRESS table
            stmt.execute("""
                    CREATE TABLE T_ADDRESS (
                        ID INTEGER PRIMARY KEY,
                        PERSON_ID INTEGER NOT NULL,
                        STREET VARCHAR(200) NOT NULL,
                        CITY VARCHAR(100) NOT NULL
                    )
                    """);

            // Insert sample person data
            stmt.execute("INSERT INTO T_PERSON VALUES (1, 'John', 'Smith', 30)");
            stmt.execute("INSERT INTO T_PERSON VALUES (2, 'Jane', 'Smith', 28)");
            stmt.execute("INSERT INTO T_PERSON VALUES (3, 'Bob', 'Jones', 45)");
            stmt.execute("INSERT INTO T_PERSON VALUES (4, 'Alice', 'Williams', 22)");
            stmt.execute("INSERT INTO T_PERSON VALUES (5, 'Charlie', 'Brown', 35)");

            // Insert sample address data
            stmt.execute("INSERT INTO T_ADDRESS VALUES (1, 1, '123 Main St', 'New York')");
            stmt.execute("INSERT INTO T_ADDRESS VALUES (2, 1, '456 Oak Ave', 'Boston')");
            stmt.execute("INSERT INTO T_ADDRESS VALUES (3, 2, '789 Elm Rd', 'Chicago')");
            stmt.execute("INSERT INTO T_ADDRESS VALUES (4, 3, '999 Pine Lane', 'Detroit')");
            stmt.execute("INSERT INTO T_ADDRESS VALUES (5, 5, '555 Maple Dr', 'Seattle')");
        }

        System.out.println("Initialized in-memory DuckDB with sample data");
    }

    private void setupRoutes() {
        server.createContext("/lsp", new LspHandler());
        server.createContext("/lsp/execute", new ExecuteHandler());
        server.createContext("/health", exchange -> {
            addCorsHeaders(exchange);
            sendResponse(exchange, 200, "{\"status\":\"ok\"}");
        });
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

                // Execute using QueryService - modelSource (definitions only), query
                // (expression only)
                BufferedResult result = queryService.execute(
                        modelSource,
                        query,
                        runtimeName,
                        duckDbConnection);

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

    private void addCorsHeaders(HttpExchange exchange) {
        var headers = exchange.getResponseHeaders();
        headers.add("Access-Control-Allow-Origin", "*");
        headers.add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        headers.add("Access-Control-Allow-Headers", "Content-Type");
    }

    private String readBody(HttpExchange exchange) throws IOException {
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

    private void sendResponse(HttpExchange exchange, int status, String body) throws IOException {
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

    public void start() {
        server.setExecutor(null);
        server.start();
        System.out.println("Legend HTTP server started on port " + server.getAddress().getPort());
    }

    public void stop() {
        server.stop(0);
        try {
            if (duckDbConnection != null && !duckDbConnection.isClosed()) {
                duckDbConnection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    public static void main(String[] args) throws IOException, SQLException {
        int port = 8081;
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port: " + args[0]);
            }
        }

        LegendHttpServer adapter = new LegendHttpServer(port);
        adapter.start();

        System.out.println();
        System.out.println("======================================");
        System.out.println("  Legend Studio Lite - Backend Ready");
        System.out.println("======================================");
        System.out.println();
        System.out.println("Endpoints:");
        System.out.println("  http://localhost:" + port + "/lsp");
        System.out.println("  http://localhost:" + port + "/lsp/execute");
        System.out.println("  http://localhost:" + port + "/health");
        System.out.println();
        System.out.println("DuckDB initialized with sample data:");
        System.out.println("  - T_PERSON (5 rows)");
        System.out.println("  - T_ADDRESS (5 rows)");
        System.out.println();
        System.out.println("Press Ctrl+C to stop");

        Runtime.getRuntime().addShutdownHook(new Thread(adapter::stop));
    }
}
