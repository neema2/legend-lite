package org.finos.legend.engine.lsp;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * HTTP adapter for the Pure LSP server.
 * 
 * This is a thin HTTP wrapper - all logic is delegated to PureLspServer.
 * Uses Java's built-in com.sun.net.httpserver - no external dependencies.
 * 
 * Endpoints:
 * - POST /lsp - Handle LSP JSON-RPC messages
 * - POST /lsp/execute - Execute Pure code and return results
 * - GET /health - Health check
 */
public class LspHttpAdapter {

    private final HttpServer server;
    private final PureLspServer lspServer;

    public LspHttpAdapter(int port) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.lspServer = new PureLspServer();
        setupRoutes();
    }

    private void setupRoutes() {
        // LSP endpoint - delegates to lspServer.handleMessage()
        server.createContext("/lsp", new LspHandler());

        // Execute endpoint - delegates to lspServer.executeCode()
        server.createContext("/lsp/execute", new ExecuteHandler());

        // Health check
        server.createContext("/health", exchange -> {
            sendResponse(exchange, 200, "{\"status\":\"ok\"}");
        });

        // CORS preflight
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

    /**
     * Handle LSP JSON-RPC messages - delegates to PureLspServer.
     */
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
                sendResponse(exchange, 500,
                        "{\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
            }
        }
    }

    /**
     * Execute Pure code - delegates to PureLspServer.
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
                Map<String, Object> request = LspJson.parseObject(body);
                String code = LspJson.getString(request, "code");

                if (code == null || code.isBlank()) {
                    sendResponse(exchange, 400, "{\"error\":\"Missing 'code' field\"}");
                    return;
                }

                // Delegate to PureLspServer
                String result = lspServer.executeCode(code);

                Map<String, Object> response = new LinkedHashMap<>();
                response.put("success", true);
                response.put("result", result);

                sendResponse(exchange, 200, LspJson.toJson(response));

            } catch (Exception e) {
                Map<String, Object> response = new LinkedHashMap<>();
                response.put("success", false);
                response.put("error", e.getMessage());

                sendResponse(exchange, 200, LspJson.toJson(response));
            }
        }
    }

    // ========== HTTP Helpers ==========

    private void addCorsHeaders(HttpExchange exchange) {
        var headers = exchange.getResponseHeaders();
        headers.add("Access-Control-Allow-Origin", "*");
        headers.add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        headers.add("Access-Control-Allow-Headers", "Content-Type");
    }

    private String readBody(HttpExchange exchange) throws IOException {
        try (InputStream is = exchange.getRequestBody();
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(is, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
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
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r");
    }

    // ========== Server Lifecycle ==========

    public void start() {
        server.setExecutor(null);
        server.start();
        System.out.println("LSP HTTP server started on port " +
                server.getAddress().getPort());
    }

    public void stop() {
        server.stop(0);
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    // ========== Main Entry Point ==========

    public static void main(String[] args) throws IOException {
        int port = 8081;
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port: " + args[0]);
            }
        }

        LspHttpAdapter adapter = new LspHttpAdapter(port);
        adapter.start();

        System.out.println("Legend-Lite LSP Server");
        System.out.println("======================");
        System.out.println("LSP endpoint: http://localhost:" + port + "/lsp");
        System.out.println("Execute endpoint: http://localhost:" + port + "/lsp/execute");
        System.out.println("Health check: http://localhost:" + port + "/health");
        System.out.println();
        System.out.println("Press Ctrl+C to stop");

        Runtime.getRuntime().addShutdownHook(new Thread(adapter::stop));
    }
}
