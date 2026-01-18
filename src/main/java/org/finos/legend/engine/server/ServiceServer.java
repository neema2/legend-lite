package org.finos.legend.engine.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * Embedded HTTP server for hosting Legend services.
 * 
 * Uses JDK's built-in com.sun.net.httpserver which is GraalVM native-image
 * compatible.
 * 
 * Features:
 * - URL pattern matching with path parameters
 * - JSON response serialization
 * - Error handling with proper HTTP status codes
 * - Connection pooling via supplier
 */
public final class ServiceServer {

    private final HttpServer server;
    private final ServiceRegistry registry;
    private final Supplier<Connection> connectionSupplier;

    /**
     * Creates a new service server.
     * 
     * @param port               The port to listen on
     * @param registry           The service registry
     * @param connectionSupplier Supplier for database connections
     * @throws IOException if the server cannot be created
     */
    public ServiceServer(int port, ServiceRegistry registry,
            Supplier<Connection> connectionSupplier) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.registry = registry;
        this.connectionSupplier = connectionSupplier;

        // Create a catch-all handler
        server.createContext("/", new ServiceHandler());

        // Use virtual threads if available, otherwise use a cached thread pool
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
    }

    /**
     * Starts the server.
     */
    public void start() {
        server.start();
        System.out.println("Legend Lite Server started on port " +
                server.getAddress().getPort());
    }

    /**
     * Stops the server.
     * 
     * @param delay Delay in seconds before forcing shutdown
     */
    public void stop(int delay) {
        server.stop(delay);
        System.out.println("Legend Lite Server stopped");
    }

    /**
     * @return The port the server is listening on
     */
    public int getPort() {
        return server.getAddress().getPort();
    }

    /**
     * Handler that routes requests to the appropriate service.
     */
    private class ServiceHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            String method = exchange.getRequestMethod();

            // Only support GET for now
            if (!"GET".equalsIgnoreCase(method)) {
                sendError(exchange, 405, "Method Not Allowed");
                return;
            }

            // Find matching service
            var match = registry.findService(path);
            if (match.isEmpty()) {
                sendError(exchange, 404, "Not Found: " + path);
                return;
            }

            // Parse query parameters
            Map<String, String> queryParams = parseQueryString(
                    exchange.getRequestURI().getQuery());

            // Execute service
            try {
                Connection conn = connectionSupplier.get();
                String result = match.get().service().executor().execute(
                        match.get().pathParameters(),
                        queryParams,
                        conn);
                sendJson(exchange, 200, result);
            } catch (Exception e) {
                sendError(exchange, 500, "Internal Server Error: " + e.getMessage());
            }
        }

        private void sendJson(HttpExchange exchange, int statusCode, String json)
                throws IOException {
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(statusCode, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }

        private void sendError(HttpExchange exchange, int statusCode, String message)
                throws IOException {
            String json = "{\"error\":\"" + escapeJson(message) + "\"}";
            sendJson(exchange, statusCode, json);
        }

        private String escapeJson(String s) {
            if (s == null)
                return "";
            return s.replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r")
                    .replace("\t", "\\t");
        }

        private Map<String, String> parseQueryString(String query) {
            Map<String, String> params = new HashMap<>();
            if (query == null || query.isEmpty()) {
                return params;
            }

            for (String pair : query.split("&")) {
                int idx = pair.indexOf("=");
                if (idx > 0) {
                    String key = URLDecoder.decode(
                            pair.substring(0, idx), StandardCharsets.UTF_8);
                    String value = URLDecoder.decode(
                            pair.substring(idx + 1), StandardCharsets.UTF_8);
                    params.put(key, value);
                }
            }
            return params;
        }
    }
}
