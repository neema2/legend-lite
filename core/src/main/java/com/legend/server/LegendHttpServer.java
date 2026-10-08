package com.legend.server;
import com.legend.json.Json;


import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Legend Studio Lite HTTP Server.
 * 
 * Uses Java's built-in com.sun.net.httpserver - no external dependencies.
 * 
 * Endpoints:
 * - POST /lsp - Handle LSP JSON-RPC messages (diagnostics, completions, etc.)
 * - POST /api/pure/v1/... - legend-engine's own pure/v1 API
 * - POST /engine/diagram - a class diagram from a Pure model
 * - GET /health - Health check
 *
 * <p>The server's doors (execution plan W0.1, 2026-09-29). It binds the LOOPBACK interface unless
 * {@code LEGEND_LITE_BIND} names another address, and it refuses every request whose {@code Origin}
 * header names a page outside the allow-list (loopback origins, plus {@code LEGEND_LITE_ALLOWED_ORIGINS}).
 * A browser always sends {@code Origin} on a cross-origin POST, including a "simple" text/plain one that
 * skips the CORS preflight, so a web page elsewhere cannot drive a model's connections through this
 * server; a request with no {@code Origin} comes from a local non-browser client. The CORS answer echoes
 * the allowed origin instead of {@code *}. The raw-SQL route {@code /engine/sql} is gone.
 */
public class LegendHttpServer {

    private final HttpServer server;
    private final PureLspServer lspServer;
    private final Origins origins;
    /** The saved-query store {@code --query-store DIR} names, or null (its calls are then refused). */
    private final @com.legend.base.Nullable SavedQueries queryStore;

    /** On the loopback interface, loopback origins only: the development server. */
    public LegendHttpServer(int port) throws IOException {
        this(port, java.net.InetAddress.getLoopbackAddress(), Origins.LOOPBACK, null);
    }

    public LegendHttpServer(int port, java.net.InetAddress bind, Origins origins,
            @com.legend.base.Nullable SavedQueries queryStore) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(bind, port), 0);
        this.lspServer = new PureLspServer();
        this.origins = origins;
        this.queryStore = queryStore;
        setupRoutes();
    }

    /**
     * The pages allowed to call this server: any loopback origin ({@code http[s]://localhost},
     * {@code 127.0.0.1} or {@code [::1]}, any port), plus the exact origins listed.
     */
    public static final class Origins {

        public static final Origins LOOPBACK = new Origins(java.util.Set.of());

        private static final Pattern LOOPBACK_ORIGIN = Pattern.compile(
                "https?://(localhost|127\\.0\\.0\\.1|\\[::1\\])(:[0-9]{1,5})?");

        private final java.util.Set<String> extra;

        private Origins(java.util.Set<String> extra) {
            this.extra = java.util.Set.copyOf(extra);
        }

        /** {@code LEGEND_LITE_ALLOWED_ORIGINS}: comma-separated exact origins, e.g. {@code https://studio.example}. */
        public static Origins fromEnv(@com.legend.base.Nullable String list) {
            if (list == null || list.isBlank()) {
                return LOOPBACK;
            }
            java.util.Set<String> out = new java.util.LinkedHashSet<>();
            for (String o : list.split(",")) {
                if (!o.isBlank()) {
                    out.add(o.strip());
                }
            }
            return new Origins(out);
        }

        public boolean allows(String origin) {
            return LOOPBACK_ORIGIN.matcher(origin).matches() || extra.contains(origin);
        }
    }

    /** Every route passes the origin check first; a refused origin never reaches a handler. */
    private void route(String path, HttpHandler handler) {
        server.createContext(path, exchange -> {
            String origin = exchange.getRequestHeaders().getFirst("Origin");
            if (origin != null && !origins.allows(origin)) {
                sendResponse(exchange, 403,
                        "{\"error\":\"origin not allowed: " + Json.escape(origin) + "\"}");
                return;
            }
            handler.handle(exchange);
        });
    }

    private void setupRoutes() {
        // LSP Protocol - diagnostics, completions, etc.
        route("/lsp", new LspHandler());

        // Engine - query and SQL execution
        // legend-engine's own pure/v1 API, exactly (PureV1Api; the user's ruling of
        // 2026-09-27: lite serves upstream's APIs and nothing of its own)
        route("/api/pure/v1/", new PureV1Handler());
        // legend-engine's query store and current user (SavedQueries; the Query app's G5/G7)
        route("/api/pure/v1/query", exchange -> {
            addCorsHeaders(exchange);
            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
                return;
            }
            String body = readBody(exchange);
            String rest = exchange.getRequestURI().getRawPath().substring("/api/pure/v1/query".length());
            PureV1Api.Answer a = SavedQueries.answer(queryStore, exchange.getRequestMethod(), rest,
                    exchange.getRequestURI().getRawQuery(), body, SavedQueries.ANONYMOUS);
            if (a.status() == 204) {
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
            } else {
                sendResponse(exchange, a.status(), a.json(), a.contentType());
            }
        });
        route("/api/server/v1/currentUser", exchange -> {
            addCorsHeaders(exchange);
            sendResponse(exchange, 200, "\"" + SavedQueries.ANONYMOUS + "\"");
        });
        route("/engine/diagram", new DiagramHandler());

        // Health check
        route("/health", exchange -> {
            addCorsHeaders(exchange);
            sendResponse(exchange, 200, "{\"status\":\"ok\"}");
        });

        // CORS preflight for all routes
        route("/", exchange -> {
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
                List<String> responses = lspServer.handleMessage(body);
                if (responses.isEmpty()) {
                    sendResponse(exchange, 204, "");
                } else if (responses.size() == 1) {
                    sendResponse(exchange, 200, responses.get(0));
                } else {
                    sendResponse(exchange, 200, "[" + String.join(",", responses) + "]");
                }
            } catch (Exception e) {
                sendResponse(exchange, 500, "{\"error\":\"" + Json.escape(e.getMessage()) + "\"}");
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
    /**
     * {@code /api/pure/v1/...}: legend-engine's API, answered by {@link PureV1Api}. The body
     * is read RAW -- a grammar text's line endings are part of its source positions.
     */
    private static final class PureV1Handler implements HttpHandler {
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
            String body;
            try (InputStream in = exchange.getRequestBody()) {
                body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
            // the routes are the API's own (PureV1Api.route, the plan side); execute runs through the driver
            PureV1Api.Answer answer = PureV1Api.route(exchange.getRequestURI().getPath(),
                    exchange.getRequestURI().getRawQuery(), body, new QueryService()::executeUpstream);
            sendResponse(exchange, answer.status(), answer.json(), answer.contentType());
        }
    }

    /** The CORS answer for an ALLOWED origin (the route's check ran first): it names that origin, never {@code *}. */
    public static void addCorsHeaders(HttpExchange exchange) {
        var headers = exchange.getResponseHeaders();
        String origin = exchange.getRequestHeaders().getFirst("Origin");
        if (origin == null) {
            return;
        }
        headers.add("Access-Control-Allow-Origin", origin);
        headers.add("Vary", "Origin");
        headers.add("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
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
        sendResponse(exchange, status, body, "application/json");
    }

    static void sendResponse(HttpExchange exchange, int status, String body, String contentType) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", contentType);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
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
                Json.Obj request = Json.parseObject(body);
                String pureSource = request.getStringOr("code", null);

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
                    sendResponse(exchange, 500, "{\"error\":\"" + Json.escape(e.getMessage()) + "\"}");
                } catch (Throwable ignore) {
                    // response already committed
                }
            }
        }
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
        String envPort = System.getenv("PORT");
        if (envPort != null && !envPort.isBlank()) {
            try {
                port = Integer.parseInt(envPort);
            } catch (NumberFormatException e) {
                System.err.println("Invalid PORT env var: " + envPort);
            }
        }
        // arguments: [port] [--query-store DIR] (the saved-query store's directory)
        SavedQueries queryStore = null;
        for (int i = 0; i < args.length; i++) {
            if ("--query-store".equals(args[i]) && i + 1 < args.length) {
                queryStore = new SavedQueries(java.nio.file.Path.of(args[++i]), System::currentTimeMillis);
            } else {
                try {
                    port = Integer.parseInt(args[i]);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid port: " + args[i]);
                }
            }
        }

        String bind = System.getenv("LEGEND_LITE_BIND");
        java.net.InetAddress address = bind == null || bind.isBlank()
                ? java.net.InetAddress.getLoopbackAddress()
                : java.net.InetAddress.getByName(bind);
        if (!address.isLoopbackAddress()) {
            System.err.println("WARNING: LEGEND_LITE_BIND=" + bind + " exposes this server beyond this machine;"
                    + " a caller's model runs its connections' setup SQL on this host.");
        }
        LegendHttpServer server = new LegendHttpServer(port, address,
                Origins.fromEnv(System.getenv("LEGEND_LITE_ALLOWED_ORIGINS")), queryStore);
        server.start();

        System.out.println();
        System.out.println("======================================");
        System.out.println("  Legend Studio Lite - Backend Ready");
        System.out.println("======================================");
        System.out.println();
        System.out.println("Endpoints:");
        System.out.println("  POST http://localhost:" + port + "/lsp         - LSP Protocol");
        System.out.println("  POST http://localhost:" + port + "/api/pure/v1/... - legend-engine's pure/v1 API");
        System.out.println("  GET  http://localhost:" + port + "/health         - Health check");
        System.out.println();
        System.out.println("Press Ctrl+C to stop");

        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
    }
}
