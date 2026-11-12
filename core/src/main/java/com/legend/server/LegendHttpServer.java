package com.legend.server;

import com.legend.queries.FileCubeRecords;
import com.legend.queries.FileRecords;
import com.legend.queries.SavedCubes;
import com.legend.queries.SavedQueries;
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
 * Endpoints (lite's own; phase D's D1.0b, 2026-11-12: legend-engine's routes are the edge's, mounted by the
 * shipped binary's root, {@code com.legend.edge.EdgeServer}, as a {@link Mount}):
 * - POST /lsp - Handle LSP JSON-RPC messages (diagnostics, completions, etc.)
 * - POST /api/lite/v1/... - lite's own API ({@link PureV1Api}: every compile error, the model routes)
 * - /api/lite/v1/query - the saved queries and DataCube's saved pages ({@link SavedQueries}, {@link SavedCubes})
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
    /** DataCube's saved pages, beside the queries ({@code DIR/dataCube}; the program's B6), or null. */
    private final @com.legend.base.Nullable SavedCubes cubeStore;
    /** The product door, its file: sources fenced to the folders the server's startup settings name, none when they
     *  name none ({@link #sources}; the user's ruling of 2026-11-10). */
    private final PureV1Api.Runner runner;

    /**
     * A module of routes the server mounts beside lite's own (phase D's D1.0b, 2026-11-12): everything under its
     * {@link #prefix}, answered by {@link #answer}. Lite's own server mounts none; the shipped binary's root, the edge's
     * ({@code com.legend.edge.EdgeServer}), mounts legend-engine's API (G5: the product names no module of the edge's).
     */
    public interface Routes {
        /** The path prefix it answers under ({@code /api/pure/v1/}). */
        String prefix();

        /** The answer of one request: its method, path, raw query string (or null) and body. */
        PureV1Api.Answer answer(String method, String path, @com.legend.base.Nullable String rawQuery, String body)
                throws IOException;
    }

    /** What the server hands a module it mounts: its file fence and its stores. */
    public record Context(String sources, @com.legend.base.Nullable SavedQueries queryStore,
            @com.legend.base.Nullable SavedCubes cubeStore) {
    }

    /** The modules a server mounts, made from its {@link Context}. */
    @FunctionalInterface
    public interface Mount {
        List<Routes> routes(Context context);
    }

    /** Lite's own server: no module beside its own routes. */
    public static final Mount LITE = context -> List.of();

    /** On the loopback interface, loopback origins only: the development server, reading no file: source. */
    public LegendHttpServer(int port) throws IOException {
        this(port, java.net.InetAddress.getLoopbackAddress(), Origins.LOOPBACK, null);
    }

    /** {@code queryStore}: the directory {@code --query-store DIR} names -- the saved queries in it, DataCube's saved
     *  pages in its {@code dataCube} -- or null, every call of the store's then refused. No file: source is read. */
    public LegendHttpServer(int port, java.net.InetAddress bind, Origins origins,
            java.nio.file.@com.legend.base.Nullable Path queryStore) throws IOException {
        this(port, bind, origins, queryStore, NO_SOURCES);
    }

    /** {@link #LegendHttpServer(int, java.net.InetAddress, Origins, java.nio.file.Path)}, its file: sources read from
     *  the folders {@code sources} names ({@link #sources}: {@code any}, {@code none}, or folders separated by the
     *  platform's path separator). */
    public LegendHttpServer(int port, java.net.InetAddress bind, Origins origins,
            java.nio.file.@com.legend.base.Nullable Path queryStore, String sources) throws IOException {
        this(port, bind, origins, queryStore, sources, LITE);
    }

    /** {@link #LegendHttpServer(int, java.net.InetAddress, Origins, java.nio.file.Path, String)} with {@code mount}'s
     *  modules beside lite's own routes. */
    public LegendHttpServer(int port, java.net.InetAddress bind, Origins origins,
            java.nio.file.@com.legend.base.Nullable Path queryStore, String sources, Mount mount) throws IOException {
        // the folders validated once, where the server starts: a folder that is none refuses the start, never every
        // query after it (the A delta audit's 5, 2026-11-11)
        validateSources(sources);
        this.runner = ConnectionResolver.runner(sources);
        this.server = HttpServer.create(new InetSocketAddress(bind, port), 0);
        this.lspServer = new PureLspServer();
        this.origins = origins;
        this.queryStore = queryStore == null ? null
                : new SavedQueries(new FileRecords(queryStore), System::currentTimeMillis);
        this.cubeStore = queryStore == null ? null
                : new SavedCubes(new FileCubeRecords(queryStore.resolve("dataCube")), System::currentTimeMillis);
        setupRoutes();
        for (Routes module : mount.routes(new Context(sources, this.queryStore, this.cubeStore))) {
            route(module.prefix(), new ModuleHandler(module));
        }
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
            this.extra = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(extra));
        }

        /** {@code LEGEND_LITE_ALLOWED_ORIGINS}: comma-separated exact origins, e.g. {@code https://studio.example}. */
        public static Origins fromEnv(@com.legend.base.Nullable String list) {
            if (list == null || list.strip().isEmpty()) {
                return LOOPBACK;
            }
            java.util.Set<String> out = new java.util.LinkedHashSet<>();
            for (String o : list.split(",")) {
                if (!o.strip().isEmpty()) {
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

        // lite's own API (PureV1Api; legend-engine's pure/v1 routes are the edge's, mounted by the shipped binary's
        // root: phase D's D1.0b, 2026-11-12)
        route("/api/lite/v1/", new PureV1Handler(runner));
        // the saved queries and DataCube's saved pages, lite's own route (the orchestrator's ruling of 2026-11-12: the
        // store is the product's; legend-engine's /api/pure/v1/query is the edge's adapter over the same store)
        route("/api/lite/v1/query", new ModuleHandler(storeRoutes("/api/lite/v1/query", queryStore, cubeStore)));
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
     * {@code /api/lite/v1/...}: lite's own API, answered by {@link PureV1Api}. The body
     * is read RAW -- a grammar text's line endings are part of its source positions.
     */
    /**
     * The saved queries and DataCube's saved pages under {@code prefix} ({@code /dataCube...} the pages, anything else
     * the queries), answered by the stores' own operations ({@link SavedQueries#answer}, {@link SavedCubes#answer}) for
     * the anonymous user: lite's own route, and the module the edge's adapter of legend-engine's path wraps.
     */
    public static Routes storeRoutes(String prefix, @com.legend.base.Nullable SavedQueries queryStore,
            @com.legend.base.Nullable SavedCubes cubeStore) {
        return new Routes() {
            @Override
            public String prefix() {
                return prefix;
            }

            @Override
            public PureV1Api.Answer answer(String method, String path, @com.legend.base.Nullable String rawQuery,
                    String body) {
                String rest = path.substring(prefix.length());
                SavedQueries.Answer a = rest.equals("/dataCube") || rest.startsWith("/dataCube/")
                        ? SavedCubes.answer(cubeStore, method, rest.substring("/dataCube".length()), rawQuery, body,
                                SavedQueries.ANONYMOUS)
                        : SavedQueries.answer(queryStore, method, rest, rawQuery, body, SavedQueries.ANONYMOUS);
                return new PureV1Api.Answer(a.status(), a.json());
            }
        };
    }

    /** A mounted module's route ({@link Routes}): the CORS preflight answered, the body read RAW, the module's answer
     *  sent with its headers (none for a 204). */
    private static final class ModuleHandler implements HttpHandler {
        private final Routes module;

        ModuleHandler(Routes module) {
            this.module = module;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCorsHeaders(exchange);
            if ("OPTIONS".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
                return;
            }
            String body;
            try (InputStream in = exchange.getRequestBody()) {
                body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
            PureV1Api.Answer answer = module.answer(exchange.getRequestMethod(), exchange.getRequestURI().getPath(),
                    exchange.getRequestURI().getRawQuery(), body);
            answer.headers().forEach((k, v) -> exchange.getResponseHeaders().add(k, v));
            if (answer.status() == 204) {
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
                return;
            }
            sendResponse(exchange, answer.status(), answer.body(), answer.contentType());
        }
    }

    private static final class PureV1Handler implements HttpHandler {
        private final PureV1Api.Runner runner;

        PureV1Handler(PureV1Api.Runner runner) {
            this.runner = runner;
        }

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
                    exchange.getRequestURI().getRawQuery(), body, runner);
            // a binary answer (the ARROW_IPC execute, track A, 2026-10-29): its bytes under upstream's headers
            answer.headers().forEach((k, v) -> exchange.getResponseHeaders().add(k, v));
            sendResponse(exchange, answer.status(), answer.body(), answer.contentType());
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
        sendResponse(exchange, status, body.getBytes(StandardCharsets.UTF_8), contentType);
    }

    static void sendResponse(HttpExchange exchange, int status, byte[] bytes, String contentType) throws IOException {
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

                if (pureSource == null || pureSource.strip().isEmpty()) {
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

    /** The fence of a server whose startup settings name no folder: none (the user's ruling of 2026-11-10). */
    public static final String NO_SOURCES = "none";

    /** The server's command line, its {@code --help}. */
    static final String USAGE = """
            legend-lite server: [PORT] [--query-store DIR] [--sources DIR]... [--help]
              PORT                the port to listen on (or $PORT; 8080 by default)
              --query-store DIR   the saved queries' directory (DataCube's saved pages in DIR/dataCube)
              --sources DIR       a folder a model's file: sources may read, repeatable (or $LEGEND_LITE_SOURCE_DIRS,
                                  folders separated by the path separator); 'any' reads every file. With none given the
                                  server reads no file: source: each is refused by name
            Environment: LEGEND_LITE_BIND (the listen address, loopback by default), LEGEND_LITE_ALLOWED_ORIGINS (the
            pages beside loopback that may call it), LEGEND_LITE_SOURCE_DIRS (as --sources).""";

    /**
     * The folders a model's {@code file:} sources may read, from the server's startup settings (the user's ruling of
     * 2026-11-10: the library reads any file by default, the SERVER fences them to the folders its settings name): each
     * {@code --sources DIR} of {@code args}, else {@code env} ({@code LEGEND_LITE_SOURCE_DIRS}), else none -- every
     * {@code file:} source refused by name until a folder is named. {@code any} widens it to every file.
     */
    static String sources(String[] args, @com.legend.base.Nullable String env) {
        java.util.List<String> dirs = new java.util.ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            if ("--sources".equals(args[i]) && i + 1 < args.length) {
                dirs.add(args[++i]);
            }
        }
        if (!dirs.isEmpty()) {
            return dirs.contains("any") ? "any" : String.join(java.io.File.pathSeparator, dirs);
        }
        return env == null || env.strip().isEmpty() ? NO_SOURCES : env.strip();
    }

    /** {@code sources} ({@link #sources}) refused by name where a folder it names is no directory: the server does not
     *  start (the A delta audit's 5, 2026-11-11). */
    static void validateSources(String sources) {
        if (sources.equals("any") || sources.equals(NO_SOURCES)) {
            return;
        }
        for (String d : sources.split(java.util.regex.Pattern.quote(java.io.File.pathSeparator))) {
            if (!d.strip().isEmpty() && !java.nio.file.Files.isDirectory(java.nio.file.Path.of(d.strip()))) {
                throw new IllegalArgumentException("the server's sources folder '" + d.strip() + "' (--sources DIR or"
                        + " LEGEND_LITE_SOURCE_DIRS) is no directory: the server does not start");
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

    /** Lite's own server, started as {@link #serve} starts one with no module mounted ({@link #LITE}). */
    public static void main(String[] args) throws IOException {
        serve(args, LITE);
    }

    /** A server started from the command line's {@code args} and the environment (the port, {@code --query-store DIR},
     *  {@code --sources DIR}..., {@code LEGEND_LITE_BIND}, {@code LEGEND_LITE_ALLOWED_ORIGINS},
     *  {@code LEGEND_LITE_SOURCE_DIRS}), {@code mount}'s modules beside lite's own routes: lite's own main, and the
     *  shipped binary's root, the edge's. */
    public static void serve(String[] args, Mount mount) throws IOException {
        int port = 8080;
        String envPort = System.getenv("PORT");
        if (envPort != null && !envPort.strip().isEmpty()) {
            try {
                port = Integer.parseInt(envPort);
            } catch (NumberFormatException e) {
                System.err.println("Invalid PORT env var: " + envPort);
            }
        }
        // arguments: [port] [--query-store DIR] (the saved-query store's directory) [--sources DIR]... (the folders
        // a model's file: sources may read) [--help]
        java.nio.file.Path queryStore = null;
        for (int i = 0; i < args.length; i++) {
            if ("--help".equals(args[i]) || "-h".equals(args[i])) {
                System.out.println(USAGE);
                return;
            }
            if ("--query-store".equals(args[i]) && i + 1 < args.length) {
                queryStore = java.nio.file.Path.of(args[++i]);
            } else if ("--sources".equals(args[i]) && i + 1 < args.length) {
                i++;   // read by sources(args, ...) below
            } else {
                try {
                    port = Integer.parseInt(args[i]);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid port: " + args[i]);
                }
            }
        }

        String bind = System.getenv("LEGEND_LITE_BIND");
        java.net.InetAddress address = bind == null || bind.strip().isEmpty()
                ? java.net.InetAddress.getLoopbackAddress()
                : java.net.InetAddress.getByName(bind);
        if (!address.isLoopbackAddress()) {
            System.err.println("WARNING: LEGEND_LITE_BIND=" + bind + " exposes this server beyond this machine;"
                    + " a caller's model runs its connections' setup SQL on this host.");
        }
        String sources = sources(args, System.getenv("LEGEND_LITE_SOURCE_DIRS"));
        if (sources.equals(NO_SOURCES)) {
            System.err.println("NOTE: no --sources DIR given: a model's file: sources are refused (any widens to every"
                    + " file).");
        }
        LegendHttpServer server;
        try {
            server = new LegendHttpServer(port, address,
                    Origins.fromEnv(System.getenv("LEGEND_LITE_ALLOWED_ORIGINS")), queryStore, sources, mount);
        } catch (IllegalArgumentException refused) {
            // a startup setting that names no folder: refused by name, the server not started
            System.err.println("ERROR: " + refused.getMessage());
            System.exit(2);
            return;
        }
        server.start();

        System.out.println();
        System.out.println("======================================");
        System.out.println("  Legend Studio Lite - Backend Ready");
        System.out.println("======================================");
        System.out.println();
        System.out.println("Endpoints:");
        System.out.println("  POST http://localhost:" + port + "/lsp         - LSP Protocol");
        System.out.println("  POST http://localhost:" + port + "/api/lite/v1/... - legend-lite's own API");
        System.out.println("  POST http://localhost:" + port + "/api/lite/v1/compilation/compile - every compile error");
        System.out.println("  GET  http://localhost:" + port + "/health         - Health check");
        System.out.println();
        System.out.println("Press Ctrl+C to stop");

        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
    }
}
