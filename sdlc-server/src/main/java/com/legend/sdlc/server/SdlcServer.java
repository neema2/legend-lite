package com.legend.sdlc.server;

import com.legend.sdlc.CoreGrammar;
import com.legend.sdlc.Sdlc;
import com.legend.sdlc.Storage;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * SDLC-lite as a server (design S1, levels 1-2): {@link Sdlc}'s rules -- the same class the page runs
 * compiled to WebAssembly -- behind the JDK's HTTP server at {@code /sdlc/api}, over a {@link Storage}
 * (a git repository on disk, {@link GitStorage}), with Depot-lite beside it at {@code /depot/api}. One request at a
 * time through the rules (the repository has one writer).
 *
 * <p>It listens on loopback only and, having no sign-in yet, trusts only pages it was told to: a request is refused
 * (403) when its {@code Host} is not this loopback server (DNS rebinding) or when it carries an {@code Origin} that is
 * not allowed -- by default a page served from this machine ({@code http://127.0.0.1:*}, {@code http://localhost:*}),
 * plus each {@code --allow-origin}. CORS answers only those origins, never {@code *}. A request without an
 * {@code Origin} (a test, a script, curl) is not a browser's and is served.
 *
 * <pre>bazel run //sdlc-server:server -- --port 6100 --repo DIR [--user id[:Name]] [--allow-origin URL]...</pre>
 */
public final class SdlcServer {
    public static final String ROOT = "/sdlc/api";
    public static final String DEPOT_ROOT = "/depot/api";

    private static final Pattern LOOPBACK_ORIGIN = Pattern.compile("^http://(127\\.0\\.0\\.1|localhost)(:\\d+)?$");

    private final Sdlc sdlc;
    private final int port;
    private final List<String> allowedOrigins;

    public SdlcServer(Sdlc sdlc, int port, List<String> allowedOrigins) {
        this.sdlc = sdlc;
        this.port = port;
        this.allowedOrigins = List.copyOf(allowedOrigins);
    }

    /** What every request must pass before it reaches the rules; null when it may, else why not. */
    @com.legend.base.Nullable String refusal(HttpExchange exchange) {
        String host = exchange.getRequestHeaders().getFirst("Host");
        if (host == null || !(host.equals("127.0.0.1:" + port) || host.equals("localhost:" + port))) {
            return "this server answers only on its loopback address (Host 127.0.0.1:" + port + " or localhost:" + port + ")";
        }
        String origin = exchange.getRequestHeaders().getFirst("Origin");
        if (origin != null && !allowed(origin)) return "origin " + origin + " is not allowed (start the server with --allow-origin)";
        return null;
    }

    private boolean allowed(String origin) {
        return LOOPBACK_ORIGIN.matcher(origin).matches() || allowedOrigins.contains(origin);
    }

    private void cors(HttpExchange exchange) {
        String origin = exchange.getRequestHeaders().getFirst("Origin");
        if (origin == null || !allowed(origin)) return;
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", origin);
        exchange.getResponseHeaders().set("Vary", "Origin");
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
        exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type, Authorization");
    }

    /** One API under {@code root}, its requests checked, then answered by {@code answer} under the repository's lock. */
    private HttpHandler mount(String root, Answer answer) {
        return exchange -> {
            try (exchange) {
                String refused = refusal(exchange);
                if (refused != null) {
                    send(exchange, 403, "{\"code\":403,\"message\":" + com.legend.json.Json.toCompact(refused) + "}");
                    return;
                }
                cors(exchange);
                if (exchange.getRequestMethod().equals("OPTIONS")) {
                    exchange.sendResponseHeaders(204, -1);
                    return;
                }
                String raw = exchange.getRequestURI().getRawPath();
                String query = exchange.getRequestURI().getRawQuery();
                String target = raw.substring(root.length()) + (query == null ? "" : "?" + query);
                String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                Object[] r;
                synchronized (sdlc) {
                    r = answer.answer(exchange.getRequestMethod(), target, body.isEmpty() ? null : body);
                }
                send(exchange, (Integer) r[0], (String) r[1]);
            }
        };
    }

    private interface Answer {
        /** {status, body-or-null}. */
        Object[] answer(String method, String target, @com.legend.base.Nullable String body);
    }

    /** The SDLC's handler for {@link #ROOT}. */
    public HttpHandler handler() {
        return mount(ROOT, (m, t, b) -> {
            Sdlc.Response r = sdlc.handle(m, t, b);
            return new Object[] {r.status(), r.body()};
        });
    }

    /** Depot-lite's handler for {@link #DEPOT_ROOT}: Depot's rules over this SDLC's version tags (design S1). */
    public HttpHandler depotHandler(com.legend.depot.Depot depot) {
        return mount(DEPOT_ROOT, (m, t, b) -> {
            com.legend.depot.Depot.Response r = depot.handle(m, t, b);
            return new Object[] {r.status(), r.body()};
        });
    }

    static void send(HttpExchange exchange, int status, @com.legend.base.Nullable String body) throws IOException {
        if (body == null) {
            exchange.sendResponseHeaders(status, -1);
            return;
        }
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    public static void main(String[] args) throws IOException {
        int port = 6100;
        Path repo = null;
        String userId = "local";
        String userName = "Local User";
        List<String> origins = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--repo" -> repo = Path.of(args[++i]);
                case "--user" -> {
                    String u = args[++i];
                    int colon = u.indexOf(':');
                    userId = colon < 0 ? u : u.substring(0, colon);
                    userName = colon < 0 ? u : u.substring(colon + 1);
                }
                case "--allow-origin" -> origins.add(args[++i]);
                default -> throw new IllegalArgumentException("unknown argument: " + args[i]
                        + " (usage: --port N --repo DIR [--user id[:Name]] [--allow-origin URL]...)");
            }
        }
        if (repo == null) throw new IllegalArgumentException("--repo DIR is required: where the projects' git repository lives");
        Sdlc sdlc = new Sdlc(new GitStorage(repo), userId, userName, new CoreGrammar(), System::currentTimeMillis).backendType("git");
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        SdlcServer home = new SdlcServer(sdlc, port, origins);
        server.createContext(ROOT + "/", home.handler());
        server.createContext(DEPOT_ROOT + "/", home.depotHandler(new com.legend.depot.Depot(sdlc.artifacts(), System::currentTimeMillis)));
        server.createContext("/health", exchange -> {
            try (exchange) {
                send(exchange, 200, "{\"status\":\"ok\"}");
            }
        });
        server.start();
        System.out.println("model home: SDLC http://127.0.0.1:" + port + ROOT + ", Depot http://127.0.0.1:" + port + DEPOT_ROOT
                + ", over " + repo.toAbsolutePath());
    }
}
