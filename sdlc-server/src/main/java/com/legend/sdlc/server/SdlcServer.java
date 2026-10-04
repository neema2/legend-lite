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

/**
 * SDLC-lite as a server (design S1, levels 1-2): {@link Sdlc}'s rules -- the same class the page runs
 * compiled to WebAssembly -- behind the JDK's HTTP server at {@code /sdlc/api}, over a {@link Storage}
 * (a git repository on disk, {@link GitStorage}). One request at a time through the rules (the
 * repository has one writer). Answers CORS so a page served from elsewhere (Studio) can call it.
 *
 * <pre>bazel run //sdlc-server:server -- --port 6100 --repo DIR [--user id[:Name]]</pre>
 */
public final class SdlcServer {
    public static final String ROOT = "/sdlc/api";

    private final Sdlc sdlc;

    public SdlcServer(Sdlc sdlc) {
        this.sdlc = sdlc;
    }

    /** The handler for {@link #ROOT}: mount it on any JDK server (the launcher mounts Depot-lite beside it). */
    public HttpHandler handler() {
        return exchange -> {
            try (exchange) {
                cors(exchange);
                if (exchange.getRequestMethod().equals("OPTIONS")) {
                    exchange.sendResponseHeaders(204, -1);
                    return;
                }
                String raw = exchange.getRequestURI().getRawPath();
                String query = exchange.getRequestURI().getRawQuery();
                String target = raw.substring(ROOT.length()) + (query == null ? "" : "?" + query);
                String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                Sdlc.Response r;
                synchronized (sdlc) {
                    r = sdlc.handle(exchange.getRequestMethod(), target, body.isEmpty() ? null : body);
                }
                send(exchange, r.status(), r.body());
            }
        };
    }

    static void cors(HttpExchange exchange) {
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
        exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type, Authorization");
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
                default -> throw new IllegalArgumentException("unknown argument: " + args[i]
                        + " (usage: --port N --repo DIR [--user id[:Name]])");
            }
        }
        if (repo == null) throw new IllegalArgumentException("--repo DIR is required: where the projects' git repository lives");
        Sdlc sdlc = new Sdlc(new GitStorage(repo), userId, userName, new CoreGrammar(), System::currentTimeMillis).backendType("git");
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        server.createContext(ROOT + "/", new SdlcServer(sdlc).handler());
        server.createContext("/health", exchange -> {
            try (exchange) {
                send(exchange, 200, "{\"status\":\"ok\"}");
            }
        });
        server.start();
        System.out.println("sdlc-server: http://127.0.0.1:" + port + ROOT + " over " + repo.toAbsolutePath());
    }
}
