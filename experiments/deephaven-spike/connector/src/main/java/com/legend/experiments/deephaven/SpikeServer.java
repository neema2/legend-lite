package com.legend.experiments.deephaven;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Stands in for legend-lite's server in the spike: it serves the page, and on request runs a plan
 * on Deephaven and answers the handle a viewer subscribes to. It is never on the tick path: the
 * browser subscribes to Deephaven directly.
 *
 * <pre>
 *   GET  /api/config             the Deephaven URL, the viewer's login, the plan names
 *   POST /api/live?plan=NAME     run the plan once (or reuse the live table already built for it)
 *   POST /api/release?ticket=ID  release this session's hold on a live table
 *   GET  /jsapi/*                Deephaven's JS API, proxied so the page can load it same-origin
 *   GET  /*                      the page (experiments/deephaven-spike/web)
 * </pre>
 */
public final class SpikeServer {

    private static final Gson GSON = new Gson();
    private static final String RUN = java.util.UUID.randomUUID().toString().substring(0, 8);

    private final DeephavenConnector connector;
    private final String deephavenUrl;
    private final String psk;
    private final Path web;
    private final Map<String, JsonObject> plans = new TreeMap<>();
    /** One live table per plan text: identical plans share one maintained table on the server. */
    private final Map<String, Map<String, Object>> liveByPlan = new ConcurrentHashMap<>();
    private final Map<String, DeephavenConnector.Live> liveByTicket = new ConcurrentHashMap<>();
    private final AtomicInteger sequence = new AtomicInteger();
    private final HttpClient http = HttpClient.newBuilder().proxy(HttpClient.Builder.NO_PROXY).build();

    private SpikeServer(DeephavenConnector connector, String deephavenUrl, String psk, Path plansDir, Path web)
            throws IOException {
        this.connector = connector;
        this.deephavenUrl = deephavenUrl;
        this.psk = psk;
        this.web = web;
        try (var files = Files.list(plansDir)) {
            for (Path p : (Iterable<Path>) files.filter(f -> f.toString().endsWith(".json"))::iterator) {
                JsonObject doc = GSON.fromJson(Files.readString(p), JsonObject.class);
                plans.put(doc.get("name").getAsString(), doc);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> opt = new LinkedHashMap<>(Map.of(
                "--deephaven-host", "localhost", "--deephaven-port", "10000", "--psk", "spike-psk",
                "--port", "8090", "--plans", "plans", "--web", "web"));
        for (int i = 0; i + 1 < args.length; i += 2) {
            if (!opt.containsKey(args[i])) {
                throw new IllegalArgumentException("unknown option " + args[i] + "; options: " + opt.keySet());
            }
            opt.put(args[i], args[i + 1]);
        }
        String host = opt.get("--deephaven-host");
        int dhPort = Integer.parseInt(opt.get("--deephaven-port"));
        DeephavenConnector connector = DeephavenConnector.open(host, dhPort, opt.get("--psk"));
        SpikeServer s = new SpikeServer(connector, "http://" + host + ":" + dhPort, opt.get("--psk"),
                Path.of(opt.get("--plans")), Path.of(opt.get("--web")));
        HttpServer server = HttpServer.create(new InetSocketAddress(Integer.parseInt(opt.get("--port"))), 0);
        server.createContext("/api/config", s::config);
        server.createContext("/api/live", s::live);
        server.createContext("/api/release", s::release);
        server.createContext("/jsapi/", s::jsapi);
        server.createContext("/", s::file);
        server.setExecutor(Executors.newFixedThreadPool(8));
        server.start();
        System.out.println("deephaven-spike connector on :" + opt.get("--port") + ", plans " + s.plans.keySet());
    }

    private void config(HttpExchange ex) throws IOException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("deephaven", deephavenUrl);
        // The spike's one shortcut: the viewer logs in with the server's PSK. A real deployment logs the
        // viewer in as themselves (OIDC, or a token this server issues and a Deephaven auth handler checks).
        body.put("login", Map.of("type", "io.deephaven.authentication.psk.PskAuthenticationHandler", "token", psk));
        body.put("plans", plans.keySet());
        json(ex, 200, body);
    }

    /** Synchronized: two first requests for one plan build one live table, not two. */
    private synchronized void live(HttpExchange ex) throws IOException {
        Map<String, String> q = query(ex);
        JsonObject doc = plans.get(q.getOrDefault("plan", ""));
        if (doc == null) {
            json(ex, 404, Map.of("error", "no plan '" + q.get("plan") + "'; plans: " + plans.keySet()));
            return;
        }
        String batchJson = doc.get("batch").toString();
        boolean fresh = "1".equals(q.get("fresh"));
        try {
            Map<String, Object> handle = fresh ? null : liveByPlan.get(batchJson);
            boolean reused = handle != null;
            if (handle == null) {
                BatchTableRequest plan = DeephavenConnector.parsePlan(batchJson);
                // Unique across connector processes: a shared id lives as long as the session that
                // published it, and a connector that dies leaves its session until the server expires it.
                String sharedId = "legend-lite/" + RUN + "/" + doc.get("name").getAsString() + "/"
                        + sequence.incrementAndGet();
                DeephavenConnector.Live live = connector.publish(plan, sharedId);
                liveByTicket.put(sharedId, live);
                handle = new LinkedHashMap<>();
                handle.put("sharedTicket", sharedId);
                handle.put("plan", doc.get("name").getAsString());
                handle.put("shape", doc.get("shape").getAsString());
                handle.put("pure", doc.get("pure").getAsString());
                handle.put("names", GSON.fromJson(doc.get("names"), Map.class));
                handle.put("operations", plan.getOpsCount());
                handle.put("publishMillis", live.publishMillis());
                handle.put("isStatic", live.isStatic());
                handle.put("sizeAtPublish", live.size());
                if (!fresh) {
                    liveByPlan.put(batchJson, handle);
                }
            }
            Map<String, Object> answer = new LinkedHashMap<>(handle);
            answer.put("reused", reused);
            json(ex, 200, answer);
        } catch (RuntimeException e) {
            json(ex, 500, Map.of("error", String.valueOf(e.getMessage())));
        }
    }

    private void release(HttpExchange ex) throws IOException {
        String ticket = query(ex).getOrDefault("ticket", "");
        DeephavenConnector.Live live = liveByTicket.remove(ticket);
        if (live == null) {
            json(ex, 404, Map.of("error", "no live table '" + ticket + "'"));
            return;
        }
        liveByPlan.values().removeIf(h -> ticket.equals(h.get("sharedTicket")));
        connector.release(live);
        json(ex, 200, Map.of("released", ticket));
    }

    private void jsapi(HttpExchange ex) throws IOException {
        try {
            HttpResponse<byte[]> r = http.send(
                    HttpRequest.newBuilder(URI.create(deephavenUrl + ex.getRequestURI())).GET().build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            r.headers().firstValue("content-type").ifPresent(v -> ex.getResponseHeaders().set("Content-Type", v));
            send(ex, r.statusCode(), r.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            send(ex, 502, new byte[0]);
        }
    }

    private void file(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath();
        Path f = web.resolve(path.equals("/") ? "index.html" : path.substring(1)).normalize();
        if (!f.startsWith(web.normalize()) || !Files.isRegularFile(f)) {
            send(ex, 404, "not found".getBytes(StandardCharsets.UTF_8));
            return;
        }
        String name = f.getFileName().toString();
        ex.getResponseHeaders().set("Content-Type", name.endsWith(".html") ? "text/html; charset=utf-8"
                : name.endsWith(".js") ? "text/javascript; charset=utf-8"
                : name.endsWith(".css") ? "text/css; charset=utf-8" : "application/octet-stream");
        send(ex, 200, Files.readAllBytes(f));
    }

    private static Map<String, String> query(HttpExchange ex) {
        Map<String, String> q = new LinkedHashMap<>();
        String raw = ex.getRequestURI().getRawQuery();
        if (raw != null) {
            for (String kv : raw.split("&")) {
                int eq = kv.indexOf('=');
                if (eq > 0) {
                    q.put(URLDecoder.decode(kv.substring(0, eq), StandardCharsets.UTF_8),
                            URLDecoder.decode(kv.substring(eq + 1), StandardCharsets.UTF_8));
                }
            }
        }
        return q;
    }

    private static void json(HttpExchange ex, int status, Object body) throws IOException {
        ex.getResponseHeaders().set("Content-Type", "application/json");
        send(ex, status, GSON.toJson(body).getBytes(StandardCharsets.UTF_8));
    }

    private static void send(HttpExchange ex, int status, byte[] body) throws IOException {
        ex.getResponseHeaders().set("Cache-Control", "no-store");
        ex.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(body);
        }
    }
}
