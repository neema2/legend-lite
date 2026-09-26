package com.legend.warehouse.server;

import com.legend.Nullable;
import com.legend.server.Json;
import com.legend.warehouse.server.duck.ArrowStreams;
import com.legend.warehouse.server.duck.DuckException;
import com.legend.warehouse.server.duck.DuckLibrary;
import com.legend.warehouse.sqlapi.ApiJson;
import com.legend.warehouse.sqlapi.SqlApi;
import com.legend.warehouse.sqlapi.SqlApi.ApiError;
import com.legend.warehouse.sqlapi.SqlApi.Chunk;
import com.legend.warehouse.sqlapi.SqlApi.ErrorCode;
import com.legend.warehouse.sqlapi.SqlApi.StatementRequest;
import com.legend.warehouse.sqlapi.SqlApi.Token;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The warehouse: the HTTP SQL API (docs/WAREHOUSE_W1_DESIGN_2026_09_26.md
 * §3) in front of DuckDB.
 *
 * <p>W1 has no authorizer: every statement runs as its signed-in user, with
 * that user's identity set first. W2 adds grants, ACL views and the
 * authorizer to this same path.
 */
public final class WarehouseServer implements AutoCloseable {

    private static final Pattern STATEMENT = Pattern.compile("/sql/v1/statements/([0-9a-f-]{36})");
    private static final Pattern CHUNK = Pattern.compile("/sql/v1/statements/([0-9a-f-]{36})/chunks/(\\d{1,9})");
    private static final Pattern CANCEL = Pattern.compile("/sql/v1/statements/([0-9a-f-]{36})/cancel");
    private static final Pattern OBJECTS = Pattern.compile("/sql/v1/catalogs/([a-z][a-z0-9_]{0,62})/objects");
    private static final Pattern SESSION = Pattern.compile("/sql/v1/sessions/([0-9a-f-]{36})");
    private static final long MAX_WAIT_MS = 30_000;

    /** Everything the process is started with. */
    public record Config(
            int port,
            Path dataDir,
            List<String> catalogs,
            List<String[]> users,
            byte @Nullable [] tokenKey,
            Duration tokenLife,
            Statements.Limits limits,
            @Nullable Path duckdbLibrary) {

        /** DuckDB's library from the classpath (DuckDB's JDBC jar carries it). */
        public Config(int port, Path dataDir, List<String> catalogs, List<String[]> users,
                byte @Nullable [] tokenKey, Duration tokenLife, Statements.Limits limits) {
            this(port, dataDir, catalogs, users, tokenKey, tokenLife, limits, null);
        }
    }

    private final HttpServer http;
    private final Identity identity;
    private final Catalogs catalogs;
    private final History history;
    private final Sessions sessions;
    private final Statements statements;

    public WarehouseServer(Config config) throws IOException, DuckException {
        DuckLibrary.load(config.duckdbLibrary());
        Clock clock = Clock.systemUTC();
        byte @Nullable [] configured = config.tokenKey();
        byte[] key;
        if (configured != null) {
            key = configured;
        } else {
            key = new byte[32];
            new SecureRandom().nextBytes(key);
        }
        identity = new Identity(key, config.tokenLife(), clock);
        for (String[] u : config.users()) identity.addUser(u[0], u[1]);
        catalogs = new Catalogs(config.dataDir(), config.catalogs());
        history = new History(config.dataDir());
        sessions = new Sessions(catalogs, Duration.ofMinutes(30), clock);
        statements = new Statements(catalogs, sessions, history, config.limits(), clock);
        http = HttpServer.create(new InetSocketAddress("127.0.0.1", config.port()), 0);
        http.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        http.createContext("/", this::handle);
        http.start();
    }

    public int port() {
        return http.getAddress().getPort();
    }

    public History history() {
        return history;
    }

    // -- routing -----------------------------------------------------------

    private void handle(HttpExchange ex) throws IOException {
        try {
            route(ex);
        } catch (Reply r) {
            send(ex, r.status, r.contentType, r.bytes);
        } catch (RuntimeException e) {
            send(ex, 500, ApiJson.errorBody(new ApiError(ErrorCode.INTERNAL, String.valueOf(e.getMessage()))));
        } finally {
            ex.close();
        }
    }

    /** A response, thrown from anywhere in a handler. */
    private static final class Reply extends Exception {
        final int status;
        final String contentType;
        final byte[] bytes;

        Reply(int status, String body) {
            this(status, "application/json", body.getBytes(StandardCharsets.UTF_8));
        }

        Reply(int status, String contentType, byte[] bytes) {
            super(null, null, false, false);
            this.status = status;
            this.contentType = contentType;
            this.bytes = bytes;
        }

        static Reply error(int status, ErrorCode code, String message) {
            return new Reply(status, ApiJson.errorBody(new ApiError(code, message)));
        }
    }

    private void route(HttpExchange ex) throws IOException, Reply {
        String method = ex.getRequestMethod();
        String path = ex.getRequestURI().getPath();
        if (path.equals("/health")) throw new Reply(200, "{\"status\":\"ok\"}");
        if (path.equals("/sql/v1/login") && method.equals("POST")) {
            login(ex);
            return;
        }
        String principal = principal(ex);
        Matcher m;
        if (path.equals("/sql/v1/statements") && method.equals("POST")) {
            submit(ex, principal);
        } else if ((m = CHUNK.matcher(path)).matches() && method.equals("GET")) {
            chunk(principal, m.group(1), Integer.parseInt(m.group(2)));
        } else if ((m = CANCEL.matcher(path)).matches() && method.equals("POST")) {
            Statements.Run run = run(principal, m.group(1));
            statements.cancel(run);
            waitFor(run, 5_000);
            throw new Reply(200, Json.toCompact(ApiJson.status(run.status(false))));
        } else if ((m = STATEMENT.matcher(path)).matches() && method.equals("GET")) {
            Statements.Run run = run(principal, m.group(1));
            waitFor(run, waitMs(ex, 0));
            reply(run, false);
        } else if (path.equals("/sql/v1/sessions") && method.equals("POST")) {
            openSession(ex, principal);
        } else if ((m = SESSION.matcher(path)).matches() && method.equals("DELETE")) {
            Sessions.Session s = sessions.find(principal, m.group(1));
            if (s == null) throw Reply.error(404, ErrorCode.NOT_FOUND, "no session " + m.group(1));
            sessions.close(s);
            throw new Reply(200, Json.toCompact(ApiJson.session(s.api())));
        } else if (path.equals("/sql/v1/catalogs") && method.equals("GET")) {
            List<Json.Node> out = new ArrayList<>();
            for (String c : catalogs.names()) {
                LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
                f.put("name", Json.str(c));
                out.add(new Json.Obj(f));
            }
            throw new Reply(200, Json.toCompact(new Json.Arr(out)));
        } else if ((m = OBJECTS.matcher(path)).matches() && method.equals("GET")) {
            objects(principal, m.group(1));
        } else {
            throw Reply.error(404, ErrorCode.NOT_FOUND, method + " " + path);
        }
    }

    private void login(HttpExchange ex) throws IOException, Reply {
        Json.Obj o;
        try {
            o = Json.parseObject(body(ex));
        } catch (RuntimeException bad) {
            throw Reply.error(400, ErrorCode.BAD_REQUEST, "the body must be {\"user\", \"password\"}");
        }
        String user = o.getStringOr("user", null);
        String password = o.getStringOr("password", null);
        Identity.Issued issued = user == null || password == null ? null : identity.login(user, password);
        if (issued == null) throw Reply.error(401, ErrorCode.AUTH_INVALID, "wrong user or password");
        throw new Reply(200, Json.toCompact(ApiJson.token(
                new Token(issued.token(), issued.expires().toString(), issued.principal()))));
    }

    /** The verified principal, from the bearer token and nothing else. */
    private String principal(HttpExchange ex) throws Reply {
        String auth = ex.getRequestHeaders().getFirst("Authorization");
        if (auth == null || !auth.startsWith("Bearer ")) {
            throw Reply.error(401, ErrorCode.AUTH_REQUIRED, "a bearer token is required");
        }
        String p = identity.verify(auth.substring("Bearer ".length()).strip());
        if (p == null) throw Reply.error(401, ErrorCode.AUTH_INVALID, "the token is invalid or expired");
        return p;
    }

    private void openSession(HttpExchange ex, String principal) throws IOException, Reply {
        String catalog;
        try {
            String c = Json.parseObject(body(ex)).getStringOr("catalog", StatementRequest.DEFAULT_CATALOG);
            catalog = c == null ? StatementRequest.DEFAULT_CATALOG : c;
        } catch (RuntimeException bad) {
            throw Reply.error(400, ErrorCode.BAD_REQUEST, "the body must be {\"catalog\"}");
        }
        Sessions.Session s;
        try {
            s = Catalogs.validName(catalog) ? sessions.open(principal, catalog) : null;
        } catch (DuckException e) {
            throw Reply.error(500, ErrorCode.INTERNAL, String.valueOf(e.getMessage()));
        }
        if (s == null) throw Reply.error(404, ErrorCode.NOT_FOUND, "no catalog '" + catalog + "'");
        throw new Reply(200, Json.toCompact(ApiJson.session(s.api())));
    }

    private void submit(HttpExchange ex, String principal) throws IOException, Reply {
        StatementRequest req;
        try {
            req = ApiJson.parseStatementRequest(body(ex));
        } catch (RuntimeException bad) {
            throw Reply.error(400, ErrorCode.BAD_REQUEST, String.valueOf(bad.getMessage()));
        }
        String sessionId = req.sessionId();
        if (sessionId != null) {
            if (sessions.find(principal, sessionId) == null) {
                throw Reply.error(404, ErrorCode.NOT_FOUND, "no session " + sessionId);
            }
        } else if (!Catalogs.validName(req.catalog()) || !catalogs.names().contains(req.catalog())) {
            throw Reply.error(404, ErrorCode.NOT_FOUND, "no catalog '" + req.catalog() + "'");
        }
        Statements.Run run;
        try {
            run = statements.submit(principal, req);
        } catch (Statements.QueueFull full) {
            throw Reply.error(503, ErrorCode.QUEUE_FULL, String.valueOf(full.getMessage()));
        }
        waitFor(run, Math.min(Math.max(0, req.waitMs()), MAX_WAIT_MS));
        reply(run, true);
    }

    private void reply(Statements.Run run, boolean withFirstChunk) throws Reply {
        int status = run.state().done() ? 200 : 202;
        throw new Reply(status, Json.toCompact(ApiJson.status(run.status(withFirstChunk))));
    }

    private void chunk(String principal, String id, int index) throws Reply {
        Statements.Run run = run(principal, id);
        if (run.format() == SqlApi.ResultFormat.ARROW) {
            byte[] a = run.arrowChunk(index);
            if (a == null) throw Reply.error(404, ErrorCode.NOT_FOUND, "no chunk " + index + " for statement " + id);
            throw new Reply(200, ArrowStreams.MEDIA_TYPE, a);
        }
        Chunk c = run.chunk(index);
        if (c == null) throw Reply.error(404, ErrorCode.NOT_FOUND, "no chunk " + index + " for statement " + id);
        throw new Reply(200, Json.toCompact(ApiJson.chunk(c)));
    }

    private Statements.Run run(String principal, String id) throws Reply {
        Statements.Run run = statements.find(principal, id);
        if (run == null) throw Reply.error(404, ErrorCode.NOT_FOUND, "no statement " + id);
        return run;
    }

    private void objects(String principal, String catalog) throws Reply {
        // W1 lists everything in the catalog; W2 filters by the caller's grants.
        Statements.Run run;
        try {
            run = statements.submit(principal, new StatementRequest("""
                    SELECT c.schema_name AS schema, c.table_name AS name,
                           CASE WHEN v.view_name IS NULL THEN 'table' ELSE 'view' END AS kind,
                           c.column_name, c.data_type
                    FROM duckdb_columns() c
                    LEFT JOIN duckdb_views() v ON v.schema_name = c.schema_name AND v.view_name = c.table_name
                    WHERE NOT c.internal
                    ORDER BY 1, 2, c.column_index""", catalog, 30_000, 30_000, 1_000_000));
        } catch (Statements.QueueFull full) {
            throw Reply.error(503, ErrorCode.QUEUE_FULL, String.valueOf(full.getMessage()));
        }
        waitFor(run, 30_000);
        Chunk c = run.chunk(0);
        if (c == null) throw Reply.error(500, ErrorCode.INTERNAL, "the catalog could not be read");
        LinkedHashMap<String, LinkedHashMap<String, Json.Node>> byObject = new LinkedHashMap<>();
        LinkedHashMap<String, List<Json.Node>> columns = new LinkedHashMap<>();
        for (List<Json.Node> row : c.rows()) {
            String key = ((Json.Str) row.get(0)).value() + "." + ((Json.Str) row.get(1)).value();
            byObject.computeIfAbsent(key, k -> {
                LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
                f.put("schema", row.get(0));
                f.put("name", row.get(1));
                f.put("kind", row.get(2));
                return f;
            });
            LinkedHashMap<String, Json.Node> col = new LinkedHashMap<>();
            col.put("name", row.get(3));
            col.put("type", row.get(4));
            columns.computeIfAbsent(key, k -> new ArrayList<>()).add(new Json.Obj(col));
        }
        List<Json.Node> out = new ArrayList<>();
        for (var e : byObject.entrySet()) {
            e.getValue().put("columns", new Json.Arr(columns.getOrDefault(e.getKey(), List.of())));
            out.add(new Json.Obj(e.getValue()));
        }
        throw new Reply(200, Json.toCompact(new Json.Arr(out)));
    }

    // -- plumbing ------------------------------------------------------------

    private static void waitFor(Statements.Run run, long ms) {
        if (ms <= 0) return;
        try {
            run.done().get(Math.min(ms, MAX_WAIT_MS), TimeUnit.MILLISECONDS);
        } catch (TimeoutException stillRunning) {
            // answered as queued or running
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (java.util.concurrent.ExecutionException impossible) {
            // the future is only ever completed normally
        }
    }

    private static long waitMs(HttpExchange ex, long def) {
        String q = ex.getRequestURI().getQuery();
        if (q == null) return def;
        for (String part : q.split("&")) {
            if (part.startsWith("waitMs=")) {
                try {
                    return Long.parseLong(part.substring("waitMs=".length()));
                } catch (NumberFormatException bad) {
                    return def;
                }
            }
        }
        return def;
    }

    private static String body(HttpExchange ex) throws IOException {
        return new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    }

    private static void send(HttpExchange ex, int status, String body) throws IOException {
        send(ex, status, "application/json", body.getBytes(StandardCharsets.UTF_8));
    }

    private static void send(HttpExchange ex, int status, String contentType, byte[] b) throws IOException {
        ex.getResponseHeaders().set("Content-Type", contentType);
        ex.sendResponseHeaders(status, b.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(b);
        }
    }

    @Override
    public void close() {
        http.stop(0);
        statements.close();
        sessions.close();
        history.close();
        catalogs.close();
    }

    /**
     * {@code --port N --data DIR --catalog NAME... --user NAME:PASSWORD...
     * --concurrency N --queue N --duckdb-library FILE}.
     */
    public static void main(String[] args) throws Exception {
        int port = 8765;
        Path data = Path.of("warehouse-data");
        List<String> cats = new ArrayList<>();
        List<String[]> users = new ArrayList<>();
        int concurrency = 2;
        int queue = 100;
        Path library = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--data" -> data = Path.of(args[++i]);
                case "--catalog" -> cats.add(args[++i]);
                case "--user" -> users.add(args[++i].split(":", 2));
                case "--concurrency" -> concurrency = Integer.parseInt(args[++i]);
                case "--queue" -> queue = Integer.parseInt(args[++i]);
                case "--duckdb-library" -> library = Path.of(args[++i]);
                default -> throw new IllegalArgumentException("unknown argument " + args[i]);
            }
        }
        if (cats.isEmpty()) cats.add(StatementRequest.DEFAULT_CATALOG);
        WarehouseServer s = new WarehouseServer(new Config(port, data, cats, users, null, Duration.ofHours(1),
                new Statements.Limits(concurrency, queue, 10_000_000, Duration.ofMinutes(10)), library));
        System.err.println("warehouse listening on 127.0.0.1:" + s.port() + ", catalogs " + cats);
    }
}
