package com.legend.warehouse.server;

import com.legend.base.Nullable;
import com.legend.json.Json;
import com.legend.warehouse.server.duck.ArrowStreams;
import com.legend.warehouse.server.duck.Database;
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
import java.util.Map;
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
            @Nullable Path duckdbLibrary,
            List<String> owners,
            List<String> allowedOrigins,
            Duration sessionLimit,
            Map<String, String> postgres,
            @Nullable Path duckdbExtensions,
            @Nullable Path site,
            @Nullable String singleUser) {

        public Config {
            if (singleUser != null && (!users.isEmpty() || !owners.isEmpty())) {
                throw new IllegalArgumentException("--single-user signs its one user in with the launch key:"
                        + " --user and --owner have no place beside it");
            }
        }

        /** DuckDB's library from the classpath (DuckDB's JDBC jar carries it). */
        public Config(int port, Path dataDir, List<String> catalogs, List<String[]> users,
                byte @Nullable [] tokenKey, Duration tokenLife, Statements.Limits limits) {
            this(port, dataDir, catalogs, users, tokenKey, tokenLife, limits, null, List.of(), List.of(),
                    Identity.DEFAULT_SESSION_LIMIT, Map.of(), null, null, null);
        }

        public Config withOwners(List<String> owners) {
            return new Config(port, dataDir, catalogs, users, tokenKey, tokenLife, limits, duckdbLibrary, owners,
                    allowedOrigins, sessionLimit, postgres, duckdbExtensions, site, singleUser);
        }

        /** The web pages (exact origins, e.g. {@code https://cube.example.com}) whose browsers may call this server. */
        public Config withAllowedOrigins(List<String> allowedOrigins) {
            return new Config(port, dataDir, catalogs, users, tokenKey, tokenLife, limits, duckdbLibrary, owners,
                    allowedOrigins, sessionLimit, postgres, duckdbExtensions, site, singleUser);
        }

        /** How long one sign-in may be kept alive by refreshing its token (`Identity.refresh`). */
        public Config withSessionLimit(Duration sessionLimit) {
            return new Config(port, dataDir, catalogs, users, tokenKey, tokenLife, limits, duckdbLibrary, owners,
                    allowedOrigins, sessionLimit, postgres, duckdbExtensions, site, singleUser);
        }

        /**
         * Postgres catalogs ({@link Postgres}), by name, each a libpq connection string, and the directory
         * holding DuckDB's {@code postgres_scanner.duckdb_extension}.
         */
        public Config withPostgres(Map<String, String> postgres, @Nullable Path duckdbExtensions) {
            return new Config(port, dataDir, catalogs, users, tokenKey, tokenLife, limits, duckdbLibrary, owners,
                    allowedOrigins, sessionLimit, Map.copyOf(postgres), duckdbExtensions, site, singleUser);
        }

        /** DuckDB's native library ({@code --duckdb-library}): what a server started in a JVM loads. */
        public Config withDuckdbLibrary(@Nullable Path duckdbLibrary) {
            return new Config(port, dataDir, catalogs, users, tokenKey, tokenLife, limits, duckdbLibrary, owners,
                    allowedOrigins, sessionLimit, postgres, duckdbExtensions, site, singleUser);
        }

        /** A DataCube page served from DIR for every GET outside the API, with {@code /config.json} naming this server. */
        public Config withSite(@Nullable Path site) {
            return new Config(port, dataDir, catalogs, users, tokenKey, tokenLife, limits, duckdbLibrary, owners,
                    allowedOrigins, sessionLimit, postgres, duckdbExtensions, site, singleUser);
        }

        /** One user, an owner, signed in by the launch key ({@link WarehouseServer#launchKey}) and no password. */
        public Config withSingleUser(@Nullable String singleUser) {
            return new Config(port, dataDir, catalogs, users, tokenKey, tokenLife, limits, duckdbLibrary, owners,
                    allowedOrigins, sessionLimit, postgres, duckdbExtensions, site, singleUser);
        }
    }

    /**
     * The command line: the server's {@link Config}, and what the launcher does once it runs. With
     * {@code --single-user} and no {@code --data}, the data directory is {@code temporaryData}: the app
     * keeps nothing between runs (no grants, results only while they are fetched), and removes it on exit.
     */
    public record CommandLine(Config config, boolean open, @Nullable String table, @Nullable Path temporaryData) {
    }

    private final HttpServer http;
    private final java.util.Set<String> allowedOrigins;
    private final Identity identity;
    private final Catalogs catalogs;
    private final Database system;
    private final History history;
    private final Grants grants;
    private final Sessions sessions;
    private final Statements statements;
    private final ResultStore results;
    private final @Nullable Path site;
    private final @Nullable String launchKey;

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
        identity = new Identity(key, config.tokenLife(), config.sessionLimit(), clock);
        for (String[] u : config.users()) {
            if (u[0].equalsIgnoreCase(Statements.SERVER)) throw new IllegalArgumentException("'" + u[0] + "' is the server's own name");
            identity.addUser(u[0], u[1]);
        }
        launchKey = config.singleUser() == null ? null : identity.launchKey(config.singleUser());
        List<String> owners = config.singleUser() == null ? config.owners() : List.of(config.singleUser());
        site = config.site();
        Map<String, Catalogs.Attach> attach = new LinkedHashMap<>();
        config.postgres().forEach((name, dsn) -> attach.put(name, new Catalogs.Attach(Attachment.POSTGRES, dsn)));
        catalogs = new Catalogs(config.dataDir(), config.catalogs(), attach, config.duckdbExtensions());
        system = Database.open(config.dataDir().resolve("system.duckdb"));
        system.lockDown(null);
        history = new History(system);
        grants = new Grants(system);
        sessions = new Sessions(catalogs, Duration.ofMinutes(30), clock);
        results = new ResultStore(config.dataDir().resolve("results"), config.limits().resultMemory());
        Authorizer authorizer;
        try (var c = system.connect(Statements.SERVER)) {
            authorizer = new Authorizer(grants, Authorizer.builtins(c));
        } catch (DuckException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("could not read DuckDB's functions", e);
        }
        statements = new Statements(catalogs, sessions, results, history,
                new Statements.Access(owners, grants, authorizer, identity::hasUser), config.limits(), clock);
        allowedOrigins = java.util.Set.copyOf(config.allowedOrigins());
        http = HttpServer.create(new InetSocketAddress("127.0.0.1", config.port()), 0);
        http.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        http.createContext("/", this::handle);
        http.start();
    }

    public int port() {
        return http.getAddress().getPort();
    }

    /** With {@code --single-user}, the key that signs its user in ({@link Identity#launchKey}); else null. */
    public @Nullable String launchKey() {
        return launchKey;
    }

    public History history() {
        return history;
    }

    // -- routing -----------------------------------------------------------

    /**
     * A browser lets a page read another origin's responses only when that origin says so (CORS).
     * An allowed origin's requests, errors included, carry its name back; its preflight is answered
     * here, before any route. Any other origin gets no CORS header at all, and the browser keeps the
     * response from the page. Exact origins only, never a wildcard; no cookies (tokens are bearer).
     */
    private boolean corsPreflight(HttpExchange ex) throws IOException {
        String origin = ex.getRequestHeaders().getFirst("Origin");
        boolean allowed = origin != null && allowedOrigins.contains(origin);
        if (allowed) {
            ex.getResponseHeaders().set("Access-Control-Allow-Origin", origin);
            ex.getResponseHeaders().add("Vary", "Origin");
        }
        if (!ex.getRequestMethod().equals("OPTIONS")
                || ex.getRequestHeaders().getFirst("Access-Control-Request-Method") == null) {
            return false;
        }
        if (allowed) {
            ex.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, DELETE");
            ex.getResponseHeaders().set("Access-Control-Allow-Headers", "Authorization, Content-Type");
            ex.getResponseHeaders().set("Access-Control-Max-Age", "600");
            ex.sendResponseHeaders(204, -1);
        } else {
            ex.sendResponseHeaders(403, -1);
        }
        return true;
    }

    private void handle(HttpExchange ex) throws IOException {
        try {
            if (corsPreflight(ex)) return;
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
        if (path.equals("/health")) {
            // finished results waiting to be fetched: bytes held in memory, and bytes spilled to files
            throw new Reply(200, "{\"status\":\"ok\",\"results\":{\"inMemoryBytes\":" + results.inMemory()
                    + ",\"spilledBytes\":" + results.spilled() + "}}");
        }
        if (site != null && method.equals("GET") && !path.startsWith("/sql/")) {
            site(ex, site, path);
            return;
        }
        if (path.equals("/sql/v1/login") && method.equals("POST")) {
            login(ex);
            return;
        }
        if (path.equals("/sql/v1/token/refresh") && method.equals("POST")) {
            refresh(ex);
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
            throw new Reply(200, Json.toCompact(ApiJson.status(run.status())));
        } else if ((m = STATEMENT.matcher(path)).matches() && method.equals("DELETE")) {
            // the client is done with the result: its memory and files go now, not at expiry
            Statements.Run run = run(principal, m.group(1));
            statements.forget(run);
            throw new Reply(200, "{\"statementId\":\"" + run.id() + "\",\"closed\":true}");
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
        } else if (path.equals("/sql/v1/history") && method.equals("GET")) {
            int limit = (int) Math.max(1, Math.min(1_000, queryLong(ex, "limit", 100)));
            try {
                throw new Reply(200, Json.toCompact(new Json.Arr(history.recent(principal, limit))));
            } catch (Reply r) {
                throw r;
            } catch (Exception e) {
                throw Reply.error(500, ErrorCode.INTERNAL, String.valueOf(e.getMessage()));
            }
        } else if (path.equals("/sql/v1/catalogs") && method.equals("GET")) {
            List<Json.Node> out = new ArrayList<>();
            for (String c : catalogs.names()) {
                LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
                f.put("name", Json.str(c));
                // the SQL a statement on it is written in
                f.put("databaseType", Json.str(catalogs.databaseType(c)));
                out.add(new Json.Obj(f));
            }
            throw new Reply(200, Json.toCompact(new Json.Arr(out)));
        } else if (path.equals("/sql/v1/objects") && method.equals("GET")) {
            allObjects(principal);
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
            throw Reply.error(400, ErrorCode.BAD_REQUEST, "the body must be {\"user\", \"password\"} or {\"key\"}");
        }
        String key = o.getStringOr("key", null);
        Identity.Issued issued;
        if (key != null) {
            issued = identity.loginWithKey(key);
            if (issued == null) throw Reply.error(401, ErrorCode.AUTH_INVALID, "wrong launch key");
        } else {
            String user = o.getStringOr("user", null);
            String password = o.getStringOr("password", null);
            issued = user == null || password == null ? null : identity.login(user, password);
            if (issued == null) throw Reply.error(401, ErrorCode.AUTH_INVALID, "wrong user or password");
        }
        throw new Reply(200, Json.toCompact(ApiJson.token(
                new Token(issued.token(), issued.expires().toString(), issued.principal()))));
    }

    /**
     * A still-valid token for a fresh one: same user, a new expiry, never past the sign-in's session
     * limit. An open page calls it before its token runs out, so nobody is asked for a password
     * mid-work; a token that is already invalid, or a session at its limit, must sign in again.
     */
    private void refresh(HttpExchange ex) throws Reply {
        String auth = ex.getRequestHeaders().getFirst("Authorization");
        if (auth == null || !auth.startsWith("Bearer ")) {
            throw Reply.error(401, ErrorCode.AUTH_REQUIRED, "a bearer token is required");
        }
        Identity.Issued issued = identity.refresh(auth.substring("Bearer ".length()).strip());
        if (issued == null) {
            throw Reply.error(401, ErrorCode.AUTH_INVALID,
                    "the token is invalid, expired, or its session has reached its limit: sign in again");
        }
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
        Attachment attachment = catalogs.attachment(catalog);
        if (attachment != null) {
            // a session keeps a DuckDB connection's state; SQL passed through to the attached database keeps none
            throw Reply.error(400, ErrorCode.BAD_REQUEST, catalog + " is attached to " + attachment.databaseType
                    + ": it has no sessions");
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
        String head = Json.toCompact(ApiJson.status(run.status()));
        byte[] first = withFirstChunk && run.format() == SqlApi.ResultFormat.JSON ? run.chunk(0) : null;
        if (first == null) throw new Reply(status, head);
        // the first chunk, spliced in as written: {..., "firstChunk": {"index": 0, "rows": [...]}}
        byte[] open = (head.substring(0, head.length() - 1) + ",\"firstChunk\":").getBytes(StandardCharsets.UTF_8);
        byte[] body = new byte[open.length + first.length + 1];
        System.arraycopy(open, 0, body, 0, open.length);
        System.arraycopy(first, 0, body, open.length, first.length);
        body[body.length - 1] = '}';
        throw new Reply(status, "application/json", body);
    }

    private void chunk(String principal, String id, int index) throws Reply {
        Statements.Run run = run(principal, id);
        if (run.format() == SqlApi.ResultFormat.ARROW) {
            byte[] a = run.chunk(index);
            if (a == null) throw Reply.error(404, ErrorCode.NOT_FOUND, "no chunk " + index + " for statement " + id);
            throw new Reply(200, ArrowStreams.MEDIA_TYPE, a);
        }
        byte[] c = run.chunk(index);
        if (c == null) throw Reply.error(404, ErrorCode.NOT_FOUND, "no chunk " + index + " for statement " + id);
        throw new Reply(200, "application/json", c);
    }

    private Statements.Run run(String principal, String id) throws Reply {
        Statements.Run run = statements.find(principal, id);
        if (run == null) throw Reply.error(404, ErrorCode.NOT_FOUND, "no statement " + id);
        return run;
    }

    /** {@code GET /sql/v1/catalogs/{catalog}/objects}: one catalog's; a reader without USAGE of an attached one is refused. */
    private void objects(String principal, String catalog) throws Reply {
        if (catalogs.attachment(catalog) != null && !mayUse(principal, catalog)) {
            throw Reply.error(403, ErrorCode.FORBIDDEN, "no USAGE granted on catalog " + catalog);
        }
        throw new Reply(200, Json.toCompact(new Json.Arr(objectsIn(principal, catalog))));
    }

    /**
     * {@code GET /sql/v1/objects}: what the caller may read in EVERY catalog, each object naming its catalog and
     * its database type ({@link Catalogs#databaseType}), so a client writes a model of any of them from one
     * listing. An attached catalog the caller holds no USAGE of is left out, not an error.
     */
    private void allObjects(String principal) throws Reply {
        List<Json.Node> out = new ArrayList<>();
        for (String catalog : catalogs.names()) {
            if (catalogs.attachment(catalog) != null && !mayUse(principal, catalog)) continue;
            out.addAll(objectsIn(principal, catalog));
        }
        throw new Reply(200, Json.toCompact(new Json.Arr(out)));
    }

    private boolean mayUse(String principal, String catalog) {
        return statements.isOwner(principal) || grants.canUseCatalog(grants.principals(principal), catalog);
    }

    /**
     * A catalog's tables and views with their columns, read by the server itself (a reader may not call
     * duckdb_columns()), then filtered to what the caller may see: an owner everything, a reader what is
     * granted to it. A native catalog answers from DuckDB's catalog; an ATTACHED one from its database's own
     * ({@link Attachment#catalogListing}: a Postgres table in Postgres's type names, so its model is written
     * by Postgres's rules), without that database's own schemas; its readers hold USAGE of the whole catalog
     * (the connection's role decides the rows, and the columns it may read).
     */
    private List<Json.Node> objectsIn(String principal, String catalog) throws Reply {
        Attachment attachment = catalogs.attachment(catalog);
        Statements.Run run;
        try {
            run = statements.submit(Statements.SERVER, new StatementRequest(attachment != null ? attachment.catalogListing() : """
                    SELECT c.schema_name AS schema, c.table_name AS name,
                           CASE WHEN v.view_name IS NULL THEN 'table' ELSE 'view' END AS kind,
                           c.column_name, c.data_type, t.logical_type, c.numeric_precision, c.numeric_scale,
                           NOT c.is_nullable AS not_null
                    FROM duckdb_columns() c
                    LEFT JOIN duckdb_views() v ON v.database_name = c.database_name
                         AND v.schema_name = c.schema_name AND v.view_name = c.table_name
                    LEFT JOIN (SELECT DISTINCT type_oid, logical_type FROM duckdb_types()
                               WHERE internal AND type_oid IS NOT NULL) t ON t.type_oid = c.data_type_id
                    WHERE NOT c.internal
                    ORDER BY 1, 2, c.column_index""", catalog, 30_000, 30_000, 1_000_000));
        } catch (Statements.QueueFull full) {
            throw Reply.error(503, ErrorCode.QUEUE_FULL, String.valueOf(full.getMessage()));
        }
        waitFor(run, 30_000);
        byte[] written = run.chunk(0);
        statements.forget(run);
        Chunk c = written == null ? null : ApiJson.parseChunk(new String(written, StandardCharsets.UTF_8));
        if (c == null) throw Reply.error(500, ErrorCode.INTERNAL, "the catalog could not be read");
        LinkedHashMap<String, LinkedHashMap<String, Json.Node>> byObject = new LinkedHashMap<>();
        LinkedHashMap<String, List<Json.Node>> columns = new LinkedHashMap<>();
        boolean owner = statements.isOwner(principal);
        java.util.Set<String> principals = grants.principals(principal);
        for (List<Json.Node> row : c.rows()) {
            String schema = ((Json.Str) row.get(0)).value(), name = ((Json.Str) row.get(1)).value();
            if (!owner && attachment == null && !grants.canSelect(principals, catalog, schema, name)) continue;
            String key = schema + "." + name;
            byObject.computeIfAbsent(key, k -> {
                LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
                f.put("catalog", Json.str(catalog));
                f.put("databaseType", Json.str(catalogs.databaseType(catalog)));
                f.put("schema", row.get(0));
                f.put("name", row.get(1));
                f.put("kind", row.get(2));
                return f;
            });
            LinkedHashMap<String, Json.Node> col = new LinkedHashMap<>();
            col.put("name", row.get(3));
            col.put("type", row.get(4));
            // STRUCTURED, as DuckDb.CATALOG_COLUMNS_SQL reads a table: the canonical type (joined on the
            // type id) and a DECIMAL's precision and scale -- a model is written from these, never by
            // parsing "type" (the user, 2026-10-01)
            col.put("logicalType", row.get(5));
            col.put("precision", row.get(6));
            col.put("scale", row.get(7));
            // a NOT NULL column is declared NOT NULL, so the compiler types it [1] (CatalogModel.Column)
            col.put("notNull", row.get(8));
            columns.computeIfAbsent(key, k -> new ArrayList<>()).add(new Json.Obj(col));
        }
        List<Json.Node> out = new ArrayList<>();
        for (var e : byObject.entrySet()) {
            e.getValue().put("columns", new Json.Arr(columns.getOrDefault(e.getKey(), List.of())));
            out.add(new Json.Obj(e.getValue()));
        }
        return out;
    }

    // -- the site --------------------------------------------------------------

    /** The media types the DataCube site is made of; anything else is served as bytes (RFC 9110 §8.3). */
    private static final Map<String, String> SITE_TYPES = Map.of(
            "html", "text/html; charset=utf-8",
            "js", "text/javascript; charset=utf-8",
            "mjs", "text/javascript; charset=utf-8",
            "css", "text/css; charset=utf-8",
            "json", "application/json",
            "wasm", "application/wasm",
            "pure", "text/plain; charset=utf-8",
            "svg", "image/svg+xml",
            "woff2", "font/woff2");

    /**
     * The DataCube page ({@code --site}): its files, and {@code /config.json} naming this server as the
     * page's warehouse, at the origin the browser used, so the page and the API are one origin and no
     * CORS is needed. Served to a loopback Host only: a page another site's name resolves here (DNS
     * rebinding) is refused.
     */
    private static void site(HttpExchange ex, Path root, String path) throws IOException, Reply {
        // every answer is a Reply, sent by handle()
        String host = ex.getRequestHeaders().getFirst("Host");
        if (host == null || !LOOPBACK_HOST.matcher(host).matches()) {
            throw Reply.error(403, ErrorCode.FORBIDDEN, "this page is served to 127.0.0.1 and localhost only");
        }
        if (path.equals("/config.json")) {
            LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
            f.put("warehouse", Json.str("http://" + host));
            throw new Reply(200, Json.toCompact(new Json.Obj(f)));
        }
        String relative = path.equals("/") ? "index.html" : path.substring(1);
        Path file = root.resolve(relative).normalize();
        if (relative.contains("\\") || List.of(relative.split("/")).contains("..") || !file.startsWith(root.normalize())
                || !java.nio.file.Files.isRegularFile(file)) {
            throw Reply.error(404, ErrorCode.NOT_FOUND, "GET " + path);
        }
        String name = file.getFileName().toString();
        String type = SITE_TYPES.get(name.substring(name.lastIndexOf('.') + 1));
        ex.getResponseHeaders().set("Cache-Control", "no-cache");
        throw new Reply(200, type == null ? "application/octet-stream" : type, java.nio.file.Files.readAllBytes(file));
    }

    private static final Pattern LOOPBACK_HOST = Pattern.compile("(127\\.0\\.0\\.1|localhost|\\[::1\\])(:\\d{1,5})?");

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
        return queryLong(ex, "waitMs", def);
    }

    /** A numeric query parameter, or {@code def} when absent or not a number. */
    private static long queryLong(HttpExchange ex, String name, long def) {
        String q = ex.getRequestURI().getQuery();
        if (q == null) return def;
        for (String part : q.split("&")) {
            if (part.startsWith(name + "=")) {
                try {
                    return Long.parseLong(part.substring(name.length() + 1));
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
        grants.close();
        system.close();
        catalogs.close();
    }

    /**
     * The token key kept in {@code file}: read when it is there, else made (32 random bytes) and written readable by
     * the owner only. A key kept across restarts keeps every issued token good across them.
     */
    public static byte[] tokenKey(Path file) throws IOException {
        if (java.nio.file.Files.exists(file)) {
            byte[] key = java.nio.file.Files.readAllBytes(file);
            if (key.length < 32) throw new IllegalArgumentException("the token key in " + file + " is under 32 bytes");
            return key;
        }
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        // owner-only where the file system has POSIX modes; elsewhere (Windows) the directory's ACL decides
        if (file.getFileSystem().supportedFileAttributeViews().contains("posix")) {
            java.nio.file.Files.createFile(file, java.nio.file.attribute.PosixFilePermissions.asFileAttribute(
                    java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")));
        } else {
            java.nio.file.Files.createFile(file);
        }
        java.nio.file.Files.write(file, key);
        return key;
    }

    /**
     * {@code --port N --data DIR --catalog NAME... --user NAME:PASSWORD...
     * --concurrency N --queue N --max-rows N --retain-minutes N --result-memory-mb N --duckdb-library FILE
     * --owner NAME... --allow-origin ORIGIN... --token-key-file FILE --token-minutes N --session-hours N}. An owner may
     * do anything; every other user is a reader (§3 of the server program). With {@code --token-key-file} tokens are
     * signed with the key in FILE (made, owner-only, when absent), so a restart does not sign everyone out.
     *
     * <p>{@code --postgres NAME=DSN...} adds a Postgres catalog ({@link Postgres}): DSN is a libpq connection
     * string, whose login role is what every user of the catalog reads as (give it SELECT only, and
     * {@code options='-c statement_timeout=<ms>'} as a backstop to the cancel). {@code --duckdb-extensions DIR}
     * is where {@code postgres_scanner.duckdb_extension} is: beside a native executable by default; on the JVM
     * it must be given. Without {@code --catalog}, the DuckDB catalog {@code main} is made, Postgres catalogs
     * or not: grants are managed from a DuckDB catalog.
     */
    public static void main(String[] args) throws Exception {
        try {
            start(args);
        } catch (IllegalArgumentException | Catalogs.AttachFailed e) {
            // the command line or a Postgres catalog: said in one line, not as a stack trace
            System.err.println("warehouse: " + e.getMessage());
            if (e instanceof Catalogs.AttachFailed f && f.missingPassword) {
                System.err.println("warehouse: put the password in ~/.pgpass or PGPASSWORD, or start it from a terminal");
            }
            System.exit(2);
        }
    }

    private static void start(String[] args) throws Exception {
        CommandLine command = commandLine(args);
        Config config = command.config();
        Path temporary = command.temporaryData();
        // the single-user app's temporary data: removed on exit, after the server has closed its DuckDB files, so
        // the removal does not depend on DuckDB opening them with delete sharing (on Windows; review of
        // neema2/legend-lite#14, 2026-10-03). Registered before the server starts, so a start that fails removes it too.
        java.util.concurrent.atomic.AtomicReference<WarehouseServer> running = new java.util.concurrent.atomic.AtomicReference<>();
        if (temporary != null) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                WarehouseServer open = running.get();
                try {
                    if (open != null) open.close();
                } finally {
                    deleteTree(temporary);
                }
            }));
        }
        WarehouseServer s;
        while (true) {
            try {
                s = new WarehouseServer(config);
                running.set(s);
                break;
            } catch (Catalogs.AttachFailed failed) {
                config = withPasswordAsked(config, failed);
            }
        }
        List<String> names = new ArrayList<>(config.catalogs());
        names.addAll(new java.util.TreeSet<>(config.postgres().keySet()));
        System.err.println("warehouse listening on 127.0.0.1:" + s.port() + ", catalogs " + names);
        if (config.site() != null && s.launchKey() != null) {
            // the page's address, with the key that signs it in: printed always, opened with --open
            String url = "http://127.0.0.1:" + s.port() + "/#key=" + s.launchKey()
                    + (command.table() == null ? "" : "&table=" + command.table());
            System.err.println("DataCube: " + url);
            System.err.println("Press Ctrl+C to stop.");
            if (command.open()) openBrowser(url);
        }
    }

    /**
     * A Postgres catalog libpq found no password for ({@code ~/.pgpass}, {@code PGPASSWORD}): asked once on
     * the terminal, as psql asks. Anything else, or no terminal to ask on, stops the server with libpq's words.
     */
    private static Config withPasswordAsked(Config config, Catalogs.AttachFailed failed) throws IOException {
        String dsn = config.postgres().get(failed.catalog);
        java.io.Console console = System.console();
        if (!failed.missingPassword || console == null || dsn == null || PostgresUrl.isUrl(dsn)) throw failed;
        char[] password = console.readPassword("%s password for %s: ", failed.kind.databaseType, failed.catalog);
        if (password == null) throw failed;
        LinkedHashMap<String, String> postgres = new LinkedHashMap<>(config.postgres());
        postgres.put(failed.catalog, PostgresUrl.withPassword(dsn, password));
        return config.withPostgres(postgres, config.duckdbExtensions());
    }

    private static void deleteTree(Path root) {
        try (var walk = java.nio.file.Files.walk(root)) {
            for (Path p : walk.sorted(java.util.Comparator.reverseOrder()).toList()) java.nio.file.Files.deleteIfExists(p);
        } catch (IOException e) {
            System.err.println("warehouse: could not remove " + root + ": " + e.getMessage());
        }
    }

    /** The default browser at {@code url}; the address is printed first, so a machine without one still has it. */
    private static void openBrowser(String url) {
        String os = System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT);
        List<String> command = os.contains("mac") ? List.of("open", url)
                : os.contains("windows") ? List.of("rundll32", "url.dll,FileProtocolHandler", url)
                : List.of("xdg-open", url);
        try {
            new ProcessBuilder(command).inheritIO().start();
        } catch (IOException e) {
            System.err.println("could not open a browser (" + e.getMessage() + "): open the address above");
        }
    }

    /** The command line, as a {@link Config} (see {@link #main}); throws IllegalArgumentException when it is wrong. */
    public static Config parse(String[] args) throws IOException {
        return commandLine(args).config();
    }

    /**
     * The command line: the server's Config and the launcher's {@code --open} and {@code --table}. Under
     * {@code bazel run}, a relative {@code --data} (its default {@code warehouse-data} among them) is where the
     * command was started ({@code BUILD_WORKING_DIRECTORY}): a launcher may start the server elsewhere, in its
     * runfiles folder (hermetic-launcher on Windows has no working-directory option), where {@code bazel clean}
     * would delete the data (review of neema2/legend-lite#14, 2026-10-03).
     */
    public static CommandLine commandLine(String[] args) throws IOException {
        String startedIn = System.getenv("BUILD_WORKING_DIRECTORY");
        return commandLine(args, startedIn == null || startedIn.isEmpty() ? null : Path.of(startedIn));
    }

    /** {@link #commandLine(String[])} with {@code BUILD_WORKING_DIRECTORY} given: null when not under {@code bazel run}. */
    static CommandLine commandLine(String[] args, @Nullable Path startedIn) throws IOException {
        int port = 8765;
        Path data = Path.of("warehouse-data");
        List<String> cats = new ArrayList<>();
        List<String[]> users = new ArrayList<>();
        int concurrency = 2;
        int queue = 100;
        long maxRows = 10_000_000;
        long retainMinutes = 10;
        long resultMemoryMb = Statements.Limits.DEFAULT_RESULT_MEMORY >> 20;
        Path library = null;
        List<String> owners = new ArrayList<>();
        List<String> origins = new ArrayList<>();
        Path tokenKeyFile = null;
        long tokenMinutes = 60;
        long sessionHours = Identity.DEFAULT_SESSION_LIMIT.toHours();
        LinkedHashMap<String, String> postgres = new LinkedHashMap<>();
        Path extensions = null;
        Path site = null;
        boolean dataGiven = false;
        boolean singleUser = false;
        boolean open = false;
        String table = null;
        for (int i = 0; i < args.length; i++) {
            if (PostgresUrl.isUrl(args[i])) {
                PostgresUrl url = PostgresUrl.parse(args[i]);
                if (postgres.put(url.catalog(), url.dsn()) != null) {
                    throw new IllegalArgumentException("catalog " + url.catalog() + " is named twice");
                }
                continue;
            }
            if (args[i].equals("--single-user")) {
                singleUser = true;
                continue;
            }
            if (args[i].equals("--open")) {
                open = true;
                continue;
            }
            if (i + 1 >= args.length) throw new IllegalArgumentException(args[i] + " needs a value");
            switch (args[i]) {
                case "--site" -> site = named(args[++i], startedIn);
                case "--table" -> table = args[++i];
                case "--postgres" -> {
                    String[] kv = args[++i].split("=", 2);
                    if (kv.length != 2 || kv[1].isBlank()) {
                        throw new IllegalArgumentException(
                                "--postgres takes NAME=DSN, e.g. sales='host=db dbname=sales user=reader'");
                    }
                    if (postgres.put(kv[0], kv[1]) != null) {
                        throw new IllegalArgumentException("catalog " + kv[0] + " is named twice");
                    }
                }
                case "--duckdb-extensions" -> extensions = directoryOf(named(args[++i], startedIn));
                case "--port" -> port = intArgument("--port", args[++i]);
                case "--data" -> {
                    data = Path.of(args[++i]);
                    dataGiven = true;
                }
                case "--catalog" -> cats.add(args[++i]);
                case "--user" -> users.add(args[++i].split(":", 2));
                case "--concurrency" -> concurrency = intArgument("--concurrency", args[++i]);
                case "--queue" -> queue = intArgument("--queue", args[++i]);
                case "--duckdb-library" -> library = named(args[++i], startedIn);
                case "--owner" -> owners.add(args[++i]);
                case "--allow-origin" -> origins.add(args[++i]);
                case "--max-rows" -> maxRows = longArgument("--max-rows", args[++i]);
                case "--retain-minutes" -> retainMinutes = longArgument("--retain-minutes", args[++i]);
                case "--result-memory-mb" -> resultMemoryMb = longArgument("--result-memory-mb", args[++i]);
                case "--token-key-file" -> tokenKeyFile = callers(args[++i], startedIn);
                case "--token-minutes" -> tokenMinutes = longArgument("--token-minutes", args[++i]);
                case "--session-hours" -> sessionHours = longArgument("--session-hours", args[++i]);
                default -> throw new IllegalArgumentException("unknown argument " + args[i]);
            }
        }
        if (startedIn != null && !data.isAbsolute()) data = startedIn.resolve(data);
        if (cats.isEmpty()) cats.add(StatementRequest.DEFAULT_CATALOG);
        for (String n : postgres.keySet()) {
            if (!Catalogs.validName(n)) throw new IllegalArgumentException("bad catalog name: " + n);
            if (cats.contains(n)) throw new IllegalArgumentException("catalog " + n + " is named twice");
        }
        // Started by Bazel (bazel run, a test's data, bazel-bin), the server finds DuckDB's library and the postgres
        // extension in its own runfiles, where //warehouse:duckdb_library and //warehouse:duckdb_extensions put them
        // (Bazel workplan P1-16): never extracted to a temporary directory.
        if (library == null) {
            library = ServerRunfiles.rlocation(ServerRunfiles.repository() + "/warehouse/" + DuckLibrary.resourceName());
        }
        if (extensions == null) {
            Path inRunfiles = ServerRunfiles.rlocation(ServerRunfiles.repository()
                    + "/warehouse/duckdb_extensions/" + Attachment.POSTGRES.extensionFile);
            if (inRunfiles != null) extensions = inRunfiles.getParent();
        }
        if (!postgres.isEmpty() && extensions == null) {
            if (!DuckLibrary.nativeImage()) {
                throw new IllegalArgumentException("a Postgres catalog needs --duckdb-extensions DIR"
                        + " (the directory holding " + Attachment.POSTGRES.extensionFile + ")");
            }
            extensions = DuckLibrary.executableDir("--duckdb-extensions");
        }
        String user = null;
        if (singleUser) {
            // the warehouse's one principal is the account running it; each Postgres catalog connects as its own user
            user = Identity.accountPrincipal(System.getProperty("user.name", ""));
        }
        if (open && (site == null || !singleUser)) {
            throw new IllegalArgumentException("--open opens the page: it needs --site and --single-user");
        }
        if (table != null && (site == null || !singleUser || !TABLE.matcher(table).matches())) {
            throw new IllegalArgumentException("--table takes schema.name, with --site and --single-user");
        }
        Path temporaryData = null;
        if (singleUser && !dataGiven) {
            temporaryData = java.nio.file.Files.createTempDirectory("datacube-");
            data = temporaryData;
        }
        Config config = new Config(port, data, cats, users,
                tokenKeyFile == null ? null : tokenKey(tokenKeyFile), Duration.ofMinutes(tokenMinutes),
                new Statements.Limits(concurrency, queue, maxRows, Duration.ofMinutes(retainMinutes), resultMemoryMb << 20),
                library, owners, origins, Duration.ofHours(sessionHours), Map.copyOf(postgres), extensions, site, user);
        return new CommandLine(config, open, table, temporaryData);
    }

    /**
     * A file or directory the command line names: absolute as given; relative, where {@code bazel run} was started
     * ({@code BUILD_WORKING_DIRECTORY}) when it is there, else a runfiles path ({@code $(rlocationpath)} in a target's
     * {@code args}) found in this process's runfiles (Bazel workplan P1-16).
     */
    static Path named(String value, @Nullable Path startedIn) {
        Path callers = callers(value, startedIn);
        if (Path.of(value).isAbsolute() || java.nio.file.Files.exists(callers)) return callers;
        Path runfile = ServerRunfiles.rlocation(value.replace('\\', '/'));
        return runfile != null ? runfile : callers;
    }

    /** A path the caller wrote: relative to where {@code bazel run} was started, when it was. */
    static Path callers(String value, @Nullable Path startedIn) {
        Path p = Path.of(value);
        return startedIn == null || p.isAbsolute() ? p : startedIn.resolve(p);
    }

    /** {@code --duckdb-extensions} names the directory, or the extension file in it ($(rlocationpath) of a file). */
    private static Path directoryOf(Path p) {
        Path parent = p.getParent();
        return java.nio.file.Files.isRegularFile(p) && parent != null ? parent : p;
    }

    /** {@code --table}'s schema.name: what the page's address carries, so nothing that needs escaping there. */
    private static final Pattern TABLE = Pattern.compile("[A-Za-z_][A-Za-z0-9_$]*\\.[A-Za-z_][A-Za-z0-9_$]*");

    /** A numeric argument, or the server's own refusal naming it (not the JDK's NumberFormatException text). */
    static int intArgument(String flag, String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(flag + " takes a number, not '" + value + "'");
        }
    }

    static long longArgument(String flag, String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(flag + " takes a number, not '" + value + "'");
        }
    }
}
