package com.legend.warehouse.sqlapi;

import com.legend.base.Nullable;
import com.legend.json.Json;
import com.legend.warehouse.sqlapi.SqlApi.ApiError;
import com.legend.warehouse.sqlapi.SqlApi.Chunk;
import com.legend.warehouse.sqlapi.SqlApi.Column;
import com.legend.warehouse.sqlapi.SqlApi.ErrorCode;
import com.legend.warehouse.sqlapi.SqlApi.ResultMeta;
import com.legend.warehouse.sqlapi.SqlApi.State;
import com.legend.warehouse.sqlapi.SqlApi.Status;
import com.legend.warehouse.sqlapi.SqlApi.StatementRequest;
import com.legend.warehouse.sqlapi.SqlApi.Token;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * The API's records on the wire, both ways: the ONE codec the server and
 * every client share, so the two cannot drift. Built on core's strict
 * RFC 8259 parser.
 *
 * <p>An answer's reader checks every value's kind before it reads it (the
 * shape helpers at the end), and refuses an answer that is not the API's
 * with an {@link IllegalArgumentException}: the readers run in WebAssembly
 * too (the page's module, {@code SqlApiCalls}), where a failed cast is a trap,
 * not an exception.
 */
public final class ApiJson {

    private ApiJson() {
    }

    // -- requests ------------------------------------------------------------

    public static String statementRequest(StatementRequest r) {
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("sql", Json.str(r.sql()));
        f.put("catalog", Json.str(r.catalog()));
        f.put("timeoutMs", Json.num(r.timeoutMs()));
        f.put("waitMs", Json.num(r.waitMs()));
        f.put("rowsPerChunk", Json.num(r.rowsPerChunk()));
        String session = r.sessionId();
        if (session != null) f.put("sessionId", Json.str(session));
        if (r.describeOnly()) f.put("describeOnly", Json.bool(true));
        if (r.format() != SqlApi.ResultFormat.JSON) f.put("resultFormat", Json.str(r.format().wire()));
        if (r.cellText()) f.put("cellText", Json.bool(true));
        if (!r.parameters().isEmpty()) f.put("parameters", parametersJson(r.parameters()));
        return Json.toCompact(new Json.Obj(f));
    }

    /** A statement's parameters as JSON: each its JSON value, a whole number its exact digits, a double its own token. */
    public static Json.Arr parametersJson(List<@com.legend.base.Nullable Object> values) {
        List<Json.Node> out = new java.util.ArrayList<>(values.size());
        for (Object v : values) {
            out.add(v == null ? Json.Null.INSTANCE : v instanceof String s ? Json.str(s)
                    : v instanceof Long l ? Json.Num.ofLong(l) : v instanceof Double d ? doubleJson(d)
                    : v instanceof Boolean b ? Json.bool(b) : refusedValue(v));
        }
        return new Json.Arr(out);
    }

    /**
     * A double parameter as its Java token ({@link Double#toString}: always a {@code .} or an exponent), which
     * {@link #parameters} reads back as a double -- never as JSON's writer prints a whole double, its digits alone
     * ({@code 2.0} as {@code 2}, {@code -0.0} as {@code 0}), which reads back as a whole number and binds as a BIGINT
     * where DuckDB's own driver binds the same {@code Double} as a DOUBLE (track Q2, 2026-11-19: measured by
     * {@code WarehouseJdbcParametersTest}).
     */
    private static Json.Num doubleJson(double d) {
        if (Double.isNaN(d) || Double.isInfinite(d)) {
            throw new IllegalArgumentException("a parameter of " + d + ", which JSON cannot carry");
        }
        return new Json.Num((long) d, d, false, null, Double.toString(d));
    }

    private static Json.Node refusedValue(Object v) {
        throw new IllegalArgumentException("a parameter of type " + v.getClass().getSimpleName()
                + ", which the warehouse does not bind");
    }

    /**
     * A statement's parameters read from JSON, in order: a string, a whole number (64-bit; one past it refused by
     * name), any other number a double, a boolean, or null. An object or an array is refused by name: the warehouse
     * binds scalars alone (lite's API v1 §4; the orchestrator's ruling (a), 2026-11-17).
     */
    public static List<@com.legend.base.Nullable Object> parameters(Json.Node json) {
        if (!(json instanceof Json.Arr arr)) {
            throw new IllegalArgumentException("'parameters' must be an array of the values the statement binds");
        }
        List<@com.legend.base.Nullable Object> out = new java.util.ArrayList<>(arr.items().size());
        int at = 0;
        for (Json.Node n : arr.items()) {
            at++;
            if (n instanceof Json.Null) {
                out.add(null);
            } else if (n instanceof Json.Str s) {
                out.add(s.value());
            } else if (n instanceof Json.Bool b) {
                out.add(b.value());
            } else if (n instanceof Json.Num num) {
                out.add(number(num, at));
            } else {
                throw new IllegalArgumentException("parameter " + at + " is " + (n instanceof Json.Obj ? "an object"
                        : "an array") + ", which the warehouse does not bind");
            }
        }
        return out;
    }

    private static Object number(Json.Num num, int at) {
        if (num.isInteger()) {
            return num.longValue();
        }
        java.math.BigDecimal exact = num.decimalValue();
        if (exact == null) {
            return num.doubleValue();
        }
        String token = num.token();
        boolean whole = token == null ? exact.scale() <= 0
                : token.indexOf('.') < 0 && token.indexOf('e') < 0 && token.indexOf('E') < 0;
        if (whole) {
            throw new IllegalArgumentException("parameter " + at + " is the whole number " + exact.toPlainString()
                    + ", past the 64 bits the warehouse binds a whole number in");
        }
        return num.doubleValue();
    }

    /** A request body, with the design's defaults for what it leaves out. */
    public static StatementRequest parseStatementRequest(String body) {
        Json.Obj o = Json.parseObject(body);
        if (!o.has("sql") || !(o.get("sql") instanceof Json.Str)) {
            throw new IllegalArgumentException("'sql' is required and must be a string");
        }
        String catalog = o.getStringOr("catalog", StatementRequest.DEFAULT_CATALOG);
        String format = o.getStringOr("resultFormat", null);
        return new StatementRequest(
                o.getString("sql"),
                catalog == null ? StatementRequest.DEFAULT_CATALOG : catalog,
                o.getLongOr("timeoutMs", StatementRequest.DEFAULT_TIMEOUT_MS),
                o.getLongOr("waitMs", StatementRequest.DEFAULT_WAIT_MS),
                o.getIntOr("rowsPerChunk", StatementRequest.DEFAULT_ROWS_PER_CHUNK),
                o.getStringOr("sessionId", null),
                o.getBoolOr("describeOnly", false),
                format == null ? SqlApi.ResultFormat.JSON : SqlApi.ResultFormat.ofWire(format),
                o.getBoolOr("cellText", false),
                o.has("parameters") ? parameters(o.get("parameters")) : List.of());
    }

    public static String openSession(String catalog) {
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("catalog", Json.str(catalog));
        return Json.toCompact(new Json.Obj(f));
    }

    public static Json.Obj session(SqlApi.Session s) {
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("sessionId", Json.str(s.sessionId()));
        f.put("catalog", Json.str(s.catalog()));
        f.put("engine", Json.str(s.engine()));
        f.put("engineVersion", Json.str(s.engineVersion()));
        return new Json.Obj(f);
    }

    public static SqlApi.Session parseSession(String body) {
        Json.Obj o = Json.parseObject(body);
        return new SqlApi.Session(str(o, "sessionId"), str(o, "catalog"), str(o, "engine"), str(o, "engineVersion"));
    }

    public static String login(String user, String password) {
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("user", Json.str(user));
        f.put("password", Json.str(password));
        return Json.toCompact(new Json.Obj(f));
    }

    /** The single-user app's sign-in: the launch key the warehouse printed (docs/DATACUBE_APP_PLAN_2026_10_02.md). */
    public static String loginKey(String key) {
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("key", Json.str(key));
        return Json.toCompact(new Json.Obj(f));
    }

    // -- responses -----------------------------------------------------------

    public static Json.Obj token(Token t) {
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("token", Json.str(t.token()));
        f.put("expiresAt", Json.str(t.expiresAt()));
        f.put("principal", Json.str(t.principal()));
        return new Json.Obj(f);
    }

    public static Token parseToken(String body) {
        Json.Obj o = Json.parseObject(body);
        return new Token(str(o, "token"), str(o, "expiresAt"), str(o, "principal"));
    }

    public static Json.Obj error(ApiError e) {
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("code", Json.str(e.code().name()));
        f.put("message", Json.str(e.message()));
        return new Json.Obj(f);
    }

    public static ApiError parseError(Json.Obj o) {
        return new ApiError(ErrorCode.valueOf(str(o, "code")), str(o, "message"));
    }

    /** An error response body: {@code {"error": {...}}}. */
    public static String errorBody(ApiError e) {
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("error", error(e));
        return Json.toCompact(new Json.Obj(f));
    }

    public static Json.Obj status(Status s) {
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("statementId", Json.str(s.statementId()));
        f.put("state", Json.str(s.state().wire()));
        ResultMeta r = s.result();
        if (r != null) f.put("result", resultMeta(r));
        ApiError e = s.error();
        if (e != null) f.put("error", error(e));
        Chunk c = s.firstChunk();
        if (c != null) f.put("firstChunk", chunk(c));
        return new Json.Obj(f);
    }

    public static Status parseStatus(String body) {
        Json.Obj o = Json.parseObject(body);
        Json.Obj r = objOr(o, "result");
        Json.Obj e = objOr(o, "error");
        Json.Obj c = objOr(o, "firstChunk");
        return new Status(
                str(o, "statementId"),
                State.ofWire(str(o, "state")),
                r == null ? null : parseResultMeta(r),
                e == null ? null : parseError(e),
                c == null ? null : parseChunk(c));
    }

    public static Json.Obj resultMeta(ResultMeta r) {
        List<Json.Node> cols = new ArrayList<>();
        for (Column c : r.columns()) {
            LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
            f.put("name", Json.str(c.name()));
            f.put("type", Json.str(c.type()));
            f.put("nullable", Json.bool(c.nullable()));
            cols.add(new Json.Obj(f));
        }
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("columns", new Json.Arr(cols));
        f.put("rowCount", Json.num(r.rowCount()));
        f.put("chunkCount", Json.num(r.chunkCount()));
        // a describe's alone (ResultMeta.parameterCount): a run's result is written as it always was
        Integer parameters = r.parameterCount();
        if (parameters != null) f.put("parameterCount", Json.num(parameters));
        return new Json.Obj(f);
    }

    public static ResultMeta parseResultMeta(Json.Obj o) {
        List<Column> cols = new ArrayList<>();
        for (Json.Node n : arr(o, "columns").items()) {
            Json.Obj c = obj(n, "a result column");
            Json.Node nullable = c.getOr("nullable", null);
            cols.add(new Column(str(c, "name"), str(c, "type"), !(nullable instanceof Json.Bool b) || b.value()));
        }
        Integer parameters = null;
        if (o.getOr("parameterCount", null) != null) {
            long n = num(o, "parameterCount").longValue();
            if (n < 0 || n > Integer.MAX_VALUE) throw notTheApis("a statement's parameterCount is " + n);
            parameters = (int) n;
        }
        return new ResultMeta(cols, num(o, "rowCount").longValue(), (int) num(o, "chunkCount").longValue(), parameters);
    }

    public static Json.Obj chunk(Chunk c) {
        List<Json.Node> rows = new ArrayList<>(c.rows().size());
        for (List<Json.Node> row : c.rows()) rows.add(new Json.Arr(row));
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("index", Json.num(c.index()));
        f.put("rows", new Json.Arr(rows));
        return new Json.Obj(f);
    }

    public static Chunk parseChunk(Json.Obj o) {
        List<List<Json.Node>> rows = new ArrayList<>();
        for (Json.Node r : arr(o, "rows").items()) {
            if (!(r instanceof Json.Arr row)) throw notTheApis("a chunk's row is not an array");
            rows.add(row.items());
        }
        return new Chunk((int) num(o, "index").longValue(), rows);
    }

    public static Chunk parseChunk(String body) {
        return parseChunk(Json.parseObject(body));
    }

    public static List<SqlApi.HistoryEntry> parseHistory(String body) {
        List<SqlApi.HistoryEntry> out = new ArrayList<>();
        for (Json.Node n : arr(Json.parse(body), "the history").items()) {
            Json.Obj o = obj(n, "a history entry");
            String rows = strOr(o, "rowCount");
            out.add(new SqlApi.HistoryEntry(str(o, "statementId"), str(o, "catalog"), str(o, "sql"),
                    State.ofWire(str(o, "state")), str(o, "submittedAt"), strOr(o, "finishedAt"),
                    rows == null ? null : Long.valueOf(rows), strOr(o, "errorCode")));
        }
        return out;
    }

    public static List<SqlApi.CatalogObject> parseObjects(String body) {
        List<SqlApi.CatalogObject> out = new ArrayList<>();
        for (Json.Node n : arr(Json.parse(body), "the objects").items()) {
            Json.Obj o = obj(n, "an object");
            List<SqlApi.CatalogColumn> columns = new ArrayList<>();
            for (Json.Node c : arr(o, "columns").items()) {
                Json.Obj col = obj(c, "a column");
                Json.Node notNull = col.getOr("notNull", null);
                columns.add(new SqlApi.CatalogColumn(str(col, "name"), str(col, "type"), strOr(col, "logicalType"),
                        intOr(col, "precision"), intOr(col, "scale"), notNull instanceof Json.Bool b && b.value()));
            }
            out.add(new SqlApi.CatalogObject(str(o, "catalog"), str(o, "databaseType"), str(o, "schema"),
                    str(o, "name"), str(o, "kind"), columns));
        }
        return out;
    }

    /** The objects as the API writes them: what {@link #parseObjects} reads, every column's structured fields. */
    public static Json.Arr objects(List<SqlApi.CatalogObject> objects) {
        List<Json.Node> out = new ArrayList<>();
        for (SqlApi.CatalogObject o : objects) {
            List<Json.Node> columns = new ArrayList<>();
            for (SqlApi.CatalogColumn c : o.columns()) {
                LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
                f.put("name", Json.str(c.name()));
                f.put("type", Json.str(c.type()));
                String logical = c.logicalType();
                Integer precision = c.precision(), scale = c.scale();
                f.put("logicalType", logical == null ? Json.nil() : Json.str(logical));
                f.put("precision", precision == null ? Json.nil() : Json.num(precision));
                f.put("scale", scale == null ? Json.nil() : Json.num(scale));
                f.put("notNull", Json.bool(c.notNull()));
                columns.add(new Json.Obj(f));
            }
            LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
            f.put("catalog", Json.str(o.catalog()));
            f.put("databaseType", Json.str(o.databaseType()));
            f.put("schema", Json.str(o.schema()));
            f.put("name", Json.str(o.name()));
            f.put("kind", Json.str(o.kind()));
            f.put("columns", new Json.Arr(columns));
            out.add(new Json.Obj(f));
        }
        return new Json.Arr(out);
    }

    /** The error a failed call carried, if its body is one of ours. */
    public static @Nullable ApiError errorOf(String body) {
        try {
            Json.Obj o = Json.parseObject(body);
            Json.Obj e = objOr(o, "error");
            return e == null ? null : parseError(e);
        } catch (IllegalArgumentException notOurs) {
            return null;
        }
    }

    // -- shape: each value's kind checked before it is read ------------------

    private static IllegalArgumentException notTheApis(String what) {
        return new IllegalArgumentException("the warehouse's answer is not the API's: " + what);
    }

    private static Json.Obj obj(Json.Node n, String what) {
        if (n instanceof Json.Obj o) return o;
        throw notTheApis(what + " is not an object");
    }

    private static Json.Arr arr(Json.Node n, String what) {
        if (n instanceof Json.Arr a) return a;
        throw notTheApis(what + " is not an array");
    }

    private static Json.Arr arr(Json.Obj o, String key) {
        if (o.getOr(key, null) instanceof Json.Arr a) return a;
        throw notTheApis("'" + key + "' is not an array");
    }

    private static String str(Json.Obj o, String key) {
        if (o.getOr(key, null) instanceof Json.Str s) return s.value();
        throw notTheApis("'" + key + "' is not a string");
    }

    private static @Nullable String strOr(Json.Obj o, String key) {
        Json.Node n = o.getOr(key, null);
        if (n == null || n instanceof Json.Null) return null;
        if (n instanceof Json.Str s) return s.value();
        throw notTheApis("'" + key + "' is not a string");
    }

    private static Json.Num num(Json.Obj o, String key) {
        if (o.getOr(key, null) instanceof Json.Num n && n.isInteger()) return n;
        throw notTheApis("'" + key + "' is not a whole number");
    }

    private static @Nullable Integer intOr(Json.Obj o, String key) {
        Json.Node n = o.getOr(key, null);
        if (n == null || n instanceof Json.Null) return null;
        if (n instanceof Json.Num v && v.isInteger()) return (int) v.longValue();
        throw notTheApis("'" + key + "' is not a whole number");
    }

    private static @Nullable Json.Obj objOr(Json.Obj o, String key) {
        Json.Node n = o.getOr(key, null);
        if (n == null || n instanceof Json.Null) return null;
        return obj(n, "'" + key + "'");
    }
}
