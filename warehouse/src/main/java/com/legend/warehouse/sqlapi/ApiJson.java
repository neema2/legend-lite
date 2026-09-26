package com.legend.warehouse.sqlapi;

import com.legend.Nullable;
import com.legend.server.Json;
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
        return Json.toCompact(new Json.Obj(f));
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
                format == null ? SqlApi.ResultFormat.JSON : SqlApi.ResultFormat.ofWire(format));
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
        return new SqlApi.Session(o.getString("sessionId"), o.getString("catalog"),
                o.getString("engine"), o.getString("engineVersion"));
    }

    public static String login(String user, String password) {
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("user", Json.str(user));
        f.put("password", Json.str(password));
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
        return new Token(o.getString("token"), o.getString("expiresAt"), o.getString("principal"));
    }

    public static Json.Obj error(ApiError e) {
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("code", Json.str(e.code().name()));
        f.put("message", Json.str(e.message()));
        return new Json.Obj(f);
    }

    public static ApiError parseError(Json.Obj o) {
        return new ApiError(ErrorCode.valueOf(o.getString("code")), o.getString("message"));
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
        Json.Obj r = o.getObjOr("result", null);
        Json.Obj e = o.getObjOr("error", null);
        Json.Obj c = o.getObjOr("firstChunk", null);
        return new Status(
                o.getString("statementId"),
                State.ofWire(o.getString("state")),
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
        return new Json.Obj(f);
    }

    public static ResultMeta parseResultMeta(Json.Obj o) {
        List<Column> cols = new ArrayList<>();
        for (Json.Node n : o.getArr("columns").items()) {
            Json.Obj c = (Json.Obj) n;
            cols.add(new Column(c.getString("name"), c.getString("type"), c.getBoolOr("nullable", true)));
        }
        return new ResultMeta(cols, o.getLong("rowCount"), o.getInt("chunkCount"));
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
        for (Json.Node r : o.getArr("rows").items()) rows.add(((Json.Arr) r).items());
        return new Chunk(o.getInt("index"), rows);
    }

    public static Chunk parseChunk(String body) {
        return parseChunk(Json.parseObject(body));
    }

    /** The error a failed call carried, if its body is one of ours. */
    public static @Nullable ApiError errorOf(String body) {
        try {
            Json.Obj o = Json.parseObject(body);
            Json.Obj e = o.getObjOr("error", null);
            return e == null ? null : parseError(e);
        } catch (RuntimeException notOurs) {
            return null;
        }
    }
}
