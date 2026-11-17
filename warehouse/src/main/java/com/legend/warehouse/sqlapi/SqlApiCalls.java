package com.legend.warehouse.sqlapi;

import com.legend.json.Json;
import com.legend.warehouse.sqlapi.SqlApi.ApiError;
import com.legend.warehouse.sqlapi.SqlApi.ErrorCode;
import com.legend.warehouse.sqlapi.SqlApi.StatementRequest;
import com.legend.warehouse.sqlapi.SqlApiBinding.HttpCall;
import com.legend.warehouse.sqlapi.SqlApiBinding.HttpResult;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE BINDING, AS EVERY DRIVER CALLS IT (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C3): {@link NativeBinding}'s calls and
 * readings as JSON in and JSON out, so the tab (its page module, //warehouse:sqlapi_page) and Python (the native
 * library, {@code lite_warehouse_call} and {@code lite_warehouse_read}) speak the warehouse's API through this one
 * implementation, never a spelling of their own. A driver only makes the HTTP calls this names and hands back what
 * came; the Arrow bytes stay the driver's (a chunk's body is used as it came: only a failed chunk's answer is read
 * here).
 *
 * <p>{@link #call}: the next call, {@code {method, path, headers, body}}. {@link #read}: what an answer says -- a
 * token, the objects, the history, or a statement's next step -- or {@code {"error": {code, message}}}, marked
 * {@code "expired": true} when a call that carried a token was answered 401 (the session is gone: the driver raises
 * that, never reading a status itself). Wrong arguments, and an answer that is not the API's, are refused the same way
 * ({@code BAD_REQUEST}, {@code INTERNAL}): nothing here throws to its caller, a WebAssembly page's or a C one's.
 */
public final class SqlApiCalls {

    /** An interactive client's statement (a page's, Python's): how long one poll waits on the server. */
    public static final long POLL_WAIT_MS = 10_000;
    /** Rows per Arrow chunk: whole DuckDB batches of 2,048 each. */
    public static final int ROWS_PER_CHUNK = 100_000;
    /** How long a statement may run before the server stops it. */
    public static final long TIMEOUT_MS = 300_000;

    private static final NativeBinding BINDING = new NativeBinding(POLL_WAIT_MS);

    private SqlApiCalls() {
    }

    /** The call {@code op} makes with {@code args} (a JSON object), or the refusal of its arguments. */
    public static String call(String op, String args) {
        try {
            // a careless caller's null (JavaScript's, C's) is refused as any wrong argument is, never thrown
            if (op == null || args == null) throw new IllegalArgumentException("a call names its op and its arguments");
            Json.Obj a = object(args);
            HttpCall call = switch (op) {
                case "login" -> BINDING.login(text(a, "user"), text(a, "password"));
                case "loginKey" -> BINDING.loginWithKey(text(a, "key"));
                case "refresh" -> BINDING.refresh(text(a, "token"));
                // a statement's values, bound at run in order (lite's API v1 §4; the orchestrator's ruling (a),
                // 2026-11-17): carried to the server, never pasted into the text
                case "submit" -> BINDING.submit(new StatementRequest(text(a, "sql"), text(a, "catalog"), TIMEOUT_MS,
                        POLL_WAIT_MS, ROWS_PER_CHUNK).as(SqlApi.ResultFormat.ARROW)
                        .withParameters(a.has("parameters") ? ApiJson.parameters(a.get("parameters")) : List.of()),
                        text(a, "token"));
                case "poll" -> BINDING.poll(text(a, "statementId"), text(a, "token"));
                case "fetchChunk" -> BINDING.fetchChunk(text(a, "statementId"), whole(a, "index"), text(a, "token"));
                case "cancel" -> BINDING.cancel(text(a, "statementId"), text(a, "token"));
                case "closeStatement" -> BINDING.closeStatement(text(a, "statementId"), text(a, "token"));
                case "history" -> BINDING.history(whole(a, "limit"), text(a, "token"));
                case "objects" -> BINDING.allObjects(text(a, "token"));
                default -> throw new IllegalArgumentException("no such call: " + op);
            };
            return Json.toCompact(call(call));
        } catch (IllegalArgumentException wrong) {
            return refusal(new ApiError(ErrorCode.BAD_REQUEST, String.valueOf(wrong.getMessage())), false);
        }
    }

    /**
     * What the answer ({@code status}, {@code body}) to {@code op}'s call says. {@code submit}'s, and each
     * {@code poll}'s, is the statement's next step: {@code {"step": "poll", statementId}} (the driver then makes
     * {@code poll}'s call),
     * {@code {"step": "done", statementId, rowCount, chunkCount}} or {@code {"step": "failed", statementId, error}}.
     */
    public static String read(String op, int status, String body) {
        if (op == null) return refusal(new ApiError(ErrorCode.BAD_REQUEST, "a reading names its op"), false);
        HttpResult result = new HttpResult(status, body == null ? "" : body);
        boolean carriedToken = !op.equals("login") && !op.equals("loginKey");
        if (status == 401 && carriedToken) {
            return refusal(failure(result), true);
        }
        try {
            Json.Node answer = switch (op) {
                case "login", "loginKey", "refresh" -> ok(status) ? ApiJson.token(BINDING.token(result)) : null;
                // the poll's own call is asked for by the driver (call "poll"), with its token: the one next() builds
                // here, for no token, is dropped by step()
                case "submit", "poll" -> step(BINDING.next(result, ""));
                case "objects" -> ok(status) ? ApiJson.objects(BINDING.objects(result)) : null;
                case "history" -> ok(status) ? history(BINDING.history(result)) : null;
                // a chunk's bytes are the driver's (as NativeBinding.arrowChunk hands them back); a statement's
                // cancel and close answer nothing to read
                case "fetchChunk" -> ok(status) ? Json.obj() : null;
                case "cancel", "closeStatement" -> status / 100 == 2 ? Json.obj() : null;
                default -> throw new IllegalArgumentException("no such call: " + op);
            };
            return answer != null ? Json.toCompact(answer) : refusal(failure(result), false);
        } catch (IllegalArgumentException notTheApis) {
            return refusal(new ApiError(ErrorCode.INTERNAL, String.valueOf(notTheApis.getMessage())), false);
        }
    }

    private static boolean ok(int status) {
        return status == 200;
    }

    /** The binding's step, as the driver reads it. */
    private static Json.Node step(SqlApiBinding.Step step) {
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        switch (step) {
            case SqlApiBinding.Poll p -> {
                f.put("step", Json.str("poll"));
                f.put("statementId", Json.str(p.statementId()));
            }
            case SqlApiBinding.Done d -> {
                SqlApi.ResultMeta meta = d.status().result();
                f.put("step", Json.str("done"));
                f.put("statementId", Json.str(d.status().statementId()));
                // the server's own count, only when it gave one; no result, no chunk to read
                if (meta != null) f.put("rowCount", Json.num(meta.rowCount()));
                f.put("chunkCount", Json.num(meta == null ? 0 : meta.chunkCount()));
            }
            case SqlApiBinding.Failed x -> {
                f.put("step", Json.str("failed"));
                String id = x.statementId();
                if (id != null) f.put("statementId", Json.str(id));
                f.put("error", ApiJson.error(x.error()));
            }
        }
        return new Json.Obj(f);
    }

    private static Json.Node history(java.util.List<SqlApi.HistoryEntry> entries) {
        java.util.List<Json.Node> out = new java.util.ArrayList<>();
        for (SqlApi.HistoryEntry e : entries) {
            LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
            f.put("statementId", Json.str(e.statementId()));
            f.put("state", Json.str(e.state().wire()));
            f.put("submittedAt", Json.str(e.submittedAt()));
            String finished = e.finishedAt();
            if (finished != null) f.put("finishedAt", Json.str(finished));
            Long rows = e.rowCount();
            f.put("rowCount", rows == null ? Json.nil() : Json.num(rows));
            out.add(new Json.Obj(f));
        }
        return new Json.Arr(out);
    }

    private static Json.Obj call(HttpCall c) {
        LinkedHashMap<String, Json.Node> headers = new LinkedHashMap<>();
        // in name order: a driver's request, and the corpus's record of it, the same whichever map built it
        for (Map.Entry<String, String> h : new java.util.TreeMap<>(c.headers()).entrySet()) {
            headers.put(h.getKey(), Json.str(h.getValue()));
        }
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("method", Json.str(c.method()));
        f.put("path", Json.str(c.path()));
        f.put("headers", new Json.Obj(headers));
        String body = c.body();
        f.put("body", body == null || body.isEmpty() ? Json.nil() : Json.str(body));
        return new Json.Obj(f);
    }

    /** A refused call's error, by the binding's one rule (NativeBinding.failure). */
    private static ApiError failure(HttpResult result) {
        return NativeBinding.failure(result);
    }

    private static String refusal(ApiError e, boolean expired) {
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("error", ApiJson.error(e));
        if (expired) f.put("expired", Json.bool(true));
        return Json.toCompact(new Json.Obj(f));
    }

    private static Json.Obj object(String args) {
        if (Json.parse(args) instanceof Json.Obj o) return o;
        throw new IllegalArgumentException("a call's arguments are a JSON object");
    }

    private static String text(Json.Obj a, String key) {
        if (a.getOr(key, null) instanceof Json.Str s) return s.value();
        throw new IllegalArgumentException("'" + key + "' is a string");
    }

    private static int whole(Json.Obj a, String key) {
        if (a.getOr(key, null) instanceof Json.Num n && n.isInteger() && n.longValue() >= 0
                && n.longValue() <= Integer.MAX_VALUE) {
            return (int) n.longValue();
        }
        throw new IllegalArgumentException("'" + key + "' is a whole number");
    }
}
