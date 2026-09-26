package com.legend.warehouse.sqlapi;

import com.legend.warehouse.sqlapi.SqlApi.ApiError;
import com.legend.warehouse.sqlapi.SqlApi.Chunk;
import com.legend.warehouse.sqlapi.SqlApi.ErrorCode;
import com.legend.warehouse.sqlapi.SqlApi.StatementRequest;
import com.legend.warehouse.sqlapi.SqlApi.Status;
import com.legend.warehouse.sqlapi.SqlApi.Token;
import java.util.Map;

/** The warehouse's own API, spoken as designed: no translation at all. */
public final class NativeBinding implements SqlApiBinding {

    /** How long one poll may wait on the server before it answers. */
    private final long pollWaitMs;

    public NativeBinding() {
        this(StatementRequest.DEFAULT_WAIT_MS);
    }

    public NativeBinding(long pollWaitMs) {
        this.pollWaitMs = pollWaitMs;
    }

    private static Map<String, String> json(String token) {
        return Map.of("Authorization", "Bearer " + token, "Content-Type", "application/json");
    }

    @Override
    public HttpCall login(String user, String password) {
        return new HttpCall("POST", "/sql/v1/login", Map.of("Content-Type", "application/json"),
                ApiJson.login(user, password));
    }

    @Override
    public Token token(HttpResult result) {
        if (result.status() != 200) throw new IllegalStateException(failure(result).message());
        return ApiJson.parseToken(result.body());
    }

    @Override
    public HttpCall submit(StatementRequest request, String token) {
        return new HttpCall("POST", "/sql/v1/statements", json(token), ApiJson.statementRequest(request));
    }

    @Override
    public Step next(HttpResult result, String token) {
        if (result.status() != 200 && result.status() != 202) {
            return new Failed(failure(result), null);
        }
        Status s = ApiJson.parseStatus(result.body());
        return switch (s.state()) {
            case QUEUED, RUNNING -> new Poll(s.statementId(), new HttpCall("GET",
                    "/sql/v1/statements/" + s.statementId() + "?waitMs=" + pollWaitMs,
                    json(token), null));
            case SUCCEEDED -> new Done(s);
            case FAILED, CANCELLED -> {
                ApiError e = s.error();
                yield new Failed(e == null
                        ? new ApiError(s.state() == SqlApi.State.CANCELLED ? ErrorCode.CANCELLED : ErrorCode.INTERNAL,
                                "statement " + s.state().wire())
                        : e, s.statementId());
            }
        };
    }

    @Override
    public HttpCall fetchChunk(String statementId, int index, String token) {
        return new HttpCall("GET", "/sql/v1/statements/" + statementId + "/chunks/" + index, json(token), null);
    }

    @Override
    public Chunk chunk(HttpResult result) {
        if (result.status() != 200) throw new IllegalStateException(failure(result).message());
        return ApiJson.parseChunk(result.body());
    }

    @Override
    public HttpCall cancel(String statementId, String token) {
        return new HttpCall("POST", "/sql/v1/statements/" + statementId + "/cancel", json(token), null);
    }

    @Override
    public HttpCall openSession(String catalog, String token) {
        return new HttpCall("POST", "/sql/v1/sessions", json(token), ApiJson.openSession(catalog));
    }

    @Override
    public SqlApi.Session session(HttpResult result) {
        if (result.status() != 200) throw new IllegalStateException(failure(result).message());
        return ApiJson.parseSession(result.body());
    }

    @Override
    public HttpCall closeSession(String sessionId, String token) {
        return new HttpCall("DELETE", "/sql/v1/sessions/" + sessionId, json(token), null);
    }

    @Override
    public HttpCall history(int limit, String token) {
        return new HttpCall("GET", "/sql/v1/history?limit=" + limit, json(token), null);
    }

    @Override
    public java.util.List<SqlApi.HistoryEntry> history(HttpResult result) {
        if (result.status() != 200) throw new IllegalStateException(failure(result).message());
        return ApiJson.parseHistory(result.body());
    }

    private static ApiError failure(HttpResult result) {
        ApiError e = ApiJson.errorOf(result.body());
        return e != null ? e : new ApiError(ErrorCode.INTERNAL, "HTTP " + result.status() + ": " + result.body());
    }
}
