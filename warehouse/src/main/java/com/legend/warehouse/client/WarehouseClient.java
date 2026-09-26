package com.legend.warehouse.client;

import com.legend.Nullable;
import com.legend.server.Json;
import com.legend.warehouse.sqlapi.SqlApi;
import com.legend.warehouse.sqlapi.SqlApi.ApiError;
import com.legend.warehouse.sqlapi.SqlApi.Chunk;
import com.legend.warehouse.sqlapi.SqlApi.ResultMeta;
import com.legend.warehouse.sqlapi.SqlApi.StatementRequest;
import com.legend.warehouse.sqlapi.SqlApi.Status;
import com.legend.warehouse.sqlapi.SqlApiBinding;
import com.legend.warehouse.sqlapi.SqlApiBinding.HttpCall;
import com.legend.warehouse.sqlapi.SqlApiBinding.HttpResult;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;

/**
 * The JVM's driver for a binding: the binding decides every call, this
 * performs them with {@code java.net.http} (docs/SERVER_PROGRAM_2026_09_26.md
 * §2c). The browser's driver does the same with {@code fetch}.
 */
public final class WarehouseClient {

    /** A finished statement: its columns and every row, across its chunks. */
    public record Result(String statementId, ResultMeta meta, List<List<Json.Node>> rows) {
    }

    /** A statement that failed, with the API's reason. */
    public static final class Failure extends Exception {
        private final ApiError error;

        public Failure(ApiError error) {
            super(error.code() + ": " + error.message());
            this.error = error;
        }

        public ApiError error() {
            return error;
        }
    }

    private final URI base;
    private final SqlApiBinding binding;
    private final HttpClient http = HttpClient.newHttpClient();
    private volatile @Nullable String token;

    public WarehouseClient(URI base, SqlApiBinding binding) {
        this.base = base;
        this.binding = binding;
    }

    public void login(String user, String password) throws IOException, InterruptedException {
        token = binding.token(send(binding.login(user, password))).token();
    }

    private String token() {
        String t = token;
        if (t == null) throw new IllegalStateException("not signed in");
        return t;
    }

    /** Run a statement to the end and fetch all of its rows. */
    public Result execute(StatementRequest request) throws IOException, InterruptedException, Failure {
        return execute(request, id -> { });
    }

    /** The same, telling {@code onId} the statement's id while it runs, so it can be cancelled. */
    public Result execute(StatementRequest request, java.util.function.Consumer<String> onId)
            throws IOException, InterruptedException, Failure {
        String t = token();
        SqlApiBinding.Step step = binding.next(send(binding.submit(request, t)), t);
        while (step instanceof SqlApiBinding.Poll p) {
            onId.accept(p.statementId());
            step = binding.next(send(p.call()), t);
        }
        if (step instanceof SqlApiBinding.Failed f) throw new Failure(f.error());
        Status s = ((SqlApiBinding.Done) step).status();
        ResultMeta meta = s.result();
        if (meta == null) throw new IllegalStateException("a finished statement without a result");
        List<List<Json.Node>> rows = new ArrayList<>();
        if (meta.chunkCount() > 0) {
            Chunk first = s.firstChunk();
            rows.addAll(first != null ? first.rows()
                    : binding.chunk(send(binding.fetchChunk(s.statementId(), 0, t))).rows());
            for (int i = 1; i < meta.chunkCount(); i++) {
                rows.addAll(binding.chunk(send(binding.fetchChunk(s.statementId(), i, t))).rows());
            }
        }
        return new Result(s.statementId(), meta, rows);
    }

    /** A session on the catalog: statements in it share one connection, in order. */
    public SqlApi.Session openSession(String catalog) throws IOException, InterruptedException {
        return binding.session(send(binding.openSession(catalog, token())));
    }

    public void closeSession(String sessionId) throws IOException, InterruptedException {
        send(binding.closeSession(sessionId, token()));
    }

    /** Ask the warehouse to stop a statement. */
    public void cancel(String statementId) throws IOException, InterruptedException {
        send(binding.cancel(statementId, token()));
    }

    private HttpResult send(HttpCall c) throws IOException, InterruptedException {
        HttpRequest.Builder b = HttpRequest.newBuilder(base.resolve(c.path()));
        c.headers().forEach(b::header);
        String body = c.body();
        b.method(c.method(), body == null ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body));
        HttpResponse<String> r = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
        return new HttpResult(r.statusCode(), r.body());
    }
}
