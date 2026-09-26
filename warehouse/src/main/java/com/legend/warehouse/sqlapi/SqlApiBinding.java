package com.legend.warehouse.sqlapi;

import com.legend.Nullable;
import com.legend.warehouse.sqlapi.SqlApi.ApiError;
import com.legend.warehouse.sqlapi.SqlApi.Chunk;
import com.legend.warehouse.sqlapi.SqlApi.StatementRequest;
import com.legend.warehouse.sqlapi.SqlApi.Status;
import com.legend.warehouse.sqlapi.SqlApi.Token;
import java.util.Map;

/**
 * How to speak ONE warehouse's SQL API, with no I/O of its own
 * (docs/SERVER_PROGRAM_2026_09_26.md §2c).
 *
 * <p>A binding builds the next HTTP call and interprets each response; a
 * DRIVER performs the calls -- {@code java.net.http} on the JVM, the
 * browser's {@code fetch} from the WebAssembly module. So the same binding
 * runs in both, and a vendor's API is one more implementation of this
 * interface, never a second client.
 */
public interface SqlApiBinding {

    /** One HTTP call, relative to the warehouse's base URL. */
    record HttpCall(String method, String path, Map<String, String> headers, @Nullable String body) {
        public HttpCall {
            headers = Map.copyOf(headers);
        }
    }

    /** What came back: the body's bytes (an Arrow chunk is binary), and its text when it is text. */
    record HttpResult(int status, byte[] bytes) {
        public HttpResult(int status, String body) {
            this(status, body.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        public String body() {
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    /** What the driver does next with a statement. */
    sealed interface Step permits Poll, Done, Failed {
    }

    /** Not finished: send this call (a long poll) and hand its result back. */
    record Poll(String statementId, HttpCall call) implements Step {
    }

    /** Finished; the first chunk, when it came with the answer. */
    record Done(Status status) implements Step {
    }

    /** Failed, with the reason. */
    record Failed(ApiError error, @Nullable String statementId) implements Step {
    }

    HttpCall login(String user, String password);

    Token token(HttpResult result);

    HttpCall submit(StatementRequest request, String token);

    /** The step after a submit's or a poll's result. */
    Step next(HttpResult result, String token);

    HttpCall fetchChunk(String statementId, int index, String token);

    Chunk chunk(HttpResult result);

    /** An Arrow result's chunk: a whole Arrow IPC stream. */
    byte[] arrowChunk(HttpResult result);

    HttpCall cancel(String statementId, String token);

    HttpCall openSession(String catalog, String token);

    SqlApi.Session session(HttpResult result);

    HttpCall closeSession(String sessionId, String token);

    /** The caller's own most recent statements, newest first. */
    HttpCall history(int limit, String token);

    java.util.List<SqlApi.HistoryEntry> history(HttpResult result);
}
