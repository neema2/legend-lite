package com.legend.warehouse.sqlapi;

import com.legend.Nullable;
import com.legend.server.Json;
import java.util.List;
import java.util.Locale;

/**
 * The HTTP SQL API's vocabulary: what a statement request, its status and
 * its results are (docs/WAREHOUSE_W1_DESIGN_2026_09_26.md §3).
 *
 * <p>Defined ONCE, here, in plain Java on java.base alone: the warehouse
 * serves it, the JVM client and JDBC driver speak it, and the same classes
 * compile into the WebAssembly module for the browser (program ruling 7).
 */
public final class SqlApi {

    private SqlApi() {
    }

    /** A statement's life. On the wire: lower case. */
    public enum State {
        QUEUED, RUNNING, SUCCEEDED, FAILED, CANCELLED;

        public String wire() {
            return name().toLowerCase(Locale.ROOT);
        }

        public boolean done() {
            return this == SUCCEEDED || this == FAILED || this == CANCELLED;
        }

        public static State ofWire(String s) {
            return State.valueOf(s.toUpperCase(Locale.ROOT));
        }
    }

    /** How a result's chunks travel. On the wire: lower case. */
    public enum ResultFormat {
        /** Rows of JSON values (§3's value rules); the first chunk comes with the status. */
        JSON,
        /** Each chunk a whole Arrow IPC stream ({@code application/vnd.apache.arrow.stream}). */
        ARROW;

        public String wire() {
            return name().toLowerCase(Locale.ROOT);
        }

        public static ResultFormat ofWire(String s) {
            return ResultFormat.valueOf(s.toUpperCase(Locale.ROOT));
        }
    }

    /** The closed set of error codes a caller can act on. */
    public enum ErrorCode {
        AUTH_REQUIRED, AUTH_INVALID, FORBIDDEN,
        BAD_REQUEST, NOT_FOUND,
        SQL_PARSE, SQL_BIND, SQL_EXECUTE, UNSUPPORTED_TYPE,
        TIMEOUT, CANCELLED, QUEUE_FULL, TOO_LARGE, INTERNAL
    }

    /**
     * What a caller asks to run. With a {@code sessionId}, it runs on that
     * session's own connection, after the session's earlier statements:
     * {@code USE}, {@code SET}, temp tables and transactions persist.
     *
     * <p>{@code describeOnly}: prepare the statement and report its result's
     * columns, without running it (a compiler asking what a query returns).
     * The result has the columns and no rows.
     */
    public record StatementRequest(
            String sql,
            String catalog,
            long timeoutMs,
            long waitMs,
            int rowsPerChunk,
            @Nullable String sessionId,
            boolean describeOnly,
            ResultFormat format,
            boolean cellText) {

        public StatementRequest(String sql, String catalog, long timeoutMs, long waitMs, int rowsPerChunk) {
            this(sql, catalog, timeoutMs, waitMs, rowsPerChunk, null, false, ResultFormat.JSON, false);
        }

        public StatementRequest(String sql, String catalog, long timeoutMs, long waitMs, int rowsPerChunk,
                @Nullable String sessionId) {
            this(sql, catalog, timeoutMs, waitMs, rowsPerChunk, sessionId, false, ResultFormat.JSON, false);
        }

        public StatementRequest inSession(String session) {
            return new StatementRequest(sql, catalog, timeoutMs, waitMs, rowsPerChunk, session, describeOnly, format, cellText);
        }

        public StatementRequest describe() {
            return new StatementRequest(sql, catalog, timeoutMs, waitMs, rowsPerChunk, sessionId, true, format, cellText);
        }

        public StatementRequest as(ResultFormat f) {
            return new StatementRequest(sql, catalog, timeoutMs, waitMs, rowsPerChunk, sessionId, describeOnly, f, cellText);
        }

        /**
         * With Arrow: each batch also carries DuckDB's own text for its nested cells (the JSON format's
         * {@code "text"}), in the batch's metadata, where other Arrow readers do not look. For clients that
         * show a nested value as DuckDB prints it (a JDBC driver's {@code getString}).
         */
        public StatementRequest withCellText() {
            return new StatementRequest(sql, catalog, timeoutMs, waitMs, rowsPerChunk, sessionId, describeOnly, format, true);
        }

        public static final String DEFAULT_CATALOG = "main";
        public static final long DEFAULT_TIMEOUT_MS = 60_000;
        public static final long DEFAULT_WAIT_MS = 2_000;
        public static final int DEFAULT_ROWS_PER_CHUNK = 10_000;

        public static StatementRequest of(String sql) {
            return new StatementRequest(sql, DEFAULT_CATALOG, DEFAULT_TIMEOUT_MS,
                    DEFAULT_WAIT_MS, DEFAULT_ROWS_PER_CHUNK);
        }
    }

    /** One result column: its name, its API type, and whether it may be null. */
    public record Column(String name, String type, boolean nullable) {
    }

    /** A finished statement's result, without its rows. */
    public record ResultMeta(List<Column> columns, long rowCount, int chunkCount) {
        public ResultMeta {
            columns = List.copyOf(columns);
        }
    }

    /** Why a statement or a call failed. */
    public record ApiError(ErrorCode code, String message) {
    }

    /**
     * Rows as JSON values, one list per row, in column order. Values follow
     * the design's rules: 64-bit and wider integers and decimals are STRINGS
     * (no precision lost in a JavaScript client), dates and times ISO-8601.
     */
    public record Chunk(int index, List<List<Json.Node>> rows) {
        public Chunk {
            rows = List.copyOf(rows);
        }
    }

    /** A statement as the server reports it. */
    public record Status(
            String statementId,
            State state,
            @Nullable ResultMeta result,
            @Nullable ApiError error,
            @Nullable Chunk firstChunk) {
    }

    /**
     * A session: a pinned connection to one catalog, and the engine behind
     * it (its name and version, as that engine's own driver reports them:
     * the SQL a session accepts is that engine's).
     */
    public record Session(String sessionId, String catalog, String engine, String engineVersion) {
    }

    /** One of the caller's statements, from {@code GET /sql/v1/history}. */
    public record HistoryEntry(String statementId, String catalog, String sql, State state, String submittedAt,
            @Nullable String finishedAt, @Nullable Long rowCount, @Nullable String errorCode) {
    }

    /** A signed-in session's token. */
    public record Token(String token, String expiresAt, String principal) {
    }
}
