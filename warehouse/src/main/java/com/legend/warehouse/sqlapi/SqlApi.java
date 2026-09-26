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

    /** The closed set of error codes a caller can act on. */
    public enum ErrorCode {
        AUTH_REQUIRED, AUTH_INVALID, FORBIDDEN,
        BAD_REQUEST, NOT_FOUND,
        SQL_PARSE, SQL_BIND, SQL_EXECUTE, UNSUPPORTED_TYPE,
        TIMEOUT, CANCELLED, QUEUE_FULL, TOO_LARGE, INTERNAL
    }

    /** What a caller asks to run. */
    public record StatementRequest(
            String sql,
            String catalog,
            long timeoutMs,
            long waitMs,
            int rowsPerChunk) {

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

    /** A signed-in session's token. */
    public record Token(String token, String expiresAt, String principal) {
    }
}
