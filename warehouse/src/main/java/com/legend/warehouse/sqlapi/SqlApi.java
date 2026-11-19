package com.legend.warehouse.sqlapi;

import com.legend.base.Nullable;
import com.legend.json.Json;
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
            boolean cellText,
            List<@Nullable Object> parameters) {

        /**
         * {@code parameters}: the values the statement's {@code ?}s bind, in order -- each a String, a Long, a
         * Double, a Boolean or null (the warehouse binds these and no other: lite's API v1 §4, a value is bound at
         * run and never pasted into the text; the orchestrator's ruling (a), 2026-11-17). Any other value is refused
         * by name, never stringified.
         */
        public StatementRequest {
            java.util.List<@Nullable Object> held = new java.util.ArrayList<>(parameters.size());
            for (int i = 0; i < parameters.size(); i++) {
                held.add(bindable(parameters.get(i), i + 1));
            }
            parameters = java.util.Collections.unmodifiableList(held);
        }

        /** A statement that binds no values. */
        public StatementRequest(String sql, String catalog, long timeoutMs, long waitMs, int rowsPerChunk,
                @Nullable String sessionId, boolean describeOnly, ResultFormat format, boolean cellText) {
            this(sql, catalog, timeoutMs, waitMs, rowsPerChunk, sessionId, describeOnly, format, cellText, List.of());
        }

        public StatementRequest(String sql, String catalog, long timeoutMs, long waitMs, int rowsPerChunk) {
            this(sql, catalog, timeoutMs, waitMs, rowsPerChunk, null, false, ResultFormat.JSON, false);
        }

        public StatementRequest(String sql, String catalog, long timeoutMs, long waitMs, int rowsPerChunk,
                @Nullable String sessionId) {
            this(sql, catalog, timeoutMs, waitMs, rowsPerChunk, sessionId, false, ResultFormat.JSON, false);
        }

        public StatementRequest inSession(String session) {
            return new StatementRequest(sql, catalog, timeoutMs, waitMs, rowsPerChunk, session, describeOnly, format,
                    cellText, parameters);
        }

        public StatementRequest describe() {
            return new StatementRequest(sql, catalog, timeoutMs, waitMs, rowsPerChunk, sessionId, true, format, cellText,
                    parameters);
        }

        public StatementRequest as(ResultFormat f) {
            return new StatementRequest(sql, catalog, timeoutMs, waitMs, rowsPerChunk, sessionId, describeOnly, f,
                    cellText, parameters);
        }

        /** The same statement, its {@code ?}s bound to {@code values} in order. */
        public StatementRequest withParameters(List<@Nullable Object> values) {
            return new StatementRequest(sql, catalog, timeoutMs, waitMs, rowsPerChunk, sessionId, describeOnly, format,
                    cellText, values);
        }

        /** {@code value} as the warehouse binds it, or refused by name: a String, a Long, a Double, a Boolean or null. */
        private static @Nullable Object bindable(@Nullable Object value, int at) {
            if (value == null || value instanceof String || value instanceof Long || value instanceof Boolean) {
                return value;
            }
            if (value instanceof Double d) {
                if (d.isNaN() || d.isInfinite()) {
                    throw new IllegalArgumentException("parameter " + at + " is " + d + ", which the warehouse binds no"
                            + " number as");
                }
                return d;
            }
            throw new IllegalArgumentException("parameter " + at + " is a " + value.getClass().getSimpleName()
                    + ", which the warehouse does not bind (a string, a whole number, a double, a boolean or null)");
        }

        /**
         * With Arrow: each batch also carries DuckDB's own text for its nested cells (the JSON format's
         * {@code "text"}), in the batch's metadata, where other Arrow readers do not look. For clients that
         * show a nested value as DuckDB prints it (a JDBC driver's {@code getString}).
         */
        public StatementRequest withCellText() {
            return new StatementRequest(sql, catalog, timeoutMs, waitMs, rowsPerChunk, sessionId, describeOnly, format, true,
                    parameters);
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

    /**
     * A finished statement's result, without its rows. {@code parameterCount}: on a describe
     * ({@link StatementRequest#describeOnly}) the number of values the statement binds -- its {@code ?}s, as DuckDB's own
     * prepare counts them -- and null on a run, which reports none (track Q2, 2026-11-19, the orchestrator's word on
     * part B: a JDBC driver's {@code getParameterMetaData}, never a count guessed from the text).
     */
    public record ResultMeta(List<Column> columns, long rowCount, int chunkCount, @Nullable Integer parameterCount) {
        public ResultMeta {
            columns = List.copyOf(columns);
            if (parameterCount != null && parameterCount < 0) {
                throw new IllegalArgumentException("a statement binds no negative number of values: " + parameterCount);
            }
        }

        /** A run's result: no parameter count. */
        public ResultMeta(List<Column> columns, long rowCount, int chunkCount) {
            this(columns, rowCount, chunkCount, null);
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
    /**
     * One table or view the caller may read, from {@code GET /sql/v1/objects} (every catalog) or
     * {@code GET /sql/v1/catalogs/{c}/objects}: its catalog, and that catalog's database type as a Pure
     * connection names it ({@code DuckDB}, {@code Postgres}) -- the SQL a statement on it is written in.
     */
    public record CatalogObject(String catalog, String databaseType, String schema, String name, String kind,
            List<CatalogColumn> columns) {
        public CatalogObject {
            columns = List.copyOf(columns);
        }
    }

    /**
     * A catalog object's column: its name and DuckDB's name for its type, and STRUCTURED, as the server reads its
     * catalog (DuckDb.CATALOG_COLUMNS_SQL): the canonical type, a DECIMAL's precision and scale, and whether the
     * catalog says it holds no NULL -- what a model is written from, never by parsing {@code type}.
     */
    public record CatalogColumn(String name, String type, @Nullable String logicalType, @Nullable Integer precision,
            @Nullable Integer scale, boolean notNull) {
    }

    public record HistoryEntry(String statementId, String catalog, String sql, State state, String submittedAt,
            @Nullable String finishedAt, @Nullable Long rowCount, @Nullable String errorCode) {
    }

    /** A signed-in session's token. */
    public record Token(String token, String expiresAt, String principal) {
    }
}
