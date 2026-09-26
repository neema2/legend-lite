package com.legend.warehouse.server;

import com.legend.Nullable;
import com.legend.server.Json;
import com.legend.warehouse.sqlapi.SqlApi.ApiError;
import com.legend.warehouse.sqlapi.SqlApi.Chunk;
import com.legend.warehouse.sqlapi.SqlApi.ErrorCode;
import com.legend.warehouse.sqlapi.SqlApi.ResultMeta;
import com.legend.warehouse.sqlapi.SqlApi.State;
import com.legend.warehouse.sqlapi.SqlApi.StatementRequest;
import com.legend.warehouse.sqlapi.SqlApi.Status;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Statements: queued, run, answered, remembered for a while.
 *
 * <p>CONCURRENCY IS FAIRNESS, NOT THROUGHPUT (W0 Q4): one DuckDB query
 * already uses every core, so a few statements run at once and the rest
 * wait in a bounded FIFO queue; a full queue refuses with QUEUE_FULL
 * rather than growing without end.
 *
 * <p>IDENTITY, BEFORE EVERY STATEMENT: each statement gets its own
 * connection, and the first thing run on it sets {@code app_user} to the
 * verified principal (program §3, 0b). Nothing a previous statement did
 * survives, because no connection is ever reused.
 */
public final class Statements implements AutoCloseable {

    /** One statement's state. Fields are written by its worker and read by callers. */
    public static final class Run {
        final String id = UUID.randomUUID().toString();
        final String principal;
        final StatementRequest request;
        final Instant submitted;
        volatile State state = State.QUEUED;
        volatile @Nullable Instant started;
        volatile @Nullable Instant finished;
        volatile @Nullable ResultMeta result;
        volatile @Nullable ApiError error;
        volatile List<Chunk> chunks = List.of();
        volatile @Nullable Statement jdbc;
        volatile boolean cancelRequested;
        volatile boolean timedOut;
        final CompletableFuture<Void> done = new CompletableFuture<>();

        Run(String principal, StatementRequest request, Instant submitted) {
            this.principal = principal;
            this.request = request;
            this.submitted = submitted;
        }

        public String id() {
            return id;
        }

        public String principal() {
            return principal;
        }

        public State state() {
            return state;
        }

        public CompletableFuture<Void> done() {
            return done;
        }

        /** The status document; the first chunk only when asked for. */
        public Status status(boolean withFirstChunk) {
            State s = state;
            List<Chunk> cs = chunks;
            Chunk first = withFirstChunk && s == State.SUCCEEDED && !cs.isEmpty() ? cs.get(0) : null;
            return new Status(id, s, s == State.SUCCEEDED ? result : null, error, first);
        }

        public @Nullable Chunk chunk(int index) {
            List<Chunk> cs = chunks;
            return state == State.SUCCEEDED && index >= 0 && index < cs.size() ? cs.get(index) : null;
        }
    }

    public record Limits(int concurrency, int queue, long maxRows, Duration retain) {
    }

    private final Catalogs catalogs;
    private final History history;
    private final Limits limits;
    private final Clock clock;
    private final ThreadPoolExecutor workers;
    private final ScheduledExecutorService timers = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "warehouse-timers");
        t.setDaemon(true);
        return t;
    });
    private final Map<String, Run> runs = new ConcurrentHashMap<>();

    public Statements(Catalogs catalogs, History history, Limits limits, Clock clock) {
        this.catalogs = catalogs;
        this.history = history;
        this.limits = limits;
        this.clock = clock;
        this.workers = new ThreadPoolExecutor(limits.concurrency(), limits.concurrency(), 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(limits.queue()), r -> {
                    Thread t = new Thread(r, "warehouse-statement");
                    t.setDaemon(true);
                    return t;
                });
        timers.scheduleWithFixedDelay(this::forgetOld, 30, 30, TimeUnit.SECONDS);
    }

    /** Queue a statement, or throw QueueFull when there is no room. */
    public Run submit(String principal, StatementRequest request) throws QueueFull {
        Run run = new Run(principal, request, clock.instant());
        runs.put(run.id, run);
        try {
            workers.execute(() -> execute(run));
        } catch (RejectedExecutionException full) {
            runs.remove(run.id);
            throw new QueueFull();
        }
        if (request.timeoutMs() > 0) {
            timers.schedule(() -> timeout(run), request.timeoutMs(), TimeUnit.MILLISECONDS);
        }
        return run;
    }

    /** The run, if it exists and belongs to this principal. Others' runs do not exist to you. */
    public @Nullable Run find(String principal, String id) {
        Run r = runs.get(id);
        return r != null && r.principal.equals(principal) ? r : null;
    }

    public void cancel(Run run) {
        run.cancelRequested = true;
        interrupt(run);
    }

    private void timeout(Run run) {
        if (run.state.done()) return;
        run.timedOut = true;
        run.cancelRequested = true;
        interrupt(run);
    }

    private static void interrupt(Run run) {
        Statement s = run.jdbc;
        if (s != null) {
            try {
                s.cancel();
            } catch (SQLException ignored) {
                // it finished between the check and the cancel
            }
        }
    }

    /** Thrown when the queue is full. */
    public static final class QueueFull extends Exception {
        QueueFull() {
            super("the statement queue is full");
        }
    }

    private void execute(Run run) {
        if (run.cancelRequested) {
            finish(run, run.timedOut ? State.FAILED : State.CANCELLED,
                    run.timedOut ? new ApiError(ErrorCode.TIMEOUT, "timed out while queued") : null);
            return;
        }
        run.state = State.RUNNING;
        run.started = clock.instant();
        String catalog = run.request.catalog();
        try (Connection c = catalogs.connect(catalog)) {
            if (c == null) {
                finish(run, State.FAILED, new ApiError(ErrorCode.NOT_FOUND, "no catalog '" + catalog + "'"));
                return;
            }
            try (Statement identity = c.createStatement()) {
                // The identity, on this statement's own connection, before anything else.
                identity.execute("SET VARIABLE app_user = '" + run.principal.replace("'", "''") + "'");
            }
            // PREPARED, then executed: DuckDB JDBC 1.5.5.1's Statement.execute
            // replaces a binder or catalog error with "Attempting to execute an
            // unsuccessful or closed pending query result" (1.4.4 reported it);
            // preparing first keeps the real message.
            try (PreparedStatement st = c.prepareStatement(run.request.sql())) {
                run.jdbc = st;
                if (run.cancelRequested) throw new SQLException("INTERRUPT Error: cancelled before start");
                boolean hasRows = st.execute();
                if (hasRows) {
                    try (ResultSet rs = st.getResultSet()) {
                        collect(run, rs);
                    }
                } else {
                    long count = st.getUpdateCount();
                    run.result = new ResultMeta(List.of(), Math.max(0, count), 0);
                }
                finish(run, State.SUCCEEDED, null);
            }
        } catch (ResultEncoder.Unsupported e) {
            finish(run, State.FAILED, new ApiError(ErrorCode.UNSUPPORTED_TYPE, String.valueOf(e.getMessage())));
        } catch (TooLarge e) {
            finish(run, State.FAILED, new ApiError(ErrorCode.TOO_LARGE, String.valueOf(e.getMessage())));
        } catch (SQLException e) {
            if (run.cancelRequested) {
                finish(run, run.timedOut ? State.FAILED : State.CANCELLED, run.timedOut
                        ? new ApiError(ErrorCode.TIMEOUT, "timed out after " + run.request.timeoutMs() + " ms")
                        : null);
            } else {
                finish(run, State.FAILED, classify(e));
            }
        } catch (RuntimeException e) {
            finish(run, State.FAILED, new ApiError(ErrorCode.INTERNAL, String.valueOf(e.getMessage())));
        } finally {
            run.jdbc = null;
        }
    }

    private static final class TooLarge extends Exception {
        TooLarge(long max) {
            super("the result has more than " + max + " rows");
        }
    }

    private void collect(Run run, ResultSet rs) throws SQLException, TooLarge {
        List<ResultEncoder.Col> cols = ResultEncoder.columns(rs.getMetaData());
        int per = Math.max(1, run.request.rowsPerChunk());
        List<Chunk> chunks = new ArrayList<>();
        List<List<Json.Node>> rows = new ArrayList<>();
        long n = 0;
        while (rs.next()) {
            if (++n > limits.maxRows()) throw new TooLarge(limits.maxRows());
            rows.add(ResultEncoder.row(rs, cols));
            if (rows.size() == per) {
                chunks.add(new Chunk(chunks.size(), rows));
                rows = new ArrayList<>();
            }
        }
        if (!rows.isEmpty() || chunks.isEmpty()) chunks.add(new Chunk(chunks.size(), rows));
        run.chunks = List.copyOf(chunks);
        run.result = new ResultMeta(ResultEncoder.api(cols), n, chunks.size());
    }

    /** DuckDB's error kinds, as the API's codes. */
    static ApiError classify(SQLException e) {
        String m = String.valueOf(e.getMessage());
        String first = m.split("\n", 2)[0];
        ErrorCode code;
        if (first.startsWith("Parser Error")) code = ErrorCode.SQL_PARSE;
        else if (first.startsWith("Binder Error") || first.startsWith("Catalog Error")) code = ErrorCode.SQL_BIND;
        else code = ErrorCode.SQL_EXECUTE;
        return new ApiError(code, m.strip());
    }

    private void finish(Run run, State state, @Nullable ApiError error) {
        run.error = error;
        run.finished = clock.instant();
        run.state = state;
        history.record(run);
        run.done.complete(null);
    }

    private void forgetOld() {
        Instant cutoff = clock.instant().minus(limits.retain());
        runs.values().removeIf(r -> {
            Instant f = r.finished;
            return f != null && f.isBefore(cutoff);
        });
    }

    @Override
    public void close() {
        workers.shutdownNow();
        timers.shutdownNow();
    }
}
