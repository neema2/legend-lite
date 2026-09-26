package com.legend.warehouse.server;

import com.legend.Nullable;
import com.legend.server.Json;
import com.legend.warehouse.server.duck.Collect;
import com.legend.warehouse.server.duck.Conn;
import com.legend.warehouse.server.duck.DuckException;
import com.legend.warehouse.server.duck.Result;
import com.legend.warehouse.sqlapi.SqlApi.ApiError;
import com.legend.warehouse.sqlapi.SqlApi.Chunk;
import com.legend.warehouse.sqlapi.SqlApi.Column;
import com.legend.warehouse.sqlapi.SqlApi.ErrorCode;
import com.legend.warehouse.sqlapi.SqlApi.ResultFormat;
import com.legend.warehouse.sqlapi.SqlApi.ResultMeta;
import com.legend.warehouse.sqlapi.SqlApi.State;
import com.legend.warehouse.sqlapi.SqlApi.StatementRequest;
import com.legend.warehouse.sqlapi.SqlApi.Status;
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
 * <p>IDENTITY, FROM OUTSIDE SQL: each statement gets its own connection
 * (a session keeps one), and the connection belongs to the verified
 * principal from the moment it opens: {@code system.main.authenticated_user()}
 * answers with it, and no statement can change it (docs/WAREHOUSE_FFM_HOMEWORK_2026_09_26.md §5).
 * Nothing a previous statement did survives, because no connection is ever reused.
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
        volatile List<byte[]> arrowChunks = List.of();
        /** The connection running it, while it runs: what a cancel interrupts. */
        volatile @Nullable Conn running;
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

        /** An Arrow result's chunk: a whole Arrow IPC stream. */
        public byte @Nullable [] arrowChunk(int index) {
            List<byte[]> cs = arrowChunks;
            return state == State.SUCCEEDED && index >= 0 && index < cs.size() ? cs.get(index) : null;
        }

        public ResultFormat format() {
            return request.format();
        }
    }

    public record Limits(int concurrency, int queue, long maxRows, Duration retain) {
    }

    private final Catalogs catalogs;
    private final Sessions sessions;
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

    public Statements(Catalogs catalogs, Sessions sessions, History history, Limits limits, Clock clock) {
        this.catalogs = catalogs;
        this.sessions = sessions;
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
        timers.scheduleWithFixedDelay(sessions::closeIdle, 60, 60, TimeUnit.SECONDS);
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
        Conn c = run.running;
        if (c != null) c.interrupt();
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
        String sessionId = run.request.sessionId();
        if (sessionId != null) {
            Sessions.Session s = sessions.find(run.principal, sessionId);
            if (s == null) {
                finish(run, State.FAILED, new ApiError(ErrorCode.NOT_FOUND, "no session " + sessionId));
                return;
            }
            // In the session's order, on its own connection, which stays open.
            s.lock.lock();
            try {
                s.lastUsed = clock.instant();
                if (run.cancelRequested) {
                    finish(run, run.timedOut ? State.FAILED : State.CANCELLED,
                            run.timedOut ? new ApiError(ErrorCode.TIMEOUT, "timed out while queued") : null);
                    return;
                }
                run.state = State.RUNNING;
                run.started = clock.instant();
                runOn(run, s.connection);
            } finally {
                s.lastUsed = clock.instant();
                s.lock.unlock();
            }
            return;
        }
        run.state = State.RUNNING;
        run.started = clock.instant();
        String catalog = run.request.catalog();
        Conn c;
        try {
            c = catalogs.connect(catalog, run.principal);
        } catch (DuckException e) {
            finish(run, State.FAILED, classify(e));
            return;
        }
        if (c == null) {
            finish(run, State.FAILED, new ApiError(ErrorCode.NOT_FOUND, "no catalog '" + catalog + "'"));
            return;
        }
        try {
            runOn(run, c);
        } finally {
            c.close();
        }
    }

    /** One statement (or script) on one connection, which already belongs to the run's principal. */
    private void runOn(Run run, Conn c) {
        run.running = c;
        try {
            if (run.cancelRequested) throw DuckException.cancelledBeforeStart();
            if (run.request.describeOnly()) {
                // What the statement will return, from DuckDB's prepare; nothing runs.
                run.result = new ResultMeta(ResultEncoder.api(c.describe(run.request.sql())), 0, 0);
                finish(run, State.SUCCEEDED, null);
                return;
            }
            try (Result r = c.execute(run.request.sql())) {
                if (r.hasRows()) {
                    collect(run, r);
                } else {
                    // The rows a write changed; -1 for a statement that changes none (DDL, SET).
                    run.result = new ResultMeta(List.of(), r.changed(), 0);
                }
            }
            finish(run, State.SUCCEEDED, null);
        } catch (ResultEncoder.Unsupported e) {
            finish(run, State.FAILED, new ApiError(ErrorCode.UNSUPPORTED_TYPE, String.valueOf(e.getMessage())));
        } catch (Collect.TooLarge e) {
            finish(run, State.FAILED, new ApiError(ErrorCode.TOO_LARGE, String.valueOf(e.getMessage())));
        } catch (DuckException e) {
            if (run.cancelRequested) {
                finish(run, run.timedOut ? State.FAILED : State.CANCELLED, run.timedOut
                        ? new ApiError(ErrorCode.TIMEOUT, "timed out after " + run.request.timeoutMs() + " ms")
                        : null);
            } else {
                finish(run, State.FAILED, classify(e));
            }
        } catch (Exception e) {
            finish(run, State.FAILED, new ApiError(ErrorCode.INTERNAL, String.valueOf(e.getMessage())));
        } finally {
            run.running = null;
        }
    }

    private void collect(Run run, Result r) throws Exception {
        List<Column> cols = ResultEncoder.api(r.columns());
        int per = Math.max(1, run.request.rowsPerChunk());
        if (run.request.format() == ResultFormat.ARROW) {
            Collect.Arrow a = Collect.arrow(r, per, limits.maxRows());
            run.arrowChunks = a.chunks();
            run.result = new ResultMeta(cols, a.rows(), a.chunks().size());
            return;
        }
        List<List<Json.Node>> rows = Collect.json(r, limits.maxRows());
        List<Chunk> chunks = new ArrayList<>();
        for (int from = 0; from < rows.size(); from += per) {
            chunks.add(new Chunk(chunks.size(), rows.subList(from, Math.min(rows.size(), from + per))));
        }
        if (chunks.isEmpty()) chunks.add(new Chunk(0, List.of()));
        run.chunks = List.copyOf(chunks);
        run.result = new ResultMeta(cols, rows.size(), chunks.size());
    }

    /** DuckDB's error kinds, as the API's codes. */
    static ApiError classify(DuckException e) {
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
