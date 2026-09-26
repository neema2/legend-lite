package com.legend.warehouse.server;

import com.legend.base.Nullable;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        /** The result's chunks (JSON or Arrow, already written), once it has some. */
        volatile ResultStore.@Nullable Stored stored;
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

        /** The status document, without its rows (a JSON result's first chunk is spliced in by the server). */
        public Status status() {
            State s = state;
            return new Status(id, s, s == State.SUCCEEDED ? result : null, error, null);
        }

        /** Chunk {@code index}, as written (JSON rows, or a whole Arrow stream); null when there is none. */
        public byte @Nullable [] chunk(int index) {
            ResultStore.Stored s = stored;
            return state == State.SUCCEEDED && s != null ? s.get(index) : null;
        }

        public ResultFormat format() {
            return request.format();
        }
    }

    /**
     * {@code resultMemory}: bytes of finished results held in memory, across every result; past it,
     * results spill to files ({@link ResultStore}).
     */
    /** The server's own principal: its internal statements (the catalog API) run as it, an owner. */
    public static final String SERVER = "warehouse";

    /** Who may do what: owners anything; every other user a reader, checked by the authorizer. */
    public record Access(List<String> owners, Grants grants, Authorizer authorizer) {
    }

    public record Limits(int concurrency, int queue, long maxRows, Duration retain, long resultMemory) {
        public static final long DEFAULT_RESULT_MEMORY = 1L << 30;

        public Limits(int concurrency, int queue, long maxRows, Duration retain) {
            this(concurrency, queue, maxRows, retain, DEFAULT_RESULT_MEMORY);
        }
    }

    private final Catalogs catalogs;
    private final Sessions sessions;
    private final ResultStore results;
    private final Access access;
    private final Set<String> owners;
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

    public Statements(Catalogs catalogs, Sessions sessions, ResultStore results, History history, Access access,
            Limits limits, Clock clock) {
        this.results = results;
        this.access = access;
        Set<String> o = new HashSet<>();
        for (String owner : access.owners()) o.add(Grants.norm(owner));
        o.add(SERVER);
        this.owners = Set.copyOf(o);
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

    public boolean isOwner(String principal) {
        return owners.contains(Grants.norm(principal));
    }

    /**
     * One statement (or script) on one connection, which already belongs to the run's principal. An
     * owner's statement runs as written, or manages grants; a reader's must pass the authorizer first.
     */
    private void runOn(Run run, Conn c) {
        run.running = c;
        try {
            if (run.cancelRequested) throw DuckException.cancelledBeforeStart();
            boolean owner = isOwner(run.principal);
            AdminStatements.Admin admin = AdminStatements.parse(run.request.sql(), run.request.catalog());
            if (admin != null && !owner) {
                finish(run, State.FAILED, new ApiError(ErrorCode.FORBIDDEN, "only an owner may manage grants"));
                return;
            }
            if (admin != null && !(admin instanceof AdminStatements.ShowGrants)) {
                if (!run.request.describeOnly()) administer(admin);   // describing a change makes none
                run.result = new ResultMeta(List.of(), run.request.describeOnly() ? 0 : -1, 0);
                finish(run, State.SUCCEEDED, null);
                return;
            }
            if (!owner) {
                try {
                    access.authorizer().check(c, run.request.sql(), run.request.catalog(), run.principal);
                } catch (Authorizer.Denied d) {
                    finish(run, State.FAILED, new ApiError(d.parse ? ErrorCode.SQL_PARSE : ErrorCode.FORBIDDEN,
                            String.valueOf(d.getMessage())));
                    return;
                }
            }
            String sql = admin != null ? showGrants() : run.request.sql();
            if (run.request.describeOnly()) {
                // What the statement will return, from DuckDB's prepare; nothing runs.
                run.result = new ResultMeta(ResultEncoder.api(c.describe(sql)), 0, 0);
                finish(run, State.SUCCEEDED, null);
                return;
            }
            try (Result r = c.execute(sql)) {
                if (r.hasRows()) {
                    collect(run, r);
                } else {
                    // The rows a write changed; -1 for a statement that changes none (DDL, SET).
                    run.result = new ResultMeta(List.of(), r.changed(), 0);
                }
            }
            finish(run, State.SUCCEEDED, null);
        } catch (IllegalArgumentException e) {
            finish(run, State.FAILED, new ApiError(ErrorCode.BAD_REQUEST, String.valueOf(e.getMessage())));
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
        ResultStore.Stored into = results.open(run.id);
        try {
            // each chunk goes to the store as it is written: a large result never sits in memory whole
            long rows = run.request.format() == ResultFormat.ARROW
                    ? Collect.arrow(r, per, limits.maxRows(), run.request.cellText(), into::add)
                    : Collect.jsonChunks(r, per, limits.maxRows(), into::add);
            run.stored = into;
            run.result = new ResultMeta(cols, rows, into.count());
        } catch (Exception e) {
            into.free();
            throw e;
        }
    }

    private void administer(AdminStatements.Admin admin) throws DuckException {
        Grants g = access.grants();
        switch (admin) {
            case AdminStatements.CreateRole r -> g.createRole(r.role());
            case AdminStatements.DropRole r -> g.dropRole(r.role());
            case AdminStatements.Select s -> {
                if (s.grant()) g.grantSelect(s.target());
                else g.revokeSelect(s.target());
            }
            case AdminStatements.Membership m -> {
                if (m.grant()) g.grantRole(m.role(), m.member());
                else g.revokeRole(m.role(), m.member());
            }
            case AdminStatements.ShowGrants s -> throw new IllegalStateException("SHOW GRANTS is a query");
        }
    }

    /** SHOW GRANTS: a query over the grants, so it answers like any result, in either format. */
    private String showGrants() {
        List<Grants.Grant> all = access.grants().all();
        StringBuilder sb = new StringBuilder("SELECT * FROM (VALUES ");
        if (all.isEmpty()) sb.append("(NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR)");
        for (int i = 0; i < all.size(); i++) {
            Grants.Grant g = all.get(i);
            sb.append(i == 0 ? "(" : ", (").append(lit(g.catalog())).append(", ").append(lit(g.schema()))
                    .append(", ").append(lit(g.name())).append(", ").append(lit(g.grantee())).append(')');
        }
        sb.append(") AS g(catalog, schema_name, name, grantee)");
        return all.isEmpty() ? sb.append(" WHERE false").toString() : sb.append(" ORDER BY ALL").toString();
    }

    private static String lit(String s) {
        return "'" + s.replace("'", "''") + "'";
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
        for (Run r : runs.values()) {
            Instant f = r.finished;
            if (f != null && f.isBefore(cutoff)) forget(r);
        }
    }

    /** The statement is done with: its result freed (memory given back, files deleted), the statement gone. */
    public void forget(Run run) {
        if (!run.state.done()) cancel(run);
        runs.remove(run.id);
        ResultStore.Stored s = run.stored;
        if (s != null) s.free();
    }

    @Override
    public void close() {
        workers.shutdownNow();
        timers.shutdownNow();
    }
}
