package com.legend.warehouse.server.duck;

import static java.lang.foreign.ValueLayout.ADDRESS;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;

/**
 * One DuckDB connection, belonging to one principal (see {@link Database#connect}).
 * Used by one thread at a time; {@link #interrupt} may come from any thread.
 */
public final class Conn implements AutoCloseable {

    private final Database db;
    private final Duck d;
    final MemorySegment handle;           // duckdb_connection
    final long id;                        // DuckDB's connection id: the identity function's key
    final MemorySegment arrowOptions;     // duckdb_arrow_options of this connection
    private volatile boolean closed;

    Conn(Database db, MemorySegment handle) {
        this.db = db;
        this.d = db.d;
        this.handle = handle;
        try (Arena a = Arena.ofConfined()) {
            MemorySegment ctx = a.allocate(ADDRESS);
            d.connectionContext.invokeExact(handle, ctx);
            this.id = (long) d.contextConnectionId.invokeExact(ctx.get(ADDRESS, 0));
            d.destroyContext.invokeExact(ctx);
            MemorySegment opts = a.allocate(ADDRESS);
            d.arrowOptions.invokeExact(handle, opts);
            this.arrowOptions = opts.get(ADDRESS, 0);
        } catch (Throwable t) {
            throw Duck.fail(t);
        }
    }

    /**
     * Runs {@code sql}: one statement, or a script split by DuckDB's own parser and run statement by
     * statement; the last statement's result is returned (as DuckDB's JDBC driver did). A syntax error
     * anywhere refuses the whole script before anything runs. The caller closes the result.
     */
    public Result execute(String sql) throws DuckException {
        List<MemorySegment> statements = prepareAll(sql, true);
        if (statements.isEmpty()) return Result.nothing(d);
        return runPrepared(statements.get(statements.size() - 1));
    }

    /**
     * The columns {@code sql} will return, without running it. In a script the leading statements do run
     * (the last one may depend on them), as DuckDB's JDBC driver's prepare did.
     */
    public List<Result.Column> describe(String sql) throws DuckException {
        List<MemorySegment> statements = prepareAll(sql, true);
        if (statements.isEmpty()) return List.of();
        MemorySegment ps = statements.get(statements.size() - 1);
        try {
            long n = (long) d.psColumnCount.invokeExact(ps);
            List<Result.Column> out = new ArrayList<>((int) n);
            for (long i = 0; i < n; i++) {
                String name = Duck.text((MemorySegment) d.psColumnName.invokeExact(ps, i));
                MemorySegment type = (MemorySegment) d.psColumnLogicalType.invokeExact(ps, i);
                try {
                    out.add(new Result.Column(name == null ? "" : name, TypeNames.of(d, type, db.keywords)));
                } finally {
                    destroyType(type);
                }
            }
            return out;
        } catch (Throwable t) {
            throw Duck.fail(t);
        } finally {
            destroyPrepared(ps);
        }
    }

    /**
     * Every statement of {@code sql}, prepared; with {@code runLeading}, each but the last is also run
     * (its result discarded) before the next is prepared, since a later statement may depend on it.
     */
    private List<MemorySegment> prepareAll(String sql, boolean runLeading) throws DuckException {
        try (Arena a = Arena.ofConfined()) {
            MemorySegment extracted = a.allocate(ADDRESS);
            long n = (long) d.extract.invokeExact(handle, a.allocateFrom(sql), extracted);
            MemorySegment ex = extracted.get(ADDRESS, 0);
            try {
                if (n == 0) {
                    String e = Duck.text((MemorySegment) d.extractError.invokeExact(ex));
                    if (e != null) throw new DuckException(-1, e);
                    return List.of();
                }
                List<MemorySegment> out = new ArrayList<>();
                for (long i = 0; i < n; i++) {
                    MemorySegment psOut = a.allocate(ADDRESS);
                    int state = (int) d.prepareExtracted.invokeExact(handle, ex, i, psOut);
                    MemorySegment ps = psOut.get(ADDRESS, 0);
                    if (state != 0) {
                        String e = Duck.text((MemorySegment) d.prepareError.invokeExact(ps));
                        destroyPrepared(ps);
                        throw new DuckException(-1, e == null ? "could not prepare the statement" : e);
                    }
                    if (runLeading && i < n - 1) {
                        runPrepared(ps).close();
                    } else {
                        out.add(ps);
                    }
                }
                return out;
            } finally {
                d.destroyExtracted.invokeExact(extracted);
            }
        } catch (DuckException e) {
            throw e;
        } catch (Throwable t) {
            throw Duck.fail(t);
        }
    }

    /** Executes a prepared statement; the result owns (and destroys) it. */
    private Result runPrepared(MemorySegment ps) throws DuckException {
        Arena arena = Arena.ofConfined();
        MemorySegment res = arena.allocate(Duck.RESULT);
        try {
            int state = (int) d.executePrepared.invokeExact(ps, res);
            if (state != 0) {
                String m = Duck.text((MemorySegment) d.resultError.invokeExact(res));
                int kind = (int) d.resultErrorType.invokeExact(res);
                d.destroyResult.invokeExact(res);
                destroyPrepared(ps);
                arena.close();
                throw new DuckException(kind, m == null ? "the statement failed" : m);
            }
            return new Result(d, arena, res, ps, arrowOptions, db.keywords);
        } catch (DuckException e) {
            throw e;
        } catch (Throwable t) {
            arena.close();
            throw Duck.fail(t);
        }
    }

    /**
     * Runs one statement with parameters ({@code ?}), each a String, a Long or null; the caller closes
     * the result. For the warehouse's own tables (history), never a user's SQL.
     */
    public Result execute(String sql, @com.legend.Nullable Object... params) throws DuckException {
        MemorySegment ps;
        try (Arena a = Arena.ofConfined()) {
            MemorySegment out = a.allocate(ADDRESS);
            int state = (int) d.prepare.invokeExact(handle, a.allocateFrom(sql), out);
            ps = out.get(ADDRESS, 0);
            if (state != 0) {
                String e = Duck.text((MemorySegment) d.prepareError.invokeExact(ps));
                destroyPrepared(ps);
                throw new DuckException(-1, e == null ? "could not prepare " + sql : e);
            }
            for (int i = 0; i < params.length; i++) {
                Object p = params[i];
                long at = i + 1;
                int st;
                if (p == null) st = (int) d.bindNull.invokeExact(ps, at);
                else if (p instanceof Long x) st = (int) d.bindInt64.invokeExact(ps, at, (long) x);
                else st = (int) d.bindVarchar.invokeExact(ps, at, a.allocateFrom(p.toString()));
                if (st != 0) {
                    destroyPrepared(ps);
                    throw new DuckException(-1, "could not bind parameter " + at + " of " + sql);
                }
            }
        } catch (DuckException e) {
            throw e;
        } catch (Throwable t) {
            throw Duck.fail(t);
        }
        return runPrepared(ps);
    }

    /** Runs internal SQL (one statement or several) and discards the result. */
    void exec(String sql) throws DuckException {
        try (Arena a = Arena.ofConfined()) {
            MemorySegment res = a.allocate(Duck.RESULT);
            int state = (int) d.query.invokeExact(handle, a.allocateFrom(sql), res);
            try {
                if (state != 0) {
                    String m = Duck.text((MemorySegment) d.resultError.invokeExact(res));
                    throw new DuckException((int) d.resultErrorType.invokeExact(res), m == null ? sql : m);
                }
            } finally {
                d.destroyResult.invokeExact(res);
            }
        } catch (DuckException e) {
            throw e;
        } catch (Throwable t) {
            throw Duck.fail(t);
        }
    }

    /** Stops what this connection is running (from any thread); it fails with an INTERRUPT error. */
    public void interrupt() {
        if (closed) return;
        try {
            d.interrupt.invokeExact(handle);
        } catch (Throwable t) {
            throw Duck.fail(t);
        }
    }

    void destroyPrepared(MemorySegment ps) {
        try (Arena a = Arena.ofConfined()) {
            MemorySegment p = a.allocate(ADDRESS);
            p.set(ADDRESS, 0, ps);
            d.destroyPrepare.invokeExact(p);
        } catch (Throwable t) {
            throw Duck.fail(t);
        }
    }

    void destroyType(MemorySegment type) {
        try (Arena a = Arena.ofConfined()) {
            MemorySegment p = a.allocate(ADDRESS);
            p.set(ADDRESS, 0, type);
            d.destroyLogicalType.invokeExact(p);
        } catch (Throwable t) {
            throw Duck.fail(t);
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        AuthenticatedUser.unbind(db.number, id);
        try (Arena a = Arena.ofConfined()) {
            MemorySegment p = a.allocate(ADDRESS);
            p.set(ADDRESS, 0, arrowOptions);
            d.destroyArrowOptions.invokeExact(p);
        } catch (Throwable t) {
            throw Duck.fail(t);
        }
        db.disconnect(handle);
    }
}
