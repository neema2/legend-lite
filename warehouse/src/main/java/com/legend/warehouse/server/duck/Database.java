package com.legend.warehouse.server.duck;

import static java.lang.foreign.ValueLayout.ADDRESS;

import com.legend.base.Nullable;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * One DuckDB database (a file, or in memory), through the C API. It carries the
 * identity function; every connection opened from it belongs to one principal.
 */
public final class Database implements AutoCloseable {

    final Duck d;
    private final MemorySegment handle;   // duckdb_database
    final long number;                    // the identity function's number for this database
    final Set<String> keywords;
    private boolean closed;

    private Database(Duck d, MemorySegment handle) throws DuckException {
        this.d = d;
        this.handle = handle;
        MemorySegment owner = rawConnect();
        try {
            this.number = AuthenticatedUser.register(d, owner);
            this.keywords = keywords(owner);
        } finally {
            disconnect(owner);
        }
    }

    /** Opens the database at {@code file} (created if absent), or an in-memory one for null. */
    public static Database open(@Nullable Path file) throws DuckException {
        Duck d = Duck.api();
        try (Arena a = Arena.ofConfined()) {
            MemorySegment out = a.allocate(ADDRESS), error = a.allocate(ADDRESS);
            MemorySegment path = file == null ? MemorySegment.NULL : a.allocateFrom(file.toString());
            int state = (int) d.openExt.invokeExact(path, out, MemorySegment.NULL, error);
            if (state != 0) {
                MemorySegment e = error.get(ADDRESS, 0);
                throw new DuckException(-1, e.equals(MemorySegment.NULL) ? "could not open " + file : d.owned(e));
            }
            return new Database(d, out.get(ADDRESS, 0));
        } catch (DuckException e) {
            throw e;
        } catch (Throwable t) {
            throw Duck.fail(t);
        }
    }

    /**
     * A connection belonging to {@code principal}: what {@code authenticated_user()} answers on it, and
     * what its {@code current_user} and {@code session_user} show.
     */
    public Conn connect(String principal) throws DuckException {
        MemorySegment c = rawConnect();
        Conn conn = new Conn(this, c);
        AuthenticatedUser.bind(number, conn.id, principal);
        try {
            conn.exec("CREATE TEMP MACRO current_user() AS system.main." + AuthenticatedUser.NAME + "();"
                    + " CREATE TEMP MACRO session_user() AS system.main." + AuthenticatedUser.NAME + "()");
        } catch (DuckException e) {
            conn.close();
            throw e;
        }
        return conn;
    }

    /**
     * Closes the database off from the machine, for good (measured on 1.5.5: nothing turns it back on
     * while the database runs): files and URLs, ATTACH, COPY, INSTALL and LOAD are refused, except
     * files under {@code importDir} (an owner loads data from there), when given. Settings stay open:
     * an owner's session sets its TimeZone; a reader can only send a SELECT.
     */
    public void lockDown(@Nullable Path importDir) throws DuckException {
        Conn c = new Conn(this, rawConnect());
        try {
            // the allowed directories first: they cannot change once external access is off
            if (importDir != null) {
                c.exec("SET allowed_directories = ['" + importDir.toAbsolutePath().toString().replace("'", "''") + "']");
            }
            c.exec("SET enable_external_access = false");
        } finally {
            c.close();
        }
    }

    private MemorySegment rawConnect() throws DuckException {
        try (Arena a = Arena.ofConfined()) {
            MemorySegment out = a.allocate(ADDRESS);
            int state;
            synchronized (this) {
                if (closed) throw new DuckException(-1, "the database is closed");
                state = (int) d.connect.invokeExact(handle, out);
            }
            if (state != 0) throw new DuckException(-1, "could not connect");
            return out.get(ADDRESS, 0);
        } catch (DuckException e) {
            throw e;
        } catch (Throwable t) {
            throw Duck.fail(t);
        }
    }

    void disconnect(MemorySegment c) {
        try (Arena a = Arena.ofConfined()) {
            MemorySegment p = a.allocate(ADDRESS);
            p.set(ADDRESS, 0, c);
            d.disconnect.invokeExact(p);
        } catch (Throwable t) {
            throw Duck.fail(t);
        }
    }

    /** DuckDB's keywords, every category: a STRUCT field named like one is quoted in a type name. */
    private Set<String> keywords(MemorySegment owner) throws DuckException {
        Set<String> out = new HashSet<>();
        try (Arena a = Arena.ofConfined()) {
            MemorySegment res = a.allocate(Duck.RESULT);
            int state = (int) d.query.invokeExact(owner, a.allocateFrom("SELECT keyword_name FROM duckdb_keywords()"), res);
            try {
                if (state != 0) throw new DuckException(-1, "could not read DuckDB's keywords");
                long rows = (long) d.rowCount.invokeExact(res);
                for (long i = 0; i < rows; i++) {
                    out.add(d.owned((MemorySegment) d.valueVarchar.invokeExact(res, 0L, i)).toLowerCase(Locale.ROOT));
                }
            } finally {
                d.destroyResult.invokeExact(res);
            }
        } catch (DuckException e) {
            throw e;
        } catch (Throwable t) {
            throw Duck.fail(t);
        }
        return Set.copyOf(out);
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        AuthenticatedUser.forget(number);
        try (Arena a = Arena.ofConfined()) {
            MemorySegment p = a.allocate(ADDRESS);
            p.set(ADDRESS, 0, handle);
            d.close.invokeExact(p);
        } catch (Throwable t) {
            throw Duck.fail(t);
        }
    }
}
