package com.legend.warehouse.server;

import com.legend.base.Nullable;
import com.legend.warehouse.server.duck.Conn;
import com.legend.warehouse.server.duck.DuckException;
import com.legend.warehouse.server.duck.DuckLibrary;
import com.legend.warehouse.sqlapi.SqlApi;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Sessions: a connection PINNED to one user and one catalog, so what a
 * statement leaves behind -- {@code USE}, {@code SET}, temp tables, an open
 * transaction -- is there for the next one. A session's statements run one
 * at a time, in the order they were submitted (its lock is fair).
 *
 * <p>The session's connection belongs to its user from the moment it opens:
 * {@code system.main.authenticated_user()} answers with it, from outside SQL,
 * and no statement on the session can change it.
 */
public final class Sessions implements AutoCloseable {

    /** One session. */
    public static final class Session {
        final String id = UUID.randomUUID().toString();
        final String principal;
        final String catalog;
        final Conn connection;
        final ReentrantLock lock = new ReentrantLock(true);
        volatile Instant lastUsed;

        Session(String principal, String catalog, Conn connection, Instant now) {
            this.principal = principal;
            this.catalog = catalog;
            this.connection = connection;
            this.lastUsed = now;
        }

        /** The session as the API reports it: its engine is DuckDB, at the library's own version. */
        public SqlApi.Session api() {
            return new SqlApi.Session(id, catalog, "DuckDB", DuckLibrary.version());
        }

        public String id() {
            return id;
        }

        public String catalog() {
            return catalog;
        }
    }

    private final Catalogs catalogs;
    private final Duration idle;
    private final Clock clock;
    private final Map<String, Session> open = new ConcurrentHashMap<>();

    public Sessions(Catalogs catalogs, Duration idle, Clock clock) {
        this.catalogs = catalogs;
        this.idle = idle;
        this.clock = clock;
    }

    /** A new session on the catalog, or null when there is no such catalog. */
    public @Nullable Session open(String principal, String catalog) throws DuckException {
        Conn c = catalogs.connect(catalog, principal);
        if (c == null) return null;
        Session s = new Session(principal, catalog, c, clock.instant());
        open.put(s.id, s);
        return s;
    }

    /** The session, if it exists and belongs to this principal. Others' sessions do not exist to you. */
    public @Nullable Session find(String principal, String id) {
        Session s = open.get(id);
        return s != null && s.principal.equals(principal) ? s : null;
    }

    public void close(Session s) {
        open.remove(s.id);
        s.lock.lock();
        try {
            s.connection.close();
        } finally {
            s.lock.unlock();
        }
    }

    /** Close the sessions idle longer than the limit and not in use. */
    void closeIdle() {
        Instant cutoff = clock.instant().minus(idle);
        for (Session s : open.values()) {
            if (s.lastUsed.isBefore(cutoff) && s.lock.tryLock()) {
                try {
                    if (s.lastUsed.isBefore(cutoff)) close(s);
                } finally {
                    s.lock.unlock();
                }
            }
        }
    }

    public int count() {
        return open.size();
    }

    @Override
    public void close() {
        for (Session s : open.values()) close(s);
    }
}
