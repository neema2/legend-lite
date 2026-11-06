package com.legend.driver;

import com.legend.compiler.element.ModelContext;
import com.legend.exec.Sessions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * THE system database of a compiled graph (user ruling 2026-09-02; the driver's since D72, 2026-10-30 -- it is
 * keyed by the compiled graph; it holds its sessions through the seam's public doors): the
 * metamodel rows live in a database of their own, SEPARATE from every user
 * connection &mdash; one per graph per database engine, written ONCE, by the
 * plan's own seed step the first time a plan of that graph reads the metamodel
 * ({@code SystemRoutes.seeds}, run by the plan runner and recorded on the session;
 * the walker-deletion program, 2026-11-03: until then this holder wrote it as it
 * opened the session, a send outside every plan), alive as long as
 * the graph ({@link ModelContext#derived}). Nothing is seeded per
 * execution any more: the executor ROUTES a store-reading body to this
 * connection (the corpus re-seeded ~20 tables of a corpus-sized graph on
 * every store-reading test &mdash; docs/GATES.md, 2026-09-02 budget entry).
 *
 * <p>The GRAPH's rows are a pure function of the compiled graph, derived
 * once per table by the planner ({@code SystemRoutes.seeds}). Nothing else is ever written: a
 * query's constructed instances ({@code ^DynaFunction(...)} trees) ride
 * the query itself as inline relations.
 *
 * <p>The engine is the database the query's runtime DECLARES (an H2
 * lane keeps exercising the metamodel queries on H2 &mdash; the 21-kind
 * union hang was an H2 finding), opened private by {@link Sessions}; the
 * connection is the ONE in-memory database of that engine.
 *
 * <p>A SERVER engine (Postgres, MariaDB, MySQL, Snowflake, Databricks,
 * BigQuery, ...) has no in-memory instance: its system database is the
 * HOST's SYSTEM CONNECTION for that engine (2026-10-11, the user's ruling
 * -- the same design over a remote database), configured apart from every
 * runtime's connection ({@link Sessions.Opener}: the platform holds no
 * server credential; a test may give it the same credentials, a host need
 * not). It is checked to BE that engine, since the query is planned in the
 * runtime's dialect. The same lifecycle: opened once per graph, every
 * table written then, read-only after. Each table is TEMPORARY, the
 * session's own (the private instance's guarantees on a shared server: no
 * other session sees it, two graphs or two runs never collide, and the
 * server drops it when the session ends, a crash included), named
 * {@link Ddl#systemTable}; the query reads it through its table renames
 * ({@link Route#renames}).
 */
public final class SystemDatabase {

    private static final java.lang.ref.Cleaner CLEANER = java.lang.ref.Cleaner.create();

    /** The Cleaner action: holds the open sessions, NEVER the store
     * (a self-reference would keep the graph alive). */
    private static final class Closer implements Runnable {
        private final List<Sessions.Session> open =
                java.util.Collections.synchronizedList(new ArrayList<>());

        @Override
        public void run() {
            for (Sessions.Session s : open) {
                try {
                    s.close();
                } catch (com.legend.error.DataError ignore) {
                    // closing a dead in-memory session: nothing to report
                }
            }
        }
    }

    private final Map<com.legend.model.ConnectionDefinition.DatabaseType, Sessions.Session> sessions = new HashMap<>();
    private final Closer closer = new Closer();

    private SystemDatabase() {
        CLEANER.register(this, closer);
    }

    /** The graph's system store (created on first ask). */
    public static SystemDatabase of(ModelContext ctx) {
        return ctx.derived(SystemDatabase.class, c -> new SystemDatabase());
    }

    /**
     * The graph's system database for {@code engine}, OPENED ON FIRST USE ({@link Sessions#lazy}; THE WALL,
     * 2026-11-06: until then the router opened it as it PLANNED a system-routed body's asserts, a database touched by
     * planning): an in-process engine's private database, a server engine's the host's system connection for it, from
     * {@code systemConnections} (a server engine with none is refused by name when a run first reaches it, never
     * routed to a user's session). The same session object on every ask, so a planner may name it -- the target its
     * statements carry -- and open nothing. The holder writes nothing: the store's tables and rows are the plan's own
     * seed step ({@code SystemRoutes.seeds}), which the runner runs on this session once (the walker-deletion program,
     * 2026-11-03: planning sends nothing).
     */
    public synchronized Sessions.Session sessionFor(com.legend.model.ConnectionDefinition.DatabaseType engine,
            Map<com.legend.model.ConnectionDefinition.DatabaseType, Sessions.Opener> systemConnections) {
        Sessions.Session s = sessions.get(engine);
        if (s == null) {
            Map<com.legend.model.ConnectionDefinition.DatabaseType, Sessions.Opener> conns =
                    java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(systemConnections));
            s = Sessions.lazy(() -> Sessions.inProcess(engine) ? openPrivate(engine) : openSystem(engine, conns));
            closer.open.add(s);
            sessions.put(engine, s);
        }
        return s;
    }

    /** The engine whose system database {@code session} is (the very session object {@link #sessionFor} answered), or
     *  null for any other session: how a body environment on the system database is known (the test-is-the-product
     *  plan's M10a: its sides' statements then name the system target, {@code JudgePlans.target}). */
    public synchronized com.legend.model.ConnectionDefinition.@com.legend.base.Nullable DatabaseType engineOf(
            Sessions.Session session) {
        for (Map.Entry<com.legend.model.ConnectionDefinition.DatabaseType, Sessions.Session> e : sessions.entrySet()) {
            if (e.getValue() == session) {
                return e.getKey();
            }
        }
        return null;
    }

    private static void closeQuietly(Sessions.Session c) {
        try {
            c.close();
        } catch (com.legend.error.DataError ignore) {
            // closing after a failure: the failure is the error to report
        }
    }

    private static Sessions.Session openPrivate(com.legend.model.ConnectionDefinition.DatabaseType engine) {
        try {
            return Sessions.openPrivate(engine);
        } catch (com.legend.error.DataError e) {
            throw new IllegalStateException("system database: cannot open the "
                    + engine + " session", e);
        }
    }

    /** The host's system connection for {@code engine}, checked to be on {@code engine} (the query is planned in its
     *  runtime's dialect): another database is refused, never written to. */
    private static Sessions.Session openSystem(com.legend.model.ConnectionDefinition.DatabaseType engine,
            Map<com.legend.model.ConnectionDefinition.DatabaseType, Sessions.Opener> systemConnections) {
        Sessions.Opener opener = systemConnections.get(engine);
        if (opener == null) {
            throw new com.legend.error.NotImplementedException("no system connection for " + engine
                    + " is configured: a query reading the system metamodel store on a server engine runs on the"
                    + " host's system connection, apart from every user connection (ExecuteOptions"
                    + ".withSystemConnections)");
        }
        Sessions.Session c;
        try {
            c = Sessions.open(opener);
        } catch (com.legend.error.DataError e) {
            throw new IllegalStateException("system database: cannot open the " + engine
                    + " system connection: " + e.getMessage(), e);
        }
        boolean checked = false;
        try {
            Sessions.check(c, engine);
            checked = true;
        } finally {
            if (!checked) {
                closeQuietly(c);
            }
        }
        return c;
    }
}
