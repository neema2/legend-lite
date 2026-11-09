package com.legend.driver;

import com.legend.exec.Sessions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * THE system database of a graph (user ruling 2026-09-02; the driver's since D72, 2026-10-30 -- keyed by the
 * graph's store key the plan names, never the compiled graph, since hole 2; it holds its sessions through the seam's
 * public doors): the
 * metamodel rows live in a database of their own, SEPARATE from every user
 * connection &mdash; one per graph per database engine, written ONCE, by the
 * plan's own seed step the first time a plan of that graph reads the metamodel
 * ({@code SystemRoutes.seeds}, run by the plan runner and recorded on the session;
 * the walker-deletion program, 2026-11-03: until then this holder wrote it as it
 * opened the session, a send outside every plan), the current
 * one of its engine until a run names another graph's (hole 2, 2026-11-09). Nothing is seeded per
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
 * runtime's dialect. The same lifecycle: opened once per current graph, every
 * table written then, read-only after. Each table is TEMPORARY, the
 * session's own (the private instance's guarantees on a shared server: no
 * other session sees it, two graphs or two runs never collide, and the
 * server drops it when the session ends, a crash included), named
 * {@link Ddl#systemTable}; the query reads it through its table renames
 * ({@link Route#renames}).
 */
public final class SystemDatabase {

    /** The CURRENT system database of each engine (the plan-as-data design's hole 2, 2026-11-09): the graph key it
     *  holds, its session and the runs using it. A run naming another graph's store replaces it -- a fresh session
     *  opened lazily, the old one closed when the last run using it ends -- so the process holds one per engine plus
     *  those still in use, found by the plan's own target, never the compiled model (until hole 2 one per graph, kept in
     *  the graph's derived cache: the run reached the model for it). */
    private static final Map<com.legend.executionplan.DatabaseType, Held> CURRENT = new HashMap<>();

    /** A system database's session and its leases; guarded by the class's lock. */
    private static final class Held {
        final String graphKey;
        final Sessions.Session session;
        int leases;
        boolean replaced;

        Held(String graphKey, Sessions.Session session) {
            this.graphKey = graphKey;
            this.session = session;
        }
    }

    private SystemDatabase() {
    }

    /** A run's leases of the system databases its plan names, opened with the host's {@code systemConnections}
     *  (a server engine's); closed when the run ends. */
    public static Leases leases(Map<com.legend.model.ConnectionDefinition.DatabaseType, Sessions.Opener> systemConnections) {
        return new Leases(java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(systemConnections)));
    }

    /** One run's leases: each system database its steps reach, held until {@link #close}. */
    public static final class Leases implements AutoCloseable {
        private final Map<com.legend.model.ConnectionDefinition.DatabaseType, Sessions.Opener> systemConnections;
        private final List<Held> held = new ArrayList<>();

        private Leases(Map<com.legend.model.ConnectionDefinition.DatabaseType, Sessions.Opener> systemConnections) {
            this.systemConnections = systemConnections;
        }

        /**
         * The system database for {@code system} -- the engine and the graph's store key the plan names -- OPENED ON
         * FIRST USE ({@link Sessions#lazy}): an in-process engine's private database, a server engine's the host's
         * system connection for it (a server engine with none is refused by name when a run first reaches it, never
         * routed to a user's session). The same session for the same key; another key replaces the engine's current
         * one. The holder writes nothing: the store's tables and rows are the plan's own seed step
         * ({@code SystemRoutes.seeds}), which the runner runs on a session once.
         */
        public Sessions.Session sessionFor(com.legend.executionplan.ExecutionPlan.Database.System system) {
            synchronized (SystemDatabase.class) {
                com.legend.executionplan.DatabaseType engine = system.type();
                Held current = CURRENT.get(engine);
                if (current == null || !current.graphKey.equals(system.graphKey())) {
                    if (current != null) {
                        current.replaced = true;
                        if (current.leases == 0) {
                            closeQuietly(current.session);
                        }
                    }
                    Map<com.legend.model.ConnectionDefinition.DatabaseType, Sessions.Opener> conns = systemConnections;
                    current = new Held(system.graphKey(), Sessions.lazy(() -> Sessions.inProcess(engine)
                            ? openPrivate(engine) : openSystem(engine, conns)));
                    CURRENT.put(engine, current);
                }
                if (!held.contains(current)) {
                    current.leases++;
                    held.add(current);
                }
                return current.session;
            }
        }

        @Override
        public void close() {
            synchronized (SystemDatabase.class) {
                for (Held h : held) {
                    h.leases--;
                    if (h.replaced && h.leases == 0) {
                        closeQuietly(h.session);
                    }
                }
                held.clear();
            }
        }
    }

    private static void closeQuietly(Sessions.Session c) {
        try {
            c.close();
        } catch (com.legend.error.DataError ignore) {
            // closing after a failure: the failure is the error to report
        }
    }

    private static Sessions.Session openPrivate(com.legend.executionplan.DatabaseType engine) {
        try {
            return Sessions.openPrivate(engine);
        } catch (com.legend.error.DataError e) {
            throw new IllegalStateException("system database: cannot open the "
                    + engine + " session", e);
        }
    }

    /** The host's system connection for {@code engine}, checked to be on {@code engine} (the query is planned in its
     *  runtime's dialect): another database is refused, never written to. */
    private static Sessions.Session openSystem(com.legend.executionplan.DatabaseType engine,
            Map<com.legend.model.ConnectionDefinition.DatabaseType, Sessions.Opener> systemConnections) {
        // the host's system connections are configured by the model's name of the engine (ExecuteOptions)
        Sessions.Opener opener = systemConnections.get(com.legend.planner.ConnectionPlans.model(engine));
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
