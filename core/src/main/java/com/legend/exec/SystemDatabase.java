// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * THE system database of a graph (user ruling 2026-09-02; exec's since slice 5, 2026-11-09, the driver's from D72 -- keyed by the
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
 * {@code Ddl.systemTable}; the query reads it through its table renames
 * ({@code Route.renames}).
 */
public final class SystemDatabase {

    /** The CURRENT system database of each engine (the plan-as-data design's hole 2, 2026-11-09): the key it holds, its
     *  session and the runs using it. On a database the platform runs in process it is the process's CATALOG (L4b,
     *  2026-11-09), keyed by the platform's content, so it is made once and never replaced; on a server engine, the
     *  graph's own, which a run naming another graph's store replaces -- a fresh session opened lazily, the old one
     *  closed when the last run using it ends. Found by the plan's own target, never the compiled model. */
    private static final Map<com.legend.executionplan.DatabaseType, Held> CURRENT = new HashMap<>();

    /** A system database's session and its leases; guarded by the class's lock. A catalog's (a target with loads) is
     *  made with its target's ONCE loads -- the store's tables and the platform's rows -- and taken by one run at a time
     *  ({@link #turn}), each run's registration inside a transaction rolled back when its leases close. */
    private static final class Held {
        final String key;
        final Sessions.Session session;
        final boolean catalog;
        final java.util.concurrent.locks.ReentrantLock turn = new java.util.concurrent.locks.ReentrantLock();
        int leases;
        boolean replaced;
        /** Guarded by {@link #turn}: the catalog's ONCE loads ran ({@code made}), or failed part-way ({@code broken}). */
        boolean made;
        boolean broken;

        Held(String key, Sessions.Session session, boolean catalog) {
            this.key = key;
            this.session = session;
            this.catalog = catalog;
        }
    }

    private SystemDatabase() {
    }

    /** A run's leases of the system databases its plan names, opened with the host's {@code systemConnections}
     *  (a server engine's); closed when the run ends. */
    public static Leases leases(Map<com.legend.executionplan.DatabaseType, Sessions.Opener> systemConnections) {
        return new Leases(java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(systemConnections)));
    }

    /** One run's leases: each system database its steps reach, held until {@link #close}, and its turns on the catalogs
     *  it reached, rolled back then. */
    public static final class Leases implements AutoCloseable {
        private final Map<com.legend.executionplan.DatabaseType, Sessions.Opener> systemConnections;
        private final List<Held> held = new ArrayList<>();
        private final List<Held> turns = new ArrayList<>();

        private Leases(Map<com.legend.executionplan.DatabaseType, Sessions.Opener> systemConnections) {
            this.systemConnections = systemConnections;
        }

        /**
         * The system database {@code target} names -- the engine and the key of what it holds -- OPENED ON FIRST USE
         * ({@link Sessions#lazy}): an in-process engine's private database, a server engine's the host's system
         * connection for it (a server engine with none is refused by name when a run first reaches it, never routed to
         * a user's session). The same session for the same target. A target with loads names the process's CATALOG:
         * made once with its ONCE loads, then this run's turn taken on it -- one run at a time, inside a transaction
         * rolled back when the run ends ({@link #close}; docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md 4.5). The holder
         * writes nothing of its own: every row is a load of the plan's.
         */
        public Sessions.Session sessionFor(com.legend.executionplan.ExecutionPlan.Target target) {
            if (!(target.database() instanceof com.legend.executionplan.ExecutionPlan.Database.System system)) {
                throw new IllegalArgumentException("a system database's lease for " + target.database());
            }
            boolean catalog = !target.loads().isEmpty();
            StringBuilder key = new StringBuilder(system.graphKey());
            // the catalog is named by what it is made with (its ONCE loads): a product query's model registration
            // (PER_ACQUISITION, L4's product half) is the run's own, run in its turn, and names no other catalog
            for (var load : target.loads()) {
                if (load.cadence() == com.legend.executionplan.ExecutionPlan.Load.Cadence.ONCE) {
                    key.append(' ').append(load.identity());
                }
            }
            Held current;
            synchronized (SystemDatabase.class) {
                com.legend.executionplan.DatabaseType engine = system.type();
                current = CURRENT.get(engine);
                if (current == null || !current.key.equals(key.toString())) {
                    if (current != null) {
                        current.replaced = true;
                        if (current.leases == 0) {
                            closeQuietly(current.session);
                        }
                    }
                    if (catalog && !Sessions.inProcess(engine)) {
                        throw new IllegalStateException("a catalog's loads on " + engine + ": the process's catalog is"
                                + " made on a database the platform runs in process");
                    }
                    Map<com.legend.executionplan.DatabaseType, Sessions.Opener> conns = systemConnections;
                    current = new Held(key.toString(), Sessions.lazy(() -> Sessions.inProcess(engine)
                            ? openPrivate(engine) : openSystem(engine, conns)), catalog);
                    CURRENT.put(engine, current);
                }
                if (!held.contains(current)) {
                    current.leases++;
                    held.add(current);
                }
            }
            if (current.catalog && !turns.contains(current)) {
                take(current, target);
            }
            return current.session;
        }

        /** This run's turn on {@code catalog}: one run at a time (a run nested in another on this thread refused by name:
         *  its rollback would undo the outer's registration), the catalog made first if it is not, then a transaction
         *  begun. */
        private void take(Held catalog, com.legend.executionplan.ExecutionPlan.Target target) {
            if (catalog.turn.isHeldByCurrentThread()) {
                throw new IllegalStateException("a run on the system catalog inside another run's turn on it, on one"
                        + " thread: its rollback would undo the other's registration");
            }
            catalog.turn.lock();
            boolean taken = false;
            try {
                if (catalog.broken) {
                    throw new IllegalStateException("the system catalog's loads failed when it was made, and are not run"
                            + " again (some of their steps ran)");
                }
                if (!catalog.made) {
                    catalog.broken = true;
                    for (var load : target.loads()) {
                        if (load.cadence() == com.legend.executionplan.ExecutionPlan.Load.Cadence.ONCE) {
                            PlanRunner.establishOn(load, catalog.session);
                        }
                    }
                    catalog.broken = false;
                    catalog.made = true;
                }
                Sessions.beginTurn(catalog.session);
                turns.add(catalog);
                taken = true;
                // the run's own loads (a product query's model registration, PER_ACQUISITION; L4's product half,
                // 2026-11-10): inside the turn, rolled back with it -- no other run sees them
                for (var load : target.loads()) {
                    switch (load.cadence()) {
                        case ONCE -> {
                        }
                        case PER_ACQUISITION -> PlanRunner.establishOn(load, catalog.session);
                        case BEFORE_STEP, AT_STEP, SOURCE, PER_TRANSACTION -> throw new IllegalStateException("a " + load.cadence()
                                + " load on the catalog's target: a program's step, never the target's");
                        // the catalog holds no request's session table: its turn is rolled back whole when the run ends
                        case AT_RELEASE -> throw new IllegalStateException("an AT_RELEASE load on the catalog's target:"
                                + " a request's session tables are never the catalog's");
                    }
                }
            } finally {
                if (!taken) {
                    catalog.turn.unlock();
                }
            }
        }

        @Override
        public void close() {
            com.legend.error.@com.legend.base.Nullable DataError failed = null;
            for (Held h : turns) {
                try {
                    Sessions.endTurn(h.session);
                } catch (com.legend.error.DataError e) {
                    // a turn not rolled back leaves rows behind: the catalog is broken, never read again
                    h.broken = true;
                    if (failed == null) {
                        failed = e;
                    } else {
                        failed.addSuppressed(e);
                    }
                } finally {
                    h.turn.unlock();
                }
            }
            turns.clear();
            synchronized (SystemDatabase.class) {
                for (Held h : held) {
                    h.leases--;
                    if (h.replaced && h.leases == 0) {
                        closeQuietly(h.session);
                    }
                }
                held.clear();
            }
            if (failed != null) {
                throw failed;
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
            Map<com.legend.executionplan.DatabaseType, Sessions.Opener> systemConnections) {
        // the host's system connections by the plan's engine (the door keys them so, 2026-11-09)
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
