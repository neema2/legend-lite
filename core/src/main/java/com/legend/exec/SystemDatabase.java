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
 * ({@code SystemCatalog.seeds}, run by the plan runner and recorded on the session;
 * the walker-deletion program, 2026-11-03: until then this holder wrote it as it
 * opened the session, a send outside every plan), the current
 * one of its engine until a run names another graph's (hole 2, 2026-11-09). Nothing is seeded per
 * execution any more: the executor ROUTES a store-reading body to this
 * connection (the corpus re-seeded ~20 tables of a corpus-sized graph on
 * every store-reading test &mdash; docs/GATES.md, 2026-09-02 budget entry).
 *
 * <p>The GRAPH's rows are a pure function of the compiled graph, derived
 * once per table by the planner ({@code SystemCatalog.seeds}). Nothing else is ever written: a
 * query's constructed instances ({@code ^DynaFunction(...)} trees) ride
 * the query itself as inline relations.
 *
 * <p>The engine is the database the query's runtime DECLARES where the platform runs it in process (an H2
 * lane keeps exercising the metamodel queries on H2 &mdash; the 21-kind
 * union hang was an H2 finding), the platform's own for a server, opened private by {@link Sessions}; the
 * connection is the ONE in-memory database of that engine.
 *
 * <p>Every system database is IN PROCESS (B4, 2026-11-11, the user's ruling: every statement reading the system
 * metamodel store runs on the process's in-process catalog, whatever database holds the user's data -- the planner
 * names the catalog of the runtime's engine where the platform runs it in process, the platform's own engine's for a
 * server, {@code SystemCatalog.engine}). The host's system connection for a server engine, and the store as that
 * session's temporary tables, are deleted (census M5, M6).
 */
public final class SystemDatabase {

    /** The CATALOG of each engine (L4b, 2026-11-09; the plan-as-data design's hole 2): the key it is made with -- the
     *  platform's content and its ONCE loads' identities, the same for every plan on the engine -- and its session. Made
     *  once a process, never replaced (B4, 2026-11-11: a target naming another key on the engine is refused by name,
     *  where a graph's own system database was replaced until then). Found by the plan's own target, never the compiled
     *  model. */
    private static final Map<com.legend.executionplan.DatabaseType, Held> CURRENT = new HashMap<>();

    /** A catalog's session, made with its target's ONCE loads -- the store's tables and the platform's rows -- and taken
     *  by one run at a time ({@link #turn}), each run's registration inside a transaction rolled back when its leases
     *  close. */
    private static final class Held {
        final String key;
        final Sessions.Session session;
        final java.util.concurrent.locks.ReentrantLock turn = new java.util.concurrent.locks.ReentrantLock();
        /** Guarded by {@link #turn}: the catalog's ONCE loads ran ({@code made}), or failed part-way ({@code broken}). */
        boolean made;
        boolean broken;

        Held(String key, Sessions.Session session) {
            this.key = key;
            this.session = session;
        }
    }

    private SystemDatabase() {
    }

    /** A run's leases of the system databases its plan names; closed when the run ends. */
    public static Leases leases() {
        return new Leases();
    }

    /** One run's leases: its turns on the catalogs its steps reach, held until {@link #close} and rolled back then. */
    public static final class Leases implements AutoCloseable {
        private final List<Held> turns = new ArrayList<>();

        private Leases() {
        }

        /**
         * The process's CATALOG {@code target} names -- its engine, which the platform runs in process, and its ONCE
         * loads (any other target refused by name: B4, 2026-11-11) -- OPENED ON FIRST USE ({@link Sessions#lazy}) as the
         * engine's private database and made with those loads once, then this run's turn taken on it: one run at a
         * time, inside a transaction rolled back when the run ends ({@link #close};
         * docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md 4.5). The same session for the same target. The holder writes
         * nothing of its own: every row is a load of the plan's.
         */
        public Sessions.Session sessionFor(com.legend.executionplan.ExecutionPlan.Target target) {
            if (!(target.database() instanceof com.legend.executionplan.ExecutionPlan.Database.System system)) {
                throw new IllegalArgumentException("a system database's lease for " + target.database());
            }
            com.legend.executionplan.DatabaseType engine = system.type();
            if (!Sessions.inProcess(engine)) {
                throw new IllegalStateException("a system database on " + engine + ": the process's catalog is made on"
                        + " a database the platform runs in process");
            }
            // the catalog is named by what it is made with (its ONCE loads): a product query's model registration
            // (PER_ACQUISITION, L4's product half) is the run's own, run in its turn, and names no other catalog
            StringBuilder key = new StringBuilder(system.graphKey());
            boolean made = false;
            for (var load : target.loads()) {
                if (load.cadence() == com.legend.executionplan.ExecutionPlan.Load.Cadence.ONCE) {
                    key.append(' ').append(load.identity());
                    made = true;
                }
            }
            if (!made) {
                throw new IllegalStateException("a system database's target with no load that makes the catalog ("
                        + target.database() + "): the process's catalog is made by its target's ONCE load");
            }
            Held current;
            synchronized (SystemDatabase.class) {
                current = CURRENT.get(engine);
                if (current == null) {
                    current = new Held(key.toString(), Sessions.lazy(() -> openPrivate(engine)));
                    CURRENT.put(engine, current);
                } else if (!current.key.equals(key.toString())) {
                    throw new IllegalStateException("a second catalog on " + engine + ": the process's catalog is made"
                            + " once, by " + current.key + ", and a target names " + key);
                }
            }
            if (!turns.contains(current)) {
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
            if (failed != null) {
                throw failed;
            }
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
}
