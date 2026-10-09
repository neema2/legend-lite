// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.cache.Hash;
import com.legend.cache.HandleStore;
import com.legend.executionplan.ExecutionPlan;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * The sessions a plan runs on, given out by its target (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 3, and
 * decision A): no model, only the target the plan names — its database, its server versions, its session statements and
 * its setup. Two runs whose targets are equal share an in-memory database, and its setup runs once, when it is opened;
 * a database reached by a URL is opened for the run and closed after it (a plan sets none up: only an in-memory
 * connection declares test data); a caller's own connection is its own.
 */
public final class PlanSessions {

    private PlanSessions() {
    }

    /** Where a runner gets the session for a plan's target. */
    @FunctionalInterface
    public interface Source {
        Sessions.Session open(ExecutionPlan.Target target);
    }

    /** The shared in-memory databases held by their one connection, by their target's content: a database lives as long
     *  as the process (dropping one would drop its tables), and an edited target is another database. */
    private static final HandleStore<Connection> SHARED = new HandleStore<>();

    /** The shared named in-memory databases the source names (H2), by their target's content: the connection that holds
     *  each open, and its name, which each run connects to. */
    private static final HandleStore<Named> NAMED = new HandleStore<>();

    /** A named in-memory database a target's run connects to, held open by {@code keeper}. */
    private record Named(Connection keeper, String name) {
    }

    /** Numbers each named database the source opens: a name is never reused, so no run connects to a database another
     *  attempt left (one whose keeper some other run still holds open). */
    private static final java.util.concurrent.atomic.AtomicLong ATTEMPTS = new java.util.concurrent.atomic.AtomicLong();

    /** The process's sessions, shared by target (decision A). A plan of phase 1 only reads, so a shared database is
     *  never changed by a run (a plan that writes is phase 2's, and throws its database away). */
    public static Source shared() {
        return PlanSessions::openShared;
    }

    /** A caller's own connection — its database the caller's, set up by the caller — checked by the runner against the
     *  target, never closed here. */
    public static Source given(Connection connection) {
        return target -> borrowed(connection);
    }

    /** A caller's own FRESH connection with {@code target}'s setup run on it now (a test's new database): the source
     *  of {@link #given} it, never closed here. */
    public static Source setUp(Connection connection, ExecutionPlan.Target target) {
        try {
            runSetup(connection, target);
        } catch (SQLException e) {
            throw dataError(e);
        }
        return given(connection);
    }

    private static Sessions.Session openShared(ExecutionPlan.Target target) {
        // the target's whole content, spelled unambiguously (a text cell holding a comma, a null cell): equal keys,
        // equal targets
        Hash key = Hash.ofUtf8(com.legend.executionplan.PlanJson.writeTarget(target));
        try {
            return switch (target.database()) {
                case ExecutionPlan.Database.Platform p -> borrowed(SHARED.getOrOpenSlowly(key, PlanSessions::dead,
                        () -> setUpOpened(Sessions.openPrivate(p.type()), target)));
                case ExecutionPlan.Database.Declared d -> switch (Sessions.openingFor(d.connection())) {
                    case Sessions.Opening.Held h -> borrowed(SHARED.getOrOpenSlowly(key, PlanSessions::dead,
                            () -> setUpOpened(Sessions.openHeld(h), target)));
                    case Sessions.Opening.Named n -> {
                        String usersName = n.name();
                        if (usersName != null) {
                            // a database the user named (an EmbeddedH2) is the user's identity, shared by design (the
                            // server's sessions connect to it too), and outlives its connections: only a LocalH2
                            // connection declares test data, so a plan sets none up
                            refuseSetup(target, d, "an in-memory database the user named ('" + usersName + "')");
                            yield owned(Sessions.openNamed(usersName));
                        }
                        // the source names it, afresh at each attempt, and it lives while its keeper is open: one
                        // whose setup failed is gone with its keeper, and never connected to again
                        Named named = NAMED.getOrOpenSlowly(key, held -> dead(held.keeper()), () -> {
                            String name = "plan_" + key.hex().substring(0, 16) + "_" + ATTEMPTS.incrementAndGet();
                            return new Named(setUpOpened(Sessions.openKept(name), target), name);
                        });
                        yield owned(Sessions.openKept(named.name()));
                    }
                    case Sessions.Opening.Url u -> {
                        // a database reached by a URL is the user's: only an in-memory connection declares test
                        // data, so a plan sets none up
                        refuseSetup(target, d, "a database reached by a URL");
                        yield owned(Sessions.open(u));
                    }
                };
            };
        } catch (SQLException e) {
            throw dataError(e);
        }
    }

    /** A database that is the user's, never set up by a plan: a plan with setup for it is refused, never run. */
    private static void refuseSetup(ExecutionPlan.Target target, ExecutionPlan.Database.Declared d, String what) {
        if (!target.setup().isEmpty()) {
            throw new IllegalStateException("connection '" + d.connection().qualifiedName() + "' is " + what
                    + ", and the plan has setup for it: never run on it");
        }
    }

    /** {@code c}, which the source opened for {@code target}, set up: a setup that fails closes it, and a close that
     *  fails rides the setup's own error, suppressed. */
    private static Connection setUpOpened(Connection c, ExecutionPlan.Target target) throws SQLException {
        try {
            return runSetup(c, target);
        } catch (SQLException | RuntimeException | Error e) {
            // every failure, rethrown as it is: the connection must not outlive a setup that failed
            try {
                c.close();
            } catch (SQLException closing) {
                e.addSuppressed(closing);
            }
            throw e;
        }
    }

    /** Runs {@code target}'s setup on {@code c}, under the SEED mark: each statement as it is, each rows step through
     *  the database's bulk loader (checked present before any statement runs). */
    private static Connection runSetup(Connection c, ExecutionPlan.Target target) throws SQLException {
        try (var origin = StatementOrigin.enter(StatementOrigin.SEED)) {
            // the loader, found before any statement runs: a plan writes rows for one only where its database has one
            // (Databases.loadsRowsInBulk), so none is a mismatch, refused untouched
            BulkLoad bulk = BulkLoads.of(c);
            if (bulk == null && target.setup().stream().anyMatch(ExecutionPlan.SetupStep.Rows.class::isInstance)) {
                throw new IllegalStateException("the plan's setup has rows for a bulk loader, and the session's"
                        + " database has none (" + c.getMetaData().getDatabaseProductName() + ")");
            }
            for (ExecutionPlan.SetupStep step : target.setup()) {
                switch (step) {
                    case ExecutionPlan.SetupStep.Statement s -> {
                        StatementOrigin.sent(s.sql());
                        try (Statement st = c.createStatement()) {
                            st.execute(s.sql());
                        }
                    }
                    case ExecutionPlan.SetupStep.Rows r -> BulkLoads.load(java.util.Objects.requireNonNull(bulk,
                            "found above"), c, r);
                }
            }
            return c;
        }
    }

    private static boolean dead(Connection c) {
        try {
            return c.isClosed();
        } catch (SQLException e) {
            return true;
        }
    }

    /** A session the source does not own: closing it releases nothing. */
    private static Sessions.Session borrowed(Connection c) {
        return new Sessions.Session() {
            @Override
            public Connection connection() {
                return c;
            }

            @Override
            public void close() {
                // shared, or the caller's
            }
        };
    }

    /** A connection opened for the run alone: closing the session closes it. */
    private static Sessions.Session owned(Connection c) {
        return new Sessions.Session() {
            @Override
            public Connection connection() {
                return c;
            }

            @Override
            public void close() {
                try {
                    c.close();
                } catch (SQLException e) {
                    throw dataError(e);
                }
            }
        };
    }

    static com.legend.error.DataError dataError(SQLException e) {
        SQLException un = RaisedErrors.unwrapped(e);
        return new com.legend.error.DataError(String.valueOf(un.getMessage()), un);
    }
}
