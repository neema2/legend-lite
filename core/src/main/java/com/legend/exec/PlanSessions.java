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

    /** The shared in-memory databases, by their target's content: a database lives as long as the process (dropping
     *  one would drop its tables), and an edited target is another database. */
    private static final HandleStore<Connection> SHARED = new HandleStore<>();

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
            setUp(connection, target, false);
        } catch (SQLException e) {
            throw dataError(e);
        }
        return given(connection);
    }

    private static Sessions.Session openShared(ExecutionPlan.Target target) {
        Hash key = Hash.ofUtf8(target.toString());
        try {
            return switch (target.database()) {
                case ExecutionPlan.Database.Platform p -> borrowed(SHARED.getOrOpen(key, PlanSessions::dead,
                        () -> setUp(Sessions.openPrivate(p.type()), target, true)));
                case ExecutionPlan.Database.Declared d -> switch (Sessions.openingFor(d.connection())) {
                    case Sessions.Opening.Held h -> borrowed(SHARED.getOrOpen(key, PlanSessions::dead,
                            () -> setUp(Sessions.openHeld(h), target, true)));
                    case Sessions.Opening.Named n -> {
                        // a named in-memory database lives while one connection holds it: the keeper, which ran its
                        // setup; each run has a connection of its own to it
                        String name = n.name() != null ? n.name() : "plan_" + key.hex().substring(0, 16);
                        SHARED.getOrOpen(key, PlanSessions::dead, () -> setUp(Sessions.openNamed(name), target, true));
                        yield owned(Sessions.openNamed(name));
                    }
                    case Sessions.Opening.Url u -> {
                        // a database reached by a URL is the user's: only a LocalH2 connection declares test data, and
                        // it is an in-memory database, so a plan never sets one up
                        if (!target.setup().isEmpty()) {
                            throw new IllegalStateException("connection '" + d.connection().qualifiedName() + "' is a"
                                    + " database reached by a URL, and the plan has setup for it: never run on it");
                        }
                        yield owned(Sessions.open(u));
                    }
                };
            };
        } catch (SQLException e) {
            throw dataError(e);
        }
    }

    /** Runs {@code target}'s setup on {@code c}, under the SEED mark: each statement as it is, each rows step through
     *  the database's bulk loader. A failing setup closes {@code c} when {@code ownsConnection}. */
    private static Connection setUp(Connection c, ExecutionPlan.Target target, boolean ownsConnection)
            throws SQLException {
        // a rows step needs the database's bulk loader: checked before any statement runs
        if (target.setup().stream().anyMatch(ExecutionPlan.SetupStep.Rows.class::isInstance)
                && BulkLoads.of(c) == null) {
            throw new IllegalStateException("the plan's setup has rows for a bulk loader, and the session's database"
                    + " has none (" + c.getMetaData().getDatabaseProductName() + ")");
        }
        try (var origin = StatementOrigin.enter(StatementOrigin.SEED)) {
            for (ExecutionPlan.SetupStep step : target.setup()) {
                switch (step) {
                    case ExecutionPlan.SetupStep.Statement s -> {
                        StatementOrigin.sent(s.sql());
                        try (Statement st = c.createStatement()) {
                            st.execute(s.sql());
                        }
                    }
                    case ExecutionPlan.SetupStep.Rows r -> BulkLoads.load(c, r);
                }
            }
            return c;
        } catch (SQLException e) {
            if (ownsConnection) {
                c.close();
            }
            throw e;
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
