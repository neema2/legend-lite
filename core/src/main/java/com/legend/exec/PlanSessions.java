// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.cache.Hash;
import com.legend.cache.HandleStore;
import com.legend.executionplan.ExecutionPlan;

import java.sql.Connection;
import java.sql.SQLException;

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

        /** The session for a plan that CHANGES its target's database (a write plan, docs/WRITES_DESIGN_2026_10_18.md
         *  §8, leg 7): by default the one {@link #open} gives -- a caller's own database keeps the change. */
        default Sessions.Session openToChange(ExecutionPlan.Target target) {
            return open(target);
        }
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

    /** The process's sessions, shared by target (decision A). A shared database is never changed by a run: a plan that
     *  changes its database (a write plan) runs on one of its own, opened and set up from the target for that run and
     *  closed after it (decision A's second rule: a database a run changed is thrown away, so the next run reads one as
     *  set up); a database the user names, or one a URL reaches, is the user's and keeps the change. */
    public static Source shared() {
        return new Source() {
            @Override
            public Sessions.Session open(ExecutionPlan.Target target) {
                return openShared(target);
            }

            @Override
            public Sessions.Session openToChange(ExecutionPlan.Target target) {
                return openOwn(target);
            }
        };
    }

    /** A caller's own connection — its database the caller's, set up by the caller — checked by the runner against the
     *  target, never closed here. */
    public static Source given(Connection connection) {
        return target -> borrowed(connection);
    }

    /** A session the body driver holds, for a plan it runs on it: its statements publish each query's
     *  execution-trace stamp to {@code trace} (an activity row records the comment its statement carried,
     *  {@link ExecutionTrace}; {@link Sessions.Session#statements(ExecutionTrace)}), its state and version the
     *  session's own, and the runner's close releases nothing -- the session is the body's (track B's B1b,
     *  2026-10-29: until then the driver handed its {@code Connection} here). */
    public static Source of(Sessions.Session session, @com.legend.base.Nullable ExecutionTrace trace) {
        return target -> new Sessions.Session() {
            @Override
            public Connection connection() {
                return session.connection();
            }

            @Override
            public Statements statements() {
                return session.statements(trace);
            }

            @Override
            public SessionState state() {
                return session.state();
            }

            @Override
            public String version() {
                return session.version();
            }

            @Override
            public void close() {
                // the body's
            }
        };
    }

    /** A caller's own connection as a session: its database the caller's, never closed here. The execution front
     *  door wraps each execute entry's connection so for the body driver, which holds no connection of its own (B1b). */
    public static Sessions.Session session(Connection connection) {
        return borrowed(connection);
    }

    /** A caller's own FRESH connection with {@code target}'s setup run on it now (a test's new database): the source
     *  of {@link #given} it, never closed here. The setup runs as the body path's seeds do (D65, 2026-10-29: until
     *  then this class sent each step with a statement of its own): each step through {@link PlanRunner#setUp} under
     *  the SEED mark, on a session the runner opens and checks against the target, the target's session statements
     *  first -- a connection's settings precede its test data, as legend-engine's test connections run their setup SQL
     *  on the connection they create ({@code LocalH2DataSourceSpecification.getConnection}). A plan with rows for a
     *  bulk loader is refused before any statement runs where the database has none. */
    public static Source setUp(Connection connection, ExecutionPlan.Target target) {
        Source source = given(connection);
        if (target.setup().stream().anyMatch(ExecutionPlan.SetupStep.Rows.class::isInstance)) {
            // the loader, found before any statement runs: a plan writes rows for one only where its database has one
            // (SqlDialect.loadsRowsInBulk), so none is a mismatch, refused untouched
            try {
                if (BulkLoads.of(connection) == null) {
                    throw new IllegalStateException("the plan's setup has rows for a bulk loader, and the session's"
                            + " database has none (" + Connectors.product(connection) + ")");
                }
            } catch (SQLException e) {
                throw dataError(e);
            }
        }
        try (var origin = StatementOrigin.enter(StatementOrigin.SEED)) {
            PlanRunner.setUp(target.setup(), target, source, null);
        }
        return source;
    }

    private static Sessions.Session openShared(ExecutionPlan.Target target) {
        // the target's whole content, spelled unambiguously (a text cell holding a comma, a null cell): equal keys,
        // equal targets
        Hash key = Hash.ofUtf8(com.legend.executionplan.PlanJson.writeTarget(target));
        try {
            return switch (target.database()) {
                case ExecutionPlan.Database.Platform p -> borrowed(SHARED.getOrOpenSlowly(key, PlanSessions::dead,
                        () -> setUpOpened(Sessions.openPrivate(p.type()), target)));
                case ExecutionPlan.Database.Declared d -> {
                    // a database with a connector (MongoDB's; docs/MONGODB_PLAN_2026_10_16.md D12, 2026-10-17) is
                    // opened by it, for the run: it is the user's, so a plan sets none up
                    DatabaseConnector connector = Connectors.of(d.connection().databaseType());
                    if (connector.ownSessions()) {
                        refuseSetup(target, d, "a database its connector opens");
                        yield connector.open(d.connection());
                    }
                    yield switch (Sessions.openingFor(d.connection())) {
                    case Sessions.Opening.Held h -> borrowed(SHARED.getOrOpenSlowly(key, PlanSessions::dead,
                            () -> setUpOpened(Sessions.openHeld(h), target)));
                    case Sessions.Opening.Named n -> {
                        String usersName = n.name();
                        if (usersName != null) {
                            // a database the user named (an EmbeddedH2) is the user's identity, shared by design (the
                            // server's sessions connect to it too), and outlives its connections: only a LocalH2
                            // connection declares test data, so a plan sets none up
                            refuseSetup(target, d, "an in-memory database the user named ('" + usersName + "')");
                            yield owned(Sessions.openNamed(n, usersName));
                        }
                        // the source names it, afresh at each attempt, and it lives while its keeper is open: one
                        // whose setup failed is gone with its keeper, and never connected to again
                        Named named = NAMED.getOrOpenSlowly(key, held -> dead(held.keeper()), () -> {
                            String name = "plan_" + key.hex().substring(0, 16) + "_" + ATTEMPTS.incrementAndGet();
                            return new Named(setUpOpened(Sessions.openKept(n, name), target), name);
                        });
                        yield owned(Sessions.joinKept(n, named.name()));
                    }
                    case Sessions.Opening.Url u -> {
                        // a database reached by a URL is the user's: only an in-memory connection declares test
                        // data, so a plan sets none up
                        refuseSetup(target, d, "a database reached by a URL");
                        yield owned(Sessions.open(u));
                    }
                };
                }
            };
        } catch (SQLException e) {
            throw dataError(e);
        }
    }

    /** A database of the run's own for {@code target}, set up from it and closed after the run (a write plan's, on a
     *  database the source would share); the user's own databases as {@link #openShared} opens them. */
    private static Sessions.Session openOwn(ExecutionPlan.Target target) {
        try {
            return switch (target.database()) {
                case ExecutionPlan.Database.Platform p -> owned(setUpOpened(Sessions.openPrivate(p.type()), target));
                case ExecutionPlan.Database.Declared d -> switch (Sessions.openingFor(d.connection())) {
                    case Sessions.Opening.Held h -> owned(setUpOpened(Sessions.openHeld(h), target));
                    case Sessions.Opening.Named n -> {
                        if (n.name() != null) {
                            yield openShared(target);
                        }
                        // named afresh, never shared: the database lives while this, its keeper, is open
                        String name = "plan_own_" + ATTEMPTS.incrementAndGet();
                        yield owned(setUpOpened(Sessions.openKept(n, name), target));
                    }
                    case Sessions.Opening.Url u -> openShared(target);
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
    private static Connection setUpOpened(Connection c, ExecutionPlan.Target target) {
        try {
            setUp(c, target);
            return c;
        } catch (RuntimeException | Error e) {
            // every failure, rethrown as it is: the connection must not outlive a setup that failed
            try {
                c.close();
            } catch (SQLException closing) {
                e.addSuppressed(closing);
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

    /** {@link #dataError(SQLException)} for a statement that failed on a session that may hold a raise: {@code held}
     *  reads back the raise its session held ({@link JdbcStatements#held}: MariaDB and MySQL keep a raise's text in
     *  the session, their errors cannot carry it), asked only where the error's own text carries none
     *  ({@link RaisedErrors#unwrapped(SQLException, java.util.function.Supplier)}); null where none. */
    static com.legend.error.DataError dataError(SQLException e,
            java.util.function.Supplier<@com.legend.base.Nullable String> held) {
        SQLException un = RaisedErrors.unwrapped(e, held);
        return new com.legend.error.DataError(String.valueOf(un.getMessage()), un);
    }
}
