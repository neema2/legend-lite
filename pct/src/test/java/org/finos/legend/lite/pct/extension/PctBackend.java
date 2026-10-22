// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.extension;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * The database a PCT lane runs its suite on, chosen by the target ({@code LEGENDLITE_PCT_BACKEND}, an environment
 * variable: it must survive the fork into the test JVM) and named as its test backend names it
 * ({@code com.legend.testbackend.TestBackends.named}); unset, the platform's own engine, the reference lane. One
 * session per executed expression: each database's session, storeless runtime and system connection are its test
 * backend's (the dialect program's Phase 6, 2026-10-16: until then a lane enum here held them, one arm per database).
 *
 * <p>Session settings are the dialect's, applied at the platform's connection seam ({@code SqlDialect.sessionSetup},
 * from {@code Compiler.dialectOf}); this only opens the connection.
 *
 * <p>A server lane is also the host of a SYSTEM CONNECTION ({@link #systemConnections}): where the platform's system
 * database for that engine lives (a query reading the metamodel store runs there, apart from the user's session;
 * exec.SystemDatabase). This harness gives it the lane's own server and credentials, as its choice; a host
 * configures it apart. An in-process lane needs none (the platform opens its own private instance).
 *
 * <p>Both channels run on it (2026-10-11, the user's ask): channel A's adapter ({@link PctExecuteNative}) and channel
 * B's runner (channelb.ChannelB), so a lane's two verdicts come from the same database.
 */
public final class PctBackend {

    private PctBackend() {
    }

    /** This lane's backend: the one {@code LEGENDLITE_PCT_BACKEND} names, or the platform's when it is unset. */
    private static TestBackend backend() {
        String name = System.getenv("LEGENDLITE_PCT_BACKEND");
        return name == null ? TestBackends.platform() : TestBackends.named(name);
    }

    /** The lane's name, as {@code LEGENDLITE_PCT_BACKEND} spells it (the platform's when unset). */
    public static String name() {
        return backend().name();
    }

    /** Whether this lane is the reference lane: the platform's own engine. */
    public static boolean isReference() {
        return backend() == TestBackends.platform();
    }

    /** Whether channel A runs through the plan path (the expression planned to Pure's TDS text and run by the plan
     *  runner on the session the plan's target opens) rather than the legacy executor on a JDBC session: every lane
     *  (the plan path program's step 9, docs/PLAN_PATH_PROGRAM_2026_10_17.md §7). The MongoDB lane ran here first
     *  (docs/MONGODB_PLAN_2026_10_16.md §6), the DuckDB lane since 2026-10-18 (the user: "flip and let's fix
     *  everything"; exact at its first landing), the rest since 2026-10-19, each burned down in its own container
     *  (docs/STEP9_LANE_BRIEF_2026_10_19.md). The legacy route is deleted with step 11. */
    public static boolean throughThePlan() {
        return true;
    }

    /** This lane's storeless runtime declared alone: the session {@link #connect} opens. */
    public static String storelessRuntime() {
        return withStorelessRuntime("");
    }

    /** {@code model} with this lane's storeless runtime declared beside it: the session {@link #connect} opens. */
    static String withStorelessRuntime(String model) {
        return backend().withStorelessRuntime(model);
    }

    /** A session on this lane's database. */
    public static Connection connect() throws SQLException {
        return backend().open();
    }

    /** Whether the lane's database's sessions are its connector's own, not JDBC's (MongoDB's,
     *  docs/MONGODB_PLAN_2026_10_16.md D12): no JDBC session to hand a plan, nor a server version to read off one. */
    private static boolean ownSessions(TestBackend backend) {
        return com.legend.exec.Connectors.of(backend.type()).ownSessions();
    }

    /**
     * The sessions channel A's plans run on ({@link PctPlanRoute}; step 9, 2026-10-19). An in-process lane's plan
     * opens its own session from its target ({@link com.legend.exec.PlanSessions#shared}: the declared in-memory
     * database, set up by the plan), as MongoDB's does through its connector. A SERVER lane's plan runs on the
     * harness's own session, as the legacy route's did ({@code PctExecuteNative} opened one per case, a cloud lane's
     * the suite's held one): where the backend holds a session ({@link TestBackend#held}: a cloud's, the one the
     * merge-base's route ran every case on), on it for a plan whose session statements are its dialect's options
     * alone; else a NEW session per plan, opened as the lane's SYSTEM session ({@link TestBackend#openSystem}: the
     * one that holds a session's own tables --
     * ClickHouse's HTTP session, Snowflake's schema, BigQuery's session -- which the plan's session statements make
     * for the system metamodel store), checked by the runner against the plan's target and given the target's
     * session statements as any session is, and closed after. The platform cannot open it itself: the storeless
     * runtime declares the server's coordinates with {@code auth: Test}, which names no user, and
     * {@code Sessions.openingFor} admits no credential and has no opening for ClickHouse or the clouds, so the driver
     * connected as the OS user (MySQL 8.4's root by caching_sha2_password, refused; Postgres's "role does not exist")
     * or not at all. A declared server connection opened with its credentials through a vault is the platform's item
     * (the program's §7 residue), not this harness's. A plan with setup on a server lane goes to the plan's own
     * source, which refuses it by name: the server's database is the user's.
     */
    public static com.legend.exec.PlanSessions.Source planSessions() {
        com.legend.exec.PlanSessions.Source own = com.legend.exec.PlanSessions.shared();
        TestBackend backend = backend();
        if (backend.inProcess() || ownSessions(backend)) {
            return own;
        }
        return target -> {
            if (!target.setup().isEmpty()) {
                return own.open(target);
            }
            Connection held;
            try {
                held = optionsOnly(target) ? backend.held() : null;
            } catch (SQLException e) {
                throw new IllegalStateException("the " + name() + " lane's held session could not be opened: "
                        + e.getMessage(), e);
            }
            if (held != null) {
                return new com.legend.exec.Sessions.Session() {
                    @Override
                    public Connection connection() {
                        return held;
                    }

                    @Override
                    public void close() {
                        // held for the suite: the JVM's exit closes it
                    }
                };
            }
            Connection c;
            try {
                c = backend.openSystem();
            } catch (SQLException e) {
                throw new IllegalStateException("the " + name() + " lane's session could not be opened: "
                        + e.getMessage(), e);
            }
            return new com.legend.exec.Sessions.Session() {
                @Override
                public Connection connection() {
                    return c;
                }

                @Override
                public void close() {
                    try {
                        c.close();
                    } catch (SQLException e) {
                        throw new IllegalStateException("the " + name() + " lane's session could not be closed: "
                                + e.getMessage(), e);
                    }
                }
            };
        };
    }

    /** Whether a plan's session statements are its dialect's own options alone (ALTER SESSION, run on any session
     *  again as on a new one): a plan that makes a table of the session's own (the system metamodel store's) runs on
     *  a new session, which drops the table when it closes. */
    private static boolean optionsOnly(com.legend.executionplan.ExecutionPlan.Target target) {
        return target.session().equals(com.legend.database.Databases.dialect(target.database().type()).sessionSetup());
    }

    private static volatile @com.legend.base.Nullable String serverVersion;

    /** The version of the server this lane's sessions connect to, read once off one session (the plan route plans
     *  for it: H2's dialect spells by the version, `TypedQuery.executionPlan`'s last argument; step 9, 2026-10-19);
     *  null for a lane with no JDBC session (MongoDB). */
    public static @com.legend.base.Nullable String serverVersion() {
        if (ownSessions(backend())) {
            return null;
        }
        String v = serverVersion;
        if (v == null) {
            try (Connection c = connect()) {
                v = com.legend.exec.Sessions.version(c);
            } catch (SQLException e) {
                throw new IllegalStateException("the " + name() + " lane's server version could not be read: "
                        + e.getMessage(), e);
            }
            serverVersion = v;
        }
        return v;
    }

    /** The host's system connections: a server lane's own engine ({@link TestBackend#openSystem}); none for an
     *  in-process lane. */
    public static java.util.Map<DatabaseType, com.legend.exec.Sessions.Opener> systemConnections() {
        TestBackend backend = backend();
        return backend.inProcess() ? java.util.Map.of() : java.util.Map.of(backend.type(), backend::openSystem);
    }
}
