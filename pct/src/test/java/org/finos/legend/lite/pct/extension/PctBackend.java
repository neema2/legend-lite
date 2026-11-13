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
 * <p>No lane hosts a system connection (B4, 2026-11-11): a query or body reading the metamodel store runs on the
 * process's in-process catalog whatever database the lane is (SystemDatabase), the platform opening its own private
 * instance.
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
        return com.legend.exec.Connectors.of(com.legend.planner.ConnectionPlans.type(backend.type()))
                .ownSessions();
    }

    /**
     * The sessions channel A's plans run on ({@link PctPlanRoute}; step 9, 2026-10-19). An in-process lane's plan
     * opens its own session from its target ({@link com.legend.exec.PlanSessions#shared}: the declared in-memory
     * database, set up by the plan), as MongoDB's does through its connector. A SERVER lane's plan runs on the
     * harness's own session, as the legacy route's did ({@code PctExecuteNative} opened one per case, a cloud lane's
     * the suite's held one): where the backend holds a session ({@link TestBackend#held}: a cloud's, the one the
     * merge-base's route ran every case on), on it; else a NEW session per plan, opened as the lane's SYSTEM session
     * ({@link TestBackend#openSystem}), checked by the runner against the plan's target and given the target's
     * session statements as any session is, and closed after. (A plan whose session statements made the system
     * metamodel store's own tables took a new session beside a held one, round 14, until B4, 2026-11-11: the store is
     * the process's in-process catalog.) The platform cannot open it itself: the storeless
     * runtime declares the server's coordinates with {@code auth: Test}, which names no user, and
     * {@code Sessions.openingFor} admits no credential and has no opening for ClickHouse or the clouds, so the driver
     * connected as the OS user (MySQL 8.4's root by caching_sha2_password, refused; Postgres's "role does not exist")
     * or not at all. A declared server connection opened with its credentials through a vault is the platform's item
     * (the program's §7 residue), not this harness's. A plan with setup on a server lane goes to the plan's own
     * source, which refuses it by name: the server's database is the user's.
     */
    public static com.legend.exec.PlanSessions.Source planSessions() {
        TestBackend backend = backend();
        if (backend.inProcess() || ownSessions(backend)) {
            return com.legend.exec.PlanSessions.shared();
        }
        // the lane's rule as exec's data (2026-11-09, the user's question "why do we have any callbacks?": a function of
        // this harness's chose the session inside the run until then)
        return com.legend.exec.PlanSessions.lane(held(backend), backend::openSystem);
    }

    /**
     * The session source channel A's queries run through THE PRODUCT DOOR on ({@link PctPlanRoute},
     * {@code Execution.run}; 2026-11-03), each session the one the plan route's sessions chose for the plan
     * ({@link #planSessions}): an in-process lane's a new private session of its own, the query's declared setup run on
     * it by the door; a lane whose sessions are its connector's (MongoDB) the connector's own for the declared
     * connection; a server lane's a new SYSTEM session per query, closed after; but where the backend holds a session
     * (a cloud's), the held one (the target the query's plan answers on, which the run hands the source). (Round 14,
     * 2026-11-03, sent a plan making the system metamodel store's session tables to a new session even beside a held
     * one -- ClickHouse keeps a temporary table in its HTTP session, Snowflake needs a current schema -- until B4,
     * 2026-11-11: getAll::testBasic reads the process's in-process catalog.)
     */
    public static com.legend.exec.SessionSource executionSource() {
        TestBackend backend = backend();
        if (ownSessions(backend)) {
            return new com.legend.exec.SessionSource.Declared();
        }
        if (backend.inProcess()) {
            return new com.legend.exec.SessionSource.Fresh(backend::open);
        }
        // the lane's rule as exec's data (as planSessions's): the held session where the backend holds one, else a new
        // system session per query
        com.legend.exec.Sessions.@com.legend.base.Nullable Session held = held(backend);
        return held == null ? new com.legend.exec.SessionSource.Fresh(backend::openSystem)
                : com.legend.exec.SessionSource.given(held);
    }

    /** The one session over the lane's held connection, where the backend holds one (a cloud's): kept for the suite,
     *  its memo kept (D70). */
    private static com.legend.exec.Sessions.@com.legend.base.Nullable Session held(TestBackend backend) {
        Connection held;
        try {
            held = backend.held();
        } catch (SQLException e) {
            throw new IllegalStateException("the " + name() + " lane's held session could not be opened: "
                    + e.getMessage(), e);
        }
        if (held == null) {
            return null;
        }
        // held for the suite (the JVM's exit closes it): ONE session over it, kept here, its memo kept (D70; the
        // audit's finding: a cloud backend's held() answered a new unclosable proxy per call, so the connection's
        // identity could not key the session; since 2026-11-06 it answers one view per held session, so
        // TestSessions.over finds the same session for channel B too)
        com.legend.exec.Sessions.Session s = heldSession;
        if (s == null) {
            s = com.legend.testbackend.TestSessions.over(held);
            heldSession = s;
        }
        return s;
    }

    /** The one session over the lane's held connection ({@link #planSessions}). */
    private static volatile com.legend.exec.Sessions.@com.legend.base.Nullable Session heldSession;
}
