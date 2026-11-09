package com.legend.server;

/**
 * The server's sessions: a query's session is opened from its PLAN's target, never from the compiled model (the
 * plan-as-data design's hole 7, the user's go of 2026-11-08). The runner's project databases
 * ({@code PlanSessions.projectDatabase}) hold a project's own data (C5): named by what they hold -- the target's
 * connection and its project, the model's stores and Data rows hashed at planning (type audit D100: two models whose
 * stores differ never share one) -- so the same stores, definition and rows keep their database across requests
 * (tables persist -- the feature) while an EDITED definition, store or row gets a fresh one. A held in-memory one is
 * made once holding the Data rows the plan carries. The platform's own engine (a runtime of model data alone) is
 * keyed by the stores alone and holds no Data rows (the full audit, 2026-11-09). Until hole 7 this class keyed and filled them itself by reading
 * the model; since slice 5 (2026-11-09) it names no runtime class at all -- its runner is the product door, and the
 * sessions' source is exec's.
 */
public final class ConnectionResolver {

    private ConnectionResolver() {
    }

    // The Lease (docs/CONNECTION_LEASE_DESIGN_2026_09_09.md) left 2026-10-30 (D70, the seam's step A): a session the
    // store owns is handed out as a non-closing VIEW (Sessions.view: releasing it closes nothing, HandleStore owns it
    // for the life of the process because evicting it would silently drop its tables), every other arm is an OWNED
    // session that closes its connection (before leases nobody closed them: 25 calls, 25 file descriptors, measured
    // by ConnectionLeaseTest). The contract is the same -- callers always close -- and the seam's session carries it.

    /** Upstream {@code pure/v1/execution/execute} (E8), the {@link PureV1Api.Runner} the server hands the API (D67,
     *  2026-10-30; {@code QueryService.executeUpstream} until then, that class's one production caller): the product
     *  door, planning the query and running it as the server runs one -- a read on its project's database, leased and
     *  established first as legend-engine establishes it on every acquisition; a program that writes on the plan's own
     *  sessions (docs/WRITES_DESIGN_2026_10_18.md §8, leg 7). A database failure is a {@link com.legend.error.DataError}.
     *  (Slice 5, 2026-11-09: the server's session source is exec's, {@code PlanSessions.PROJECT_DATABASES}; the tests'
     *  session for a runtime of a compiled model is the tests' {@code ServerSessions}.) */
    public static final PureV1Api.Runner RUNNER = com.legend.Execution::runOnProjectDatabases;
}
