package com.legend.server;

/**
 * The server's sessions: a query's session is opened from its PLAN's target, never from the compiled model (the
 * plan-as-data design's hole 7, the user's go of 2026-11-08). The runner's project databases
 * ({@code PlanSessions.projectDatabase}) hold a project's own data (C5): named by what they hold -- the target's
 * connection and its project, the model's stores and Data rows hashed at planning (type audit D100: two models whose
 * stores differ never share one) -- so the same stores, definition and rows keep their database across requests
 * (tables persist -- the feature) while an EDITED definition, store or row gets a fresh one. A held in-memory one is
 * made once holding the Data rows the plan carries. Until hole 7 this class keyed and filled them itself by reading
 * the model. A specification or database type not built is refused by name.
 */
public final class ConnectionResolver {

    private ConnectionResolver() {
    }

    // The Lease (docs/CONNECTION_LEASE_DESIGN_2026_09_09.md) left 2026-10-30 (D70, the seam's step A): a session the
    // store owns is handed out as a non-closing VIEW (Sessions.view: releasing it closes nothing, HandleStore owns it
    // for the life of the process because evicting it would silently drop its tables), every other arm is an OWNED
    // session that closes its connection (before leases nobody closed them: 25 calls, 25 file descriptors, measured
    // by ConnectionLeaseTest). The contract is the same -- callers always close -- and the seam's session carries it.

    /** The server's session source: the plan's target opened as a project's own database
     *  ({@code PlanSessions.projectDatabase}) -- a declared connection by {@code Sessions}' opening, the platform's
     *  engine for a runtime binding only model data. Public since D67 (2026-10-30): the tests' twin reads the server's
     *  route through it ({@code testing.Lean.service}). */
    public static final com.legend.Execution.Source SOURCE = com.legend.exec.PlanSessions::projectDatabase;

    /** Upstream {@code pure/v1/execution/execute} (E8), the {@link PureV1Api.Runner} the server hands the API (D67,
     *  2026-10-30; {@code QueryService.executeUpstream} until then, that class's one production caller): a query that
     *  reads runs on its runtime's connection, leased, established first as legend-engine establishes it on every
     *  acquisition (its declared test data run on it); a program that writes runs from its plan on the plan's sessions
     *  (docs/WRITES_DESIGN_2026_10_18.md §8, leg 7). A database failure is a {@link com.legend.error.DataError}. */
    public static final PureV1Api.Runner RUNNER = (query, runtime, sinkOf) -> com.legend.Execution.run(query, runtime,
            SOURCE, com.legend.exec.PlanSessions.shared(), com.legend.Execution.Establish.DECLARED_SETUP, sinkOf);

    /** The session for {@code runtimeFqn} of the compiled model {@code ctx}: the same session a query on that runtime
     *  executes on (a caller that seeds tables before querying shares the database) -- the runtime's target planned,
     *  then opened as any query's is. The caller closes it. Tests' alone. */
    static com.legend.exec.Sessions.Session session(com.legend.compiler.element.ModelContext ctx, String runtimeFqn) {
        return SOURCE.open(com.legend.planner.PlanMaker.runtimeTarget(ctx, runtimeFqn));
    }
}
