package com.legend.server;

import com.legend.model.ConnectionDefinition;

import com.legend.cache.HandleStore;
import com.legend.cache.Hash;

/**
 * The server's sessions: it opens the target the compiler decided from the
 * compiled model ({@code Compiler.executesOn}, C3b) — never a connection of
 * its own choosing — through {@code exec.Sessions}, which owns how each
 * database and specification is opened. In-memory connections are cached in the
 * content-addressed {@link HandleStore} (D5): the key hashes the
 * connection DEFINITION + FQN + the model's STORE declarations (type
 * audit D100) + its Data rows (C5: a held one is made holding them), so the
 * same stores + definition + rows keep their database across requests
 * (tables persist — the feature) while an EDITED definition, store or row
 * gets a fresh one (an FQN-only key desynced: engine planCache scar). A specification or database type not built is
 * refused by name (the folds to in-memory are gone, C3b).
 */
public final class ConnectionResolver {

    private ConnectionResolver() {
    }

    private static final HandleStore<com.legend.exec.Sessions.Session> STORE = new HandleStore<>();

    // The Lease (docs/CONNECTION_LEASE_DESIGN_2026_09_09.md) left 2026-10-30 (D70, the seam's step A): a session the
    // store owns is handed out as a non-closing VIEW (Sessions.view: releasing it closes nothing, HandleStore owns it
    // for the life of the process because evicting it would silently drop its tables), every other arm is an OWNED
    // session that closes its connection (before leases nobody closed them: 25 calls, 25 file descriptors, measured
    // by ConnectionLeaseTest). The contract is the same -- callers always close -- and the seam's session carries it.

    /** The server's session source (C3b): the compiler decides the target from the compiled model
     *  ({@code Compiler.executesOn}), this opens exactly it — a declared connection by {@code Sessions}' opening,
     *  the platform's DuckDB for a runtime binding only model data. One session per query: a runtime whose
     *  connections are DIFFERENT definitions is refused by name. Public since D67 (2026-10-30): the tests' twin reads
     *  the server's route through it ({@code testing.Lean.service}). */
    public static final com.legend.Execution.Source SOURCE = ConnectionResolver::open;

    /** Upstream {@code pure/v1/execution/execute} (E8), the {@link PureV1Api.Runner} the server hands the API (D67,
     *  2026-10-30; {@code QueryService.executeUpstream} until then, that class's one production caller): a query that
     *  reads runs on its runtime's connection, leased, established first as legend-engine establishes it on every
     *  acquisition (its declared test data run on it); a program that writes runs from its plan on the plan's sessions
     *  (docs/WRITES_DESIGN_2026_10_18.md §8, leg 7). A database failure is a {@link com.legend.error.DataError}. */
    public static final PureV1Api.Runner RUNNER = (query, runtime, sinkOf) -> com.legend.Execution.run(query, runtime,
            SOURCE, com.legend.exec.PlanSessions.shared(), com.legend.Execution.Establish.DECLARED_SETUP, sinkOf);

    /** The session for {@code runtimeFqn} of the compiled model {@code ctx}: the same session a query on that runtime
     *  executes on (a caller that seeds tables before querying shares the database). The caller closes it. */
    static com.legend.exec.Sessions.Session session(com.legend.compiler.element.ModelContext ctx, String runtimeFqn) {
        return open(com.legend.Compiler.executesOn(ctx, runtimeFqn), ctx);
    }

    private static com.legend.exec.Sessions.Session open(com.legend.database.Target target,
            com.legend.compiler.element.ModelContext ctx) {
        Hash stores = storesKey(ctx);
        return switch (target) {
            case com.legend.database.Target.Platform p -> com.legend.exec.Sessions.view(STORE.getOrOpen(
                    Hash.combine(stores, Hash.ofUtf8("platform")), com.legend.exec.Sessions::closed,
                    () -> com.legend.exec.Sessions.openPrivate(p.type())));
            case com.legend.database.Target.Declared d -> {
                var distinct = new java.util.LinkedHashSet<String>();
                d.connections().forEach(c -> distinct.add(definitionText(c)));
                if (distinct.size() > 1) {
                    throw new com.legend.error.NotImplementedException("the runtime binds "
                            + d.connections().stream().map(ConnectionDefinition::qualifiedName).toList()
                            + ", different connections: the server opens one session per query");
                }
                yield connect(stores, d.connections().get(0), ctx);
            }
        };
    }

    /** {@code made} filled from the model's Data rows, by the statements its database's dialect writes for them: the
     *  setup steps of a target on the definition's database, run by the plan runner as any seed is (D65, 2026-10-29:
     *  until then this class sent each with a statement of its own) -- the dialect's session statements first, then
     *  each step, under the SEED mark. */
    private static void filled(com.legend.exec.Sessions.Session made, com.legend.compiler.element.ModelContext ctx,
            ConnectionDefinition def) {
        boolean done = false;
        try {
            com.legend.sql.dialect.SqlDialect dialect = com.legend.database.Databases.dialect(def.databaseType());
            java.util.List<com.legend.executionplan.ExecutionPlan.SetupStep> steps = new java.util.ArrayList<>();
            for (String sql : com.legend.setup.ProjectData.fill(ctx, dialect)) {
                steps.add(new com.legend.executionplan.ExecutionPlan.SetupStep.Statement(sql));
            }
            com.legend.executionplan.ExecutionPlan.Target target = new com.legend.executionplan.ExecutionPlan.Target(
                    new com.legend.executionplan.ExecutionPlan.Database.Declared(def),
                    new com.legend.executionplan.ExecutionPlan.Servers.Every(), dialect.sessionSetup(), steps);
            try (var origin = com.legend.exec.StatementOrigin.enter(com.legend.exec.StatementOrigin.SEED)) {
                com.legend.exec.PlanRunner.setUp(steps, target, com.legend.exec.PlanSessions.of(made), null);
            }
            done = true;
        } finally {
            // a Data element refused (a table no Database declares), or a statement: the database made is let go;
            // a close that fails is told with the setup's failure, not instead of it (the D70 audit)
            if (!done) {
                try {
                    made.close();
                } catch (com.legend.error.DataError closing) {
                    // the setup's own failure is in flight: closing's rides with it as a message, since a
                    // finally cannot reach the thrown one
                    System.err.println("[connection resolver] closing the database made for '" + def.qualifiedName()
                            + "' after its fill failed: " + closing.getMessage());
                }
            }
        }
    }

    /** A definition's content without its name: two names for one database are one session. */
    private static String definitionText(ConnectionDefinition def) {
        return def.databaseType() + "|" + def.specification() + "|" + def.authentication();
    }

    /** The model's STORE-SHAPING content: every parsed
     * {@code ###Relational} Database definition, FQN-sorted (record
     * toString covers tables/columns/joins deterministically; source
     * whitespace and non-store elements do not perturb it — a later
     * request's model must key like the /engine/sql model that seeded
     * the tables). */
    private static Hash storesKey(com.legend.compiler.element.ModelContext ctx) {
        return Hash.ofUtf8(ctx.databases()
                .sorted(java.util.Comparator.comparing(
                        com.legend.model.DatabaseDefinition::qualifiedName))
                .map(Object::toString)
                .reduce("", (a, b) -> a + "\n" + b));
    }

    /** Content key: the model's STORE declarations + the definition's
     * full record content + FQN. The value behind the key is a live
     * in-memory database whose tables are shaped by the model's stores
     * and the SQL the caller runs — a connection-text-only key handed
     * two UNRELATED models each other's tables, the compiler's static
     * type violated by the returned rows (type audit D100, the one
     * cross-caller leak in the audit; its repro differed exactly in
     * the store's column type). Same stores + same definition keeps
     * the database across requests (tables persist — the feature, and
     * the interactive model+query blob flow); an edited STORE rotates
     * (the planCache-scar direction: a changed declaration must never
     * desync onto a stale physical schema). */
    private static Hash contentKey(Hash storesKey, ConnectionDefinition def) {
        return Hash.combine(storesKey,
                Hash.ofUtf8(def.qualifiedName()),
                Hash.ofUtf8(def.toString()));
    }

    private static com.legend.exec.Sessions.Session connect(Hash storesKey, ConnectionDefinition def,
            com.legend.compiler.element.ModelContext ctx) {
        return switch (com.legend.exec.Sessions.openingFor(def)) {
            case com.legend.exec.Sessions.Opening.Url u -> com.legend.exec.Sessions.open(u);
            // a named in-memory database is kept alive by its name — the user's EmbeddedH2 name, else one
            // per model stores + definition (D5): each use opens a connection the lease closes
            case com.legend.exec.Sessions.Opening.Named n -> com.legend.exec.Sessions.openNamed(n,
                    n.name() != null ? n.name() : "c_" + contentKey(storesKey, def).hex().substring(0, 16));
            // a held one lives as long as its connection: the HandleStore owns it -- made holding the
            // model's own Data rows, as a tab's or Python's project database does (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md
            // C5), so it is kept by them too: an edited row is a fresh database
            case com.legend.exec.Sessions.Opening.Held h -> {
                String rows = com.legend.setup.ProjectData.rowsKey(ctx);
                Hash key = rows.isEmpty() ? contentKey(storesKey, def)
                        : Hash.combine(contentKey(storesKey, def), Hash.ofUtf8(rows));
                // opened outside the store's lock (Compiler Rewrite's review): a database loaded with its rows, as
                // PlanSessions opens its held ones -- a failed open forgotten, a later caller for the key waiting
                yield com.legend.exec.Sessions.view(STORE.getOrOpenSlowly(key, com.legend.exec.Sessions::closed, () -> {
                    com.legend.exec.Sessions.Session made = com.legend.exec.Sessions.openHeld(h);
                    if (!rows.isEmpty()) {
                        filled(made, ctx, def);
                    }
                    return made;
                }));
            }
        };
    }
}
