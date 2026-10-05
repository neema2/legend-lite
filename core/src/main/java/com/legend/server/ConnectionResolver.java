package com.legend.server;

import com.legend.model.ConnectionDefinition;

import com.legend.cache.HandleStore;
import com.legend.cache.Hash;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * The server's sessions: it opens the target the compiler decided from the
 * compiled model ({@code Compiler.executesOn}, C3b) — never a connection of
 * its own choosing — through {@code exec.Sessions}, which owns how each
 * database and specification is opened. In-memory connections are cached in the
 * content-addressed {@link HandleStore} (D5): the key hashes the
 * connection DEFINITION + FQN + the model's STORE declarations (type
 * audit D100), so the same stores + definition keep their database
 * across requests (tables persist — the feature) while an EDITED
 * definition or store gets a fresh one (an FQN-only key desynced:
 * engine planCache scar). A specification or database type not built is
 * refused by name (the folds to in-memory are gone, C3b).
 */
final class ConnectionResolver {

    private ConnectionResolver() {
    }

    private static final HandleStore<Connection> STORE = new HandleStore<>();

    /**
     * A BORROWED connection. {@code close()} RELEASES it, and what releasing
     * means is the lease's business, not the caller's:
     *
     * <ul>
     *   <li>a STORE-OWNED handle (the in-memory arms) is released by doing
     *       NOTHING — {@link HandleStore} owns it for the life of the process
     *       because evicting it would silently drop its tables, and closing it
     *       here would break the persistence feature
     *       {@code ConnectionIsolationTest} pins;</li>
     *   <li>every other arm is a fresh {@code DriverManager} connection that
     *       this lease closes. Before leases nobody closed them, so each call
     *       through an auto-resolving {@link QueryService} method leaked one
     *       connection — 25 calls, 25 file descriptors, measured
     *       (ConnectionLeaseTest). Silent on POSIX, and on Windows the symptom
     *       was a database file that could not be deleted.</li>
     * </ul>
     *
     * <p>Callers always close. That is the whole contract, and it is the
     * engine's (a Hikari connection's {@code close} returns it to the pool)
     * reached without the engine's pool — which cannot be ported here because
     * the engine RE-SEEDS per acquisition and never persists across requests.
     * See docs/CONNECTION_LEASE_DESIGN_2026_09_09.md.
     */
    static final class Lease implements AutoCloseable {

        private final Connection connection;
        private final boolean storeOwned;

        private Lease(Connection connection, boolean storeOwned) {
            this.connection = connection;
            this.storeOwned = storeOwned;
        }

        /** A handle the {@link HandleStore} owns: releasing is a no-op. */
        static Lease borrowed(Connection c) {
            return new Lease(c, true);
        }

        /** A connection opened for this call alone: the lease closes it. */
        static Lease owned(Connection c) {
            return new Lease(c, false);
        }

        Connection connection() {
            return connection;
        }

        @Override
        public void close() throws SQLException {
            if (!storeOwned) {
                connection.close();
            }
        }
    }

    /** A connection is DEAD when closed — or unanswerable, which only a
     * broken handle produces; treating it live would cache the wreck. */
    private static boolean dead(Connection c) {
        try {
            return c.isClosed();
        } catch (SQLException e) {
            return true;
        }
    }

    /** The server's session source (C3b): the compiler decides the target from the compiled model
     *  ({@code Compiler.executesOn}), this opens exactly it — a declared connection by {@code Sessions}' opening,
     *  the platform's DuckDB for a runtime binding only model data. One session per query: a runtime whose
     *  connections are DIFFERENT definitions is refused by name. */
    static final com.legend.exec.Sessions.Source SOURCE = (target, ctx) -> {
        try {
            Lease lease = open(target, ctx);
            return new com.legend.exec.Sessions.Session() {
                @Override
                public Connection connection() {
                    return lease.connection();
                }

                @Override
                public void close() {
                    try {
                        lease.close();
                    } catch (SQLException e) {
                        throw new com.legend.error.DataError(String.valueOf(e.getMessage()), e);
                    }
                }
            };
        } catch (SQLException e) {
            throw new com.legend.error.DataError(String.valueOf(e.getMessage()), e);
        }
    };

    /** The session for {@code runtimeFqn} of the compiled model {@code ctx}: the same lease a query on that runtime
     *  executes on (a caller that seeds tables before querying shares the database). */
    static Lease lease(com.legend.compiler.element.ModelContext ctx, String runtimeFqn) throws SQLException {
        return open(com.legend.Compiler.executesOn(ctx, runtimeFqn), ctx);
    }

    private static Lease open(com.legend.database.Target target, com.legend.compiler.element.ModelContext ctx)
            throws SQLException {
        Hash stores = storesKey(ctx);
        return switch (target) {
            case com.legend.database.Target.Platform p -> Lease.borrowed(STORE.getOrOpen(
                    Hash.combine(stores, Hash.ofUtf8("platform")), ConnectionResolver::dead,
                    () -> com.legend.exec.Sessions.openPrivate(p.type())));
            case com.legend.database.Target.Declared d -> {
                var distinct = new java.util.LinkedHashSet<String>();
                d.connections().forEach(c -> distinct.add(definitionText(c)));
                if (distinct.size() > 1) {
                    throw new com.legend.error.NotImplementedException("the runtime binds "
                            + d.connections().stream().map(ConnectionDefinition::qualifiedName).toList()
                            + ", different connections: the server opens one session per query");
                }
                yield connect(stores, d.connections().get(0));
            }
        };
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

    private static Lease connect(Hash storesKey, ConnectionDefinition def)
            throws SQLException {
        return switch (com.legend.exec.Sessions.openingFor(def)) {
            case com.legend.exec.Sessions.Opening.Url u -> Lease.owned(com.legend.exec.Sessions.open(u));
            // a named in-memory database (H2) is kept alive by its name — the user's EmbeddedH2 name, else one
            // per model stores + definition (D5): each use opens a connection the lease closes
            case com.legend.exec.Sessions.Opening.Named n -> Lease.owned(com.legend.exec.Sessions.openNamed(
                    n.name() != null ? n.name() : "c_" + contentKey(storesKey, def).hex().substring(0, 16)));
            // a held one (DuckDB, SQLite) lives as long as its connection: the HandleStore owns it
            case com.legend.exec.Sessions.Opening.Held h -> Lease.borrowed(STORE.getOrOpen(
                    contentKey(storesKey, def), ConnectionResolver::dead,
                    () -> com.legend.exec.Sessions.openHeld(h)));
        };
    }
}
