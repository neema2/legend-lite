package com.legend.exec;

import com.legend.compiler.element.ModelContext;
import com.legend.model.DatabaseDefinition;
import com.legend.sql.dialect.SqlDialect;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * THE system database of a compiled graph (user ruling 2026-09-02): the
 * metamodel rows live in a database of their own, SEPARATE from every user
 * connection &mdash; one per graph per database engine, written ONCE the
 * first time a query of that graph reads the metamodel, alive as long as
 * the graph ({@link ModelContext#derived}). Nothing is seeded per
 * execution any more: the executor ROUTES a store-reading body to this
 * connection (the corpus re-seeded ~20 tables of a corpus-sized graph on
 * every store-reading test &mdash; docs/GATES.md, 2026-09-02 budget entry).
 *
 * <p>The GRAPH's rows are a pure function of the compiled graph, derived
 * once per table (the derivation is the caller's &mdash; this package
 * cannot see the seed derivations). Nothing else is ever written: a
 * query's constructed instances ({@code ^DynaFunction(...)} trees) ride
 * the query itself as inline relations.
 *
 * <p>The engine is the database the query's runtime DECLARES (an H2
 * lane keeps exercising the metamodel queries on H2 &mdash; the 21-kind
 * union hang was an H2 finding), opened private by {@link Sessions}; the
 * connection is the ONE in-memory database of that engine.
 */
public final class SystemDatabase {

    private static final java.lang.ref.Cleaner CLEANER = java.lang.ref.Cleaner.create();

    /** The Cleaner action: holds the open connections, NEVER the store
     * (a self-reference would keep the graph alive). */
    private static final class Closer implements Runnable {
        private final List<Connection> open =
                java.util.Collections.synchronizedList(new ArrayList<>());

        @Override
        public void run() {
            for (Connection c : open) {
                try {
                    c.close();
                } catch (SQLException ignore) {
                    // closing a dead in-memory session: nothing to report
                }
            }
        }
    }

    /** One engine's session. */
    private static final class Session {
        private final Connection connection;

        private Session(Connection connection) {
            this.connection = connection;
        }
    }

    private final Map<com.legend.model.ConnectionDefinition.DatabaseType, Session> sessions = new HashMap<>();
    private final Map<String, List<List<String>>> rows = new ConcurrentHashMap<>();
    private final Closer closer = new Closer();

    private SystemDatabase() {
        CLEANER.register(this, closer);
    }

    /** The graph's system store (created on first ask). */
    public static SystemDatabase of(ModelContext ctx) {
        return ctx.derived(SystemDatabase.class, c -> new SystemDatabase());
    }

    /**
     * The connection holding the graph's metamodel for the engine of
     * {@code session}, opened and written on first use. {@code store} is
     * the store's Database element (its DDL enumerates the tables);
     * {@code rowsOf} derives one table's rows (called once per table per
     * graph). READ-ONLY after that: a query's constructed instances ride
     * the query as inline relations (the resolver's scoped class sources),
     * never this database.
     */
    public synchronized Connection connectionFor(
            com.legend.model.ConnectionDefinition.DatabaseType engine,
            SqlDialect dialect, DatabaseDefinition store,
            Function<String, List<List<String>>> rowsOf) {
        Session s = sessions.get(engine);
        if (s == null) {
            s = open(engine, dialect, store, rowsOf);
            sessions.put(engine, s);
        }
        return s.connection;
    }

    private Session open(com.legend.model.ConnectionDefinition.DatabaseType engine, SqlDialect dialect,
            DatabaseDefinition store, Function<String, List<List<String>>> rowsOf) {
        Connection c;
        try {
            c = Sessions.openPrivate(engine);
        } catch (SQLException e) {
            throw new IllegalStateException("system database: cannot open the "
                    + engine + " session", e);
        }
        closer.open.add(c);
        for (String setup : dialect.sessionSetup()) {
            try (var __o = StatementOrigin.enter(StatementOrigin.SYSTEM)) {
                Executor.executeRaw(c, setup);
            }
        }
        for (DatabaseDefinition.SchemaDefinition schema : store.schemas()) {
            for (DatabaseDefinition.TableDefinition def : schema.tables()) {
                List<List<String>> r = rows.computeIfAbsent(def.name(), rowsOf);
                try (var __o = StatementOrigin.enter(StatementOrigin.SYSTEM)) {
                    for (String stmt : Ddl.metamodelSeed(def, schema.name(), dialect)) {
                        Executor.executeRaw(c, stmt);
                    }
                    Executor.load(c, dialect, Ddl.metamodelRows(def, schema.name(), r));
                }
            }
        }
        return new Session(c);
    }
}
