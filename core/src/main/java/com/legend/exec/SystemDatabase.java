package com.legend.exec;

import com.legend.compiler.element.ModelContext;
import com.legend.executionplan.ExecutionPlan;
import com.legend.model.DatabaseDefinition;
import com.legend.setup.Ddl;
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
 *
 * <p>A SERVER engine (Postgres, MariaDB, MySQL, Snowflake, Databricks,
 * BigQuery, ...) has no in-memory instance: its system database is the
 * HOST's SYSTEM CONNECTION for that engine (2026-10-11, the user's ruling
 * -- the same design over a remote database), configured apart from every
 * runtime's connection ({@link Sessions.Opener}: the platform holds no
 * server credential; a test may give it the same credentials, a host need
 * not). It is checked to BE that engine, since the query is planned in the
 * runtime's dialect. The same lifecycle: opened once per graph, every
 * table written then, read-only after. Each table is TEMPORARY, the
 * session's own (the private instance's guarantees on a shared server: no
 * other session sees it, two graphs or two runs never collide, and the
 * server drops it when the session ends, a crash included), named
 * {@link Ddl#systemTable}; the query reads it through its table renames
 * ({@link Route#renames}).
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

    /** One engine's session, and the store's tables as it names them. */
    private static final class Session {
        private final Sessions.Session session;
        private final Map<String, String> renames;

        private Session(Sessions.Session session, Map<String, String> renames) {
            this.session = session;
            this.renames = renames;
        }
    }

    /** Where a query reading the system store runs: the session holding the graph's metamodel (one per engine, the
     *  same object on every route, so the body driver's identity compare of two sides' sessions holds), and each
     *  store table ({@code schema.table}) renamed as that session holds it -- none in a private in-process database,
     *  which holds them as declared. */
    public record Route(Sessions.Session session, Map<String, String> renames) {
        public Route {
            renames = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(renames));
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
     * never this database. A server engine's session is the host's system
     * connection for it, from {@code systemConnections}; a server engine
     * with none is refused by name, never routed to a user's session.
     */
    public synchronized Route routeFor(
            com.legend.model.ConnectionDefinition.DatabaseType engine,
            SqlDialect dialect, DatabaseDefinition store,
            Function<String, List<List<String>>> rowsOf,
            Map<com.legend.model.ConnectionDefinition.DatabaseType, Sessions.Opener> systemConnections) {
        Session s = sessions.get(engine);
        if (s == null) {
            s = open(engine, dialect, store, rowsOf, systemConnections);
            sessions.put(engine, s);
        }
        return new Route(s.session, s.renames);
    }

    private Session open(com.legend.model.ConnectionDefinition.DatabaseType engine, SqlDialect dialect,
            DatabaseDefinition store, Function<String, List<List<String>>> rowsOf,
            Map<com.legend.model.ConnectionDefinition.DatabaseType, Sessions.Opener> systemConnections) {
        boolean inProcess = Sessions.inProcess(engine);
        Connection c = inProcess ? openPrivate(engine) : openSystem(engine, systemConnections);
        Session s = null;
        try {
            s = seed(c, engine, inProcess, dialect, store, rowsOf);
            closer.open.add(c);
            return s;
        } finally {
            if (s == null) {
                // a half-written system database is no one's: closed now (a server drops its temporary tables), and
                // the next query of the graph opens and writes it again; the seed's failure is the error reported
                closeQuietly(c);
            }
        }
    }

    private static void closeQuietly(Connection c) {
        try {
            c.close();
        } catch (SQLException ignore) {
            // closing after a failure: the failure is the error to report
        }
    }

    /** The store seeded on {@code c} as a plan's setup steps the plan runner runs on it ({@link PlanRunner#setUp},
     *  the doors, 2026-10-29; until then {@code SetupRunner} sent each on the session's statements): the dialect's
     *  session statements, then each table's DDL and rows ({@link Ddl#metamodelSteps}: for the database's bulk loader
     *  where it has one, else its one INSERT) in process, or its temporary table on a server engine, read under its
     *  system name through the renames. Under the SYSTEM origin. */
    private Session seed(Connection c, com.legend.model.ConnectionDefinition.DatabaseType engine, boolean inProcess,
            SqlDialect dialect, DatabaseDefinition store, Function<String, List<List<String>>> rowsOf) {
        Sessions.Session opened = PlanSessions.session(c);
        List<ExecutionPlan.SetupStep> steps = new ArrayList<>();
        for (String setup : dialect.sessionSetup()) {
            steps.add(new ExecutionPlan.SetupStep.Statement(setup));
        }
        Map<String, String> renames = new java.util.LinkedHashMap<>();
        for (DatabaseDefinition.SchemaDefinition schema : store.schemas()) {
            for (DatabaseDefinition.TableDefinition def : schema.tables()) {
                List<List<String>> r = rows.computeIfAbsent(def.name(), rowsOf);
                if (inProcess) {
                    steps.addAll(Ddl.metamodelSteps(def, schema.name(), r, dialect));
                } else {
                    for (String stmt : Ddl.metamodelTemporary(def, schema.name(), r, dialect)) {
                        steps.add(new ExecutionPlan.SetupStep.Statement(stmt));
                    }
                    renames.put(schema.name() + "." + def.name(), Ddl.systemTable(schema.name(), def.name()));
                }
            }
        }
        // the session's engine is the target's database: the one fact the runner checks a session against
        ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(engine),
                new ExecutionPlan.Servers.Every(), List.of(), List.of());
        try (var __o = StatementOrigin.enter(StatementOrigin.SYSTEM)) {
            PlanRunner.setUp(steps, target, PlanSessions.given(c), null);
        }
        return new Session(opened, renames);
    }

    private static Connection openPrivate(com.legend.model.ConnectionDefinition.DatabaseType engine) {
        try {
            return Sessions.openPrivate(engine);
        } catch (SQLException e) {
            throw new IllegalStateException("system database: cannot open the "
                    + engine + " session", e);
        }
    }

    /** The host's system connection for {@code engine}, checked to be on {@code engine} (the query is planned in its
     *  runtime's dialect): another database is refused, never written to. */
    private static Connection openSystem(com.legend.model.ConnectionDefinition.DatabaseType engine,
            Map<com.legend.model.ConnectionDefinition.DatabaseType, Sessions.Opener> systemConnections) {
        Sessions.Opener opener = systemConnections.get(engine);
        if (opener == null) {
            throw new com.legend.error.NotImplementedException("no system connection for " + engine
                    + " is configured: a query reading the system metamodel store on a server engine runs on the"
                    + " host's system connection, apart from every user connection (ExecuteOptions"
                    + ".withSystemConnections)");
        }
        Connection c;
        try {
            c = opener.open();
        } catch (SQLException e) {
            throw new IllegalStateException("system database: cannot open the " + engine
                    + " system connection: " + e.getMessage(), e);
        }
        boolean checked = false;
        try {
            Sessions.check(c, engine);
            checked = true;
        } finally {
            if (!checked) {
                closeQuietly(c);
            }
        }
        return c;
    }
}
