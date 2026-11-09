// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.DatabaseType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * THE execution-side entry to a SESSION (docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md C3b): the product
 * name a database's JDBC driver reports (what a handed session is checked against), how a declared connection is
 * opened, and a private in-memory instance of a database (the platform's engine, the system database). Each answer is
 * the database's connector's ({@link Connectors}, the dialect program's Phase 5, 2026-10-16: until then this class held
 * a switch of every database for each); this class decides nothing by a database of its own. Upstream's counterpart is
 * the Java {@code ConnectionManagerSelector} and its per-type {@code DatabaseManager}s.
 *
 * <p>THE ONE WAY IN (D70, 2026-10-30, the seam's step A): a session is made here and nowhere else. A connection the
 * caller opened enters as {@link #owned} (closed with the session) or {@link #borrowed} (never closed here); a session
 * is shared as a {@link #view} (closing the view closes nothing); the openers ({@link #open(Opening.Url)},
 * {@link #openPrivate} and the rest) return owned sessions. {@link Session} is an abstract class whose constructor and
 * whose one door to the driver's connection are package-private: no class outside this package can be a session, and
 * nothing outside it reaches a connection through one (the pass-along hole the D69 audit named, closed by the Java
 * access rules); a holder of a view can learn which session it views ({@link #identity}: an opaque token) and nothing
 * more. Until D70 the seam offered ten ways in across two classes, and the server's resolver and the test backends
 * implemented sessions of their own over raw connections. A product session OWNS its connection: {@link #owned} is
 * called by the openers alone, and {@link #borrowed} by no product class
 * ({@code ArchitectureTest.theProductBorrowsNoConnection}) -- a connection wrapped twice would be two sessions with
 * two memos, and a declared seed (DROP IF EXISTS, CREATE) re-run on the second would silently drop what the first
 * wrote; the test side, which holds raw connections, keeps one session per connection ({@code TestSessions}).
 *
 * <p>The SQL a database is written in is the plan side's ({@code com.legend.database.Databases}); this class never
 * renders.
 */
public final class Sessions {
    private Sessions() {
    }

    /** A session handed to execution for a target (the driver's {@code com.legend.exec.SessionSource} since D72, 2026-10-30): closing it releases it -- for a session the caller owns,
     *  nothing; for one the source opened, whatever that source's lease says. Its state ({@link SessionState}: the
     *  seeds established on it, the database's reported column types, the session statements run, D66) is the owning
     *  session's own, one per connection, shared by every {@link #view} of it -- until D70 a process-wide weak map
     *  keyed by the driver's connection held it, because a connection was wrapped as a session anew at every call.
     *  A session is made by this class alone (D70): the constructor and {@link #connection()} are package-private. */
    public abstract static class Session implements AutoCloseable {
        /** This session's identity ({@link Sessions#identity}): an opaque token, so a holder of a view can tell
         *  which session it views and reach nothing of it (the D70 audit: the owner itself would be a closing
         *  session in a view-holder's hands). */
        private final Object token = new Object();

        Session() {
        }

        /** The token of the session this one is: its own, or the owner's for a view. */
        Object identity() {
            return token;
        }

        /** The driver's connection beneath this session: the seam's alone. A session whose database speaks no JDBC
         *  (a connector's own, MongoDB's) refuses by name. */
        abstract Connection connection();

        /** What a body driver established on this session ({@link SessionState}): the seeds it ran, the database's
         *  reported column types per statement, whether a write ran since, the session statements run -- facts that
         *  live exactly as long as the session, however many views of it are taken (B1b; D70: the owner's). */
        public abstract SessionState state();

        /** The statements this session runs for the plan runner ({@link Statements}): JDBC's, over
         *  {@link #connection()}, unless the session's database speaks none -- a connector's session gives its own
         *  (2026-10-17, docs/MONGODB_PLAN_2026_10_16.md D12). Taken inside a plan's run alone (THE WALL's rule 3,
         *  {@link com.legend.base.RunScope#statement}, 2026-11-06): the one door to a session's statements. */
        public final Statements statements() {
            com.legend.base.RunScope.statement("a session's statements");
            return statementsOf(null);
        }

        /** The body driver's statements on this session: as {@link #statements()}, each query's execution-trace
         *  stamp published to {@code trace} (an activity row records the comment its statement carried,
         *  {@link ExecutionTrace}); as {@link #statements()} when {@code trace} is null. A session whose statements
         *  carry no SQL comment refuses by name (track B's B1b, 2026-10-29: the driver holds a session, never a
         *  connection, and reaches the database only through its statements). */
        public final Statements statements(@com.legend.base.Nullable ExecutionTrace trace) {
            com.legend.base.RunScope.statement("a session's statements");
            return statementsOf(trace);
        }

        /** This session's statements, unguarded: the session's own form (JDBC's; a view's its owner's; a connector's
         *  its own), reached through {@link #statements} alone -- and by the owner's transaction bracket. */
        Statements statementsOf(@com.legend.base.Nullable ExecutionTrace trace) {
            return trace == null ? new JdbcStatements(connection(), state())
                    : new JdbcStatements(connection(), state(), trace);
        }

        /** {@code work} in one transaction on this session ({@link Statements#inTransaction}: a caller's open one
         *  joined): the body driver's bracket for a program that writes or stages rows (the doors, 2026-10-29: a
         *  session's own fact, reached without its statements, which only the plan runner takes). The owner's act,
         *  as an open is: refused while this thread plans (THE WALL's rule 2). */
        public <T> T inTransaction(java.util.function.Supplier<T> work) {
            com.legend.base.RunScope.opening("a transaction bracket");
            return statementsOf(null).inTransaction(work);
        }

        /** Whether a send on this session owns its transaction ({@link Statements#ownsTransaction}). The owner's
         *  question: refused while this thread plans. */
        public boolean ownsTransaction() {
            com.legend.base.RunScope.opening("a transaction's owner asked");
            return statementsOf(null).ownsTransaction();
        }

        @Override
        public abstract void close();
    }

    /** A session over a JDBC connection: the owning one, whose state is the connection's. */
    private static final class Jdbc extends Session {
        private final Connection c;
        private final boolean owning;
        private final SessionState state = new SessionState();

        Jdbc(Connection c, boolean owning) {
            this.c = c;
            this.owning = owning;
        }

        @Override
        Connection connection() {
            return c;
        }

        @Override
        public SessionState state() {
            return state;
        }

        @Override
        public void close() {
            if (!owning) {
                return;
            }
            try {
                c.close();
            } catch (SQLException e) {
                throw RaisedErrors.dataError(e);
            }
        }
    }

    /** A non-closing view of a session: the owner's connection, state and version; its statements traced to
     *  {@code trace} where the view was taken with one. */
    private static final class View extends Session {
        private final Session owner;
        private final @com.legend.base.Nullable ExecutionTrace trace;

        View(Session owner, @com.legend.base.Nullable ExecutionTrace trace) {
            this.owner = owner;
            this.trace = trace;
        }

        @Override
        Connection connection() {
            return owner.connection();
        }

        @Override
        Object identity() {
            return owner.identity();
        }

        @Override
        public SessionState state() {
            return owner.state();
        }

        @Override
        Statements statementsOf(@com.legend.base.Nullable ExecutionTrace traced) {
            return owner.statementsOf(traced != null ? traced : trace);
        }

        @Override
        public void close() {
            // a view: the owner closes
        }
    }

    /** A session OPENED ON FIRST USE (THE WALL, 2026-11-06): {@code open} runs when the session's connection is first
     *  reached -- a statement taken, inside a plan's run (rule 3) -- never when the session is made, so a planner may
     *  name it (the graph's system database, {@code SystemDatabase}) and open nothing (rule 2). Its state is its own
     *  from the start; closing it closes what it opened, if anything. */
    public static Session lazy(java.util.function.Supplier<Session> open) {
        return new Lazy(open);
    }

    /** {@link #lazy}'s session. */
    private static final class Lazy extends Session {
        private final java.util.function.Supplier<Session> open;
        private final SessionState state = new SessionState();
        /** The session beneath, once opened (set once). */
        private final java.util.concurrent.atomic.AtomicReference<@com.legend.base.Nullable Session> opened =
                new java.util.concurrent.atomic.AtomicReference<>();

        Lazy(java.util.function.Supplier<Session> open) {
            this.open = open;
        }

        /** Whether this session was closed: it opens nothing after (the final audit, 2026-11-06). */
        private final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();

        /** The session beneath, opened now if it is not yet; refused once this session is closed. */
        synchronized Session opened() {
            if (closed.get()) {
                throw new IllegalStateException("a session opened on first use, reached after it was closed");
            }
            Session o = opened.get();
            if (o == null) {
                o = open.get();
                opened.set(o);
            }
            return o;
        }

        /** The session beneath where it was opened, else null (nothing opened by asking). */
        @com.legend.base.Nullable Session ifOpened() {
            return opened.get();
        }

        @Override
        Connection connection() {
            return opened().connection();
        }

        @Override
        public SessionState state() {
            return state;
        }

        @Override
        public synchronized void close() {
            closed.set(true);
            Session o = ifOpened();
            if (o != null) {
                o.close();
            }
        }
    }

    /** A session that OWNS {@code connection}: closing it closes the connection (a close that fails is a
     *  {@link com.legend.error.DataError}). */
    public static Session owned(Connection connection) {
        return new Jdbc(connection, true);
    }

    /** A session over a connection its caller keeps: closing it closes nothing. One session per connection is the
     *  caller's to keep (D70): a second session over the same connection has a state of its own. */
    public static Session borrowed(Connection connection) {
        return new Jdbc(connection, false);
    }

    /** A non-closing view of {@code session}: the same connection, state and version; closing it closes nothing
     *  (the program entry's, the test runners': D68). */
    public static Session view(Session session) {
        return new View(session, null);
    }

    /** {@link #view(Session)} whose statements publish each query's stamp to {@code trace} (the body driver's). */
    static Session view(Session session, @com.legend.base.Nullable ExecutionTrace trace) {
        return new View(session, trace);
    }

    /** The identity of the session {@code session} is or views: an opaque token, the same for every view of one
     *  session and for the session itself, and nothing else -- not the session, whose close is its owner's. A test
     *  harness keeps its own connection per session it opened by this ({@code TestSessions}: the corpus's referee
     *  and fixture asides send their own JDBC on the body's connection); the product asks nothing of it. */
    public static Object identity(Session session) {
        return session.identity();
    }

    /** The session beneath any number of views (and a lazy session's, opened if it is not yet): the one whose
     *  connection they share. */
    private static Session root(Session session) {
        Session s = session;
        while (true) {
            if (s instanceof View v) {
                s = v.owner;
            } else if (s instanceof Lazy l) {
                s = l.opened();
            } else {
                return s;
            }
        }
    }

    /** Whether {@code session}'s connection is closed -- or unanswerable, which only a broken connection produces;
     *  treating it live would cache the wreck (the handle stores' liveness test). A session over no JDBC connection
     *  (a connector's own) is never closed by this test: none is kept in a handle store. */
    public static boolean closed(Session session) {
        Session s = session;
        while (s instanceof View v) {
            s = v.owner;
        }
        if (s instanceof Lazy l) {
            Session o = l.ifOpened();
            if (o == null) {
                return false;   // nothing opened: nothing closed (asking opens nothing)
            }
            return closed(o);
        }
        if (!(s instanceof Jdbc jdbc)) {
            return false;
        }
        try {
            return jdbc.c.isClosed();
        } catch (SQLException e) {
            return true;
        }
    }

    /** Whether the session's database has a bulk loader for a plan's rows ({@link BulkLoads}); a session whose
     *  database speaks no JDBC (a connector's own) answers for itself at the load. */
    static boolean loadsRows(Session session) {
        if (!(root(session) instanceof Jdbc jdbc)) {
            return true;
        }
        try {
            return BulkLoads.of(jdbc.c) != null;
        } catch (SQLException e) {
            throw RaisedErrors.dataError(e);
        }
    }

    /** The product name the driver of {@code session} reports: the handle a refusal names. */
    static String product(Session session) {
        return product(session.connection());
    }

    /** Opens a session on a system connection: the HOST's to configure, apart from every runtime's (the platform holds
     *  no server credential, C3b) -- a server engine's system database lives in it (the driver's {@code SystemDatabase} since D72). */
    @FunctionalInterface
    public interface Opener {
        Connection open() throws SQLException;
    }

    /** The session {@code opener} opens, owned: a failure of the open is a {@link com.legend.error.DataError}. */
    public static Session open(Opener opener) {
        com.legend.base.RunScope.opening("a session opened");
        try {
            return owned(opener.open());
        } catch (SQLException e) {
            throw RaisedErrors.dataError(e);
        }
    }

    /** Does {@code type} have a private in-process instance ({@link #openPrivate})? A server engine has none: its
     *  system database is on a system connection the host configures ({@link Opener}). */
    public static boolean inProcess(DatabaseType type) {
        return Connectors.of(type).inProcess();
    }

    /** The product name the JDBC driver of {@code type} reports ({@code DatabaseMetaData.getDatabaseProductName}). */
    public static String jdbcProduct(DatabaseType type) {
        return Connectors.of(type).jdbcProduct();
    }

    /** {@code session} is a session on {@code declared}: a session on another database is refused, never
     *  reinterpreted (D67, 2026-10-30: the execution front door holds no connection). */
    public static void check(Session session, DatabaseType declared) {
        com.legend.base.RunScope.statement("a session's check");
        check(session.connection(), declared);
    }

    /** {@link #check(Session, DatabaseType)} of the driver's connection (the seam's). */
    static void check(Connection session, DatabaseType declared) {
        String product = product(session);
        if (!jdbcProduct(declared).equals(product)) {
            throw new com.legend.error.NotImplementedException("the query executes on " + declared
                    + " but the session is " + product + " — dialect/connection mismatch");
        }
    }

    /** Whether the session commits each statement itself, so a transaction is its holder's to open: a caller that
     *  turned autocommit off (the corpus harness brackets an effect body as one attempt, committed on a pass and
     *  rolled back on a failure) holds it; a script's bracket must not nest inside, and a failure is the caller's to
     *  unwind. */
    static boolean ownsTransaction(Connection session) {
        try {
            return session.getAutoCommit();
        } catch (SQLException e) {
            throw new com.legend.error.DataError(String.valueOf(e.getMessage()), e);
        }
    }

    /**
     * {@code work} in one transaction of {@code session} (the writes design's L0, §4): when the session commits each
     * statement itself, autocommit is turned off, the work's statements commit together, and on any failure they roll
     * back, autocommit restored either way; when the caller turned autocommit off, the transaction is the caller's:
     * joined, neither committed nor rolled back here (the parity harnesses roll every write case back). {@code state}
     * is the session's: a rollback clears its memo of the session statements (D66).
     */
    static <T> T inTransaction(Connection session, SessionState state, java.util.function.Supplier<T> work) {
        if (!ownsTransaction(session)) {
            return work.get();
        }
        try {
            session.setAutoCommit(false);
        } catch (SQLException e) {
            throw new com.legend.error.DataError(String.valueOf(e.getMessage()), e);
        }
        T out;
        try {
            out = work.get();
            session.commit();
        } catch (RuntimeException | Error failure) {
            unwind(session, state, failure);
            throw failure;
        } catch (SQLException e) {
            com.legend.error.DataError failure = RaisedErrors.dataError(e, () -> JdbcStatements.held(session));
            unwind(session, state, failure);
            throw failure;
        }
        try {
            session.setAutoCommit(true);
        } catch (SQLException e) {
            throw new com.legend.error.DataError(String.valueOf(e.getMessage()), e);
        }
        return out;
    }

    /** A run's turn on the process's system CATALOG begins (docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md 4.5, L4b,
     *  2026-11-09): autocommit off, so what the run registers there -- its model's rows -- stays inside one transaction
     *  that {@link #endTurn} rolls back. Its holder's act ({@code SystemDatabase}), refused while this thread plans. */
    static void beginTurn(Session session) {
        com.legend.base.RunScope.opening("a catalog's turn");
        try {
            session.connection().setAutoCommit(false);
        } catch (SQLException e) {
            throw new com.legend.error.DataError(String.valueOf(e.getMessage()), e);
        }
    }

    /** A run's turn on the catalog ends: its transaction rolled back -- whatever the run registered or read -- and
     *  autocommit restored; the session's memo forgets what ran inside it (a registration, a session statement). */
    static void endTurn(Session session) {
        com.legend.base.RunScope.opening("a catalog's turn");
        session.state().rolledBack();
        try {
            Connection c = session.connection();
            c.rollback();
            c.setAutoCommit(true);
        } catch (SQLException e) {
            throw new com.legend.error.DataError(String.valueOf(e.getMessage()), e);
        }
    }

    /** The transaction rolled back and autocommit restored after {@code failure}; a failure of either is suppressed
     *  into it, so the work's own failure is the one reported. */
    private static void unwind(Connection session, SessionState state, Throwable failure) {
        // a session statement run inside the transaction may be undone with it (Postgres's SET is transactional): the
        // session's memo forgets them, and the next statement runs them again (D66) -- before the rollback, whose own
        // failure would leave the transaction's fate unknown
        state.rolledBack();
        try {
            session.rollback();
        } catch (SQLException e) {
            failure.addSuppressed(e);
        }
        try {
            session.setAutoCommit(true);
        } catch (SQLException e) {
            failure.addSuppressed(e);
        }
    }

    /** The database product {@code session} reports (its connector reads it). */
    private static String product(Connection session) {
        try {
            return Connectors.product(session);
        } catch (SQLException e) {
            throw new com.legend.error.DataError(String.valueOf(e.getMessage()), e);
        }
    }

    /** How a declared connection is opened, as its database's connector answers: a JDBC URL opened fresh per use, or an
     *  in-memory database whose identity the opener keeps (its tables persist across uses — the server's feature). */
    public sealed interface Opening {
        /** A database reached by URL; each use opens (and its lease closes) a connection. */
        record Url(String url) implements Opening {
        }

        /** An in-memory database a connector keeps by name: the user's name where the declaration gives one, else one
         *  the opener makes. */
        record Named(DatabaseConnector.NamedMemory memory, @com.legend.base.Nullable String name) implements Opening {
        }

        /** An in-memory database that lives exactly as long as its ONE connection: the opener keeps that connection
         *  ({@link #openHeld}, {@code connector}'s private instance) for as long as the database's tables must
         *  persist. */
        record Held(DatabaseConnector connector) implements Opening {
        }
    }

    /** The opening for {@code def}: its database's connector's, from its specification, with its authentication
     *  checked first: one that carries a credential is refused by name, never ignored (no session the platform opens
     *  reads one yet). */
    public static Opening openingFor(com.legend.executionplan.Connection def) {
        if (!def.authentication().credentialFree()) {
            throw new com.legend.error.NotImplementedException(def.authentication().getClass().getSimpleName()
                    + " authentication is not implemented (connection '" + def.qualifiedName() + "')");
        }
        return Connectors.of(def.databaseType()).opening(def);
    }

    /** Opens {@code url}: an owned session (a failure of the open is a {@link com.legend.error.DataError}, as every
     *  opener's is since D70). */
    public static Session open(Opening.Url url) {
        com.legend.base.RunScope.opening("a session opened");
        try {
            return owned(DriverManager.getConnection(url.url()));
        } catch (SQLException e) {
            throw RaisedErrors.dataError(e);
        }
    }

    /** Opens a session on {@code named}'s in-memory database {@code name}, which outlives it. */
    public static Session openNamed(Opening.Named named, String name) {
        com.legend.base.RunScope.opening("a session opened");
        try {
            return owned(named.memory().openNamed(name));
        } catch (SQLException e) {
            throw RaisedErrors.dataError(e);
        }
    }

    /** Opens a session on {@code named}'s in-memory database {@code name}, which lives only while a connection to
     *  it is open: its opener keeps one, and when that one closes with no other open, the database is gone. */
    public static Session openKept(Opening.Named named, String name) {
        com.legend.base.RunScope.opening("a session opened");
        try {
            return owned(named.memory().openKept(name));
        } catch (SQLException e) {
            throw RaisedErrors.dataError(e);
        }
    }

    /** Opens another session on the kept database {@code name} ({@link #openKept}), which must still be open: one
     *  whose keeper closed is gone, and connecting fails by name rather than making an empty database of that name. */
    public static Session joinKept(Opening.Named named, String name) {
        com.legend.base.RunScope.opening("a session opened");
        try {
            return owned(named.memory().joinKept(name));
        } catch (SQLException e) {
            throw RaisedErrors.dataError(e);
        }
    }

    /** Opens the in-memory database a {@link Opening.Held} opening names: it lives as long as this session. */
    public static Session openHeld(Opening.Held held) {
        com.legend.base.RunScope.opening("a session opened");
        try {
            return owned(held.connector().openPrivate());
        } catch (SQLException e) {
            throw RaisedErrors.dataError(e);
        }
    }

    /** A PRIVATE in-memory database of {@code type}, gone when this session closes (the system database, the
     *  platform's engine); a type with none is refused by name. */
    public static Session openPrivate(DatabaseType type) {
        com.legend.base.RunScope.opening("a session opened");
        try {
            return owned(Connectors.of(type).openPrivate());
        } catch (SQLException e) {
            throw RaisedErrors.dataError(e);
        }
    }
}
