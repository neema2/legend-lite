// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.database.Target;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionDefinition.DatabaseType;

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
 * <p>The SQL a database is written in is the plan side's ({@code com.legend.database.Databases}); this class never
 * renders.
 */
public final class Sessions {

    private Sessions() {
    }

    /** A session handed to execution for a {@link Target}: closing it releases it — for a session the caller owns,
     *  nothing; for one the source opened, whatever that source's lease says. */
    public interface Session extends AutoCloseable {
        Connection connection();

        /** The statements this session runs for the plan runner ({@link Statements}): JDBC's, over
         *  {@link #connection()}, unless the session's database speaks none -- a connector's session gives its own
         *  (2026-10-17, docs/MONGODB_PLAN_2026_10_16.md D12). */
        default Statements statements() {
            return new JdbcStatements(connection());
        }

        /** The body driver's statements on this session: as {@link #statements()}, each query's execution-trace
         *  stamp published to {@code trace} (an activity row records the comment its statement carried,
         *  {@link ExecutionTrace}); as {@link #statements()} when {@code trace} is null. A session whose statements
         *  carry no SQL comment refuses by name (track B's B1b, 2026-10-29: the driver holds a session, never a
         *  connection, and reaches the database only through its statements). */
        default Statements statements(@com.legend.base.Nullable ExecutionTrace trace) {
            return new JdbcStatements(connection(), trace);
        }

        /** What a body driver established on this session ({@link SessionState}): the seeds it ran, the database's
         *  reported column types per statement, whether a write ran since -- facts that live exactly as long as the
         *  session's connection, however many times the connection is wrapped as a session (B1b). */
        default SessionState state() {
            return SessionState.of(connection());
        }

        /** The database server's version, as its driver reports it ({@link Sessions#version}): what a plan's target
         *  is written for (B1b: the judges read it through the session). */
        default String version() {
            return Sessions.version(connection());
        }

        /** {@code work} in one transaction on this session ({@link Statements#inTransaction}: a caller's open one
         *  joined): the body driver's bracket for a program that writes or stages rows (the doors, 2026-10-29: a
         *  session's own fact, reached without its statements, which only the plan runner takes). */
        default <T> T inTransaction(java.util.function.Supplier<T> work) {
            return statements().inTransaction(work);
        }

        /** Whether a send on this session owns its transaction ({@link Statements#ownsTransaction}). */
        default boolean ownsTransaction() {
            return statements().ownsTransaction();
        }

        @Override
        void close();
    }

    /** Where execution gets the session for a target the compiler decided (the server's connection resolver; a caller's
     *  own connection, {@link #given}). {@code ctx} is the compiled model the target was decided from. */
    @FunctionalInterface
    public interface Source {
        Session open(Target target, com.legend.compiler.element.ModelContext ctx);
    }

    /** Opens a session on a system connection: the HOST's to configure, apart from every runtime's (the platform holds
     *  no server credential, C3b) -- a server engine's system database lives in it ({@link SystemDatabase}). */
    @FunctionalInterface
    public interface Opener {
        Connection open() throws SQLException;
    }

    /** Does {@code type} have a private in-process instance ({@link #openPrivate})? A server engine has none: its
     *  system database is on a system connection the host configures ({@link Opener}). */
    public static boolean inProcess(DatabaseType type) {
        return Connectors.of(type).inProcess();
    }

    /** A caller's own session: checked against the target's database, never closed here. */
    public static Source given(Connection connection) {
        return (target, ctx) -> {
            check(connection, target.type());
            return new Session() {
                @Override
                public Connection connection() {
                    return connection;
                }

                @Override
                public void close() {
                    // the caller owns its connection
                }
            };
        };
    }

    /** The product name the JDBC driver of {@code type} reports ({@code DatabaseMetaData.getDatabaseProductName}). */
    public static String jdbcProduct(DatabaseType type) {
        return Connectors.of(type).jdbcProduct();
    }

    /** {@code session} is a session on {@code declared}: a session on another database is refused, never reinterpreted. */
    public static void check(Connection session, DatabaseType declared) {
        String product = metadata(session, true);
        if (!jdbcProduct(declared).equals(product)) {
            throw new com.legend.error.NotImplementedException("the query executes on " + declared
                    + " but the session is " + product + " — dialect/connection mismatch");
        }
    }

    /** Whether the session commits each statement itself, so a transaction is its holder's to open: a caller that
     *  turned autocommit off (the corpus harness brackets an effect body as one attempt, committed on a pass and
     *  rolled back on a failure) holds it; a script's bracket must not nest inside, and a failure is the caller's to
     *  unwind. */
    public static boolean ownsTransaction(Connection session) {
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
     * joined, neither committed nor rolled back here (the parity harnesses roll every write case back).
     */
    public static <T> T inTransaction(Connection session, java.util.function.Supplier<T> work) {
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
            unwind(session, failure);
            throw failure;
        } catch (SQLException e) {
            com.legend.error.DataError failure = PlanSessions.dataError(e, () -> JdbcStatements.held(session));
            unwind(session, failure);
            throw failure;
        }
        try {
            session.setAutoCommit(true);
        } catch (SQLException e) {
            throw new com.legend.error.DataError(String.valueOf(e.getMessage()), e);
        }
        return out;
    }

    /** The transaction rolled back and autocommit restored after {@code failure}; a failure of either is suppressed
     *  into it, so the work's own failure is the one reported. */
    private static void unwind(Connection session, Throwable failure) {
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

    /** The server version {@code session} reports ({@code DatabaseMetaData.getDatabaseProductVersion}). */
    public static String version(Connection session) {
        return metadata(session, false);
    }

    private static String metadata(Connection session, boolean product) {
        try {
            return product ? Connectors.product(session) : session.getMetaData().getDatabaseProductVersion();
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

        /** An in-memory database that outlives its connections by NAME ({@code memory}'s, its database's connector):
         *  each use opens a connection ({@link #openNamed}) the lease closes. {@code name} is the user's when the
         *  specification names it (an embedded database's name, shared by design), else null and the opener names it
         *  by the identity it keeps. */
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
    public static Opening openingFor(ConnectionDefinition def) {
        if (!def.authentication().credentialFree()) {
            throw new com.legend.error.NotImplementedException(def.authentication().getClass().getSimpleName()
                    + " authentication is not implemented (connection '" + def.qualifiedName() + "')");
        }
        return Connectors.of(def.databaseType()).opening(def);
    }

    /** Opens {@code url}. */
    public static Connection open(Opening.Url url) throws SQLException {
        return DriverManager.getConnection(url.url());
    }

    /** Opens a connection to {@code named}'s in-memory database {@code name}, which outlives it. */
    public static Connection openNamed(Opening.Named named, String name) throws SQLException {
        return named.memory().openNamed(name);
    }

    /** Opens a connection to {@code named}'s in-memory database {@code name}, which lives only while a connection to
     *  it is open: its opener keeps one, and when that one closes with no other open, the database is gone. */
    public static Connection openKept(Opening.Named named, String name) throws SQLException {
        return named.memory().openKept(name);
    }

    /** Opens another connection to the kept database {@code name} ({@link #openKept}), which must still be open: one
     *  whose keeper closed is gone, and connecting fails by name rather than making an empty database of that name. */
    public static Connection joinKept(Opening.Named named, String name) throws SQLException {
        return named.memory().joinKept(name);
    }

    /** Opens the in-memory database a {@link Opening.Held} opening names: it lives as long as this connection. */
    public static Connection openHeld(Opening.Held held) throws SQLException {
        return held.connector().openPrivate();
    }

    /** A PRIVATE in-memory database of {@code type}, gone when this connection closes (the system database, the
     *  platform's engine). */
    public static Connection openPrivate(DatabaseType type) throws SQLException {
        return Connectors.of(type).openPrivate();
    }
}
