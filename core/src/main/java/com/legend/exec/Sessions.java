// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.database.Target;
import com.legend.model.AuthenticationSpec;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.model.ConnectionSpecification;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * THE execution-side owner of every per-database decision about a SESSION (docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER
 * _2026_10_03.md C3b): the product name a database's JDBC driver reports (what a handed session is checked against),
 * how a declared connection is opened (its JDBC URL from its specification and authentication), and a private
 * in-memory instance of a database (the platform's engine, the system database). Upstream's counterpart is the Java
 * {@code ConnectionManagerSelector} and its per-type {@code DatabaseManager}s. Every database type, specification and
 * authentication is listed; none has a {@code default} arm: anything not built is refused by name, never folded into
 * something that is.
 *
 * <p>The SQL a database is written in is the plan side's ({@code com.legend.database.Databases}); this class never
 * renders.
 */
public final class Sessions {

    private static final AtomicInteger PRIVATE_IDS = new AtomicInteger();

    private Sessions() {
    }

    /** A session handed to execution for a {@link Target}: closing it releases it — for a session the caller owns,
     *  nothing; for one the source opened, whatever that source's lease says. */
    public interface Session extends AutoCloseable {
        Connection connection();

        @Override
        void close();
    }

    /** Where execution gets the session for a target the compiler decided (the server's connection resolver; a caller's
     *  own connection, {@link #given}). {@code ctx} is the compiled model the target was decided from. */
    @FunctionalInterface
    public interface Source {
        Session open(Target target, com.legend.compiler.element.ModelContext ctx);
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
        return switch (type) {
            case DuckDB -> "DuckDB";
            case H2 -> "H2";
            case Postgres -> "PostgreSQL";
            case SQLite -> "SQLite";
            case DB2, MemSQL, Sybase, SybaseIQ, Composite, SqlServer, Hive, Snowflake, Presto, Trino, BigQuery,
                 Redshift, Databricks, Spanner, Athena, Oracle, ClickHouse, Aurora ->
                    throw new com.legend.error.NotImplementedException(
                            "sessions on database type '" + type + "' are not implemented");
        };
    }

    /** {@code session} is a session on {@code declared}: a session on another database is refused, never reinterpreted. */
    public static void check(Connection session, DatabaseType declared) {
        String product = metadata(session, true);
        if (!jdbcProduct(declared).equals(product)) {
            throw new com.legend.error.NotImplementedException("the query executes on " + declared
                    + " but the session is " + product + " — dialect/connection mismatch");
        }
    }

    /** The server version {@code session} reports ({@code DatabaseMetaData.getDatabaseProductVersion}). */
    public static String version(Connection session) {
        return metadata(session, false);
    }

    private static String metadata(Connection session, boolean product) {
        try {
            return product
                    ? session.getMetaData().getDatabaseProductName()
                    : session.getMetaData().getDatabaseProductVersion();
        } catch (SQLException e) {
            throw new com.legend.error.DataError(String.valueOf(e.getMessage()), e);
        }
    }

    /** How a declared connection is opened: a JDBC URL opened fresh per use, or an in-memory database whose identity
     *  the opener keeps (its tables persist across uses — the server's feature). */
    public sealed interface Opening {
        /** A database reached by URL; each use opens (and its lease closes) a connection. */
        record Url(String url) implements Opening {
        }

        /** An in-memory database that outlives its connections by NAME (H2, {@code DB_CLOSE_DELAY=-1}): each use
         *  opens a connection ({@link #openNamed}) the lease closes. {@code name} is the user's when the
         *  specification names it (an {@code EmbeddedH2} database name, shared by design), else null and the
         *  opener names it by the identity it keeps. */
        record Named(@com.legend.base.Nullable String name) implements Opening {
        }

        /** An in-memory database that lives exactly as long as its ONE connection (DuckDB, SQLite): the opener
         *  keeps that connection ({@link #openHeld}) for as long as the database's tables must persist. */
        record Held(DatabaseType type) implements Opening {
        }
    }

    /** The opening for {@code def}: its database type and specification, with its authentication checked. */
    public static Opening openingFor(ConnectionDefinition def) {
        authentication(def);
        ConnectionSpecification spec = def.specification();
        return switch (def.databaseType()) {
            case DuckDB -> switch (spec) {
                case ConnectionSpecification.LocalFile(String path) -> new Opening.Url("jdbc:duckdb:" + path);
                case ConnectionSpecification.InMemory() -> new Opening.Held(DatabaseType.DuckDB);
                case ConnectionSpecification.LocalH2 x -> refused(def);
                case ConnectionSpecification.EmbeddedH2 x -> refused(def);
                case ConnectionSpecification.StaticDatasource x -> refused(def);
                case ConnectionSpecification.Snowflake x -> refused(def);
                case ConnectionSpecification.Spanner x -> refused(def);
                case ConnectionSpecification.Databricks x -> refused(def);
                case ConnectionSpecification.BigQuery x -> refused(def);
            };
            case SQLite -> switch (spec) {
                case ConnectionSpecification.LocalFile(String path) -> new Opening.Url("jdbc:sqlite:" + path);
                case ConnectionSpecification.InMemory() -> new Opening.Held(DatabaseType.SQLite);
                case ConnectionSpecification.LocalH2 x -> refused(def);
                case ConnectionSpecification.EmbeddedH2 x -> refused(def);
                case ConnectionSpecification.StaticDatasource x -> refused(def);
                case ConnectionSpecification.Snowflake x -> refused(def);
                case ConnectionSpecification.Spanner x -> refused(def);
                case ConnectionSpecification.Databricks x -> refused(def);
                case ConnectionSpecification.BigQuery x -> refused(def);
            };
            case H2 -> switch (spec) {
                case ConnectionSpecification.LocalFile(String path) -> new Opening.Url("jdbc:h2:file:" + path);
                case ConnectionSpecification.StaticDatasource(String host, int port, String database) ->
                        new Opening.Url("jdbc:h2:tcp://" + host + ":" + port + "/" + database);
                // A19: a DISTINCT in-memory database per databaseName — the engine's directory-backed isolation
                // without disk side effects; a USER-NAMED database is user-chosen identity and shares BY DESIGN
                case ConnectionSpecification.EmbeddedH2(String dbName, String dir, boolean auto) ->
                        new Opening.Named(dbName);
                // legend-engine's test specification and an in-memory one: the opener names it
                case ConnectionSpecification.LocalH2 h -> new Opening.Named(null);
                case ConnectionSpecification.InMemory() -> new Opening.Named(null);
                case ConnectionSpecification.Snowflake x -> refused(def);
                case ConnectionSpecification.Spanner x -> refused(def);
                case ConnectionSpecification.Databricks x -> refused(def);
                case ConnectionSpecification.BigQuery x -> refused(def);
            };
            case Postgres -> switch (spec) {
                case ConnectionSpecification.StaticDatasource(String host, int port, String database) ->
                        new Opening.Url("jdbc:postgresql://" + host + ":" + port + "/" + database);
                case ConnectionSpecification.InMemory x -> refused(def);
                case ConnectionSpecification.LocalFile x -> refused(def);
                case ConnectionSpecification.LocalH2 x -> refused(def);
                case ConnectionSpecification.EmbeddedH2 x -> refused(def);
                case ConnectionSpecification.Snowflake x -> refused(def);
                case ConnectionSpecification.Spanner x -> refused(def);
                case ConnectionSpecification.Databricks x -> refused(def);
                case ConnectionSpecification.BigQuery x -> refused(def);
            };
            case DB2, MemSQL, Sybase, SybaseIQ, Composite, SqlServer, Hive, Snowflake, Presto, Trino, BigQuery,
                 Redshift, Databricks, Spanner, Athena, Oracle, ClickHouse, Aurora ->
                    throw new com.legend.error.NotImplementedException("connections to database type '"
                            + def.databaseType() + "' are not implemented (connection '" + def.qualifiedName() + "')");
        };
    }

    private static Opening refused(ConnectionDefinition def) {
        throw new com.legend.error.NotImplementedException("a " + def.databaseType() + " connection with a "
                + def.specification().getClass().getSimpleName() + " specification is not implemented (connection '"
                + def.qualifiedName() + "')");
    }

    /** The authentications a session needs nothing for; every other is refused by name, never ignored. */
    private static void authentication(ConnectionDefinition def) {
        switch (def.authentication()) {
            case AuthenticationSpec.NoAuth a -> {
            }
            case AuthenticationSpec.DefaultH2 a -> {
            }
            case AuthenticationSpec.TestAuth a -> {
            }
            case AuthenticationSpec.UsernamePassword a -> refusedAuth(def);
            case AuthenticationSpec.DelegatedKerberos a -> refusedAuth(def);
            case AuthenticationSpec.VaultUserNamePassword a -> refusedAuth(def);
            case AuthenticationSpec.SnowflakePublic a -> refusedAuth(def);
            case AuthenticationSpec.GCPApplicationDefaultCredentials a -> refusedAuth(def);
            case AuthenticationSpec.ApiToken a -> refusedAuth(def);
            case AuthenticationSpec.MiddleTierUserNamePassword a -> refusedAuth(def);
            case AuthenticationSpec.OAuth a -> refusedAuth(def);
            case AuthenticationSpec.GcpWorkloadIdentityFederation a -> refusedAuth(def);
        }
    }

    private static void refusedAuth(ConnectionDefinition def) {
        throw new com.legend.error.NotImplementedException(def.authentication().getClass().getSimpleName()
                + " authentication is not implemented (connection '" + def.qualifiedName() + "')");
    }

    /** Opens {@code url}. */
    public static Connection open(Opening.Url url) throws SQLException {
        return DriverManager.getConnection(url.url());
    }

    /** Opens a connection to the named H2 in-memory database {@code name}, which outlives it. */
    public static Connection openNamed(String name) throws SQLException {
        return DriverManager.getConnection("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1" + H2Settings.SETTINGS);
    }

    /** Opens the in-memory database a {@link Opening.Held} opening names: it lives as long as this connection. */
    public static Connection openHeld(Opening.Held held) throws SQLException {
        return openPrivate(held.type());
    }

    /** A PRIVATE in-memory database of {@code type}, gone when this connection closes (the system database, the
     *  platform's engine). */
    public static Connection openPrivate(DatabaseType type) throws SQLException {
        return switch (type) {
            case H2 -> DriverManager.getConnection("jdbc:h2:mem:private" + PRIVATE_IDS.getAndIncrement()
                    + H2Settings.SETTINGS, "sa", "");
            case DuckDB -> DriverManager.getConnection("jdbc:duckdb:");
            case SQLite -> DriverManager.getConnection("jdbc:sqlite::memory:");
            case Postgres, DB2, MemSQL, Sybase, SybaseIQ, Composite, SqlServer, Hive, Snowflake, Presto, Trino,
                 BigQuery, Redshift, Databricks, Spanner, Athena, Oracle, ClickHouse, Aurora ->
                    throw new com.legend.error.NotImplementedException(
                            "no in-memory instance of database type '" + type + "'");
        };
    }
}
