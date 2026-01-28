package org.finos.legend.engine.execution;

import org.finos.legend.pure.dsl.definition.AuthenticationSpec;
import org.finos.legend.pure.dsl.definition.ConnectionDefinition;
import org.finos.legend.pure.dsl.definition.ConnectionSpecification;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves a ConnectionDefinition to a live java.sql.Connection.
 * 
 * Supports:
 * - InMemory DuckDB and SQLite
 * - LocalFile DuckDB
 * - Static datasource with host/port/database
 * 
 * In-memory connections are cached so that tables persist across requests.
 */
public class ConnectionResolver {

    // Force-load JDBC drivers at class initialization
    static {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            System.err.println("DuckDB driver not found in classpath");
        }
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            System.err.println("SQLite driver not found in classpath");
        }
    }

    // Cache for in-memory connections to persist tables across requests
    private static final Map<String, Connection> connectionCache = new ConcurrentHashMap<>();

    /**
     * Resolves the given ConnectionDefinition to a JDBC Connection.
     * In-memory connections are cached for persistence across requests.
     * 
     * @param connectionDef The connection definition to resolve
     * @return A live JDBC Connection
     * @throws SQLException If connection fails
     */
    public Connection resolve(ConnectionDefinition connectionDef) throws SQLException {
        return switch (connectionDef.databaseType()) {
            case DuckDB -> resolveDuckDB(connectionDef);
            case SQLite -> resolveSQLite(connectionDef);
            case H2 -> resolveH2(connectionDef);
            case Postgres -> resolvePostgres(connectionDef);
            case Snowflake, BigQuery -> throw new UnsupportedOperationException(
                    "Cloud databases not yet supported: " + connectionDef.databaseType());
        };
    }

    private Connection resolveDuckDB(ConnectionDefinition connectionDef) throws SQLException {
        return switch (connectionDef.specification()) {
            case ConnectionSpecification.InMemory() -> getOrCreateCached(
                    "duckdb:inmemory:" + connectionDef.qualifiedName(),
                    () -> DriverManager.getConnection("jdbc:duckdb:"));
            case ConnectionSpecification.LocalFile(String path) ->
                DriverManager.getConnection("jdbc:duckdb:" + path);
            case ConnectionSpecification.StaticDatasource staticDs ->
                throw new UnsupportedOperationException("DuckDB does not support remote connections");
        };
    }

    private Connection resolveSQLite(ConnectionDefinition connectionDef) throws SQLException {
        return switch (connectionDef.specification()) {
            case ConnectionSpecification.InMemory() -> getOrCreateCached(
                    "sqlite:inmemory:" + connectionDef.qualifiedName(),
                    () -> DriverManager.getConnection("jdbc:sqlite::memory:"));
            case ConnectionSpecification.LocalFile(String path) ->
                DriverManager.getConnection("jdbc:sqlite:" + path);
            case ConnectionSpecification.StaticDatasource staticDs ->
                throw new UnsupportedOperationException("SQLite does not support remote connections");
        };
    }

    private Connection resolveH2(ConnectionDefinition connectionDef) throws SQLException {
        String jdbcUrl = switch (connectionDef.specification()) {
            case ConnectionSpecification.InMemory() -> "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
            case ConnectionSpecification.LocalFile(String path) -> "jdbc:h2:file:" + path;
            case ConnectionSpecification.StaticDatasource(String host, int port, String database) ->
                "jdbc:h2:tcp://" + host + ":" + port + "/" + database;
        };

        return applyAuthentication(DriverManager.getConnection(jdbcUrl), connectionDef);
    }

    private Connection resolvePostgres(ConnectionDefinition connectionDef) throws SQLException {
        ConnectionSpecification.StaticDatasource staticDs = switch (connectionDef.specification()) {
            case ConnectionSpecification.StaticDatasource s -> s;
            default -> throw new UnsupportedOperationException(
                    "Postgres requires static datasource with host/port/database");
        };

        String jdbcUrl = "jdbc:postgresql://" + staticDs.host() + ":" + staticDs.port() + "/" + staticDs.database();

        return applyAuthentication(DriverManager.getConnection(jdbcUrl), connectionDef);
    }

    private Connection applyAuthentication(Connection connection, ConnectionDefinition connectionDef)
            throws SQLException {
        return switch (connectionDef.authentication()) {
            case AuthenticationSpec.NoAuth() -> connection;
            case AuthenticationSpec.UsernamePassword(String username, String passwordVaultRef) -> {
                // For now, we only support NoAuth in the clean room implementation
                // Vault integration would be added later
                throw new UnsupportedOperationException(
                        "UsernamePassword authentication not yet implemented. " +
                                "Use NoAuth for in-memory databases.");
            }
        };
    }

    /**
     * Gets a cached connection or creates a new one.
     * Used for in-memory databases to persist tables across requests.
     */
    private Connection getOrCreateCached(String key, ConnectionSupplier supplier) throws SQLException {
        Connection cached = connectionCache.get(key);
        if (cached != null && !cached.isClosed()) {
            return cached;
        }

        Connection newConn = supplier.get();
        connectionCache.put(key, newConn);
        return newConn;
    }

    @FunctionalInterface
    private interface ConnectionSupplier {
        Connection get() throws SQLException;
    }

    /**
     * Creates an in-memory DuckDB connection without a definition.
     * Convenience method for testing.
     */
    public static Connection createInMemoryDuckDB() throws SQLException {
        return DriverManager.getConnection("jdbc:duckdb:");
    }

    /**
     * Creates an in-memory SQLite connection without a definition.
     * Convenience method for testing.
     */
    public static Connection createInMemorySQLite() throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite::memory:");
    }

    /**
     * Clears all cached connections. Useful for testing.
     */
    public static void clearCache() {
        connectionCache.values().forEach(conn -> {
            try {
                conn.close();
            } catch (SQLException ignored) {
            }
        });
        connectionCache.clear();
    }
}
