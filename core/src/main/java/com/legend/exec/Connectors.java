// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionDefinition.DatabaseType;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * THE registry of the execution side (the dialect program's Phase 5): each declared {@link DatabaseType} to the
 * {@link DatabaseConnector} that opens its sessions, as {@code database.Databases} gives it its dialect. Every type is
 * listed, none with a {@code default} arm: a type no connector is built for is refused by name.
 */
public final class Connectors {

    private Connectors() {
    }

    /** {@code type}'s connector. */
    public static DatabaseConnector of(DatabaseType type) {
        return switch (type) {
            case DuckDB -> DuckDbConnector.INSTANCE;
            case H2 -> H2Connector.INSTANCE;
            case SQLite -> SqliteConnector.INSTANCE;
            case Postgres -> PostgresConnector.INSTANCE;
            case MariaDB -> MariaDbConnector.INSTANCE;
            case MySQL -> MySqlConnector.INSTANCE;
            case Snowflake -> SnowflakeConnector.INSTANCE;
            case Databricks -> DatabricksConnector.INSTANCE;
            case BigQuery -> BigQueryConnector.INSTANCE;
            case Trino -> TrinoConnector.INSTANCE;
            case ClickHouse -> ClickHouseConnector.INSTANCE;
            // built beside the JDBC drivers, not in core (docs/MONGODB_PLAN_2026_10_16.md D12): its client library is
            // no dependency of core's
            case MongoDB -> LOADED.stream().filter(c -> c.type() == type).findFirst()
                    .orElseGet(() -> new Unbuilt(type, true, "its connector rides with the drivers (core/src/main/mongodb),"
                            + " and none is on the class path"));
            case DB2, MemSQL, Sybase, SybaseIQ, Composite, SqlServer, Hive, Presto, Redshift, Spanner, Athena, Oracle,
                 Aurora -> new Unbuilt(type, false, null);
        };
    }

    /** The connectors built beside the JDBC drivers, found as they are, by {@link java.util.ServiceLoader} (the bulk
     *  loaders' way); two for one type is a packaging mistake, said at once. */
    private static final java.util.List<DatabaseConnector> LOADED = loaded();

    private static java.util.List<DatabaseConnector> loaded() {
        java.util.List<DatabaseConnector> found = java.util.ServiceLoader.load(DatabaseConnector.class).stream()
                .map(java.util.ServiceLoader.Provider::get).toList();
        java.util.Set<DatabaseType> types = java.util.EnumSet.noneOf(DatabaseType.class);
        for (DatabaseConnector c : found) {
            if (!types.add(c.type())) {
                throw new IllegalStateException("two connectors for " + c.type() + " on the class path");
            }
        }
        return found;
    }

    /** The product name {@code session}'s JDBC driver reports ({@code DatabaseMetaData.getDatabaseProductName}): the
     *  one read of which database a session is on, matched against each connector's {@link
     *  DatabaseConnector#jdbcProduct}. */
    static String product(Connection session) throws SQLException {
        return session.getMetaData().getDatabaseProductName();
    }

    /** {@code session}'s connector: the built one whose JDBC driver reports its product name, or null when none
     *  does. */
    static @com.legend.base.Nullable DatabaseConnector of(Connection session) throws SQLException {
        String product = product(session);
        for (DatabaseType t : DatabaseType.values()) {
            DatabaseConnector c = of(t);
            if (!(c instanceof Unbuilt) && !c.ownSessions() && c.jdbcProduct().equals(product)) {
                return c;
            }
        }
        return null;
    }

    /** A type no connector is built for, or none is on the class path (then {@code why} says so): its sessions refused
     *  by name (no
     *  product name, no declared connection opened, no private instance, no storeless runtime); it has no in-process
     *  instance, as a server has none. {@code ownSessions}: whether its sessions would be the connector's own, so a
     *  declared connection is refused for the missing connector, not for the credential it carries. */
    private record Unbuilt(DatabaseType type, boolean ownSessions, @com.legend.base.Nullable String why)
            implements DatabaseConnector {

        @Override
        public Sessions.Session open(ConnectionDefinition def) {
            throw unbuilt();
        }

        @Override
        public String jdbcProduct() {
            throw unbuilt();
        }

        @Override
        public Sessions.Opening opening(ConnectionDefinition def) {
            return DatabaseConnector.notOpened(def);
        }

        @Override
        public boolean inProcess() {
            return false;
        }

        @Override
        public @com.legend.base.Nullable RaiseRecovery raiseRecovery() {
            throw unbuilt();
        }

        @Override
        public Decoding decoding() {
            throw unbuilt();   // refused by name, as every fact of an unbuilt connector (the D72 audit)
        }

        private com.legend.error.NotImplementedException unbuilt() {
            return new com.legend.error.NotImplementedException(
                    "sessions on database type '" + type + "' are not implemented" + (why == null ? "" : ": " + why));
        }
    }
}
