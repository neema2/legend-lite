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
            case DB2, MemSQL, Sybase, SybaseIQ, Composite, SqlServer, Hive, Presto, Redshift, Spanner, Athena, Oracle,
                 Aurora, Deephaven -> new Unbuilt(type);
        };
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
            if (!(c instanceof Unbuilt) && c.jdbcProduct().equals(product)) {
                return c;
            }
        }
        return null;
    }

    /** A type no connector is built for: its sessions refused by name (no product name, no declared connection
     *  opened, no private instance, no storeless runtime); it has no in-process instance, as a server has none. */
    private record Unbuilt(DatabaseType type) implements DatabaseConnector {

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
        public @com.legend.base.Nullable String raisedMessage(Connection session) {
            throw unbuilt();
        }

        private com.legend.error.NotImplementedException unbuilt() {
            return new com.legend.error.NotImplementedException(
                    "sessions on database type '" + type + "' are not implemented");
        }
    }
}
