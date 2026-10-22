// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testbackend;

import com.legend.database.Databases;
import com.legend.exec.Sessions;
import com.legend.model.ConnectionDefinition.DatabaseType;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * THE registry of the tests' databases (the dialect program's Phase 6): each declared {@link DatabaseType} to the
 * {@link TestBackend} its tests run on, as {@code exec.Connectors} gives it its connector and {@code Databases} its
 * dialect. Every type is listed, none with a {@code default} arm: a type no backend is built for is refused by name.
 */
public final class TestBackends {

    private TestBackends() {
    }

    /** {@code type}'s backend. */
    public static TestBackend of(DatabaseType type) {
        return switch (type) {
            case DuckDB -> DuckDbBackend.INSTANCE;
            case H2 -> H2Backend.INSTANCE;
            case SQLite -> SqliteBackend.INSTANCE;
            case Postgres -> PostgresBackend.INSTANCE;
            case MariaDB -> MariaDbBackend.INSTANCE;
            case MySQL -> MySqlBackend.INSTANCE;
            case Trino -> TrinoBackend.INSTANCE;
            case ClickHouse -> ClickHouseBackend.INSTANCE;
            case Snowflake -> SnowflakeBackend.INSTANCE;
            case Databricks -> DatabricksBackend.INSTANCE;
            case BigQuery -> BigQueryBackend.INSTANCE;
            case DB2, MemSQL, Sybase, SybaseIQ, Composite, SqlServer, Hive, Presto, Redshift, Spanner, Athena, Oracle,
                 Aurora, Deephaven -> throw new com.legend.error.NotImplementedException(
                         "no test backend for database type '" + type + "': the platform does not execute on it");
        };
    }

    /** The platform's own engine ({@code Databases.PLATFORM}): where a test that is about no database runs. */
    public static TestBackend platform() {
        return of(Databases.PLATFORM);
    }

    /** The database the SQL-text replay referee runs on ({@code Databases.REPLAY_REFEREE}): a backend with the
     *  referee's own duties. */
    public static RefereeBackend referee() {
        if (of(Databases.REPLAY_REFEREE) instanceof RefereeBackend referee) {
            return referee;
        }
        throw new IllegalStateException("the replay referee's database " + Databases.REPLAY_REFEREE
                + " has no referee backend");
    }

    /** The backend {@code session} is on: the database whose connector names the product its driver reports
     *  ({@code exec.Sessions.jdbcProduct}). A session on no database the platform executes on is refused, as an
     *  {@link SQLException} its reader declines by. */
    public static TestBackend of(Connection session) throws SQLException {
        String product = session.getMetaData().getDatabaseProductName();
        for (DatabaseType type : Databases.implemented()) {
            if (Sessions.jdbcProduct(type).equals(product)) {
                return of(type);
            }
        }
        throw new SQLException("no test backend's database reports the product '" + product + "'");
    }

    /** Every database the platform executes on ({@code Databases.implemented()}), each one's backend. */
    public static List<TestBackend> all() {
        return Databases.implemented().stream().map(TestBackends::of).toList();
    }

    /** Every database the platform runs in-process, each one's backend: where a test that runs on each database
     *  without a server runs. */
    public static List<TestBackend> inProcess() {
        return all().stream().filter(TestBackend::inProcess).toList();
    }

    /** The backend a lane names ({@link TestBackend#name}); a name no backend has is refused, listing those there
     *  are. */
    public static TestBackend named(String name) {
        for (TestBackend b : lanes()) {
            if (b.name().equals(name)) {
                return b;
            }
        }
        throw new IllegalStateException("no test backend is named '" + name + "': the backends are "
                + lanes().stream().map(TestBackend::name).collect(Collectors.joining(", ")));
    }

    /** Every backend a lane may name: each database's, and the warehouse's (DuckDB as the warehouse serves it, no
     *  database type of its own). */
    private static List<TestBackend> lanes() {
        List<TestBackend> lanes = new java.util.ArrayList<>(all());
        lanes.add(DuckDbWarehouseBackend.INSTANCE);
        return lanes;
    }
}
