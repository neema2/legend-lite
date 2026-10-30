// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.database;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.dialect.SqlDialect;

/**
 * THE plan-side owner of every per-database decision (docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md
 * C3): given a DECLARED {@link DatabaseType}, the dialect this platform writes its SQL in, which answers every plan
 * fact of its database itself (its server versions, how its rows load: the dialect program's Phase 5, 2026-10-16,
 * when this class shrank to type to dialect). legend-engine's golden TEXT for a type is not here: its renderers are
 * quarantined to the root layer (AGENTS invariant 4d), so their one owner is {@code com.legend.driver.EngineText}.
 * Upstream's own split: SQL generation per database type ({@code dbExtension.pure}, {@code loadDbExtension}) apart
 * from connection management (the execution side, {@code com.legend.exec.Connectors}). Every type is listed in the
 * switch, none has a {@code default} arm, and a database with no dialect is refused by name. Nothing here touches
 * JDBC: the browser planner uses it.
 */
public final class Databases {

    private Databases() {
    }

    /** The database the platform runs a query on when its runtime binds no database at all — only model data
     *  (a {@code ModelStore}'s JSON, instances): its own in-process engine (SEMANTICS_REGISTER S27, decision D3). */
    public static final DatabaseType PLATFORM = DatabaseType.DuckDB;

    /** The database the SQL-text replay referee runs on: its ledger records DDL in this database's spelling. */
    public static final DatabaseType REPLAY_REFEREE = DatabaseType.H2;

    /** A {@code DatabaseType} by its Pure enum name ({@code meta::relational::runtime::DatabaseType.DB2} → DB2).
     *  The Pure enum also has {@code SparkSQL} and {@code DebugPrint}, which this platform does not model: refused. */
    public static DatabaseType named(String name) {
        for (DatabaseType t : DatabaseType.values()) {
            if (t.name().equals(name)) {
                return t;
            }
        }
        throw new com.legend.error.NotImplementedException("database type '" + name
                + "' is not one this platform models " + java.util.Arrays.toString(DatabaseType.values()));
    }

    /** Every database the platform executes on: those {@link #dialect} gives a dialect, in the declared type's order
     *  (the dialect program's Phase 6, 2026-10-16: tests and tools iterate it instead of listing databases by hand). */
    public static java.util.List<DatabaseType> implemented() {
        java.util.List<DatabaseType> out = new java.util.ArrayList<>();
        for (DatabaseType type : DatabaseType.values()) {
            if (executes(type)) {
                out.add(type);
            }
        }
        return java.util.Collections.unmodifiableList(out);
    }

    /** Every database a {@code RelationalDatabaseConnection} declares: those {@link #implemented} whose dialect is
     *  relational ({@link SqlDialect#relational}: all but MongoDB's, 2026-10-22), in the declared type's order. */
    public static java.util.List<DatabaseType> relational() {
        return implemented().stream().filter(t -> dialect(t).relational()).toList();
    }

    /** Whether the platform executes on {@code type}: {@link #dialect} gives it one (a test holds the two together). */
    private static boolean executes(DatabaseType type) {
        return switch (type) {
            case DuckDB, H2, Postgres, Databricks, Snowflake, BigQuery, SQLite, MariaDB, MySQL, Trino,
                 ClickHouse, MongoDB -> true;
            case DB2, MemSQL, Sybase, SybaseIQ, Composite, SqlServer, Hive, Presto, Redshift, Spanner, Athena, Oracle,
                 Aurora -> false;
        };
    }

    /** The dialect a query on {@code type} is planned and executed with. */
    public static SqlDialect dialect(DatabaseType type) {
        return switch (type) {
            case DuckDB -> new com.legend.sql.dialect.DuckDb();
            case H2 -> new com.legend.sql.dialect.H2();
            case Postgres -> new com.legend.sql.dialect.Postgres();
            case Databricks -> new com.legend.sql.dialect.Databricks();
            case Snowflake -> new com.legend.sql.dialect.Snowflake();
            case BigQuery -> new com.legend.sql.dialect.BigQuery();
            // a dialect of its own since 2026-10-11 (docs/CLOUD_BACKENDS_PCT_2026_10_11.md §12): T3.2 made it a
            // Lexicon row when it differed only lexically; its PCT lane measured otherwise
            case SQLite -> new com.legend.sql.dialect.SQLite();
            // docs/CLOUD_BACKENDS_PCT_2026_10_11.md §13
            case MariaDB -> new com.legend.sql.dialect.MariaDb();
            // docs/CLOUD_BACKENDS_PCT_2026_10_11.md §14: MariaDB's dialect where the two agree
            case MySQL -> new com.legend.sql.dialect.MySql();
            // docs/CLOUD_BACKENDS_PCT_2026_10_11.md §18
            case Trino -> new com.legend.sql.dialect.Trino();
            // docs/CLOUD_BACKENDS_PCT_2026_10_11.md §19
            case ClickHouse -> new com.legend.sql.dialect.ClickHouse();
            // docs/MONGODB_PLAN_2026_10_16.md (D1, D2): the aggregation pipeline as the one compiler's target
            case MongoDB -> new com.legend.sql.dialect.MongoDb();
            case DB2, MemSQL, Sybase, SybaseIQ, Composite, SqlServer, Hive, Presto,
                 Redshift, Spanner, Athena, Oracle, Aurora ->
                    throw new com.legend.error.NotImplementedException(
                            "SQL dialect for database type '" + type + "' is not implemented yet");
        };
    }

    /** A server version's {@code major.minor} ({@code 2.4.240} is {@code 2.4}): the prefix a plan's versions name and
     *  its runner checks the session's version against. A version with no dot is itself. */
    public static String majorMinor(String version) {
        int first = version.indexOf('.');
        int second = first < 0 ? -1 : version.indexOf('.', first + 1);
        return second < 0 ? version : version.substring(0, second);
    }
}
