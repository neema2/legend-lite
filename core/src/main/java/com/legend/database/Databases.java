// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.database;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.dialect.SqlDialect;

/**
 * THE plan-side owner of every per-database decision (docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md
 * C3): given a DECLARED {@link DatabaseType}, the SQL this platform writes for it — the execution dialect and its
 * plan-protocol facts. legend-engine's golden TEXT for a type is not here: its renderers are quarantined to the root
 * layer (AGENTS invariant 4d), so their one owner is {@code com.legend.EngineText}. Upstream's own split: SQL generation per database type
 * ({@code dbExtension.pure}, {@code loadDbExtension}) apart from connection management (the execution side,
 * {@code com.legend.exec.Sessions}). Every type is listed in every switch, none has a {@code default} arm, and a
 * capability a database lacks is refused by name. Nothing here touches JDBC: the browser planner uses it.
 */
public final class Databases {

    private Databases() {
    }

    /** The database the platform runs a query on when its runtime binds no database at all — only model data
     *  (a {@code ModelStore}'s JSON, instances): its own in-process engine (SEMANTICS_REGISTER S27, decision D3). */
    public static final DatabaseType PLATFORM = DatabaseType.DuckDB;

    /** The database the SQL-text replay oracle runs on: its ledger records DDL in this database's spelling. */
    public static final DatabaseType REPLAY_ORACLE = DatabaseType.H2;

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

    /** The dialect a query on {@code type} is planned and executed with. */
    public static SqlDialect dialect(DatabaseType type) {
        return switch (type) {
            case DuckDB -> new com.legend.sql.dialect.DuckDb();
            case H2 -> new com.legend.sql.dialect.H2();
            case Postgres -> new com.legend.sql.dialect.Postgres();
            // SQLite differs from the ANSI baseline ONLY lexically — a Lexicon row, not a dialect subclass
            // (remediation T3.2)
            case SQLite -> new com.legend.sql.dialect.AnsiSqlRenderer("SQLite", com.legend.sql.dialect.Lexicon.SQLITE,
                    com.legend.sql.dialect.TypeNames.ANSI, com.legend.sql.dialect.Spellings.DUCKDB);
            case DB2, MemSQL, Sybase, SybaseIQ, Composite, SqlServer, Hive, Snowflake, Presto, Trino, BigQuery,
                 Redshift, Databricks, Spanner, Athena, Oracle, ClickHouse, Aurora ->
                    throw new com.legend.error.NotImplementedException(
                            "SQL dialect for database type '" + type + "' is not implemented yet");
        };
    }

    /** legend-engine's IN-list-to-temp-table facts for {@code type}: the temp table's name prefix and the list size past
     *  which the engine uses one (null: never, for this database). */
    public record InListTempTables(String tablePrefix, @com.legend.base.Nullable Integer threshold) {
    }

    public static InListTempTables inListTempTables(DatabaseType type) {
        return switch (type) {
            case DB2 -> new InListTempTables("SESSION.tempTableForIn_", 32767);
            case H2, DuckDB, Postgres, SQLite, MemSQL, Sybase, SybaseIQ, Composite, SqlServer, Hive, Snowflake, Presto,
                 Trino, BigQuery, Redshift, Databricks, Spanner, Athena, Oracle, ClickHouse, Aurora ->
                    new InListTempTables("tempTableForIn_", null);
        };
    }
}
