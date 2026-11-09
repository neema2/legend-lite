// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.executionplan;

/**
 * The database a connection declares, as the plan names it: the engine's DatabaseType enum verbatim (protocol
 * connection/DatabaseType.java, 4.138.2) plus lite's own flavours (SQLite, MariaDB, MySQL, MongoDB).
 *
 * <p>THE PLAN'S OWN COPY (docs/PLAN_AS_DATA_2026_11_07.md 2.6, the design's step 7, 2026-11-08: the plan holds its own
 * connection description, FAITHFUL -- the same variants under the same names with the same fields as the model's
 * {@code com.legend.model.ConnectionDefinition.DatabaseType}, which the planner copies field by field: {@code planner/ConnectionPlans}). Exec opens
 * a session from this copy alone; the model keeps its own (invariant 6j: the model depends on no plan).
 */
public enum DatabaseType {
    DB2,
    H2,
    MemSQL,
    Sybase,
    SybaseIQ,
    Composite,
    Postgres,
    SqlServer,
    Hive,
    Snowflake,
    Presto,
    Trino,
    BigQuery,
    Redshift,
    Databricks,
    Spanner,
    Athena,
    DuckDB,
    Oracle,
    ClickHouse,
    Aurora,
    SQLite,
    // not in legend-engine's DatabaseType (2026-10-11, docs/CLOUD_BACKENDS_PCT_2026_10_11.md §13; as SQLite before
    // them): the open databases the user asked for, each a dialect of its own
    MariaDB,
    MySQL,
    // 2026-10-16 (docs/MONGODB_PLAN_2026_10_16.md, D4): lite's own, as SQLite, MariaDB and MySQL are -- the
    // database a MongoDBConnection (legend-engine's non-relational store connection) declares, never
    // writable in a RelationalDatabaseConnection (the grammar's lite types do not list it)
    MongoDB
}
