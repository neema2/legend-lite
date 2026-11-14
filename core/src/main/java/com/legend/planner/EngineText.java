// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.model.ConnectionDefinition.DatabaseType;

/**
 * legend-engine's text FACTS the planner reads per declared {@link DatabaseType}: the database its test connections
 * declare, and its IN-list temp tables. The renderers that WRITE the engine's text are the compatibility edge's
 * ({@code com.legend.edge.PureText.engineText}/{@code enginePlanText}, reached through the planning's
 * {@link com.legend.database.PlanStyle}: phase D, 2026-11-12, D1.7's move pulled forward). Every type is listed in
 * every switch; none has a {@code default} arm.
 */
public final class EngineText {

    private EngineText() {
    }

    /** The database legend-engine's test connections declare, so the database its golden TEXT is written for
     *  (plan text, an activity's SQL, setup DDL: "goldens are engine-H2-spelled"). Where the connection behind a text
     *  cannot be read, the text is this database's — the reader gap docs/…_2026_10_03.md §3.2 hands to the rebuild
     *  (W2.1), which reads the real type instead. */
    public static final DatabaseType ENGINE_TEST_DATABASE = DatabaseType.H2;

    /** legend-engine's IN-list-to-temp-table facts for a database: the temp table's name prefix and the list size past
     *  which the engine uses one (null: never, for this database). */
    record InListTempTables(String tablePrefix, @com.legend.base.Nullable Integer threshold) {
    }

    /** legend-engine's IN-list-to-temp-table facts for {@code type} (the dialect program's Phase 5, 2026-10-16: moved
     *  here from {@code database.Databases}, which writes none of the engine's text; they are per declared type, not
     *  per engine-text dialect, as Composite's plan text is DB2's and its IN lists are not). */
    public static InListTempTables inListTempTables(DatabaseType type) {
        return switch (type) {
            case DB2 -> new InListTempTables("SESSION.tempTableForIn_", 32767);
            case H2, DuckDB, Postgres, SQLite, MemSQL, Sybase, SybaseIQ, Composite, SqlServer, Hive, Snowflake, Presto,
                 Trino, BigQuery, Redshift, Databricks, Spanner, Athena, Oracle, ClickHouse, Aurora, MariaDB, MySQL,
                 MongoDB ->
                    new InListTempTables("tempTableForIn_", null);
        };
    }
}
