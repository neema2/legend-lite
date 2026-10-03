// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.dialect.EngineStyleH2;

/**
 * THE owner of which legend-engine golden-text renderer a declared {@link DatabaseType} gets
 * (docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md C3a). It sits in the root layer, beside its only
 * consumers, because the engine-style renderers are quarantined here (AGENTS invariant 4d,
 * {@code ArchitectureTest.engineStyleRendererIsQuarantinedToTheRootLayer}): an execution path that reached one would
 * run engine-H2 TEXT semantics against a real session. Every type is listed in every switch; none has a
 * {@code default} arm.
 */
final class EngineText {

    private EngineText() {
    }

    /** The database legend-engine's test connections declare, so the database its golden TEXT is written for
     *  (plan text, an activity's SQL, setup DDL: "goldens are engine-H2-spelled"). Where the connection behind a text
     *  cannot be read, the text is this database's — the reader gap docs/…_2026_10_03.md §3.2 hands to the rebuild
     *  (W2.1), which reads the real type instead. */
    static final DatabaseType ENGINE_TEST_DATABASE = DatabaseType.H2;

    /** legend-engine's exact SQL text for {@code type}, as {@code toSQLString} prints it: Composite takes the engine's
     *  DEFAULT spellings (native trim/pad/cbrt, plain char_length). */
    static EngineStyleH2 engineText(DatabaseType type) {
        return switch (type) {
            case H2 -> new EngineStyleH2();
            case DB2 -> new com.legend.sql.dialect.EngineStyleDB2();
            case Composite -> new com.legend.sql.dialect.EngineStyleComposite();
            case DuckDB, Postgres, SQLite, MemSQL, Sybase, SybaseIQ, SqlServer, Hive, Snowflake, Presto, Trino, BigQuery,
                 Redshift, Databricks, Spanner, Athena, Oracle, ClickHouse, Aurora ->
                    throw new com.legend.error.NotImplementedException("toSQLString for DatabaseType." + type
                            + " — only the H2, DB2 and Composite engine-style renderers are built");
        };
    }

    /** legend-engine's exact PLAN text for {@code type}: the plan goldens pin Composite to the DB2-family spelling
     *  (paren-wrapped conjunctions, quoted boolean placeholders) — unlike {@link #engineText}. */
    static EngineStyleH2 enginePlanText(DatabaseType type, boolean quoteIdentifiers,
            @com.legend.base.Nullable String timeZone) {
        return switch (type) {
            case H2 -> new EngineStyleH2(quoteIdentifiers, timeZone);
            case DB2, Composite -> new com.legend.sql.dialect.EngineStyleDB2(quoteIdentifiers, timeZone);
            case DuckDB, Postgres, SQLite, MemSQL, Sybase, SybaseIQ, SqlServer, Hive, Snowflake, Presto, Trino, BigQuery,
                 Redshift, Databricks, Spanner, Athena, Oracle, ClickHouse, Aurora ->
                    throw new com.legend.error.NotImplementedException("plan text for DatabaseType." + type
                            + " — only the H2 and DB2-family engine-style renderers are built");
        };
    }
}
