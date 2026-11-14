package com.legend.model;

import java.util.List;
import java.util.Objects;

/**
 * A parsed Pure {@code RelationalDatabaseConnection} declaration &mdash; binds
 * a relational store to a database type, a connection specification, and an
 * authentication scheme.
 *
 * <p>Pure syntax:
 * <pre>
 *   RelationalDatabaseConnection store::InMemoryDuckDb
 *   {
 *     store:          store::PersonDb;
 *     type:           DuckDB;
 *     specification:  InMemory {};
 *     auth:           NoAuth {};
 *   }
 * </pre>
 *
 * <p>Mirrors engine's {@code com.gs.legend.model.def.ConnectionDefinition},
 * minus {@code simpleName()} / {@code packagePath()} (see
 * {@link PackageableElement}).
 *
 * @param qualifiedName  fully qualified connection name
 * @param storeName      qualified name of the {@code Database} this connection serves;
 *                       {@code null} only if absent in source (parser will still capture)
 * @param databaseType   one of the supported relational dialects
 * @param specification  how to reach the database instance
 * @param authentication how to authenticate
 */
public record ConnectionDefinition(
        String qualifiedName,
        @com.legend.base.Nullable String storeName,
        DatabaseType databaseType,
        ConnectionSpecification specification,
        AuthenticationSpec authentication,
        List<String> rowMappers) implements PackageableElement {

    public ConnectionDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        Objects.requireNonNull(databaseType, "Database type cannot be null");
        Objects.requireNonNull(specification, "Specification cannot be null");
        Objects.requireNonNull(authentication, "Authentication cannot be null");
        rowMappers = List.copyOf(rowMappers);
    }

    /** A connection that declares no post-processor that maps its tables (the common form). */
    public ConnectionDefinition(String qualifiedName, @com.legend.base.Nullable String storeName,
            DatabaseType databaseType, ConnectionSpecification specification, AuthenticationSpec authentication) {
        this(qualifiedName, storeName, databaseType, specification, authentication, List.of());
    }

    /** Supported relational dialects. Drives downstream SQL dialect selection.
     *  Corpus-censused types beyond the executable set (Spanner, MemSQL,
     *  Databricks) parse and carry; dialect selection refuses them loudly at
     *  execution time ({@code Compiler.dialectOf}). */
    /** The ENGINE's DatabaseType enum verbatim (protocol
     *  connection/DatabaseType.java, 4.138.2) plus lite's SQLite
     *  extension flavor — the MODEL accepts what the engine parses;
     *  whether a backend can EXECUTE a type is the lowering's concern. */
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
        MongoDB,
        // not in legend-engine's DatabaseType either (2026-10-22, docs/DEEPHAVEN_PLAN_2026_10_22.md): a Deephaven
        // server, whose tables an ordinary ###Relational store declares; its own specification and authentication
        Deephaven,
        // lite's own as Deephaven is (2026-11-14, docs/DEEPHAVEN_TABLES_AND_SOURCES_2026_10_29.md §4.6, the desk
        // roadmap's B2): a topic store's brokers, whose topics an ordinary ###Relational store declares as tables; no
        // query runs on it -- a stream's ingest reads its topics
        Kafka
    }
}
