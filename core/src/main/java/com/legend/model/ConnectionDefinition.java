package com.legend.model;

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
        AuthenticationSpec authentication) implements PackageableElement {

    public ConnectionDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        Objects.requireNonNull(databaseType, "Database type cannot be null");
        Objects.requireNonNull(specification, "Specification cannot be null");
        Objects.requireNonNull(authentication, "Authentication cannot be null");
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
        SQLite
    }
}
