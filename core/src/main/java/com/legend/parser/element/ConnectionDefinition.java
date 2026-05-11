package com.legend.parser.element;

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
        String storeName,
        DatabaseType databaseType,
        ConnectionSpecification specification,
        AuthenticationSpec authentication) implements PackageableElement {

    public ConnectionDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        Objects.requireNonNull(databaseType, "Database type cannot be null");
        Objects.requireNonNull(specification, "Specification cannot be null");
        Objects.requireNonNull(authentication, "Authentication cannot be null");
    }

    /** Supported relational dialects. Drives downstream SQL dialect selection. */
    public enum DatabaseType {
        DuckDB,
        SQLite,
        H2,
        Postgres,
        Snowflake,
        BigQuery
    }
}
