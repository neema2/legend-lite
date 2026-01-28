package org.finos.legend.pure.dsl.definition;

/**
 * Sealed interface for connection specifications.
 * Defines how to connect to a specific database instance.
 */
public sealed interface ConnectionSpecification
        permits ConnectionSpecification.InMemory,
        ConnectionSpecification.LocalFile,
        ConnectionSpecification.StaticDatasource {

    /**
     * In-memory database connection (DuckDB, SQLite, H2).
     * No additional configuration needed.
     */
    record InMemory() implements ConnectionSpecification {
    }

    /**
     * Local file-based database connection.
     * Used for DuckDB with a file path.
     */
    record LocalFile(String path) implements ConnectionSpecification {
    }

    /**
     * Static datasource with explicit host/port/database.
     * Used for connecting to remote database servers.
     */
    record StaticDatasource(
            String host,
            int port,
            String database) implements ConnectionSpecification {
    }
}
