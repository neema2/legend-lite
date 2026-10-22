package com.legend.model;

/**
 * How to connect to a specific database instance. Sealed root for the three
 * connection-specification flavors recognized inside a
 * {@code RelationalDatabaseConnection}'s {@code specification:} key.
 *
 * <p>Mirrors engine's {@code com.gs.legend.model.def.ConnectionSpecification}.
 */
public sealed interface ConnectionSpecification
        permits ConnectionSpecification.InMemory,
                ConnectionSpecification.LocalFile,
                ConnectionSpecification.LocalH2,
                ConnectionSpecification.EmbeddedH2,
                ConnectionSpecification.StaticDatasource,
                ConnectionSpecification.Trino,
                ConnectionSpecification.Snowflake,
                ConnectionSpecification.Spanner,
                ConnectionSpecification.Databricks,
                ConnectionSpecification.BigQuery,
                ConnectionSpecification.Deephaven {

    /** The Pure class of the datasource specification that carries a connection's test data ({@link #testData}) where a
     *  Pure body builds the connection ({@code ^RelationalDatabaseConnection(datasourceSpecification = ^...)}):
     *  {@link LocalH2}'s. */
    String TEST_DATA_SPECIFICATION_CLASS =
            "meta::pure::alloy::connections::alloy::specification::LocalH2DatasourceSpecification";

    /** Whether {@code nameOrFqn} (as a Pure body spells a class: bare or in full) is {@link
     *  #TEST_DATA_SPECIFICATION_CLASS}. */
    static boolean isTestDataSpecificationClass(String nameOrFqn) {
        return TEST_DATA_SPECIFICATION_CLASS.equals(nameOrFqn) || "LocalH2DatasourceSpecification".equals(nameOrFqn);
    }

    /** The test data the specification carries for its connection's database (the engine's {@code testDataSetupCsv}
     *  and {@code testDataSetupSqls}), or null for a specification that carries none: only {@link LocalH2} does. */
    default @com.legend.base.Nullable TestData testData() {
        return null;
    }

    /** A specification's test data: CSV blocks to seed, and SQL statements written in the database its connection
     *  declares; either may be absent. */
    record TestData(@com.legend.base.Nullable String csv, java.util.@com.legend.base.Nullable List<String> sqls) {
    }

    /** A specification that carries {@code csv} as its connection's test data (a service test's runtime): the
     *  vocabulary's one such, {@link LocalH2}'s. A connection opened through {@code exec.Sessions} with it opens on
     *  H2 only; a runtime overlay that pairs it with another type opens its own session, as ServiceTestRunner does,
     *  and the specification only carries the rows. */
    static ConnectionSpecification carryingTestData(String csv) {
        return new LocalH2(null, csv, null);
    }

    /**
     * In-memory database connection (DuckDB / SQLite / H2). No configuration.
     * Pure: {@code specification: InMemory {};}.
     */
    record InMemory() implements ConnectionSpecification {}

    /**
     * Local file-based database connection (e.g. DuckDB pointed at a file path).
     * Pure: {@code specification: LocalFile { path: '/tmp/db.duckdb'; };}.
     */
    record LocalFile(String path) implements ConnectionSpecification {}

    /** Local H2-style test datasource ({@code LocalH2 {}} or
     *  {@code LocalH2 { url: '...' }}). The engine's
     *  LocalH2DatasourceSpecification carries NO url at all (it
     *  synthesizes an in-memory database) — a bare {@code LocalH2 {}}
     *  is the common grammar form; an explicit url is a legend-lite
     *  extension carried verbatim when present. Engine's
     *  {@code testDataSetupCSV} / {@code testDataSetupSqls} keys are
     *  carried as data for the test-seeding path. */
    record LocalH2(@com.legend.base.Nullable String url,
            @com.legend.base.Nullable String testDataSetupCsv,
            java.util.@com.legend.base.Nullable List<String> testDataSetupSqls)
            implements ConnectionSpecification {
        public LocalH2(@com.legend.base.Nullable String url) {
            this(url, null, null);
        }

        @Override
        public TestData testData() {
            return new TestData(testDataSetupCsv, testDataSetupSqls);
        }
    }

    /** Engine's EmbeddedH2DatasourceSpecification — carried WHOLE
     * (A19: the old fold to {@code LocalH2(null,null,null)} discarded
     * databaseName/directory/autoServerMode, and every such connection
     * then shared one fixed {@code jdbc:h2:mem:testdb} instance).
     * Lite execution semantics: a DISTINCT in-memory database per
     * {@code databaseName} — the engine's directory-backed isolation
     * without test-run disk side effects (deliberate divergence,
     * spelled at the resolver). */
    record EmbeddedH2(String databaseName, String directory,
            boolean autoServerMode) implements ConnectionSpecification {
    }

    /**
     * Static datasource with explicit host/port/database. Used for remote servers.
     * Pure: {@code specification: Static { host: '...'; port: 5432; database: '...'; };}.
     */
    record StaticDatasource(String host, int port, String database) implements ConnectionSpecification {}

    /**
     * Engine's Trino datasource: a coordinator at host/port, its session's catalog and schema (legend-engine's
     * {@code TrinoManager.buildURL}: {@code jdbc:trino://host:port/catalog/schema}), and whether its clients connect
     * over TLS and tag their queries. Its own shape, not folded into {@link StaticDatasource}: the schema is part of
     * the session (step 9, 2026-10-19: the plan route opens it).
     */
    record Trino(String host, int port, @com.legend.base.Nullable String catalog,
            @com.legend.base.Nullable String schema, @com.legend.base.Nullable String clientTags, boolean ssl)
            implements ConnectionSpecification {}

    /** Engine's Snowflake datasource; carried for parse coverage — dialect
     *  selection refuses Snowflake execution loudly. */
    record Snowflake(String databaseName, String accountName, String warehouseName,
            String region, @com.legend.base.Nullable String accountType,
            @com.legend.base.Nullable String cloudType,
            @com.legend.base.Nullable Boolean enableQueryTags,
            @com.legend.base.Nullable String organization,
            @com.legend.base.Nullable String role)
            implements ConnectionSpecification {}

    /** Engine's GCP Spanner datasource; carried for parse coverage. */
    record Spanner(String projectId, String instanceId, String databaseId)
            implements ConnectionSpecification {}

    /** Engine's Databricks datasource; carried for parse coverage. */
    record Databricks(String hostname, String port, String protocol,
            String httpPath) implements ConnectionSpecification {}

    /** Engine's BigQuery datasource; carried for parse coverage. */
    record BigQuery(String projectId, String defaultDataset)
            implements ConnectionSpecification {}

    /** A Deephaven server (lite's own, docs/DEEPHAVEN_PLAN_2026_10_22.md §4): its host, its gRPC port, and whether the
     *  channel is TLS. Every field is written; none defaults (AGENTS invariant 4). */
    record Deephaven(String host, int port, boolean useTls) implements ConnectionSpecification {}
}
