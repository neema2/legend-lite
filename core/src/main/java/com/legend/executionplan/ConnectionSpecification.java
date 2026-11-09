package com.legend.executionplan;

/**
 * How to connect to a specific database instance. Sealed root for the three
 * connection-specification flavors recognized inside a
 * {@code RelationalDatabaseConnection}'s {@code specification:} key.
 *
 * <p>Mirrors engine's {@code com.gs.legend.model.def.ConnectionSpecification}.
*
 * <p>THE PLAN'S OWN COPY (docs/PLAN_AS_DATA_2026_11_07.md 2.6, the design's step 7, 2026-11-08: the plan holds its own
 * connection description, FAITHFUL -- the same variants under the same names with the same fields as the model's
 * {@code com.legend.model.ConnectionSpecification}, which the planner copies field by field: {@code planner/ConnectionPlans};
 * but a connection's declared test data, which the plan carries as its target's loads, L2). Exec opens
 * a session from this copy alone; the model keeps its own (invariant 6j: the model depends on no plan).
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
                ConnectionSpecification.BigQuery {

    /** Whether the platform MAKES the database a connection of {@code spec} names (an in-memory database: an
     *  {@code InMemory} one, or legend-engine's {@code LocalH2}), as opposed to one the user names or a server holds.
     *  A project's Data rows load only into a database the platform makes (the M1 tenets audit's finding 1,
     *  2026-11-09): the planner plans the fill only there, and exec refuses a fill for any other. The one rule both
     *  read; {@code exec.ConnectorsTest} holds it equal to each connector's opening (held, or named by the opener). */
    static boolean platformMakes(ConnectionSpecification spec) {
        return spec instanceof InMemory || spec instanceof LocalH2;
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
     *  extension carried verbatim when present. The model's
     *  {@code testDataSetupCSV} / {@code testDataSetupSqls} are NOT
     *  copied: the plan carries a connection's test data as its target's
     *  loads (docs/DATA_IN_THE_PLAN_2026_11_09.md L2, M3, 2026-11-09: the
     *  same data carried twice in one plan until then). */
    record LocalH2(@com.legend.base.Nullable String url) implements ConnectionSpecification {
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
}
