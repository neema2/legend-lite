package org.finos.legend.engine.execution;

import org.finos.legend.pure.dsl.PureCompiler;
import org.finos.legend.pure.dsl.definition.ConnectionDefinition;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;
import org.finos.legend.pure.dsl.definition.RuntimeDefinition;
import org.finos.legend.engine.plan.RelationNode;
import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLDialect;
import org.finos.legend.engine.transpiler.SQLGenerator;
import org.finos.legend.engine.transpiler.SQLiteDialect;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * Executes Pure queries using a Runtime definition.
 * 
 * The executor:
 * 1. Compiles the Pure query to a RelationNode (logical plan)
 * 2. Generates SQL from the plan using the appropriate dialect
 * 3. Resolves the connection from the Runtime
 * 4. Executes the SQL and returns results
 */
public class QueryExecutor {

    private final PureModelBuilder modelBuilder;
    private final Map<String, ConnectionDefinition> connections;
    private final Map<String, RuntimeDefinition> runtimes;
    private final ConnectionResolver connectionResolver;

    public QueryExecutor(
            PureModelBuilder modelBuilder,
            Map<String, ConnectionDefinition> connections,
            Map<String, RuntimeDefinition> runtimes) {
        this.modelBuilder = modelBuilder;
        this.connections = connections;
        this.runtimes = runtimes;
        this.connectionResolver = new ConnectionResolver();
    }

    /**
     * Executes a Pure query using the specified runtime.
     * 
     * @param pureQuery   The Pure query to execute
     * @param runtimeName The name of the runtime to use
     * @return The ResultSet from execution
     * @throws SQLException If execution fails
     */
    public ResultSet execute(String pureQuery, String runtimeName) throws SQLException {
        RuntimeDefinition runtime = runtimes.get(runtimeName);
        if (runtime == null) {
            throw new IllegalArgumentException("Runtime not found: " + runtimeName);
        }

        return execute(pureQuery, runtime);
    }

    /**
     * Executes a Pure query using the specified runtime definition.
     */
    public ResultSet execute(String pureQuery, RuntimeDefinition runtime) throws SQLException {
        // Get connection definition to determine dialect
        ConnectionDefinition connectionDef = getConnectionDef(runtime);

        // 1. Compile the Pure query to a logical plan
        PureCompiler compiler = new PureCompiler(modelBuilder.getMappingRegistry());
        RelationNode plan = compiler.compile(pureQuery);

        // 2. Generate SQL from the plan using appropriate dialect
        SQLDialect dialect = getDialect(connectionDef);
        SQLGenerator sqlGenerator = new SQLGenerator(dialect);
        String sql = sqlGenerator.generate(plan);

        // 3. Resolve connection from the runtime
        Connection conn = connectionResolver.resolve(connectionDef);

        // 4. Execute and return
        Statement stmt = conn.createStatement();
        return stmt.executeQuery(sql);
    }

    /**
     * Executes a Pure query and returns typed record results.
     * 
     * Uses Java Record metadata to hydrate typed instances from ResultSet.
     * GraalVM native-image compatible.
     * 
     * @param pureQuery   The Pure query to execute
     * @param runtimeName The name of the runtime to use
     * @param recordType  The record class to hydrate results into
     * @return List of typed record instances
     * @throws SQLException If execution fails
     */
    public <T extends Record> List<T> executeTyped(
            String pureQuery, String runtimeName, Class<T> recordType) throws SQLException {
        RuntimeDefinition runtime = runtimes.get(runtimeName);
        if (runtime == null) {
            throw new IllegalArgumentException("Runtime not found: " + runtimeName);
        }
        return executeTyped(pureQuery, runtime, recordType);
    }

    /**
     * Executes a Pure query and returns typed record results.
     */
    public <T extends Record> List<T> executeTyped(
            String pureQuery, RuntimeDefinition runtime, Class<T> recordType) throws SQLException {
        try (ResultSet rs = execute(pureQuery, runtime)) {
            RecordResultBuilder<T> builder = new RecordResultBuilder<>(recordType);
            return builder.buildAll(rs);
        }
    }

    /**
     * Executes a Pure relation query and returns tabular results.
     * 
     * @param pureQuery   The Pure query to execute
     * @param runtimeName The name of the runtime to use
     * @return RelationResult containing column metadata and rows
     * @throws SQLException If execution fails
     */
    public RelationResult executeRelation(String pureQuery, String runtimeName) throws SQLException {
        RuntimeDefinition runtime = runtimes.get(runtimeName);
        if (runtime == null) {
            throw new IllegalArgumentException("Runtime not found: " + runtimeName);
        }
        return executeRelation(pureQuery, runtime);
    }

    /**
     * Executes a Pure relation query and returns tabular results.
     */
    public RelationResult executeRelation(String pureQuery, RuntimeDefinition runtime) throws SQLException {
        try (ResultSet rs = execute(pureQuery, runtime)) {
            return RelationResult.fromResultSet(rs);
        }
    }

    /**
     * Executes a Pure query and returns the generated SQL without executing.
     * Uses DuckDB dialect by default.
     */
    public String generateSQL(String pureQuery) {
        return generateSQL(pureQuery, DuckDBDialect.INSTANCE);
    }

    /**
     * Executes a Pure query and returns the generated SQL for the specified
     * dialect.
     */
    public String generateSQL(String pureQuery, SQLDialect dialect) {
        PureCompiler compiler = new PureCompiler(modelBuilder.getMappingRegistry());
        RelationNode plan = compiler.compile(pureQuery);
        return new SQLGenerator(dialect).generate(plan);
    }

    /**
     * Maps DatabaseType to SQLDialect.
     */
    private SQLDialect getDialect(ConnectionDefinition connectionDef) {
        return switch (connectionDef.databaseType()) {
            case DuckDB -> DuckDBDialect.INSTANCE;
            case SQLite -> SQLiteDialect.INSTANCE;
            case H2, Postgres, Snowflake, BigQuery -> DuckDBDialect.INSTANCE; // Use DuckDB syntax as default
        };
    }

    /**
     * Gets the ConnectionDefinition from a Runtime.
     */
    private ConnectionDefinition getConnectionDef(RuntimeDefinition runtime) {
        if (runtime.connectionBindings().isEmpty()) {
            throw new IllegalStateException("Runtime has no connection bindings: " + runtime.qualifiedName());
        }

        String connectionName = runtime.connectionBindings().values().iterator().next();
        ConnectionDefinition connectionDef = connections.get(connectionName);
        if (connectionDef == null) {
            throw new IllegalArgumentException("Connection not found: " + connectionName);
        }

        return connectionDef;
    }

    /**
     * Builder for creating a QueryExecutor from Pure source.
     */
    public static class Builder {
        private final PureModelBuilder modelBuilder = new PureModelBuilder();
        private final java.util.Map<String, ConnectionDefinition> connections = new java.util.HashMap<>();
        private final java.util.Map<String, RuntimeDefinition> runtimes = new java.util.HashMap<>();

        public Builder addSource(String pureSource) {
            modelBuilder.addSource(pureSource);
            return this;
        }

        public Builder addConnection(ConnectionDefinition conn) {
            connections.put(conn.qualifiedName(), conn);
            connections.put(conn.simpleName(), conn);
            return this;
        }

        public Builder addRuntime(RuntimeDefinition runtime) {
            runtimes.put(runtime.qualifiedName(), runtime);
            runtimes.put(runtime.simpleName(), runtime);
            return this;
        }

        public QueryExecutor build() {
            return new QueryExecutor(modelBuilder, connections, runtimes);
        }
    }
}
