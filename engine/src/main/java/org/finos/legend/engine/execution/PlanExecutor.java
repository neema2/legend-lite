package org.finos.legend.engine.execution;

import org.finos.legend.engine.plan.ExecutionPlan;
import org.finos.legend.engine.serialization.ResultSerializer;
import org.finos.legend.engine.serialization.SerializerRegistry;
import org.finos.legend.engine.server.QueryService.ResultMode;
import org.finos.legend.pure.dsl.definition.ConnectionDefinition;
import org.finos.legend.pure.dsl.definition.RuntimeDefinition;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * Executes pre-built ExecutionPlans.
 * 
 * This is the simple execution component that:
 * 1. Looks up SQL from the plan
 * 2. Resolves connection from Runtime
 * 3. Executes and returns results
 * 
 * Supports both buffered and streaming result modes.
 * 
 * Designed to be deployed on Execution Server separate from Plan Server.
 */
public class PlanExecutor {

    private static final int DEFAULT_FETCH_SIZE = 100;

    private final Map<String, ConnectionDefinition> connections;
    private final Map<String, RuntimeDefinition> runtimes;
    private final ConnectionResolver connectionResolver;

    public PlanExecutor(
            Map<String, ConnectionDefinition> connections,
            Map<String, RuntimeDefinition> runtimes) {
        this.connections = connections;
        this.runtimes = runtimes;
        this.connectionResolver = new ConnectionResolver();
    }

    /**
     * Executes a plan using the default (first) store.
     * Returns a buffered result.
     * 
     * @param plan The execution plan
     * @return The execution result as BufferedResult
     * @throws SQLException If execution fails
     */
    public BufferedResult execute(ExecutionPlan plan) throws SQLException {
        String storeRef = plan.getDefaultStoreRef();
        if (storeRef == null) {
            throw new IllegalStateException("Plan has no store bindings");
        }
        return execute(plan, storeRef);
    }

    /**
     * Executes a plan against a specific store.
     * Returns a buffered result.
     * 
     * @param plan     The execution plan
     * @param storeRef The store qualified name
     * @return The execution result as BufferedResult
     * @throws SQLException If execution fails
     */
    public BufferedResult execute(ExecutionPlan plan, String storeRef) throws SQLException {
        return execute(plan, storeRef, ResultMode.BUFFERED).toBuffered();
    }

    /**
     * Executes a plan with explicit result mode.
     * 
     * @param plan     The execution plan
     * @param storeRef The store qualified name
     * @param mode     The result mode (BUFFERED or STREAMING)
     * @return The execution result
     * @throws SQLException If execution fails
     */
    public Result execute(ExecutionPlan plan, String storeRef, ResultMode mode) throws SQLException {
        // 1. Get pre-generated SQL from plan
        String sql = plan.getSql(storeRef);
        if (sql == null) {
            throw new IllegalArgumentException("No SQL generated for store: " + storeRef);
        }

        // 2. Resolve connection from Runtime
        RuntimeDefinition runtime = runtimes.get(plan.runtimeRef());
        if (runtime == null) {
            throw new IllegalArgumentException("Runtime not found: " + plan.runtimeRef());
        }

        String connectionRef = runtime.connectionBindings().get(storeRef);
        if (connectionRef == null) {
            throw new IllegalArgumentException("No connection binding for store: " + storeRef);
        }

        ConnectionDefinition connectionDef = connections.get(connectionRef);
        if (connectionDef == null) {
            throw new IllegalArgumentException("Connection not found: " + connectionRef);
        }

        // 3. Execute with specified mode
        Connection conn = connectionResolver.resolve(connectionDef);
        return executeWithMode(conn, sql, mode);
    }

    /**
     * Executes a plan and serializes the result directly to an output stream.
     * 
     * @param plan     The execution plan
     * @param storeRef The store qualified name
     * @param out      The output stream to write to
     * @param format   The serialization format
     * @throws SQLException If execution fails
     * @throws IOException  If serialization fails
     */
    public void executeAndSerialize(ExecutionPlan plan, String storeRef,
            OutputStream out, String format) throws SQLException, IOException {
        ResultSerializer serializer = SerializerRegistry.get(format);
        ResultMode mode = serializer.supportsStreaming() ? ResultMode.STREAMING : ResultMode.BUFFERED;

        try (Result result = execute(plan, storeRef, mode)) {
            result.writeTo(out, serializer);
        }
    }

    private Result executeWithMode(Connection conn, String sql, ResultMode mode) throws SQLException {
        return switch (mode) {
            case BUFFERED -> executeBuffered(conn, sql);
            case STREAMING -> executeStreaming(conn, sql);
            case SCALAR -> executeScalar(conn, sql);
        };
    }

    private Result executeScalar(Connection conn, String sql) throws SQLException {
        BufferedResult buffered = executeBuffered(conn, sql);
        if (buffered.rowCount() == 1 && buffered.columnCount() == 1) {
            String sqlType = buffered.columns().getFirst().sqlType();
            return new ScalarResult(buffered.getValue(0, 0), sqlType);
        }
        return buffered;
    }

    private BufferedResult executeBuffered(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            return BufferedResult.fromResultSet(rs);
        }
    }

    private StreamingResult executeStreaming(Connection conn, String sql) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        return StreamingResult.fromResultSet(rs, stmt, conn, DEFAULT_FETCH_SIZE);
    }

    /**
     * Executes a plan and returns typed record results.
     * 
     * @param plan       The execution plan
     * @param storeRef   The store qualified name
     * @param recordType The record class to hydrate
     * @return List of typed record instances
     */
    public <T extends Record> java.util.List<T> executeTyped(
            ExecutionPlan plan,
            String storeRef,
            Class<T> recordType) throws SQLException {

        String sql = plan.getSql(storeRef);
        if (sql == null) {
            throw new IllegalArgumentException("No SQL generated for store: " + storeRef);
        }

        RuntimeDefinition runtime = runtimes.get(plan.runtimeRef());
        String connectionRef = runtime.connectionBindings().get(storeRef);
        ConnectionDefinition connectionDef = connections.get(connectionRef);

        Connection conn = connectionResolver.resolve(connectionDef);
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            RecordResultBuilder<T> builder = new RecordResultBuilder<>(recordType);
            return builder.buildAll(rs);
        }
    }

    /**
     * Builder for PlanExecutor.
     */
    public static class Builder {
        private final Map<String, ConnectionDefinition> connections = new java.util.HashMap<>();
        private final Map<String, RuntimeDefinition> runtimes = new java.util.HashMap<>();

        public Builder addConnection(ConnectionDefinition connection) {
            connections.put(connection.qualifiedName(), connection);
            connections.put(connection.simpleName(), connection);
            return this;
        }

        public Builder addRuntime(RuntimeDefinition runtime) {
            runtimes.put(runtime.qualifiedName(), runtime);
            runtimes.put(runtime.simpleName(), runtime);
            return this;
        }

        public PlanExecutor build() {
            return new PlanExecutor(connections, runtimes);
        }
    }
}
