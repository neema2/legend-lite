package org.finos.legend.engine.execution;

import org.finos.legend.engine.plan.ExecutionPlan;
import org.finos.legend.engine.plan.ResultSchema;
import org.finos.legend.pure.dsl.definition.ConnectionDefinition;
import org.finos.legend.pure.dsl.definition.RuntimeDefinition;

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
 * Designed to be deployed on Execution Server separate from Plan Server.
 */
public class PlanExecutor {

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
     * 
     * @param plan The execution plan
     * @return The execution result as RelationResult
     * @throws SQLException If execution fails
     */
    public RelationResult execute(ExecutionPlan plan) throws SQLException {
        String storeRef = plan.getDefaultStoreRef();
        if (storeRef == null) {
            throw new IllegalStateException("Plan has no store bindings");
        }
        return execute(plan, storeRef);
    }

    /**
     * Executes a plan against a specific store.
     * 
     * @param plan     The execution plan
     * @param storeRef The store qualified name
     * @return The execution result as RelationResult
     * @throws SQLException If execution fails
     */
    public RelationResult execute(ExecutionPlan plan, String storeRef) throws SQLException {
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

        // 3. Execute and build result
        Connection conn = connectionResolver.resolve(connectionDef);
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            return RelationResult.fromResultSet(rs);
        }
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
