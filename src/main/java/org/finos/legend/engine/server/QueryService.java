package org.finos.legend.engine.server;

import org.finos.legend.engine.execution.ConnectionResolver;
import org.finos.legend.engine.execution.RelationResult;
import org.finos.legend.engine.plan.ExecutionPlan;
import org.finos.legend.engine.plan.RelationNode;
import org.finos.legend.engine.plan.ResultSchema;
import org.finos.legend.engine.store.MappingRegistry;
import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLDialect;
import org.finos.legend.engine.transpiler.SQLGenerator;
import org.finos.legend.engine.transpiler.SQLiteDialect;
import org.finos.legend.pure.dsl.PureCompiler;
import org.finos.legend.pure.dsl.definition.ConnectionDefinition;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;
import org.finos.legend.pure.dsl.definition.RuntimeDefinition;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Stateless query service for ad-hoc query execution.
 * 
 * Performs compile + plan generation + execution in a single call.
 * No state is retained between calls - each request is independent.
 * 
 * Use cases:
 * - Ad-hoc queries without pre-deployed models
 * - Exploratory data analysis
 * - One-off function calls
 * 
 * Example:
 * 
 * <pre>
 * QueryService service = new QueryService();
 * 
 * RelationResult result = service.execute(
 *         modelSource, // Classes, Mappings, Connections, Runtimes
 *         "Person.all()->filter({p | $p.age > 30})->project(...)",
 *         "app::MyRuntime");
 * </pre>
 */
public class QueryService {

    private final ConnectionResolver connectionResolver = new ConnectionResolver();

    /**
     * Compiles Pure source, generates a plan, and executes - all in one call.
     * 
     * @param pureSource  The complete Pure source (model + runtime definitions)
     * @param query       The Pure query to execute
     * @param runtimeName The qualified name of the Runtime to use
     * @return The execution result as tabular data
     * @throws SQLException If execution fails
     */
    public RelationResult execute(String pureSource, String query, String runtimeName)
            throws SQLException {

        // 1. Compile the Pure source
        PureModelBuilder model = new PureModelBuilder().addSource(pureSource);

        // 2. Look up runtime
        RuntimeDefinition runtime = model.getRuntime(runtimeName);
        if (runtime == null) {
            throw new IllegalArgumentException("Runtime not found: " + runtimeName);
        }

        // 3. Compile query to IR
        MappingRegistry mappingRegistry = model.getMappingRegistry();
        PureCompiler compiler = new PureCompiler(mappingRegistry, model);
        RelationNode ir = compiler.compile(query);

        // 4. Get connection and generate SQL
        String storeRef = runtime.connectionBindings().keySet().iterator().next();
        String connectionRef = runtime.connectionBindings().get(storeRef);
        ConnectionDefinition connectionDef = model.getConnection(connectionRef);

        if (connectionDef == null) {
            throw new IllegalArgumentException("Connection not found: " + connectionRef);
        }

        SQLDialect dialect = getDialect(connectionDef.databaseType());
        String sql = new SQLGenerator(dialect).generate(ir);

        // 5. Resolve connection and execute
        Connection conn = connectionResolver.resolve(connectionDef);
        return executeWithConnection(conn, sql);
    }

    /**
     * Compiles, generates, and executes using a provided connection.
     * This is useful for testing where the connection is pre-configured with test
     * data.
     * 
     * @param pureSource  The complete Pure source (model + runtime definitions)
     * @param query       The Pure query to execute
     * @param runtimeName The qualified name of the Runtime (for dialect resolution)
     * @param connection  The JDBC connection to use (must already contain test
     *                    data)
     * @return The execution result as tabular data
     * @throws SQLException If execution fails
     */
    public RelationResult execute(String pureSource, String query, String runtimeName, Connection connection)
            throws SQLException {

        // 1. Compile the Pure source
        PureModelBuilder model = new PureModelBuilder().addSource(pureSource);

        // 2. Look up runtime for dialect
        RuntimeDefinition runtime = model.getRuntime(runtimeName);
        if (runtime == null) {
            throw new IllegalArgumentException("Runtime not found: " + runtimeName);
        }

        // 3. Compile query to IR
        MappingRegistry mappingRegistry = model.getMappingRegistry();
        PureCompiler compiler = new PureCompiler(mappingRegistry, model);
        RelationNode ir = compiler.compile(query);

        // 4. Get dialect from runtime's connection
        String storeRef = runtime.connectionBindings().keySet().iterator().next();
        String connectionRef = runtime.connectionBindings().get(storeRef);
        ConnectionDefinition connectionDef = model.getConnection(connectionRef);

        if (connectionDef == null) {
            throw new IllegalArgumentException("Connection not found: " + connectionRef);
        }

        SQLDialect dialect = getDialect(connectionDef.databaseType());
        String sql = new SQLGenerator(dialect).generate(ir);

        System.out.println("Pure Query: " + query);
        System.out.println("Generated SQL: " + sql);

        // 5. Execute using provided connection
        return executeWithConnection(connection, sql);
    }

    private RelationResult executeWithConnection(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            return RelationResult.fromResultSet(rs);
        }
    }

    /**
     * Compiles and generates a plan without executing.
     * Useful for plan introspection or caching.
     * 
     * @param pureSource  The complete Pure source
     * @param query       The Pure query
     * @param runtimeName The Runtime name
     * @return The execution plan
     */
    public ExecutionPlan compile(String pureSource, String query, String runtimeName) {
        // 1. Compile the Pure source
        PureModelBuilder model = new PureModelBuilder().addSource(pureSource);

        // 2. Look up runtime
        RuntimeDefinition runtime = model.getRuntime(runtimeName);
        if (runtime == null) {
            throw new IllegalArgumentException("Runtime not found: " + runtimeName);
        }

        // 3. Compile query to IR
        MappingRegistry mappingRegistry = model.getMappingRegistry();
        PureCompiler compiler = new PureCompiler(mappingRegistry, model);
        RelationNode ir = compiler.compile(query);

        // 4. Generate SQL for all connections in runtime
        Map<String, ExecutionPlan.GeneratedSql> sqlByStore = new HashMap<>();

        for (Map.Entry<String, String> binding : runtime.connectionBindings().entrySet()) {
            String storeRef = binding.getKey();
            String connectionRef = binding.getValue();

            ConnectionDefinition connection = model.getConnection(connectionRef);
            if (connection != null) {
                SQLDialect dialect = getDialect(connection.databaseType());
                String sql = new SQLGenerator(dialect).generate(ir);
                sqlByStore.put(storeRef, new ExecutionPlan.GeneratedSql(connection.databaseType(), sql));
            }
        }

        return new ExecutionPlan(
                UUID.randomUUID().toString(),
                ir,
                new ResultSchema(java.util.List.of()),
                sqlByStore,
                runtime.qualifiedName());
    }

    private SQLDialect getDialect(ConnectionDefinition.DatabaseType dbType) {
        return switch (dbType) {
            case DuckDB -> DuckDBDialect.INSTANCE;
            case SQLite -> SQLiteDialect.INSTANCE;
            default -> DuckDBDialect.INSTANCE;
        };
    }
}
