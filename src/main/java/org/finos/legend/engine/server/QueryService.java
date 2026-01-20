package org.finos.legend.engine.server;

import org.finos.legend.engine.execution.BufferedResult;
import org.finos.legend.engine.execution.ConnectionResolver;
import org.finos.legend.engine.execution.Result;
import org.finos.legend.engine.execution.StreamingResult;
import org.finos.legend.engine.plan.*;
import org.finos.legend.engine.serialization.ResultSerializer;
import org.finos.legend.engine.serialization.SerializerRegistry;
import org.finos.legend.engine.store.MappingRegistry;
import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLDialect;
import org.finos.legend.engine.transpiler.SQLGenerator;
import org.finos.legend.engine.transpiler.SQLiteDialect;
import org.finos.legend.engine.transpiler.json.DuckDbJsonDialect;
import org.finos.legend.engine.transpiler.json.JsonSqlDialect;
import org.finos.legend.engine.transpiler.json.JsonSqlGenerator;
import org.finos.legend.pure.dsl.PureCompiler;
import org.finos.legend.pure.dsl.definition.ConnectionDefinition;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;
import org.finos.legend.pure.dsl.definition.RuntimeDefinition;

import java.io.IOException;
import java.io.OutputStream;
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
 * Supports both buffered (default) and streaming result modes, as well as
 * direct serialization to various output formats (JSON, CSV, etc).
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
 * BufferedResult result = service.execute(
 *         modelSource, // Classes, Mappings, Connections, Runtimes
 *         "Person.all()->filter({p | $p.age > 30})->project(...)",
 *         "app::MyRuntime");
 * </pre>
 */
public class QueryService {

    private static final int DEFAULT_FETCH_SIZE = 100;

    private final ConnectionResolver connectionResolver = new ConnectionResolver();

    /**
     * Compiles Pure source, generates a plan, and executes - all in one call.
     * Returns a buffered result (all rows materialized in memory).
     * 
     * @param pureSource  The complete Pure source (model + runtime definitions)
     * @param query       The Pure query to execute
     * @param runtimeName The qualified name of the Runtime to use
     * @return The execution result as buffered tabular data
     * @throws SQLException If execution fails
     */
    public BufferedResult execute(String pureSource, String query, String runtimeName)
            throws SQLException {
        return execute(pureSource, query, runtimeName, ResultMode.BUFFERED).toBuffered();
    }

    /**
     * Compiles, generates, and executes using a provided connection.
     * Returns a buffered result.
     * 
     * @param pureSource  The complete Pure source (model + runtime definitions)
     * @param query       The Pure query to execute
     * @param runtimeName The qualified name of the Runtime (for dialect resolution)
     * @param connection  The JDBC connection to use (must already contain test
     *                    data)
     * @return The execution result as buffered tabular data
     * @throws SQLException If execution fails
     */
    public BufferedResult execute(String pureSource, String query, String runtimeName, Connection connection)
            throws SQLException {
        return execute(pureSource, query, runtimeName, connection, ResultMode.BUFFERED).toBuffered();
    }

    /**
     * Executes with explicit result mode selection.
     * 
     * @param pureSource  The complete Pure source
     * @param query       The Pure query to execute
     * @param runtimeName The qualified name of the Runtime
     * @param mode        The result mode (BUFFERED or STREAMING)
     * @return The execution result
     * @throws SQLException If execution fails
     */
    public Result execute(String pureSource, String query, String runtimeName, ResultMode mode)
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
        return executeWithMode(conn, sql, mode);
    }

    /**
     * Executes with explicit result mode using a provided connection.
     */
    public Result execute(String pureSource, String query, String runtimeName,
            Connection connection, ResultMode mode) throws SQLException {

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
        return executeWithMode(connection, sql, mode);
    }

    /**
     * Executes a query and serializes the result directly to an output stream.
     * Automatically chooses streaming mode if the serializer supports it.
     * 
     * @param pureSource  The complete Pure source
     * @param query       The Pure query to execute
     * @param runtimeName The qualified name of the Runtime
     * @param out         The output stream to write to
     * @param format      The serialization format (e.g., "json", "csv")
     * @throws SQLException If execution fails
     * @throws IOException  If serialization fails
     */
    public void executeAndSerialize(String pureSource, String query, String runtimeName,
            OutputStream out, String format) throws SQLException, IOException {
        ResultSerializer serializer = SerializerRegistry.get(format);
        ResultMode mode = serializer.supportsStreaming() ? ResultMode.STREAMING : ResultMode.BUFFERED;

        try (Result result = execute(pureSource, query, runtimeName, mode)) {
            result.writeTo(out, serializer);
        }
    }

    /**
     * Executes a query with provided connection and serializes to output stream.
     */
    public void executeAndSerialize(String pureSource, String query, String runtimeName,
            Connection connection, OutputStream out, String format)
            throws SQLException, IOException {
        ResultSerializer serializer = SerializerRegistry.get(format);
        ResultMode mode = serializer.supportsStreaming() ? ResultMode.STREAMING : ResultMode.BUFFERED;

        try (Result result = execute(pureSource, query, runtimeName, connection, mode)) {
            result.writeTo(out, serializer);
        }
    }

    private Result executeWithMode(Connection conn, String sql, ResultMode mode) throws SQLException {
        return switch (mode) {
            case BUFFERED -> executeBuffered(conn, sql);
            case STREAMING -> executeStreaming(conn, sql);
        };
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

    // ==================== M2M graphFetch Execution ====================

    /**
     * Executes a graphFetch/serialize query and returns JSON.
     * 
     * This is the Legend-compatible M2M execution path:
     * - Input: Person.all()->graphFetch(#{...}#)->serialize(#{...}#)
     * - Output: JSON array of objects, e.g., [{"fullName": "John Smith"}, ...]
     * 
     * @param pureSource  The complete Pure source (model + runtime + M2M mappings)
     * @param query       The graphFetch/serialize Pure query
     * @param runtimeName The qualified name of the Runtime
     * @param connection  The JDBC connection for execution
     * @return JSON string (array of objects)
     * @throws SQLException If execution fails
     */
    public String executeGraphFetch(String pureSource, String query, String runtimeName,
            Connection connection) throws SQLException {

        // 1. Compile the Pure source
        PureModelBuilder model = new PureModelBuilder().addSource(pureSource);

        // 2. Look up runtime for dialect
        RuntimeDefinition runtime = model.getRuntime(runtimeName);
        if (runtime == null) {
            throw new IllegalArgumentException("Runtime not found: " + runtimeName);
        }

        // 3. Compile query to IR (PureCompiler handles graphFetch/serialize)
        MappingRegistry mappingRegistry = model.getMappingRegistry();
        PureCompiler compiler = new PureCompiler(mappingRegistry, model);
        RelationNode ir = compiler.compile(query);

        // 4. Get JSON dialect from runtime's connection
        String storeRef = runtime.connectionBindings().keySet().iterator().next();
        String connectionRef = runtime.connectionBindings().get(storeRef);
        ConnectionDefinition connectionDef = model.getConnection(connectionRef);

        if (connectionDef == null) {
            throw new IllegalArgumentException("Connection not found: " + connectionRef);
        }

        // 5. Generate JSON-producing SQL using JsonSqlGenerator
        JsonSqlDialect jsonDialect = getJsonDialect(connectionDef.databaseType());

        // The IR should be a ProjectNode for graphFetch queries
        if (!(ir instanceof ProjectNode projectNode)) {
            throw new IllegalArgumentException("graphFetch query must compile to ProjectNode, got: " + ir.getClass());
        }

        String sql = new JsonSqlGenerator(jsonDialect).generateJsonSql(projectNode);

        System.out.println("graphFetch Query: " + query);
        System.out.println("Generated JSON SQL: " + sql);

        // 6. Execute and return JSON directly from database
        return executeJsonSql(connection, sql);
    }

    /**
     * Executes JSON-producing SQL and returns the result.
     * The SQL returns a single column containing the full JSON array.
     */
    private String executeJsonSql(Connection connection, String sql) throws SQLException {
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                String jsonResult = rs.getString(1);
                // Handle null result (empty table)
                return jsonResult != null ? jsonResult : "[]";
            }
            return "[]";
        }
    }

    /**
     * Executes SQL and converts ResultSet to JSON array.
     * Uses column names from the IR ProjectNode as JSON property names.
     */
    private String executeAndSerializeToJson(Connection connection, String sql, RelationNode ir)
            throws SQLException {
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            StringBuilder json = new StringBuilder("[");
            boolean first = true;

            // Get column count and names from ResultSet metadata
            int columnCount = rs.getMetaData().getColumnCount();
            String[] columnNames = new String[columnCount];
            for (int i = 0; i < columnCount; i++) {
                columnNames[i] = rs.getMetaData().getColumnLabel(i + 1);
            }

            while (rs.next()) {
                if (!first) {
                    json.append(", ");
                }
                first = false;

                json.append("{");
                for (int i = 0; i < columnCount; i++) {
                    if (i > 0) {
                        json.append(", ");
                    }

                    String columnName = columnNames[i];
                    Object value = rs.getObject(i + 1);

                    json.append("\"").append(columnName).append("\": ");
                    if (value == null) {
                        json.append("null");
                    } else if (value instanceof String strVal) {
                        // Escape quotes in strings
                        json.append("\"").append(strVal.replace("\"", "\\\"")).append("\"");
                    } else if (value instanceof Number) {
                        json.append(value);
                    } else if (value instanceof Boolean) {
                        json.append(value);
                    } else {
                        json.append("\"").append(value.toString().replace("\"", "\\\"")).append("\"");
                    }
                }
                json.append("}");
            }

            json.append("]");
            return json.toString();
        }
    }

    private SQLDialect getDialect(ConnectionDefinition.DatabaseType dbType) {
        return switch (dbType) {
            case DuckDB -> DuckDBDialect.INSTANCE;
            case SQLite -> SQLiteDialect.INSTANCE;
            default -> DuckDBDialect.INSTANCE;
        };
    }

    private JsonSqlDialect getJsonDialect(ConnectionDefinition.DatabaseType dbType) {
        return switch (dbType) {
            case DuckDB -> DuckDbJsonDialect.INSTANCE;
            // SQLite also uses same JSON function names as DuckDB
            case SQLite -> DuckDbJsonDialect.INSTANCE;
            default -> DuckDbJsonDialect.INSTANCE;
        };
    }

    /**
     * Determines whether to buffer or stream results.
     */
    public enum ResultMode {
        /** Materialize all rows in memory (current behavior) */
        BUFFERED,
        /** Lazy iteration with held connection */
        STREAMING
    }
}
