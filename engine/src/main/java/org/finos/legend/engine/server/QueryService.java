package org.finos.legend.engine.server;

import org.finos.legend.engine.execution.ExecutionResult;
import org.finos.legend.engine.plan.PlanExecutor;
import org.finos.legend.engine.plan.SingleExecutionPlan;
import org.finos.legend.engine.serialization.ResultSerializer;
import org.finos.legend.engine.serialization.SerializerRegistry;
import org.finos.legend.engine.store.MappingRegistry;
import org.finos.legend.engine.transpiler.SQLDialect;
import org.finos.legend.engine.transpiler.json.JsonSqlDialect;
import org.finos.legend.engine.transpiler.json.JsonSqlGenerator;
import org.finos.legend.pure.dsl.PureCompiler;
import org.finos.legend.pure.dsl.ast.PlanGenerator;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;
import org.finos.legend.engine.plan.*;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Stateless query execution: parse → compile → plan → execute.
 *
 * <pre>
 * var result = new QueryService().execute(model, query, "app::Runtime", conn);
 * var tabular = result.asTabular();  // typed access
 * </pre>
 */
public class QueryService {

    // ==================== Public API ====================

    /**
     * Parse → compile → generate plan → execute with typed result.
     * Returns the right ExecutionResult variant based on plan's returnType.
     */
    public ExecutionResult execute(String pureSource, String query, String runtimeName,
            Connection connection) throws SQLException {

        SingleExecutionPlan plan = PlanGenerator.generate(pureSource, query, runtimeName);

        System.out.println("Pure Query: " + query);
        System.out.println("Generated SQL: " + plan.sql());

        return PlanExecutor.execute(plan, connection);
    }

    /**
     * Convenience: resolves connection from Runtime, then executes.
     * Uses model-based overload to avoid double-parsing.
     */
    public ExecutionResult execute(String pureSource, String query, String runtimeName)
            throws SQLException {

        PureModelBuilder model = new PureModelBuilder().addSource(pureSource);
        Connection conn = model.resolveConnection(runtimeName);
        SingleExecutionPlan plan = PlanGenerator.generate(model, query, runtimeName);

        System.out.println("Pure Query: " + query);
        System.out.println("Generated SQL: " + plan.sql());

        return PlanExecutor.execute(plan, conn);
    }

    /**
     * Execute raw SQL against the connection from the Runtime.
     * No Pure compilation — just SQL passthrough for DDL/DML/SELECT.
     */
    public ExecutionResult executeSql(String pureSource, String sql, String runtimeName)
            throws SQLException {

        PureModelBuilder model = new PureModelBuilder().addSource(pureSource);
        Connection conn = model.resolveConnection(runtimeName);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            return ExecutionResult.empty();
        }
    }

    /**
     * Execute a graphFetch/serialize query, returning JSON.
     * Uses old pipeline (PureCompiler) until graphFetch is in new pipeline.
     */
    public String executeGraphFetch(String pureSource, String query, String runtimeName,
            Connection connection) throws SQLException {

        PureModelBuilder model = new PureModelBuilder().addSource(pureSource);
        MappingRegistry mappingRegistry = model.getMappingRegistry();

        RelationNode ir = new PureCompiler(mappingRegistry, model).compile(query);
        JsonSqlDialect jsonDialect = SQLDialect.forConnection(connection).getJsonDialect();

        if (!(ir instanceof ProjectNode projectNode)) {
            throw new IllegalArgumentException(
                    "graphFetch query must compile to ProjectNode, got: " + ir.getClass());
        }

        String sql = new JsonSqlGenerator(jsonDialect).generateJsonSql(projectNode);

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() ? (rs.getString(1) != null ? rs.getString(1) : "[]") : "[]";
        }
    }

    /**
     * Parse → compile → plan → stream results to output.
     */
    public void stream(String pureSource, String query, String runtimeName,
            Connection connection, OutputStream out, String format)
            throws SQLException, IOException {

        SingleExecutionPlan plan = PlanGenerator.generate(pureSource, query, runtimeName);
        ExecutionResult result = PlanExecutor.execute(plan, connection);

        ResultSerializer serializer = SerializerRegistry.get(format);
        serializer.serialize(result, out);
    }
}
