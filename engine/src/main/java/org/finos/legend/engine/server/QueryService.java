package org.finos.legend.engine.server;

import org.finos.legend.engine.execution.ExecutionResult;
import org.finos.legend.engine.plan.PlanExecutor;
import org.finos.legend.engine.plan.SingleExecutionPlan;
import org.finos.legend.engine.serialization.ResultSerializer;
import org.finos.legend.engine.serialization.SerializerRegistry;
import org.finos.legend.pure.dsl.ast.PlanGenerator;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Stateless query execution: parse → compile → plan → execute.
 *
 * <pre>
 * var result = new QueryService().execute(model, query, "app::Runtime", conn);
 * var tabular = result.asTabular();  // typed access
 * </pre>
 */
public class QueryService {

    /**
     * Parse → compile → generate plan → execute with typed result.
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
     */
    public ExecutionResult executeSql(String pureSource, String sql, String runtimeName)
            throws SQLException {

        PureModelBuilder model = new PureModelBuilder().addSource(pureSource);
        Connection conn = model.resolveConnection(runtimeName);

        try (java.sql.Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            return ExecutionResult.empty();
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
