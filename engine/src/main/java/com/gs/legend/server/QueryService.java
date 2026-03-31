package com.gs.legend.server;

import com.gs.legend.compiler.MappingResolver;
import com.gs.legend.compiler.TypeChecker;
import com.gs.legend.compiler.TypeCheckResult;
import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.exec.PlanExecutor;
import com.gs.legend.model.PureModelBuilder;
import com.gs.legend.parser.PureParser;
import com.gs.legend.plan.PlanGenerator;
import com.gs.legend.plan.SingleExecutionPlan;
import com.gs.legend.serial.ResultSerializer;
import com.gs.legend.serial.SerializerRegistry;
import com.gs.legend.sqlgen.SQLDialect;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Stateless query execution: parse → compile → plan → execute.
 *
 * <p>QueryService is the mapping entry point. It orchestrates the full pipeline
 * and threads mapping information explicitly to MappingResolver.
 *
 * <pre>
 * var result = new QueryService().execute(model, query, "app::Runtime", conn);
 * var tabular = result.asTabular();  // typed access
 * </pre>
 */
public class QueryService {

    /**
     * Parse → compile → generate plan → execute with typed result.
     * Mappings are auto-discovered from the registry.
     */
    public ExecutionResult execute(String pureSource, String query, String runtimeName,
            Connection connection) throws SQLException {

        PureModelBuilder model = new PureModelBuilder().addSource(pureSource);
        SingleExecutionPlan plan = generatePlan(model, query, runtimeName);

        System.out.println("Pure Query: " + query);
        System.out.println("Generated SQL: " + plan.sql());

        return PlanExecutor.execute(plan, connection);
    }

    /**
     * Convenience: resolves connection from Runtime, then executes.
     * Mappings are auto-discovered from the registry.
     */
    public ExecutionResult execute(String pureSource, String query, String runtimeName)
            throws SQLException {

        PureModelBuilder model = new PureModelBuilder().addSource(pureSource);
        Connection conn = model.resolveConnection(runtimeName);
        SingleExecutionPlan plan = generatePlan(model, query, runtimeName);

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

        PureModelBuilder model = new PureModelBuilder().addSource(pureSource);
        SingleExecutionPlan plan = generatePlan(model, query, runtimeName);
        ExecutionResult result = PlanExecutor.execute(plan, connection);

        ResultSerializer serializer = SerializerRegistry.get(format);
        serializer.serialize(result, out);
    }

    // ==================== Pipeline ====================

    /**
     * Full pipeline: parse → typecheck → resolve mappings → generate plan.
     *
     * <p>This is the single orchestration point. MappingResolver does its own
     * registry lookups in a single AST walk. PlanGenerator receives
     * pre-resolved StoreResolutions and does zero mapping work.
     */
    private SingleExecutionPlan generatePlan(PureModelBuilder model, String query,
            String runtimeName) {
        SQLDialect dialect = model.resolveDialect(runtimeName);
        var vs = PureParser.parseQuery(query);
        TypeCheckResult unit = new TypeChecker(model).check(vs);

        var storeResolutions = new MappingResolver(unit, model.getMappingRegistry(), model).resolve();
        return new PlanGenerator(unit, dialect, storeResolutions).generate();
    }
}
