package org.finos.legend.engine.plan;

import org.finos.legend.engine.execution.ExecutionResult;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Executes a {@link SingleExecutionPlan} against a JDBC connection.
 *
 * <p>
 * Stateless — the plan already contains the rendered SQL and the
 * returnType, so no dialect or other state is needed.
 *
 * <p>
 * Returns a typed {@link ExecutionResult} (TabularResult, ScalarResult,
 * CollectionResult) based on the plan's returnType.
 *
 * <pre>
 * var result = PlanExecutor.execute(plan, connection);
 * var tabular = result.asTabular();
 * </pre>
 */
public class PlanExecutor {

    /**
     * Executes the plan and returns a typed ExecutionResult based on the
     * plan's returnType.
     */
    public static ExecutionResult execute(SingleExecutionPlan plan, Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(plan.sql())) {
            return ExecutionResult.fromResultSet(plan.returnType(), rs);
        }
    }
}
