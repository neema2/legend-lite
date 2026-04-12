package com.gs.legend.exec;

import com.gs.legend.plan.SingleExecutionPlan;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Executes a {@link SingleExecutionPlan} against a JDBC connection.
 *
 * <p>The plan's SQL is fully self-contained — JSON sources are inlined
 * as VARIANT subqueries in the FROM clause. No temp tables, no parameters.
 *
 * <pre>
 * var result = PlanExecutor.execute(plan, connection);
 * var tabular = result.asTabular();
 * </pre>
 */
public class PlanExecutor {

    public static ExecutionResult execute(SingleExecutionPlan plan, Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(plan.sql())) {
            return ExecutionResult.fromResultSet(plan, rs);
        }
    }
}
