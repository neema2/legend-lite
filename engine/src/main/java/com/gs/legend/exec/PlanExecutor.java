package com.gs.legend.exec;
import com.gs.legend.plan.*;
import com.gs.legend.model.*;
import com.gs.legend.model.def.*;
import com.gs.legend.model.m3.*;
import com.gs.legend.model.store.*;
import com.gs.legend.model.mapping.*;
import com.gs.legend.compiler.*;
import com.gs.legend.sqlgen.*;
import com.gs.legend.exec.ExecutionResult;

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
