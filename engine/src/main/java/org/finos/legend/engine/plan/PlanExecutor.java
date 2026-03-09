package org.finos.legend.engine.plan;

import org.finos.legend.engine.execution.BufferedResult;
import org.finos.legend.engine.execution.Result;
import org.finos.legend.engine.execution.ScalarResult;
import org.finos.legend.engine.execution.StreamingResult;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Executes a {@link SingleExecutionPlan} against a JDBC connection.
 *
 * <p>
 * Stateless — the plan already contains the rendered SQL, so no
 * dialect or other state is needed. Just pass the plan and a connection.
 *
 * <p>
 * Usage:
 * 
 * <pre>
 * var result = new PlanExecutor().execute(plan, connection);
 * </pre>
 */
public class PlanExecutor {

    private static final int DEFAULT_FETCH_SIZE = 1000;

    /**
     * Executes the plan and returns a buffered result (all rows in memory).
     */
    public BufferedResult execute(SingleExecutionPlan plan, Connection conn) throws SQLException {
        return executeBuffered(conn, plan.sql());
    }

    /**
     * Executes the plan with the specified result mode.
     */
    public Result execute(SingleExecutionPlan plan, Connection conn, ResultMode mode) throws SQLException {
        return executeWithMode(conn, plan.sql(), mode);
    }

    // ========== Internal Execution ==========

    private Result executeWithMode(Connection conn, String sql, ResultMode mode) throws SQLException {
        return switch (mode) {
            case BUFFERED -> executeBuffered(conn, sql);
            case STREAMING -> executeStreaming(conn, sql);
            case SCALAR -> executeScalar(conn, sql);
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

    private Result executeScalar(Connection conn, String sql) throws SQLException {
        BufferedResult buffered = executeBuffered(conn, sql);
        if (buffered.rowCount() == 1 && buffered.columnCount() == 1) {
            String sqlType = buffered.columns().getFirst().sqlType();
            return new ScalarResult(buffered.getValue(0, 0), sqlType);
        }
        return buffered;
    }

    /**
     * Determines whether to buffer or stream results.
     */
    public enum ResultMode {
        /** Materialize all rows in memory. */
        BUFFERED,
        /** Lazy iteration with held connection. */
        STREAMING,
        /** Single scalar value (for constant queries). */
        SCALAR
    }
}
