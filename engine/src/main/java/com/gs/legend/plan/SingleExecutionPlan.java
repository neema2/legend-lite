package com.gs.legend.plan;
import com.gs.legend.model.m3.*;
import com.gs.legend.model.store.*;
import java.util.Objects;

/**
 * A single-root execution plan, aligned with legend-engine's
 * {@code SingleExecutionPlan}.
 *
 * <p>
 * Contains a single {@link ExecutionNode} tree rooted at
 * {@link #rootExecutionNode} and the Pure-level {@link #returnType}
 * of the compiled expression.
 */
public record SingleExecutionPlan(
        ExecutionNode rootExecutionNode,
        GenericType returnType) {

    public SingleExecutionPlan {
        Objects.requireNonNull(returnType, "returnType must not be null — compiler bug if missing");
    }

    /**
     * Convenience: returns the SQL when the plan has a single SQL node at root.
     *
     * @return the rendered SQL string
     * @throws UnsupportedOperationException if root is not a SQL node
     */
    public String sql() {
        if (rootExecutionNode instanceof SQLExecutionNode sql) {
            return sql.sqlQuery();
        }
        throw new UnsupportedOperationException(
                "Plan root is not a SQL node: " + rootExecutionNode.getClass().getSimpleName());
    }
}


