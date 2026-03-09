package org.finos.legend.engine.plan;

/**
 * A single-root execution plan, aligned with legend-engine's
 * {@code SingleExecutionPlan}.
 *
 * <p>
 * Contains a single {@link ExecutionNode} tree rooted at
 * {@link #rootExecutionNode}. For relational queries, the root is
 * typically a {@link SQLExecutionNode}.
 */
public record SingleExecutionPlan(
        ExecutionNode rootExecutionNode) {
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
