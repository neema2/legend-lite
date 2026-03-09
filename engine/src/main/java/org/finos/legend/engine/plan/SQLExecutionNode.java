package org.finos.legend.engine.plan;

import java.util.List;

/**
 * Execution node that runs a SQL query against a relational database.
 *
 * <p>
 * Aligned with legend-engine's {@code SQLExecutionNode}, which holds:
 * <ul>
 * <li>{@code sqlQuery} — the rendered SQL string</li>
 * <li>{@code connection} — database connection reference</li>
 * <li>{@code resultColumns} — output column metadata</li>
 * </ul>
 *
 * <p>
 * SQL is rendered at plan generation time (by {@code PlanGenerator}),
 * so the plan is ready to execute without needing a dialect reference.
 */
public record SQLExecutionNode(
        String sqlQuery,
        RelationType resultType,
        String connectionRef) implements ExecutionNode {

    /**
     * Returns result column metadata derived from the result type.
     * Aligns with legend-engine's {@code SQLExecutionNode.resultColumns}.
     */
    public List<ResultColumn> resultColumns() {
        if (resultType == null)
            return List.of();
        return resultType.columns().entrySet().stream()
                .map(e -> new ResultColumn(e.getKey(), e.getValue().toString()))
                .toList();
    }

    /**
     * Describes a single output column from the SQL query.
     */
    public record ResultColumn(String name, String dataType) {
    }
}
