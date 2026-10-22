package com.legend.plan;

import com.legend.compiler.element.type.ExprType;

import java.util.Objects;

/**
 * The compile-only result of the pipeline &mdash; everything
 * {@link Executor} would consume, WITHOUT executing: the rendered SQL,
 * the root's typed contract, and the execution-shape classification.
 * The {@code planSql} seam for SQL-shape assertions and plan inspection.
 *
 * @param sql      rendered SQL in the runtime's dialect
 * @param rootType the query root's {@link ExprType} (schema + multiplicity)
 * @param shape    how a result of {@code rootType} is consumed
 * @param columns  the result's columns with their three type facts ({@link PlanColumns}: the Pure type and
 *                 multiplicity, the slot's SQL type, the store's declared type or none); none for a graph
 */
public record QueryPlan(String sql, ExprType rootType, ResultShape shape,
        java.util.List<com.legend.executionplan.ExecutionPlan.Column> columns) {
    public QueryPlan {
        Objects.requireNonNull(sql, "sql");
        Objects.requireNonNull(rootType, "rootType");
        Objects.requireNonNull(shape, "shape");
        columns = java.util.List.copyOf(columns);
    }
}
