package com.legend.exec;

import com.legend.compiler.element.type.ExprType;

import java.util.Objects;

/**
 * The compile-only result of the pipeline &mdash; everything
 * {@link Executor} would consume, WITHOUT executing: the rendered SQL,
 * the root's typed contract, and the execution-shape classification.
 * The {@code planSql} seam for SQL-shape assertions and plan inspection;
 * bridges (engine {@code CoreBridge}) re-wrap these fields verbatim.
 *
 * @param sql      rendered SQL in the runtime's dialect
 * @param rootType the query root's {@link ExprType} (schema + multiplicity)
 * @param shape    how a result of {@code rootType} is consumed
 */
public record QueryPlan(String sql, ExprType rootType, ResultShape shape) {

    public QueryPlan {
        Objects.requireNonNull(sql, "sql");
        Objects.requireNonNull(rootType, "rootType");
        Objects.requireNonNull(shape, "shape");
    }
}
