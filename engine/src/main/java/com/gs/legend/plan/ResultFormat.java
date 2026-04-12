package com.gs.legend.plan;

/**
 * Execution format — HOW the SQL result should be interpreted.
 *
 * <p>Separates language type ({@link com.gs.legend.compiler.ExpressionType},
 * stamped by the Compiler) from execution format (stamped by
 * {@link PlanGenerator}). {@link com.gs.legend.exec.ExecutionResult}
 * dispatches on this, not on the expression type.
 *
 * <p>Mirrors legend-engine's {@code ResultType} hierarchy
 * (ClassResultType, TDSResultType, DataTypeResultType) without
 * carrying redundant metadata — our ExpressionType already has that.
 */
public sealed interface ResultFormat {

    /** JSON-wrapped class instances: json_group_array(json_object(...)) → single JSON string. */
    record Graph() implements ResultFormat {}

    /** Columnar result: schema-driven columns + rows. */
    record Tabular() implements ResultFormat {}

    /** Single value or collection: multiplicity distinguishes scalar vs collection. */
    record Scalar() implements ResultFormat {}
}
