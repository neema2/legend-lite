package com.gs.legend.plan;

import com.gs.legend.compiler.typed.TypedGraphFetch;
import com.gs.legend.compiler.typed.TypedSerialize;
import com.gs.legend.compiler.typed.TypedSerializeImplicit;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.compiler.typed.TypedWrite;
import com.gs.legend.model.m3.Type;

/**
 * Execution format — HOW the SQL result should be interpreted.
 *
 * <p>Separates language type ({@link com.gs.legend.compiler.ExpressionType},
 * stamped by the Compiler) from execution format (stamped by
 * {@link PlanGenerator} via {@link #from(TypedSpec)}).
 * {@link com.gs.legend.exec.ExecutionResult} dispatches on this, not on
 * the expression type.
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

    /**
     * Classify a HIR root into one of {@link Graph} / {@link Tabular} /
     * {@link Scalar}. Pure pass, independent of lowering: walks
     * {@link TypedSpec} only and reads {@code info().type()}, keeping
     * execution-format concerns out of {@code SqlRelation} / printing.
     *
     * <p>Graph terminators: {@link TypedSerialize} (explicit
     * {@code ->serialize()}), {@link TypedGraphFetch}, and
     * {@link TypedSerializeImplicit} (the legend-lite marker synthesized
     * by {@code MappingResolver} for bare {@code Class[*]} roots — same
     * wire format as explicit serialize).
     */
    static ResultFormat from(TypedSpec node) {
        if (node instanceof TypedGraphFetch
                || node instanceof TypedSerialize
                || node instanceof TypedSerializeImplicit) {
            return new Graph();
        }
        if (node instanceof TypedWrite) {
            return new Scalar();
        }
        return node.type() instanceof Type.Relation ? new Tabular() : new Scalar();
    }
}
