package com.gs.legend.plan.resultformat;

import com.gs.legend.compiler.typed.*;
import com.gs.legend.model.m3.Type;
import com.gs.legend.plan.PlanGenNotPortedException;
import com.gs.legend.plan.ResultFormat;

/**
 * Classify a typed HIR root into one of the three runtime {@link ResultFormat}s:
 * {@link ResultFormat.Scalar} (single value or collection), {@link ResultFormat.Tabular}
 * (relation with column schema), or {@link ResultFormat.Graph} (class-instance JSON).
 *
 * <p>Pure pass, independent of lowering: it walks {@link TypedSpec} only, reading
 * {@code info().type()} and picking shape from there. This keeps execution-format
 * concerns out of {@code SqlRelation} / printing.
 */
public final class ResultFormatClassifier {
    private ResultFormatClassifier() {}

    /** Classify a HIR root. */
    public static ResultFormat classify(TypedSpec node) {
        // GraphFetch + Serialize are unambiguous Graph terminators.
        if (node instanceof TypedGraphFetch || node instanceof TypedSerialize) {
            return new ResultFormat.Graph();
        }
        // Write -> rowcount scalar
        if (node instanceof TypedWrite) {
            return new ResultFormat.Scalar();
        }
        // UserCall: reclassify on the inlined body.
        if (node instanceof TypedUserCall uc) {
            return classify(uc.callee().body().hir());
        }
        Type t = node.type();
        if (t instanceof Type.Relation) {
            return new ResultFormat.Tabular();
        }
        // Class-typed bare root (no graph-fetch wrapping) — rare; treat as Graph.
        if (t instanceof Type.ClassType) {
            return new ResultFormat.Graph();
        }
        // Primitives, enums, tuples, precision-decimals, collections of primitives — all scalar.
        return new ResultFormat.Scalar();
    }
}
