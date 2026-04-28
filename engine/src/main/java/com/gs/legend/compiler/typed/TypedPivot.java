package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;
import com.gs.legend.compiler.NativeFunctionDef;

import java.util.List;

/**
 * Relational pivot: {@code source->pivot([pivotCols], [aggs])}.
 *
 * <p>Placeholder carrier — full typed sub-variants for pivot columns and
 * aggregations will land when PlanGenerator migrates to read this node.
 */
public record TypedPivot(
        TypedSpec source,
        List<String> pivotColumns,
        List<TypedAggCall> aggs,
        NativeFunctionDef def,
        ExpressionType info
) implements TypedNative {
    public TypedPivot {
        pivotColumns = List.copyOf(pivotColumns);
        aggs = List.copyOf(aggs);
    }
}
