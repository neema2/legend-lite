package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;
import com.gs.legend.compiler.NativeFunctionDef;

import java.util.List;

/**
 * Relational aggregate (no group-by keys): {@code source->aggregate([aggs])}.
 *
 * <p>Like {@link TypedGroupBy} but with no grouping — collapses the whole
 * input to a single row per aggregate column.
 */
public record TypedAggregate(
        TypedSpec source,
        List<TypedAggCall> aggs,
        NativeFunctionDef def,
        ExpressionType info
) implements TypedNative {
    public TypedAggregate {
        aggs = List.copyOf(aggs);
    }
}
