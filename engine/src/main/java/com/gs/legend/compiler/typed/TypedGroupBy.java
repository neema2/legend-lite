package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;
import com.gs.legend.compiler.NativeFunctionDef;

import java.util.List;

/**
 * Relational group-by: {@code source->groupBy([keys], [aggs])}.
 *
 * <p>Keys and aggs are embedded as typed records — no index-zipping with the
 * parallel AST that the legacy {@code TypeInfo} pattern required.
 */
public record TypedGroupBy(
        TypedSpec source,
        List<TypedGroupKey> keys,
        List<TypedAggCall> aggs,
        NativeFunctionDef def,
        ExpressionType info
) implements TypedNative {
    public TypedGroupBy {
        keys = List.copyOf(keys);
        aggs = List.copyOf(aggs);
    }
}
