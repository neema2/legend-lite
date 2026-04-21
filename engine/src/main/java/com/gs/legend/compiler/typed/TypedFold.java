package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/**
 * {@code source->fold(reducer, init)} — reduction over a collection.
 *
 * <p>{@link FoldStrategy} is a sealed hierarchy carrying the classification
 * (concatenation / same-type / map-reduce / collection-build) plus any
 * strategy-specific typed children — replaces the legacy string-tagged
 * {@code foldStrategy} / {@code elementTransform} / {@code accParam} fields.
 */
public record TypedFold(
        TypedSpec source,
        TypedLambda reducer,
        TypedSpec init,
        FoldStrategy strategy,
        ExpressionType info
) implements TypedSpec {}
