package com.gs.legend.compiler.typed;

/**
 * Fold strategy: per-element transform composed with a reducer over an accumulator.
 * Carries the element-transform expression, the reducer body, and the parameter
 * names used for accumulator/fresh-element binding.
 */
public record MapReduce(
        TypedSpec elementTransform,
        TypedSpec reducerBody,
        String accParam,
        String freshParam
) implements FoldStrategy {}
