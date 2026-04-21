package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.NativeFunctionDef;
import com.gs.legend.model.m3.Type;

import java.util.Optional;

/**
 * Typed aggregate call, Calcite/Spark/Substrait shape.
 *
 * <p>Aggregates have two phases captured as separate lambdas:
 * {@code fn1} extracts per-row values from the source relation and {@code fn2}
 * is the aggregation body applied to the collected values. {@code castType}
 * holds an outer CAST wrapper when the aggregate's native result requires
 * one to match the declared column type (e.g., {@code AVG} → {@code DECIMAL}).
 */
public record TypedAggCall(
        String alias,
        NativeFunctionDef func,
        TypedLambda fn1,
        TypedLambda fn2,
        Type returnType,
        Optional<Type> castType
) {}
