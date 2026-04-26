package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.NativeFunctionDef;
import com.gs.legend.model.m3.Type;

import java.util.List;
import java.util.Optional;

/**
 * Typed aggregate call, Calcite/Spark/Substrait shape.
 *
 * <p>Aggregates have two phases captured as separate lambdas:
 * {@code fn1} extracts per-row values from the source relation and {@code fn2}
 * is the aggregation body applied to the collected values.
 *
 * <p>{@code extraArgs} carries the additional operands of multi-operand
 * reducer calls — the separator in {@code joinStrings(values, sep)}, the
 * percentile {@code p} in {@code percentile(values, p)}, the second column
 * in {@code corr(x, y)}. The first reducer operand (the values being
 * aggregated) is produced by {@code fn1}; the rest are extracted from
 * {@code fn2}'s body at compile time and lowered alongside fn1's result.
 * Empty for unary aggregates (sum/avg/count/etc.).
 *
 * <p>{@code castType} holds an outer CAST wrapper when the aggregate's
 * native result requires one to match the declared column type
 * (e.g., {@code AVG} → {@code DECIMAL}).
 */
public record TypedAggCall(
        String alias,
        NativeFunctionDef func,
        TypedLambda fn1,
        TypedLambda fn2,
        List<TypedSpec> extraArgs,
        Type returnType,
        Optional<Type> castType
) {
    /** Convenience for unary aggregates. */
    public TypedAggCall(String alias, NativeFunctionDef func,
                        TypedLambda fn1, TypedLambda fn2,
                        Type returnType, Optional<Type> castType) {
        this(alias, func, fn1, fn2, List.of(), returnType, castType);
    }
}
