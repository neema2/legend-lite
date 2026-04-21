package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.NativeFunctionDef;
import com.gs.legend.model.m3.Type;

import java.util.Optional;

/** Window extend: {@code over(partitionBy, sortBy, frame) → func(fn1, fn2)}. */
public record TypedWindowExtendCol(
        String alias,
        NativeFunctionDef func,
        TypedLambda fn1,
        Optional<TypedLambda> fn2,
        TypedOver over,
        Type returnType,
        Optional<Type> castType
) implements TypedExtendCol {}
