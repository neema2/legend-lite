package com.gs.legend.compiler.typed;

import com.gs.legend.model.m3.Type;

/** Scalar per-row extend: {@code ~name:row|<expr>}. */
public record TypedScalarExtendCol(
        String alias,
        TypedLambda expression,
        Type returnType
) implements TypedExtendCol {}
