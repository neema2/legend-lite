package com.legend.compiler.spec.typed;

import com.legend.compiler.spec.ExprType;

import java.util.List;

/** A type-checked string literal. */
public record TypedCString(String value, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of();
    }
}
