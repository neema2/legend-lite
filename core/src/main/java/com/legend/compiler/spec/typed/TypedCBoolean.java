package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/** A type-checked boolean literal. */
public record TypedCBoolean(boolean value, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of();
    }
}
