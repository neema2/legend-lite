package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A type-checked {@code let name = value} binding (engine {@code TypedLet};
 * parsed as {@code letFunction}). Introduces {@code name} into scope for the
 * statements that follow; its own value is {@code value}'s, so {@link #info()}
 * is {@code value.info()}.
 */
public record TypedLet(String name, TypedSpec value, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of(value);
    }
}
