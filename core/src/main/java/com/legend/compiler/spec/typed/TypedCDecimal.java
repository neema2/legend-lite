package com.legend.compiler.spec.typed;

import com.legend.compiler.spec.ExprType;

import java.util.List;

/** A type-checked decimal literal; its info carries the precision/scale (§8). */
public record TypedCDecimal(java.math.BigDecimal value, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of();
    }
}
