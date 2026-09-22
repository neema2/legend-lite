package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/** A type-checked float literal. {@code exact} carries the decimal-exact
 * digits when the double round-trip is lossy (legend-pure's Float IS
 * BigDecimal-backed — see {@code CFloat}); the TYPE stays Float. */
public record TypedCFloat(double value,
        java.math.@com.legend.base.Nullable BigDecimal exact,
        ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of();
    }

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 0, "TypedCFloat");
        return this;
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedCFloat(value, exact, info);
    }
}
