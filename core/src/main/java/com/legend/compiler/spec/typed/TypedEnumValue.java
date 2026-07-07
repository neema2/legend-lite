package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A type-checked enum value reference {@code my::pkg::Kind.VALUE} (e.g.
 * {@code JoinKind.INNER}) &mdash; typed {@code EnumType(fqn)[1]} after validating
 * both the enumeration and the value exist.
 *
 * @param enumFqn the enumeration's fully-qualified name
 * @param value   the value name
 * @param info    {@code EnumType(enumFqn)[1]}
 */
public record TypedEnumValue(String enumFqn, String value, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of();
    }
}
