package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A type-checked reference to a packageable element used as a value (engine
 * {@code TypedPackageableRef}) &mdash; currently a class reference, whose type is
 * {@code Class<ThatClass>[1]} (e.g. the {@code Person} in {@code Person.all()}).
 */
public record TypedPackageableRef(String fullPath, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of();
    }
}
