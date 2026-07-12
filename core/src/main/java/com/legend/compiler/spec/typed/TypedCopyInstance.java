package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.Map;

/**
 * Copy-with-update {@code ^$existing(prop=value, …)}: a new instance
 * structurally equal to {@code source} except for the listed overrides.
 * The class is the source's static type; the result multiplicity is [1].
 */
public record TypedCopyInstance(
        TypedSpec source,
        String classFqn,
        Map<String, TypedSpec> overrides,
        ExprType info) implements TypedSpec {

    public TypedCopyInstance {
        // deterministic child order (the TypedNewInstance audit finding)
        overrides = java.util.Collections.unmodifiableMap(
                new java.util.LinkedHashMap<>(overrides));
    }

    @Override
    public java.util.List<TypedSpec> children() {
        java.util.List<TypedSpec> out = new java.util.ArrayList<>();
        out.add(source);
        out.addAll(overrides.values());
        return java.util.List.copyOf(out);
    }
}
