package com.legend.compiler.spec.typed;

import com.legend.compiler.spec.ExprType;

import java.util.List;

import java.util.Map;

/**
 * A type-checked instance construction {@code ^Class(prop=value, …)} (engine
 * {@code TypedNewInstance}). Each property value is type-checked against the
 * class's declared property; the result is {@code Class[1]}.
 */
public record TypedNewInstance(String classFqn, Map<String, TypedSpec> properties,
                               ExprType info) implements TypedSpec {
    public TypedNewInstance {
        // ORDER-PRESERVING copy: Map.copyOf randomizes iteration order per
        // JVM run, which would make children() — and any lowering emitted
        // from it — nondeterministic (audit finding). NewChecker builds a
        // LinkedHashMap in declaration order; keep it.
        properties = java.util.Collections.unmodifiableMap(
                new java.util.LinkedHashMap<>(properties));
    }

    @Override
    public List<TypedSpec> children() {
        return List.copyOf(properties.values());
    }
}
