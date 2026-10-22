package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

import java.util.Map;

/**
 * A type-checked instance construction {@code ^Class(prop=value, …)} (engine
 * {@code TypedNewInstance}). Each property value is type-checked against the
 * class's declared property; the result is {@code Class[1]}.
 */
public record TypedNewInstance(String classFqn, Map<String, TypedSpec> properties,
                               ExprType info, Evaluation evaluation) implements TypedSpec {
    /** A construction the compiler builds, its own evaluation. */
    public TypedNewInstance(String classFqn, Map<String, TypedSpec> properties, ExprType info) {
        this(classFqn, properties, info, Evaluation.synthesized());
    }

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

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, properties.size(), "TypedNewInstance");
        java.util.Map<String, TypedSpec> ps = new java.util.LinkedHashMap<>();
        int i = 0;
        for (String k : properties.keySet()) {
            ps.put(k, kids.get(i++));
        }
        return new TypedNewInstance(classFqn, ps, info, evaluation);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedNewInstance(classFqn, properties, info, evaluation);
    }

    /** The same construction (its evaluation kept) with {@code ps} as its property values. */
    public TypedNewInstance withProperties(Map<String, TypedSpec> ps) {
        return new TypedNewInstance(classFqn, ps, info, evaluation);
    }

    public TypedNewInstance withEvaluation(Evaluation e) {
        return new TypedNewInstance(classFqn, properties, info, e);
    }
}
