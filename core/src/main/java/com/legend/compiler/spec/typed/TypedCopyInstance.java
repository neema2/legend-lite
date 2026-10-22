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
        ExprType info,
        Evaluation evaluation) implements TypedSpec {

    /** A copy the compiler builds, its own evaluation. */
    public TypedCopyInstance(TypedSpec source, String classFqn, Map<String, TypedSpec> overrides, ExprType info) {
        this(source, classFqn, overrides, info, Evaluation.synthesized());
    }

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

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 1 + overrides.size(), "TypedCopyInstance");
        java.util.Map<String, TypedSpec> os = new java.util.LinkedHashMap<>();
        int i = 1;
        for (String k : overrides.keySet()) {
            os.put(k, kids.get(i++));
        }
        return new TypedCopyInstance(kids.get(0), classFqn, os, info, evaluation);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedCopyInstance(source, classFqn, overrides, info, evaluation);
    }

    public TypedCopyInstance withEvaluation(Evaluation e) {
        return new TypedCopyInstance(source, classFqn, overrides, info, e);
    }
}
