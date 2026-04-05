package com.gs.legend.compiler;

import com.gs.legend.model.ModelContext;
import com.gs.legend.model.mapping.ClassMapping;

import java.util.Map;
import java.util.Optional;

/**
 * Immutable, self-contained snapshot of a single mapping scope.
 *
 * <p>Produced by {@link MappingNormalizer}, consumed by {@link MappingResolver}
 * and (indirectly via {@code ModelContext.findMappingExpression()}) by
 * {@link TypeChecker}.
 *
 * <p>Replaces {@link com.gs.legend.model.mapping.MappingRegistry} for
 * MappingResolver. All M2M chains are pre-resolved at construction time —
 * no mutation methods exist.
 *
 * <h3>Encapsulation Boundaries</h3>
 * <ul>
 *   <li><b>TypeChecker</b> sees only {@link #findMappingExpression(String)}
 *       (via ModelContext delegation)</li>
 *   <li><b>MappingResolver</b> sees {@link #findClassMapping(String)}
 *       and {@link #findSourceRelation(String)} — read-only.
 *       Association joins are embedded in sourceRelation as extend() nodes
 *       with fn1=traverse (walked by MappingResolver directly).</li>
 *   <li>Neither sees {@code MappingRegistry} directly</li>
 * </ul>
 */
public final class NormalizedMapping {

    private final Map<String, ClassMapping> classMappings;
    private final Map<String, ModelContext.MappingExpression> expressions;

    public NormalizedMapping(
            Map<String, ClassMapping> classMappings,
            Map<String, ModelContext.MappingExpression> expressions) {
        this.classMappings = Map.copyOf(classMappings);
        this.expressions = Map.copyOf(expressions);
    }

    /**
     * Finds a fully-resolved class mapping by class name.
     * Used by MappingResolver (replaces registry.findAnyMapping).
     */
    public Optional<ClassMapping> findClassMapping(String className) {
        return Optional.ofNullable(classMappings.get(className));
    }

    /**
     * Returns the synthesized sourceRelation for a relational class.
     * Used by MappingResolver to build StoreResolution without touching MappingExpression.
     */
    public com.gs.legend.ast.ValueSpecification findSourceRelation(String className) {
        return findMappingExpression(className)
                .filter(e -> e instanceof ModelContext.MappingExpression.Relational)
                .map(e -> ((ModelContext.MappingExpression.Relational) e).sourceRelation())
                .orElse(null);
    }

    /**
     * Finds the compiler-visible mapping expression for a class.
     * Used by TypeChecker via ModelContext.findMappingExpression() delegation.
     */
    public Optional<ModelContext.MappingExpression> findMappingExpression(String className) {
        return Optional.ofNullable(expressions.get(className));
    }

    /**
     * @return all class mappings in this scope
     */
    public Map<String, ClassMapping> allClassMappings() {
        return classMappings;
    }

    /**
     * Empty snapshot — no mappings, no expressions.
     */
    public static NormalizedMapping empty() {
        return new NormalizedMapping(Map.of(), Map.of());
    }
}
