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
 *   <li><b>MappingResolver</b> sees {@link #findClassMapping(String)},
 *       {@link #findSourceRelation(String)}, {@link #findAssociationJoins(String)},
 *       and {@link #findM2MAssociationNavigations(String)} — read-only</li>
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
     * Returns pre-resolved association joins for a relational class.
     * Key = property name, value = AssociationJoinInfo (targetClassName, traversal, isToMany).
     * Used by MappingResolver to build JoinResolutions.
     */
    public java.util.Map<String, ModelContext.AssociationJoinInfo> findAssociationJoins(String className) {
        return findMappingExpression(className)
                .filter(e -> e instanceof ModelContext.MappingExpression.Relational)
                .map(e -> ((ModelContext.MappingExpression.Relational) e).associationJoins())
                .orElse(java.util.Map.of());
    }

    /**
     * Returns pre-resolved association navigations for an M2M class.
     * Key = M2M property name, value = AssociationJoinInfo from the source class's relational joins.
     * Used by MappingResolver to build JoinResolutions.
     */
    public java.util.Map<String, ModelContext.AssociationJoinInfo> findM2MAssociationNavigations(String className) {
        return findMappingExpression(className)
                .filter(e -> e instanceof ModelContext.MappingExpression.M2M)
                .map(e -> ((ModelContext.MappingExpression.M2M) e).associationNavigations())
                .orElse(java.util.Map.of());
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
