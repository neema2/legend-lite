package com.gs.legend.compiler;

import com.gs.legend.model.ModelContext;
import com.gs.legend.model.mapping.ClassMapping;
import com.gs.legend.model.store.Join;

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
 *   <li><b>MappingResolver</b> sees {@link #findClassMapping(String)} and
 *       {@link #findJoin(String)} — read-only</li>
 *   <li>Neither sees {@code MappingRegistry} directly</li>
 * </ul>
 */
public final class NormalizedMapping {

    private final Map<String, ClassMapping> classMappings;
    private final Map<String, ModelContext.MappingExpression> expressions;
    private final Map<String, Join> joins;

    public NormalizedMapping(
            Map<String, ClassMapping> classMappings,
            Map<String, ModelContext.MappingExpression> expressions,
            Map<String, Join> joins) {
        this.classMappings = Map.copyOf(classMappings);
        this.expressions = Map.copyOf(expressions);
        this.joins = Map.copyOf(joins);
    }

    /**
     * Finds a fully-resolved class mapping by class name.
     * Used by MappingResolver (replaces registry.findAnyMapping).
     */
    public Optional<ClassMapping> findClassMapping(String className) {
        return Optional.ofNullable(classMappings.get(className));
    }

    /**
     * Finds a join by name.
     * Used by MappingResolver (replaces registry.findJoin).
     */
    public Optional<Join> findJoin(String joinName) {
        return Optional.ofNullable(joins.get(joinName));
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
     * Empty snapshot — no mappings, no joins.
     */
    public static NormalizedMapping empty() {
        return new NormalizedMapping(Map.of(), Map.of(), Map.of());
    }
}
