package com.gs.legend.compiler;

import com.gs.legend.model.ModelContext;
import com.gs.legend.model.SymbolTable;
import com.gs.legend.model.mapping.ClassMapping;

import java.util.LinkedHashMap;
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
 * <p>All lookups resolve through {@link SymbolTable#resolveId(String)} —
 * both FQN and simple name lookups work without dual-key storage.
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

    private final SymbolTable symbols;
    private final Map<Integer, ClassMapping> classMappings;
    private final Map<Integer, ModelContext.MappingExpression> expressions;

    public NormalizedMapping(
            SymbolTable symbols,
            Map<Integer, ClassMapping> classMappings,
            Map<Integer, ModelContext.MappingExpression> expressions) {
        this.symbols = symbols;
        this.classMappings = Map.copyOf(classMappings);
        this.expressions = Map.copyOf(expressions);
    }

    /**
     * Finds a fully-resolved class mapping by class name (simple or qualified).
     * Used by MappingResolver (replaces registry.findAnyMapping).
     */
    public Optional<ClassMapping> findClassMapping(String className) {
        int id = symbols.resolveId(className);
        return id >= 0 ? Optional.ofNullable(classMappings.get(id)) : Optional.empty();
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
        int id = symbols.resolveId(className);
        return id >= 0 ? Optional.ofNullable(expressions.get(id)) : Optional.empty();
    }

    /**
     * @return true if this snapshot contains any class mappings
     */
    public boolean hasClassMappings() {
        return !classMappings.isEmpty();
    }

    /**
     * @return all class mappings in this scope (keyed by FQN)
     */
    public Map<String, ClassMapping> allClassMappings() {
        var result = new LinkedHashMap<String, ClassMapping>(classMappings.size());
        for (var entry : classMappings.entrySet()) {
            result.put(symbols.nameOf(entry.getKey()), entry.getValue());
        }
        return Map.copyOf(result);
    }

    /**
     * Empty snapshot — no mappings, no expressions.
     */
    public static NormalizedMapping empty() {
        return new NormalizedMapping(new SymbolTable(), Map.of(), Map.of());
    }
}
