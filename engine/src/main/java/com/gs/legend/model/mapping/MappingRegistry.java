package com.gs.legend.model.mapping;

import com.gs.legend.model.store.Join;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Build-time accumulator for mappings and joins.
 *
 * <p>PureModelBuilder writes here during parsing. After parsing,
 * <b>MappingNormalizer</b> reads scoped maps ({@link #getAllClassMappings})
 * and joins ({@link #getAllJoins}) to produce an immutable {@code NormalizedMapping}
 * snapshot. MappingResolver reads from NormalizedMapping, not from this class.
 *
 * <p>All maps are plain HashMaps — this is single-threaded build-time code.
 */
public final class MappingRegistry {

    // Flat maps — class name → mapping (searched across all scopes)
    private final Map<String, RelationalMapping> relationalByClass = new HashMap<>();
    private final Map<String, PureClassMapping> pureByTargetClass = new HashMap<>();
    private final Map<String, Join> joinsByName = new HashMap<>();

    // Multi-mapping support — class name → all mappings for that class
    private final Map<String, List<RelationalMapping>> relationalMultiByClass = new HashMap<>();
    // Set ID index — setId → mapping
    private final Map<String, RelationalMapping> relationalBySetId = new HashMap<>();

    // Scoped maps — mappingName → (className → mapping)
    private final Map<String, Map<String, RelationalMapping>> scopedRelational = new HashMap<>();
    private final Map<String, Map<String, PureClassMapping>> scopedPure = new HashMap<>();

    // ==================== Registration (PureModelBuilder) ====================

    /**
     * Registers a relational mapping under a mapping name scope and flat index.
     */
    public void register(String mappingName, RelationalMapping mapping) {
        String qualifiedName = mapping.pureClass().qualifiedName();
        String simpleName = mapping.pureClass().name();

        // Scoped — root mapping wins, otherwise first registered
        var scope = scopedRelational.computeIfAbsent(mappingName, k -> new HashMap<>());
        if (mapping.isRoot() || !scope.containsKey(qualifiedName)) {
            scope.put(qualifiedName, mapping);
            scope.put(simpleName, mapping);
        }

        // Flat — root mapping wins, otherwise first registered
        if (mapping.isRoot() || !relationalByClass.containsKey(qualifiedName)) {
            relationalByClass.put(qualifiedName, mapping);
            relationalByClass.put(simpleName, mapping);
        }

        // Multi-mapping list
        relationalMultiByClass.computeIfAbsent(qualifiedName, k -> new ArrayList<>()).add(mapping);
        relationalMultiByClass.computeIfAbsent(simpleName, k -> new ArrayList<>()).add(mapping);

        // Set ID index
        if (mapping.setId() != null) {
            relationalBySetId.put(mapping.setId(), mapping);
        }
    }

    /**
     * Registers a PureClassMapping under a mapping name scope and flat index.
     */
    public void registerPureClassMapping(String mappingName, PureClassMapping mapping) {
        // Scoped
        var scope = scopedPure.computeIfAbsent(mappingName, k -> new HashMap<>());
        scope.put(mapping.targetClassName(), mapping);

        // Flat
        pureByTargetClass.put(mapping.targetClassName(), mapping);
    }

    /**
     * Registers a Join by name.
     */
    public void registerJoin(Join join) {
        joinsByName.put(join.name(), join);
    }

    // ==================== Set ID / Root Lookups ====================

    /**
     * Finds a relational mapping by set ID.
     */
    public Optional<RelationalMapping> findBySetId(String setId) {
        return Optional.ofNullable(relationalBySetId.get(setId));
    }

    /**
     * Finds the root mapping for a class (the one marked with *).
     * Falls back to the single mapping if only one exists.
     */
    public Optional<RelationalMapping> findRootMapping(String className) {
        var mappings = relationalMultiByClass.get(className);
        if (mappings == null || mappings.isEmpty()) return Optional.empty();
        if (mappings.size() == 1) return Optional.of(mappings.get(0));
        return mappings.stream().filter(RelationalMapping::isRoot).findFirst();
    }

    /**
     * Returns all relational mappings for a class name (multiple set IDs).
     */
    public List<RelationalMapping> findAllByClassName(String className) {
        return relationalMultiByClass.getOrDefault(className, List.of());
    }

    // ==================== Flat Lookups (PureModelBuilder) ====================

    /**
     * Finds a relational mapping by class name.
     */
    public Optional<RelationalMapping> findByClassName(String className) {
        return Optional.ofNullable(relationalByClass.get(className));
    }

    /**
     * Finds a PureClassMapping by target class name.
     */
    public Optional<PureClassMapping> findPureClassMapping(String targetClassName) {
        return Optional.ofNullable(pureByTargetClass.get(targetClassName));
    }

    /**
     * Gets a relational mapping or throws.
     * Used by legacy tests — prefer findByClassName for production code.
     */
    public RelationalMapping getByClassName(String className) {
        return findByClassName(className)
                .orElseThrow(() -> new IllegalArgumentException(
                        "No mapping found for class: " + className));
    }

    /**
     * Finds a Join by name.
     */
    public Optional<Join> findJoin(String joinName) {
        return Optional.ofNullable(joinsByName.get(joinName));
    }

    // ==================== Scoped Lookups (MappingNormalizer) ====================

    /**
     * Returns all class mappings (relational + M2M) under a mapping name.
     */
    public Map<String, ClassMapping> getAllClassMappings(String mappingName) {
        Map<String, ClassMapping> result = new LinkedHashMap<>();
        var relScope = scopedRelational.get(mappingName);
        if (relScope != null) result.putAll(relScope);
        var pureScope = scopedPure.get(mappingName);
        if (pureScope != null) result.putAll(pureScope);
        return result;
    }

    /**
     * Returns all registered joins.
     */
    public Map<String, Join> getAllJoins() {
        return Map.copyOf(joinsByName);
    }
}
