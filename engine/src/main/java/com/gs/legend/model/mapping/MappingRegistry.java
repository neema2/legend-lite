package com.gs.legend.model.mapping;

import com.gs.legend.model.SymbolTable;
import com.gs.legend.model.store.Join;

import java.util.ArrayList;
import java.util.HashMap;
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
 * <p>All class-name-keyed maps use integer IDs from {@link SymbolTable} —
 * eliminates the dual-key (FQN + simpleName) antipattern.
 */
public final class MappingRegistry {

    private final SymbolTable symbols;

    // Flat maps — classId → mapping (searched across all scopes)
    private final Map<Integer, RelationalMapping> relationalByClass = new HashMap<>();
    private final Map<Integer, PureClassMapping> pureByTargetClass = new HashMap<>();
    private final Map<String, Join> joinsByName = new HashMap<>();

    // Multi-mapping support — classId → all mappings for that class
    private final Map<Integer, List<RelationalMapping>> relationalMultiByClass = new HashMap<>();
    // Set ID index — setId → mapping (arbitrary strings, not packageable elements)
    private final Map<String, RelationalMapping> relationalBySetId = new HashMap<>();

    // Scoped maps — mappingId → (classId → mapping)
    private final Map<Integer, Map<Integer, RelationalMapping>> scopedRelational = new HashMap<>();
    private final Map<Integer, Map<Integer, PureClassMapping>> scopedPure = new HashMap<>();

    public MappingRegistry(SymbolTable symbols) {
        this.symbols = symbols;
    }

    // ==================== Registration (PureModelBuilder) ====================

    /**
     * Registers a relational mapping under a mapping name scope and flat index.
     * Single int-keyed put per map — no dual-key (FQN + simpleName).
     */
    public void register(String mappingName, RelationalMapping mapping) {
        int classId = symbols.resolveId(mapping.pureClass().qualifiedName());
        int mappingId = symbols.resolveId(mappingName);

        // Scoped — root mapping wins, otherwise first registered
        var scope = scopedRelational.computeIfAbsent(mappingId, k -> new HashMap<>());
        if (mapping.isRoot() || !scope.containsKey(classId)) {
            scope.put(classId, mapping);
        }

        // Flat — root mapping wins, otherwise first registered
        if (mapping.isRoot() || !relationalByClass.containsKey(classId)) {
            relationalByClass.put(classId, mapping);
        }

        // Multi-mapping list
        relationalMultiByClass.computeIfAbsent(classId, k -> new ArrayList<>()).add(mapping);

        // Set ID index
        if (mapping.setId() != null) {
            relationalBySetId.put(mapping.setId(), mapping);
        }
    }

    /**
     * Registers a PureClassMapping under a mapping name scope and flat index.
     */
    public void registerPureClassMapping(String mappingName, PureClassMapping mapping) {
        int classId = symbols.resolveId(mapping.targetClassName());
        int mappingId = symbols.resolveId(mappingName);

        // Scoped
        var scope = scopedPure.computeIfAbsent(mappingId, k -> new HashMap<>());
        scope.put(classId, mapping);

        // Flat
        pureByTargetClass.put(classId, mapping);
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
        int id = symbols.resolveId(className);
        if (id < 0) return Optional.empty();
        var mappings = relationalMultiByClass.get(id);
        if (mappings == null || mappings.isEmpty()) return Optional.empty();
        if (mappings.size() == 1) return Optional.of(mappings.get(0));
        return mappings.stream().filter(RelationalMapping::isRoot).findFirst();
    }

    /**
     * Returns all relational mappings for a class name (multiple set IDs).
     */
    public List<RelationalMapping> findAllByClassName(String className) {
        int id = symbols.resolveId(className);
        if (id < 0) return List.of();
        return relationalMultiByClass.getOrDefault(id, List.of());
    }

    // ==================== Flat Lookups (PureModelBuilder) ====================

    /**
     * Finds a relational mapping by class name (simple or qualified).
     */
    public Optional<RelationalMapping> findByClassName(String className) {
        int id = symbols.resolveId(className);
        return id >= 0 ? Optional.ofNullable(relationalByClass.get(id)) : Optional.empty();
    }

    /**
     * Finds a PureClassMapping by target class name.
     */
    public Optional<PureClassMapping> findPureClassMapping(String targetClassName) {
        int id = symbols.resolveId(targetClassName);
        return id >= 0 ? Optional.ofNullable(pureByTargetClass.get(id)) : Optional.empty();
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
     * Keys are integer IDs from SymbolTable.
     */
    public Map<Integer, ClassMapping> getAllClassMappings(String mappingName) {
        int mappingId = symbols.resolveId(mappingName);
        if (mappingId < 0) return Map.of();
        Map<Integer, ClassMapping> result = new HashMap<>();
        var relScope = scopedRelational.get(mappingId);
        if (relScope != null) result.putAll(relScope);
        var pureScope = scopedPure.get(mappingId);
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
