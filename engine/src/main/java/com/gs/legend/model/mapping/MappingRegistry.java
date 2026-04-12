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
    private final ArrayList<RelationalMapping> relationalByClass = new ArrayList<>();
    private final ArrayList<PureClassMapping> pureByTargetClass = new ArrayList<>();
    private final Map<String, Join> joinsByName = new HashMap<>();

    // Multi-mapping support — classId → all mappings for that class
    private final ArrayList<List<RelationalMapping>> relationalMultiByClass = new ArrayList<>();
    // Set ID index — setId → mapping (arbitrary strings, not packageable elements)
    private final Map<String, RelationalMapping> relationalBySetId = new HashMap<>();

    // Scoped maps — mappingId → (classId → mapping)
    private final ArrayList<Map<Integer, RelationalMapping>> scopedRelational = new ArrayList<>();
    private final ArrayList<Map<Integer, PureClassMapping>> scopedPure = new ArrayList<>();

    // Primitive-int-indexed helpers — zero boxing, O(1) access
    private static <T> T idGet(ArrayList<T> list, int id) {
        return id >= 0 && id < list.size() ? list.get(id) : null;
    }
    private static <T> void idPut(ArrayList<T> list, int id, T value) {
        int gap = id - list.size() + 1;
        if (gap > 0) list.addAll(java.util.Collections.nCopies(gap, null));
        list.set(id, value);
    }

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
        Map<Integer, RelationalMapping> scope = idGet(scopedRelational, mappingId);
        if (scope == null) { scope = new HashMap<>(); idPut(scopedRelational, mappingId, scope); }
        if (mapping.isRoot() || !scope.containsKey(classId)) {
            scope.put(classId, mapping);
        }

        // Flat — root mapping wins, otherwise first registered
        if (mapping.isRoot() || idGet(relationalByClass, classId) == null) {
            idPut(relationalByClass, classId, mapping);
        }

        // Multi-mapping list
        List<RelationalMapping> multiList = idGet(relationalMultiByClass, classId);
        if (multiList == null) { multiList = new ArrayList<>(); idPut(relationalMultiByClass, classId, multiList); }
        multiList.add(mapping);

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
        Map<Integer, PureClassMapping> scope = idGet(scopedPure, mappingId);
        if (scope == null) { scope = new HashMap<>(); idPut(scopedPure, mappingId, scope); }
        scope.put(classId, mapping);

        // Flat
        idPut(pureByTargetClass, classId, mapping);
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
        var mappings = idGet(relationalMultiByClass, id);
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
        List<RelationalMapping> list = idGet(relationalMultiByClass, id);
        return list != null ? list : List.of();
    }

    // ==================== Flat Lookups (PureModelBuilder) ====================

    /**
     * Finds a relational mapping by class name (simple or qualified).
     */
    public Optional<RelationalMapping> findByClassName(String className) {
        int id = symbols.resolveId(className);
        return id >= 0 ? Optional.ofNullable(idGet(relationalByClass, id)) : Optional.empty();
    }

    /**
     * Finds a PureClassMapping by target class name.
     */
    public Optional<PureClassMapping> findPureClassMapping(String targetClassName) {
        int id = symbols.resolveId(targetClassName);
        return id >= 0 ? Optional.ofNullable(idGet(pureByTargetClass, id)) : Optional.empty();
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
        var relScope = idGet(scopedRelational, mappingId);
        if (relScope != null) result.putAll(relScope);
        var pureScope = idGet(scopedPure, mappingId);
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
