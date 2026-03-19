package com.gs.legend.model.mapping;
import com.gs.legend.model.store.*;
import com.gs.legend.model.m3.PureClass;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for holding relational and M2M mappings.
 * Thread-safe and designed for runtime lookup of class-to-table mappings.
 */
public final class MappingRegistry {

    private final Map<String, RelationalMapping> mappingsByClassName;
    private final Map<String, RelationalMapping> mappingsByTableName;
    private final Map<String, PureClassMapping> pureClassMappingsByTargetClass;
    private final Map<String, Join> joinsByName;

    public MappingRegistry() {
        this.mappingsByClassName = new ConcurrentHashMap<>();
        this.mappingsByTableName = new ConcurrentHashMap<>();
        this.pureClassMappingsByTargetClass = new ConcurrentHashMap<>();
        this.joinsByName = new ConcurrentHashMap<>();
    }

    /**
     * Registers a relational mapping.
     * Registers by both qualified name and simple name for flexible lookup.
     * 
     * @param mapping The mapping to register
     * @return this registry for fluent API
     */
    public MappingRegistry register(RelationalMapping mapping) {
        String qualifiedClassName = mapping.pureClass().qualifiedName();
        String simpleClassName = mapping.pureClass().name();
        String tableKey = mapping.table().qualifiedName();

        // Register by both qualified and simple name
        mappingsByClassName.put(qualifiedClassName, mapping);
        mappingsByClassName.put(simpleClassName, mapping);
        mappingsByTableName.put(tableKey, mapping);

        return this;
    }



    /**
     * Registers a PureClassMapping (clean AST, replaces M2M).
     */
    public MappingRegistry registerPureClassMapping(PureClassMapping mapping) {
        pureClassMappingsByTargetClass.put(mapping.targetClassName(), mapping);
        return this;
    }

    /**
     * Looks up a PureClassMapping by target class name.
     */
    public Optional<PureClassMapping> findPureClassMapping(String targetClassName) {
        return Optional.ofNullable(pureClassMappingsByTargetClass.get(targetClassName));
    }

    /**
     * Finds any ClassMapping (relational or M2M) by class name.
     * Checks relational mappings first, then PureClassMappings.
     */
    public Optional<ClassMapping> findAnyMapping(String className) {
        RelationalMapping rm = mappingsByClassName.get(className);
        if (rm != null) return Optional.of(rm);
        PureClassMapping pcm = pureClassMappingsByTargetClass.get(className);
        if (pcm != null) return Optional.of(pcm);
        return Optional.empty();
    }

    /**
     * Updates a PureClassMapping with a resolved version (e.g., after chain resolution).
     */
    public void updatePureClassMapping(String targetClassName, PureClassMapping resolved) {
        pureClassMappingsByTargetClass.put(targetClassName, resolved);
    }

    /**
     * Looks up a mapping by Pure class name.
     * 
     * @param className The fully qualified class name
     * @return Optional containing the mapping if found
     */
    public Optional<RelationalMapping> findByClassName(String className) {
        return Optional.ofNullable(mappingsByClassName.get(className));
    }

    /**
     * Looks up a mapping by Pure class.
     * 
     * @param pureClass The Pure class
     * @return Optional containing the mapping if found
     */
    public Optional<RelationalMapping> findByClass(PureClass pureClass) {
        return findByClassName(pureClass.qualifiedName());
    }

    /**
     * Looks up a mapping by table name.
     * 
     * @param tableName The fully qualified table name
     * @return Optional containing the mapping if found
     */
    public Optional<RelationalMapping> findByTableName(String tableName) {
        return Optional.ofNullable(mappingsByTableName.get(tableName));
    }



    /**
     * @param className The fully qualified class name
     * @return The mapping
     * @throws IllegalArgumentException if no mapping exists
     */
    public RelationalMapping getByClassName(String className) {
        return findByClassName(className)
                .orElseThrow(() -> new IllegalArgumentException(
                        "No mapping found for class: " + className));
    }

    /**
     * @return Number of registered relational mappings
     */
    public int size() {
        return mappingsByClassName.size();
    }



    // ==================== Join Registration ====================

    /**
     * Registers a Join by name.
     * 
     * @param join The Join to register
     * @return this registry for fluent API
     */
    public MappingRegistry registerJoin(Join join) {
        joinsByName.put(join.name(), join);
        return this;
    }

    /**
     * Looks up a Join by name.
     * 
     * @param joinName The join name
     * @return Optional containing the Join if found
     */
    public Optional<Join> findJoin(String joinName) {
        return Optional.ofNullable(joinsByName.get(joinName));
    }

    /**
     * Gets a Join or throws.
     * 
     * @param joinName The join name
     * @return The Join
     * @throws IllegalArgumentException if no join exists
     */
    public Join getJoin(String joinName) {
        return findJoin(joinName)
                .orElseThrow(() -> new IllegalArgumentException(
                        "No join found with name: " + joinName));
    }

    /**
     * Finds a Join that connects two tables (by name, in either direction).
     */
    public Optional<Join> findJoinBetweenTables(String table1, String table2) {
        for (Join join : joinsByName.values()) {
            if ((join.leftTable().equals(table1) && join.rightTable().equals(table2))
                    || (join.leftTable().equals(table2) && join.rightTable().equals(table1))) {
                return Optional.of(join);
            }
        }
        return Optional.empty();
    }

    /**
     * Clears all registered mappings.
     */
    public void clear() {
        mappingsByClassName.clear();
        mappingsByTableName.clear();
        pureClassMappingsByTargetClass.clear();
        joinsByName.clear();
    }
}
