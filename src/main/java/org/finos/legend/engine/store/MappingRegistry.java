package org.finos.legend.engine.store;

import org.finos.legend.pure.m3.PureClass;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for holding relational mappings.
 * Thread-safe and designed for runtime lookup of class-to-table mappings.
 */
public final class MappingRegistry {
    
    private final Map<String, RelationalMapping> mappingsByClassName;
    private final Map<String, RelationalMapping> mappingsByTableName;
    
    public MappingRegistry() {
        this.mappingsByClassName = new ConcurrentHashMap<>();
        this.mappingsByTableName = new ConcurrentHashMap<>();
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
     * @return Number of registered mappings
     */
    public int size() {
        return mappingsByClassName.size();
    }
    
    /**
     * Clears all registered mappings.
     */
    public void clear() {
        mappingsByClassName.clear();
        mappingsByTableName.clear();
    }
}
