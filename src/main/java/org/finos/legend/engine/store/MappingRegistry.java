package org.finos.legend.engine.store;

import org.finos.legend.pure.dsl.m2m.M2MClassMapping;
import org.finos.legend.pure.m3.PureClass;

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
    private final Map<String, M2MClassMapping> m2mMappingsByTargetClass;

    public MappingRegistry() {
        this.mappingsByClassName = new ConcurrentHashMap<>();
        this.mappingsByTableName = new ConcurrentHashMap<>();
        this.m2mMappingsByTargetClass = new ConcurrentHashMap<>();
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
     * Registers an M2M class mapping.
     * 
     * @param mapping The M2M mapping to register
     * @return this registry for fluent API
     */
    public MappingRegistry registerM2M(M2MClassMapping mapping) {
        m2mMappingsByTargetClass.put(mapping.targetClassName(), mapping);
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
     * Checks if a class is M2M-mapped.
     * 
     * @param className The class name (simple or qualified)
     * @return true if the class has an M2M mapping
     */
    public boolean isM2MMapped(String className) {
        return m2mMappingsByTargetClass.containsKey(className);
    }

    /**
     * Looks up an M2M mapping by target class name.
     * 
     * @param targetClassName The target class name
     * @return Optional containing the M2M mapping if found
     */
    public Optional<M2MClassMapping> findM2MMapping(String targetClassName) {
        return Optional.ofNullable(m2mMappingsByTargetClass.get(targetClassName));
    }

    /**
     * Gets an M2M mapping or throws.
     * 
     * @param targetClassName The target class name
     * @return The M2M mapping
     * @throws IllegalArgumentException if no mapping exists
     */
    public M2MClassMapping getM2MMapping(String targetClassName) {
        return findM2MMapping(targetClassName)
                .orElseThrow(() -> new IllegalArgumentException(
                        "No M2M mapping found for class: " + targetClassName));
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

    /**
     * @return Number of registered M2M mappings
     */
    public int m2mSize() {
        return m2mMappingsByTargetClass.size();
    }

    /**
     * Clears all registered mappings.
     */
    public void clear() {
        mappingsByClassName.clear();
        mappingsByTableName.clear();
        m2mMappingsByTargetClass.clear();
    }
}
