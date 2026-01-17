package org.finos.legend.pure.dsl.definition;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a Pure Mapping definition.
 * 
 * Pure syntax:
 * <pre>
 * Mapping package::MappingName
 * (
 *     ClassName: Relational
 *     {
 *         ~mainTable [DatabaseName] TABLE_NAME
 *         propertyName: [DatabaseName] TABLE_NAME.COLUMN_NAME,
 *         ...
 *     }
 * )
 * </pre>
 * 
 * Example:
 * <pre>
 * Mapping model::PersonMapping
 * (
 *     Person: Relational
 *     {
 *         ~mainTable [MyDatabase] T_PERSON
 *         firstName: [MyDatabase] T_PERSON.FIRST_NAME,
 *         lastName: [MyDatabase] T_PERSON.LAST_NAME,
 *         age: [MyDatabase] T_PERSON.AGE_VAL
 *     }
 * )
 * </pre>
 * 
 * @param qualifiedName The fully qualified mapping name
 * @param classMappings The list of class mappings
 */
public record MappingDefinition(
        String qualifiedName,
        List<ClassMappingDefinition> classMappings
) implements PureDefinition {
    
    public MappingDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        Objects.requireNonNull(classMappings, "Class mappings cannot be null");
        classMappings = List.copyOf(classMappings);
    }
    
    /**
     * @return The simple mapping name (without package)
     */
    public String simpleName() {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(idx + 2) : qualifiedName;
    }
    
    /**
     * Finds a class mapping by class name.
     */
    public Optional<ClassMappingDefinition> findClassMapping(String className) {
        return classMappings.stream()
                .filter(cm -> cm.className().equals(className))
                .findFirst();
    }
    
    /**
     * Represents a class mapping within a mapping.
     * 
     * @param className The class being mapped
     * @param mappingType The mapping type (e.g., "Relational")
     * @param mainTable The main table reference (optional)
     * @param propertyMappings The property-to-column mappings
     */
    public record ClassMappingDefinition(
            String className,
            String mappingType,
            TableReference mainTable,
            List<PropertyMappingDefinition> propertyMappings
    ) {
        public ClassMappingDefinition {
            Objects.requireNonNull(className, "Class name cannot be null");
            Objects.requireNonNull(mappingType, "Mapping type cannot be null");
            Objects.requireNonNull(propertyMappings, "Property mappings cannot be null");
            propertyMappings = List.copyOf(propertyMappings);
        }
        
        /**
         * Finds a property mapping by property name.
         */
        public Optional<PropertyMappingDefinition> findPropertyMapping(String propertyName) {
            return propertyMappings.stream()
                    .filter(pm -> pm.propertyName().equals(propertyName))
                    .findFirst();
        }
    }
    
    /**
     * Represents a table reference [DatabaseName] TABLE_NAME.
     * 
     * @param databaseName The database name
     * @param tableName The table name
     */
    public record TableReference(
            String databaseName,
            String tableName
    ) {
        public TableReference {
            Objects.requireNonNull(databaseName, "Database name cannot be null");
            Objects.requireNonNull(tableName, "Table name cannot be null");
        }
    }
    
    /**
     * Represents a property mapping within a class mapping.
     * 
     * @param propertyName The Pure property name
     * @param columnReference The column reference
     */
    public record PropertyMappingDefinition(
            String propertyName,
            ColumnReference columnReference
    ) {
        public PropertyMappingDefinition {
            Objects.requireNonNull(propertyName, "Property name cannot be null");
            Objects.requireNonNull(columnReference, "Column reference cannot be null");
        }
    }
    
    /**
     * Represents a column reference [DatabaseName] TABLE_NAME.COLUMN_NAME.
     * 
     * @param databaseName The database name
     * @param tableName The table name
     * @param columnName The column name
     */
    public record ColumnReference(
            String databaseName,
            String tableName,
            String columnName
    ) {
        public ColumnReference {
            Objects.requireNonNull(databaseName, "Database name cannot be null");
            Objects.requireNonNull(tableName, "Table name cannot be null");
            Objects.requireNonNull(columnName, "Column name cannot be null");
        }
    }
}
