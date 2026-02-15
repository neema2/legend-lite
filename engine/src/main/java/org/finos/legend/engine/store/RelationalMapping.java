package org.finos.legend.engine.store;

import org.finos.legend.pure.m3.PureClass;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Links a Pure Class to a relational Table with property-to-column mappings.
 * 
 * @param pureClass        The Pure class being mapped
 * @param table            The target relational table
 * @param propertyMappings Mappings from Pure properties to table columns
 */
public record RelationalMapping(
        PureClass pureClass,
        Table table,
        List<PropertyMapping> propertyMappings) {

    public RelationalMapping {
        Objects.requireNonNull(pureClass, "Pure class cannot be null");
        Objects.requireNonNull(table, "Table cannot be null");
        Objects.requireNonNull(propertyMappings, "Property mappings cannot be null");

        // Ensure immutability
        propertyMappings = List.copyOf(propertyMappings);
    }

    /**
     * Gets the column name for a given property name.
     * 
     * @param propertyName The Pure property name
     * @return Optional containing the column name if mapped
     */
    public Optional<String> getColumnForProperty(String propertyName) {
        return propertyMappings.stream()
                .filter(pm -> pm.propertyName().equals(propertyName))
                .map(PropertyMapping::columnName)
                .findFirst();
    }

    /**
     * Gets the property name for a given column name.
     * 
     * @param columnName The relational column name
     * @return Optional containing the property name if mapped
     */
    public Optional<String> getPropertyForColumn(String columnName) {
        return propertyMappings.stream()
                .filter(pm -> pm.columnName().equals(columnName))
                .map(PropertyMapping::propertyName)
                .findFirst();
    }

    /**
     * @return A map from property names to column names
     */
    public Map<String, String> propertyToColumnMap() {
        return propertyMappings.stream()
                .collect(Collectors.toMap(
                        PropertyMapping::propertyName,
                        PropertyMapping::columnName));
    }

    /**
     * @return A map from column names to property names
     */
    public Map<String, String> columnToPropertyMap() {
        return propertyMappings.stream()
                .collect(Collectors.toMap(
                        PropertyMapping::columnName,
                        PropertyMapping::propertyName));
    }

    /**
     * Gets the property mapping for a given property name.
     * 
     * @param propertyName The Pure property name
     * @return Optional containing the PropertyMapping if found
     */
    public Optional<PropertyMapping> getPropertyMapping(String propertyName) {
        return propertyMappings.stream()
                .filter(pm -> pm.propertyName().equals(propertyName))
                .findFirst();
    }

    /**
     * Gets the GenericType for a given property name by looking up the property's
     * generic type in the Pure class definition.
     * 
     * @param propertyName The Pure property name
     * @return The GenericType for this property
     * @throws IllegalArgumentException if property not found in Pure class
     */
    public org.finos.legend.engine.plan.GenericType pureTypeForProperty(String propertyName) {
        return pureClass.findProperty(propertyName)
                .map(p -> org.finos.legend.engine.plan.GenericType.fromType(p.genericType()))
                .orElseThrow(() -> new IllegalArgumentException(
                        "Property '" + propertyName + "' not found in class " + pureClass.name()));
    }

    @Override
    public String toString() {
        var sb = new StringBuilder();
        sb.append("Mapping ").append(pureClass.qualifiedName())
                .append(" -> ").append(table.qualifiedName()).append(" {\n");
        for (PropertyMapping pm : propertyMappings) {
            sb.append("    ").append(pm).append("\n");
        }
        sb.append("}");
        return sb.toString();
    }
}
