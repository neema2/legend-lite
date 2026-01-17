package org.finos.legend.engine.store;

import java.util.Objects;

/**
 * Maps a Pure property to a relational column.
 * 
 * @param propertyName The Pure property name (e.g., "firstName")
 * @param columnName The relational column name (e.g., "FIRST_NAME")
 */
public record PropertyMapping(
        String propertyName,
        String columnName
) {
    public PropertyMapping {
        Objects.requireNonNull(propertyName, "Property name cannot be null");
        Objects.requireNonNull(columnName, "Column name cannot be null");
        
        if (propertyName.isBlank()) {
            throw new IllegalArgumentException("Property name cannot be blank");
        }
        if (columnName.isBlank()) {
            throw new IllegalArgumentException("Column name cannot be blank");
        }
    }
    
    @Override
    public String toString() {
        return propertyName + " -> " + columnName;
    }
}
