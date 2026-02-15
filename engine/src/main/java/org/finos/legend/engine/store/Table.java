package org.finos.legend.engine.store;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a physical relational table.
 * 
 * @param schema The database schema (can be empty for default schema)
 * @param name The table name
 * @param columns Immutable list of columns
 */
public record Table(
        String schema,
        String name,
        List<Column> columns
) {
    public Table {
        Objects.requireNonNull(schema, "Schema cannot be null (use empty string for default)");
        Objects.requireNonNull(name, "Table name cannot be null");
        Objects.requireNonNull(columns, "Columns cannot be null");
        
        if (name.isBlank()) {
            throw new IllegalArgumentException("Table name cannot be blank");
        }
        
        // Ensure immutability
        columns = List.copyOf(columns);
    }
    
    /**
     * Creates a table in the default schema.
     */
    public Table(String name, List<Column> columns) {
        this("", name, columns);
    }
    
    /**
     * @return The fully qualified table name (schema.table or just table)
     */
    public String qualifiedName() {
        return schema.isEmpty() ? name : schema + "." + name;
    }
    
    /**
     * Finds a column by name.
     * 
     * @param columnName The column name to search for
     * @return Optional containing the column if found
     */
    public Optional<Column> findColumn(String columnName) {
        return columns.stream()
                .filter(c -> c.name().equals(columnName))
                .findFirst();
    }
    
    /**
     * @param columnName The column name to look up
     * @return The column
     * @throws IllegalArgumentException if column not found
     */
    public Column getColumn(String columnName) {
        return findColumn(columnName)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Column '" + columnName + "' not found in table " + qualifiedName()));
    }

    /**
     * Returns the Pure GenericType for a column.
     * @throws IllegalArgumentException if column not found
     */
    public org.finos.legend.engine.plan.GenericType getColumnType(String columnName) {
        return findColumn(columnName)
                .map(c -> c.dataType().toGenericType())
                .orElseThrow(() -> new IllegalArgumentException(
                        "Column '" + columnName + "' not found in table " + qualifiedName()));
    }
}
