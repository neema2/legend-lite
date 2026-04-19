package com.gs.legend.model.store;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a physical relational table.
 * 
 * @param db     The parent database FQN (e.g., "store::DB"). Never empty.
 * @param schema The database schema (can be empty for default schema)
 * @param name   The table name
 * @param columns Immutable list of columns
 */
public record Table(
        String db,
        String schema,
        String name,
        List<Column> columns
) {
    public Table {
        Objects.requireNonNull(db, "Database FQN cannot be null");
        if (db.isBlank()) throw new IllegalArgumentException("Database FQN cannot be blank");
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
    public Table(String db, String name, List<Column> columns) {
        this(db, "", name, columns);
    }
    
    /**
     * @return The database-local name (schema.table or just table). Used in SQL and join matching.
     */
    public String dbName() {
        return schema.isEmpty() ? name : schema + "." + name;
    }

    /**
     * @return The fully qualified name: db + "." + dbName(). Used as SymbolTable key and for all lookups.
     */
    public String qualifiedName() {
        return db + "." + dbName();
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
                        "Column '" + columnName + "' not found in table " + dbName()));
    }

    /**
     * Returns the Pure Type for a column.
     * @throws IllegalArgumentException if column not found
     */
    public com.gs.legend.model.m3.Type getColumnType(String columnName) {
        return findColumn(columnName)
                .map(c -> c.dataType().toGenericType())
                .orElseThrow(() -> new IllegalArgumentException(
                        "Column '" + columnName + "' not found in table " + dbName()));
    }
}
