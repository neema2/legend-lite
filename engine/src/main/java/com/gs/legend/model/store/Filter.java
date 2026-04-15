package com.gs.legend.model.store;

import com.gs.legend.model.def.RelationalOperation;

import java.util.Objects;

/**
 * Represents a named filter within a Database definition.
 * 
 * Pure syntax: {@code Filter FilterName(dbOperation)}
 * 
 * Referenced from class mappings via {@code ~filter [DB]filterName} or
 * {@code ~filter [DB]@Join1 | [DB]filterName}.
 * 
 * @param db        The parent database FQN (e.g., "store::DB"). Never empty.
 * @param name      The filter name
 * @param condition The filter condition expression tree
 */
public record Filter(
        String db,
        String name,
        RelationalOperation condition
) {
    public Filter {
        Objects.requireNonNull(db, "Database FQN cannot be null");
        if (db.isBlank()) throw new IllegalArgumentException("Database FQN cannot be blank");
        Objects.requireNonNull(name, "Filter name cannot be null");
        Objects.requireNonNull(condition, "Filter condition cannot be null");
    }

    /**
     * @return The database-local name (just the filter name).
     */
    public String dbName() {
        return name;
    }

    /**
     * @return The fully qualified name: db + "." + name. Used as SymbolTable key.
     */
    public String qualifiedName() {
        return db + "." + name;
    }
}
