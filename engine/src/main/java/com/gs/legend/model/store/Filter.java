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
 * @param name      The filter name
 * @param condition The filter condition expression tree
 */
public record Filter(
        String name,
        RelationalOperation condition
) {
    public Filter {
        Objects.requireNonNull(name, "Filter name cannot be null");
        Objects.requireNonNull(condition, "Filter condition cannot be null");
    }
}
