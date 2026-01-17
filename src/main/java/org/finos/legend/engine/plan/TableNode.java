package org.finos.legend.engine.plan;

import org.finos.legend.engine.store.Table;

import java.util.Objects;

/**
 * Represents a physical table scan in the relational algebra.
 * This corresponds to the FROM clause in SQL.
 * 
 * @param table The physical table to scan
 * @param alias The table alias for SQL generation (e.g., "t0")
 */
public record TableNode(
        Table table,
        String alias
) implements RelationNode {
    
    public TableNode {
        Objects.requireNonNull(table, "Table cannot be null");
        Objects.requireNonNull(alias, "Alias cannot be null");
        
        if (alias.isBlank()) {
            throw new IllegalArgumentException("Alias cannot be blank");
        }
    }
    
    /**
     * Creates a TableNode with auto-generated alias.
     */
    public TableNode(Table table) {
        this(table, "t0");
    }
    
    @Override
    public <T> T accept(RelationNodeVisitor<T> visitor) {
        return visitor.visit(this);
    }
    
    @Override
    public String toString() {
        return "TableNode(" + table.qualifiedName() + " AS " + alias + ")";
    }
}
