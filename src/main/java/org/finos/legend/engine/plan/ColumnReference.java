package org.finos.legend.engine.plan;

import java.util.Objects;

/**
 * Represents a reference to a column in the logical plan.
 * 
 * @param tableAlias The alias of the table containing the column
 * @param columnName The column name
 */
public record ColumnReference(
        String tableAlias,
        String columnName
) implements Expression {
    
    public ColumnReference {
        Objects.requireNonNull(tableAlias, "Table alias cannot be null");
        Objects.requireNonNull(columnName, "Column name cannot be null");
    }
    
    /**
     * Creates a column reference without a table alias.
     */
    public static ColumnReference of(String columnName) {
        return new ColumnReference("", columnName);
    }
    
    /**
     * Creates a column reference with a table alias.
     */
    public static ColumnReference of(String tableAlias, String columnName) {
        return new ColumnReference(tableAlias, columnName);
    }
    
    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitColumnReference(this);
    }
    
    @Override
    public String toString() {
        return tableAlias.isEmpty() ? columnName : tableAlias + "." + columnName;
    }
}
