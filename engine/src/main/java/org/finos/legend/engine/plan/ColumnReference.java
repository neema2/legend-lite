package org.finos.legend.engine.plan;

import java.util.Objects;

/**
 * Represents a reference to a column in the logical plan.
 * 
 * @param tableAlias The alias of the table containing the column
 * @param columnName The column name
 * @param pureType   The Pure type of this column (for type-aware compilation)
 */
public record ColumnReference(
        String tableAlias,
        String columnName,
        GenericType pureType) implements Expression {

    public ColumnReference {
        Objects.requireNonNull(tableAlias, "Table alias cannot be null");
        Objects.requireNonNull(columnName, "Column name cannot be null");
        Objects.requireNonNull(pureType, "Pure type cannot be null");
    }

    /**
     * Creates a column reference with explicit type. Every call site must provide a type.
     */
    public static ColumnReference of(String tableAlias, String columnName, GenericType type) {
        return new ColumnReference(tableAlias, columnName, type);
    }

    /**
     * Creates a column reference without a table alias, with explicit type.
     */
    public static ColumnReference of(String columnName, GenericType type) {
        return new ColumnReference("", columnName, type);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitColumnReference(this);
    }

    @Override
    public GenericType type() {
        return pureType;
    }

    @Override
    public String toString() {
        return tableAlias.isEmpty() ? columnName : tableAlias + "." + columnName;
    }
}
