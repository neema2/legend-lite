package org.finos.legend.engine.plan;

import java.util.Objects;

/**
 * Represents a reference to a column in the logical plan.
 * 
 * @param tableAlias The alias of the table containing the column
 * @param columnName The column name
 * @param sqlType    The SQL type of this column (for type-aware SQL generation)
 */
public record ColumnReference(
        String tableAlias,
        String columnName,
        SqlType sqlType) implements Expression {

    public ColumnReference {
        Objects.requireNonNull(tableAlias, "Table alias cannot be null");
        Objects.requireNonNull(columnName, "Column name cannot be null");
        Objects.requireNonNull(sqlType, "SQL type cannot be null");
    }

    /**
     * Creates a column reference without a table alias (UNKNOWN type).
     */
    public static ColumnReference of(String columnName) {
        return new ColumnReference("", columnName, SqlType.UNKNOWN);
    }

    /**
     * Creates a column reference with a table alias (UNKNOWN type).
     */
    public static ColumnReference of(String tableAlias, String columnName) {
        return new ColumnReference(tableAlias, columnName, SqlType.UNKNOWN);
    }

    /**
     * Creates a column reference with explicit type.
     */
    public static ColumnReference of(String tableAlias, String columnName, SqlType type) {
        return new ColumnReference(tableAlias, columnName, type);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitColumnReference(this);
    }

    @Override
    public SqlType type() {
        return sqlType;
    }

    @Override
    public String toString() {
        return tableAlias.isEmpty() ? columnName : tableAlias + "." + columnName;
    }
}
