package org.finos.legend.engine.plan;

import java.util.Objects;

/**
 * Represents a column projection with an optional alias.
 * Supports "Structural Projections" - mapping SQL columns to property names.
 * 
 * @param expression The expression to project (typically a ColumnReference)
 * @param alias The output alias (typically the property name)
 */
public record Projection(
        Expression expression,
        String alias
) {
    public Projection {
        Objects.requireNonNull(expression, "Expression cannot be null");
        Objects.requireNonNull(alias, "Alias cannot be null");
        
        if (alias.isBlank()) {
            throw new IllegalArgumentException("Alias cannot be blank");
        }
    }
    
    /**
     * Creates a projection from a column reference with a property alias.
     * 
     * @param tableAlias The table alias
     * @param columnName The column name
     * @param propertyName The property name to use as alias
     * @return A new Projection
     */
    public static Projection column(String tableAlias, String columnName, String propertyName) {
        return new Projection(
                ColumnReference.of(tableAlias, columnName, GenericType.Primitive.ANY),
                propertyName
        );
    }

    /**
     * Creates a typed projection from a column reference with a property alias.
     */
    public static Projection column(String tableAlias, String columnName, String propertyName, GenericType type) {
        return new Projection(
                ColumnReference.of(tableAlias, columnName, type),
                propertyName
        );
    }
    
    /**
     * Creates a projection from a column reference using the column name as alias.
     */
    public static Projection column(String tableAlias, String columnName) {
        return column(tableAlias, columnName, columnName);
    }
    
    @Override
    public String toString() {
        return expression + " AS " + alias;
    }
}
