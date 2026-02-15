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
     * Creates a typed projection from a column reference with a property alias.
     */
    public static Projection column(String tableAlias, String columnName, String propertyName, GenericType type) {
        return new Projection(
                ColumnReference.of(tableAlias, columnName, type),
                propertyName
        );
    }
    
    @Override
    public String toString() {
        return expression + " AS " + alias;
    }
}
