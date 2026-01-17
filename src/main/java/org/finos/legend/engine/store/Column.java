package org.finos.legend.engine.store;

import java.util.Objects;

/**
 * Represents a column in a relational table.
 * 
 * @param name The column name
 * @param dataType The SQL data type (VARCHAR, INTEGER, etc.)
 * @param nullable Whether the column allows NULL values
 */
public record Column(
        String name,
        SqlDataType dataType,
        boolean nullable
) {
    public Column {
        Objects.requireNonNull(name, "Column name cannot be null");
        Objects.requireNonNull(dataType, "Column dataType cannot be null");
        
        if (name.isBlank()) {
            throw new IllegalArgumentException("Column name cannot be blank");
        }
    }
    
    /**
     * Factory for a non-nullable column.
     */
    public static Column required(String name, SqlDataType dataType) {
        return new Column(name, dataType, false);
    }
    
    /**
     * Factory for a nullable column.
     */
    public static Column nullable(String name, SqlDataType dataType) {
        return new Column(name, dataType, true);
    }
}
