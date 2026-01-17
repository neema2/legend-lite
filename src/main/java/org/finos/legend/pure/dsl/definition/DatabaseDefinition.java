package org.finos.legend.pure.dsl.definition;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a Pure Database (Store) definition.
 * 
 * Pure syntax:
 * <pre>
 * Database package::DatabaseName
 * (
 *     Table TABLE_NAME
 *     (
 *         COLUMN_NAME DATA_TYPE [PRIMARY KEY] [NOT NULL],
 *         ...
 *     )
 * )
 * </pre>
 * 
 * Example:
 * <pre>
 * Database store::MyDatabase
 * (
 *     Table T_PERSON
 *     (
 *         ID INTEGER PRIMARY KEY,
 *         FIRST_NAME VARCHAR(100) NOT NULL,
 *         LAST_NAME VARCHAR(100) NOT NULL,
 *         AGE_VAL INTEGER
 *     )
 * )
 * </pre>
 * 
 * @param qualifiedName The fully qualified database name
 * @param tables The list of table definitions
 */
public record DatabaseDefinition(
        String qualifiedName,
        List<TableDefinition> tables
) implements PureDefinition {
    
    public DatabaseDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        Objects.requireNonNull(tables, "Tables cannot be null");
        tables = List.copyOf(tables);
    }
    
    /**
     * @return The simple database name (without package)
     */
    public String simpleName() {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(idx + 2) : qualifiedName;
    }
    
    /**
     * Finds a table by name.
     */
    public Optional<TableDefinition> findTable(String tableName) {
        return tables.stream()
                .filter(t -> t.name().equals(tableName))
                .findFirst();
    }
    
    /**
     * Represents a table definition within a database.
     * 
     * @param name The table name
     * @param columns The list of column definitions
     */
    public record TableDefinition(
            String name,
            List<ColumnDefinition> columns
    ) {
        public TableDefinition {
            Objects.requireNonNull(name, "Table name cannot be null");
            Objects.requireNonNull(columns, "Columns cannot be null");
            columns = List.copyOf(columns);
        }
        
        /**
         * Finds a column by name.
         */
        public Optional<ColumnDefinition> findColumn(String columnName) {
            return columns.stream()
                    .filter(c -> c.name().equals(columnName))
                    .findFirst();
        }
    }
    
    /**
     * Represents a column definition within a table.
     * 
     * @param name The column name
     * @param dataType The SQL data type (e.g., "INTEGER", "VARCHAR(100)")
     * @param primaryKey Whether this column is part of the primary key
     * @param notNull Whether this column is NOT NULL
     */
    public record ColumnDefinition(
            String name,
            String dataType,
            boolean primaryKey,
            boolean notNull
    ) {
        public ColumnDefinition {
            Objects.requireNonNull(name, "Column name cannot be null");
            Objects.requireNonNull(dataType, "Data type cannot be null");
        }
        
        public static ColumnDefinition of(String name, String dataType) {
            return new ColumnDefinition(name, dataType, false, false);
        }
        
        public static ColumnDefinition primaryKey(String name, String dataType) {
            return new ColumnDefinition(name, dataType, true, true);
        }
        
        public static ColumnDefinition notNull(String name, String dataType) {
            return new ColumnDefinition(name, dataType, false, true);
        }
    }
}
