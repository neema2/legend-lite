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
 *     Join JoinName(TABLE_A.COLUMN_A = TABLE_B.COLUMN_B)
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
 *         FIRST_NAME VARCHAR(100) NOT NULL
 *     )
 *     Table T_ADDRESS
 *     (
 *         ID INTEGER PRIMARY KEY,
 *         PERSON_ID INTEGER NOT NULL,
 *         STREET VARCHAR(200) NOT NULL
 *     )
 *     Join Person_Address(T_PERSON.ID = T_ADDRESS.PERSON_ID)
 * )
 * </pre>
 * 
 * @param qualifiedName The fully qualified database name
 * @param tables The list of table definitions
 * @param joins The list of join definitions
 */
public record DatabaseDefinition(
        String qualifiedName,
        List<TableDefinition> tables,
        List<JoinDefinition> joins
) implements PureDefinition {
    
    public DatabaseDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        Objects.requireNonNull(tables, "Tables cannot be null");
        Objects.requireNonNull(joins, "Joins cannot be null");
        tables = List.copyOf(tables);
        joins = List.copyOf(joins);
    }
    
    /**
     * Constructor without joins for backward compatibility.
     */
    public DatabaseDefinition(String qualifiedName, List<TableDefinition> tables) {
        this(qualifiedName, tables, List.of());
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
     * Finds a join by name.
     */
    public Optional<JoinDefinition> findJoin(String joinName) {
        return joins.stream()
                .filter(j -> j.name().equals(joinName))
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
    
    /**
     * Represents a join definition within a database.
     * 
     * Pure syntax: Join JoinName(TABLE_A.COLUMN_A = TABLE_B.COLUMN_B)
     * 
     * @param name The join name
     * @param leftTable The left table name
     * @param leftColumn The left column name
     * @param rightTable The right table name
     * @param rightColumn The right column name
     */
    public record JoinDefinition(
            String name,
            String leftTable,
            String leftColumn,
            String rightTable,
            String rightColumn
    ) {
        public JoinDefinition {
            Objects.requireNonNull(name, "Join name cannot be null");
            Objects.requireNonNull(leftTable, "Left table cannot be null");
            Objects.requireNonNull(leftColumn, "Left column cannot be null");
            Objects.requireNonNull(rightTable, "Right table cannot be null");
            Objects.requireNonNull(rightColumn, "Right column cannot be null");
        }
        
        /**
         * Checks if this join involves the given table.
         */
        public boolean involvesTable(String tableName) {
            return leftTable.equals(tableName) || rightTable.equals(tableName);
        }
        
        /**
         * Gets the other table in the join.
         */
        public String getOtherTable(String tableName) {
            if (leftTable.equals(tableName)) return rightTable;
            if (rightTable.equals(tableName)) return leftTable;
            throw new IllegalArgumentException("Table " + tableName + " not in join " + name);
        }
    }
}
