package org.finos.legend.engine.store;

import java.util.Objects;

/**
 * Represents a Join definition between two tables in a relational database.
 * 
 * Pure syntax:
 * <pre>
 * Join JoinName(TABLE_A.COLUMN_A = TABLE_B.COLUMN_B)
 * </pre>
 * 
 * Example:
 * <pre>
 * Database store::MyDB
 * (
 *     Table T_PERSON (...)
 *     Table T_ADDRESS (...)
 *     Join Person_Address(T_PERSON.ID = T_ADDRESS.PERSON_ID)
 * )
 * </pre>
 * 
 * @param name The join name
 * @param leftTable The left table in the join
 * @param leftColumn The left column
 * @param rightTable The right table in the join  
 * @param rightColumn The right column
 */
public record Join(
        String name,
        String leftTable,
        String leftColumn,
        String rightTable,
        String rightColumn
) {
    
    public Join {
        Objects.requireNonNull(name, "Join name cannot be null");
        Objects.requireNonNull(leftTable, "Left table cannot be null");
        Objects.requireNonNull(leftColumn, "Left column cannot be null");
        Objects.requireNonNull(rightTable, "Right table cannot be null");
        Objects.requireNonNull(rightColumn, "Right column cannot be null");
    }
    
    /**
     * Creates a join with explicit table and column names.
     */
    public static Join of(String name, String leftTable, String leftColumn, 
                          String rightTable, String rightColumn) {
        return new Join(name, leftTable, leftColumn, rightTable, rightColumn);
    }
    
    /**
     * Gets the table that is NOT the given table name.
     * Useful for navigating from one table to another via the join.
     */
    public String getOtherTable(String tableName) {
        if (leftTable.equals(tableName)) {
            return rightTable;
        }
        if (rightTable.equals(tableName)) {
            return leftTable;
        }
        throw new IllegalArgumentException("Table " + tableName + " is not part of join " + name);
    }
    
    /**
     * Gets the column for a given table.
     */
    public String getColumnForTable(String tableName) {
        if (leftTable.equals(tableName)) {
            return leftColumn;
        }
        if (rightTable.equals(tableName)) {
            return rightColumn;
        }
        throw new IllegalArgumentException("Table " + tableName + " is not part of join " + name);
    }
    
    /**
     * Checks if this join involves the given table.
     */
    public boolean involvesTable(String tableName) {
        return leftTable.equals(tableName) || rightTable.equals(tableName);
    }
    
    @Override
    public String toString() {
        return "Join " + name + "(" + leftTable + "." + leftColumn + " = " + 
               rightTable + "." + rightColumn + ")";
    }
}
