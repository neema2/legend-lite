package com.gs.legend.model.store;

import com.gs.legend.model.def.RelationalOperation;

import java.util.Objects;

/**
 * Represents a Join definition between two tables in a relational database.
 * 
 * The join condition is a full {@link RelationalOperation} expression tree, supporting:
 * <pre>
 * Join Simple(T1.ID = T2.FK_ID)                              -- simple equi-join
 * Join Multi(T1.A = T2.A and T1.B = T2.B)                    -- multi-column
 * Join SelfJoin(T1.PARENT_ID = {target}.ID)                  -- self-join
 * Join FuncJoin(concat('pfx_', T1.NAME) = T2.PREFIXED_NAME)  -- function-based
 * </pre>
 * 
 * For backward compatibility, simple equi-joins expose {@link #leftTable()}, {@link #leftColumn()},
 * {@link #rightTable()}, {@link #rightColumn()}, and {@link #getColumnForTable(String)} accessors
 * that extract from the expression tree. These throw {@link UnsupportedOperationException} for
 * complex join conditions.
 * 
 * @param name      The join name
 * @param condition The full expression tree for the join condition
 */
public record Join(
        String name,
        RelationalOperation condition
) {
    
    public Join {
        Objects.requireNonNull(name, "Join name cannot be null");
        Objects.requireNonNull(condition, "Join condition cannot be null");
    }

    /**
     * Backward-compatible constructor for simple equi-joins.
     */
    public Join(String name, String leftTable, String leftColumn,
                String rightTable, String rightColumn) {
        this(name, RelationalOperation.Comparison.eq(
                RelationalOperation.ColumnRef.of(leftTable, leftColumn),
                RelationalOperation.ColumnRef.of(rightTable, rightColumn)));
    }

    /**
     * Creates a join with explicit table and column names (backward compat).
     */
    public static Join of(String name, String leftTable, String leftColumn, 
                          String rightTable, String rightColumn) {
        return new Join(name, leftTable, leftColumn, rightTable, rightColumn);
    }

    // ==================== Simple equi-join accessors (backward compat) ====================

    private RelationalOperation.SimpleEquiJoin equiJoin() {
        var eq = condition.asSimpleEquiJoin();
        if (eq == null) throw new UnsupportedOperationException(
                "Not a simple equi-join: " + name + " — use condition() for complex joins");
        return eq;
    }

    /** Left table name. Throws if not a simple equi-join. */
    public String leftTable() { return equiJoin().leftTable(); }

    /** Left column name. Throws if not a simple equi-join. */
    public String leftColumn() { return equiJoin().leftColumn(); }

    /** Right table name. Throws if not a simple equi-join. */
    public String rightTable() { return equiJoin().rightTable(); }

    /** Right column name. Throws if not a simple equi-join. */
    public String rightColumn() { return equiJoin().rightColumn(); }

    /**
     * Gets the table that is NOT the given table name.
     * For simple equi-joins only.
     */
    public String getOtherTable(String tableName) {
        var eq = equiJoin();
        if (eq.leftTable().equals(tableName)) return eq.rightTable();
        if (eq.rightTable().equals(tableName)) return eq.leftTable();
        throw new IllegalArgumentException("Table " + tableName + " is not part of join " + name);
    }
    
    /**
     * Gets the column for a given table.
     * For simple equi-joins only.
     */
    public String getColumnForTable(String tableName) {
        var eq = equiJoin();
        if (eq.leftTable().equals(tableName)) return eq.leftColumn();
        if (eq.rightTable().equals(tableName)) return eq.rightColumn();
        throw new IllegalArgumentException("Table " + tableName + " is not part of join " + name);
    }
    
    /**
     * Checks if this join involves the given table.
     * For simple equi-joins only; returns false for complex joins.
     */
    public boolean involvesTable(String tableName) {
        var eq = condition.asSimpleEquiJoin();
        if (eq == null) return false;
        return eq.leftTable().equals(tableName) || eq.rightTable().equals(tableName);
    }

    /**
     * Returns true if this is a simple equi-join (T1.C1 = T2.C2).
     */
    public boolean isSimpleEquiJoin() {
        return condition.asSimpleEquiJoin() != null;
    }
    
    @Override
    public String toString() {
        var eq = condition.asSimpleEquiJoin();
        if (eq != null) {
            return "Join " + name + "(" + eq.leftTable() + "." + eq.leftColumn() + " = " + 
                   eq.rightTable() + "." + eq.rightColumn() + ")";
        }
        return "Join " + name + "(" + condition + ")";
    }
}
