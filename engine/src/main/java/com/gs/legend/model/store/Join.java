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
 * PlanGenerator uses {@code renderJoinCondition(join.condition(), tableToAlias)} to render
 * the full expression tree to SQL. No decomposition into left/right table/column is needed.
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
     * Convenience constructor for simple equi-joins: builds {@code Comparison(ColumnRef, "=", ColumnRef)}.
     */
    public Join(String name, String leftTable, String leftColumn,
                String rightTable, String rightColumn) {
        this(name, RelationalOperation.Comparison.eq(
                RelationalOperation.ColumnRef.of(leftTable, leftColumn),
                RelationalOperation.ColumnRef.of(rightTable, rightColumn)));
    }

    /**
     * Convenience factory for simple equi-joins.
     */
    public static Join of(String name, String leftTable, String leftColumn, 
                          String rightTable, String rightColumn) {
        return new Join(name, leftTable, leftColumn, rightTable, rightColumn);
    }
    
    @Override
    public String toString() {
        return "Join " + name + "(" + condition + ")";
    }
}
