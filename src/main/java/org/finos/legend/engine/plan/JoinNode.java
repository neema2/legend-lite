package org.finos.legend.engine.plan;

import java.util.Objects;

/**
 * Represents a JOIN operation in the relational algebra tree.
 * 
 * This node combines two relation nodes based on a join condition.
 * Supports INNER, LEFT, RIGHT, and FULL outer joins.
 * 
 * @param left The left input relation
 * @param right The right input relation
 * @param condition The join condition (typically a comparison expression)
 * @param joinType The type of join
 */
public record JoinNode(
        RelationNode left,
        RelationNode right,
        Expression condition,
        JoinType joinType
) implements RelationNode {
    
    public JoinNode {
        Objects.requireNonNull(left, "Left relation cannot be null");
        Objects.requireNonNull(right, "Right relation cannot be null");
        Objects.requireNonNull(condition, "Join condition cannot be null");
        Objects.requireNonNull(joinType, "Join type cannot be null");
    }
    
    /**
     * Creates an INNER JOIN.
     */
    public static JoinNode inner(RelationNode left, RelationNode right, Expression condition) {
        return new JoinNode(left, right, condition, JoinType.INNER);
    }
    
    /**
     * Creates a LEFT OUTER JOIN.
     */
    public static JoinNode leftOuter(RelationNode left, RelationNode right, Expression condition) {
        return new JoinNode(left, right, condition, JoinType.LEFT_OUTER);
    }
    
    /**
     * Creates a RIGHT OUTER JOIN.
     */
    public static JoinNode rightOuter(RelationNode left, RelationNode right, Expression condition) {
        return new JoinNode(left, right, condition, JoinType.RIGHT_OUTER);
    }
    
    /**
     * Creates a FULL OUTER JOIN.
     */
    public static JoinNode fullOuter(RelationNode left, RelationNode right, Expression condition) {
        return new JoinNode(left, right, condition, JoinType.FULL_OUTER);
    }
    
    @Override
    public <T> T accept(RelationNodeVisitor<T> visitor) {
        return visitor.visit(this);
    }
    
    /**
     * Types of SQL joins.
     */
    public enum JoinType {
        INNER("INNER JOIN"),
        LEFT_OUTER("LEFT OUTER JOIN"),
        RIGHT_OUTER("RIGHT OUTER JOIN"),
        FULL_OUTER("FULL OUTER JOIN");
        
        private final String sql;
        
        JoinType(String sql) {
            this.sql = sql;
        }
        
        public String toSql() {
            return sql;
        }
    }
}
