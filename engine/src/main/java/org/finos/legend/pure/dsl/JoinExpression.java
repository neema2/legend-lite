package org.finos.legend.pure.dsl;

import java.util.Objects;

/**
 * Represents a join() function call on Relations.
 * 
 * Only works on RelationExpression (from project() or Relation literal).
 * This is enforced at parse time!
 * 
 * Example: ->join(otherRelation, JoinType.LEFT_OUTER, {l, r | $l.id ==
 * $r.personId})
 * 
 * @param left      The left RELATION expression (compile-time enforced!)
 * @param right     The right RELATION expression to join
 * @param joinType  The type of join (INNER, LEFT_OUTER, RIGHT_OUTER,
 *                  FULL_OUTER)
 * @param condition The join condition lambda with two parameters
 */
public record JoinExpression(
        PureExpression left,
        PureExpression right,
        JoinType joinType,
        LambdaExpression condition) implements RelationExpression {

    public JoinExpression {
        Objects.requireNonNull(left, "Left relation cannot be null");
        Objects.requireNonNull(right, "Right relation cannot be null");
        Objects.requireNonNull(joinType, "Join type cannot be null");
        Objects.requireNonNull(condition, "Join condition cannot be null");
    }

    /**
     * Types of SQL joins.
     */
    public enum JoinType {
        INNER,
        LEFT_OUTER,
        RIGHT_OUTER,
        FULL_OUTER;

        /**
         * Parse join type from string (case-insensitive).
         * Supports: INNER, LEFT, LEFT_OUTER, RIGHT, RIGHT_OUTER, FULL, FULL_OUTER
         */
        public static JoinType fromString(String s) {
            return switch (s.toUpperCase().replace("JOINTYPE.", "")) {
                case "INNER" -> INNER;
                case "LEFT", "LEFT_OUTER" -> LEFT_OUTER;
                case "RIGHT", "RIGHT_OUTER" -> RIGHT_OUTER;
                case "FULL", "FULL_OUTER" -> FULL_OUTER;
                default -> throw new IllegalArgumentException("Unknown join type: " + s);
            };
        }
    }
}
