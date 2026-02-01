package org.finos.legend.pure.dsl;

import java.util.Objects;

/**
 * Represents an asOfJoin() function call on Relations.
 * 
 * AsOf join is a temporal join that finds the closest matching row from the
 * right relation
 * where the temporal condition is satisfied (e.g., left.time > right.time).
 * 
 * Two overloads:
 * 1. asOfJoin(rightRel, matchCondition) - just temporal match
 * 2. asOfJoin(rightRel, matchCondition, keyMatch) - temporal match + key
 * equality
 * 
 * Examples:
 * ->asOfJoin(other, {x,y | $x.time > $y.time})
 * ->asOfJoin(other, {x,y | $x.time > $y.time}, {x,y | $x.key == $y.key})
 * 
 * DuckDB SQL: left ASOF LEFT JOIN right ON matchCondition [AND keyMatch]
 */
public record AsOfJoinExpression(
        PureExpression left,
        PureExpression right,
        LambdaExpression matchCondition,
        LambdaExpression keyCondition // optional, null if not specified
) implements RelationExpression {

    public AsOfJoinExpression {
        Objects.requireNonNull(left, "Left relation cannot be null");
        Objects.requireNonNull(right, "Right relation cannot be null");
        Objects.requireNonNull(matchCondition, "Match condition cannot be null");
        // keyCondition can be null
    }

    /**
     * Check if this asOfJoin has a key match condition.
     */
    public boolean hasKeyCondition() {
        return keyCondition != null;
    }
}
