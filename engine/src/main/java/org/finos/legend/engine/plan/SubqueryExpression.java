package org.finos.legend.engine.plan;

import java.util.Objects;

/**
 * Expression representing a scalar subquery.
 * 
 * Used in deep fetch for 1-to-many associations where we need a correlated
 * subquery instead of a JOIN to avoid row duplication.
 * 
 * Example:
 * 
 * <pre>
 * // In IR:
 * SubqueryExpression(
 *     FilterNode(
 *         TableNode(T_ADDRESS, t1),
 *         t1.PERSON_ID = t0.ID  // correlation
 *     ),
 *     json_group_array(json_object('city', t1.CITY))
 * )
 * 
 * // Rendered as SQL:
 * (SELECT json_group_array(json_object('city', t1.CITY))
 *  FROM T_ADDRESS t1 WHERE t1.PERSON_ID = t0.ID)
 * </pre>
 * 
 * @param source           The source relation (typically FilterNode with
 *                         correlation)
 * @param selectExpression The expression to SELECT (typically json_group_array)
 */
public record SubqueryExpression(
        RelationNode source,
        Expression selectExpression) implements Expression {

    public SubqueryExpression {
        Objects.requireNonNull(source, "Source cannot be null");
        Objects.requireNonNull(selectExpression, "Select expression cannot be null");
    }

    @Override
    public PureType type() {
        return selectExpression.type();
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
