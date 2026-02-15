package org.finos.legend.engine.plan;

import java.util.Objects;

/**
 * Expression representing a JSON array aggregation.
 * 
 * Used in deep fetch for 1-to-many associations to aggregate
 * multiple rows into a JSON array.
 * 
 * Example:
 * 
 * <pre>
 * // In IR:
 * JsonArrayExpression(
 *     JsonObjectExpression([city, street])
 * )
 * 
 * // Rendered as SQL:
 * json_group_array(json_object('city', t1.CITY, 'street', t1.STREET))
 * </pre>
 * 
 * @param elementExpression The expression to aggregate into an array (typically
 *                          JsonObjectExpression)
 */
public record JsonArrayExpression(
        Expression elementExpression) implements Expression {

    public JsonArrayExpression {
        Objects.requireNonNull(elementExpression, "Element expression cannot be null");
    }

    @Override
    public PureType type() {
        return PureType.JSON;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
