package org.finos.legend.engine.plan;

import java.util.List;
import java.util.Objects;

/**
 * Expression representing a nested JSON object.
 * 
 * Used in deep fetch to represent nested 1-to-1 associations that should
 * be rendered as nested json_object() in the SQL output.
 * 
 * Example:
 * 
 * <pre>
 * // In IR:
 * JsonObjectExpression([
 *     Projection(ColumnRef("t1", "CITY"), "city"),
 *     Projection(ColumnRef("t1", "STREET"), "street")
 * ])
 * 
 * // Rendered as SQL:
 * json_object('city', t1.CITY, 'street', t1.STREET)
 * </pre>
 * 
 * @param projections The list of projections (key-value pairs) for the JSON
 *                    object
 */
public record JsonObjectExpression(List<Projection> projections) implements Expression {

    public JsonObjectExpression {
        Objects.requireNonNull(projections, "Projections cannot be null");
        projections = List.copyOf(projections);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /**
     * @return true if this object has any nested projections
     */
    public boolean hasProjections() {
        return !projections.isEmpty();
    }
}
