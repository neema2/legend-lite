package org.finos.legend.engine.plan;

import java.util.Objects;

/**
 * Represents a CASE WHEN expression in SQL.
 * 
 * Used to compile if(condition, |then, |else) from Pure.
 * 
 * Simple form (single condition):
 * <pre>
 * CASE WHEN condition THEN thenValue ELSE elseValue END
 * </pre>
 * 
 * Nested forms compile to:
 * <pre>
 * CASE 
 *     WHEN cond1 THEN val1
 *     WHEN cond2 THEN val2
 *     ELSE val3
 * END
 * </pre>
 * 
 * @param condition The condition to evaluate
 * @param thenValue The value if condition is true
 * @param elseValue The value if condition is false
 */
public record CaseExpression(
        Expression condition,
        Expression thenValue,
        Expression elseValue
) implements Expression {
    
    public CaseExpression {
        Objects.requireNonNull(condition, "Condition cannot be null");
        Objects.requireNonNull(thenValue, "Then value cannot be null");
        Objects.requireNonNull(elseValue, "Else value cannot be null");
    }
    
    public static CaseExpression of(Expression condition, Expression thenValue, Expression elseValue) {
        return new CaseExpression(condition, thenValue, elseValue);
    }
    
    /**
     * @return True if the else branch is another CaseExpression (nested if)
     */
    public boolean hasNestedElse() {
        return elseValue instanceof CaseExpression;
    }
    
    @Override
    public GenericType type() {
        GenericType thenType = thenValue.type();
        return thenType != GenericType.Primitive.ANY ? thenType : elseValue.type();
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitCase(this);
    }
    
    @Override
    public String toString() {
        return "CASE WHEN " + condition + " THEN " + thenValue + " ELSE " + elseValue + " END";
    }
}
