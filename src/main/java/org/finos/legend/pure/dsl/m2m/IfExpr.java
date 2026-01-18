package org.finos.legend.pure.dsl.m2m;

import java.util.Objects;

/**
 * Represents a conditional expression in M2M mappings.
 * 
 * Legend Pure syntax uses pipes before the then/else values:
 * <pre>
 * if($src.age < 18, |'Minor', |'Adult')
 * 
 * // Nested:
 * if($src.age < 18, |'Minor', |if($src.age < 65, |'Adult', |'Senior'))
 * </pre>
 * 
 * @param condition The condition to evaluate
 * @param thenBranch The expression to use if condition is true
 * @param elseBranch The expression to use if condition is false
 */
public record IfExpr(
        M2MExpression condition,
        M2MExpression thenBranch,
        M2MExpression elseBranch
) implements M2MExpression {
    
    public IfExpr {
        Objects.requireNonNull(condition, "Condition cannot be null");
        Objects.requireNonNull(thenBranch, "Then branch cannot be null");
        Objects.requireNonNull(elseBranch, "Else branch cannot be null");
    }
    
    public static IfExpr of(M2MExpression condition, M2MExpression thenBranch, M2MExpression elseBranch) {
        return new IfExpr(condition, thenBranch, elseBranch);
    }
    
    @Override
    public <T> T accept(M2MExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
    
    @Override
    public String toString() {
        return "if(" + condition + ", |" + thenBranch + ", |" + elseBranch + ")";
    }
}
