package org.finos.legend.engine.plan;

import java.util.List;

/**
 * Represents an IN expression: expr IN (value1, value2, ...)
 * 
 * Used for:
 * - $x->in(['a', 'b', 'c']) translates to x IN ('a', 'b', 'c')
 * - Contains() member checks
 */
public record InExpression(
        Expression operand,
        List<Expression> values,
        boolean negated) implements Expression {

    public static InExpression of(Expression operand, List<Expression> values) {
        return new InExpression(operand, values, false);
    }

    public static InExpression notIn(Expression operand, List<Expression> values) {
        return new InExpression(operand, values, true);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public SqlType type() {
        return SqlType.BOOLEAN;
    }
}
