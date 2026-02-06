package org.finos.legend.engine.plan;

import org.finos.legend.pure.dsl.m2m.BinaryArithmeticExpr;

import java.util.Objects;

/**
 * Represents an arithmetic expression in SQL.
 * 
 * Supports: +, -, *, /
 * 
 * @param left     The left operand
 * @param operator The arithmetic operator
 * @param right    The right operand
 */
public record ArithmeticExpression(
        Expression left,
        BinaryArithmeticExpr.Operator operator,
        Expression right) implements Expression {

    public ArithmeticExpression {
        Objects.requireNonNull(left, "Left operand cannot be null");
        Objects.requireNonNull(operator, "Operator cannot be null");
        Objects.requireNonNull(right, "Right operand cannot be null");
    }

    public static ArithmeticExpression add(Expression left, Expression right) {
        return new ArithmeticExpression(left, BinaryArithmeticExpr.Operator.ADD, right);
    }

    public static ArithmeticExpression subtract(Expression left, Expression right) {
        return new ArithmeticExpression(left, BinaryArithmeticExpr.Operator.SUBTRACT, right);
    }

    public static ArithmeticExpression multiply(Expression left, Expression right) {
        return new ArithmeticExpression(left, BinaryArithmeticExpr.Operator.MULTIPLY, right);
    }

    public static ArithmeticExpression divide(Expression left, Expression right) {
        return new ArithmeticExpression(left, BinaryArithmeticExpr.Operator.DIVIDE, right);
    }

    /**
     * @return The SQL operator symbol
     */
    public String sqlOperator() {
        return operator.symbol();
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitArithmetic(this);
    }

    @Override
    public SqlType type() {
        // Integer arithmetic stays Integer (except division which may produce Double)
        if (left.type() == SqlType.INTEGER && right.type() == SqlType.INTEGER
                && operator != BinaryArithmeticExpr.Operator.DIVIDE) {
            return SqlType.INTEGER;
        }
        return SqlType.DOUBLE;
    }

    @Override
    public String toString() {
        return "(" + left + " " + operator.symbol() + " " + right + ")";
    }
}
