package org.finos.legend.pure.dsl.m2m;

/**
 * Visitor pattern for M2M expressions.
 * 
 * @param <T> The return type of the visitor methods
 */
public interface M2MExpressionVisitor<T> {

    T visit(SourcePropertyRef sourcePropertyRef);

    T visit(M2MLiteral literal);

    T visit(StringConcatExpr concat);

    T visit(FunctionCallExpr functionCall);

    T visit(IfExpr ifExpr);

    T visit(BinaryArithmeticExpr arithmetic);

    T visit(M2MComparisonExpr comparison);

    T visit(AssociationRef associationRef);
}
