package org.finos.legend.pure.dsl.m2m;

/**
 * Sealed interface for Model-to-Model mapping expressions.
 * 
 * These expressions appear on the right-hand side of M2M property mappings:
 * <pre>
 * Person: Pure
 * {
 *     ~src RawPerson
 *     fullName: $src.firstName + ' ' + $src.lastName,  // StringConcatExpr
 *     upperName: $src.firstName->toUpper(),            // FunctionCallExpr
 *     ageGroup: if($src.age < 18, |'Minor', |'Adult')  // IfExpr
 * }
 * </pre>
 */
public sealed interface M2MExpression 
        permits SourcePropertyRef, M2MLiteral, StringConcatExpr, 
                FunctionCallExpr, IfExpr, BinaryArithmeticExpr,
                M2MComparisonExpr {
    
    /**
     * Accepts a visitor to process this expression.
     * 
     * @param visitor The visitor
     * @param <T> The return type
     * @return The result of visiting
     */
    <T> T accept(M2MExpressionVisitor<T> visitor);
}
