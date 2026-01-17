package org.finos.legend.engine.plan;

/**
 * Visitor interface for traversing Expression trees.
 * 
 * @param <T> The return type of the visitor methods
 */
public interface ExpressionVisitor<T> {
    
    /**
     * Visit a column reference expression.
     */
    T visitColumnReference(ColumnReference columnRef);
    
    /**
     * Visit a literal expression.
     */
    T visitLiteral(Literal literal);
    
    /**
     * Visit a comparison expression.
     */
    T visitComparison(ComparisonExpression comparison);
    
    /**
     * Visit a logical expression.
     */
    T visitLogical(LogicalExpression logical);
}
