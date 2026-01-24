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

    /**
     * Visit an EXISTS expression (correlated subquery).
     */
    T visitExists(ExistsExpression exists);

    /**
     * Visit a string concatenation expression.
     */
    T visitConcat(ConcatExpression concat);

    /**
     * Visit a SQL function call expression.
     */
    T visitFunctionCall(SqlFunctionCall functionCall);

    /**
     * Visit a CASE/conditional expression.
     */
    T visitCase(CaseExpression caseExpr);

    /**
     * Visit an arithmetic expression.
     */
    T visitArithmetic(ArithmeticExpression arithmetic);

    /**
     * Visit an aggregate expression (SUM, COUNT, etc.).
     */
    T visitAggregate(AggregateExpression aggregate);

    /**
     * Visit a nested JSON object expression (for deep fetch).
     */
    T visit(JsonObjectExpression jsonObject);

    /**
     * Visit a scalar subquery expression (for 1-to-many deep fetch).
     */
    T visit(SubqueryExpression subquery);

    /**
     * Visit a JSON array aggregation expression (for 1-to-many deep fetch).
     */
    T visit(JsonArrayExpression jsonArray);

    /**
     * Visit a collection function call (map, filter, fold, flatten).
     */
    T visitCollectionCall(SqlCollectionCall collectionCall);
}
