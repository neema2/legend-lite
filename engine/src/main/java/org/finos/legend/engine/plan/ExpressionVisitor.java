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
    T visitFunctionCall(FunctionExpression functionCall);

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
    T visitCollectionCall(CollectionExpression collectionCall);

    /**
     * Visit a date extraction expression (year, month, day, etc.).
     */
    T visit(DateFunctionExpression dateFunction);

    /**
     * Visit a current date/time expression (now, today).
     */
    T visit(CurrentDateExpression currentDate);

    /**
     * Visit a date difference expression (dateDiff).
     */
    T visit(DateDiffExpression dateDiff);

    /**
     * Visit a date adjustment expression (adjust/dateAdd).
     */
    T visit(DateAdjustExpression dateAdjust);

    /**
     * Visit a date truncation expression (firstDayOfMonth, etc.).
     */
    T visit(DateTruncExpression dateTrunc);

    /**
     * Visit an epoch conversion expression (fromEpochValue, toEpochValue).
     */
    T visitEpochExpression(EpochExpression epoch);

    /**
     * Visit a date comparison expression (isOnDay, isAfterDay, etc.).
     */
    T visitDateComparisonExpression(DateComparisonExpression dateComparison);

    /**
     * Visit a time bucket expression.
     */
    T visit(TimeBucketExpression timeBucket);

    /**
     * Visit a min/max expression (GREATEST/LEAST).
     */
    T visit(MinMaxExpression minMax);

    /**
     * Visit an IN expression (operand IN (values)).
     */
    T visit(InExpression inExpr);

    /**
     * Visit a list literal expression ([elem1, elem2, ...]).
     */
    T visit(ListLiteral listLiteral);

    /**
     * Visit a CAST expression (CAST(expr AS type)).
     */
    T visit(CastExpression castExpr);

    /**
     * Visit a list_filter expression (list_filter(array, x -> condition)).
     */
    T visit(ListFilterExpression listFilter);

    /**
     * Visit a struct literal expression ({'key': value, ...}).
     */
    T visit(StructLiteralExpression structLiteral);
}
