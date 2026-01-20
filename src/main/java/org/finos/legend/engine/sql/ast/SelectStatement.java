package org.finos.legend.engine.sql.ast;

import java.util.List;

/**
 * Represents a SQL SELECT statement.
 * 
 * @param distinct    Whether DISTINCT was specified
 * @param selectItems The projection list (may contain * for all columns)
 * @param from        The FROM clause items
 * @param where       Optional WHERE condition
 * @param groupBy     Optional GROUP BY columns
 * @param having      Optional HAVING condition
 * @param orderBy     Optional ORDER BY specifications
 * @param limit       Optional LIMIT value
 * @param offset      Optional OFFSET value
 */
public record SelectStatement(
        boolean distinct,
        List<SelectItem> selectItems,
        List<FromItem> from,
        Expression where,
        List<Expression> groupBy,
        Expression having,
        List<OrderSpec> orderBy,
        Integer limit,
        Integer offset) implements SQLNode {

    /**
     * Creates a simple SELECT statement with just projections and FROM.
     */
    public static SelectStatement simple(List<SelectItem> items, List<FromItem> from) {
        return new SelectStatement(false, items, from, null, List.of(), null, List.of(), null, null);
    }

    /**
     * Creates a SELECT statement with WHERE clause.
     */
    public SelectStatement withWhere(Expression condition) {
        return new SelectStatement(distinct, selectItems, from, condition, groupBy, having, orderBy, limit, offset);
    }

    /**
     * Creates a SELECT statement with ORDER BY.
     */
    public SelectStatement withOrderBy(List<OrderSpec> orderSpecs) {
        return new SelectStatement(distinct, selectItems, from, where, groupBy, having, orderSpecs, limit, offset);
    }

    /**
     * Creates a SELECT statement with LIMIT/OFFSET.
     */
    public SelectStatement withLimit(Integer limitVal, Integer offsetVal) {
        return new SelectStatement(distinct, selectItems, from, where, groupBy, having, orderBy, limitVal, offsetVal);
    }

    public boolean hasWhere() {
        return where != null;
    }

    public boolean hasGroupBy() {
        return groupBy != null && !groupBy.isEmpty();
    }

    public boolean hasHaving() {
        return having != null;
    }

    public boolean hasOrderBy() {
        return orderBy != null && !orderBy.isEmpty();
    }

    public boolean hasLimit() {
        return limit != null;
    }
}
