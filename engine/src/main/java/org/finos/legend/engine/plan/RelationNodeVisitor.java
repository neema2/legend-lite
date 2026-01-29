package org.finos.legend.engine.plan;

/**
 * Visitor interface for traversing RelationNode trees.
 * 
 * @param <T> The return type of the visitor methods
 */
public interface RelationNodeVisitor<T> {

    /**
     * Visit a table node (FROM clause).
     */
    T visit(TableNode table);

    /**
     * Visit a filter node (WHERE clause).
     */
    T visit(FilterNode filter);

    /**
     * Visit a project node (SELECT clause).
     */
    T visit(ProjectNode project);

    /**
     * Visit a join node (JOIN clause).
     */
    T visit(JoinNode join);

    /**
     * Visit a group by node (GROUP BY clause).
     */
    T visit(GroupByNode groupBy);

    /**
     * Visit a sort node (ORDER BY clause).
     */
    T visit(SortNode sort);

    /**
     * Visit a limit node (LIMIT/OFFSET clause).
     */
    T visit(LimitNode limit);

    /**
     * Visit a from node (runtime binding).
     */
    T visit(FromNode from);

    /**
     * Visit an extend node (window functions).
     */
    T visit(ExtendNode extend);

    /**
     * Visit a lateral join node (CROSS JOIN LATERAL UNNEST).
     */
    T visit(LateralJoinNode lateralJoin);

    /**
     * Visit a distinct node (SELECT DISTINCT).
     */
    T visit(DistinctNode distinct);

    /**
     * Visit a rename node (column aliasing).
     */
    T visit(RenameNode rename);

    /**
     * Visit a concatenate node (UNION ALL).
     */
    T visit(ConcatenateNode concatenate);

    /**
     * Visit a pivot node (PIVOT operation).
     */
    T visit(PivotNode pivot);

    /**
     * Visit a TDS literal node (inline tabular data).
     */
    T visit(TdsLiteralNode tdsLiteral);

    /**
     * Visit a constant node (SELECT expression without FROM).
     */
    T visit(ConstantNode constant);
}
