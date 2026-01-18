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
}
