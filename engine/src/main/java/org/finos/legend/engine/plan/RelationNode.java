package org.finos.legend.engine.plan;

/**
 * Sealed interface representing nodes in the relational algebra tree.
 * This is the Intermediate Representation (IR) for query execution plans.
 * 
 * The hierarchy models relational operations that can be pushed down to SQL:
 * - TableNode: Physical table scan (FROM clause)
 * - FilterNode: Row filtering (WHERE clause)
 * - ProjectNode: Column projection (SELECT clause)
 * - JoinNode: Table join (JOIN clause)
 */
public sealed interface RelationNode
        permits TableNode, FilterNode, ProjectNode, JoinNode, GroupByNode, AggregateNode, SortNode, LimitNode, FromNode,
        ExtendNode, LateralJoinNode, DistinctNode, RenameNode, ConcatenateNode, PivotNode, TdsLiteralNode, ConstantNode,
        StructLiteralNode {

    /**
     * Accept method for the visitor pattern.
     * 
     * @param visitor The visitor to accept
     * @param <T>     The return type of the visitor
     * @return The result of visiting this node
     */
    <T> T accept(RelationNodeVisitor<T> visitor);
}
