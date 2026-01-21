package org.finos.legend.engine.plan;

/**
 * Sealed interface for mutation operations.
 * 
 * The mutation IR represents write operations (INSERT, UPDATE, DELETE).
 */
public sealed interface MutationNode permits InsertNode, UpdateNode, DeleteNode {

    /**
     * Accept a visitor for this mutation node.
     */
    <T> T accept(MutationNodeVisitor<T> visitor);
}
