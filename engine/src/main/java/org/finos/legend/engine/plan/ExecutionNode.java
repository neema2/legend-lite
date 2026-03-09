package org.finos.legend.engine.plan;

/**
 * Base interface for execution plan nodes.
 *
 * <p>
 * Sealed to control the type hierarchy — mirrors legend-engine's
 * ExecutionNode class hierarchy but as a clean Java sealed interface.
 *
 * <p>
 * Current implementations:
 * <ul>
 * <li>{@link SQLExecutionNode} — executes a SQL query against a database</li>
 * </ul>
 */
public sealed interface ExecutionNode
        permits SQLExecutionNode {

    /** The result schema for this node's output. */
    RelationType resultType();
}
