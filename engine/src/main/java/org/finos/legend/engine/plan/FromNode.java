package org.finos.legend.engine.plan;

/**
 * A plan node that binds a query to a runtime for execution.
 * 
 * The FromNode wraps a compiled query with the runtime reference that
 * provides the database connection for execution.
 * 
 * Example:
 * 
 * <pre>
 * #>{MyDb.T_EMPLOYEE}->select(~name)->from(My::Runtime::DuckDb)
 * </pre>
 * 
 * The SQL is generated from the source node, and the runtimeRef is used
 * during execution to resolve the database connection.
 * 
 * @param source     The compiled query plan
 * @param runtimeRef The qualified runtime reference (e.g.,
 *                   "My::Runtime::DuckDb")
 */
public record FromNode(
        RelationNode source,
        String runtimeRef) implements RelationNode {

    public FromNode {
        java.util.Objects.requireNonNull(source, "Source cannot be null");
        java.util.Objects.requireNonNull(runtimeRef, "Runtime reference cannot be null");
    }

    @Override
    public <T> T accept(RelationNodeVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
