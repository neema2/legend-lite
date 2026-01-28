package org.finos.legend.pure.dsl;

/**
 * Represents a from() binding expression that connects a query to its execution
 * context.
 * 
 * For Relation queries (working with raw tables), ->from(runtime) binds to a
 * runtime
 * which provides the database connection.
 * 
 * Syntax: query->from(runtime::Name)
 * 
 * Example:
 * 
 * <pre>
 * #>{store::DB.TABLE}
 *     ->select(~col1, ~col2)
 *     ->filter(x | $x.col1 > 10)
 *     ->from(myRuntime)
 * </pre>
 * 
 * @param source     The source expression (RelationExpression chain)
 * @param runtimeRef The runtime reference
 */
public record FromExpression(
        PureExpression source,
        String runtimeRef) implements PureExpression {

    public FromExpression {
        java.util.Objects.requireNonNull(source, "Source expression cannot be null");
        java.util.Objects.requireNonNull(runtimeRef, "Runtime reference cannot be null");
    }

    @Override
    public String toString() {
        return source + "->from(" + runtimeRef + ")";
    }
}
