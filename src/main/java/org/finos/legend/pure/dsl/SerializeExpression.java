package org.finos.legend.pure.dsl;


/**
 * Represents a serialize() call that terminates a graphFetch chain.
 * 
 * Syntax: Person.all()->graphFetch(#{ ... }#)->serialize(#{ ... }#)
 * 
 * serialize() converts the fetched graph to a serialized output format (JSON).
 * This is the terminal expression for M2M queries.
 * 
 * Unlike project() which returns tabular Relation data, serialize() returns
 * structured object data (JSON array of objects).
 */
public record SerializeExpression(
        GraphFetchExpression source,
        GraphFetchTree serializeTree) implements PureExpression {

    /**
     * @return The class being serialized
     */
    public String getRootClassName() {
        return source.fetchTree().rootClass();
    }

    /**
     * @return The graphFetch tree from the source (defines what was fetched)
     */
    public GraphFetchTree fetchTree() {
        return source.fetchTree();
    }
}
