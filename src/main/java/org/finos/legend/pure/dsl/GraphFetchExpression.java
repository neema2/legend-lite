package org.finos.legend.pure.dsl;

import org.finos.legend.pure.dsl.graphfetch.GraphFetchTree;

/**
 * Represents a graphFetch() call on a class expression.
 * 
 * Syntax: Person.all()->graphFetch(#{ Person { prop1, prop2 } }#)
 * 
 * graphFetch() specifies which properties to fetch from the object graph.
 * It maintains the ClassExpression type semantics until serialize() is called.
 */
public record GraphFetchExpression(
        ClassExpression source,
        GraphFetchTree fetchTree) implements ClassExpression {
}
