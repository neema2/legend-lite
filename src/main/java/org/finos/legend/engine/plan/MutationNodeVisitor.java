package org.finos.legend.engine.plan;

/**
 * Visitor for mutation nodes.
 */
public interface MutationNodeVisitor<T> {
    T visit(InsertNode insert);

    T visit(UpdateNode update);

    T visit(DeleteNode delete);
}
