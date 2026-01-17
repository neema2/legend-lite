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
    T visitTable(TableNode table);
    
    /**
     * Visit a filter node (WHERE clause).
     */
    T visitFilter(FilterNode filter);
    
    /**
     * Visit a project node (SELECT clause).
     */
    T visitProject(ProjectNode project);
}
