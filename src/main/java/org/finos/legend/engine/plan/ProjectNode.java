package org.finos.legend.engine.plan;

import java.util.List;
import java.util.Objects;

/**
 * Represents a projection operation in the relational algebra.
 * This corresponds to the SELECT clause in SQL.
 * 
 * Supports "Structural Projections" - each projection maps a SQL column
 * to a property name (alias), enabling the result set to be mapped back
 * to Pure objects.
 * 
 * @param source The source relation to project
 * @param projections The list of columns to project with their aliases
 */
public record ProjectNode(
        RelationNode source,
        List<Projection> projections
) implements RelationNode {
    
    public ProjectNode {
        Objects.requireNonNull(source, "Source cannot be null");
        Objects.requireNonNull(projections, "Projections cannot be null");
        
        if (projections.isEmpty()) {
            throw new IllegalArgumentException("Projections cannot be empty");
        }
        
        // Ensure immutability
        projections = List.copyOf(projections);
    }
    
    /**
     * Factory for creating a ProjectNode with varargs projections.
     */
    public static ProjectNode of(RelationNode source, Projection... projections) {
        return new ProjectNode(source, List.of(projections));
    }
    
    @Override
    public <T> T accept(RelationNodeVisitor<T> visitor) {
        return visitor.visit(this);
    }
    
    @Override
    public String toString() {
        return "ProjectNode(" + projections + " <- " + source + ")";
    }
}
