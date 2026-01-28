package org.finos.legend.pure.dsl;

import java.util.List;
import java.util.Objects;

/**
 * Represents a project() function call for selecting specific properties.
 * 
 * Converts ClassExpression to RelationExpression by extracting typed columns.
 * 
 * Example: ->project([p | $p.firstName, p | $p.lastName], ['firstName',
 * 'lastName'])
 * 
 * @param source      The CLASS expression being projected (compile-time
 *                    enforced!)
 * @param projections List of lambda expressions for property access
 * @param aliases     List of column aliases (optional, can be derived from
 *                    property names)
 */
public record ProjectExpression(
        ClassExpression source,
        List<LambdaExpression> projections,
        List<String> aliases) implements RelationExpression {
    public ProjectExpression {
        Objects.requireNonNull(source, "Source cannot be null");
        Objects.requireNonNull(projections, "Projections cannot be null");

        projections = List.copyOf(projections);
        aliases = aliases != null ? List.copyOf(aliases) : List.of();

        if (projections.isEmpty()) {
            throw new IllegalArgumentException("Projections cannot be empty");
        }
    }

    /**
     * Creates a project expression with aliases derived from property names.
     */
    public ProjectExpression(ClassExpression source, List<LambdaExpression> projections) {
        this(source, projections, List.of());
    }
}
