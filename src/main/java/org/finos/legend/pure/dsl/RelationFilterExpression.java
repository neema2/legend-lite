package org.finos.legend.pure.dsl;

import java.util.Objects;

/**
 * Represents a filter() on a Relation expression.
 * 
 * Example: ...->project([...])->filter({r | $r.salary > 50000})
 * 
 * Input: RelationExpression (e.g., ProjectExpression)
 * Output: RelationExpression (stays Relation type)
 * 
 * @param source The Relation expression being filtered
 * @param lambda The filter predicate lambda
 */
public record RelationFilterExpression(
        RelationExpression source,
        LambdaExpression lambda) implements RelationExpression {
    public RelationFilterExpression {
        Objects.requireNonNull(source, "Source cannot be null");
        Objects.requireNonNull(lambda, "Lambda cannot be null");
    }
}
