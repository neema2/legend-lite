package org.finos.legend.pure.dsl;

import java.util.Objects;

/**
 * Represents a filter() function call.
 * 
 * Example: ->filter(p | $p.lastName == 'Smith')
 * 
 * @param source The source expression being filtered
 * @param lambda The filter predicate lambda
 */
public record FilterExpression(
        PureExpression source,
        LambdaExpression lambda
) implements PureExpression {
    public FilterExpression {
        Objects.requireNonNull(source, "Source cannot be null");
        Objects.requireNonNull(lambda, "Lambda cannot be null");
    }
}
