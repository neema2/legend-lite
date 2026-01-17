package org.finos.legend.pure.dsl;

import java.util.Objects;

/**
 * Represents a lambda expression.
 * 
 * Example: p | $p.lastName == 'Smith'
 *          x | $x.age
 * 
 * @param parameter The lambda parameter name (e.g., "p", "x")
 * @param body The lambda body expression
 */
public record LambdaExpression(
        String parameter,
        PureExpression body
) implements PureExpression {
    public LambdaExpression {
        Objects.requireNonNull(parameter, "Parameter cannot be null");
        Objects.requireNonNull(body, "Body cannot be null");
    }
}
