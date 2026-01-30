package org.finos.legend.pure.dsl;

import java.util.List;
import java.util.Objects;

/**
 * Represents a lambda expression with one or more parameters.
 * 
 * Examples:
 * p | $p.lastName == 'Smith' (single param)
 * {l, r | $l.id == $r.personId} (multi-param for joins)
 * 
 * @param parameters The lambda parameter names (e.g., ["p"] or ["l", "r"])
 * @param body       The lambda body expression
 */
public record LambdaExpression(
        List<String> parameters,
        PureExpression body) implements PureExpression {

    public LambdaExpression {
        Objects.requireNonNull(parameters, "Parameters cannot be null");
        if (parameters.isEmpty()) {
            throw new IllegalArgumentException("Lambda must have at least one parameter");
        }
        Objects.requireNonNull(body, "Body cannot be null");
        // Make immutable copy
        parameters = List.copyOf(parameters);
    }

    /**
     * Convenience constructor for single-parameter lambdas.
     */
    public LambdaExpression(String parameter, PureExpression body) {
        this(List.of(parameter), body);
    }

    /**
     * Get the first (or only) parameter name.
     * For backward compatibility with code expecting single-parameter lambdas.
     */
    public String parameter() {
        return parameters.getFirst();
    }

    /**
     * Check if this is a multi-parameter lambda.
     */
    public boolean isMultiParam() {
        return parameters.size() > 1;
    }
}
