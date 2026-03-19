package com.gs.legend.ast;

import java.util.List;

/**
 * Lambda function expression.
 *
 * <p>
 * Represents {@code {x | $x.name}} or {@code {x, y | $x + $y}} in Pure.
 *
 * <p>
 * In the legend-engine protocol, LambdaFunction has:
 * <ul>
 * <li>{@code parameters} — List of Variable (the lambda parameters)</li>
 * <li>{@code body} — List of ValueSpecification (the lambda body
 * expressions)</li>
 * </ul>
 *
 * @param parameters The lambda parameter variables
 * @param body       The lambda body expressions (usually a single expression)
 */
public record LambdaFunction(
        List<Variable> parameters,
        List<ValueSpecification> body) implements ValueSpecification {

    public LambdaFunction {
        parameters = List.copyOf(parameters);
        body = List.copyOf(body);
    }

    /**
     * Convenience: creates a lambda with a single body expression.
     */
    public LambdaFunction(List<Variable> parameters, ValueSpecification singleBody) {
        this(parameters, List.of(singleBody));
    }
}
